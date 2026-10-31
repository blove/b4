use super::RecoveryConfinementBackendError as Error;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShapeStats {
    pub bytes: usize,
    pub nodes: usize,
}

/// Check borrowed lexical shape before owned decoding. Complete JSON grammar
/// and the decoded record's validity must still be checked by the caller.
pub fn check_raw(payload: &[u8], byte_limit: usize) -> Result<ShapeStats, Error> {
    let mut guard = Guard::new(byte_limit);
    guard.consume(payload)?;
    guard.finish()
}

/// Discard encoded fragments directly, without an intermediate JSON buffer.
pub fn measure<T: Serialize + ?Sized>(value: &T, byte_limit: usize) -> Result<ShapeStats, Error> {
    let mut guard = Guard::new(byte_limit);
    if serde_json::to_writer(&mut guard, value).is_err() {
        return Err(guard.refusal.unwrap_or(Error::Unavailable));
    }
    guard.finish()
}

/// Grow a fallible output buffer only after each fragment passes its bounds.
pub fn encode<T: Serialize + ?Sized>(value: &T, byte_limit: usize) -> Result<Vec<u8>, Error> {
    let mut writer = Output {
        guard: Guard::new(byte_limit),
        bytes: Vec::new(),
    };
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(writer.guard.refusal.unwrap_or(Error::Unavailable));
    }
    writer.guard.finish()?;
    Ok(writer.bytes)
}

const DEPTH: usize = 64;
const NODES: usize = 16_384;

struct Guard {
    byte_limit: usize,
    bytes: usize,
    nodes: usize,
    stack: [u8; DEPTH],
    depth: usize,
    string: bool,
    escaped: bool,
    primitive: bool,
    refusal: Option<Error>,
}

impl Guard {
    fn new(byte_limit: usize) -> Self {
        Self {
            byte_limit,
            bytes: 0,
            nodes: 0,
            stack: [0; DEPTH],
            depth: 0,
            string: false,
            escaped: false,
            primitive: false,
            refusal: None,
        }
    }

    fn consume(&mut self, fragment: &[u8]) -> Result<(), Error> {
        if let Some(error) = self.refusal {
            return Err(error);
        }
        // Resident strings may be scanned for escaping by serde before this
        // fragment arrives. The guard promises no CPU or resident-memory bound.
        if fragment.len() > self.byte_limit - self.bytes {
            return self.refuse(Error::LimitExceeded);
        }
        self.bytes += fragment.len();
        for &byte in fragment {
            if self.string {
                if byte < 0x20 {
                    return self.refuse(Error::CorruptRecord);
                }
                if self.escaped {
                    self.escaped = false;
                } else {
                    match byte {
                        b'\\' => self.escaped = true,
                        b'"' => self.string = false,
                        _ => {}
                    }
                }
                continue;
            }
            match byte {
                b'{' | b'[' => {
                    self.node()?;
                    if self.depth == DEPTH {
                        return self.refuse(Error::LimitExceeded);
                    }
                    self.stack[self.depth] = byte;
                    self.depth += 1;
                    self.primitive = false;
                }
                b'}' | b']' => {
                    let opening = if byte == b'}' { b'{' } else { b'[' };
                    if self.depth == 0 || self.stack[self.depth - 1] != opening {
                        return self.refuse(Error::CorruptRecord);
                    }
                    self.depth -= 1;
                    self.primitive = false;
                }
                b'"' => {
                    self.node()?;
                    self.string = true;
                    self.primitive = false;
                }
                b',' | b':' | b' ' | b'\t' | b'\r' | b'\n' => self.primitive = false,
                _ if !self.primitive => {
                    self.node()?;
                    self.primitive = true;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn finish(&self) -> Result<ShapeStats, Error> {
        if let Some(error) = self.refusal {
            return Err(error);
        }
        if self.string || self.depth != 0 || self.nodes == 0 {
            return Err(Error::CorruptRecord);
        }
        Ok(ShapeStats {
            bytes: self.bytes,
            nodes: self.nodes,
        })
    }

    fn node(&mut self) -> Result<(), Error> {
        if self.nodes == NODES {
            return self.refuse(Error::LimitExceeded);
        }
        self.nodes += 1;
        Ok(())
    }

    fn refuse(&mut self, error: Error) -> Result<(), Error> {
        self.refusal = Some(error);
        Err(error)
    }
}

impl std::io::Write for Guard {
    fn write(&mut self, fragment: &[u8]) -> std::io::Result<usize> {
        self.consume(fragment)
            .map_err(|_| std::io::Error::other("confinement_shape_refused"))?;
        Ok(fragment.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Output {
    guard: Guard,
    bytes: Vec<u8>,
}

impl std::io::Write for Output {
    fn write(&mut self, fragment: &[u8]) -> std::io::Result<usize> {
        self.guard
            .consume(fragment)
            .map_err(|_| std::io::Error::other("confinement_shape_refused"))?;
        // Allocation failure is a static infrastructure refusal. This buffer
        // contains only checked output, and never precedes the shape guard.
        if self.bytes.try_reserve_exact(fragment.len()).is_err() {
            self.guard.refusal = Some(Error::Unavailable);
            return Err(std::io::Error::other("confinement_output_unavailable"));
        }
        self.bytes.extend_from_slice(fragment);
        Ok(fragment.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn raw_strings_escapes_and_object_keys_have_independent_shape_counts() {
        let raw = br#"{"a":[true,null,"brackets [{}] and quote \" and slash \\"],"b":{}}"#;
        serde_json::from_slice::<Value>(raw).unwrap();
        assert_eq!(
            check_raw(raw, raw.len()).unwrap(),
            ShapeStats {
                bytes: raw.len(),
                nodes: 8
            }
        );
        assert_eq!(check_raw(raw, raw.len() - 1), Err(Error::LimitExceeded));
    }

    #[test]
    fn raw_unclosed_mismatched_and_unescaped_control_shapes_refuse_statically() {
        for raw in [
            b"[".as_slice(),
            b"]",
            b"[}",
            b"\"unfinished",
            b"\"control\n\"",
        ] {
            assert_eq!(check_raw(raw, 100), Err(Error::CorruptRecord));
        }
    }

    #[test]
    fn raw_depth_64_is_inclusive_and_65_refuses_before_owned_decode() {
        let at = format!("{}null{}", "[".repeat(64), "]".repeat(64));
        assert_eq!(check_raw(at.as_bytes(), 1_000).unwrap().nodes, 65);
        let over = format!("[{at}]");
        assert_eq!(check_raw(over.as_bytes(), 1_000), Err(Error::LimitExceeded));
    }

    #[test]
    fn raw_nodes_include_container_and_stop_at_16384() {
        let values = vec![Value::Null; 16_383];
        let at = serde_json::to_vec(&values).unwrap();
        assert_eq!(check_raw(&at, 1_048_576).unwrap().nodes, 16_384);
        let over = serde_json::to_vec(&vec![Value::Null; 16_384]).unwrap();
        assert_eq!(check_raw(&over, 1_048_576), Err(Error::LimitExceeded));
    }

    #[test]
    fn borrowed_measurement_counts_encoded_unicode_and_escapes_without_buffer_contract() {
        let value = json!({"text":"é\n\"\\", "items":[true, false, null]});
        let expected = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            measure(&value, expected.len()).unwrap(),
            ShapeStats {
                bytes: expected.len(),
                nodes: 8
            }
        );
        assert_eq!(
            measure(&value, expected.len() - 1),
            Err(Error::LimitExceeded)
        );
    }

    #[test]
    fn borrowed_measurement_checks_container_depth_before_descending_further() {
        let mut value = Value::Null;
        for _ in 0..64 {
            value = Value::Array(vec![value]);
        }
        assert_eq!(measure(&value, 1_000).unwrap().nodes, 65);
        value = Value::Array(vec![value]);
        assert_eq!(measure(&value, 1_000), Err(Error::LimitExceeded));
    }

    #[test]
    fn bounded_encoder_returns_exact_canonical_bytes_or_static_limit() {
        let value = json!({"numbers":[100000000.0, -2, 0], "escaped":"\n\\\""});
        let expected = serde_json::to_vec(&value).unwrap();
        assert_eq!(encode(&value, expected.len()).unwrap(), expected);
        assert_eq!(
            encode(&value, expected.len() - 1),
            Err(Error::LimitExceeded)
        );
        assert_eq!(
            encode(&"resident".repeat(100_000), 10),
            Err(Error::LimitExceeded)
        );
    }

    #[test]
    fn serializer_errors_keep_only_static_unavailable() {
        struct Broken;
        impl Serialize for Broken {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom(
                    "opaque private serialization cause",
                ))
            }
        }
        assert_eq!(measure(&Broken, 100), Err(Error::Unavailable));
        assert_eq!(encode(&Broken, 100), Err(Error::Unavailable));
    }
}
