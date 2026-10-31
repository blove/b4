use crate::KernelError;
use b4_contract::Snapshot;
use std::io;

const DEPTH: usize = 64;
const NODES: usize = 16_384;

/// Serialize directly into fixed lexical state, without buffering the encoded
/// Snapshot. A container opening is checked before its children are visited.
pub(super) fn check_snapshot(snapshot: &Snapshot, max_bytes: usize) -> Result<(), KernelError> {
    let mut guard = Guard::new(max_bytes);
    if serde_json::to_writer(&mut guard, snapshot).is_err() {
        return Err(if guard.limit_exceeded {
            limit()
        } else {
            encoding()
        });
    }
    if guard.string || guard.depth != 0 || guard.nodes == 0 {
        return Err(encoding());
    }
    Ok(())
}

fn limit() -> KernelError {
    KernelError::InvalidTransition("recovery_confinement_limit_exceeded")
}

fn encoding() -> KernelError {
    KernelError::InvalidTransition("recovery_confinement_encoding_failed")
}

struct Guard {
    max_bytes: usize,
    bytes: usize,
    nodes: usize,
    stack: [u8; DEPTH],
    depth: usize,
    string: bool,
    escaped: bool,
    primitive: bool,
    limit_exceeded: bool,
}

impl Guard {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            bytes: 0,
            nodes: 0,
            stack: [0; DEPTH],
            depth: 0,
            string: false,
            escaped: false,
            primitive: false,
            limit_exceeded: false,
        }
    }

    fn consume(&mut self, bytes: &[u8]) -> io::Result<()> {
        // Serde may scan an already resident string for escaping before this
        // fragment reaches the writer. These limits do not promise CPU time.
        if self.limit_exceeded || bytes.len() > self.max_bytes - self.bytes {
            return self.refuse_limit();
        }
        self.bytes += bytes.len();
        for &byte in bytes {
            if self.string {
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
                        return self.refuse_limit();
                    }
                    self.stack[self.depth] = byte;
                    self.depth += 1;
                    self.primitive = false;
                }
                b'}' | b']' => {
                    let opening = if byte == b'}' { b'{' } else { b'[' };
                    if self.depth == 0 || self.stack[self.depth - 1] != opening {
                        return Err(io::Error::other("confinement_encoding_failed"));
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

    fn node(&mut self) -> io::Result<()> {
        if self.nodes == NODES {
            return self.refuse_limit();
        }
        self.nodes += 1;
        Ok(())
    }

    fn refuse_limit(&mut self) -> io::Result<()> {
        self.limit_exceeded = true;
        Err(io::Error::other("confinement_shape_refused"))
    }
}

impl io::Write for Guard {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.consume(bytes)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
