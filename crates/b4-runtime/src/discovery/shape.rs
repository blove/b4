use super::{DiscoveryError, DISCOVERY_JSON_DEPTH, DISCOVERY_JSON_NODES, DISCOVERY_SNAPSHOT_BYTES};
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ShapeStats {
    pub bytes: usize,
    pub nodes: usize,
    pub depth: usize,
}

/// A lexical budget guard, not a replacement for the JSON decoder. Fixed state
/// counts container openings before serde can visit their children. The writer
/// discards every byte and must be used directly, without buffering in front.
pub(super) struct JsonShapeGuard {
    bytes: usize,
    nodes: usize,
    stack: [u8; DISCOVERY_JSON_DEPTH],
    depth: usize,
    string: bool,
    escaped: bool,
    primitive: bool,
    refusal: Option<DiscoveryError>,
}

impl JsonShapeGuard {
    pub fn new() -> Self {
        Self {
            bytes: 0,
            nodes: 0,
            stack: [0; DISCOVERY_JSON_DEPTH],
            depth: 0,
            string: false,
            escaped: false,
            primitive: false,
            refusal: None,
        }
    }

    pub fn consume(&mut self, bytes: &[u8]) -> Result<(), DiscoveryError> {
        if let Some(error) = self.refusal {
            return Err(error);
        }
        // Check the entire borrowed fragment before scanning it. serde's string
        // formatter may already have scanned resident input to find escapes;
        // this guard makes no CPU-time or resident-memory promise about that.
        if bytes.len() > DISCOVERY_SNAPSHOT_BYTES - self.bytes {
            return self.refuse(DiscoveryError::LimitExceeded);
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
                        0..=0x1f => return self.refuse(DiscoveryError::CorruptRecord),
                        _ => {}
                    }
                }
                continue;
            }
            match byte {
                b'{' | b'[' => {
                    self.node()?;
                    if self.depth == DISCOVERY_JSON_DEPTH {
                        return self.refuse(DiscoveryError::LimitExceeded);
                    }
                    self.stack[self.depth] = byte;
                    self.depth += 1;
                    self.primitive = false;
                }
                b'}' | b']' => {
                    let opening = if byte == b'}' { b'{' } else { b'[' };
                    if self.depth == 0 || self.stack[self.depth - 1] != opening {
                        return self.refuse(DiscoveryError::CorruptRecord);
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

    pub fn finish(&self) -> Result<ShapeStats, DiscoveryError> {
        if let Some(error) = self.refusal {
            return Err(error);
        }
        if self.string || self.depth != 0 || self.nodes == 0 {
            return Err(DiscoveryError::CorruptRecord);
        }
        Ok(ShapeStats {
            bytes: self.bytes,
            nodes: self.nodes,
            depth: self.depth,
        })
    }

    pub fn own_limit_refusal(&self) -> bool {
        self.refusal == Some(DiscoveryError::LimitExceeded)
    }

    fn node(&mut self) -> Result<(), DiscoveryError> {
        if self.nodes == DISCOVERY_JSON_NODES {
            return self.refuse(DiscoveryError::LimitExceeded);
        }
        self.nodes += 1;
        Ok(())
    }

    fn refuse(&mut self, error: DiscoveryError) -> Result<(), DiscoveryError> {
        self.refusal = Some(error);
        Err(error)
    }
}

impl io::Write for JsonShapeGuard {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.consume(bytes)
            .map_err(|_| io::Error::other("discovery_shape_refused"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
