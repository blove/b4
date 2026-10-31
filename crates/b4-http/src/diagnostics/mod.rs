//! Bounded, redacted request-boundary records for local embedding.
mod buffer;
mod observation;

use crate::ConfigError;
use axum::http::Method;
use buffer::{Record, RequestId, Ring, Taken};
use serde::{ser::SerializeSeq, Serialize, Serializer};
use std::{
    fmt,
    io::Write,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, TryLockError,
    },
};

pub(crate) use observation::RequestObservation;

pub const REQUEST_ID_HEADER: &str = "x-b4-request-id";
pub const REQUEST_DIAGNOSTIC_CAPACITY: usize = 256;
pub const MAX_DIAGNOSTIC_BATCH_BYTES: usize = 65_536;

/// A resource-free handle to a destructive, best-effort request record buffer.
#[derive(Clone)]
pub struct RequestDiagnostics {
    inner: Arc<Inner>,
}

struct Inner {
    seed: [u8; 16],
    counter: AtomicU64,
    ring: Mutex<Ring>,
    record_loss: AtomicBool,
    correlation_exhausted: AtomicBool,
    #[cfg(test)]
    finish_gate: Mutex<Option<Arc<observation::FinishGate>>>,
}

impl RequestDiagnostics {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let mut seed = [0; 16];
        let result = getrandom::fill(&mut seed).map(|()| seed).map_err(|_| ());
        Self::from_entropy(result)
    }

    fn from_entropy(seed: Result<[u8; 16], ()>) -> Result<Self, ConfigError> {
        let seed = seed.map_err(|()| ConfigError("diagnostics_random_unavailable"))?;
        Ok(Self {
            inner: Arc::new(Inner {
                seed,
                counter: AtomicU64::new(0),
                ring: Mutex::new(Ring::new()),
                record_loss: AtomicBool::new(false),
                correlation_exhausted: AtomicBool::new(false),
                #[cfg(test)]
                finish_gate: Mutex::new(None),
            }),
        })
    }

    pub(crate) fn start(&self, method: &Method) -> RequestObservation {
        RequestObservation::new(self.inner.clone(), method)
    }

    /// Take retained records in insertion order and encode one bounded batch.
    /// Busy/unavailable leaves the ring untouched; an encoding refusal marks loss
    /// after taking records. This is best-effort export, not durable delivery.
    pub fn drain(&self) -> Result<DiagnosticBatch, DiagnosticsReadError> {
        self.drain_with_cap(MAX_DIAGNOSTIC_BATCH_BYTES)
    }

    fn drain_with_cap(&self, cap: usize) -> Result<DiagnosticBatch, DiagnosticsReadError> {
        let taken = {
            let mut ring = match self.inner.ring.try_lock() {
                Ok(ring) => ring,
                Err(TryLockError::WouldBlock) => return Err(DiagnosticsReadError::Busy),
                Err(TryLockError::Poisoned(_)) => return Err(DiagnosticsReadError::Unavailable),
            };
            ring.take()
        };
        let batch = BatchDto {
            diagnostic_batch_version: 1,
            scope: "request_boundary",
            redacted: true,
            capacity: REQUEST_DIAGNOSTIC_CAPACITY,
            record_loss: self.inner.record_loss.load(Ordering::Relaxed),
            total_evicted_records: taken.total_evicted_records,
            correlation_exhausted: self.inner.correlation_exhausted.load(Ordering::Relaxed),
            records: Records(&taken),
        };
        encode(&batch, cap).inspect_err(|_| {
            self.inner.record_loss.store(true, Ordering::Relaxed);
        })
    }

    #[cfg(test)]
    fn hold_finish(&self) -> Arc<observation::FinishGate> {
        let gate = Arc::new(observation::FinishGate::new());
        *self.inner.finish_gate.lock().unwrap() = Some(gate.clone());
        gate
    }
}

impl Inner {
    fn next_id(&self) -> Option<RequestId> {
        let previous =
            match self
                .counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |counter| {
                    counter.checked_add(1)
                }) {
                Ok(previous) => previous,
                Err(_) => {
                    self.correlation_exhausted.store(true, Ordering::Relaxed);
                    self.record_loss.store(true, Ordering::Relaxed);
                    return None;
                }
            };
        let counter = previous.saturating_add(1);
        let mut id = [0; 53];
        id[..4].copy_from_slice(b"b4r_");
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (index, byte) in self.seed.iter().enumerate() {
            id[4 + index * 2] = HEX[usize::from(byte >> 4)];
            id[5 + index * 2] = HEX[usize::from(byte & 15)];
        }
        id[36] = b'_';
        for (index, byte) in counter.to_be_bytes().iter().enumerate() {
            id[37 + index * 2] = HEX[usize::from(byte >> 4)];
            id[38 + index * 2] = HEX[usize::from(byte & 15)];
        }
        Some(RequestId(id))
    }

    fn observe(&self, record: Record) {
        match self.ring.try_lock() {
            Ok(mut ring) => {
                if ring.push(record) {
                    self.record_loss.store(true, Ordering::Relaxed);
                }
            }
            Err(_) => self.record_loss.store(true, Ordering::Relaxed),
        }
    }
}

#[derive(Serialize)]
struct BatchDto<'a> {
    diagnostic_batch_version: u8,
    scope: &'static str,
    redacted: bool,
    capacity: usize,
    record_loss: bool,
    total_evicted_records: u64,
    correlation_exhausted: bool,
    records: Records<'a>,
}

struct Records<'a>(&'a Taken);

impl Serialize for Records<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let mut sequence = serializer.serialize_seq(Some(self.0.len))?;
        for record in &self.0.records[..self.0.len] {
            let record = record
                .as_ref()
                .ok_or_else(|| S::Error::custom("diagnostics_encode_failed"))?;
            sequence.serialize_element(record)?;
        }
        sequence.end()
    }
}

struct CappedWriter {
    bytes: Vec<u8>,
    cap: usize,
    exceeded: bool,
}

impl Write for CappedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.cap.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other("diagnostics_limit_exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode(batch: &BatchDto<'_>, cap: usize) -> Result<DiagnosticBatch, DiagnosticsReadError> {
    let mut writer = CappedWriter {
        bytes: Vec::new(),
        cap,
        exceeded: false,
    };
    if serde_json::to_writer(&mut writer, batch).is_err() {
        return Err(if writer.exceeded {
            DiagnosticsReadError::LimitExceeded
        } else {
            DiagnosticsReadError::EncodeFailed
        });
    }
    Ok(DiagnosticBatch(writer.bytes))
}

/// Complete compact JSON bytes, without a newline or retained host resources.
pub struct DiagnosticBatch(Vec<u8>);

impl AsRef<[u8]> for DiagnosticBatch {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticsReadError {
    Busy,
    Unavailable,
    EncodeFailed,
    LimitExceeded,
}

impl DiagnosticsReadError {
    pub fn code(self) -> &'static str {
        match self {
            Self::Busy => "diagnostics_busy",
            Self::Unavailable => "diagnostics_unavailable",
            Self::EncodeFailed => "diagnostics_encode_failed",
            Self::LimitExceeded => "diagnostics_limit_exceeded",
        }
    }
}

impl fmt::Debug for DiagnosticsReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl fmt::Display for DiagnosticsReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for DiagnosticsReadError {}

#[cfg(test)]
mod tests;
