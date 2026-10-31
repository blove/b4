use super::REQUEST_DIAGNOSTIC_CAPACITY;
use serde::{ser::Error, Serialize, Serializer};

#[derive(Clone, Copy)]
pub(super) struct RequestId(pub(super) [u8; 53]);

impl Serialize for RequestId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let id = std::str::from_utf8(&self.0)
            .map_err(|_| S::Error::custom("diagnostics_encode_failed"))?;
        serializer.serialize_str(id)
    }
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Event {
    ResponseReady,
    RequestFutureDropped,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum MethodLabel {
    Get,
    Post,
    Delete,
    Other,
}

#[derive(Clone, Copy, Serialize)]
pub(super) struct Record {
    pub(super) request_id: RequestId,
    pub(super) event: Event,
    pub(super) method: MethodLabel,
    pub(super) status_code: Option<u16>,
    pub(super) elapsed_ms: u64,
}

pub(super) struct Ring {
    entries: [Option<Record>; REQUEST_DIAGNOSTIC_CAPACITY],
    head: usize,
    len: usize,
    pub(super) total_evicted_records: u64,
}

pub(super) struct Taken {
    pub(super) records: [Option<Record>; REQUEST_DIAGNOSTIC_CAPACITY],
    pub(super) len: usize,
    pub(super) total_evicted_records: u64,
}

impl Ring {
    pub(super) fn new() -> Self {
        Self {
            entries: [None; REQUEST_DIAGNOSTIC_CAPACITY],
            head: 0,
            len: 0,
            total_evicted_records: 0,
        }
    }

    /// No allocation, serialization or caller code runs under the ring lock.
    pub(super) fn push(&mut self, record: Record) -> bool {
        if self.len == REQUEST_DIAGNOSTIC_CAPACITY {
            self.entries[self.head] = Some(record);
            self.head = (self.head + 1) % REQUEST_DIAGNOSTIC_CAPACITY;
            self.total_evicted_records = self.total_evicted_records.saturating_add(1);
            true
        } else {
            // Both operands stay below the fixed capacity; their sum is <512.
            let index = (self.head + self.len) % REQUEST_DIAGNOSTIC_CAPACITY;
            self.entries[index] = Some(record);
            self.len += 1;
            false
        }
    }

    pub(super) fn take(&mut self) -> Taken {
        let mut taken = Taken {
            records: [None; REQUEST_DIAGNOSTIC_CAPACITY],
            len: self.len,
            total_evicted_records: self.total_evicted_records,
        };
        for (offset, destination) in taken.records[..self.len].iter_mut().enumerate() {
            let index = (self.head + offset) % REQUEST_DIAGNOSTIC_CAPACITY;
            *destination = self.entries[index].take();
        }
        self.head = 0;
        self.len = 0;
        taken
    }
}
