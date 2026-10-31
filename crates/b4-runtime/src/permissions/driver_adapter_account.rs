//! Independent functional limits for the narrow Driver bridge.
use super::PermissionError;
use serde::Serialize;
use std::io::{self, Write};

const BYTES: usize = 16 * 1024 * 1024;
const NODES: usize = 131_072;
const OPERATIONS: usize = 128;
const VALUE_BYTES: usize = 65_536;
const VALUE_NODES: usize = 16_384;
const DEPTH: usize = 64;

/// Bounded copied observations; construction and counters remain private.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermissionUseDriverUsage {
    bytes: usize,
    nodes: usize,
    operations: usize,
    completion_entries: usize,
    unsupported_completions: usize,
    committed_completions: usize,
    refused_completions: usize,
    candidate_entries: usize,
    candidate_refusals: usize,
}
impl PermissionUseDriverUsage {
    pub fn encoded_bytes(self) -> usize {
        self.bytes
    }
    pub fn structural_nodes(self) -> usize {
        self.nodes
    }
    pub fn operations(self) -> usize {
        self.operations
    }
    pub fn completion_entries(self) -> usize {
        self.completion_entries
    }
    pub fn unsupported_completions(self) -> usize {
        self.unsupported_completions
    }
    pub fn committed_completions(self) -> usize {
        self.committed_completions
    }
    pub fn refused_completions(self) -> usize {
        self.refused_completions
    }
    pub fn candidate_entries(self) -> usize {
        self.candidate_entries
    }
    pub fn candidate_refusals(self) -> usize {
        self.candidate_refusals
    }
}

pub(super) struct DriverAdapterAccount {
    usage: PermissionUseDriverUsage,
    exhausted: bool,
    original_always: bool,
}
impl Default for DriverAdapterAccount {
    fn default() -> Self {
        Self {
            usage: PermissionUseDriverUsage {
                bytes: 0,
                nodes: 0,
                operations: 0,
                completion_entries: 0,
                unsupported_completions: 0,
                committed_completions: 0,
                refused_completions: 0,
                candidate_entries: 0,
                candidate_refusals: 0,
            },
            exhausted: false,
            original_always: false,
        }
    }
}
impl DriverAdapterAccount {
    // Selected only by the explicit original entry. Actual NewHuman provenance
    // remains checked by claim production before callback work can be armed.
    pub(super) fn for_original_always() -> Self {
        Self {
            original_always: true,
            ..Self::default()
        }
    }
    fn turn_history_limit(&self) -> usize {
        if self.original_always {
            2
        } else {
            1
        }
    }
    pub(super) fn usage(&self) -> PermissionUseDriverUsage {
        self.usage
    }
    pub(super) fn purchase(
        &mut self,
        bytes: usize,
        nodes: usize,
        operations: usize,
    ) -> Result<(), PermissionError> {
        let next = (
            self.usage.bytes.checked_add(bytes),
            self.usage.nodes.checked_add(nodes),
            self.usage.operations.checked_add(operations),
        );
        match next {
            (Some(b), Some(n), Some(o))
                if !self.exhausted && b <= BYTES && n <= NODES && o <= OPERATIONS =>
            {
                self.usage.bytes = b;
                self.usage.nodes = n;
                self.usage.operations = o;
                Ok(())
            }
            _ => {
                self.exhausted = true;
                Err(PermissionError::LimitExceeded)
            }
        }
    }
    pub(super) fn begin_completion(&mut self) -> Result<(), PermissionError> {
        self.purchase(0, 0, 1)?;
        self.usage.completion_entries += 1;
        Ok(())
    }
    pub(super) fn unsupported_completion(&mut self) {
        self.usage.unsupported_completions += 1;
        self.finish_completion(false);
    }
    // These infallible observations are bounded by prepaid completion/phase
    // entries (at most 128); acknowledgement adds no fallible debit or copy.
    pub(super) fn finish_completion(&mut self, committed: bool) {
        if committed {
            self.usage.committed_completions += 1;
        } else {
            self.usage.refused_completions += 1;
        }
    }
    pub(super) fn before_candidate(&mut self) -> Result<(), PermissionError> {
        self.purchase(256, 32, 1)?;
        self.usage.candidate_entries += 1;
        Ok(())
    }
    pub(super) fn refused_candidate(&mut self) {
        self.usage.candidate_refusals += 1;
    }
    /// Streaming traversal has no aggregate buffer. Every emitted chunk is paid
    /// before inspection; lexical state persists across serializer chunk splits.
    pub(super) fn measure<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(usize, usize), PermissionError> {
        self.purchase(0, 0, 1)?;
        let mut sink = Measure {
            account: self,
            bytes: 0,
            nodes: 0,
            depth: 0,
            string: false,
            escape: false,
            bare: false,
        };
        if serde_json::to_writer(&mut sink, value).is_err() {
            sink.account.exhausted = true;
            return Err(PermissionError::LimitExceeded);
        }
        Ok((sink.bytes, sink.nodes))
    }
    pub(super) fn before_copy<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), PermissionError> {
        let (bytes, nodes) = self.measure(value)?;
        self.purchase(bytes, nodes, 1)
    }
}
// Bound caller-owned JSON before the serializer can inspect/escape a String.
// Lengths are observed in constant time; each reached key/value is paid before
// traversal. Conservative six-byte JSON escaping keeps this finite profile small.
struct RawBound {
    bytes: usize,
    nodes: usize,
}
impl DriverAdapterAccount {
    pub(super) fn json_map(
        &mut self,
        map: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), PermissionError> {
        self.purchase(0, 0, 1)?;
        let mut bound = RawBound { bytes: 0, nodes: 0 };
        let result = self.raw_map(map, 1, &mut bound);
        if result.is_err() {
            self.exhausted = true;
        }
        result
    }
    fn raw_charge(
        &mut self,
        bytes: usize,
        nodes: usize,
        bound: &mut RawBound,
    ) -> Result<(), PermissionError> {
        bound.bytes = bound
            .bytes
            .checked_add(bytes)
            .ok_or(PermissionError::LimitExceeded)?;
        bound.nodes = bound
            .nodes
            .checked_add(nodes)
            .ok_or(PermissionError::LimitExceeded)?;
        if bound.bytes > VALUE_BYTES || bound.nodes > VALUE_NODES {
            return Err(PermissionError::LimitExceeded);
        }
        self.purchase(bytes, nodes, 0)
    }
    fn raw_string(&mut self, text: &str, bound: &mut RawBound) -> Result<(), PermissionError> {
        let encoded = text
            .len()
            .checked_mul(6)
            .and_then(|n| n.checked_add(2))
            .ok_or(PermissionError::LimitExceeded)?;
        self.raw_charge(encoded, 1, bound)
    }
    fn raw_map(
        &mut self,
        map: &serde_json::Map<String, serde_json::Value>,
        depth: usize,
        bound: &mut RawBound,
    ) -> Result<(), PermissionError> {
        if depth > DEPTH || map.len() > VALUE_NODES.saturating_sub(bound.nodes) {
            return Err(PermissionError::LimitExceeded);
        }
        self.raw_charge(2, 1, bound)?;
        for (key, value) in map {
            self.raw_string(key, bound)?;
            self.raw_value(value, depth, bound)?;
        }
        Ok(())
    }
    fn raw_value(
        &mut self,
        value: &serde_json::Value,
        depth: usize,
        bound: &mut RawBound,
    ) -> Result<(), PermissionError> {
        match value {
            serde_json::Value::Object(map) => self.raw_map(map, depth + 1, bound),
            serde_json::Value::Array(items) => {
                if depth >= DEPTH || items.len() > VALUE_NODES.saturating_sub(bound.nodes) {
                    return Err(PermissionError::LimitExceeded);
                }
                self.raw_charge(2, 1, bound)?;
                for item in items {
                    self.raw_value(item, depth + 1, bound)?;
                }
                Ok(())
            }
            serde_json::Value::String(text) => self.raw_string(text, bound),
            _ => self.raw_charge(32, 1, bound),
        }
    }
    pub(super) fn snapshot(
        &mut self,
        execution: &crate::StoredExecution,
    ) -> Result<(), PermissionError> {
        let snapshot = &execution.snapshot;
        // Only the approved Ready / claimed InFlight shape enters Task1.
        if snapshot.tasks.len() != 1
            || snapshot.definition.batches.len() != 1
            || snapshot.definition.batches[0].len() != 1
            || snapshot.turn_history.len() > self.turn_history_limit()
            || snapshot.attempt_history.len() > 1
            || snapshot.output.is_some()
            || snapshot.failure.is_some()
            || snapshot.tasks[0].result.is_some()
            || snapshot.tasks[0].failure.is_some()
            || snapshot.tasks[0]
                .interrupt
                .as_ref()
                .is_some_and(|interrupt| !self.original_always && !interrupt.prompt.is_null())
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if self.original_always {
            if let Some(interrupt) = &snapshot.tasks[0].interrupt {
                self.json_value(&interrupt.prompt)?;
            }
        }
        self.json_map(&snapshot.values)?;
        self.json_map(&snapshot.tasks[0].input)
    }
}

struct Measure<'a> {
    account: &'a mut DriverAdapterAccount,
    bytes: usize,
    nodes: usize,
    depth: usize,
    string: bool,
    escape: bool,
    bare: bool,
}
impl Measure<'_> {
    fn node(&mut self) -> io::Result<()> {
        self.account
            .purchase(0, 1, 0)
            .map_err(|_| io::Error::other("driver_limit"))?;
        self.nodes += 1;
        if self.nodes > VALUE_NODES {
            return Err(io::Error::other("driver_value_nodes"));
        }
        Ok(())
    }
}
impl Write for Measure<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("driver_value_bytes"))?;
        if next > VALUE_BYTES {
            return Err(io::Error::other("driver_value_bytes"));
        }
        self.account
            .purchase(bytes.len(), 0, 0)
            .map_err(|_| io::Error::other("driver_limit"))?;
        self.bytes = next;
        for byte in bytes {
            if self.string {
                if self.escape {
                    self.escape = false
                } else if *byte == b'\\' {
                    self.escape = true
                } else if *byte == b'"' {
                    self.string = false
                }
                continue;
            }
            match byte {
                b'"' => {
                    self.node()?;
                    self.string = true;
                    self.bare = false;
                }
                b'{' | b'[' => {
                    self.node()?;
                    self.depth += 1;
                    self.bare = false;
                    if self.depth > DEPTH {
                        return Err(io::Error::other("driver_value_depth"));
                    }
                }
                b'}' | b']' => {
                    self.depth = self
                        .depth
                        .checked_sub(1)
                        .ok_or_else(|| io::Error::other("driver_value_depth"))?;
                    self.bare = false;
                }
                b',' | b':' | b' ' | b'\n' | b'\r' | b'\t' => self.bare = false,
                _ if !self.bare => {
                    self.node()?;
                    self.bare = true;
                }
                _ => {}
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl DriverAdapterAccount {
    fn json_value(&mut self, value: &serde_json::Value) -> Result<(), PermissionError> {
        self.purchase(0, 0, 1)?;
        let mut bound = RawBound { bytes: 0, nodes: 0 };
        let result = self.raw_value(value, 0, &mut bound);
        if result.is_err() {
            self.exhausted = true;
        }
        result
    }
    fn text(&mut self, text: &str) -> Result<(), PermissionError> {
        let mut bound = RawBound { bytes: 0, nodes: 0 };
        let result = self.raw_string(text, &mut bound);
        if result.is_err() {
            self.exhausted = true;
        }
        result
    }
    // Terminal candidate traversal is separate from the initial/PreparedEffect
    // profile. It covers every JSON-bearing result/write before serialization.
    pub(super) fn plan(&mut self, plan: &b4_contract::CommitPlan) -> Result<(), PermissionError> {
        let snapshot = &plan.snapshot;
        if snapshot.tasks.len() != 1
            || snapshot.definition.batches.len() != 1
            || snapshot.definition.batches[0].len() != 1
            || snapshot.turn_history.len() > self.turn_history_limit()
            || snapshot.attempt_history.len() > 1
            || plan.dispatchable.len() > 1
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.json_map(&snapshot.values)?;
        if let Some(value) = &snapshot.output {
            self.json_value(value)?;
        }
        if let Some(message) = &snapshot.failure {
            self.text(message)?;
        }
        let task = &snapshot.tasks[0];
        self.json_map(&task.input)?;
        if let Some(interrupt) = &task.interrupt {
            self.json_value(&interrupt.prompt)?;
        }
        if let Some(message) = &task.failure {
            self.text(message)?;
        }
        if let Some(result) = &task.result {
            self.json_value(&result.output)?;
            if result.writes.len() > VALUE_NODES {
                self.exhausted = true;
                return Err(PermissionError::LimitExceeded);
            }
            self.purchase(0, result.writes.len(), 0)?;
            for write in &result.writes {
                match write {
                    b4_contract::StateWrite::Replace { field, value } => {
                        self.text(field)?;
                        self.json_value(value)?;
                    }
                    b4_contract::StateWrite::Append { field, items } => {
                        self.text(field)?;
                        if items.len() > VALUE_NODES {
                            self.exhausted = true;
                            return Err(PermissionError::LimitExceeded);
                        }
                        self.purchase(0, items.len(), 0)?;
                        for item in items {
                            self.json_value(item)?;
                        }
                    }
                }
            }
        }
        self.measure(plan)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn driver_adapter_account_is_cumulative_and_sticky() {
        let mut account = DriverAdapterAccount::default();
        assert!(account.purchase(BYTES, NODES, OPERATIONS).is_ok());
        assert!(account.purchase(1, 0, 0).is_err());
        assert!(account.purchase(0, 0, 0).is_err());
        let mut overflow = DriverAdapterAccount::default();
        assert!(overflow.purchase(1, 0, 0).is_ok());
        assert!(overflow.purchase(usize::MAX, 0, 0).is_err());
        assert!(overflow.purchase(0, 0, 0).is_err());
    }
    #[test]
    fn driver_adapter_measure_chunk_boundaries_and_escapes() {
        let mut account = DriverAdapterAccount::default();
        let mut sink = Measure {
            account: &mut account,
            bytes: 0,
            nodes: 0,
            depth: 0,
            string: false,
            escape: false,
            bare: false,
        };
        for chunk in [
            b"{\"a".as_slice(),
            b"\\",
            b"\"{",
            b"}\":",
            b"12",
            b"3,\"b\":[true,null]}",
        ] {
            sink.write_all(chunk).unwrap()
        }
        assert_eq!(sink.nodes, 7); // object, two keys, number, array, true, null
        assert_eq!(sink.depth, 0);
        assert!(!sink.string);
        assert!(!sink.escape);
    }
    #[test]
    fn driver_adapter_raw_json_refuses_large_strings_before_serialization() {
        let mut account = DriverAdapterAccount::default();
        let map = serde_json::Map::from_iter([(
            "input".to_owned(),
            serde_json::Value::String("x".repeat(VALUE_BYTES)),
        )]);
        assert!(account.json_map(&map).is_err());
        assert!(account.usage().encoded_bytes() < 100);
        assert!(account.measure(&true).is_err());
    }
    #[test]
    fn driver_adapter_measure_caps_before_unbounded_copy() {
        let mut account = DriverAdapterAccount::default();
        assert!(account.measure(&"x".repeat(VALUE_BYTES)).is_err());
        assert!(account.measure(&true).is_err());
        let mut value = serde_json::Value::Null;
        for _ in 0..DEPTH {
            value = serde_json::Value::Array(vec![value]);
        }
        assert!(DriverAdapterAccount::default().measure(&value).is_ok());
        let deeper = serde_json::Value::Array(vec![value]);
        assert!(DriverAdapterAccount::default().measure(&deeper).is_err());
        assert!(DriverAdapterAccount::default()
            .measure(&"x".repeat(VALUE_BYTES - 2))
            .is_ok());
        assert!(DriverAdapterAccount::default()
            .measure(&vec![0u8; VALUE_NODES - 1])
            .is_ok());
        assert!(DriverAdapterAccount::default()
            .measure(&vec![0u8; VALUE_NODES])
            .is_err());
    }
}
