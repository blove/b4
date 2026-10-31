//! Bounded, transient projection adapters. These never own execution authority.
use crate::{control::RunControl, viewer::Frame, Operation};
use b4_contract::TaskResult;
use b4_runtime::{EffectInvocation, NativeChunkResult, NativeChunkSink};
use serde::Serialize;
use serde_json::Value;
use std::io::{self, Write};

pub(crate) fn selected(
    enabled: bool,
    operation: Operation,
    new_run: bool,
    continuation: &crate::approval::AcceptedContinuation,
) -> bool {
    enabled
        && operation == Operation::RunStream
        && new_run
        && matches!(
            continuation,
            crate::approval::AcceptedContinuation::Ordinary
        )
}

pub(crate) struct Prepared {
    pub frame: Frame,
    pub entry: Value,
    pub entry_bytes: usize,
}
struct Capped {
    bytes: Vec<u8>,
    cap: usize,
}
impl Write for Capped {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.cap.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("native_projection_limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
// The traversal itself has finite work, including keys and empty containers.
// It runs before serde recursion, encoding, cloning, or acquiring publication.
fn preflight(value: &Value, depth: usize, work: &mut usize) -> Result<(), ()> {
    if depth > 64 {
        return Err(());
    }
    let cost = match value {
        Value::String(s) => s.len().saturating_add(1),
        _ => 1,
    };
    *work = work.checked_sub(cost).ok_or(())?;
    match value {
        Value::Array(values) => {
            for value in values {
                preflight(value, depth + 1, work)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                *work = work.checked_sub(key.len().saturating_add(1)).ok_or(())?;
                preflight(value, depth + 1, work)?;
            }
        }
        _ => (),
    }
    Ok(())
}
fn prepare(name: &'static str, payload: &impl Serialize, cap: usize) -> Result<Prepared, ()> {
    let mut writer = Capped {
        bytes: Vec::new(),
        cap,
    };
    serde_json::to_writer(&mut writer, payload).map_err(|_| ())?;
    let payload: Value = serde_json::from_slice(&writer.bytes).map_err(|_| ())?;
    let data = String::from_utf8(writer.bytes).map_err(|_| ())?;
    #[derive(Serialize)]
    struct Entry<'a> {
        event: &'static str,
        data: &'a Value,
    }
    let mut digest = Capped {
        bytes: Vec::new(),
        cap: cap.saturating_add(64),
    };
    serde_json::to_writer(
        &mut digest,
        &Entry {
            event: name,
            data: &payload,
        },
    )
    .map_err(|_| ())?;
    let entry_bytes = digest.bytes.len();
    let entry = serde_json::from_slice(&digest.bytes).map_err(|_| ())?;
    let bytes = 16usize
        .checked_add(name.len())
        .and_then(|n| n.checked_add(data.len()))
        .ok_or(())?;
    Ok(Prepared {
        frame: Frame {
            name,
            data: data.into(),
            bytes,
        },
        entry,
        entry_bytes,
    })
}
pub(crate) fn chunk(value: &Value, cap: usize) -> Result<Prepared, ()> {
    let mut work = cap;
    preflight(value, 0, &mut work)?;
    prepare("chunk", value, cap)
}
pub(crate) fn tool_call(call: &EffectInvocation, cap: usize) -> Result<Prepared, ()> {
    let mut work = cap;
    // The outer payload and input map each consume a nesting level.
    for (key, value) in &call.input {
        work = work.checked_sub(key.len().saturating_add(1)).ok_or(())?;
        preflight(value, 2, &mut work)?;
    }
    #[derive(Serialize)]
    struct Payload<'a> {
        id: &'a b4_contract::AttemptId,
        name: &'a b4_contract::EffectId,
        input: &'a serde_json::Map<String, Value>,
    }
    prepare(
        "tool_call",
        &Payload {
            id: &call.attempt_id,
            name: &call.effect_id,
            input: &call.input,
        },
        cap,
    )
}
pub(crate) fn tool_result(
    call: &EffectInvocation,
    result: &TaskResult,
    cap: usize,
) -> Result<Prepared, ()> {
    let mut work = cap;
    preflight(&result.output, 1, &mut work)?;
    #[derive(Serialize)]
    struct Payload<'a> {
        id: &'a b4_contract::AttemptId,
        name: &'a b4_contract::EffectId,
        output: &'a Value,
    }
    prepare(
        "tool_result",
        &Payload {
            id: &call.attempt_id,
            name: &call.effect_id,
            output: &result.output,
        },
        cap,
    )
}
pub(crate) struct Sink<'a> {
    pub control: &'a RunControl,
    pub cap: usize,
}
impl NativeChunkSink for Sink<'_> {
    fn emit(&mut self, value: &Value) -> NativeChunkResult {
        self.control.native_chunk(chunk(value, self.cap))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_selection() {
        use crate::approval::AcceptedContinuation::{Ordinary, OriginalAlways};
        use crate::Operation;
        assert!(selected(true, Operation::RunStream, true, &Ordinary));
        assert!(!selected(false, Operation::RunStream, true, &Ordinary));
        assert!(!selected(true, Operation::RunStream, false, &Ordinary));
        assert!(!selected(true, Operation::RunStream, true, &OriginalAlways));
        for operation in [Operation::RunWait, Operation::Resume, Operation::Recover] {
            assert!(!selected(true, operation, true, &Ordinary));
        }
    }
}
