use crate::{DriverConfigError, EffectOutcome};
use b4_contract::{StateWrite, MAX_TASK_FAILURE_BYTES};
use serde_json::Value;
use std::io::{self, Write};

struct Bound {
    used: usize,
    maximum: usize,
}
impl Write for Bound {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.used) {
            return Err(io::Error::other("outcome_limit"));
        }
        self.used += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn depth_ok(root: &Value, nodes_left: &mut usize) -> bool {
    let mut work = vec![(root, 0usize)];
    while let Some((value, depth)) = work.pop() {
        if *nodes_left == 0 {
            return false;
        }
        *nodes_left -= 1;
        match value {
            Value::Array(values) => {
                if depth >= 64 || values.len().saturating_add(work.len()) > *nodes_left {
                    return false;
                }
                work.extend(values.iter().map(|v| (v, depth + 1)));
            }
            Value::Object(values) => {
                if depth >= 64 || values.len().saturating_add(work.len()) > *nodes_left {
                    return false;
                }
                work.extend(values.values().map(|v| (v, depth + 1)));
            }
            _ => {}
        }
    }
    true
}
pub(super) fn bounded(outcome: &EffectOutcome, maximum: usize) -> bool {
    let mut writer = Bound { used: 0, maximum };
    match outcome {
        EffectOutcome::Succeeded(result) => {
            let mut nodes_left = maximum;
            if result.writes.len() > maximum
                || !depth_ok(&result.output, &mut nodes_left)
                || !result.writes.iter().all(|w| match w {
                    StateWrite::Replace { value, .. } => depth_ok(value, &mut nodes_left),
                    StateWrite::Append { items, .. } => {
                        items.len() <= nodes_left
                            && items.iter().all(|v| depth_ok(v, &mut nodes_left))
                    }
                })
            {
                return false;
            }
            serde_json::to_writer(&mut writer, result).is_ok()
        }
        EffectOutcome::Failed(message) => {
            !message.is_empty()
                && message.len() <= MAX_TASK_FAILURE_BYTES
                && serde_json::to_writer(&mut writer, message).is_ok()
        }
        EffectOutcome::Cancelled => true,
    }
}

/// Validate the same finite settled-result bounds used before driver enqueue.
/// Application builders can reject invalid static effects before admitting work.
pub fn validate_effect_outcome(
    outcome: &EffectOutcome,
    maximum: usize,
) -> Result<(), DriverConfigError> {
    if !(1..=65_536).contains(&maximum) {
        return Err(DriverConfigError("invalid_outcome_byte_limit"));
    }
    if !bounded(outcome, maximum) {
        return Err(DriverConfigError("effect_outcome_limit"));
    }
    Ok(())
}
