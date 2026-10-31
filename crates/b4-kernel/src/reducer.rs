use b4_contract::{StateWrite, TaskRecord, TaskStatus};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

// Return the exact actual callback unchanged; no body is invoked by this guard.
fn checked_ready_authorization_reducer_value<F>(value: F) -> F {
    const { assert!(std::mem::size_of::<F>() <= 128) };
    value
}

/// Reduce a join into a scratch copy so a rejected patch never partially applies.
pub(crate) fn reduce(
    values: &Map<String, Value>,
    tasks: &[&TaskRecord],
) -> Result<Map<String, Value>, &'static str> {
    let mut writers = BTreeMap::<&str, (BTreeSet<usize>, bool)>::new();
    for (index, task) in tasks.iter().enumerate() {
        if task.status != TaskStatus::Succeeded {
            continue;
        }
        for write in &task.result.as_ref().expect("validated result").writes {
            let (field, replace) = match write {
                StateWrite::Replace { field, .. } => (field.as_str(), true),
                StateWrite::Append { field, .. } => (field.as_str(), false),
            };
            if field.is_empty() {
                return Err("invalid_state_field");
            }
            let (task_writers, has_replace) = writers.entry(field).or_default();
            task_writers.insert(index);
            *has_replace |= replace;
        }
    }
    if writers
        .values()
        .any(checked_ready_authorization_reducer_value(
            |(tasks, replace): &(BTreeSet<usize>, bool)| tasks.len() > 1 && *replace,
        ))
    {
        return Err("conflicting_state_writes");
    }
    let mut next = values.clone();
    for task in tasks {
        if task.status != TaskStatus::Succeeded {
            continue;
        }
        for write in &task.result.as_ref().expect("validated result").writes {
            match write {
                StateWrite::Replace { field, value } => {
                    next.insert(field.clone(), value.clone());
                }
                StateWrite::Append { field, items } => {
                    let value = next.entry(field.clone()).or_insert_with(
                        checked_ready_authorization_reducer_value(|| Value::Array(vec![])),
                    );
                    let Value::Array(array) = value else {
                        return Err("append_target_not_array");
                    };
                    array.extend(items.iter().cloned());
                }
            }
        }
    }
    Ok(next)
}
