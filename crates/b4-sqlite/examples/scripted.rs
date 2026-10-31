//! Run a deterministic effect through the real SQLite commit boundary.
use b4_contract::*;
use b4_kernel::new_execution;
use b4_runtime::{ExecutionFence, Runtime, Store};
use b4_sqlite::SqliteStore;
use serde_json::{json, Map};
use std::time::{SystemTime, UNIX_EPOCH};

fn facts() -> TransitionFacts {
    TransitionFacts {
        now_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("host clock predates Unix epoch")
            .as_millis()
            .try_into()
            .expect("host timestamp exceeds u64"),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let directory = args
        .next()
        .ok_or("usage: scripted <data-directory> <unique-run-id>")?;
    let run_id = args
        .next()
        .ok_or("usage: scripted <data-directory> <unique-run-id>")?;
    if args.next().is_some() {
        return Err("usage: scripted <data-directory> <unique-run-id>".into());
    }
    let runtime = Runtime::new(SqliteStore::open(directory)?);
    let thread_id = ThreadId::new(format!("thread-{run_id}"))?;
    let execution_id = ExecutionId::new(run_id.clone())?;
    let task_id = TaskId::new("fixture")?;
    runtime.store().create_thread(thread_id.clone())?;
    let accepted = runtime.admit(
        &thread_id,
        new_execution(
            execution_id.clone(),
            TurnId::new(format!("turn-{run_id}"))?,
            runtime.store().owner_generation(),
            Definition {
                route: RouteKey::new("/scripted#agent")?,
                revision: ExecutionRevision::new("fixture-v1")?,
                batches: vec![vec![TaskSpec {
                    id: task_id.clone(),
                    effect_id: EffectId::new(format!("effect-{run_id}"))?,
                    recovery: RecoveryPolicy::Pure,
                    requires_approval: false,
                }]],
                max_attempts: 1,
            },
            Map::new(),
            facts(),
        )?,
    )?;
    let running = runtime.apply(
        &execution_id,
        ExecutionFence::from(&accepted.snapshot),
        KernelCommand::Start,
        facts(),
    )?;
    let completed = runtime.dispatch(
        &execution_id,
        ExecutionFence::from(&running.snapshot),
        task_id,
        AttemptId::new(format!("attempt-{run_id}"))?,
        facts(),
        facts,
        |call| {
            Ok(TaskResult {
                output: json!({"effect_id":call.effect_id}),
                writes: vec![StateWrite::Replace {
                    field: "message".into(),
                    value: json!("B4run committed this scripted effect through SQLite."),
                }],
            })
        },
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "execution": completed,
            "journal": runtime.store().journal(&execution_id)?,
            "scope": "native scripted SQLite example; no HTTP or provider"
        }))?
    );
    Ok(())
}
