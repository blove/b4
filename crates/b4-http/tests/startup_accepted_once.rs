#![cfg(unix)]
mod startup_accepted_once_support;
use startup_accepted_once_support::{child_main, run, Failure, LANE_WAIT};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_startup_accepted_once_after_expiry_preserves_intent_and_sibling() {
    tokio::time::timeout(LANE_WAIT, run())
        .await
        .expect("accepted Once lane watchdog expired")
        .expect("accepted Once startup qualification failed");
}

/// An owned child selector supplies infrastructure, never another credited case.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "invoked only by the owned accepted Once subprocess harness"]
async fn startup_accepted_once_child_helper() {
    let result: std::result::Result<(), Failure> = child_main().await;
    result.expect("accepted Once child qualification failed");
}
