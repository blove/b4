#![cfg(unix)]
mod startup_restart_support;
use startup_restart_support::{child_main, run_lane, Failure, LANE_WAIT};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_startup_park_preserves_once_and_sibling_without_replay() {
    tokio::time::timeout(LANE_WAIT, run_lane("approval"))
        .await
        .expect("restart lane watchdog expired")
        .expect("restart approval qualification failed");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_startup_unsafe_effect_is_blocked_before_readiness_without_replay() {
    tokio::time::timeout(LANE_WAIT, run_lane("unsafe"))
        .await
        .expect("restart lane watchdog expired")
        .expect("restart unsafe qualification failed");
}
/// Child selections are infrastructure, never credited parent behavioral tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "invoked only by the owned startup restart subprocess harness"]
async fn startup_restart_child_helper() {
    let result: std::result::Result<(), Failure> = child_main().await;
    result.expect("startup restart child failed");
}
