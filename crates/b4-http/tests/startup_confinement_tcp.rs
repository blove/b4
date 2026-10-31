#[allow(dead_code)]
mod startup_confinement_support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparing_listener_stays_closed_through_confinement_authority_and_cleanup() {
    tokio::time::timeout(
        std::time::Duration::from_secs(120),
        startup_confinement_support::run_tcp_single(),
    )
    .await
    .expect("whole TCP qualifier watchdog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_startup_opens_after_confinement_before_runnable_callback_settlement() {
    tokio::time::timeout(
        std::time::Duration::from_secs(120),
        startup_confinement_support::run_tcp_mixed(),
    )
    .await
    .expect("whole mixed TCP qualifier watchdog");
}
