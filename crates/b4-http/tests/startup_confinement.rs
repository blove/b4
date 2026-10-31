#[allow(dead_code)]
mod startup_confinement_support;

use startup_confinement_support::{run_positive, PositiveCase};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opt_in_missing_exact_definition_confines_original_prior_owner() {
    run_positive(PositiveCase::MissingDefinition).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opt_in_denied_recover_policy_preserves_accepted_once() {
    run_positive(PositiveCase::DeniedRecover).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_preparation_refusal_remains_unchanged() {
    startup_confinement_support::run_default_refusal().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unresolved_principal_and_denied_confinement_authority_refuse() {
    startup_confinement_support::run_principal_authority_refusals().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audited_prior_block_counts_without_readoption() {
    startup_confinement_support::run_already_blocked().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn available_historical_definition_uses_original_typed_recovery() {
    startup_confinement_support::run_available_allowed().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_and_changed_custom_store_refuse_before_write() {
    startup_confinement_support::run_custom_store_refusals().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_limit_preserves_two_confirmed_blocks_and_stops_third_candidate() {
    startup_confinement_support::run_scan_limit().await;
}
