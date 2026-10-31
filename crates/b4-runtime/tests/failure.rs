mod support;

use b4_runtime::MemoryStore;

#[test]
fn explicit_failure_is_committed_with_journal_and_releases_admission() {
    support::failures::known_failure(MemoryStore::new());
}

#[test]
fn succeeded_outcome_preserves_success_semantics() {
    support::failures::success_outcome(MemoryStore::new());
}

#[test]
fn outer_error_retains_uncertain_attempt() {
    support::failures::uncertain_error(MemoryStore::new());
}

#[test]
fn rejected_attempt_commit_invokes_no_callback() {
    support::failures::claim_failure(MemoryStore::new());
}

#[test]
fn failed_outcome_commit_retains_borrowed_outcome_without_replay() {
    support::failures::completion_failure(MemoryStore::new());
}

#[test]
fn outcome_completion_checks_owner_attempt_conflicts_and_reason_bounds() {
    support::failures::completion_fences(MemoryStore::new());
}

#[test]
fn failed_batch_drains_in_flight_sibling_before_releasing_admission() {
    support::failures::drain(MemoryStore::new());
}

#[test]
fn outcome_completion_refreshes_revision_and_host_facts() {
    support::failures::fresh_completion(MemoryStore::new());
}

#[test]
fn settled_application_failure_takes_precedence_over_requested_cancel() {
    support::failures::cancel_precedence(MemoryStore::new());
}

#[test]
fn concurrent_outcome_conflict_retains_completion_without_callback_replay() {
    support::failures::concurrent_conflict(MemoryStore::new());
}
