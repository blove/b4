// Existing legal seed and six-table inspection helpers remain byte exact.
mod fixtures;
// Keep protected legacy helper reexports used by its original test target.
#[allow(unused_imports)]
#[path = "../startup_preparation_support/mod.rs"]
mod inherited;

#[allow(unused_imports)] // API and TCP targets use separate parts of this shared module.
pub use fixtures::{
    run_default_refusal, run_positive, run_principal_authority_refusals, PositiveCase,
};

mod gate;
mod server;

#[allow(unused_imports)]
pub use fixtures::run_tcp_single;

#[allow(unused_imports)]
pub use fixtures::{
    run_already_blocked, run_available_allowed, run_custom_store_refusals, run_scan_limit,
    run_tcp_mixed,
};
