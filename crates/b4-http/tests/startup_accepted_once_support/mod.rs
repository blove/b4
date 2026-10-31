//! Separate accepted-Once fixture; inherited low-level sources remain exact.
#[path = "../startup_restart_support/evidence.rs"]
mod evidence;
mod fixture;
mod gates;
mod model;
#[path = "../startup_restart_support/process.rs"]
mod process;
#[path = "../startup_restart_support/server.rs"]
mod server;
mod store;

pub use fixture::{child_main, run};
pub use model::Failure;

pub const WAIT: std::time::Duration = std::time::Duration::from_secs(30);
pub const GATE_WAIT: std::time::Duration = std::time::Duration::from_secs(60);
pub const LANE_WAIT: std::time::Duration = std::time::Duration::from_secs(120);
pub const THREAD: &str = "accepted-once-selected-thread";
pub const APPLICATION: &str = "accepted-once-fixture-application";
pub const ASSEMBLY: &str = "accepted-once-fixture-assembly-1";
pub const POLICY: &str = "accepted-once-fixture-policy-1";
pub const CREDENTIAL: &str = "Bearer accepted-once-fixture";
pub const CHILD_ROOT: &str = "B4_STARTUP_ACCEPTED_ONCE_ROOT";
pub const CHILD_LANE: &str = "B4_STARTUP_ACCEPTED_ONCE_LANE";
pub const CHILD_ROLE: &str = "B4_STARTUP_ACCEPTED_ONCE_ROLE";
pub const CHILD_SELECTOR: &str = "startup_accepted_once_child_helper";
