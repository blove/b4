//! Private-source-only candidate. Root source review precedes adoption/execution.
mod evidence;
mod fixture;
mod model;
mod process;
mod server;

pub use fixture::{child_main, run_lane};
pub use model::Failure;

pub const WAIT: std::time::Duration = std::time::Duration::from_secs(30);
pub const GATE_WAIT: std::time::Duration = std::time::Duration::from_secs(60);
pub const LANE_WAIT: std::time::Duration = std::time::Duration::from_secs(120);
pub const THREAD: &str = "restart-selected-thread";
pub const HEALTH_THREAD: &str = "restart-health-thread";
pub const APPLICATION: &str = "restart-fixture-application";
pub const ASSEMBLY: &str = "restart-fixture-assembly-1";
pub const POLICY: &str = "restart-fixture-policy-1";
pub const CREDENTIAL: &str = "Bearer restart-fixture";
pub const CHILD_ROOT: &str = "B4_STARTUP_RESTART_ROOT";
pub const CHILD_LANE: &str = "B4_STARTUP_RESTART_LANE";
pub const CHILD_ROLE: &str = "B4_STARTUP_RESTART_ROLE";
pub const CHILD_SELECTOR: &str = "startup_restart_child_helper";
