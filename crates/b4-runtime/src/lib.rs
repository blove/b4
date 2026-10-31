//! Synchronous, trusted-host execution service and transactional store boundary.
pub mod approval;
pub mod confinement;
pub mod discovery;
mod driver;
mod memory;
pub mod permissions;
mod runtime;
pub mod store;
pub use confinement::{
    RecoveryConfinementBackendError, RecoveryConfinementOutcome, RecoveryConfinementReason,
    RecoveryConfinementReceipt, RecoveryConfinementTarget,
};
pub use discovery::{
    DiscoveryError, DiscoveryOwnerRelation, UnfinishedDiscoveryCursor, UnfinishedDiscoveryPage,
    UnfinishedExecutionRef, DISCOVERY_JSON_DEPTH, DISCOVERY_JSON_NODES, DISCOVERY_SCANNED_ROWS,
    DISCOVERY_SNAPSHOT_BYTES, DISCOVERY_TOTAL_SNAPSHOT_BYTES,
};
pub use driver::*;
pub use memory::MemoryStore;
pub use runtime::*;
pub use store::*;
mod application;
pub use application::*;
