//! Explicit, bounded prior-owner confinement for trusted embeddings.

pub mod audit;
pub mod backend;
mod model;
pub mod shape;

pub use b4_kernel::RecoveryConfinementReason;
pub use model::{
    RecoveryConfinementBackendError, RecoveryConfinementOutcome, RecoveryConfinementReceipt,
    RecoveryConfinementTarget,
};

#[cfg(test)]
mod tests;
