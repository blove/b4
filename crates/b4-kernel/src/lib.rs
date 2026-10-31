//! Pure execution transitions. Plans must be committed before their effects run.
mod confinement;
mod permissions;
mod reducer;
mod transition;
mod validation;

pub use confinement::{propose_recovery_confinement, RecoveryConfinementReason};
pub use permissions::{authorize_ready, PermissionAuthorizationCommand};
pub use transition::{new_execution, transition, KernelError};
pub use validation::validate_snapshot;
