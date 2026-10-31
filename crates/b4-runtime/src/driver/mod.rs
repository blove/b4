//! Synchronous single-writer scheduling with scoped, bounded native callbacks.
mod capacity;
mod completion;
mod coordinator;
mod identity;
mod model;
mod outcome;
pub use capacity::TaskCapacity;
pub use coordinator::drive_execution;
pub use model::*;
pub use outcome::validate_effect_outcome;
