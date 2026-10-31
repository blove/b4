use super::NativeOutcome;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Trusted producer of accepted durable cancellation intent.
///
/// This primitive does not authorize or mutate a Store. Its owning host must
/// publish only after accepting its exact durable cancellation decision.
#[derive(Debug)]
pub struct NativeCancellationSignal {
    requested: Arc<AtomicBool>,
}

/// Read-only accepted cancellation intent with no host or application ownership.
///
/// Notification does not mean completion, physical quiescence or rollback.
#[derive(Debug, Clone)]
pub struct NativeCancellationObserver {
    requested: Arc<AtomicBool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ControlledNativeOutcome {
    /// A genuine success or known failure with the existing native semantics.
    Settled(NativeOutcome),
    /// Acknowledges that callback-owned work and descendants stopped before return.
    ///
    /// The host separately validates its exact accepted durable cancellation.
    Cancelled,
}

impl NativeCancellationSignal {
    /// Creates an initially clear signal; dropping it does not cancel anything.
    pub fn new() -> Self {
        Self {
            requested: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns a resource-free read-only observer of this signal alone.
    pub fn observer(&self) -> NativeCancellationObserver {
        NativeCancellationObserver {
            requested: Arc::clone(&self.requested),
        }
    }

    /// Monotonically publishes the trusted owner's accepted durable decision.
    ///
    /// This method itself supplies no Store authority and performs no Store I/O.
    pub fn commit_cancellation(&self) {
        self.requested.store(true, Ordering::Release);
    }
}

impl Default for NativeCancellationSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl NativeCancellationObserver {
    /// Reports accepted intent, independently of callback or owner retirement.
    pub fn cancellation_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}
