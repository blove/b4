//! Authenticated scripted and native Agent Protocol HTTP subset.
//!
//! Applications provide an authenticator, resource policy, store and immutable
//! pure scripted routes or an immutable native application registry. The router uses the embedding application's Tokio
//! executor; owned blocking coordinators use the shared runtime driver. Sibling
//! callbacks run within per-execution and shared task limits; the kernel joins
//! their results in definition order. Request, definition, input, result, queue
//! and active-work limits are finite. Total
//! thread/history retention and connection limits remain application concerns.
//!
//! AP thread/state projections are separate from portable execution snapshots.
//! Run input is retained under the native state `input` key without changing
//! null, scalar or array values. Wait returns the last committed task output;
//! POST stream emits one `done` payload with an `output` property. Explicit
//! scripted void returns wait JSON null and stream `done` `{}`, matching the
//! selected pinned TypeScript observation. A detached/overflowed viewer has no
//! terminal completeness guarantee. Its worker still settles durably.
//!
//! Explicit `new_with_approvals` hosts use the same durable store for required
//! Once/Deny grants and accepted continuation. Dedicated authorized pending
//! projections and interrupt events contain sensitive tokens; ordinary state
//! and terminal/error envelopes do not. Park settles wait with null. Always
//! grants remain unsupported. Read-only attachment uses bounded independent
//! viewers and an immutable native turn cut. Fenced cancellation ends the owned
//! turn's viewer promptly while its worker retains exclusion through effect
//! quiescence. Fenced DELETE removes idle/settled/stable parked history while
//! retaining owned worker exclusion. Prior-owner
//! unfinished executions can enter `recover_execution` with a trusted verified
//! principal and exact `RecoveryTarget` captured from an ordinary coherent view.
//! This known-ID embedding call is absent from the public router and preserves
//! durable approval decisions, original budgets and owned worker guards.
//! Unexpected runtime or store failures mark the host unready and retain
//! uncertain state.

mod supervisor;
pub use supervisor::{
    ShutdownHandle, ShutdownOutcome, ShutdownPhase, ShutdownStatus, ShutdownWaitError,
    DEFAULT_SHUTDOWN_WAIT, MAX_SHUTDOWN_WAIT,
};
mod parser;
pub use parser::parse_json;
mod host;
mod lifecycle;
pub use host::*;
mod application;
mod native_stream;
mod permission_cancellation;
mod permission_deletion;
mod permissions;
pub use permissions::{PermissionHostConfig, PermissionUseHostConfig};
mod capabilities;
pub use application::{HostLimits, NativeHostConfig};
mod approval;
mod attachment;
mod cancellation;
mod claim;
mod confinement;
mod control;
pub use confinement::{
    RecoveryConfinementAuthority, RecoveryConfinementAuthorityRefusal,
    RecoveryConfinementBackendError, RecoveryConfinementError, RecoveryConfinementOutcome,
    RecoveryConfinementReason, RecoveryConfinementReceipt, RecoveryConfinementRefusal,
    RecoveryConfinementTarget,
};
mod deletion;
mod diagnostics;
mod recovery;
mod route;
mod startup;
mod transport;
pub use diagnostics::{
    DiagnosticBatch, DiagnosticsReadError, RequestDiagnostics, MAX_DIAGNOSTIC_BATCH_BYTES,
    REQUEST_DIAGNOSTIC_CAPACITY, REQUEST_ID_HEADER,
};
mod viewer;
mod worker;
pub use approval::ApprovalHostConfig;
pub use recovery::{
    RecoveryAdmission, RecoveryAdmissionError, RecoveryAdoption, RecoveryDisposition,
    RecoveryRefusal, RecoveryTarget,
};
pub use startup::{
    PreparingHttpHost, StartupIncompleteReason, StartupPreparationHandle, StartupPreparationLimits,
    StartupPreparationOutcome, StartupPreparationPhase, StartupPreparationStatus,
    StartupPrincipalResolver, StartupProgress, StartupResolutionError, StartupStartError,
    StartupWaitError, StartupWaitOutcome,
};
pub use startup::{
    StartupConfinementPreparationHandle, StartupConfinementPreparationOutcome,
    StartupConfinementPreparationStatus, StartupConfinementProgress, StartupConfinementWaitOutcome,
};
