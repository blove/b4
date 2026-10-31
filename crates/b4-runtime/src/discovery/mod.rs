//! Bounded recorded observations for trusted embeddings, without recovery authority.

pub mod backend;
pub(crate) mod memory;
mod shape;

#[cfg(test)]
mod tests;

use crate::ExecutionFence;
use b4_contract::{
    ExecutionId, ExecutionRevision, ExecutionStatus, RouteKey, ThreadGeneration, ThreadId,
};
use std::fmt;
use std::sync::Arc;

pub const DISCOVERY_SCANNED_ROWS: usize = 32;
pub const DISCOVERY_SNAPSHOT_BYTES: usize = 1_048_576;
pub const DISCOVERY_TOTAL_SNAPSHOT_BYTES: usize = 33_554_432;
pub const DISCOVERY_JSON_DEPTH: usize = 64;
pub const DISCOVERY_JSON_NODES: usize = 16_384;

/// Recorded ownership relative to this owned Store instance, not retry eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryOwnerRelation {
    Current,
    Prior,
}

/// Sensitive typed identities and fences, for separately authorized recovery.
///
/// This observation contains no execution payload or permission decision. It must
/// not be automatically logged or published as an operator diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnfinishedExecutionRef {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub execution_id: ExecutionId,
    pub execution_fence: ExecutionFence,
    pub resource_revision: u64,
    pub route: RouteKey,
    pub definition_revision: ExecutionRevision,
    pub status: ExecutionStatus,
    pub owner_relation: DiscoveryOwnerRelation,
    pub has_in_flight_task: bool,
    pub has_outcome_unknown_task: bool,
}

/// One coherent read; continuation does not create a snapshot across pages.
///
/// An empty result may still advance through terminal history. Exhaustion does
/// not certify whole-Store integrity, restart completion or readiness.
#[derive(Debug, Clone)]
pub struct UnfinishedDiscoveryPage {
    pub owner_generation: u64,
    pub scanned_rows: usize,
    pub records: Vec<UnfinishedExecutionRef>,
    pub next_cursor: Option<UnfinishedDiscoveryCursor>,
}

/// A process-local continuation retaining only a marker and bounded identity.
///
/// It owns no Store, connection, owner lock, application or execution resource.
/// Its private source/owner identity must be checked before enumeration.
#[derive(Debug, Clone)]
pub struct UnfinishedDiscoveryCursor {
    marker: Arc<()>,
    owner_generation: u64,
    after: ExecutionId,
}

/// Static discovery refusals; no source data or underlying error is retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryError {
    UnsupportedOperation,
    CursorMismatch,
    LimitExceeded,
    UnsupportedSnapshot,
    UnsupportedSchema,
    OwnerConflict,
    CorruptRecord,
    Unavailable,
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedOperation => "discovery_unsupported_operation",
            Self::CursorMismatch => "discovery_cursor_mismatch",
            Self::LimitExceeded => "discovery_limit_exceeded",
            Self::UnsupportedSnapshot => "discovery_unsupported_snapshot",
            Self::UnsupportedSchema => "discovery_unsupported_schema",
            Self::OwnerConflict => "discovery_owner_conflict",
            Self::CorruptRecord => "discovery_corrupt_record",
            Self::Unavailable => "discovery_unavailable",
        })
    }
}

impl std::error::Error for DiscoveryError {}
