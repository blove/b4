//! Required, native approvals with dedicated sensitive records and atomic continuation.
mod model;
mod service;
mod validation;
pub use model::*;
pub use service::*;
pub use validation::*;

use crate::{Store, StoredExecution};
use b4_contract::{CommitPlan, ExecutionId};
use std::sync::Arc;

pub trait ApprovalStore: Store {
    fn approval_durable(&self) -> bool;
    /// Sensitive host-only view. The service authorizes through ordinary Store
    /// projections before calling this method.
    fn approval_view(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Option<ApprovalView>, ApprovalError>;
    fn commit_approval(
        &self,
        execution_id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError>;
}
impl<S: ApprovalStore + ?Sized> ApprovalStore for Arc<S> {
    fn approval_durable(&self) -> bool {
        (**self).approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        (**self).approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        (**self).commit_approval(id, fence, plan, mutation)
    }
}

pub(crate) fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[usize::from(byte >> 4)] as char);
        result.push(HEX[usize::from(byte & 15)] as char);
    }
    result
}
