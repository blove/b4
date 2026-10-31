use super::*;
use crate::{validate_commit, StoreError, ThreadView};
use b4_contract::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub fn validate_approval_context(context: &ApprovalContext) -> Result<(), ApprovalError> {
    if !valid_component(&context.policy_revision)
        || [
            &context.actor.application,
            &context.actor.tenant,
            &context.actor.principal,
        ]
        .iter()
        .any(checked_ready_authorization_context_metadata_value(
            |component: &&String| !valid_component(component),
        ))
    {
        return Err(ApprovalError::InvalidContext);
    }
    Ok(())
}
fn valid_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value
            .chars()
            .any(checked_ready_authorization_context_metadata_value(
                char::is_control,
            ))
}
fn digest_shape(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

// Walk borrowed values iteratively before recursive cloning, sorting or serde.
// The input map itself is a container; prompt scalars have depth zero.
fn validate_json_depth<'a>(
    values: impl IntoIterator<Item = &'a Value>,
    initial_depth: usize,
) -> Result<(), ApprovalError> {
    let mut pending: Vec<_> = values
        .into_iter()
        .map(|value| (value, initial_depth))
        .collect();
    while let Some((value, depth)) = pending.pop() {
        let children: Vec<&Value> = match value {
            Value::Object(object) => object.values().collect(),
            Value::Array(array) => array.iter().collect(),
            _ => continue,
        };
        let next_depth = depth + 1;
        if next_depth > MAX_APPROVAL_JSON_DEPTH {
            return Err(ApprovalError::InvalidProposal);
        }
        pending.extend(children.into_iter().map(|value| (value, next_depth)));
    }
    Ok(())
}

/// Versioned canonical proposal bytes. Objects are sorted recursively, arrays
/// preserve order, and explicit nulls remain present.
pub fn canonical_proposal(task: &TaskRecord, prompt: &Value) -> Result<Vec<u8>, ApprovalError> {
    validate_json_depth(std::iter::once(prompt), 0)?;
    validate_json_depth(task.input.values(), 1)?;
    let prompt_bytes = serde_json::to_vec(prompt).map_err(|_| ApprovalError::InvalidProposal)?;
    if prompt_bytes.len() > MAX_APPROVAL_BYTES {
        return Err(ApprovalError::ProposalTooLarge);
    }
    let proposal = json!({"version":1,"task":task.spec,"input":task.input,"prompt":prompt});
    let bytes =
        serde_json::to_vec(&sorted(proposal)).map_err(|_| ApprovalError::InvalidProposal)?;
    if bytes.len() > MAX_APPROVAL_BYTES {
        return Err(ApprovalError::ProposalTooLarge);
    }
    Ok(bytes)
}
fn sorted(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries: Vec<_> = object.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, sorted(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(sorted).collect()),
        other => other,
    }
}
pub fn proposal_digest(task: &TaskRecord, prompt: &Value) -> Result<String, ApprovalError> {
    Ok(hex_bytes(&Sha256::digest(canonical_proposal(
        task, prompt,
    )?)))
}
pub fn token_verifier(token: &ApprovalToken) -> [u8; 32] {
    Sha256::digest(token.as_str().as_bytes()).into()
}
fn same_verifier(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0_u8, |diff, (a, b)| diff | (a ^ b)) == 0
}
pub fn validate_grant_record(grant: &GrantRecord) -> Result<(), ApprovalError> {
    validate_json_depth(std::iter::once(&grant.prompt), 0)?;
    validate_approval_context(&ApprovalContext {
        actor: grant.origin.clone(),
        policy_revision: grant.policy_revision.clone(),
    })?;
    if grant.task_parking_revision == 0
        || !digest_shape(&grant.proposal_digest)
        || !digest_shape(&grant.resume_key)
        || !same_verifier(&grant.verifier, &token_verifier(&grant.token))
    {
        return Err(ApprovalError::InvalidGrant);
    }
    let ttl = grant
        .expires_at_ms
        .checked_sub(grant.issued_at_ms)
        .ok_or(ApprovalError::InvalidTtl)?;
    if ttl == 0 || ttl > MAX_APPROVAL_TTL_MS {
        return Err(ApprovalError::InvalidTtl);
    }
    if serde_json::to_vec(&grant.prompt)
        .map_err(|_| ApprovalError::InvalidProposal)?
        .len()
        > MAX_APPROVAL_BYTES
    {
        return Err(ApprovalError::ProposalTooLarge);
    }
    Ok(())
}
pub fn validate_acceptance_record(acceptance: &ApprovalAcceptance) -> Result<(), ApprovalError> {
    validate_approval_context(&acceptance.context)?;
    let mut tasks = BTreeSet::new();
    let mut interrupts = BTreeSet::new();
    if acceptance.decisions.is_empty()
        || acceptance.decisions.iter().any(|entry| {
            !matches!(
                entry.decision,
                ApprovalDecision::Once | ApprovalDecision::Deny
            ) || entry.task_parking_revision == 0
                || entry.task_parking_revision > acceptance.previous_revision
                || !digest_shape(&entry.proposal_digest)
                || !tasks.insert(entry.task_id.clone())
                || !interrupts.insert(entry.interrupt_id.clone())
        })
    {
        return Err(ApprovalError::InvalidGrant);
    }
    Ok(())
}

pub fn validate_approval_fence(
    view: &ApprovalView,
    fence: ApprovalFence,
    current_owner: u64,
) -> Result<(), ApprovalError> {
    let current = &view.execution.snapshot;
    if current.owner_generation != current_owner
        || fence.execution.owner_generation != current.owner_generation
    {
        return Err(StoreError::OwnerConflict.into());
    }
    if fence.execution.revision != current.revision
        || fence.resource_revision != view.thread.resource_revision
    {
        return Err(StoreError::RevisionConflict.into());
    }
    if view.thread.id != view.execution.thread_id
        || view.thread.active_execution_id.as_ref() != Some(&current.execution_id)
    {
        return Err(StoreError::ExecutionNotActive.into());
    }
    Ok(())
}

pub(crate) fn validate_attachment_pending_resource(
    current: &ThreadView,
    captured: &ThreadView,
    current_owner: u64,
) -> Result<(), ApprovalError> {
    if current != captured {
        return Err(StoreError::RevisionConflict.into());
    }
    let execution = current
        .execution
        .as_ref()
        .ok_or(ApprovalError::NotStablePark)?;
    if execution.thread_id != current.thread.id
        || current.thread.active_execution_id.as_ref() != Some(&execution.snapshot.execution_id)
    {
        return Err(StoreError::Corrupt("attachment_approval_resource_invalid".into()).into());
    }
    b4_kernel::validate_snapshot(&execution.snapshot)
        .map_err(|_| StoreError::Corrupt("attachment_approval_snapshot_invalid".into()))?;
    if execution.snapshot.status != ExecutionStatus::WaitingForApproval {
        return Err(ApprovalError::NotStablePark);
    }
    if current_owner == 0 || execution.snapshot.owner_generation > current_owner {
        return Err(StoreError::OwnerConflict.into());
    }
    Ok(())
}

/// Validate every sensitive record before producing a read-only projection.
/// Stored corruption is infrastructure failure; a genuine current-policy
/// mismatch remains an ordinary refusal.
pub(crate) fn attachment_pending_records(
    view: &ApprovalView,
    context: &ApprovalContext,
) -> Result<Vec<SensitivePendingApproval>, ApprovalError> {
    let corrupt = || {
        ApprovalError::Store(StoreError::Corrupt(
            "attachment_approval_records_invalid".into(),
        ))
    };
    let snapshot = &view.execution.snapshot;
    let mut interrupts = BTreeSet::new();
    let mut unconsumed_tasks = BTreeSet::new();
    for grant in &view.grants {
        let task = snapshot
            .tasks
            .iter()
            .find(|task| task.spec.id == grant.task_id)
            .ok_or_else(corrupt)?;
        valid_binding(view, grant, task, &grant.policy_revision).map_err(|_| corrupt())?;
        if !interrupts.insert(&grant.interrupt_id)
            || grant.issued_at_ms > snapshot.updated_at_ms
            || (!grant.consumed
                && (task.status != TaskStatus::WaitingForApproval
                    || !unconsumed_tasks.insert(&grant.task_id)))
        {
            return Err(corrupt());
        }
    }
    let mut turns = BTreeSet::new();
    let mut revisions = BTreeSet::new();
    let mut accepted_interrupts = BTreeSet::new();
    for acceptance in &view.acceptances {
        validate_acceptance_record(acceptance).map_err(|_| corrupt())?;
        if acceptance.execution_id != snapshot.execution_id
            || acceptance.thread_id != view.thread.id
            || acceptance.thread_generation != view.thread.generation
            || view.thread.origin.as_ref() != Some(&acceptance.context.actor)
            || acceptance.definition_revision != snapshot.definition.revision
            || acceptance.previous_revision >= snapshot.revision
            || acceptance.accepted_at_ms > snapshot.updated_at_ms
            || !snapshot.turn_history.contains(&acceptance.turn_id)
            || !turns.insert(&acceptance.turn_id)
            || !revisions.insert(acceptance.previous_revision)
        {
            return Err(corrupt());
        }
        for decision in &acceptance.decisions {
            let grant = view
                .grants
                .iter()
                .find(|grant| {
                    grant.interrupt_id == decision.interrupt_id && grant.task_id == decision.task_id
                })
                .ok_or_else(corrupt)?;
            let task = snapshot
                .tasks
                .iter()
                .find(|task| task.spec.id == decision.task_id)
                .ok_or_else(corrupt)?;
            if !grant.consumed
                || grant.policy_revision != acceptance.context.policy_revision
                || grant.proposal_digest != decision.proposal_digest
                || grant.task_parking_revision != decision.task_parking_revision
                || grant.task_parking_revision > acceptance.previous_revision
                || grant.issued_at_ms > acceptance.accepted_at_ms
                || acceptance.accepted_at_ms >= grant.expires_at_ms
                || !accepted_interrupts.insert(&decision.interrupt_id)
                || (decision.decision == ApprovalDecision::Once
                    && (!task.approved || task.status == TaskStatus::Denied))
                || (decision.decision == ApprovalDecision::Deny
                    && (task.status != TaskStatus::Denied || task.approved))
            {
                return Err(corrupt());
            }
        }
    }
    if view
        .grants
        .iter()
        .any(|grant| grant.consumed != accepted_interrupts.contains(&grant.interrupt_id))
    {
        return Err(corrupt());
    }
    for task in snapshot.tasks.iter().filter(|task| {
        task.spec.requires_approval && (task.approved || task.status == TaskStatus::Denied)
    }) {
        let matching: Vec<_> = view
            .grants
            .iter()
            .filter(|grant| grant.task_id == task.spec.id)
            .collect();
        if matching.len() != 1
            || !matching[0].consumed
            || !accepted_interrupts.contains(&matching[0].interrupt_id)
        {
            return Err(corrupt());
        }
    }
    let mut pending = Vec::new();
    for task in snapshot
        .tasks
        .iter()
        .filter(|task| task.status == TaskStatus::WaitingForApproval)
    {
        let interrupt = task.interrupt.as_ref().ok_or_else(corrupt)?;
        let grant = view
            .grants
            .iter()
            .find(|grant| grant.interrupt_id == interrupt.id && grant.task_id == task.spec.id)
            .ok_or_else(corrupt)?;
        if grant.consumed {
            return Err(corrupt());
        }
        pending.push(grant);
    }
    if pending.is_empty() || pending.len() != unconsumed_tasks.len() {
        return Err(corrupt());
    }
    if pending
        .iter()
        .any(|grant| grant.policy_revision != context.policy_revision)
    {
        return Err(ApprovalError::InvalidGrant);
    }
    Ok(pending
        .into_iter()
        .map(|grant| SensitivePendingApproval {
            task_id: grant.task_id.clone(),
            interrupt_id: grant.interrupt_id.clone(),
            prompt: grant.prompt.clone(),
            token: grant.token.clone(),
            resume_key: grant.resume_key.clone(),
            expires_at_ms: grant.expires_at_ms,
        })
        .collect())
}
pub(crate) fn valid_binding(
    view: &ApprovalView,
    grant: &GrantRecord,
    task: &TaskRecord,
    policy_revision: &str,
) -> Result<(), ApprovalError> {
    validate_grant_record(grant).map_err(|_| ApprovalError::InvalidGrant)?;
    let current = &view.execution.snapshot;
    let pending = task.interrupt.as_ref().ok_or(ApprovalError::InvalidGrant)?;
    if grant.thread_id != view.thread.id
        || grant.thread_generation != view.thread.generation
        || view.thread.origin.as_ref() != Some(&grant.origin)
        || grant.execution_id != current.execution_id
        || grant.route != current.definition.route
        || grant.definition_revision != current.definition.revision
        || grant.task_id != task.spec.id
        || grant.effect_id != task.spec.effect_id
        || grant.interrupt_id != pending.id
        || grant.prompt != pending.prompt
        || grant.policy_revision != policy_revision
        || grant.task_parking_revision > current.revision
        || grant.proposal_digest
            != proposal_digest(task, &pending.prompt).map_err(|_| ApprovalError::InvalidGrant)?
    {
        return Err(ApprovalError::InvalidGrant);
    }
    Ok(())
}
pub(crate) fn validate_actor(
    view: &ApprovalView,
    context: &ApprovalContext,
) -> Result<(), ApprovalError> {
    validate_approval_context(context)?;
    if view.thread.origin.as_ref() != Some(&context.actor) {
        return Err(ApprovalError::Unauthorized);
    }
    Ok(())
}

/// Validate a complete transaction and reconstruct the exact kernel proposal.
/// Returned records are prepared for persistence; this function mutates nothing.
pub fn validate_approval_change(
    view: &ApprovalView,
    fence: ApprovalFence,
    plan: &CommitPlan,
    mutation: &ApprovalMutation,
    current_owner: u64,
) -> Result<ApprovalChanges, ApprovalError> {
    let current = &view.execution.snapshot;
    match mutation {
        ApprovalMutation::Issue(grant) => {
            validate_approval_fence(view, fence, current_owner)?;
            validate_grant_record(grant)?;
            if grant.consumed
                || grant.task_parking_revision
                    != current
                        .revision
                        .checked_add(1)
                        .ok_or(ApprovalError::InvalidGrant)?
                || grant.issued_at_ms != plan.snapshot.updated_at_ms
                || view.grants.iter().any(|existing| {
                    existing.interrupt_id == grant.interrupt_id
                        || (existing.task_id == grant.task_id && !existing.consumed)
                })
            {
                return Err(ApprovalError::InvalidGrant);
            }
            let expected = b4_kernel::transition(
                current,
                KernelCommand::Park {
                    task_id: grant.task_id.clone(),
                    interrupt_id: grant.interrupt_id.clone(),
                    prompt: grant.prompt.clone(),
                },
                TransitionFacts {
                    now_ms: grant.issued_at_ms,
                },
            )?;
            if expected != *plan {
                return Err(ApprovalError::InvalidProposal);
            }
            let proposed_view = ApprovalView {
                thread: view.thread.clone(),
                execution: crate::StoredExecution {
                    thread_id: view.execution.thread_id.clone(),
                    snapshot: plan.snapshot.clone(),
                },
                grants: vec![],
                acceptances: vec![],
            };
            let task = plan
                .snapshot
                .tasks
                .iter()
                .find(|task| task.spec.id == grant.task_id)
                .ok_or(ApprovalError::InvalidGrant)?;
            valid_binding(&proposed_view, grant, task, &grant.policy_revision)?;
            validate_commit(current, fence.execution, plan, current_owner)?;
            Ok(ApprovalChanges {
                grants: vec![grant.clone()],
                acceptance: None,
            })
        }
        ApprovalMutation::Accept(request) => {
            validate_approval_fence(view, fence, current_owner)?;
            let (expected, changes) = prepare_acceptance(view, request)?;
            if expected != *plan {
                return Err(ApprovalError::InvalidProposal);
            }
            validate_commit(current, fence.execution, plan, current_owner)?;
            Ok(changes)
        }
        ApprovalMutation::Recover(request) => {
            let expected = prepare_recovery(view, fence, request, current_owner)?;
            if expected != *plan {
                return Err(ApprovalError::InvalidProposal);
            }
            validate_commit(current, fence.execution, plan, current_owner)?;
            Ok(ApprovalChanges {
                grants: vec![],
                acceptance: None,
            })
        }
    }
}

/// Recovery alone may compare an older owner. Reconstructing the exact kernel
/// plan prevents this authority path from approving a forged ordinary command.
pub(crate) fn prepare_recovery(
    view: &ApprovalView,
    fence: ApprovalFence,
    request: &ApprovalRecoveryRequest,
    current_owner: u64,
) -> Result<CommitPlan, ApprovalError> {
    validate_actor(view, &request.context)?;
    let current = &view.execution.snapshot;
    if request.thread_id != view.thread.id
        || view.thread.id != view.execution.thread_id
        || request.execution_id != current.execution_id
        || view.thread.active_execution_id.as_ref() != Some(&current.execution_id)
    {
        return Err(StoreError::ExecutionNotActive.into());
    }
    if request.thread_generation != view.thread.generation
        || fence.execution.revision != current.revision
        || fence.resource_revision != view.thread.resource_revision
    {
        return Err(StoreError::RevisionConflict.into());
    }
    if fence.execution.owner_generation != current.owner_generation
        || current_owner == 0
        || current.owner_generation > current_owner
    {
        return Err(StoreError::OwnerConflict.into());
    }
    if current.owner_generation == current_owner
        || !matches!(
            current.status,
            ExecutionStatus::Accepted
                | ExecutionStatus::Running
                | ExecutionStatus::WaitingForApproval
                | ExecutionStatus::CancelRequested
                | ExecutionStatus::RecoveryBlocked
        )
    {
        return Err(ApprovalError::RecoveryNotNeeded);
    }
    if request.available_definition != current.definition {
        return Err(ApprovalError::RecoveryDefinitionMismatch);
    }
    Ok(b4_kernel::transition(
        current,
        KernelCommand::Recover {
            available_revision: request.available_definition.revision.clone(),
            turn_id: request.turn_id.clone(),
            owner_generation: current_owner,
        },
        request.facts.clone(),
    )?)
}

pub(crate) fn prepare_acceptance(
    view: &ApprovalView,
    request: &ApprovalAcceptanceRequest,
) -> Result<(CommitPlan, ApprovalChanges), ApprovalError> {
    let current = &view.execution.snapshot;
    validate_actor(view, &request.context)?;
    if current.status != ExecutionStatus::WaitingForApproval {
        return Err(ApprovalError::NotStablePark);
    }
    if request.route != current.definition.route
        || request.definition_revision != current.definition.revision
    {
        return Err(ApprovalError::RouteMismatch);
    }
    let pending: Vec<_> = current
        .tasks
        .iter()
        .filter(|task| task.status == TaskStatus::WaitingForApproval)
        .collect();
    let pending_ids: BTreeSet<_> = pending
        .iter()
        .map(|task| {
            task.interrupt
                .as_ref()
                .expect("validated snapshot")
                .id
                .clone()
        })
        .collect();
    let submitted: BTreeSet<_> = request
        .decisions
        .iter()
        .map(|entry| entry.interrupt_id.clone())
        .collect();
    if pending_ids.is_empty()
        || pending_ids != submitted
        || submitted.len() != request.decisions.len()
    {
        return Err(ApprovalError::InterruptSetMismatch);
    }
    if request
        .decisions
        .iter()
        .any(|entry| entry.decision == ApprovalDecision::Always)
    {
        return Err(ApprovalError::UnsupportedAlways);
    }
    let mut grants = Vec::new();
    let mut decisions = Vec::new();
    let mut kernel_decisions = Vec::new();
    for task in pending {
        let interrupt = &task.interrupt.as_ref().expect("validated snapshot").id;
        let mut matching = view
            .grants
            .iter()
            .filter(|grant| &grant.interrupt_id == interrupt);
        let grant = matching.next().ok_or(ApprovalError::GrantUnavailable)?;
        if matching.next().is_some() {
            return Err(ApprovalError::InvalidGrant);
        }
        let submitted = request
            .decisions
            .iter()
            .find(|entry| &entry.interrupt_id == interrupt)
            .expect("validated exact set");
        let token = submitted
            .token
            .as_ref()
            .ok_or(ApprovalError::MissingToken)?;
        valid_binding(view, grant, task, &request.context.policy_revision)?;
        if !same_verifier(&grant.verifier, &token_verifier(token)) {
            return Err(ApprovalError::InvalidGrant);
        }
        if grant.consumed {
            return Err(ApprovalError::Consumed);
        }
        if request.now_ms >= grant.expires_at_ms {
            return Err(ApprovalError::Expired);
        }
        let decision = if submitted.cancelled {
            ApprovalDecision::Deny
        } else {
            submitted.decision
        };
        decisions.push(AcceptedDecision {
            task_id: task.spec.id.clone(),
            interrupt_id: interrupt.clone(),
            decision,
            proposal_digest: grant.proposal_digest.clone(),
            task_parking_revision: grant.task_parking_revision,
        });
        kernel_decisions.push(Decision {
            interrupt_id: interrupt.clone(),
            decision,
        });
        let mut consumed = grant.clone();
        consumed.consumed = true;
        grants.push(consumed);
    }
    let expected = b4_kernel::transition(
        current,
        KernelCommand::Resume {
            route: request.route.clone(),
            decisions: kernel_decisions,
            turn_id: request.turn_id.clone(),
        },
        TransitionFacts {
            now_ms: request.now_ms,
        },
    )?;
    let acceptance = ApprovalAcceptance {
        execution_id: current.execution_id.clone(),
        thread_id: view.thread.id.clone(),
        thread_generation: view.thread.generation.clone(),
        context: request.context.clone(),
        previous_revision: current.revision,
        turn_id: request.turn_id.clone(),
        definition_revision: current.definition.revision.clone(),
        accepted_at_ms: request.now_ms,
        decisions,
    };
    validate_acceptance_record(&acceptance)?;
    Ok((
        expected,
        ApprovalChanges {
            grants,
            acceptance: Some(acceptance),
        },
    ))
}

/// Required Ready tasks gain approval only through the permission transaction.
/// Required-grant tasks are approved or denied only by the grant transaction.
/// Trusted recovery and cancellation remain available through ordinary commits.
pub fn reject_unrecorded_decisions(
    current: &Snapshot,
    proposed: &Snapshot,
    grants: &[GrantRecord],
) -> Result<(), StoreError> {
    for task in &current.tasks {
        if task.spec.requires_approval && task.status == TaskStatus::Ready && !task.approved {
            for next in &proposed.tasks {
                if next.spec.id == task.spec.id && next.approved {
                    return Err(StoreError::InvalidCommit(
                        "required_permission_authorization_transaction".into(),
                    ));
                }
            }
        }
        if task.status != TaskStatus::WaitingForApproval
            || !grants.iter().any(|grant| {
                grant.execution_id == current.execution_id
                    && grant.task_id == task.spec.id
                    && task
                        .interrupt
                        .as_ref()
                        .is_some_and(|pending| pending.id == grant.interrupt_id)
            })
        {
            continue;
        }
        let next = proposed
            .tasks
            .iter()
            .find(|next| next.spec.id == task.spec.id);
        if next.is_none_or(|next| {
            next.approved
                || matches!(
                    next.status,
                    TaskStatus::Ready
                        | TaskStatus::Denied
                        | TaskStatus::InFlight
                        | TaskStatus::Succeeded
                )
        }) {
            return Err(StoreError::InvalidCommit(
                "required_approval_transaction".into(),
            ));
        }
    }
    Ok(())
}

// Preserves the exact callback value/body; it gives no authority or deep-owner bound.
fn checked_ready_authorization_context_metadata_value<F>(value: F) -> F {
    const {
        assert!(std::mem::size_of::<F>() <= 128);
    }
    value
}
