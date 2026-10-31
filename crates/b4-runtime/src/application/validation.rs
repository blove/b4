use super::*;
use b4_contract::{ExecutionId, TransitionFacts, TurnId};
use serde_json::Map;
use std::collections::BTreeSet;

pub(super) fn label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

pub(super) fn validate(
    identity: &ApplicationIdentity,
    routes: &[RouteRegistration],
) -> Result<(), ApplicationConfigError> {
    if !label(&identity.application) || !label(&identity.revision) {
        return Err(ApplicationConfigError("invalid_application_identity"));
    }
    if !(1..=64).contains(&routes.len()) {
        return Err(ApplicationConfigError("invalid_application_route_limit"));
    }
    let mut seen = BTreeSet::new();
    for route in routes {
        if !seen.insert(&route.definition.route) {
            return Err(ApplicationConfigError("duplicate_application_route"));
        }
        if route.definition.batches.len() > 64 {
            return Err(ApplicationConfigError("invalid_application_program_limit"));
        }
        let tasks: Vec<_> = route.definition.batches.iter().flatten().take(65).collect();
        if !(1..=64).contains(&tasks.len())
            || !(1..=64).contains(&route.definition.max_attempts)
            || route.definition.max_attempts < tasks.len() as u32
            || route.effects.len() > 64
        {
            return Err(ApplicationConfigError("invalid_application_program_limit"));
        }
        b4_kernel::new_execution(
            ExecutionId::new("application-validation").expect("static valid identity"),
            TurnId::new("application-validation-turn").expect("static valid identity"),
            1,
            route.definition.clone(),
            Map::new(),
            TransitionFacts { now_ms: 0 },
        )
        .map_err(|_| ApplicationConfigError("invalid_application_definition"))?;
        let required: BTreeSet<_> = tasks.iter().map(|task| &task.effect_id).collect();
        if required != route.effects.keys().collect() {
            return Err(ApplicationConfigError("application_effect_coverage"));
        }
    }
    Ok(())
}

pub(super) fn validate_history(
    identity: &ApplicationIdentity,
    current: &[RouteRegistration],
    historical: &[RouteRegistration],
) -> Result<(), ApplicationConfigError> {
    if current
        .len()
        .checked_add(historical.len())
        .is_none_or(|count| count > 64)
    {
        return Err(ApplicationConfigError("invalid_application_route_limit"));
    }
    validate(identity, current)?;
    let mut seen: BTreeSet<_> = current
        .iter()
        .map(|registration| {
            (
                &registration.definition.route,
                &registration.definition.revision,
            )
        })
        .collect();
    for registration in historical {
        if !seen.insert((
            &registration.definition.route,
            &registration.definition.revision,
        )) {
            return Err(ApplicationConfigError("duplicate_application_revision"));
        }
        // Bound and validate each complete program before scanning approval
        // tasks, including future batches. No callback or validator is called.
        validate(identity, std::slice::from_ref(registration))?;
        if registration
            .definition
            .batches
            .iter()
            .flatten()
            .any(|task| task.requires_approval)
        {
            return Err(ApplicationConfigError("historical_approval_unsupported"));
        }
    }
    Ok(())
}
