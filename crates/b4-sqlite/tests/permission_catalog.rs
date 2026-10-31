#[path = "permission_support/mod.rs"]
mod support;
use b4_contract::*;
use b4_runtime::permissions::*;
use std::sync::Arc;
use support::{AllowConstraint, Fixture};

fn reject_after_real_once(
    entries: impl FnOnce(&Fixture) -> Vec<ToolBindingRegistration>,
    message: &str,
) {
    let fixture = Fixture::new(true);
    let bad = PermissionCatalog::new(fixture.application.clone(), entries(&fixture));
    let rejected = bad.is_err();
    drop(bad);
    let good = Arc::new(
        PermissionCatalog::new(
            fixture.application.clone(),
            vec![fixture.registration(Arc::new(AllowConstraint))],
        )
        .unwrap(),
    );
    let request = fixture.once_request();
    let accepted = accept_permission_human(
        fixture.store.clone(),
        fixture.authority.clone(),
        good.clone(),
        &fixture.execution,
        fixture.fence(),
        request,
    )
    .unwrap();
    assert_eq!(accepted.snapshot.status, ExecutionStatus::Running);
    assert!(accepted.snapshot.tasks[0].approved);
    drop(good);
    let persisted = fixture.close();
    assert_eq!(persisted, accepted);
    assert!(rejected, "{message}");
}

#[test]
fn catalog_rejects_missing_current_mapping_after_legal_once() {
    reject_after_real_once(|_| vec![], "catalog must reject a missing current mapping");
}
#[test]
fn catalog_rejects_duplicate_definition_effect_mapping_after_legal_once() {
    reject_after_real_once(
        |f| {
            vec![
                f.registration(Arc::new(AllowConstraint)),
                f.registration(Arc::new(AllowConstraint)),
            ]
        },
        "catalog must reject duplicate Definition/effect mapping",
    );
}
#[test]
fn catalog_rejects_full_definition_difference_after_legal_once() {
    reject_after_real_once(
        |f| {
            let mut entry = f.registration(Arc::new(AllowConstraint));
            entry.definition.max_attempts += 1;
            vec![entry]
        },
        "catalog must require the exact complete current Definition",
    );
}
#[test]
fn catalog_rejects_uninstalled_effect_after_legal_once() {
    reject_after_real_once(
        |f| {
            let mut entry = f.registration(Arc::new(AllowConstraint));
            entry.effect_id = EffectId::new("foreign-effect").unwrap();
            vec![entry]
        },
        "catalog must reject an effect absent from the current native registration",
    );
}
#[test]
fn catalog_requires_nonapproval_current_mapping() {
    let fixture = Fixture::new(false);
    let bad = PermissionCatalog::new(fixture.application.clone(), vec![]);
    let rejected = bad.is_err();
    drop(bad);
    let persisted = fixture.close();
    assert_eq!(persisted.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(!persisted.snapshot.tasks[0].approved);
    assert!(rejected, "catalog must also map current nonapproval tasks");
}
#[test]
fn catalog_rejects_historical_registration_even_when_current_mapping_is_complete() {
    let fixture = Fixture::with_history();
    let current = fixture.registration(Arc::new(AllowConstraint));
    let mut historical = fixture.registration(Arc::new(AllowConstraint));
    historical.definition = fixture.historical.as_ref().unwrap().clone();
    assert!(fixture
        .application
        .definition_exact(&historical.definition)
        .is_some());
    assert_ne!(historical.definition, fixture.definition);
    let bad = PermissionCatalog::new(fixture.application.clone(), vec![current, historical]);
    let rejected = bad.is_err();
    drop(bad);
    let persisted = fixture.close();
    assert_eq!(persisted.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(rejected, "catalog must select current registrations only");
}

#[test]
fn catalog_rejects_sixty_five_legitimate_native_mappings() {
    let fixture = Fixture::with_binding_count(65);
    let entries = fixture.registrations(Arc::new(AllowConstraint));
    assert_eq!(entries.len(), 65);
    assert_eq!(fixture.application.definitions().count(), 2);
    for entry in &entries {
        assert_eq!(
            fixture.application.definition(&entry.definition.route),
            Some(&entry.definition)
        );
        assert!(fixture
            .application
            .output_kind(&entry.definition, &entry.effect_id)
            .is_some());
    }
    let bad = PermissionCatalog::new(fixture.application.clone(), entries);
    let rejected = bad.is_err();
    drop(bad);
    let persisted = fixture.close();
    assert_eq!(persisted.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(
        rejected,
        "catalog must enforce its sixty-four-binding ceiling"
    );
}
#[test]
fn catalog_accepts_sixty_four_legitimate_native_mappings_control() {
    let fixture = Fixture::with_binding_count(64);
    let entries = fixture.registrations(Arc::new(AllowConstraint));
    assert_eq!(entries.len(), 64);
    let catalog = PermissionCatalog::new(fixture.application.clone(), entries);
    let accepted = catalog.is_ok();
    drop(catalog);
    let persisted = fixture.close();
    assert_eq!(persisted.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(
        accepted,
        "the catalog ceiling must allow sixty-four complete mappings"
    );
}
