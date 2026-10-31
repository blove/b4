use super::*;

fn preparing() -> ShutdownHandle {
    ShutdownHandle::new_preparing(Arc::new(AtomicBool::new(false)))
}
fn complete(progress: StartupProgress) -> CompletionCandidate {
    CompletionCandidate {
        progress,
        reason: None,
    }
}

#[test]
fn startup_gate_closes_ordinary_admission_before_begin() {
    let h = preparing();
    assert_eq!(h.status().phase, ShutdownPhase::Serving);
    assert_eq!(
        h.preparation_status().phase,
        StartupPreparationPhase::NotStarted
    );
    let owner = h.register_owner();
    drop(owner);
    assert!(!h.is_serving());
    assert!(h.register_owner().is_none());
}

#[test]
fn startup_begin_installs_one_counted_exact_role() {
    let h = preparing();
    let (cap, owner) = h.begin_startup().unwrap();
    assert_eq!(h.status().active_owners, 1);
    let worker = h.register_startup_owner(&cap);
    assert!(owner.try_admit().is_some());
    drop(worker);
    drop(owner);
    assert_eq!(h.status().active_owners, 0);
    assert_eq!(
        h.preparation_status().phase,
        StartupPreparationPhase::Preparing
    );
    assert!(matches!(
        h.begin_startup(),
        Err(StartupStartError::AlreadyStarted)
    ));
}

#[test]
fn startup_capability_cannot_authorize_another_supervisor() {
    let h = preparing();
    let other = preparing();
    let (cap, owner) = h.begin_startup().unwrap();
    let unauthorized = other.register_startup_owner(&cap);
    let admitted = unauthorized.is_some();
    drop(unauthorized);
    drop(owner);
    assert!(!admitted);
    assert_eq!(other.status().active_owners, 0);
}

#[test]
fn startup_completion_opens_only_after_exact_finished_pass() {
    let h = preparing();
    let (cap, owner) = h.begin_startup().unwrap();
    let progress = StartupProgress {
        pages: 1,
        scanned_rows: 3,
        ..StartupProgress::default()
    };
    h.update_startup_progress(&cap, progress);
    let finished = h.finish_startup(&cap, complete(progress));
    drop(owner);
    assert!(finished);
    assert!(h.is_serving());
    assert_eq!(
        h.preparation_status().phase,
        StartupPreparationPhase::Complete
    );
    assert_eq!(h.preparation_status().progress, progress);
}

#[test]
fn startup_lease_and_activation_cannot_win_shutdown_cut() {
    let h = preparing();
    let (cap, owner) = h.begin_startup().unwrap();
    let won = owner.try_admit().unwrap();
    h.begin_shutdown();
    assert!(owner.try_admit().is_none());
    assert!(h.register_startup_owner(&cap).is_none());
    assert!(!h.finish_startup(&cap, complete(StartupProgress::default())));
    assert!(!h.is_serving());
    drop(owner);
    assert_eq!(h.status().active_owners, 1);
    drop(won);
    assert_eq!(h.status().active_owners, 0);
}
