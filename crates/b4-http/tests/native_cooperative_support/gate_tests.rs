use super::*;

#[test]
fn fixture_expired_entry_after_early_release_is_not_positive() {
    let gate = Gate::with_lifetime(Duration::from_millis(10));
    gate.release();
    std::thread::sleep(Duration::from_millis(20));
    assert!(
        !gate.hold(),
        "late entry must retain expiry despite early release"
    );
    assert!(gate.facts().expired);
}

#[test]
fn fixture_expired_return_after_explicit_wake_is_not_positive() {
    let gate = Gate::with_lifetime(Duration::from_millis(50));
    let worker = {
        let held = gate.clone();
        std::thread::spawn(move || held.hold())
    };
    let entered_deadline = Instant::now() + Duration::from_secs(1);
    while !gate.facts().entered {
        assert!(Instant::now() < entered_deadline, "fixture worker entered");
        std::thread::yield_now();
    }
    {
        let mut facts = gate.facts.lock().unwrap();
        facts.explicit = true;
        gate.changed.notify_all();
        // Force the actual awakened worker to reacquire only after its lifetime.
        std::thread::sleep(
            gate.deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(10),
        );
    }
    assert!(
        !worker.join().unwrap(),
        "late actual return must retain expiry"
    );
    assert!(gate.facts().expired);
}
