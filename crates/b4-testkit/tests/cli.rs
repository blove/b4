use serde_json::{json, Value};
use std::{fs, process::Command};

fn run(arguments: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_b4-conformance"))
        .args(arguments)
        .output()
        .unwrap()
}

#[test]
fn foundation_cli_runs_bundled_scenarios_and_emits_scoped_receipt() {
    let output = run(&["--profile", "foundation"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["receipt_version"], 1);
    assert_eq!(receipt["profile"], "foundation");
    assert_eq!(receipt["passed"], true);
    assert_eq!(receipt["evidence_class"], "native_requirement");
    assert_eq!(receipt["scenarios"].as_array().unwrap().len(), 2);
    for scenario in receipt["scenarios"].as_array().unwrap() {
        assert_eq!(scenario["passed"], true);
        assert_eq!(scenario["observation"]["snapshot_round_trips"], 1);
    }
    let limitations = receipt["limitations"].as_array().unwrap();
    for expected in [
        "SQLite durability",
        "HTTP runtime",
        "production authentication",
        "TypeScript comparison",
    ] {
        assert!(
            limitations
                .iter()
                .any(|limit| limit.as_str().unwrap().contains(expected)),
            "missing limitation: {expected}"
        );
    }
}

#[test]
fn unsupported_full_profile_returns_failure_receipt_and_nonzero_exit() {
    let output = run(&["--profile", "full"]);
    assert_eq!(output.status.code(), Some(2));
    let receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["passed"], false);
    assert_eq!(receipt["profile"], "full");
    assert!(receipt["error"]
        .as_str()
        .unwrap()
        .contains("unsupported profile"));
    assert!(receipt["scenarios"].as_array().unwrap().is_empty());
}

#[test]
fn incorrect_custom_scenario_expectation_returns_failure_receipt_and_nonzero_exit() {
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/scenarios/parallel_approval_snapshot_round_trip.json"
    ))
    .unwrap();
    fixture["expect"]["values"] = json!({"items": ["B", "A"]});
    let path = std::env::temp_dir().join(format!(
        "b4-conformance-wrong-expectation-{}.json",
        std::process::id()
    ));
    fs::write(&path, fixture.to_string()).unwrap();
    let output = run(&[
        "--profile",
        "foundation",
        "--scenario",
        path.to_str().unwrap(),
    ]);
    fs::remove_file(path).unwrap();
    assert_eq!(output.status.code(), Some(1));
    let receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["passed"], false);
    assert_eq!(
        receipt["scenarios"][0]["name"],
        "parallel_approval_snapshot_round_trip"
    );
    assert_eq!(receipt["scenarios"][0]["passed"], false);
    assert!(receipt["scenarios"][0]["error"]
        .as_str()
        .unwrap()
        .contains("expectation mismatch: values"));
}

#[test]
fn unknown_cli_option_returns_failure_receipt() {
    let output = run(&["--live-provider"]);
    assert_eq!(output.status.code(), Some(2));
    let receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["passed"], false);
    assert!(receipt["error"]
        .as_str()
        .unwrap()
        .contains("unknown argument"));
}

#[test]
fn unreadable_custom_fixture_returns_failure_receipt() {
    let path =
        std::env::temp_dir().join(format!("b4-conformance-absent-{}.json", std::process::id()));
    let output = run(&["--scenario", path.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(1));
    let receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["passed"], false);
    assert!(receipt["scenarios"][0]["error"]
        .as_str()
        .unwrap()
        .contains("cannot read scenario"));
}
