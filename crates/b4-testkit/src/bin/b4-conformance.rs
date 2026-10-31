use b4_testkit::{
    parse_scenario, run_scenario, EvidenceClass, ScenarioObservation, FOUNDATION_SCENARIOS,
};
use serde::Serialize;
use std::fs;

const LIMITATIONS: &[&str] = &[
    "SQLite durability and transactions are outside this foundation profile.",
    "HTTP runtime and SSE transport are outside this foundation profile.",
    "production authentication and grant validation are outside this foundation profile.",
    "TypeScript comparison is outside this foundation profile; native requirements are not reference-runtime observations.",
    "Snapshot restoration here is an in-memory JSON round trip, not disk durability or process restart evidence.",
    "Scripted effects are fixtures; this foundation profile does not qualify live adapters.",
];

#[derive(Serialize)]
struct Receipt {
    receipt_version: u32,
    profile: String,
    evidence_class: EvidenceClass,
    passed: bool,
    scenarios: Vec<ScenarioReceipt>,
    limitations: &'static [&'static str],
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct ScenarioReceipt {
    name: String,
    passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    observation: Option<ScenarioObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn main() {
    let (receipt, exit_code) = run(std::env::args().skip(1));
    println!(
        "{}",
        serde_json::to_string(&receipt).expect("receipt contains serializable data")
    );
    std::process::exit(exit_code);
}

fn run(arguments: impl Iterator<Item = String>) -> (Receipt, i32) {
    let mut arguments = arguments;
    let mut profile = "foundation".to_owned();
    let mut scenario_path = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--profile" => {
                let Some(value) = arguments.next() else {
                    return failure(profile, "--profile requires a value".into());
                };
                profile = value;
            }
            "--scenario" => {
                let Some(value) = arguments.next() else {
                    return failure(profile, "--scenario requires a path".into());
                };
                scenario_path = Some(value);
            }
            _ => return failure(profile, format!("unknown argument: {argument}")),
        }
    }
    if profile != "foundation" {
        return failure(
            profile.clone(),
            format!("unsupported profile: {profile}; supported profile is foundation"),
        );
    }
    let scenarios = if let Some(path) = scenario_path {
        vec![match fs::read_to_string(&path) {
            Ok(json) => execute(&json, &path),
            Err(error) => ScenarioReceipt {
                name: path.clone(),
                passed: false,
                observation: None,
                error: Some(format!("cannot read scenario {path}: {error}")),
            },
        }]
    } else {
        FOUNDATION_SCENARIOS
            .iter()
            .map(|json| execute(json, "invalid bundled scenario"))
            .collect()
    };
    let passed = scenarios.iter().all(|scenario| scenario.passed);
    (
        Receipt {
            receipt_version: 1,
            profile,
            evidence_class: EvidenceClass::NativeRequirement,
            passed,
            scenarios,
            limitations: LIMITATIONS,
            error: None,
        },
        if passed { 0 } else { 1 },
    )
}

fn failure(profile: String, error: String) -> (Receipt, i32) {
    (
        Receipt {
            receipt_version: 1,
            profile,
            evidence_class: EvidenceClass::NativeRequirement,
            passed: false,
            scenarios: vec![],
            limitations: LIMITATIONS,
            error: Some(error),
        },
        2,
    )
}

fn execute(json: &str, fallback_name: &str) -> ScenarioReceipt {
    let scenario = match parse_scenario(json) {
        Ok(scenario) => scenario,
        Err(error) => {
            return ScenarioReceipt {
                name: fallback_name.to_owned(),
                passed: false,
                observation: None,
                error: Some(error),
            }
        }
    };
    match run_scenario(&scenario) {
        Ok(observation) => ScenarioReceipt {
            name: scenario.name,
            passed: true,
            observation: Some(observation),
            error: None,
        },
        Err(error) => ScenarioReceipt {
            name: scenario.name,
            passed: false,
            observation: None,
            error: Some(error),
        },
    }
}
