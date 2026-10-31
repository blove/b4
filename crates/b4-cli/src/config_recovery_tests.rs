use crate::config::{self, Command, LauncherError};
use std::ffi::OsString;

fn parse_with_recorded_environment(args: &[&str]) -> (Result<Command, LauncherError>, Vec<String>) {
    let mut reads = Vec::new();
    let result = config::parse(args.iter().map(|arg| OsString::from(*arg)), |key| {
        reads.push(key.to_owned());
        (key == "B4RUN_TOKEN").then(|| OsString::from("private-parser-test-token"))
    });
    (result, reads)
}

fn expected_reads() -> Vec<String> {
    [
        "B4RUN_TOKEN",
        "B4RUN_PORT",
        "B4RUN_DATA_DIR",
        "B4RUN_SHUTDOWN_WAIT_MS",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[test]
fn explicit_recovery_selects_distinct_serve_command_in_both_option_orders() {
    for args in [
        ["serve", "--demo", "--recover"],
        ["serve", "--recover", "--demo"],
    ] {
        let (parsed, reads) = parse_with_recorded_environment(&args);
        let Command::ServeRecover(config) =
            parsed.expect("explicit recovery grammar must be accepted")
        else {
            panic!("explicit recovery must have its own private command selection");
        };
        assert_eq!(
            reads,
            expected_reads(),
            "accepted options preserve configuration read order"
        );
        assert_eq!(config.port(), 3001);
        assert_eq!(config.data_dir(), std::path::Path::new(".b4run-data"));
        assert_eq!(
            config.shutdown_wait(),
            std::time::Duration::from_millis(30_000)
        );
    }
}

#[test]
fn recovery_config_check_accepts_both_orders_without_a_serve_command() {
    for args in [
        ["config-check", "--demo", "--recover"],
        ["config-check", "--recover", "--demo"],
    ] {
        let (parsed, reads) = parse_with_recorded_environment(&args);
        let Command::ConfigCheck(config) =
            parsed.expect("recovery config-check grammar must be accepted")
        else {
            panic!("configuration validation must retain its existing no-serving selection");
        };
        assert_eq!(reads, expected_reads());
        assert_eq!(config.port(), 3001);
        assert_eq!(
            config.shutdown_wait(),
            std::time::Duration::from_millis(30_000)
        );
    }
}

#[test]
fn recovery_grammar_refuses_malformed_options_before_environment_access() {
    for args in [
        vec!["serve", "--recover"],
        vec!["config-check", "--recover"],
        vec!["serve", "--recover", "--recover"],
        vec!["config-check", "--recover", "--recover"],
        vec!["serve", "--demo", "--demo"],
        vec!["serve", "--recover", "foreign-parser-marker"],
        vec!["serve", "--demo", "--recover", "--demo"],
        vec![
            "config-check",
            "--recover",
            "--demo",
            "foreign-parser-marker",
        ],
        vec!["inspect", "--recover"],
        vec!["inspect", "--blocked", "--recover"],
        vec!["--help", "--recover"],
        vec!["--version", "--recover"],
        vec!["--recover", "--demo"],
    ] {
        let (parsed, reads) = parse_with_recorded_environment(&args);
        assert!(
            matches!(parsed, Err(LauncherError::InvalidArguments)),
            "invalid recovery syntax must refuse"
        );
        assert!(
            reads.is_empty(),
            "grammar refusal must precede every environment read"
        );
    }
    let mut items = ["serve", "--demo", "--recover", "foreign-parser-marker"].into_iter();
    let bounded = std::iter::from_fn(|| {
        Some(OsString::from(items.next().expect(
            "fourth argument must reject without asking for a fifth",
        )))
    });
    let rejected = config::parse(bounded, |_| {
        panic!("malformed argument list cannot read environment")
    });
    assert!(matches!(rejected, Err(LauncherError::InvalidArguments)));
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        for args in [
            vec![
                OsString::from("serve"),
                OsString::from("--demo"),
                OsString::from_vec(vec![0xff]),
            ],
            vec![
                OsString::from("serve"),
                OsString::from_vec(vec![0xff]),
                OsString::from("--recover"),
            ],
            vec![
                OsString::from_vec(vec![0xff]),
                OsString::from("--demo"),
                OsString::from("--recover"),
            ],
        ] {
            let parsed = config::parse(args, |_| panic!("non-UTF8 syntax cannot read environment"));
            assert!(matches!(parsed, Err(LauncherError::InvalidArguments)));
        }
    }
}

#[test]
fn recovery_selection_preserves_defaults_and_redacts_private_configuration() {
    let (parsed, reads) = parse_with_recorded_environment(&["serve", "--demo"]);
    let Command::Serve(config) = parsed.expect("ordinary serve remains accepted") else {
        panic!("default serve selection changed");
    };
    assert_eq!(reads, expected_reads());
    let selection = Command::ServeRecover(config);
    assert_eq!(format!("{selection:?}"), "ServeRecover { redacted }");
    for args in [["--help"], ["--version"]] {
        let (parsed, reads) = parse_with_recorded_environment(&args);
        assert!(matches!(parsed, Ok(Command::Help | Command::Version)));
        assert!(reads.is_empty());
    }
    let (parsed, _) = parse_with_recorded_environment(&["config-check", "--demo"]);
    assert!(matches!(parsed, Ok(Command::ConfigCheck(_))));
    assert_eq!(
        format!("{:?}", LauncherError::InvalidArguments),
        "b4run_invalid_arguments"
    );
}
