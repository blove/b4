use crate::config::{parse, Command, LauncherError};
use std::{ffi::OsString, path::Path};

fn inspect_command(args: &[&str], data_dir: Option<OsString>) -> Result<Command, LauncherError> {
    let mut reads = Vec::new();
    let command = parse(args.iter().map(OsString::from), |key| {
        reads.push(key.to_owned());
        assert_eq!(key, "B4RUN_DATA_DIR");
        data_dir.clone()
    })?;
    assert_eq!(reads, ["B4RUN_DATA_DIR"]);
    Ok(command)
}

#[test]
fn inspection_parser_accepts_exact_commands_and_only_reads_data_dir() {
    for (args, blocked) in [
        (vec!["inspect"], false),
        (vec!["inspect", "--blocked"], true),
    ] {
        let command = inspect_command(&args, None).expect("inspection should require no token");
        assert_eq!(format!("{command:?}"), "Inspect { redacted }");
        let Command::Inspect(config) = command else {
            panic!("wrong command");
        };
        assert_eq!(config.data_dir(), Path::new(".b4run-data"));
        assert_eq!(config.blocked(), blocked);
        assert_eq!(format!("{config:?}"), "InspectConfig { redacted }");
    }
}

#[test]
fn inspection_parser_rejects_options_and_positions_before_environment() {
    for args in [
        vec!["inspect", "--blocked", "--blocked"],
        vec!["inspect", "--unknown"],
        vec!["inspect", "--demo"],
        vec!["inspect", "--live"],
        vec!["inspect", "--force"],
        vec!["inspect", "--blocked=true"],
        vec!["inspect", "--blocked=false"],
        vec!["inspect", "--data-dir=private-path"],
        vec!["inspect", "--version"],
        vec!["inspect", "--help"],
        vec!["inspect", "private-path"],
        vec!["inspect", "--blocked", "private-path"],
        vec!["--blocked", "inspect"],
        vec!["inspect", ""],
    ] {
        let error = parse(args.into_iter().map(OsString::from), |_| {
            panic!("invalid arguments must not read environment")
        })
        .unwrap_err();
        assert_eq!(error.code(), "b4run_invalid_arguments");
        assert_eq!(error.exit_code(), 2);
        assert_eq!(error.to_string(), "b4run_invalid_arguments");
        assert_eq!(format!("{error:?}"), "b4run_invalid_arguments");
    }
}

#[test]
fn inspection_parser_accepts_bounded_paths_and_redacts_them() {
    for path in [
        OsString::from("private-path-marker/with space?mode=rw#fragment"),
        OsString::from("é"),
        OsString::from("x".repeat(4096)),
    ] {
        let command = inspect_command(&["inspect"], Some(path.clone())).unwrap();
        assert_eq!(format!("{command:?}"), "Inspect { redacted }");
        let Command::Inspect(config) = command else {
            panic!("wrong command");
        };
        assert_eq!(config.data_dir(), Path::new(&path));
    }
}

#[test]
fn inspection_parser_refuses_empty_nul_and_overlong_paths_statically() {
    for path in [
        OsString::new(),
        OsString::from("private\0path-marker"),
        OsString::from("x".repeat(4097)),
        OsString::from("é".repeat(2049)),
    ] {
        let error = inspect_command(&["inspect", "--blocked"], Some(path)).unwrap_err();
        assert_eq!(error.code(), "b4run_inspection_invalid_source");
        assert_eq!(error.exit_code(), 2);
        assert_eq!(error.to_string(), "b4run_inspection_invalid_source");
        assert_eq!(format!("{error:?}"), "b4run_inspection_invalid_source");
    }
}

#[cfg(unix)]
#[test]
fn inspection_parser_preserves_raw_unix_paths_and_refuses_raw_arguments() {
    use std::os::unix::ffi::OsStringExt;

    let raw = OsString::from_vec(vec![b'p', b'/', 0xff, b'?', b'#']);
    let command = inspect_command(&["inspect"], Some(raw.clone()))
        .expect("raw Unix data-directory bytes are supported");
    assert_eq!(format!("{command:?}"), "Inspect { redacted }");
    let Command::Inspect(config) = command else {
        panic!("wrong command");
    };
    assert_eq!(config.data_dir().as_os_str(), raw);
    for args in [
        vec![OsString::from_vec(vec![0xff])],
        vec![OsString::from("inspect"), OsString::from_vec(vec![0xff])],
    ] {
        let error = parse(args, |_| {
            panic!("invalid arguments must not read environment")
        })
        .unwrap_err();
        assert_eq!(error.code(), "b4run_invalid_arguments");
        assert_eq!(error.exit_code(), 2);
    }
}

#[test]
fn inspection_errors_preserve_all_static_codes_exits_and_no_error_sources() {
    use b4_sqlite::InspectionError;
    use std::error::Error;

    for (inspection_error, code, exit) in [
        (
            InspectionError::InvalidSource,
            "b4run_inspection_invalid_source",
            2,
        ),
        (InspectionError::AlreadyOwned, "b4run_storage_owned", 1),
        (
            InspectionError::SidecarsPresent,
            "b4run_inspection_sidecars_present",
            2,
        ),
        (
            InspectionError::UnsupportedSchema,
            "b4run_inspection_unsupported_schema",
            2,
        ),
        (
            InspectionError::UnsupportedSnapshot,
            "b4run_inspection_unsupported_snapshot",
            2,
        ),
        (
            InspectionError::LimitExceeded,
            "b4run_inspection_limit_exceeded",
            2,
        ),
        (
            InspectionError::CorruptSource,
            "b4run_inspection_corrupt_source",
            1,
        ),
        (
            InspectionError::Unavailable,
            "b4run_inspection_unavailable",
            1,
        ),
        (
            InspectionError::CloseUncertain,
            "b4run_inspection_close_uncertain",
            1,
        ),
        (
            InspectionError::UnsupportedPlatform,
            "b4run_unsupported_platform",
            2,
        ),
    ] {
        let error = LauncherError::Inspection(inspection_error);
        assert_eq!(error.code(), code);
        assert_eq!(error.exit_code(), exit);
        assert_eq!(error.to_string(), code);
        assert_eq!(format!("{error:?}"), code);
        assert!(error.source().is_none());
    }
}

#[test]
fn inspection_output_writes_complete_report_with_exactly_one_newline() {
    let mut output = Vec::new();
    crate::inspection::write_report(&mut output, br#"{"redacted":true}"#).unwrap();
    assert_eq!(output, b"{\"redacted\":true}\n");
}

struct FailingOutput {
    accepted: Vec<u8>,
    remaining: usize,
    fail_flush: bool,
}

impl std::io::Write for FailingOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "private-output-error-marker",
            ));
        }
        let count = self.remaining.min(bytes.len());
        self.accepted.extend_from_slice(&bytes[..count]);
        self.remaining -= count;
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.fail_flush {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "private-flush-error-marker",
            ));
        }
        Ok(())
    }
}

#[test]
fn inspection_output_broken_writer_returns_static_error_and_permits_safe_partial_bytes() {
    let report = br#"{"redacted":true}"#;
    for remaining in [0, 5, report.len()] {
        let mut output = FailingOutput {
            accepted: Vec::new(),
            remaining,
            fail_flush: false,
        };
        let error = crate::inspection::write_report(&mut output, report).unwrap_err();
        assert_eq!(error.code(), "b4run_inspection_output_failed");
        assert_eq!(error.exit_code(), 1);
        assert_eq!(error.to_string(), "b4run_inspection_output_failed");
        assert_eq!(format!("{error:?}"), "b4run_inspection_output_failed");
        assert_eq!(output.accepted, report[..remaining]);
    }
}

#[test]
fn inspection_output_flush_failure_returns_static_error() {
    let mut output = FailingOutput {
        accepted: Vec::new(),
        remaining: usize::MAX,
        fail_flush: true,
    };
    let error = crate::inspection::write_report(&mut output, b"{}").unwrap_err();
    assert_eq!(output.accepted, b"{}\n");
    assert_eq!(error.code(), "b4run_inspection_output_failed");
    assert_eq!(error.exit_code(), 1);
}
