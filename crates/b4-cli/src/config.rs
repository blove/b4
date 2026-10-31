use std::{
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LauncherError {
    InvalidArguments,
    InvalidConfiguration,
    InvalidStorage,
    InvalidApplication,
    #[cfg(not(unix))]
    UnsupportedPlatform,
    StorageOwned,
    StorageStartup,
    Signal,
    Executor,
    Listener,
    Host,
    Server,
    HostFault,
    Preparation,
    Inspection(b4_sqlite::InspectionError),
    InspectionOutput,
}

impl LauncherError {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::InvalidArguments => "b4run_invalid_arguments",
            Self::InvalidConfiguration => "b4run_invalid_configuration",
            Self::InvalidStorage => "b4run_invalid_storage",
            Self::InvalidApplication => "b4run_invalid_application",
            #[cfg(not(unix))]
            Self::UnsupportedPlatform => "b4run_unsupported_platform",
            Self::StorageOwned => "b4run_storage_owned",
            Self::StorageStartup => "b4run_storage_failed",
            Self::Signal => "b4run_signal_failed",
            Self::Executor => "b4run_executor_failed",
            Self::Listener => "b4run_listener_failed",
            Self::Host => "b4run_host_failed",
            Self::Server => "b4run_server_failed",
            Self::HostFault => "b4run_host_fault",
            Self::Preparation => "b4run_preparation_failed",
            Self::Inspection(error) => error.code(),
            Self::InspectionOutput => "b4run_inspection_output_failed",
        }
    }

    pub(crate) fn exit_code(self) -> u8 {
        match self {
            Self::Inspection(error) => error.exit_code(),
            Self::InvalidArguments | Self::InvalidConfiguration | Self::InvalidStorage => 2,
            #[cfg(not(unix))]
            Self::UnsupportedPlatform => 2,
            _ => 1,
        }
    }
}

impl fmt::Debug for LauncherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl fmt::Display for LauncherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for LauncherError {}

pub(crate) enum Command {
    Version,
    Help,
    ConfigCheck(Config),
    Serve(Config),
    ServeRecover(Config),
    Inspect(InspectConfig),
}
impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Version => "Version",
            Self::Help => "Help",
            Self::ConfigCheck(_) => "ConfigCheck { redacted }",
            Self::Serve(_) => "Serve { redacted }",
            Self::ServeRecover(_) => "ServeRecover { redacted }",
            Self::Inspect(_) => "Inspect { redacted }",
        })
    }
}

pub(crate) struct InspectConfig {
    data_dir: PathBuf,
    blocked: bool,
}

impl InspectConfig {
    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub(crate) fn blocked(&self) -> bool {
        self.blocked
    }
}

impl fmt::Debug for InspectConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InspectConfig { redacted }")
    }
}

pub(crate) struct Config {
    token: String,
    port: u16,
    data_dir: PathBuf,
    shutdown_wait: Duration,
}
impl Config {
    pub(crate) fn token(&self) -> &str {
        &self.token
    }
    pub(crate) fn port(&self) -> u16 {
        self.port
    }
    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }
    pub(crate) fn shutdown_wait(&self) -> Duration {
        self.shutdown_wait
    }
}
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Config { redacted }")
    }
}

pub(crate) fn parse(
    args: impl IntoIterator<Item = OsString>,
    mut env: impl FnMut(&str) -> Option<OsString>,
) -> Result<Command, LauncherError> {
    let mut args = args.into_iter();
    let first = args.next();
    let second = args.next();
    let third = args.next();
    if third.is_some() && args.next().is_some() {
        return Err(LauncherError::InvalidArguments);
    }
    let first = first.as_ref().and_then(|arg| arg.to_str());
    let second = second.as_ref().map(|arg| arg.to_str());
    let third = third.as_ref().map(|arg| arg.to_str());
    let (serve, recover) = match (first, second, third) {
        (Some("--version"), None, None) => return Ok(Command::Version),
        (Some("--help"), None, None) => return Ok(Command::Help),
        (Some("inspect"), None, None) => return inspection_config(&mut env, false),
        (Some("inspect"), Some(Some("--blocked")), None) => {
            return inspection_config(&mut env, true);
        }
        (Some("serve"), Some(Some("--demo")), None) => (true, false),
        (Some("config-check"), Some(Some("--demo")), None) => (false, false),
        (Some("serve"), Some(Some("--demo")), Some(Some("--recover")))
        | (Some("serve"), Some(Some("--recover")), Some(Some("--demo"))) => (true, true),
        (Some("config-check"), Some(Some("--demo")), Some(Some("--recover")))
        | (Some("config-check"), Some(Some("--recover")), Some(Some("--demo"))) => (false, false),
        _ => return Err(LauncherError::InvalidArguments),
    };
    let invalid = LauncherError::InvalidConfiguration;
    let token = env("B4RUN_TOKEN")
        .ok_or(invalid)?
        .into_string()
        .map_err(|_| invalid)?;
    if token.is_empty()
        || token.len() > 4096
        || !token.bytes().all(|byte| (b'!'..=b'~').contains(&byte))
    {
        return Err(invalid);
    }
    let port = decimal(&value(&mut env, "B4RUN_PORT", "3001")?)?;
    let port = u16::try_from(port).map_err(|_| invalid)?;
    let data_dir = value(&mut env, "B4RUN_DATA_DIR", ".b4run-data")?;
    if data_dir.is_empty() || data_dir.len() > 4096 || data_dir.contains('\0') {
        return Err(invalid);
    }
    let wait = decimal(&value(&mut env, "B4RUN_SHUTDOWN_WAIT_MS", "30000")?)?;
    if !(1..=300_000).contains(&wait) {
        return Err(invalid);
    }
    let config = Config {
        token,
        port,
        data_dir: PathBuf::from(data_dir),
        shutdown_wait: Duration::from_millis(u64::from(wait)),
    };
    Ok(if recover {
        Command::ServeRecover(config)
    } else if serve {
        Command::Serve(config)
    } else {
        Command::ConfigCheck(config)
    })
}

fn inspection_config(
    env: &mut impl FnMut(&str) -> Option<OsString>,
    blocked: bool,
) -> Result<Command, LauncherError> {
    let data_dir = env("B4RUN_DATA_DIR").unwrap_or_else(|| ".b4run-data".into());
    let bytes = data_dir.as_encoded_bytes();
    if bytes.is_empty() || bytes.len() > 4096 || bytes.contains(&0) {
        return Err(LauncherError::Inspection(
            b4_sqlite::InspectionError::InvalidSource,
        ));
    }
    Ok(Command::Inspect(InspectConfig {
        data_dir: PathBuf::from(data_dir),
        blocked,
    }))
}

fn value(
    env: &mut impl FnMut(&str) -> Option<OsString>,
    key: &str,
    default: &str,
) -> Result<String, LauncherError> {
    env(key).map_or_else(
        || Ok(default.into()),
        |value| {
            value
                .into_string()
                .map_err(|_| LauncherError::InvalidConfiguration)
        },
    )
}

fn decimal(value: &str) -> Result<u32, LauncherError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(LauncherError::InvalidConfiguration);
    }
    value
        .parse()
        .map_err(|_| LauncherError::InvalidConfiguration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn command(args: &[&str], values: &[(&str, OsString)]) -> Result<Command, LauncherError> {
        let values: BTreeMap<_, _> = values.iter().cloned().collect();
        parse(args.iter().map(OsString::from), |key| {
            values.get(key).cloned()
        })
    }

    fn config(values: &[(&str, OsString)]) -> Result<Config, LauncherError> {
        let mut values = values.to_vec();
        if !values.iter().any(|(key, _)| *key == "B4RUN_TOKEN") {
            values.push(("B4RUN_TOKEN", "private-token".into()));
        }
        match command(&["serve", "--demo"], &values)? {
            Command::Serve(config) => Ok(config),
            _ => panic!("wrong command"),
        }
    }

    #[test]
    fn informational_commands_do_not_read_environment() {
        assert!(matches!(
            parse(["--version".into()], |_| panic!("environment read")),
            Ok(Command::Version)
        ));
        assert!(matches!(
            parse(["--help".into()], |_| panic!("environment read")),
            Ok(Command::Help)
        ));
    }

    #[test]
    fn demo_selection_and_defaults_are_explicit() {
        let config = config(&[]).expect("valid configuration");
        assert_eq!(config.token(), "private-token");
        assert_eq!(config.port(), 3001);
        assert_eq!(config.data_dir(), Path::new(".b4run-data"));
        assert_eq!(config.shutdown_wait(), Duration::from_millis(30_000));
        assert!(matches!(
            command(
                &["config-check", "--demo"],
                &[("B4RUN_TOKEN", "value".into())]
            ),
            Ok(Command::ConfigCheck(_))
        ));
    }

    #[test]
    fn unknown_duplicate_missing_options_are_static_usage_errors() {
        for args in [
            vec![],
            vec!["serve"],
            vec!["config-check"],
            vec!["help"],
            vec!["--demo"],
            vec!["serve", "--demo", "--demo"],
            vec!["serve", "--demo=secret"],
            vec!["serve", "--demo", "private-argument"],
            vec!["--version", "--demo"],
            vec!["config-check", "--unknown"],
        ] {
            let result = parse(args.into_iter().map(OsString::from), |_| {
                panic!("environment read")
            });
            assert!(matches!(result, Err(LauncherError::InvalidArguments)));
            let error = result.unwrap_err();
            assert_eq!(error.to_string(), "b4run_invalid_arguments");
            assert_eq!(format!("{error:?}"), "b4run_invalid_arguments");
            assert_eq!(error.exit_code(), 2);
        }
    }

    #[test]
    fn token_requires_visible_ascii_and_bounded_length() {
        for token in [
            "",
            " ",
            "private token",
            "private\ttoken",
            "private\ntoken",
            "private\0token",
            "é",
            "\u{7f}",
        ] {
            assert!(matches!(
                config(&[("B4RUN_TOKEN", token.into())]),
                Err(LauncherError::InvalidConfiguration)
            ));
        }
        assert!(matches!(
            command(&["serve", "--demo"], &[]),
            Err(LauncherError::InvalidConfiguration)
        ));
        assert!(config(&[("B4RUN_TOKEN", "!".into())]).is_ok());
        assert!(config(&[("B4RUN_TOKEN", "A".repeat(4096).into())]).is_ok());
        assert!(matches!(
            config(&[("B4RUN_TOKEN", "A".repeat(4097).into())]),
            Err(LauncherError::InvalidConfiguration)
        ));
        let visible: String = (b'!'..=b'~').map(char::from).collect();
        assert_eq!(
            config(&[("B4RUN_TOKEN", visible.clone().into())])
                .unwrap()
                .token(),
            visible
        );
    }

    #[test]
    fn port_is_only_decimal_u16_and_accepts_zero() {
        for (port, expected) in [("0", 0), ("00001", 1), ("65535", 65535)] {
            assert_eq!(
                config(&[("B4RUN_PORT", port.into())]).unwrap().port(),
                expected
            );
        }
        for port in [
            "",
            "+1",
            "-1",
            "65536",
            " 1",
            "1 ",
            "1.0",
            "1_000",
            "１２",
            "999999999999999999999",
        ] {
            assert!(matches!(
                config(&[("B4RUN_PORT", port.into())]),
                Err(LauncherError::InvalidConfiguration)
            ));
        }
    }

    #[test]
    fn shutdown_wait_is_only_decimal_in_reviewed_bounds() {
        for (wait, expected) in [("1", 1), ("00001", 1), ("300000", 300000)] {
            assert_eq!(
                config(&[("B4RUN_SHUTDOWN_WAIT_MS", wait.into())])
                    .unwrap()
                    .shutdown_wait(),
                Duration::from_millis(expected)
            );
        }
        for wait in [
            "",
            "0",
            "300001",
            "+1",
            "-1",
            " 1",
            "1 ",
            "1.0",
            "1_000",
            "999999999999999999999",
        ] {
            assert!(matches!(
                config(&[("B4RUN_SHUTDOWN_WAIT_MS", wait.into())]),
                Err(LauncherError::InvalidConfiguration)
            ));
        }
    }

    #[test]
    fn data_path_preserves_valid_utf8_and_enforces_byte_bounds() {
        for path in [
            "relative/private",
            "/private/tmp/private",
            "with space",
            "é",
        ] {
            assert_eq!(
                config(&[("B4RUN_DATA_DIR", path.into())])
                    .unwrap()
                    .data_dir(),
                Path::new(path)
            );
        }
        assert!(config(&[("B4RUN_DATA_DIR", "x".repeat(4096).into())]).is_ok());
        for path in [
            String::new(),
            "private\0path".into(),
            "x".repeat(4097),
            "é".repeat(2049),
        ] {
            assert!(matches!(
                config(&[("B4RUN_DATA_DIR", path.into())]),
                Err(LauncherError::InvalidConfiguration)
            ));
        }
    }

    #[test]
    fn diagnostics_do_not_retain_configuration_contents() {
        let config = config(&[
            ("B4RUN_TOKEN", "secret-token-marker".into()),
            ("B4RUN_DATA_DIR", "secret-path-marker".into()),
            ("B4RUN_PORT", "19431".into()),
        ])
        .unwrap();
        assert_eq!(format!("{config:?}"), "Config { redacted }");
        assert_eq!(
            format!("{:?}", Command::Serve(config)),
            "Serve { redacted }"
        );
        let error = config_error("secret-token-marker\n");
        assert_eq!(error.to_string(), "b4run_invalid_configuration");
        assert_eq!(format!("{error:?}"), "b4run_invalid_configuration");
        assert_eq!(error.exit_code(), 2);
    }

    fn config_error(token: &str) -> LauncherError {
        config(&[("B4RUN_TOKEN", token.into())]).unwrap_err()
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_arguments_and_recognized_environment_are_rejected() {
        use std::os::unix::ffi::OsStringExt;
        let invalid = || OsString::from_vec(vec![0xff]);
        assert!(matches!(
            parse([invalid()], |_| panic!("environment read")),
            Err(LauncherError::InvalidArguments)
        ));
        for key in [
            "B4RUN_TOKEN",
            "B4RUN_PORT",
            "B4RUN_DATA_DIR",
            "B4RUN_SHUTDOWN_WAIT_MS",
        ] {
            assert!(matches!(
                config(&[(key, invalid())]),
                Err(LauncherError::InvalidConfiguration)
            ));
        }
    }
}
