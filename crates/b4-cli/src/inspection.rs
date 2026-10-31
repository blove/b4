use crate::config::{InspectConfig, LauncherError};
use std::io::Write;

pub(super) fn run(config: InspectConfig) -> Result<(), LauncherError> {
    let report = b4_sqlite::inspect_stopped(
        config.data_dir(),
        b4_sqlite::InspectionOptions {
            blocked: config.blocked(),
        },
    )
    .map_err(LauncherError::Inspection)?;
    write_report(&mut std::io::stdout().lock(), report.as_ref())
}

pub(super) fn write_report(output: &mut impl Write, report: &[u8]) -> Result<(), LauncherError> {
    output
        .write_all(report)
        .and_then(|()| output.write_all(b"\n"))
        .and_then(|()| output.flush())
        .map_err(|_| LauncherError::InspectionOutput)
}
