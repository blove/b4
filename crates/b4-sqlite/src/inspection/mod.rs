//! Bounded, redacted inspection of an existing stopped store.

use std::path::Path;

#[cfg(any(target_os = "macos", target_os = "linux"))]
mod report;
#[cfg(any(target_os = "macos", target_os = "linux"))]
mod source;
#[cfg(any(target_os = "macos", target_os = "linux"))]
mod validation;

/// Selects the optional report-local unfinished execution rows.
#[derive(Clone, Copy, Debug, Default)]
pub struct InspectionOptions {
    pub blocked: bool,
}

/// Complete redacted JSON, without a trailing newline or source resources.
pub struct InspectionReport(Vec<u8>);

impl AsRef<[u8]> for InspectionReport {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Static inspection refusals; no source data is retained or formatted.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum InspectionError {
    InvalidSource,
    AlreadyOwned,
    SidecarsPresent,
    UnsupportedSchema,
    UnsupportedSnapshot,
    LimitExceeded,
    CorruptSource,
    Unavailable,
    CloseUncertain,
    UnsupportedPlatform,
}

impl InspectionError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidSource => "b4run_inspection_invalid_source",
            Self::AlreadyOwned => "b4run_storage_owned",
            Self::SidecarsPresent => "b4run_inspection_sidecars_present",
            Self::UnsupportedSchema => "b4run_inspection_unsupported_schema",
            Self::UnsupportedSnapshot => "b4run_inspection_unsupported_snapshot",
            Self::LimitExceeded => "b4run_inspection_limit_exceeded",
            Self::CorruptSource => "b4run_inspection_corrupt_source",
            Self::Unavailable => "b4run_inspection_unavailable",
            Self::CloseUncertain => "b4run_inspection_close_uncertain",
            Self::UnsupportedPlatform => "b4run_unsupported_platform",
        }
    }

    pub const fn exit_code(self) -> u8 {
        match self {
            Self::AlreadyOwned | Self::CorruptSource | Self::Unavailable | Self::CloseUncertain => {
                1
            }
            _ => 2,
        }
    }
}

impl std::fmt::Display for InspectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

impl std::fmt::Debug for InspectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for InspectionError {}

/// Inspect without starting, repairing, migrating, or changing an existing store.
pub fn inspect_stopped(
    data_dir: impl AsRef<Path>,
    options: InspectionOptions,
) -> Result<InspectionReport, InspectionError> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        let budget = validation::Budget::new();
        let mut reader = source::Reader::open(data_dir.as_ref())?;
        let result = (|| {
            validation::configure(reader.connection(), &budget, true)?;
            let projection = validation::read(&mut reader, &budget, options.blocked)?;
            budget.check()?;
            let report = projection.encode()?;
            budget.check()?;
            Ok(report)
        })();
        // Retirement uncertainty overrides every result, including refusals.
        reader.finish()?;
        result
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (data_dir, options);
        Err(InspectionError::UnsupportedPlatform)
    }
}
