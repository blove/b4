//! Standalone native process policy, distinct from the embeddable HTTP host.
mod config;
#[cfg(test)]
mod config_recovery_tests;
mod demo;
mod inspection;
#[cfg(test)]
mod inspection_tests;
mod process;
mod storage;
/// Process entrypoint with only fixed public diagnostics.
pub fn main_entry() -> u8 {
    install_panic_hook();
    let result =
        config::parse(std::env::args_os().skip(1), |key| std::env::var_os(key)).and_then(run);
    match result {
        Ok(()) => 0,
        Err(error) => {
            emit_error(error.code());
            error.exit_code()
        }
    }
}

fn run(command: config::Command) -> Result<(), config::LauncherError> {
    match command {
        config::Command::Version => emit_status("b4run 0.1.0"),
        config::Command::Help => emit_status(
            "b4run 0.1.0\nUsage: b4run --version | --help | config-check --demo [--recover] | serve --demo [--recover] | inspect [--blocked]\nEnvironment (serve/config-check): B4RUN_TOKEN, B4RUN_PORT, B4RUN_DATA_DIR, B4RUN_SHUTDOWN_WAIT_MS\nEnvironment (inspect): B4RUN_DATA_DIR (default: .b4run-data)",
        ),
        config::Command::ConfigCheck(config) => {
            let _application = demo::assemble()?;
            storage::validate(config.data_dir())?;
            emit_status("b4run_config_valid");
        }
        config::Command::Serve(config) => {
            let application = demo::assemble()?;
            return process::serve(config, application);
        }
        config::Command::ServeRecover(config) => {
            let application = demo::assemble()?;
            return process::serve_recover(config, application);
        }
        config::Command::Inspect(config) => return inspection::run(config),
    }
    Ok(())
}

fn install_panic_hook() {
    std::panic::set_hook(Box::new(|_| emit_error("b4run_panic")));
}

// A closed diagnostics pipe must not unwind a serving process past its
// registered-work and transport ownership witnesses.
fn emit_status(line: &str) {
    use std::io::Write;
    let _ = writeln!(std::io::stdout().lock(), "{line}");
}

fn emit_error(line: &str) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr().lock(), "{line}");
}
