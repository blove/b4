//! Explicit ordinary startup preparation for the fixed server-owned demo.
use super::*;
use b4_http::{
    ConfigError, HttpHost, PreparingHttpHost, StartupPreparationHandle, StartupPreparationLimits,
    StartupPreparationOutcome, StartupPrincipalResolver, StartupResolutionError, StartupStartError,
    StartupWaitOutcome,
};
use b4_runtime::ThreadOrigin;

struct DemoResolver;
impl StartupPrincipalResolver for DemoResolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        if origin.application != "native-demo"
            || origin.tenant != "local"
            || origin.principal != "owner"
        {
            return Err(StartupResolutionError::Unauthorized);
        }
        Ok(Principal {
            application: "native-demo".into(),
            tenant: "local".into(),
            principal: "owner".into(),
        })
    }
}

pub(super) enum Host {
    Ordinary(HttpHost<SqliteStore>),
    Preparing(PreparingHttpHost<SqliteStore>),
}
impl Host {
    pub(super) fn new(
        recover: bool,
        store: Arc<SqliteStore>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: NativeHostConfig,
    ) -> Result<Self, ConfigError> {
        if recover {
            HttpHost::new_with_application_preparing(store, auth, policy, config)
                .map(Self::Preparing)
        } else {
            HttpHost::new_with_application(store, auth, policy, config).map(Self::Ordinary)
        }
    }
    pub(super) fn router(&self) -> axum::Router {
        match self {
            Self::Ordinary(host) => host.router(),
            Self::Preparing(host) => host.router(),
        }
    }
    pub(super) fn shutdown_handle(&self) -> ShutdownHandle {
        match self {
            Self::Ordinary(host) => host.shutdown_handle(),
            Self::Preparing(host) => host.shutdown_handle(),
        }
    }
    pub(super) fn begin_preparation(
        &self,
    ) -> Option<Result<StartupPreparationHandle, StartupStartError>> {
        match self {
            Self::Ordinary(_) => None,
            Self::Preparing(host) => {
                crate::emit_status("b4run_preparing");
                Some(
                    host.begin_preparation(
                        Arc::new(DemoResolver),
                        StartupPreparationLimits::default(),
                    ),
                )
            }
        }
    }
}

/// Pure classification; the real server result stays in its supervising owner.
pub(super) enum Trigger {
    Interrupt(bool),
    Terminate(bool),
    Server,
    Preparation,
}

pub(super) async fn first_trigger(
    preparation: Result<StartupPreparationHandle, StartupStartError>,
    server: &mut JoinHandle<Result<(), std::io::Error>>,
    signals: &mut Signals,
    server_result: &mut Option<ServerResult>,
    wait: Duration,
    #[cfg(test)] observer: Option<&super::tests::Observer>,
) -> Trigger {
    let Ok(handle) = preparation else {
        return Trigger::Preparation;
    };
    let mut preparing = true;
    loop {
        tokio::select! {
            signal = signals.interrupt.recv() => return Trigger::Interrupt(signal.is_none()),
            signal = signals.terminate.recv() => return Trigger::Terminate(signal.is_none()),
            result = &mut *server => {
                // Cache before returning only a resource-free trigger. Never
                // transfer an opaque JoinError into a temporary outcome value.
                *server_result = Some(result);
                return Trigger::Server;
            }
            outcome = handle.wait(wait), if preparing => {
                match outcome {
                    Ok(StartupWaitOutcome::Pending(_)) => {}
                    Ok(StartupWaitOutcome::Finished(StartupPreparationOutcome::Complete { .. })) => {
                        preparing = false;
                        crate::emit_status("b4run_ready");
                        #[cfg(test)]
                        if let Some(observer) = observer {
                            observer.marker("startup-complete");
                        }
                    }
                    Ok(StartupWaitOutcome::Finished(StartupPreparationOutcome::Incomplete { .. })) | Err(_) => {
                        return Trigger::Preparation;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "process_startup_tests.rs"]
pub(super) mod tests;
