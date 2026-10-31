//! The executable owns its executor and releases it only after two witnesses.
use crate::{
    config::{Config, LauncherError},
    demo::AppAssembly,
};

pub(crate) fn serve(config: Config, assembly: AppAssembly) -> Result<(), LauncherError> {
    #[cfg(unix)]
    {
        serve_owned(
            config,
            assembly,
            false,
            #[cfg(test)]
            None,
        )
    }
    #[cfg(not(unix))]
    {
        let _ = (config, assembly);
        Err(LauncherError::UnsupportedPlatform)
    }
}

pub(crate) fn serve_recover(config: Config, assembly: AppAssembly) -> Result<(), LauncherError> {
    #[cfg(unix)]
    {
        serve_owned(
            config,
            assembly,
            true,
            #[cfg(test)]
            None,
        )
    }
    #[cfg(not(unix))]
    {
        let _ = (config, assembly);
        Err(LauncherError::UnsupportedPlatform)
    }
}

#[cfg(unix)]
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
#[cfg(unix)]
use b4_http::{
    AuthenticationError, Authenticator, NativeHostConfig, Principal, ResourceContext,
    ResourcePolicy, ShutdownHandle, ShutdownOutcome,
};
#[cfg(unix)]
use b4_sqlite::SqliteStore;
#[cfg(unix)]
use std::{net::Ipv4Addr, sync::Arc, time::Duration};
#[cfg(unix)]
use tokio::{
    net::TcpListener,
    signal::unix::{signal, Signal, SignalKind},
    sync::oneshot,
    task::JoinHandle,
};

#[cfg(unix)]
struct Signals {
    interrupt: Signal,
    terminate: Signal,
}
#[cfg(unix)]
struct BearerAuth(String);
#[cfg(unix)]
impl Authenticator for BearerAuth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers.get_all("authorization").iter().count() != 1
            || headers.get("authorization").and_then(|v| v.to_str().ok()) != Some(self.0.as_str())
        {
            return Err(AuthenticationError);
        }
        Ok(Principal {
            application: "native-demo".into(),
            tenant: "local".into(),
            principal: "owner".into(),
        })
    }
}
#[cfg(unix)]
struct OwnerPolicy;
#[cfg(unix)]
impl ResourcePolicy for OwnerPolicy {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        context.origin.as_ref().is_none_or(|origin| {
            origin.application == principal.application
                && origin.tenant == principal.tenant
                && origin.principal == principal.principal
        })
    }
}
#[cfg(unix)]
async fn browser_guard(
    State(host): State<Arc<String>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if request.headers().contains_key("origin")
        || request.headers().get_all("host").iter().count() != 1
        || request.headers().get("host").and_then(|v| v.to_str().ok()) != Some(host.as_str())
    {
        return (
            StatusCode::FORBIDDEN,
            [("content-type", "application/json")],
            r#"{"error":{"kind":"request_error","message":"request_origin_denied","details":{}}}"#,
        )
            .into_response();
    }
    next.run(request).await
}

#[cfg(unix)]
fn serve_owned(
    config: Config,
    assembly: AppAssembly,
    recover: bool,
    #[cfg(test)] observer: Option<tests::Observer>,
) -> Result<(), LauncherError> {
    // Pure checks precede every socket and storage mutation. Signal receivers
    // are kept outside block_on and survive normal executor destruction.
    crate::storage::validate(config.data_dir())?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| LauncherError::Executor)?;
    let mut signals = runtime.block_on(async {
        Ok::<_, LauncherError>(Signals {
            interrupt: signal(SignalKind::interrupt()).map_err(|_| LauncherError::Signal)?,
            terminate: signal(SignalKind::terminate()).map_err(|_| LauncherError::Signal)?,
        })
    })?;
    let result = runtime.block_on(async {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, config.port()))
            .await
            .map_err(|_| LauncherError::Listener)?;
        let address = listener.local_addr().map_err(|_| LauncherError::Listener)?;
        let directory = crate::storage::prepare(config.data_dir())?;
        let store = SqliteStore::open(&directory).map_err(|error| {
            if matches!(error, b4_runtime::StoreError::AlreadyOwned) {
                LauncherError::StorageOwned
            } else {
                LauncherError::StorageStartup
            }
        })?;
        crate::storage::validate_opened(&directory)?;
        let policy: Arc<dyn ResourcePolicy> = Arc::new(OwnerPolicy);
        #[cfg(test)]
        let policy = tests::policy(observer.as_ref(), policy);
        #[cfg(test)]
        let policy = startup::tests::policy(observer.as_ref(), policy);
        #[cfg(test)]
        let application_weak = Arc::downgrade(&assembly.application);
        let mut host_config = NativeHostConfig::new(assembly.application, assembly.approval);
        host_config.limits = assembly.limits;
        let store = Arc::new(store);
        let auth: Arc<dyn Authenticator> =
            Arc::new(BearerAuth(format!("Bearer {}", config.token())));
        #[cfg(test)]
        let witnesses = startup::tests::Witnesses::new(&store, &auth, &policy, application_weak);
        let host = startup::Host::new(recover, store, auth, policy, host_config)
            .map_err(|_| LauncherError::Host)?;
        let shutdown = host.shutdown_handle();
        let router = host.router().layer(middleware::from_fn_with_state(
            Arc::new(address.to_string()),
            browser_guard,
        ));
        let (stop_tx, stop_rx) = oneshot::channel();
        #[cfg(test)]
        let listener = tests::ObservedListener::new(listener, observer.clone());
        #[cfg(test)]
        let server_observer = observer.clone();
        let server = tokio::spawn(async move {
            let serving = async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        let _ = stop_rx.await;
                    })
                    .await
            };
            #[cfg(test)]
            {
                tests::serve_with_panic_probe(serving, server_observer).await
            }
            #[cfg(not(test))]
            {
                serving.await
            }
        });
        #[cfg(test)]
        if let Some(observer) = observer.as_ref() {
            tokio::spawn(observer.clone().probe(server.abort_handle()));
            observer.listening(address);
        }
        crate::emit_status(&format!("b4run_listening http://{address}"));
        // The actual outer server task is owned before synchronous startup
        // registration. A failed start enters the same owned drain below.
        let preparation = host.begin_preparation();
        let outcome = supervise(
            &shutdown,
            server,
            stop_tx,
            &mut signals,
            config.shutdown_wait(),
            preparation,
            #[cfg(test)]
            observer.as_ref(),
        )
        .await;
        // Both physical witnesses have been cached before these owned captures
        // and the normal Runtime destructor can execute.
        drop(host);
        #[cfg(test)]
        if recover {
            witnesses.retired(observer.as_ref());
        }
        Ok::<_, LauncherError>(outcome)
    });
    drop(runtime);
    #[cfg(test)]
    if recover {
        startup::tests::runtime_destroyed(observer.as_ref());
    }
    match result {
        Ok(outcome) => {
            crate::emit_status("b4run_stopped");
            outcome
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Transport {
    Pending,
    Retired,
    Lost,
}

#[cfg(unix)]
type ServerResult = Result<Result<(), std::io::Error>, tokio::task::JoinError>;

#[cfg(unix)]
fn observe_server(
    result: &ServerResult,
    #[cfg(test)] observer: Option<&tests::Observer>,
) -> Transport {
    if matches!(result, Ok(Ok(()))) {
        #[cfg(test)]
        if let Some(observer) = observer {
            observer.marker("server-success");
        }
        Transport::Retired
    } else {
        crate::emit_error(LauncherError::Server.code());
        #[cfg(test)]
        if let Some(observer) = observer {
            observer.marker("server-failure");
        }
        Transport::Lost
    }
}

#[cfg(unix)]
async fn supervise(
    shutdown: &ShutdownHandle,
    mut server: JoinHandle<Result<(), std::io::Error>>,
    stop_tx: oneshot::Sender<()>,
    signals: &mut Signals,
    wait: Duration,
    preparation: Option<Result<b4_http::StartupPreparationHandle, b4_http::StartupStartError>>,
    #[cfg(test)] observer: Option<&tests::Observer>,
) -> Result<(), LauncherError> {
    let mut interrupt_open = true;
    let mut terminate_open = true;
    let mut infrastructure_fault = false;
    // A failed JoinError can own an opaque panic payload with a panicking or
    // blocking destructor. Inspect it only by reference, retaining the entire
    // result for the nonreturning Lost state until external process termination.
    let mut server_result = None;
    let mut first_signal_failed = false;
    let mut preparation_failed = false;
    if let Some(preparation) = preparation {
        match startup::first_trigger(
            preparation,
            &mut server,
            signals,
            &mut server_result,
            wait,
            #[cfg(test)]
            observer,
        )
        .await
        {
            startup::Trigger::Interrupt(closed) => {
                if closed {
                    interrupt_open = false;
                    infrastructure_fault = true;
                    first_signal_failed = true;
                }
            }
            startup::Trigger::Terminate(closed) => {
                if closed {
                    terminate_open = false;
                    infrastructure_fault = true;
                    first_signal_failed = true;
                }
            }
            startup::Trigger::Server => infrastructure_fault = true,
            startup::Trigger::Preparation => preparation_failed = true,
        }
    } else {
        tokio::select! {
            signal = signals.interrupt.recv() => { if signal.is_none() { interrupt_open = false; infrastructure_fault = true; first_signal_failed = true; } }
            signal = signals.terminate.recv() => { if signal.is_none() { terminate_open = false; infrastructure_fault = true; first_signal_failed = true; } }
            result = &mut server => { server_result = Some(result); infrastructure_fault = true; }
        }
    }
    drain(
        shutdown,
        server,
        stop_tx,
        signals,
        wait,
        DrainTrigger {
            interrupt_open,
            terminate_open,
            infrastructure_fault,
            first_signal_failed,
            preparation_failed,
        },
        &mut server_result,
        #[cfg(test)]
        observer,
    )
    .await
}

#[cfg(unix)]
struct DrainTrigger {
    interrupt_open: bool,
    terminate_open: bool,
    infrastructure_fault: bool,
    first_signal_failed: bool,
    preparation_failed: bool,
}

#[cfg(unix)]
#[cfg_attr(test, allow(clippy::too_many_arguments))] // Test-only borrowed observer accompanies the existing owned drain inputs.
async fn drain(
    shutdown: &ShutdownHandle,
    mut server: JoinHandle<Result<(), std::io::Error>>,
    stop_tx: oneshot::Sender<()>,
    signals: &mut Signals,
    wait: Duration,
    trigger: DrainTrigger,
    server_result: &mut Option<ServerResult>,
    #[cfg(test)] observer: Option<&tests::Observer>,
) -> Result<(), LauncherError> {
    let DrainTrigger {
        mut interrupt_open,
        mut terminate_open,
        mut infrastructure_fault,
        first_signal_failed,
        preparation_failed,
    } = trigger;
    let mut transport = Transport::Pending;
    // Admission closure is synchronous and precedes notifying Axum.
    shutdown.begin_shutdown();
    let _ = stop_tx.send(());
    crate::emit_status("b4run_draining");
    #[cfg(test)]
    if let Some(observer) = observer {
        observer.marker("draining");
    }
    if first_signal_failed {
        crate::emit_error(LauncherError::Signal.code());
    }
    if let Some(result) = server_result.as_ref() {
        transport = observe_server(
            result,
            #[cfg(test)]
            observer,
        );
        if transport == Transport::Retired {
            crate::emit_error(LauncherError::Server.code());
        }
    }
    let mut quiescent = None;
    let mut registered_wait = Box::pin(shutdown.wait_quiescent(wait));
    let mut transport_wait = Box::pin(tokio::time::sleep(wait));
    loop {
        if let Some(faulted) = quiescent {
            if transport == Transport::Retired {
                return if faulted {
                    Err(LauncherError::HostFault)
                } else if infrastructure_fault {
                    Err(LauncherError::Server)
                } else if preparation_failed {
                    Err(LauncherError::Preparation)
                } else {
                    Ok(())
                };
            }
        }
        tokio::select! {
            signal = signals.interrupt.recv(), if interrupt_open => { if signal.is_none() { interrupt_open = false; infrastructure_fault = true; crate::emit_error(LauncherError::Signal.code()); } }
            signal = signals.terminate.recv(), if terminate_open => { if signal.is_none() { terminate_open = false; infrastructure_fault = true; crate::emit_error(LauncherError::Signal.code()); } }
            result = &mut server, if transport == Transport::Pending => { transport = observe_server(server_result.insert(result), #[cfg(test)] observer); if transport == Transport::Lost { infrastructure_fault = true; } }
            outcome = &mut registered_wait, if quiescent.is_none() => {
                match outcome {
                    Ok(ShutdownOutcome::Quiescent { faulted }) => {
                        quiescent = Some(faulted);
                        transport_wait.as_mut().reset(tokio::time::Instant::now() + wait);
                        #[cfg(test)]
                        if let Some(observer) = observer {
                            observer.marker("quiescent");
                            if transport == Transport::Pending && !server.is_finished() {
                                observer.marker("pending-server-at-quiescence");
                            }
                        }
                    }
                    Ok(ShutdownOutcome::Incomplete { .. }) => {
                        crate::emit_status("b4run_drain_incomplete");
                        #[cfg(test)]
                        if let Some(observer) = observer { observer.incomplete(); }
                        registered_wait = Box::pin(shutdown.wait_quiescent(wait));
                    }
                    Err(_) => {
                        // Configuration admitted only validated durations. Even
                        // an internal wait fault cannot authorize owner release.
                        infrastructure_fault = true;
                        registered_wait = Box::pin(shutdown.wait_quiescent(b4_http::DEFAULT_SHUTDOWN_WAIT));
                    }
                }
            }
            _ = &mut transport_wait, if quiescent.is_some() => {
                crate::emit_status("b4run_drain_incomplete");
                #[cfg(test)]
                if let Some(observer) = observer { observer.incomplete(); }
                transport_wait.as_mut().reset(tokio::time::Instant::now() + wait);
            }
        }
    }
}

#[cfg(all(test, unix))]
fn serve_observed(
    config: Config,
    assembly: AppAssembly,
    observer: tests::Observer,
) -> Result<(), LauncherError> {
    serve_owned(config, assembly, false, Some(observer))
}
#[cfg(all(test, unix))]
#[path = "process_tests.rs"]
mod tests;

#[cfg(unix)]
#[path = "process_startup.rs"]
mod startup;
