#![cfg(unix)]

mod cli_support;

use cli_support::*;
use serde_json::json;
use std::ffi::OsString;
use std::net::TcpListener;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

fn static_failure(output: &Captured, code: i32, diagnostic: &str) {
    assert!(
        output.status.code() == Some(code),
        "refusal must use the specified public exit code"
    );
    assert!(
        output.stdout.is_empty(),
        "refusal must not announce readiness or disclose input"
    );
    assert!(
        output.stderr == format!("{diagnostic}\n").as_bytes(),
        "refusal must contain only the exact public static diagnostic"
    );
}

fn native(address: std::net::SocketAddr, thread: &str, input: &str) {
    let response = exchange(
        address,
        "POST",
        &format!("/threads/{thread}/runs/wait"),
        &format!(r#"{{"route":"/native#workflow","input":{input}}}"#),
    );
    assert!(
        response.status == 200,
        "provider-free native run must complete"
    );
    assert!(
        response.json()["task"] == "second",
        "native run output must follow immutable definition order"
    );
}

fn park(address: std::net::SocketAddr, thread: &str) -> serde_json::Value {
    let response = exchange(
        address,
        "POST",
        &format!("/threads/{thread}/runs/wait"),
        r#"{"route":"/approve#workflow","input":null}"#,
    );
    assert!(
        response.status == 200 && response.body == b"null",
        "required approval must park without publishing private grant in wait output"
    );
    let response = exchange(
        address,
        "GET",
        &format!("/threads/{thread}/pending_interrupts"),
        "",
    );
    assert!(
        response.status == 200 && response.headers.contains("cache-control: no-store"),
        "pending approval authority must be authenticated and uncacheable"
    );
    let interrupts = response.json()["interrupts"].clone();
    assert!(
        interrupts.as_array().is_some_and(|items| items.len() == 1),
        "required release must issue exactly one durable approval"
    );
    let interrupt = interrupts[0].clone();
    assert!(
        interrupt["grant"]
            .as_str()
            .is_some_and(|token| token.starts_with("b4ag_")),
        "required approval must contain real issued authority"
    );
    interrupt
}

#[test]
fn config_check_validates_demo_without_creating_storage() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-state");
    let output = run(command(&data).args(["config-check", "--demo"]), &sandbox);
    assert!(
        output.status.success(),
        "valid demo configuration must succeed"
    );
    assert!(
        output.stdout == b"b4run_config_valid\n",
        "configuration success must be static"
    );
    assert!(
        output.stderr.is_empty(),
        "valid configuration must have no diagnostic"
    );
    assert!(
        !data.exists(),
        "configuration validation must not create storage"
    );
}

#[test]
fn version_and_help_do_not_require_environment_storage_or_a_free_port() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-path-marker");
    let occupied =
        TcpListener::bind("127.0.0.1:0").expect("real occupied loopback port must be available");
    for flag in ["--version", "--help"] {
        let mut cmd = command(&data);
        cmd.current_dir(&sandbox.path);
        cmd.env_remove("B4RUN_TOKEN");
        cmd.env(
            "B4RUN_PORT",
            occupied.local_addr().unwrap().port().to_string(),
        );
        cmd.env("B4RUN_DATA_DIR", OsString::from_vec(vec![0xff]));
        cmd.env("B4RUN_SHUTDOWN_WAIT_MS", "invalid-private-marker");
        let output = run(cmd.arg(flag), &sandbox);
        assert!(
            output.status.success(),
            "public information must succeed without usable configuration"
        );
        assert!(
            output.stderr.is_empty(),
            "public information must have no diagnostic"
        );
        if flag == "--version" {
            assert!(
                output.stdout == b"b4run 0.1.0\n",
                "version must be exact fixed public text"
            );
        } else {
            let first = output.stdout;
            let second = run(command(&data).arg(flag), &sandbox);
            assert!(
                first == second.stdout && !first.is_empty(),
                "help must be fixed public text independent of configuration"
            );
        }
        assert!(
            !data.exists() && !sandbox.path.join(".b4run-data").exists(),
            "public information must not create storage"
        );
    }
}

#[test]
fn exact_command_grammar_refuses_missing_duplicate_unknown_and_non_utf8_options() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-path-marker");
    for args in [
        vec![],
        vec!["serve"],
        vec!["config-check"],
        vec!["--demo"],
        vec!["serve", "--demo", "--demo"],
        vec!["config-check", "--demo", "--demo"],
        vec!["serve", "--demo", "private-argument-marker"],
        vec!["config-check", "--demo", "--token=private-argument-marker"],
        vec!["--version", "--demo"],
        vec!["--help", "--help"],
    ] {
        let output = run(command(&data).args(args), &sandbox);
        static_failure(&output, 2, "b4run_invalid_arguments");
        assert!(
            !data.exists(),
            "usage refusal must precede storage mutation"
        );
    }
    let output = run(
        command(&data).args([
            OsString::from("serve"),
            OsString::from("--demo"),
            OsString::from_vec(vec![0xff]),
        ]),
        &sandbox,
    );
    static_failure(&output, 2, "b4run_invalid_arguments");
    assert!(
        !data.exists(),
        "non UTF8 usage refusal must precede storage mutation"
    );
}

#[test]
fn malformed_environment_is_static_and_never_creates_storage() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-path-marker");
    let oversized = "x".repeat(4097);
    for (key, value) in [
        ("B4RUN_TOKEN", ""),
        ("B4RUN_TOKEN", "private token"),
        ("B4RUN_TOKEN", "private\ncontrol"),
        ("B4RUN_TOKEN", "private\tcontrol"),
        ("B4RUN_TOKEN", "privateé"),
        ("B4RUN_TOKEN", oversized.as_str()),
        ("B4RUN_PORT", "65536"),
        ("B4RUN_PORT", "-1"),
        ("B4RUN_PORT", "+1"),
        ("B4RUN_PORT", " 1"),
        ("B4RUN_PORT", ""),
        ("B4RUN_PORT", "private-port-marker"),
        ("B4RUN_SHUTDOWN_WAIT_MS", "0"),
        ("B4RUN_SHUTDOWN_WAIT_MS", "300001"),
        ("B4RUN_SHUTDOWN_WAIT_MS", "-1"),
        ("B4RUN_SHUTDOWN_WAIT_MS", "+1"),
        ("B4RUN_SHUTDOWN_WAIT_MS", "1 "),
        ("B4RUN_SHUTDOWN_WAIT_MS", ""),
        ("B4RUN_DATA_DIR", ""),
        ("B4RUN_DATA_DIR", oversized.as_str()),
    ] {
        let mut cmd = command(&data);
        cmd.env(key, value);
        let output = run(cmd.args(["config-check", "--demo"]), &sandbox);
        static_failure(&output, 2, "b4run_invalid_configuration");
        assert!(
            !data.exists(),
            "invalid environment must not create storage"
        );
    }
    for key in [
        "B4RUN_TOKEN",
        "B4RUN_PORT",
        "B4RUN_SHUTDOWN_WAIT_MS",
        "B4RUN_DATA_DIR",
    ] {
        let mut cmd = command(&data);
        cmd.env(key, OsString::from_vec(vec![0xff]));
        let output = run(cmd.args(["config-check", "--demo"]), &sandbox);
        static_failure(&output, 2, "b4run_invalid_configuration");
    }
    let output = run(
        command(&data)
            .env_remove("B4RUN_TOKEN")
            .args(["serve", "--demo"]),
        &sandbox,
    );
    static_failure(&output, 2, "b4run_invalid_configuration");
    assert!(
        !data.exists(),
        "invalid serve configuration must not create storage"
    );
}

#[test]
fn recognized_bounds_and_defaults_validate_without_storage_mutation() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-state");
    for (token, port, wait) in [
        ("x".to_owned(), "0", "1"),
        ("x".repeat(4096), "65535", "300000"),
    ] {
        let output = run(
            command(&data)
                .env("B4RUN_TOKEN", token)
                .env("B4RUN_PORT", port)
                .env("B4RUN_SHUTDOWN_WAIT_MS", wait)
                .args(["config-check", "--demo"]),
            &sandbox,
        );
        assert!(
            output.status.success()
                && output.stdout == b"b4run_config_valid\n"
                && output.stderr.is_empty(),
            "all inclusive recognized bounds must validate"
        );
    }
    let output = run(
        command(&data)
            .current_dir(&sandbox.path)
            .env_remove("B4RUN_PORT")
            .env_remove("B4RUN_DATA_DIR")
            .env_remove("B4RUN_SHUTDOWN_WAIT_MS")
            .args(["config-check", "--demo"]),
        &sandbox,
    );
    assert!(
        output.status.success() && output.stdout == b"b4run_config_valid\n",
        "default local configuration must validate without mutation"
    );
    assert!(
        !data.exists() && !sandbox.path.join(".b4run-data").exists(),
        "configuration checking must not create default or explicit storage"
    );
}

#[test]
fn config_check_validates_filesystem_without_reading_database_or_creating_missing_parents() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-state");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&data)
        .expect("private directory fixture must be created");
    for name in ["runtime.sqlite3", "owner.lock"] {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(data.join(name))
            .expect("private uninitialized file fixture must be created");
        file.write_all(b"private-uninitialized-database-marker")
            .expect("private uninitialized fixture bytes must be written");
    }
    let before = file_bytes(&data);
    let output = run(command(&data).args(["config-check", "--demo"]), &sandbox);
    assert!(
        output.status.success()
            && output.stdout == b"b4run_config_valid\n"
            && output.stderr.is_empty(),
        "configuration checking must not inspect SQLite integrity or acquire a runtime owner"
    );
    assert!(
        before == file_bytes(&data),
        "configuration checking must preserve uninitialized database and owner bytes exactly"
    );
    let missing_parent = sandbox.path.join("absent-parent");
    let output = run(
        command(&missing_parent.join("private-state")).args(["config-check", "--demo"]),
        &sandbox,
    );
    static_failure(&output, 2, "b4run_invalid_storage");
    assert!(
        !missing_parent.exists(),
        "readonly path validation must not create missing parents"
    );
}

#[test]
fn config_check_does_not_open_or_advance_live_owned_database_or_bind_occupied_port() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-state");
    let (mut process, address) = serve(&data, &sandbox);
    native(address, "native", "null");
    let _authority = park(address, "parked");
    let rows = all_rows(&data);
    let bytes = file_bytes(&data);
    let output = run(
        command(&data)
            .env("B4RUN_PORT", address.port().to_string())
            .args(["config-check", "--demo"]),
        &sandbox,
    );
    assert!(
        output.status.success()
            && output.stdout == b"b4run_config_valid\n"
            && output.stderr.is_empty(),
        "valid configuration must succeed while configured port and owner are already occupied"
    );
    assert!(
        bytes == file_bytes(&data),
        "configuration checking must preserve all actual storage bytes"
    );
    assert!(rows == all_rows(&data), "configuration checking must preserve actual owner epoch and every runtime or approval record");
    assert!(
        exchange(address, "GET", "/b4/capabilities", "").status == 200,
        "source owner must remain operational after readonly checking"
    );
    clean_stop(&mut process, "-TERM");
}

#[test]
fn prior_database_bytes_rows_and_epoch_survive_config_and_bind_failures() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-state");
    let (mut process, address) = serve(&data, &sandbox);
    native(address, "native", "{}");
    let _authority = park(address, "parked");
    clean_stop(&mut process, "-TERM");
    let rows = all_rows(&data);
    let bytes = file_bytes(&data);
    let output = run(
        command(&data)
            .env("B4RUN_TOKEN", "private invalid marker")
            .args(["serve", "--demo"]),
        &sandbox,
    );
    static_failure(&output, 2, "b4run_invalid_configuration");
    assert!(
        bytes == file_bytes(&data) && rows == all_rows(&data),
        "configuration refusal must leave prior durable state and owner epoch byte exact"
    );
    let occupied = TcpListener::bind("127.0.0.1:0").expect("real loopback port must be held");
    let output = run(
        command(&data)
            .env(
                "B4RUN_PORT",
                occupied.local_addr().unwrap().port().to_string(),
            )
            .args(["serve", "--demo"]),
        &sandbox,
    );
    static_failure(&output, 1, "b4run_listener_failed");
    assert!(
        bytes == file_bytes(&data) && rows == all_rows(&data),
        "bind refusal must precede SQLite opening and preserve prior rows bytes and owner epoch"
    );
}

#[test]
fn shipped_server_has_exact_browser_auth_guards_native_completion_and_no_control_routes() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-state");
    let (mut process, address) = serve(&data, &sandbox);
    let capabilities = exchange(address, "GET", "/b4/capabilities", "");
    assert!(
        capabilities.status == 200,
        "authenticated provider-free capability route must work"
    );
    assert!(
        !capabilities.headers.contains("transfer-encoding: chunked")
            && capabilities.headers.contains("cache-control: no-store"),
        "public JSON capability descriptor must be bounded and uncacheable"
    );
    let descriptor = capabilities.json();
    assert!(
        descriptor["descriptor_version"] == 2,
        "shipped native capability descriptor must remain v2"
    );
    assert!(
        descriptor["application"] == json!({"id":"native-demo", "revision":"native-demo-v1"}),
        "shipped demo identity must be immutable"
    );
    assert!(
        descriptor["store"]
            == json!({"kind":"sqlite", "durable_runtime":true,"durable_approvals":true}),
        "shipped server must report actual durable SQLite approvals"
    );
    assert!(
        descriptor["features"]["supervised_admission_drain"] == true
            && descriptor["features"]["graceful_shutdown"] == false,
        "supervised drain must not overclaim full graceful shutdown"
    );
    assert!(
        descriptor["features"]["required_once_deny"] == true,
        "required once approval must be active"
    );
    for headers in [
        format!(
            "Host: localhost:{}\r\nAuthorization: Bearer {TOKEN}\r\n",
            address.port()
        ),
        format!("Host: {address}\r\nOrigin: null\r\nAuthorization: Bearer {TOKEN}\r\n"),
        format!("Host: {address}\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\n"),
        format!("Authorization: Bearer {TOKEN}\r\n"),
    ] {
        let response = raw_exchange(address, "GET", "/b4/capabilities", &headers, "");
        assert!(
            response.status == 403,
            "exact Host and no Origin guard must reject browser or ambiguous host requests"
        );
    }
    for headers in [
        format!("Host: {address}\r\n"),
        format!("Host: {address}\r\nAuthorization: Bearer incorrect\r\n"),
        format!("Host: {address}\r\nAuthorization: bearer {TOKEN}\r\n"),
        format!(
            "Host: {address}\r\nAuthorization: Bearer {TOKEN}\r\nAuthorization: Bearer {TOKEN}\r\n"
        ),
    ] {
        assert!(
            raw_exchange(address, "GET", "/b4/capabilities", &headers, "").status == 401,
            "exact single Bearer authentication must be enforced"
        );
    }
    native(address, "ordered", r#"{"request":"qualified"}"#);
    let state = snapshot(&data, "ordered");
    assert!(
        state["status"] == "completed" && state["values"]["items"] == json!(["first", "second"]),
        "actual native durable state must join finite callbacks in definition order"
    );
    let _authority = park(address, "parked");
    let state = snapshot(&data, "parked");
    assert!(
        state["status"] == "waiting_for_approval",
        "required release must remain in a stable parked execution"
    );
    assert!(
        state["tasks"][0]["status"] == "succeeded"
            && state["tasks"][0]["result"]["writes"] == json!([{"type":"append","field":"items","items":["prepared"]}])
            && state["tasks"][1]["status"] == "waiting_for_approval"
            && state["tasks"][1]["result"].is_null(),
        "parked native state must retain the succeeded prepare result while release has no callback result"
    );
    for path in [
        "/fixture",
        "/fixture/release",
        "/fixture/control",
        "/control",
        "/shutdown",
        "/signal",
    ] {
        let response = exchange(address, "POST", path, "{}");
        assert!(
            response.status == 501 && response.json()["error"]["details"]["code"] == "unsupported_operation",
            "fixture and process control paths must receive only the existing unsupported-operation response"
        );
    }
    private_modes(&data);
    let output = clean_stop(&mut process, "-TERM");
    assert!(
        !String::from_utf8(output.stdout).unwrap().contains(TOKEN),
        "status output must not retain authentication secret"
    );
}

#[test]
fn actual_second_process_owner_refusal_preserves_live_authority_and_state() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-state");
    let (mut owner, address) = serve(&data, &sandbox);
    let authority = park(address, "parked");
    let rows = all_rows(&data);
    let bytes = file_bytes(&data);
    let output = run(command(&data).args(["serve", "--demo"]), &sandbox);
    static_failure(&output, 1, "b4run_storage_owned");
    assert!(
        bytes == file_bytes(&data) && rows == all_rows(&data),
        "actual contender must not truncate chmod or advance live owner's durable state"
    );
    let pending = exchange(address, "GET", "/threads/parked/pending_interrupts", "").json();
    assert!(
        pending["interrupts"][0] == authority,
        "actual source owner must retain exact approval authority after contender refusal"
    );
    clean_stop(&mut owner, "-INT");
}

#[test]
fn sigint_and_sigterm_preserve_exact_parked_authority_and_runtime_state_then_reopen() {
    for signal in ["-INT", "-TERM"] {
        let sandbox = Sandbox::new();
        let data = sandbox.path.join("private-state");
        let (mut process, address) = serve(&data, &sandbox);
        native(address, "native", "null");
        let authority = park(address, "parked");
        let rows = all_rows(&data);
        let grants = sql_rows(&data, "approval_grants");
        let acceptances = sql_rows(&data, "approval_acceptances");
        assert!(
            grants.len() == 1 && acceptances.is_empty(),
            "park must durably issue one unconsumed authority without accepting it"
        );
        clean_stop(&mut process, signal);
        assert!(rows == all_rows(&data), "actual clean signal shutdown must preserve exact runtime state and all authority records");
        private_modes(&data);
        let (mut reopened, address) = serve(&data, &sandbox);
        assert!(
            grants == sql_rows(&data, "approval_grants")
                && acceptances == sql_rows(&data, "approval_acceptances"),
            "fresh actual owner must preserve exact token prompt expiry and acceptance bytes"
        );
        let pending = exchange(address, "GET", "/threads/parked/pending_interrupts", "");
        assert!(
            pending.status == 501
                && pending.json()["error"]["details"]["code"] == "unsupported_execution_state",
            "ordinary pending must preserve the existing prior-owner unsupported-state response"
        );
        let attachment = exchange(address, "GET", "/threads/parked/runs/stream", "");
        assert!(
            attachment.status == 200 && attachment.headers.contains("cache-control: no-store"),
            "reopened owner must allow authenticated readonly parked attachment"
        );
        let frames = attachment.frames();
        assert!(frames.len() == 2 && frames[0].0 == "state" && frames[0].1["status"] == "interrupted" && frames[0].1["interrupts"][0] == authority && frames[1] == ("done".to_owned(), json!({"output":null})), "readonly parked attachment after reopen must project exact original authority and finite inactive completion");
        assert!(grants == sql_rows(&data, "approval_grants") && acceptances == sql_rows(&data, "approval_acceptances"), "readonly prior-owner attachment must not renew expire consume or rewrite original authority");
        let state = snapshot(&data, "native");
        assert!(
            state["status"] == "completed"
                && state["values"]["items"] == json!(["first", "second"]),
            "reopened server must retain completed ordered native state"
        );
        clean_stop(&mut reopened, signal);
    }
}

#[test]
fn shipped_startup_repairs_only_new_files_under_restrictive_umask() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-state");
    let mut process = Process::spawn(&mut command_under_restrictive_umask(&data), &sandbox);
    let address = process.listening();
    private_modes(&data);
    native(address, "native", "null");
    private_modes(&data);
    clean_stop(&mut process, "-TERM");
    private_modes(&data);
}
