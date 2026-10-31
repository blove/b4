#![cfg(unix)]

// Reuse only this protected zero-declaration helper module.
#[allow(dead_code)]
#[path = "cli_support/mod.rs"]
mod cli_support;

use b4_runtime::Store;
use b4_sqlite::SqliteStore;
use cli_support::*;
use rusqlite::types::Value as SqlValue;
use std::net::{SocketAddr, TcpListener};

fn stop_with_startup_statuses(process: &mut Process) {
    process.signal("-INT");
    let output = process.wait();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert!(output.stdout.ends_with(b"b4run_stopped\n"));
    let text = std::str::from_utf8(&output.stdout).unwrap();
    assert!(text.lines().any(|line| line == "b4run_draining"));
    for line in text.lines() {
        let listener = line
            .strip_prefix("b4run_listening http://")
            .and_then(|address| address.parse::<SocketAddr>().ok())
            .is_some_and(|address| {
                address.is_ipv4() && address.ip().is_loopback() && address.port() != 0
            });
        assert!(
            listener
                || [
                    "b4run_preparing",
                    "b4run_ready",
                    "b4run_draining",
                    "b4run_drain_incomplete",
                    "b4run_stopped"
                ]
                .contains(&line)
        );
    }
}

#[test]
fn recovery_config_check_both_orders_validate_without_storage_or_listener_acquisition() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("configuration-only-state");
    let occupied = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = occupied.local_addr().unwrap().port().to_string();
    for options in [
        ["config-check", "--demo", "--recover"],
        ["config-check", "--recover", "--demo"],
    ] {
        let output = run(
            command(&data).args(options).env("B4RUN_PORT", &port),
            &sandbox,
        );
        assert!(output.status.success());
        assert_eq!(output.stdout, b"b4run_config_valid\n");
        assert!(output.stderr.is_empty());
        assert!(
            !data.exists(),
            "validation must not create storage or advance an owner"
        );
    }
    drop(occupied);
}

#[test]
fn explicit_empty_store_startup_preserves_empty_work_and_releases_its_real_owner() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("empty-recovery-state");
    let mut process = Process::spawn(
        command(&data).args(["serve", "--recover", "--demo"]),
        &sandbox,
    );
    let address = process.listening();
    assert!(address.is_ipv4() && address.ip().is_loopback() && address.port() != 0);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(12);
    loop {
        let response = exchange(address, "GET", "/readyz", "");
        if response.status == 200 {
            break;
        }
        assert_eq!(response.status, 503);
        assert!(
            std::time::Instant::now() < deadline,
            "empty startup must complete within the observation bound"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    private_modes(&data);
    let before = all_rows(&data);
    assert_eq!(before.len(), 6);
    assert_eq!(before[0].len(), 1);
    assert_eq!(before[0][0].len(), 4);
    assert!(before.iter().skip(1).all(Vec::is_empty));
    let SqlValue::Blob(owner_bytes) = &before[0][0][3] else {
        panic!("actual metadata owner must be its fixed byte encoding");
    };
    let owner = u64::from_be_bytes(owner_bytes.clone().try_into().unwrap());
    stop_with_startup_statuses(&mut process);
    let writer = SqliteStore::open(&data)
        .expect("normal empty startup retirement must permit a genuine next writer");
    let next_owner = writer.owner_generation();
    assert_eq!(next_owner, owner + 1);
    drop(writer);
    let mut expected = before;
    expected[0][0][3] = SqlValue::Blob(next_owner.to_be_bytes().to_vec());
    assert_eq!(all_rows(&data), expected);
    private_modes(&data);
}
