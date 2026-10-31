use super::*;
use crate::SqliteStore;
use b4_runtime::StoreError;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-inspection-physical-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        drop(SqliteStore::open(&path).unwrap());
        for name in ["runtime.sqlite3", "owner.lock"] {
            std::fs::set_permissions(path.join(name), std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ChildProtocol {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl ChildProtocol {
    fn start(directory: &Directory, mode: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "inspection::source::tests::child_protocol",
                "--nocapture",
            ])
            .env("B4_INSPECTION_PRIVATE_CHILD", mode)
            .env("B4_INSPECTION_PRIVATE_DIRECTORY", &directory.0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            input,
            output,
        }
    }

    fn expect(&mut self, marker: &str) {
        loop {
            let mut line = String::new();
            assert!(
                self.output.read_line(&mut line).unwrap() > 0,
                "child must reach {marker}"
            );
            if line.trim() == marker {
                break;
            }
        }
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "child remains alive at {marker}"
        );
    }

    fn advance(&mut self) {
        self.input.write_all(b"continue\n").unwrap();
        self.input.flush().unwrap();
    }
    fn done(mut self) {
        self.advance();
        assert!(self.child.wait().unwrap().success());
    }
}

impl Drop for ChildProtocol {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn marker(value: &str) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}

fn wait_for_parent() {
    let mut line = String::new();
    assert!(std::io::stdin().read_line(&mut line).unwrap() > 0);
}

#[test]
fn child_protocol() {
    let Some(mode) = std::env::var_os("B4_INSPECTION_PRIVATE_CHILD") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("B4_INSPECTION_PRIVATE_DIRECTORY").unwrap());
    let mut reader = Reader::open(&path).unwrap();
    let budget = if mode == "limit" {
        super::super::validation::Budget::with_callbacks(1)
    } else {
        super::super::validation::Budget::new()
    };
    super::super::validation::configure(reader.connection(), &budget, true).unwrap();
    let inherited = reader.lock.as_ref().unwrap().try_clone().unwrap();
    marker("reader-held");
    wait_for_parent();
    match mode.to_str().unwrap() {
        "normal" => {
            reader.finish().unwrap();
            marker("reader-retired-and-unlocked");
        }
        "refusal" | "limit" => {
            let expected = if mode == "limit" {
                InspectionError::LimitExceeded
            } else {
                InspectionError::UnsupportedSchema
            };
            assert_eq!(
                super::super::validation::read(&mut reader, &budget, false).err(),
                Some(expected)
            );
            assert!(reader.connection.is_some());
            assert!(reader.expected_connection.is_none());
            reader.finish().unwrap();
            marker("refusal-reader-retired-and-unlocked");
        }
        "busy" => {
            let statement = reader
                .connection()
                .prepare("SELECT id FROM metadata")
                .unwrap();
            // Actual live SQLite statement, forgotten only in this private test.
            std::mem::forget(statement);
            assert_eq!(reader.finish().err(), Some(InspectionError::CloseUncertain));
            marker("actual-close-busy-quarantine");
        }
        "expected-busy" => {
            reader.expected_connection = Some(Connection::open_in_memory().unwrap());
            let statement = reader
                .expected_connection
                .as_ref()
                .unwrap()
                .prepare("SELECT 1")
                .unwrap();
            std::mem::forget(statement);
            assert_eq!(reader.finish().err(), Some(InspectionError::CloseUncertain));
            marker("actual-expected-close-busy-quarantine");
        }
        "pre-unlock" => {
            // This exercises retention after verified close. It is deliberately
            // not represented as an operating-system unlock fault.
            reader.close().unwrap();
            assert!(reader.connection.is_none());
            reader.quarantine(None);
            drop(reader);
            marker("closed-real-fd-retained-before-unlock");
        }
        "unwind" => {
            std::panic::set_hook(Box::new(|_| {}));
            let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _held = reader;
                panic!("private intentional unwind");
            }));
            assert!(unwind.is_err());
            marker("unwind-retired-and-unlocked");
        }
        _ => panic!("unknown private test mode"),
    }
    wait_for_parent();
    drop(inherited);
}

#[test]
fn actual_reader_blocks_writer_until_positive_close_and_explicit_unlock_with_duplicate_alive() {
    let directory = Directory::new();
    let mut child = ChildProtocol::start(&directory, "normal");
    child.expect("reader-held");
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    child.advance();
    child.expect("reader-retired-and-unlocked");
    let successor = SqliteStore::open(&directory.0).unwrap();
    drop(successor);
    child.done();
}

#[test]
fn actual_close_busy_quarantines_exact_live_connection_and_lock_while_child_alive() {
    let directory = Directory::new();
    let mut child = ChildProtocol::start(&directory, "busy");
    child.expect("reader-held");
    child.advance();
    child.expect("actual-close-busy-quarantine");
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    child.done();
    // Process cleanup, not a library successful-close witness.
    drop(SqliteStore::open(&directory.0).unwrap());
}

#[test]
fn actual_expected_close_busy_quarantines_both_connections_and_exact_lock() {
    let directory = Directory::new();
    let mut child = ChildProtocol::start(&directory, "expected-busy");
    child.expect("reader-held");
    child.advance();
    child.expect("actual-expected-close-busy-quarantine");
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    child.done();
}

#[test]
fn private_pre_unlock_refusal_retains_real_descriptor_after_verified_close() {
    let directory = Directory::new();
    let mut child = ChildProtocol::start(&directory, "pre-unlock");
    child.expect("reader-held");
    child.advance();
    child.expect("closed-real-fd-retained-before-unlock");
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    child.done();
}

#[test]
fn unwind_retires_reader_before_explicit_unlock_with_duplicate_alive() {
    let directory = Directory::new();
    let mut child = ChildProtocol::start(&directory, "unwind");
    child.expect("reader-held");
    child.advance();
    child.expect("unwind-retired-and-unlocked");
    drop(SqliteStore::open(&directory.0).unwrap());
    child.done();
}

#[test]
fn uri_encodes_raw_non_utf8_bytes_and_all_query_fragment_metacharacters() {
    let directory = Directory::new();
    let mut source = Source::preflight(&directory.0).unwrap();
    source.directory = PathBuf::from(std::ffi::OsString::from_vec(b"/tmp/odd\xff?#%&".to_vec()));
    assert_eq!(
        source.uri(),
        "file:/tmp/odd%FF%3F%23%25%26/runtime.sqlite3?mode=ro&immutable=1"
    );
}

#[test]
fn ordinary_schema_refusal_and_actual_sql_limit_retire_before_explicit_unlock() {
    for mode in ["refusal", "limit"] {
        let directory = Directory::new();
        let connection = Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
        if mode == "refusal" {
            connection
                .execute_batch("CREATE TABLE extra(x INTEGER)")
                .unwrap();
        } else {
            connection.execute_batch("WITH RECURSIVE rows(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM rows WHERE x<5000) INSERT INTO threads(id) SELECT printf('thread-%d',x) FROM rows;").unwrap();
        }
        connection.close().unwrap();
        let mut child = ChildProtocol::start(&directory, mode);
        child.expect("reader-held");
        assert!(matches!(
            SqliteStore::open(&directory.0),
            Err(StoreError::AlreadyOwned)
        ));
        child.advance();
        child.expect("refusal-reader-retired-and-unlocked");
        if mode == "limit" {
            drop(SqliteStore::open(&directory.0).unwrap());
        } else {
            // The unknown-schema refusal occurs only after acquiring ownership,
            // proving the reader released it while the child is still alive.
            assert!(matches!(
                SqliteStore::open(&directory.0),
                Err(StoreError::UnsupportedSchema)
            ));
        }
        child.done();
    }
}
