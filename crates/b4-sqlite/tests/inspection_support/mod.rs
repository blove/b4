use b4_contract::*;
use b4_runtime::Store;
use b4_sqlite::SqliteStore;
use rusqlite::{params, Connection};
use serde_json::Map;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct Directory(PathBuf);

pub type FileWitness = (PathBuf, Vec<u8>, u64, u64, u32, u64, i64, i64);

impl Directory {
    pub fn clean() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-inspection-{}-{}",
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
        assert!(!path.join("runtime.sqlite3-wal").exists());
        assert!(!path.join("runtime.sqlite3-shm").exists());
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn witness(&self) -> Vec<FileWitness> {
        let mut paths = std::fs::read_dir(&self.0)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect::<Vec<_>>();
        paths.sort();
        let mut witnesses = paths
            .into_iter()
            .map(|path| {
                let m = std::fs::symlink_metadata(&path).unwrap();
                (
                    path.clone(),
                    std::fs::read(&path).unwrap(),
                    m.dev(),
                    m.ino(),
                    m.mode(),
                    m.len(),
                    m.mtime(),
                    m.mtime_nsec(),
                )
            })
            .collect::<Vec<_>>();
        let m = std::fs::metadata(&self.0).unwrap();
        witnesses.push((
            self.0.clone(),
            Vec::new(),
            m.dev(),
            m.ino(),
            m.mode(),
            m.len(),
            m.mtime(),
            m.mtime_nsec(),
        ));
        witnesses
    }

    pub fn populate(&self, count: usize) -> Vec<Snapshot> {
        let store = SqliteStore::open(self.path()).unwrap();
        let mut snapshots = Vec::new();
        for index in 0..count {
            let thread = ThreadId::new(format!("secret-thread-{index:04}")).unwrap();
            store.create_thread(thread.clone()).unwrap();
            let snapshot = initial(
                &format!("secret-execution-{index:04}"),
                store.owner_generation(),
            );
            snapshots.push(store.admit(&thread, snapshot).unwrap().snapshot);
        }
        drop(store);
        snapshots
    }

    pub fn sql(&self, sql: &str) {
        let connection = Connection::open(self.path().join("runtime.sqlite3")).unwrap();
        connection.execute_batch(sql).unwrap();
        connection.close().unwrap();
    }

    pub fn replace(&self, snapshot: &Snapshot) {
        b4_kernel::validate_snapshot(snapshot).unwrap();
        let connection = Connection::open(self.path().join("runtime.sqlite3")).unwrap();
        connection
            .execute(
                "UPDATE executions SET revision=?1, owner_epoch=?2, snapshot=?3 WHERE id=?4",
                params![
                    snapshot.revision.to_be_bytes().as_slice(),
                    snapshot.owner_generation.to_be_bytes().as_slice(),
                    serde_json::to_string(snapshot).unwrap(),
                    snapshot.execution_id.as_str()
                ],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE metadata SET owner_epoch = max(owner_epoch, ?1)",
                [snapshot.owner_generation.to_be_bytes().as_slice()],
            )
            .unwrap();
        connection.close().unwrap();
    }

    pub fn replace_json(&self, id: &str, payload: &str) {
        let connection = Connection::open(self.path().join("runtime.sqlite3")).unwrap();
        connection
            .execute(
                "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
                params![payload, id],
            )
            .unwrap();
        connection.close().unwrap();
    }

    pub fn rename(&mut self, name: std::ffi::OsString) {
        let path = self.path().with_file_name(name);
        std::fs::rename(&self.0, &path).unwrap();
        self.0 = path;
    }
}

pub fn initial(id: &str, owner: u64) -> Snapshot {
    b4_kernel::new_execution(
        ExecutionId::new(id).unwrap(),
        TurnId::new(format!("secret-turn-{id}")).unwrap(),
        owner,
        Definition {
            route: RouteKey::new("/secret-route#agent").unwrap(),
            revision: ExecutionRevision::new("secret-definition").unwrap(),
            batches: Vec::new(),
            max_attempts: 64,
        },
        Map::new(),
        TransitionFacts { now_ms: 123456 },
    )
    .unwrap()
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
