use std::fs::{DirBuilder, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

pub struct Directory(PathBuf);

impl Directory {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-unfinished-discovery-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        builder.create(&path).unwrap();
        let directory = Self(path);
        for name in ["owner.lock", "runtime.sqlite3"] {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            drop(options.open(directory.0.join(name)).unwrap());
        }
        directory.assert_private();
        directory
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn assert_private(&self) {
        #[cfg(unix)]
        {
            assert_eq!(
                std::fs::symlink_metadata(&self.0)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            for name in ["owner.lock", "runtime.sqlite3"] {
                let metadata = std::fs::symlink_metadata(self.0.join(name)).unwrap();
                assert!(metadata.is_file());
                assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            }
        }
    }

    pub fn connection(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }

    pub fn rows(&self) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
        let connection = self.connection();
        [
            "metadata",
            "threads",
            "executions",
            "journal",
            "approval_grants",
            "approval_acceptances",
        ]
        .into_iter()
        .map(|table| {
            let mut statement = connection
                .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
                .unwrap();
            let columns = statement.column_count();
            let rows = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| row.get(column))
                        .collect::<rusqlite::Result<Vec<rusqlite::types::Value>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            (table.into(), rows)
        })
        .collect()
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
