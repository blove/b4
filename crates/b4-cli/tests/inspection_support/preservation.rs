use rusqlite::{Connection, OpenFlags};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[derive(PartialEq, Eq)]
struct Entry {
    dev: u64,
    inode: u64,
    mode: u32,
    len: u64,
    mtime: i64,
    mtime_nsec: i64,
    bytes: Option<Vec<u8>>,
    link: Option<PathBuf>,
}

fn entry(path: &Path) -> Entry {
    let metadata = std::fs::symlink_metadata(path).unwrap();
    Entry {
        dev: metadata.dev(),
        inode: metadata.ino(),
        mode: metadata.mode(),
        len: metadata.len(),
        mtime: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
        bytes: metadata.is_file().then(|| std::fs::read(path).unwrap()),
        link: metadata
            .file_type()
            .is_symlink()
            .then(|| std::fs::read_link(path).unwrap()),
    }
}

#[derive(PartialEq, Eq)]
pub struct SourceWitness {
    directory: Entry,
    entries: BTreeMap<OsString, Entry>,
}

pub fn witness(data: &Path) -> SourceWitness {
    SourceWitness {
        directory: entry(data),
        entries: std::fs::read_dir(data)
            .unwrap()
            .map(|item| {
                let item = item.unwrap();
                (item.file_name(), entry(&item.path()))
            })
            .collect(),
    }
}

pub fn raw_connection(data: &Path) -> Connection {
    let path = std::fs::canonicalize(data.join("runtime.sqlite3")).unwrap();
    let mut uri = String::from("file:");
    for byte in path.as_os_str().as_bytes() {
        if byte.is_ascii_alphanumeric() || b"/_-.".contains(byte) {
            uri.push(char::from(*byte));
        } else {
            use std::fmt::Write;
            write!(&mut uri, "%{byte:02X}").unwrap();
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .unwrap()
}

pub fn owner_epoch(data: &Path) -> Vec<u8> {
    raw_connection(data)
        .query_row("SELECT owner_epoch FROM metadata WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap()
}

pub fn mutate(data: &Path, action: impl FnOnce(&Connection)) {
    let connection = Connection::open(data.join("runtime.sqlite3")).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    action(&connection);
    connection.close().unwrap();
    assert_eq!(
        std::fs::read_dir(data).unwrap().count(),
        2,
        "closed mutation fixture must have no sidecars"
    );
}
