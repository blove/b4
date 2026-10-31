use b4_runtime::confinement::backend::AuditBudget;
use b4_runtime::RecoveryConfinementBackendError as Error;
use rusqlite::{types::ValueRef, Row};

#[derive(Clone, Copy)]
pub(super) enum Kind {
    Text(usize),
    Blob8,
    Integer,
    Null,
}

/// Only fixed compile-time table/column/tail strings are passed here. CASE
/// prevents over-cap payloads from being exposed to owned decoding.
pub(super) fn select(table: &str, fields: &[(&str, Kind)], tail: &str) -> String {
    let columns = fields.iter().map(|(column, kind)| {
        let predicate = match kind {
            Kind::Text(cap) => format!("typeof({column}) = 'text' AND length(CAST({column} AS BLOB)) <= {cap}"),
            Kind::Blob8 => format!("typeof({column}) = 'blob' AND length({column}) = 8"),
            Kind::Integer => format!("typeof({column}) = 'integer'"),
            Kind::Null => format!("typeof({column}) = 'null'"),
        };
        format!("typeof({column}), length(CAST({column} AS BLOB)), CASE WHEN {predicate} THEN {column} END")
    }).collect::<Vec<_>>().join(", ");
    format!("SELECT {columns} FROM {table} {tail}")
}

fn type_name<'row>(row: &'row Row<'_>, field: usize) -> Result<&'row str, Error> {
    match row.get_ref(field * 3).map_err(|_| Error::CorruptRecord)? {
        ValueRef::Text(bytes) => std::str::from_utf8(bytes).map_err(|_| Error::CorruptRecord),
        _ => Err(Error::CorruptRecord),
    }
}

pub(super) fn length(row: &Row<'_>, field: usize) -> Result<usize, Error> {
    match row
        .get_ref(field * 3 + 1)
        .map_err(|_| Error::CorruptRecord)?
    {
        ValueRef::Integer(n) => usize::try_from(n).map_err(|_| Error::CorruptRecord),
        _ => Err(Error::CorruptRecord),
    }
}

pub(super) fn text<'row>(row: &'row Row<'_>, field: usize, cap: usize) -> Result<&'row str, Error> {
    let len = text_len(row, field, cap)?;
    match row
        .get_ref(field * 3 + 2)
        .map_err(|_| Error::CorruptRecord)?
    {
        ValueRef::Text(bytes) if bytes.len() == len => {
            std::str::from_utf8(bytes).map_err(|_| Error::CorruptRecord)
        }
        _ => Err(Error::CorruptRecord),
    }
}

pub(super) fn text_len(row: &Row<'_>, field: usize, cap: usize) -> Result<usize, Error> {
    if type_name(row, field)? != "text" {
        return Err(Error::CorruptRecord);
    }
    let len = length(row, field)?;
    if len > cap {
        return Err(Error::LimitExceeded);
    }
    Ok(len)
}

pub(super) fn optional_len(row: &Row<'_>, field: usize, cap: usize) -> Result<usize, Error> {
    if type_name(row, field)? == "null" {
        null(row, field)?;
        return Ok(0);
    }
    text_len(row, field, cap)
}

pub(super) fn payload<'row>(
    row: &'row Row<'_>,
    field: usize,
    cap: usize,
    budget: &mut AuditBudget,
) -> Result<&'row [u8], Error> {
    let len = text_len(row, field, cap)?;
    budget.check_bytes(len)?;
    let bytes = text(row, field, cap)?.as_bytes();
    budget.raw(bytes, cap)?;
    Ok(bytes)
}

pub(super) fn indexed_text<'row>(
    row: &'row Row<'_>,
    field: usize,
    cap: usize,
    budget: &mut AuditBudget,
) -> Result<&'row str, Error> {
    let value = text(row, field, cap)?;
    budget.add_bytes(value.len())?;
    Ok(value)
}

pub(super) fn optional_text<'row>(
    row: &'row Row<'_>,
    field: usize,
    cap: usize,
) -> Result<Option<&'row str>, Error> {
    if type_name(row, field)? == "null" {
        null(row, field)?;
        return Ok(None);
    }
    text(row, field, cap).map(Some)
}

pub(super) fn unsigned(
    row: &Row<'_>,
    field: usize,
    budget: &mut AuditBudget,
) -> Result<u64, Error> {
    if type_name(row, field)? != "blob" || length(row, field)? != 8 {
        return Err(Error::CorruptRecord);
    }
    match row
        .get_ref(field * 3 + 2)
        .map_err(|_| Error::CorruptRecord)?
    {
        ValueRef::Blob(bytes) => {
            let bytes: [u8; 8] = bytes.try_into().map_err(|_| Error::CorruptRecord)?;
            budget.add_bytes(8)?;
            Ok(u64::from_be_bytes(bytes))
        }
        _ => Err(Error::CorruptRecord),
    }
}

pub(super) fn integer(row: &Row<'_>, field: usize) -> Result<i64, Error> {
    if type_name(row, field)? != "integer" {
        return Err(Error::CorruptRecord);
    }
    match row
        .get_ref(field * 3 + 2)
        .map_err(|_| Error::CorruptRecord)?
    {
        ValueRef::Integer(n) => Ok(n),
        _ => Err(Error::CorruptRecord),
    }
}

pub(super) fn null(row: &Row<'_>, field: usize) -> Result<(), Error> {
    if type_name(row, field)? != "null"
        || row
            .get_ref(field * 3 + 1)
            .map_err(|_| Error::CorruptRecord)?
            != ValueRef::Null
        || row
            .get_ref(field * 3 + 2)
            .map_err(|_| Error::CorruptRecord)?
            != ValueRef::Null
    {
        return Err(Error::CorruptRecord);
    }
    Ok(())
}

pub(super) fn sql_error(_: rusqlite::Error) -> Error {
    Error::Unavailable
}
