use super::*;

#[test]
fn lexical_depth_boundary_uses_fixed_stack() {
    let valid = format!("{}0{}", "[".repeat(64), "]".repeat(64));
    assert_eq!(json_preflight(valid.as_bytes()), Ok(None));
    let over = format!("{}0{}", "[".repeat(65), "]".repeat(65));
    assert_eq!(
        json_preflight(over.as_bytes()),
        Err(InspectionError::LimitExceeded)
    );
}

#[test]
fn lexical_nodes_count_keys_strings_containers_and_primitives() {
    let valid = format!("[{}]", vec!["0"; MAX_NODES - 1].join(","));
    assert_eq!(json_preflight(valid.as_bytes()), Ok(None));
    let over = format!("[{}]", vec!["0"; MAX_NODES].join(","));
    assert_eq!(
        json_preflight(over.as_bytes()),
        Err(InspectionError::LimitExceeded)
    );
    // Each object consumes a container, a key, and a string value token.
    let keys = format!(
        "[{}]",
        vec![r#"{"key":"value"}"#; MAX_NODES / 3 + 1].join(",")
    );
    assert_eq!(
        json_preflight(keys.as_bytes()),
        Err(InspectionError::LimitExceeded)
    );
    assert_eq!(
        json_preflight(br#"{"schema_version":1,"escaped":"\"[{}]\\"}"#),
        Ok(Some(1))
    );
    assert_eq!(json_preflight(br#"{"schema_vers\u0069on":2}"#), Ok(Some(2)));
}

#[test]
fn bounded_schema_enumerator_n_and_n_plus_one() {
    let connection = Connection::open_in_memory().unwrap();
    let budget = Budget::new();
    configure(&connection, &budget, false).unwrap();
    connection
        .execute_batch("CREATE TABLE one(x INTEGER); CREATE TABLE two(x INTEGER);")
        .unwrap();
    assert_eq!(schema_objects(&connection, &budget, 2).unwrap().len(), 2);
    assert_eq!(
        schema_objects(&connection, &budget, 1).err(),
        Some(InspectionError::LimitExceeded)
    );
    connection.close().unwrap();
}

#[test]
fn progress_interrupt_is_actual_engine_work_and_positive_own_flag() {
    let connection = Connection::open_in_memory().unwrap();
    let budget = Arc::new(Budget {
        start: Instant::now(),
        callbacks: AtomicU64::new(0),
        exhausted: AtomicBool::new(false),
        max_callbacks: 2,
        elapsed: Duration::from_secs(5),
    });
    configure(&connection, &budget, false).unwrap();
    let error = connection.query_row::<i64, _, _>("WITH RECURSIVE work(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM work WHERE x<1000000) SELECT sum(x) FROM work", [], |row| row.get(0)).unwrap_err();
    assert_eq!(
        error.sqlite_error_code(),
        Some(ErrorCode::OperationInterrupted)
    );
    assert!(budget.exhausted.load(Ordering::Relaxed));
    assert_eq!(budget.callbacks.load(Ordering::Relaxed), 2);
    assert_eq!(
        budget.sql(error, InspectionError::CorruptSource),
        InspectionError::LimitExceeded
    );
    connection.close().unwrap();
}

#[test]
fn cumulative_budget_spans_both_connections_and_cpu_phases() {
    let budget = Arc::new(Budget {
        start: Instant::now(),
        callbacks: AtomicU64::new(0),
        exhausted: AtomicBool::new(false),
        max_callbacks: 2000,
        elapsed: Duration::from_secs(5),
    });
    for _ in 0..2 {
        let connection = Connection::open_in_memory().unwrap();
        configure(&connection, &budget, false).unwrap();
        connection.query_row::<i64, _, _>("WITH RECURSIVE work(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM work WHERE x<1000) SELECT sum(x) FROM work", [], |row| row.get(0)).unwrap();
        connection.close().unwrap();
    }
    assert!(budget.callbacks.load(Ordering::Relaxed) > 20);
    let expired = Budget {
        start: Instant::now(),
        callbacks: AtomicU64::new(0),
        exhausted: AtomicBool::new(false),
        max_callbacks: 2000,
        elapsed: Duration::ZERO,
    };
    assert_eq!(expired.check(), Err(InspectionError::LimitExceeded));
    assert!(expired.exhausted.load(Ordering::Relaxed));
}

#[test]
fn configured_engine_rejects_actual_oversized_cell() {
    let connection = Connection::open_in_memory().unwrap();
    let budget = Budget::new();
    configure(&connection, &budget, false).unwrap();
    let error = connection
        .query_row::<Vec<u8>, _, _>("SELECT zeroblob(2097153)", [], |row| row.get(0))
        .unwrap_err();
    assert_eq!(error.sqlite_error_code(), Some(ErrorCode::TooBig));
    assert_eq!(
        budget.sql(error, InspectionError::CorruptSource),
        InspectionError::LimitExceeded
    );
    connection.close().unwrap();
}

#[test]
fn actual_fixed_quick_check_work_hits_cooperative_engine_limit() {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(crate::SCHEMA).unwrap();
    connection.execute_batch("WITH RECURSIVE rows(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM rows WHERE x<20000) INSERT INTO threads(id) SELECT printf('thread-%d',x) FROM rows;").unwrap();
    let budget = Arc::new(Budget {
        start: Instant::now(),
        callbacks: AtomicU64::new(0),
        exhausted: AtomicBool::new(false),
        max_callbacks: 2,
        elapsed: Duration::from_secs(5),
    });
    configure(&connection, &budget, true).unwrap();
    assert_eq!(
        integrity(&connection, &budget),
        Err(InspectionError::LimitExceeded)
    );
    assert!(budget.exhausted.load(Ordering::Relaxed));
    assert_eq!(budget.callbacks.load(Ordering::Relaxed), 2);
    connection.close().unwrap();
}
