use b4_http::parse_json;

#[test]
fn duplicates_are_rejected_at_every_depth() {
    assert_eq!(
        parse_json(br#"{"input":{"a":1,"a":2}}"#),
        Err("invalid_json")
    );
    assert_eq!(
        parse_json(br#"{"input":[{"a":1,"a":2}]}"#),
        Err("invalid_json")
    );
}

#[test]
fn body_and_nesting_are_bounded() {
    assert_eq!(parse_json(&vec![b' '; 65_537]), Err("body_too_large"));
    let json = format!("{}0{}", "[".repeat(65), "]".repeat(65));
    assert_eq!(parse_json(json.as_bytes()), Err("invalid_json"));
    let json = format!("{}0{}", "[".repeat(64), "]".repeat(64));
    assert!(parse_json(json.as_bytes()).is_ok());
}
