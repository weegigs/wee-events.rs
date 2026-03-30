use std::str::FromStr;
use wee_events::{AggregateId, AggregateIdParseError};

#[test]
fn display_format() {
    let id = AggregateId::new("campaign", "c1");
    assert_eq!(id.to_string(), "campaign:c1");
}

#[test]
fn parse_simple() {
    let id: AggregateId = "campaign:c1".parse().unwrap();
    assert_eq!(id.aggregate_type().as_str(), "campaign");
    assert_eq!(id.aggregate_key(), "c1");
}

#[test]
fn display_parse_round_trip() {
    let original = AggregateId::new("campaign", "c1");
    let parsed: AggregateId = original.to_string().parse().unwrap();
    assert_eq!(original, parsed);
}

#[test]
fn key_with_colons_round_trips() {
    let original = AggregateId::new("campaign", "run:01ABC:phase-2");
    assert_eq!(original.to_string(), "campaign:run:01ABC:phase-2");

    let parsed: AggregateId = original.to_string().parse().unwrap();
    assert_eq!(parsed.aggregate_type().as_str(), "campaign");
    assert_eq!(parsed.aggregate_key(), "run:01ABC:phase-2");
    assert_eq!(original, parsed);
}

#[test]
fn key_as_urn_round_trips() {
    let original = AggregateId::new("character", "urn:uuid:550e8400-e29b-41d4-a716-446655440000");
    let parsed: AggregateId = original.to_string().parse().unwrap();
    assert_eq!(parsed.aggregate_type().as_str(), "character");
    assert_eq!(
        parsed.aggregate_key(),
        "urn:uuid:550e8400-e29b-41d4-a716-446655440000"
    );
    assert_eq!(original, parsed);
}

#[test]
fn try_from_str() {
    let id = AggregateId::try_from("counter:my-counter").unwrap();
    assert_eq!(id.aggregate_type().as_str(), "counter");
    assert_eq!(id.aggregate_key(), "my-counter");
}

#[test]
fn try_from_string() {
    let id = AggregateId::try_from(String::from("counter:my-counter")).unwrap();
    assert_eq!(id.aggregate_type().as_str(), "counter");
    assert_eq!(id.aggregate_key(), "my-counter");
}

#[test]
fn try_from_invalid_returns_error() {
    let err = AggregateId::try_from("no-colon").unwrap_err();
    assert_eq!(err, AggregateIdParseError::MissingColon);
}

#[test]
fn parse_missing_colon() {
    let err = AggregateId::from_str("no-colon-here").unwrap_err();
    assert_eq!(err, AggregateIdParseError::MissingColon);
}

#[test]
fn parse_empty_type() {
    let err = AggregateId::from_str(":key-only").unwrap_err();
    assert_eq!(err, AggregateIdParseError::EmptyType);
}

#[test]
fn parse_empty_key() {
    let err = AggregateId::from_str("type-only:").unwrap_err();
    assert_eq!(err, AggregateIdParseError::EmptyKey);
}
