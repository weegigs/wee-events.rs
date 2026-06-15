use serde::{Deserialize, Serialize};
use wee_events::{
    Aggregate, AggregateId, DecodeError, Encoding, EventData, EventDecoder, EventEncoder, EventId,
    EventMetadata, EventPattern, EventType, RecordedEvent, RenderError, Renderer, Revision,
    encoding::json,
};

#[cfg(feature = "cbor")]
use wee_events::encoding::cbor;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Payload {
    amount: i64,
}

#[test]
fn json_encoder_and_decoder_round_trip_event_data() {
    let data = json::Encoder
        .serialize(&Payload { amount: 7 })
        .expect("json encode should succeed");

    assert_eq!(data.encoding, Encoding::Json);

    let decoded: Payload = json::Decoder
        .deserialize(&data)
        .expect("json decode should succeed");
    assert_eq!(decoded, Payload { amount: 7 });
}

#[cfg(feature = "cbor")]
#[test]
fn cbor_encoder_and_decoder_round_trip_event_data() {
    let data = cbor::Encoder
        .serialize(&Payload { amount: 9 })
        .expect("cbor encode should succeed");

    assert_eq!(data.encoding, Encoding::Cbor);
    assert_ne!(data.encoding, Encoding::Json);

    let decoded: Payload = cbor::Decoder
        .deserialize(&data)
        .expect("cbor decode should succeed");
    assert_eq!(decoded, Payload { amount: 9 });
}

#[cfg(feature = "cbor")]
#[test]
fn encoding_dispatch_selects_decoder_by_event_data_encoding() {
    let data = cbor::Encoder
        .serialize(&Payload { amount: 11 })
        .expect("cbor encode should succeed");

    let decoded: Payload = data
        .encoding
        .decode(&data)
        .expect("dispatch should find cbor decoder");

    assert_eq!(decoded, Payload { amount: 11 });
}

#[test]
fn encoding_dispatch_reports_unknown_encoding() {
    let error = Encoding::from_encoding_str("application/x-custom")
        .expect_err("unknown encoding should fail");

    assert!(matches!(
        error,
        DecodeError::UnknownEncoding { encoding } if encoding == "application/x-custom"
    ));
}

#[test]
fn event_data_json_helpers_preserve_existing_behavior() {
    let data = EventData::json(&Payload { amount: 13 }).expect("json helper should encode");
    let decoded: Payload = data.deserialize_json().expect("json helper should decode");

    assert_eq!(decoded, Payload { amount: 13 });
}

#[cfg(feature = "cbor")]
#[test]
fn renderer_can_decode_events_from_event_data_encoding() {
    #[derive(Default)]
    struct State {
        amount: i64,
    }

    fn reduce(
        state: &mut State,
        _event_type: &EventType,
        data: &EventData,
    ) -> Result<(), DecodeError> {
        let payload: Payload = data.encoding.decode(data)?;
        state.amount += payload.amount;
        Ok(())
    }

    let data = cbor::Encoder
        .serialize(&Payload { amount: 21 })
        .expect("cbor encode should succeed");
    let aggregate = Aggregate::from_events(
        AggregateId::new("counter", "codec-aware"),
        vec![RecordedEvent {
            event_id: EventId::new("event-1"),
            event_type: EventType::new("counter:incremented"),
            revision: Revision::generate(),
            metadata: EventMetadata::default(),
            data,
        }],
    );
    let renderer = Renderer::new().with("counter:incremented", reduce);

    let entity = renderer
        .render(aggregate)
        .expect("renderer should decode by encoding");

    assert_eq!(entity.state.amount, 21);
}

#[test]
fn renderer_errors_on_unhandled_event_type() {
    #[derive(Debug, Default)]
    struct State;

    let aggregate = Aggregate::from_events(
        AggregateId::new("counter", "strict"),
        vec![RecordedEvent {
            event_id: EventId::new("event-1"),
            event_type: EventType::new("counter:unknown"),
            revision: Revision::generate(),
            metadata: EventMetadata::default(),
            data: EventData::json(&serde_json::json!({})).expect("json encode should succeed"),
        }],
    );
    let renderer = Renderer::<State>::new();

    let error = renderer
        .render(aggregate)
        .expect_err("unhandled event types should fail entity rendering");

    assert!(matches!(
        error,
        RenderError::UnhandledEventType { context } if context.event_type.as_str() == "counter:unknown"
    ));
}

#[test]
fn renderer_can_explicitly_ignore_globbed_event_types() {
    #[derive(Default)]
    struct State;

    let aggregate = Aggregate::from_events(
        AggregateId::new("counter", "strict"),
        vec![RecordedEvent {
            event_id: EventId::new("event-1"),
            event_type: EventType::new("counter:legacy-reset"),
            revision: Revision::generate(),
            metadata: EventMetadata::default(),
            data: EventData::json(&serde_json::json!({})).expect("json encode should succeed"),
        }],
    );
    let renderer = Renderer::<State>::new().ignore(EventPattern::glob("counter:legacy-*").unwrap());

    renderer
        .render(aggregate)
        .expect("explicitly ignored event types should not fail");
}

#[test]
fn renderer_can_reduce_globbed_event_types() {
    #[derive(Default)]
    struct State {
        count: u32,
    }

    #[allow(clippy::unnecessary_wraps)]
    fn reduce(
        state: &mut State,
        _event_type: &EventType,
        _data: &EventData,
    ) -> Result<(), DecodeError> {
        state.count += 1;
        Ok(())
    }

    let aggregate = Aggregate::from_events(
        AggregateId::new("counter", "strict"),
        vec![RecordedEvent {
            event_id: EventId::new("event-1"),
            event_type: EventType::new("counter:legacy-reset"),
            revision: Revision::generate(),
            metadata: EventMetadata::default(),
            data: EventData::json(&serde_json::json!({})).expect("json encode should succeed"),
        }],
    );
    let renderer = Renderer::new().with(EventPattern::glob("counter:legacy-*").unwrap(), reduce);

    let entity = renderer
        .render(aggregate)
        .expect("globbed event types should be reducible");

    assert_eq!(entity.state.count, 1);
}
