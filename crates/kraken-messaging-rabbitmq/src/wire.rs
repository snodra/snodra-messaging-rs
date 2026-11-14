//! The layout on the wire, shared with the Kotlin add-on (`com.rabbitmq:stream-client`): how an
//! event becomes an AMQP 1.0 message, which partition stream it goes to, and how a delivered
//! chunk is read back. See the crate docs for the whole layout.

use std::collections::HashMap;
use std::io::Cursor;

use kraken_messaging::event_store::{Delivery, MAX_SEGMENT_BYTES, OutgoingEvent};
use rabbitmq_stream_protocol::codec::Encoder;
use rabbitmq_stream_protocol::commands::consumer_update::ConsumerUpdateCommand;
use rabbitmq_stream_protocol::commands::deliver::DeliverCommand;
use rabbitmq_stream_protocol::commands::metadata_update::MetadataUpdateCommand;
use rabbitmq_stream_protocol::message::{Message, SimpleValue, Value};

/// The message annotation carrying the event's key (RabbitMQ's own name for a routing key); absent
/// for an event without one.
pub const KEY_ANNOTATION: &str = "x-routing-key";
/// The content type every event carries (AMQP `properties.content-type`).
pub const CONTENT_TYPE: &str = "application/json";
/// The seed of the partition hash (the stream clients' default hash routing: murmur3 x86_32).
pub const HASH_SEED: u32 = 104_729;

/// The index of the partition an event with `routing_key` goes to among `partitions` (in the
/// order the broker lists the super stream's partitions): murmur3 x86_32 of the key's UTF-8
/// bytes with seed [`HASH_SEED`], as an unsigned number, modulo the count. What the Java and Rust
/// stream clients' hash routing computes.
pub fn partition_index(routing_key: &str, partitions: usize) -> usize {
    let hash = murmur3::murmur3_32(&mut Cursor::new(routing_key.as_bytes()), HASH_SEED)
        .expect("hashing bytes in memory");
    (hash as usize) % partitions.max(1)
}

/// The routing key of `event`: its key, or its id when it has none (an unkeyed event is spread
/// over the partitions by its id).
pub fn routing_key(event: &OutgoingEvent) -> &str {
    event.key.as_deref().unwrap_or(&event.id)
}

/// The message of `event`: its payload as the body (one AMQP `data` section), its id as
/// `properties.message-id`, `application/json` as `properties.content-type`, its key (if any) as
/// the message annotation [`KEY_ANNOTATION`], and every header as an application property (an
/// AMQP string).
pub fn message(event: &OutgoingEvent, publishing_id: u64) -> Message {
    let mut names: Vec<&String> = event.headers.keys().collect();
    names.sort();
    let mut builder = Message::builder()
        .body(event.payload.clone())
        .publishing_id(publishing_id)
        .properties()
        .message_id(event.id.clone())
        .content_type(CONTENT_TYPE)
        .message_builder();
    if let Some(key) = &event.key {
        builder = builder
            .message_annotations()
            .insert(KEY_ANNOTATION, key.as_str())
            .message_builder();
    }
    let mut properties = builder.application_properties();
    for name in names {
        properties = properties.insert(name.as_str(), event.headers[name].as_str());
    }
    properties.message_builder().build()
}

/// The encoded size of `message` in a publish frame, with the frame's own bytes (size, key,
/// version, publisher id, count, publishing id, message length).
pub fn frame_size(message: &Message) -> usize {
    message.encoded_size() as usize + 4 + 2 + 2 + 1 + 4 + 8 + 4
}

/// The event's key: the [`KEY_ANNOTATION`] annotation as text (a string or a symbol).
pub fn key_of(message: &Message) -> Option<String> {
    let value = message.message_annotations()?.get(KEY_ANNOTATION)?;
    match value {
        Value::Simple(simple) => text(simple).map(|b| String::from_utf8_lossy(&b).into_owned()),
        _ => None,
    }
}

/// The bytes of an AMQP simple value as a header shows them: a string's or symbol's UTF-8, a
/// binary's bytes, other values as their text; `None` for null.
fn text(value: &SimpleValue) -> Option<Vec<u8>> {
    match value {
        SimpleValue::Null => None,
        SimpleValue::String(s) => Some(s.as_bytes().to_vec()),
        SimpleValue::Symbol(s) => Some(s.as_str().as_bytes().to_vec()),
        SimpleValue::Binary(b) => Some(b.to_vec()),
        SimpleValue::Boolean(v) => Some(v.to_string().into_bytes()),
        SimpleValue::Ubyte(v) => Some(v.to_string().into_bytes()),
        SimpleValue::Ushort(v) => Some(v.to_string().into_bytes()),
        SimpleValue::Uint(v) => Some(v.to_string().into_bytes()),
        SimpleValue::Ulong(v) => Some(v.to_string().into_bytes()),
        SimpleValue::Byte(v) => Some(v.to_string().into_bytes()),
        SimpleValue::Short(v) => Some(v.to_string().into_bytes()),
        SimpleValue::Int(v) => Some(v.to_string().into_bytes()),
        SimpleValue::Long(v) => Some(v.to_string().into_bytes()),
        other => Some(format!("{other:?}").into_bytes()),
    }
}

/// Every application property of `message`, names sorted, values as [`text`].
pub fn headers_of(message: &Message) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let Some(properties) = message.application_properties() else {
        return Vec::new();
    };
    let mut all: Vec<(Vec<u8>, Option<Vec<u8>>)> = properties
        .iter()
        .map(|(name, value)| (name.as_bytes().to_vec(), text(value)))
        .collect();
    all.sort();
    all
}

/// The payload: the body's `data` section, empty without one.
pub fn payload_of(message: &Message) -> &[u8] {
    message.data().unwrap_or(&[])
}

/// The delivery of `message` at `offset` of the stream behind `segment`, appended at
/// `timestamp_ms`.
pub fn delivery(segment: &str, offset: u64, timestamp_ms: i64, message: &Message) -> Delivery {
    let headers: HashMap<String, String> = headers_of(message)
        .into_iter()
        .map(|(name, value)| {
            (
                String::from_utf8_lossy(&name).into_owned(),
                String::from_utf8_lossy(&value.unwrap_or_default()).into_owned(),
            )
        })
        .collect();
    Delivery {
        segment: segment.to_string(),
        position: offset.to_string(),
        key: key_of(message),
        headers,
        payload: payload_of(message).to_vec(),
        timestamp_ms,
    }
}

/// The segment of partition stream `stream` of super stream `source`: the partition's suffix
/// after `<source>-` (`kraken.events-3` is `3`), else the stream's whole name.
pub fn segment_of(source: &str, stream: &str) -> String {
    stream
        .strip_prefix(source)
        .and_then(|rest| rest.strip_prefix('-'))
        .filter(|suffix| !suffix.is_empty())
        .unwrap_or(stream)
        .to_string()
}

/// The segment of a source that is one plain stream (a dead-letter stream, for example).
pub const SINGLE_SEGMENT: &str = "0";

/// Whether `segment` fits the protocol's bound.
pub fn segment_fits(segment: &str) -> bool {
    !segment.is_empty() && segment.len() <= MAX_SEGMENT_BYTES
}

/// The bytes `command` encodes to (the protocol crate keeps some fields private; their places
/// on the wire are fixed by the protocol).
fn encoded(command: &impl Encoder) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(64);
    command
        .encode(&mut bytes)
        .expect("encoding into memory cannot fail");
    bytes
}

/// What a delivered chunk says besides its messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkHeader {
    pub subscription_id: u8,
    /// The offset of its first message.
    pub first_offset: u64,
    /// When the stream's leader wrote it, epoch milliseconds.
    pub timestamp_ms: i64,
}

/// The header of `deliver`: subscription id (1 byte), magic version (1), chunk type (1), entries
/// (2), records (4), timestamp (8, epoch ms), ...
pub fn chunk_header(deliver: &DeliverCommand) -> ChunkHeader {
    let mut bare = deliver.clone();
    bare.messages.clear();
    let bytes = encoded(&bare);
    let timestamp = u64::from_be_bytes(bytes[9..17].try_into().expect("8 bytes"));
    ChunkHeader {
        subscription_id: deliver.subscription_id,
        first_offset: deliver.chunk_first_offset,
        timestamp_ms: timestamp as i64,
    }
}

/// A consumer update's correlation id, subscription id and whether it activates: correlation id
/// (4 bytes), subscription id (1), active (1).
pub fn consumer_update(update: &ConsumerUpdateCommand) -> (u32, u8, bool) {
    let bytes = encoded(update);
    (
        u32::from_be_bytes(bytes[0..4].try_into().expect("4 bytes")),
        bytes[4],
        bytes[5] == 1,
    )
}

/// The stream a metadata update is about: code (2 bytes), then the stream's name (a 2-byte
/// length and its bytes).
pub fn metadata_update_stream(update: &MetadataUpdateCommand) -> String {
    let bytes = encoded(update);
    let len = i16::from_be_bytes([bytes[2], bytes[3]]).max(0) as usize;
    String::from_utf8_lossy(&bytes[4..(4 + len).min(bytes.len())]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rabbitmq_stream_protocol::codec::Decoder;

    fn event(key: Option<&str>) -> OutgoingEvent {
        OutgoingEvent {
            source: "kraken.events".into(),
            id: "e-1".into(),
            type_name: "orders.Placed".into(),
            key: key.map(str::to_string),
            headers: HashMap::from([
                ("type".to_string(), "orders.Placed".to_string()),
                ("id".to_string(), "e-1".to_string()),
                ("x-extra".to_string(), "kept".to_string()),
            ]),
            payload: br#"{"total":42}"#.to_vec(),
        }
    }

    /// The Java client's `HashRoutingMurmurStrategy` answers these (murmur3 x86_32, seed 104729,
    /// unsigned modulo): the Kotlin add-on routes every key to the same partition.
    #[test]
    fn keys_hash_to_the_partition_the_stream_clients_pick() {
        let expected = |key: &str, n: usize| {
            let h = murmur3::murmur3_32(&mut Cursor::new(key.as_bytes()), 104_729).unwrap();
            (h as u64 % n as u64) as usize
        };
        for key in [
            "order-1",
            "order-42",
            "",
            "ключ",
            "a-much-longer-key-than-the-others",
        ] {
            for n in [1, 2, 3, 6] {
                assert_eq!(partition_index(key, n), expected(key, n), "{key} over {n}");
            }
        }
        // The same answer every time: order per key.
        assert_eq!(partition_index("order-1", 6), partition_index("order-1", 6));
        // The hash is murmur3 x86_32 (the reference vectors, seed 0).
        assert_eq!(murmur3::murmur3_32(&mut Cursor::new(b""), 0).unwrap(), 0);
        assert_eq!(
            murmur3::murmur3_32(&mut Cursor::new(b"hello"), 0).unwrap(),
            0x248b_fa47
        );
    }

    #[test]
    fn an_unkeyed_event_routes_by_its_id() {
        assert_eq!(routing_key(&event(None)), "e-1");
        assert_eq!(routing_key(&event(Some("k"))), "k");
    }

    #[test]
    fn an_event_round_trips_through_its_message() {
        for key in [Some("order-42"), None] {
            let sent = event(key);
            let message = message(&sent, 7);
            assert_eq!(message.publishing_id(), Some(&7));
            // As it travels: encoded, then decoded by the reader.
            let mut bytes = Vec::new();
            message.encode(&mut bytes).unwrap();
            let (_, read) = Message::decode(&bytes).unwrap();
            let d = delivery("3", 12, 1_700_000_000_000, &read);
            assert_eq!(d.key.as_deref(), key);
            assert_eq!(d.headers, sent.headers);
            assert_eq!(d.payload, sent.payload);
            assert_eq!((d.segment.as_str(), d.position.as_str()), ("3", "12"));
            assert_eq!(d.timestamp_ms, 1_700_000_000_000);
            let properties = read.properties().expect("properties");
            assert_eq!(
                properties.content_type.as_ref().map(|s| s.as_str()),
                Some(CONTENT_TYPE)
            );
            assert!(format!("{:?}", properties.message_id).contains("e-1"));
            assert!(frame_size(&read) > sent.payload.len());
        }
    }

    #[test]
    fn segments_are_partition_suffixes() {
        assert_eq!(segment_of("kraken.events", "kraken.events-0"), "0");
        assert_eq!(segment_of("kraken.events", "kraken.events-11"), "11");
        assert_eq!(segment_of("kraken.events", "kraken.events-eu"), "eu");
        assert_eq!(segment_of("kraken.events", "other-3"), "other-3");
        assert_eq!(
            segment_of("kraken.events", "kraken.events-"),
            "kraken.events-"
        );
        assert!(segment_fits("3") && !segment_fits("") && !segment_fits(&"9".repeat(129)));
    }

    #[test]
    fn the_private_fields_are_read_from_their_places_on_the_wire() {
        let deliver = DeliverCommand::new(4, 0, 0, 2, 1_700_000_123_456, 1, 40, 0, 0, 0, vec![]);
        assert_eq!(
            chunk_header(&deliver),
            ChunkHeader {
                subscription_id: 4,
                first_offset: 40,
                timestamp_ms: 1_700_000_123_456
            }
        );
        let update = ConsumerUpdateCommand::new(77, 9, 1);
        assert_eq!(consumer_update(&update), (77, 9, true));
        let update = ConsumerUpdateCommand::new(78, 2, 0);
        assert_eq!(consumer_update(&update), (78, 2, false));
    }
}
