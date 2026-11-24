//! The layout on the wire, shared by every RabbitMQ Streams add-on of a Snodra library: how an
//! event becomes an AMQP 1.0 message, which partition stream it goes to, and how a delivered
//! chunk is read back. See the crate docs for the whole layout.

use std::collections::HashMap;
use std::io::Cursor;

use rabbitmq_stream_protocol::codec::Encoder;
use rabbitmq_stream_protocol::commands::consumer_update::ConsumerUpdateCommand;
use rabbitmq_stream_protocol::commands::deliver::DeliverCommand;
use rabbitmq_stream_protocol::commands::metadata_update::MetadataUpdateCommand;
use rabbitmq_stream_protocol::message::{Message, SimpleValue, Value};
use snodra_messaging::event_store::{Delivery, MAX_SEGMENT_BYTES, OutgoingEvent};

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
/// after `<source>-` (`snodra.events-3` is `3`), else the stream's whole name.
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

/// The first `limit` bytes `command` encodes to (the protocol crate keeps some fields private;
/// their places on the wire are fixed by the protocol). Encoding stops there: a delivery's
/// messages are never encoded or copied for it. `None` when it encodes to fewer.
fn prefix(command: &impl Encoder, limit: usize) -> Option<Vec<u8>> {
    /// Takes `limit` bytes, then refuses (which ends the encoding).
    struct Prefix {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl std::io::Write for Prefix {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let room = self.limit - self.bytes.len();
            if room == 0 {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            let n = room.min(buf.len());
            self.bytes.extend_from_slice(&buf[..n]);
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut out = Prefix {
        bytes: Vec::with_capacity(limit),
        limit,
    };
    let _ = command.encode(&mut out);
    (out.bytes.len() == limit).then_some(out.bytes)
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
/// (2), records (4), timestamp (8, epoch ms), ... `None` when the bytes are not there (the
/// caller fails the subscription rather than wait on it).
pub fn chunk_header(deliver: &DeliverCommand) -> Option<ChunkHeader> {
    let bytes = prefix(deliver, 17)?;
    let timestamp = u64::from_be_bytes(bytes.get(9..17)?.try_into().ok()?);
    Some(ChunkHeader {
        subscription_id: deliver.subscription_id,
        first_offset: deliver.chunk_first_offset,
        timestamp_ms: timestamp as i64,
    })
}

/// A consumer update's correlation id, subscription id and whether it activates; only the
/// subscription id is read from the bytes: correlation id (4 bytes), subscription id (1).
pub fn consumer_update(update: &ConsumerUpdateCommand) -> Option<(u32, u8, bool)> {
    let bytes = prefix(update, 5)?;
    Some((
        update.get_correlation_id(),
        *bytes.get(4)?,
        update.is_active() == 1,
    ))
}

/// The stream a metadata update is about: code (2 bytes), then the stream's name (a 2-byte
/// length and its bytes); `None` when the bytes are not there.
pub fn metadata_update_stream(update: &MetadataUpdateCommand) -> Option<String> {
    let mut bytes = Vec::new();
    update.encode(&mut bytes).ok()?;
    let len = usize::try_from(i16::from_be_bytes([*bytes.get(2)?, *bytes.get(3)?])).ok()?;
    Some(String::from_utf8_lossy(bytes.get(4..4 + len)?).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rabbitmq_stream_protocol::codec::Decoder;

    fn event(key: Option<&str>) -> OutgoingEvent {
        OutgoingEvent {
            source: "snodra.events".into(),
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

    /// The partition table (murmur3 x86_32, seed 104729, the hash as an
    /// unsigned number modulo 6 partitions; a signed modulo is wrong for `order-7`, whose hash
    /// has its top bit set).
    #[test]
    fn keys_hash_to_the_partition_the_stream_clients_pick() {
        for (key, hash, partition) in [
            ("order-1", 0x3163_ba7a_u32, 0),
            ("order-2", 0x4aa7_bf62, 2),
            ("customer-123", 0x6fab_e837, 5),
            ("order-7", 0x813d_1adb, 3),
            ("", 0xc675_7d56, 4),
        ] {
            let h = murmur3::murmur3_32(&mut Cursor::new(key.as_bytes()), HASH_SEED).unwrap();
            assert_eq!(h, hash, "the hash of {key:?}");
            assert_eq!(
                partition_index(key, 6),
                partition,
                "the partition of {key:?}"
            );
        }
        assert_ne!(
            (0x813d_1adb_u32 as i32 % 6).unsigned_abs() as usize,
            partition_index("order-7", 6),
            "a signed modulo gives another partition"
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
        assert_eq!(segment_of("snodra.events", "snodra.events-0"), "0");
        assert_eq!(segment_of("snodra.events", "snodra.events-11"), "11");
        assert_eq!(segment_of("snodra.events", "snodra.events-eu"), "eu");
        assert_eq!(segment_of("snodra.events", "other-3"), "other-3");
        assert_eq!(
            segment_of("snodra.events", "snodra.events-"),
            "snodra.events-"
        );
        assert!(segment_fits("3") && !segment_fits("") && !segment_fits(&"9".repeat(129)));
    }

    #[test]
    fn the_private_fields_are_read_from_their_places_on_the_wire() {
        let one = Message::builder().body(b"{}".to_vec()).build();
        let deliver = DeliverCommand::new(4, 0, 0, 1, 1_700_000_123_456, 1, 40, 0, 0, 0, vec![one]);
        assert_eq!(
            chunk_header(&deliver),
            Some(ChunkHeader {
                subscription_id: 4,
                first_offset: 40,
                timestamp_ms: 1_700_000_123_456
            })
        );
        let update = ConsumerUpdateCommand::new(77, 9, 1);
        assert_eq!(consumer_update(&update), Some((77, 9, true)));
        let update = ConsumerUpdateCommand::new(78, 2, 0);
        assert_eq!(consumer_update(&update), Some((78, 2, false)));
    }

    /// A metadata update as the broker sends it (code, then the stream's name), decoded by the
    /// protocol crate, gives its stream back.
    #[test]
    fn a_metadata_update_names_its_stream() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&6u16.to_be_bytes()); // stream not available
        frame.extend_from_slice(&(15i16).to_be_bytes());
        frame.extend_from_slice(b"snodra.events-3");
        let (_, update) = MetadataUpdateCommand::decode(&frame).expect("a metadata update");
        assert_eq!(
            metadata_update_stream(&update).as_deref(),
            Some("snodra.events-3")
        );
    }

    /// Fewer bytes than a field's place: `None`, never a panic.
    #[test]
    fn a_short_encoding_reads_as_none() {
        struct Short;
        impl Encoder for Short {
            fn encoded_size(&self) -> u32 {
                3
            }
            fn encode(
                &self,
                writer: &mut impl std::io::Write,
            ) -> Result<(), rabbitmq_stream_protocol::error::EncodeError> {
                writer.write_all(&[1, 2, 3])?;
                Ok(())
            }
        }
        assert_eq!(prefix(&Short, 17), None);
        assert_eq!(prefix(&Short, 2), Some(vec![1, 2]));
    }
}

/// The encoded message, section by section, with the AMQP type codes of what Snodra's layout
/// fixes.
#[cfg(test)]
mod golden {
    use super::*;

    /// One AMQP value: its type code and its body (a string's bytes, a list's or map's
    /// elements).
    #[derive(Debug, Clone, PartialEq)]
    enum V {
        Null,
        Str(Vec<u8>),
        Sym(Vec<u8>),
        Bin(Vec<u8>),
        List(Vec<V>),
        Map(Vec<(V, V)>),
        Other(u8),
    }

    fn take<'a>(b: &mut &'a [u8], n: usize) -> &'a [u8] {
        let (head, rest) = b.split_at(n);
        *b = rest;
        head
    }

    fn u32_at(b: &mut &[u8]) -> usize {
        u32::from_be_bytes(take(b, 4).try_into().unwrap()) as usize
    }

    fn value(b: &mut &[u8]) -> V {
        let code = take(b, 1)[0];
        match code {
            0x40 => V::Null,
            0xa1 => {
                let n = take(b, 1)[0] as usize;
                V::Str(take(b, n).to_vec())
            }
            0xb1 => {
                let n = u32_at(b);
                V::Str(take(b, n).to_vec())
            }
            0xa3 => {
                let n = take(b, 1)[0] as usize;
                V::Sym(take(b, n).to_vec())
            }
            0xb3 => {
                let n = u32_at(b);
                V::Sym(take(b, n).to_vec())
            }
            0xa0 => {
                let n = take(b, 1)[0] as usize;
                V::Bin(take(b, n).to_vec())
            }
            0xb0 => {
                let n = u32_at(b);
                V::Bin(take(b, n).to_vec())
            }
            0x45 => V::List(vec![]),
            0xc0 | 0xc1 | 0xd0 | 0xd1 => {
                let (size, count) = if code < 0xd0 {
                    (take(b, 1)[0] as usize - 1, take(b, 1)[0] as usize)
                } else {
                    (u32_at(b) - 4, u32_at(b))
                };
                let mut inner = take(b, size);
                let items: Vec<V> = (0..count).map(|_| value(&mut inner)).collect();
                if code == 0xc1 || code == 0xd1 {
                    V::Map(
                        items
                            .chunks(2)
                            .map(|kv| (kv[0].clone(), kv[1].clone()))
                            .collect(),
                    )
                } else {
                    V::List(items)
                }
            }
            0x83 => {
                take(b, 8);
                V::Other(code)
            }
            other => V::Other(other),
        }
    }

    /// The sections of an encoded message: (descriptor code, value).
    fn sections(mut b: &[u8]) -> Vec<(u8, V)> {
        let mut all = Vec::new();
        while !b.is_empty() {
            assert_eq!(take(&mut b, 2), [0x00, 0x53], "a small-ulong descriptor");
            let code = take(&mut b, 1)[0];
            all.push((code, value(&mut b)));
        }
        all
    }

    #[test]
    fn the_message_has_the_types_of_the_wire_layout() {
        let event = OutgoingEvent {
            source: "snodra.events".into(),
            id: "e-42".into(),
            type_name: "orders.Placed".into(),
            key: Some("order-42".into()),
            headers: HashMap::from([
                ("type".to_string(), "orders.Placed".to_string()),
                ("id".to_string(), "e-42".to_string()),
            ]),
            payload: br#"{"total":42}"#.to_vec(),
        };
        let mut bytes = Vec::new();
        message(&event, 1).encode(&mut bytes).unwrap();
        let sections = sections(&bytes);
        let of = |code: u8| -> Vec<&V> {
            sections
                .iter()
                .filter(|(c, _)| *c == code)
                .map(|(_, v)| v)
                .collect()
        };
        // Properties (0x73): message-id a string, content-type the symbol application/json.
        let V::List(properties) = of(0x73)[0] else {
            panic!("properties: {sections:?}")
        };
        assert_eq!(properties[0], V::Str(b"e-42".to_vec()), "message-id");
        assert_eq!(
            properties[6],
            V::Sym(b"application/json".to_vec()),
            "content-type"
        );
        // Message annotations (0x72): x-routing-key, a symbol, with a string value.
        let V::Map(annotations) = of(0x72)[0] else {
            panic!("annotations: {sections:?}")
        };
        assert_eq!(
            annotations,
            &vec![(
                V::Sym(b"x-routing-key".to_vec()),
                V::Str(b"order-42".to_vec())
            )]
        );
        // Application properties (0x74): string keys, string values.
        let V::Map(headers) = of(0x74)[0] else {
            panic!("application properties: {sections:?}")
        };
        assert_eq!(headers.len(), 2);
        for (k, v) in headers {
            assert!(matches!((k, v), (V::Str(_), V::Str(_))), "{k:?} = {v:?}");
        }
        // Exactly one data section (0x75), the payload; no amqp-value.
        assert_eq!(of(0x75), vec![&V::Bin(br#"{"total":42}"#.to_vec())]);
        assert!(of(0x77).is_empty());
    }

    #[test]
    fn an_unkeyed_event_has_no_annotation() {
        let event = OutgoingEvent {
            source: "s".into(),
            id: "e-1".into(),
            type_name: "t".into(),
            key: None,
            headers: HashMap::new(),
            payload: b"{}".to_vec(),
        };
        let mut bytes = Vec::new();
        message(&event, 1).encode(&mut bytes).unwrap();
        assert!(sections(&bytes).iter().all(|(code, _)| *code != 0x72));
    }

    /// A chunk with sub-entries (batched or compressed by another producer), as the broker
    /// delivers it: the protocol crate's decoder, which the client runs on its connection's
    /// read task before any handler, cannot read it (it panics there, and the connection stops
    /// reading). Nothing of the add-on runs first, so its guard is a liveness check of the
    /// connection (the subscription's status refresh) and the deadline of every read.
    #[test]
    fn a_chunk_with_sub_entries_cannot_be_decoded_by_the_client() {
        use rabbitmq_stream_protocol::Response;
        use rabbitmq_stream_protocol::codec::Decoder;
        let mut body = vec![1u8, 0, 0]; // subscription, magic, chunk type (user)
        body.extend_from_slice(&1u16.to_be_bytes()); // one entry...
        body.extend_from_slice(&5u32.to_be_bytes()); // ...holding five records
        body.extend_from_slice(&1_700_000_000_000u64.to_be_bytes());
        body.extend_from_slice(&1u64.to_be_bytes()); // epoch
        body.extend_from_slice(&0u64.to_be_bytes()); // first offset
        body.extend_from_slice(&0i32.to_be_bytes()); // crc
        body.extend_from_slice(&16u32.to_be_bytes()); // data length
        body.extend_from_slice(&0u32.to_be_bytes()); // trailer
        body.extend_from_slice(&0u32.to_be_bytes()); // reserved
        // A sub-entry: the type bit set, gzip, 5 records, lengths.
        body.extend_from_slice(&[0x80 | 0x10, 0, 5, 0, 0, 0, 100, 0, 0, 0, 6]);
        body.extend_from_slice(&[0; 6]);
        let mut frame = Vec::new();
        frame.extend_from_slice(&((4 + body.len()) as u32).to_be_bytes());
        frame.extend_from_slice(&0x0008u16.to_be_bytes()); // deliver
        frame.extend_from_slice(&1u16.to_be_bytes());
        frame.extend_from_slice(&body);
        let decoded = std::panic::catch_unwind(|| Response::decode(&frame).is_ok());
        // Today a capacity overflow (the entry header read as a negative length).
        assert!(
            decoded.is_err(),
            "the client reads sub-entries now ({decoded:?}): revisit the guard"
        );
    }
}
