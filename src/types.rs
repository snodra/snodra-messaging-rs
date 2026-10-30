//! Message traits. Every message type names its wire type explicitly with [`message_type!`].

use serde::Serialize;
use serde::de::DeserializeOwned;

/// Stable wire type name, for example `examples.SendGreeting`. Shared with the Kotlin side through
/// the `@MessageType` annotation there.
pub trait MessageType {
    const TYPE: &'static str;
}

/// An intent handled by exactly one service; routed through the hub. `Reply` is the response type
/// (`()` for commands without a result, encoded as `{}`).
pub trait Command: MessageType + Serialize + DeserializeOwned + Send + Sync + 'static {
    type Reply: Serialize + DeserializeOwned + Send + 'static;
}

/// A read request handled by one service; routed through the hub.
pub trait Query: MessageType + Serialize + DeserializeOwned + Send + Sync + 'static {
    type Reply: Serialize + DeserializeOwned + Send + 'static;
}

/// A fact that happened; published to Kafka topic `kraken.events`.
pub trait Event: MessageType + Serialize + DeserializeOwned + Send + Sync + 'static {}

/// Declares the wire type name of a message struct.
///
/// ```ignore
/// message_type!(SendGreeting, "examples.SendGreeting");
/// ```
#[macro_export]
macro_rules! message_type {
    ($t:ty, $name:literal) => {
        impl $crate::MessageType for $t {
            const TYPE: &'static str = $name;
        }
    };
}
