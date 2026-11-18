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

/// A read request whose result is a flow of `Item`s from one instance of one owning service,
/// under the caller's back-pressure (ADR 0017, P4b: `Request.mode = STREAMING`, credits). Routed
/// through the hub to an instance whose library negotiated `streaming`.
pub trait StreamingQuery:
    MessageType + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Item: Serialize + DeserializeOwned + Send + 'static;
}

/// A read request whose result is an initial value followed by the updates every owner instance
/// pushes, until the subscriber cancels or the emitters complete (ADR 0017, P4c: `Request.mode =
/// SUBSCRIPTION`). Routed through the hub to every instance whose library negotiated
/// `subscription` and lists the type; one of them also answers the initial result.
pub trait SubscriptionQuery:
    MessageType + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Initial: Serialize + DeserializeOwned + Send + 'static;
    type Update: Serialize + DeserializeOwned + Send + 'static;
}

/// A fact that happened; published to the event store (source `snodra.events` by default).
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
