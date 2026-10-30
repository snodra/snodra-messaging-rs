//! JSON on the wire: UTF-8 `serde_json` documents, timestamps as RFC 3339 (what Jackson writes).

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::MessagingError;

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, MessagingError> {
    Ok(serde_json::to_vec(value)?)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, MessagingError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// True when `bytes` parse as a JSON object (the only payload shape the consumer accepts).
pub fn is_json_object(bytes: &[u8]) -> bool {
    matches!(
        serde_json::from_slice::<serde_json::Value>(bytes),
        Ok(serde_json::Value::Object(_))
    )
}
