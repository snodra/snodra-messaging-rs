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

fn is_unit<T: 'static>() -> bool {
    std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>()
}

/// Encodes a handler's reply; the unit reply `()` (which serde writes as `null`) becomes `{}`, the
/// empty object Jackson writes for commands without a result. Other `null`s (an `Option::None`) stay `null`.
pub fn encode_reply<T: Serialize + 'static>(value: &T) -> Result<Vec<u8>, MessagingError> {
    if is_unit::<T>() {
        return Ok(b"{}".to_vec());
    }
    encode(value)
}

/// Decodes a reply; for the unit type `()` an empty object (or an empty body) is accepted.
pub fn decode_reply<T: DeserializeOwned + 'static>(bytes: &[u8]) -> Result<T, MessagingError> {
    if is_unit::<T>() {
        let trimmed = String::from_utf8_lossy(bytes);
        let trimmed = trimmed.trim();
        if trimmed == "{}" || trimmed.is_empty() {
            return decode::<T>(b"null");
        }
    }
    decode::<T>(bytes)
}
