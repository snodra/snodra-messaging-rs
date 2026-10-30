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

/// Encodes a handler's reply; a unit reply (`()`, which serde writes as `null`) becomes `{}`.
pub fn encode_reply<T: Serialize>(value: &T) -> Result<Vec<u8>, MessagingError> {
    let bytes = encode(value)?;
    Ok(if bytes == b"null" {
        b"{}".to_vec()
    } else {
        bytes
    })
}

/// Decodes a reply; an empty object also decodes as the unit type `()`.
pub fn decode_reply<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, MessagingError> {
    match decode::<T>(bytes) {
        Ok(v) => Ok(v),
        Err(e) => {
            let trimmed = String::from_utf8_lossy(bytes);
            let trimmed = trimmed.trim();
            if trimmed == "{}" || trimmed.is_empty() {
                decode::<T>(b"null").map_err(|_| e)
            } else {
                Err(e)
            }
        }
    }
}
