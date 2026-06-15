use serde::{Serialize, de::DeserializeOwned};

use super::{DecodeError, EncodeError, Encoding, EventDecoder, EventEncoder};
use crate::event::EventData;

pub const ENCODING: &str = "application/json";

#[derive(Debug, Clone, Copy, Default)]
pub struct Encoder;

#[derive(Debug, Clone, Copy, Default)]
pub struct Decoder;

impl EventEncoder for Encoder {
    const ENCODING: &'static str = ENCODING;

    #[inline]
    fn serialize<T>(&self, value: &T) -> Result<EventData, EncodeError>
    where
        T: Serialize,
    {
        Ok(EventData::raw(Encoding::Json, serde_json::to_vec(value)?))
    }
}

impl EventDecoder for Decoder {
    const ENCODING: &'static str = ENCODING;

    #[inline]
    fn deserialize<T>(&self, data: &EventData) -> Result<T, DecodeError>
    where
        T: DeserializeOwned,
    {
        if !matches!(data.encoding, Encoding::Json) {
            return Err(DecodeError::EncodingMismatch {
                expected: Encoding::Json,
                actual: data.encoding,
            });
        }
        serde_json::from_slice(&data.data).map_err(DecodeError::Json)
    }
}
