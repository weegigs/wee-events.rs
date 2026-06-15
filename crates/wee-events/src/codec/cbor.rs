use serde::{Serialize, de::DeserializeOwned};

use super::{DecodeError, EncodeError, Encoding, EventDecoder, EventEncoder};
use crate::event::EventData;

pub const ENCODING: &str = "application/cbor";

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
        let mut data = Vec::new();
        ciborium::into_writer(value, &mut data)?;
        Ok(EventData::raw(Encoding::Cbor, data))
    }
}

impl EventDecoder for Decoder {
    const ENCODING: &'static str = ENCODING;

    #[inline]
    fn deserialize<T>(&self, data: &EventData) -> Result<T, DecodeError>
    where
        T: DeserializeOwned,
    {
        if !matches!(data.encoding, Encoding::Cbor) {
            return Err(DecodeError::EncodingMismatch {
                expected: Encoding::Cbor,
                actual: data.encoding,
            });
        }
        ciborium::from_reader(data.data.as_slice()).map_err(DecodeError::Cbor)
    }
}
