//! Event payload encoding.
//!
//! One module per encoding (`json`, `cbor`, ...) — each gated behind a Cargo
//! feature. Each module exposes a unit-struct `Encoder` and `Decoder`
//! implementing [`EventEncoder`] / [`EventDecoder`]; runtime dispatch goes
//! through the [`Encoding`] enum's [`encode`](Encoding::encode) /
//! [`decode`](Encoding::decode) methods.
//!
//! To add an encoding: create `codec/<name>.rs`, declare a `<name>` feature in
//! `Cargo.toml`, add the variant + match arms in this module.

use serde::{Serialize, de::DeserializeOwned};

use crate::event::EventData;

pub mod json;

#[cfg(feature = "cbor")]
pub mod cbor;

/// Closed set of supported payload encodings.
///
/// Variants are feature-gated where their backing crate is optional. Always
/// includes [`Encoding::Json`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    Json,

    #[cfg(feature = "cbor")]
    Cbor,
}

impl Encoding {
    /// The MIME-style identifier written into [`EventData::encoding`].
    #[inline]
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Encoding::Json => json::ENCODING,
            #[cfg(feature = "cbor")]
            Encoding::Cbor => cbor::ENCODING,
        }
    }

    /// Resolves an encoding identifier from a payload.
    ///
    /// Returns [`DecodeError::UnknownEncoding`] if no enabled encoding matches.
    #[inline]
    pub fn from_encoding_str(s: &str) -> Result<Self, DecodeError> {
        if s == json::ENCODING {
            return Ok(Encoding::Json);
        }

        #[cfg(feature = "cbor")]
        if s == cbor::ENCODING {
            return Ok(Encoding::Cbor);
        }

        Err(DecodeError::UnknownEncoding {
            encoding: s.to_string(),
        })
    }

    /// Encodes a value via the selected encoding.
    #[inline]
    pub fn encode<T>(&self, value: &T) -> Result<EventData, EncodeError>
    where
        T: Serialize,
    {
        match self {
            Encoding::Json => json::Encoder.serialize(value),
            #[cfg(feature = "cbor")]
            Encoding::Cbor => cbor::Encoder.serialize(value),
        }
    }

    /// Decodes a payload using the selected encoding.
    ///
    /// Returns [`DecodeError::EncodingMismatch`] if `data.encoding` doesn't
    /// match this variant.
    #[inline]
    pub fn decode<T>(&self, data: &EventData) -> Result<T, DecodeError>
    where
        T: DeserializeOwned,
    {
        match self {
            Encoding::Json => json::Decoder.deserialize(data),
            #[cfg(feature = "cbor")]
            Encoding::Cbor => cbor::Decoder.deserialize(data),
        }
    }
}

/// Contract satisfied by each per-encoding `Encoder`.
pub trait EventEncoder {
    const ENCODING: &'static str;

    fn serialize<T>(&self, value: &T) -> Result<EventData, EncodeError>
    where
        T: Serialize;
}

/// Contract satisfied by each per-encoding `Decoder`.
pub trait EventDecoder {
    const ENCODING: &'static str;

    fn deserialize<T>(&self, data: &EventData) -> Result<T, DecodeError>
    where
        T: DeserializeOwned;
}

/// Stores advertise the [`Encoding`] they write through this trait.
///
/// [`crate::Publisher`] reads it to pick the encoder when serialising events.
pub trait EncodesEvents {
    fn encoding(&self) -> Encoding;
}

impl<T> EncodesEvents for std::sync::Arc<T>
where
    T: EncodesEvents + ?Sized,
{
    fn encoding(&self) -> Encoding {
        (**self).encoding()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("json encode: {0}")]
    Json(#[from] serde_json::Error),

    #[cfg(feature = "cbor")]
    #[error("cbor encode: {0}")]
    Cbor(#[from] ciborium::ser::Error<std::io::Error>),
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("unknown encoding: {encoding}")]
    UnknownEncoding { encoding: String },

    #[error("encoding mismatch: expected {expected:?}, actual {actual:?}")]
    EncodingMismatch {
        expected: Encoding,
        actual: Encoding,
    },
    #[error("json decode: {0}")]
    Json(#[from] serde_json::Error),

    #[cfg(feature = "cbor")]
    #[error("cbor decode: {0}")]
    Cbor(#[from] ciborium::de::Error<std::io::Error>),
}

/// Unified codec failure covering both encode and decode directions.
///
/// Used at boundaries where a single error type needs to carry either kind
/// of codec failure (e.g. `ServiceError::Codec`).
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error(transparent)]
    Encode(#[from] EncodeError),

    #[error(transparent)]
    Decode(#[from] DecodeError),
}
