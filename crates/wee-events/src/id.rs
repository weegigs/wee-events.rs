use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Initial/zero revision — 26 zeros. Matches Go's `InitialRevision`.
const ZERO_REVISION: &str = "00000000000000000000000000";

/// Generates the common boilerplate for an opaque string newtype identifier.
///
/// Produces: struct definition with standard derives, `new()`, `as_str()`,
/// `Display`, `From<String>`, `From<&str>`.
macro_rules! newtype_id {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident;
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        $vis struct $name(String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_string())
            }
        }
    };
}

newtype_id! {
    /// Unique identifier for a recorded event. Opaque string — stores generate
    /// these however they want (ULIDs, UUIDs, padded integers, etc.).
    pub struct EventId;
}

/// Revision marker for an event within an aggregate's stream. Opaque,
/// lexicographically comparable string — stores generate these however
/// they want (ULIDs, padded hex integers, etc.).
///
/// The only invariant: revisions within an aggregate must sort in the
/// order events were appended. Zero revision (`"00000000000000000000000000"`)
/// means "no events yet."
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Revision(String);

impl Revision {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// The zero revision — represents "no events yet."
    pub fn zero() -> Self {
        Self(ZERO_REVISION.to_string())
    }

    pub fn is_zero(&self) -> bool {
        self.0 == ZERO_REVISION
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Revision {
    fn default() -> Self {
        Self::zero()
    }
}

impl fmt::Display for Revision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for Revision {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Revision {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Composite identifier for an aggregate: type + key.
///
/// Follows the `std::net::SocketAddrV4` pattern: [`new`](Self::new) assembles
/// from already-typed parts without re-validation, while [`FromStr`] /
/// [`TryFrom<&str>`] validate untrusted string input. Fields are private;
/// access via [`aggregate_type`](Self::aggregate_type) and
/// [`aggregate_key`](Self::aggregate_key).
///
/// # Wire format
///
/// Serializes to `"type:key"` via `Display` and parses back via `FromStr`.
/// The split point is the **first colon**: everything before it is the
/// aggregate type (a simple kebab-case token, no colons), everything after
/// it is the key (which may contain additional colons for compound
/// identifiers like `"run:01ABC"` or URN-style values).
///
/// This format is safe for use in URLs, path segments, and query parameters.
/// The `Display` → `FromStr` round-trip is guaranteed.
///
/// # Construction
///
/// | Method | Input | Validates | Fails |
/// |--------|-------|-----------|-------|
/// | [`new`](Self::new) | typed parts | no | never |
/// | [`FromStr`] | `"type:key"` string | yes | `AggregateIdParseError` |
/// | [`TryFrom<&str>`] | `"type:key"` string | yes | `AggregateIdParseError` |
///
/// `new()` trusts its inputs — passing an empty `AggregateType` or empty key
/// produces a value that serializes to `":key"` or `"type:"`, which will
/// fail to round-trip through `FromStr`. This mirrors how
/// `SocketAddrV4::new(Ipv4Addr, u16)` trusts its typed arguments.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AggregateId {
    aggregate_type: AggregateType,
    aggregate_key: String,
}

impl AggregateId {
    /// Creates an `AggregateId` from typed parts.
    ///
    /// No validation is performed — the caller provides already-typed values.
    /// Use [`FromStr`] or [`TryFrom<&str>`] for untrusted string input.
    pub fn new(aggregate_type: impl Into<AggregateType>, aggregate_key: impl Into<String>) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_key: aggregate_key.into(),
        }
    }

    pub fn aggregate_type(&self) -> &AggregateType {
        &self.aggregate_type
    }

    pub fn aggregate_key(&self) -> &str {
        &self.aggregate_key
    }
}

impl fmt::Display for AggregateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.aggregate_type, self.aggregate_key)
    }
}

impl FromStr for AggregateId {
    type Err = AggregateIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (agg_type, agg_key) = s
            .split_once(':')
            .ok_or(AggregateIdParseError::MissingColon)?;

        if agg_type.is_empty() {
            return Err(AggregateIdParseError::EmptyType);
        }
        if agg_key.is_empty() {
            return Err(AggregateIdParseError::EmptyKey);
        }

        Ok(Self {
            aggregate_type: AggregateType::new(agg_type),
            aggregate_key: agg_key.to_string(),
        })
    }
}

impl TryFrom<&str> for AggregateId {
    type Error = AggregateIdParseError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl TryFrom<String> for AggregateId {
    type Error = AggregateIdParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

/// Error returned when parsing an `AggregateId` from a string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AggregateIdParseError {
    #[error("aggregate ID must contain a colon separating type and key")]
    MissingColon,
    #[error("aggregate type (before colon) must not be empty")]
    EmptyType,
    #[error("aggregate key (after colon) must not be empty")]
    EmptyKey,
}

newtype_id! {
    /// The type of aggregate (e.g., "campaign", "character"). Kebab-case by convention.
    pub struct AggregateType;
}

newtype_id! {
    /// Discriminator for event types at the store boundary (e.g., "fph:crew-injured").
    /// Kebab-case by convention, prefix:variant format.
    pub struct EventType;
}

newtype_id! {
    /// Name of a command (e.g., "campaign:advance-turn"). Kebab-case, prefix:action format.
    pub struct CommandName;
}

newtype_id! {
    /// Correlation identifier for tracing related events across aggregates.
    pub struct CorrelationId;
}
