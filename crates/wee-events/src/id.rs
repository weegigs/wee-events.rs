use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Deserializer, Serialize};
use ulid::Ulid;

/// Initial/zero revision — 26 zeros. Matches Go's `InitialRevision`.
const ZERO_REVISION: &str = "00000000000000000000000000";

/// Generates the common boilerplate for an opaque string newtype identifier.
///
/// Backed by `Cow<'static, str>` so `&'static str` literals (declared event
/// types, command names) don't allocate when constructed via [`new_const`].
/// Dynamic strings still flow through [`new`] / `From<String>` / `From<&str>`
/// and become `Cow::Owned`.
///
/// Produces: struct definition with standard derives, `new()`, `new_const()`,
/// `as_str()`, `Display`, `From<String>`, `From<&str>`.
macro_rules! newtype_id {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident;
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        $vis struct $name(Cow<'static, str>);

        impl $name {
            /// Constructs from an owned or borrowed string. Allocates if the
            /// caller's lifetime isn't `'static`.
            pub fn new(s: impl Into<String>) -> Self {
                Self(Cow::Owned(s.into()))
            }

            /// Zero-allocation constructor for compile-time-known names —
            /// derive macros and well-known constants take this path.
            pub const fn new_const(s: &'static str) -> Self {
                Self(Cow::Borrowed(s))
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
                Self(Cow::Owned(s))
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(Cow::Owned(s.to_string()))
            }
        }
    };
}

newtype_id! {
    /// Unique identifier for a recorded event. Opaque string — stores generate
    /// these however they want (ULIDs, UUIDs, padded integers, etc.).
    pub struct EventId;
}

/// Revision marker for an event within an aggregate's stream. Lex-comparable
/// ULID string — `expected_revision < new_revision` is how stores detect
/// optimistic-concurrency conflicts, so the lex-ordering invariant is
/// load-bearing.
///
/// The only ways to construct one are [`from_ulid`](Self::from_ulid) /
/// [`generate`](Self::generate) / [`zero`](Self::zero), or via the validating
/// [`FromStr`] / [`TryFrom<&str>`] / [`TryFrom<String>`] impls. Deserialization
/// validates too. Strings that aren't a 26-char Crockford ULID (or the
/// zero-revision constant) are rejected.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Revision(Arc<str>);

fn zero_revision_arc() -> Arc<str> {
    static ZERO: OnceLock<Arc<str>> = OnceLock::new();
    ZERO.get_or_init(|| Arc::from(ZERO_REVISION)).clone()
}

impl Revision {
    /// Wraps a `Ulid`. Infallible — the type carries the lex-comparable
    /// invariant the conflict-detection comparison relies on.
    #[must_use]
    pub fn from_ulid(ulid: Ulid) -> Self {
        Self(Arc::from(ulid.to_string()))
    }

    /// Generates a fresh ULID-backed revision. Suitable for tests and ad-hoc
    /// callers; production stores should drive a long-lived
    /// [`ulid::Generator`] for monotonicity under bursty traffic.
    #[must_use]
    pub fn generate() -> Self {
        Self::from_ulid(Ulid::new())
    }

    /// The zero revision — represents "no events yet."
    #[must_use]
    pub fn zero() -> Self {
        Self(zero_revision_arc())
    }

    #[must_use]
    pub fn is_zero(&self) -> bool {
        &*self.0 == ZERO_REVISION
    }

    #[must_use]
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

impl From<Ulid> for Revision {
    fn from(u: Ulid) -> Self {
        Self::from_ulid(u)
    }
}

impl FromStr for Revision {
    type Err = RevisionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == ZERO_REVISION {
            return Ok(Self::zero());
        }
        if s.len() != 26 {
            return Err(RevisionParseError::WrongLength(s.len()));
        }
        let ulid = Ulid::from_string(s).map_err(RevisionParseError::InvalidUlid)?;
        Ok(Self::from_ulid(ulid))
    }
}

impl TryFrom<&str> for Revision {
    type Error = RevisionParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl TryFrom<String> for Revision {
    type Error = RevisionParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl<'de> Deserialize<'de> for Revision {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Revision::try_from(s).map_err(serde::de::Error::custom)
    }
}

/// Error from parsing a string into [`Revision`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RevisionParseError {
    #[error("revision must be 26 chars (ULID); got {0} chars")]
    WrongLength(usize),
    #[error("revision is not a valid ULID: {0}")]
    InvalidUlid(ulid::DecodeError),
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
    aggregate_key: Arc<str>,
}

impl AggregateId {
    /// Creates an `AggregateId` from typed parts.
    ///
    /// No validation is performed — the caller provides already-typed values.
    /// Use [`FromStr`] or [`TryFrom<&str>`] for untrusted string input.
    pub fn new(aggregate_type: impl Into<AggregateType>, aggregate_key: impl Into<String>) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_key: Arc::from(aggregate_key.into()),
        }
    }

    #[must_use]
    pub fn aggregate_type(&self) -> &AggregateType {
        &self.aggregate_type
    }

    #[must_use]
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
            aggregate_key: Arc::from(agg_key),
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

/// The type of aggregate (e.g., "campaign", "character"). Kebab-case by convention.
///
/// Arc<str>-backed: clones are refcount bumps. `AggregateId::clone()` is on
/// the publish/load hot path; this avoids an alloc per clone.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AggregateType(Arc<str>);

impl AggregateType {
    pub fn new(s: impl Into<String>) -> Self {
        Self(Arc::from(s.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AggregateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for AggregateType {
    fn from(s: String) -> Self {
        Self(Arc::from(s))
    }
}

impl From<&str> for AggregateType {
    fn from(s: &str) -> Self {
        Self(Arc::from(s))
    }
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

#[cfg(test)]
mod revision_tests {
    use super::{Revision, RevisionParseError, Ulid};

    #[test]
    fn rejects_garbage_strings() {
        assert!(matches!(
            Revision::try_from("zzz"),
            Err(RevisionParseError::WrongLength(3))
        ));
        assert!(matches!(
            Revision::try_from(""),
            Err(RevisionParseError::WrongLength(0))
        ));
        assert!(matches!(
            Revision::try_from("a"),
            Err(RevisionParseError::WrongLength(1))
        ));
        // 26 chars but invalid Crockford alphabet (lowercase + i/l/o/u).
        assert!(matches!(
            Revision::try_from("iiiiiiiiiiiiiiiiiiiiiiiiii"),
            Err(RevisionParseError::InvalidUlid(_))
        ));
    }

    #[test]
    fn accepts_zero_revision_and_real_ulids() {
        Revision::try_from("00000000000000000000000000").unwrap();
        Revision::try_from("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let rev = Revision::from_ulid(Ulid::new());
        assert_eq!(rev.as_str().len(), 26);
    }

    #[test]
    fn deserialize_rejects_garbage() {
        let bad = serde_json::from_str::<Revision>("\"zzz\"");
        assert!(bad.is_err(), "deserialize must validate, got {bad:?}");
    }

    #[test]
    fn deserialize_accepts_real_ulid() {
        let good: Revision = serde_json::from_str("\"01ARZ3NDEKTSV4RRFFQ69G5FAV\"").unwrap();
        assert_eq!(good.as_str(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
    }
}
