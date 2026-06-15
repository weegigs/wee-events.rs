use std::fmt;

use crate::id::Revision;

//TODO: @Kevin -- Are retries not a concern for the caller?
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryDiagnostics {
    pub last_attempted_revision: Revision,
    pub observed_max_revision: Revision,
    pub possible_clock_skew_ms: Option<u64>,
}

impl fmt::Display for RetryDiagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "last attempted revision {}, observed max revision {}",
            self.last_attempted_revision, self.observed_max_revision
        )?;
        if let Some(skew_ms) = self.possible_clock_skew_ms {
            write!(f, ", possible clock skew {skew_ms} ms")?;
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("revision conflict: expected {expected}, found {actual}")]
    RevisionConflict {
        expected: Revision,
        actual: Revision,
    },

    #[error("encoding mismatch: expected {expected}, actual {actual}")]
    EncodingMismatch { expected: String, actual: String },

    #[error("unhandled event type: {event_type}")]
    UnhandledEventType { event_type: String },

    /// Backend-specific error escape hatch. Stores wrap their internal
    /// concrete error types (`libsql::Error`, `serde_json::Error`,
    /// [`RetryExhausted`], …) into this variant when surfacing failures
    /// across the `EventStore` trait boundary, so callers see one error
    /// type while preserving the original error chain via `source()`.
    #[error(transparent)]
    Custom(Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Framework-side outcome: an auto-retry loop tried `attempts` times and
/// never landed a consistent commit. Lives outside [`Error`]'s structural
/// variants because retry is a caller-policy concern, not a property of
/// the persistent state. Surfaced through [`Error::Custom`]; recover via
/// [`std::error::Error::source`] or [`Error::retry_exhausted`].
#[derive(Debug, thiserror::Error)]
#[error("publish failed after {attempts} attempts ({diagnostics})")]
pub struct RetryExhausted {
    pub attempts: usize,
    pub diagnostics: RetryDiagnostics,
}

impl Error {
    /// Wraps any `std::error::Error` into [`Error::Custom`]. Use this at
    /// backend boundaries to surface infrastructure failures through the
    /// fixed `EventStore::Error = wee_events::Error` contract.
    pub fn custom<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Error::Custom(Box::new(error))
    }

    /// Convenience constructor: wraps a [`RetryExhausted`] in
    /// [`Error::Custom`]. Recover the typed value via
    /// `error.downcast_ref::<RetryExhausted>()`.
    #[must_use]
    pub fn retry_exhausted(attempts: usize, diagnostics: RetryDiagnostics) -> Self {
        Error::custom(RetryExhausted {
            attempts,
            diagnostics,
        })
    }

    /// Returns the inner boxed error of [`Error::Custom`] as a concrete
    /// `&T`, if it matches. Returns `None` for structural variants and
    /// for `Custom` payloads of a different type.
    #[must_use]
    pub fn downcast_ref<T: std::error::Error + 'static>(&self) -> Option<&T> {
        match self {
            Error::Custom(boxed) => boxed.downcast_ref::<T>(),
            _ => None,
        }
    }
}

impl From<crate::EncodeError> for Error {
    fn from(err: crate::EncodeError) -> Self {
        Error::custom(err)
    }
}

impl From<crate::DecodeError> for Error {
    fn from(err: crate::DecodeError) -> Self {
        Error::custom(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::custom(err)
    }
}

#[cfg(test)]
mod tests {
    use super::{RetryDiagnostics, Revision};

    #[test]
    fn retry_diagnostics_display_without_clock_skew_hint() {
        let diagnostics = RetryDiagnostics {
            last_attempted_revision: Revision::try_from("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            observed_max_revision: Revision::try_from("01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
            possible_clock_skew_ms: None,
        };

        assert_eq!(
            diagnostics.to_string(),
            "last attempted revision 01ARZ3NDEKTSV4RRFFQ69G5FAV, observed max revision 01ARZ3NDEKTSV4RRFFQ69G5FAW"
        );
    }

    #[test]
    fn retry_diagnostics_display_with_clock_skew_hint() {
        let diagnostics = RetryDiagnostics {
            last_attempted_revision: Revision::try_from("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            observed_max_revision: Revision::try_from("01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
            possible_clock_skew_ms: Some(17),
        };

        assert_eq!(
            diagnostics.to_string(),
            "last attempted revision 01ARZ3NDEKTSV4RRFFQ69G5FAV, observed max revision 01ARZ3NDEKTSV4RRFFQ69G5FAW, possible clock skew 17 ms"
        );
    }
}
