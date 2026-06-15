#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("libsql error: {0}")]
    Libsql(#[from] libsql::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    WeeEvents(#[from] wee_events::Error),
}

impl<E> From<wee_events::RenderError<E>> for Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: wee_events::RenderError<E>) -> Self {
        Error::WeeEvents(err.into())
    }
}

/// Bridges the sqlite-internal error type to the trait-level
/// [`wee_events::Error`] returned by `EventStore` methods. Structural
/// failures pass through unchanged; all other variants flow into
/// [`wee_events::Error::Custom`] preserving the original error chain.
impl From<Error> for wee_events::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::WeeEvents(e) => e,
            other => wee_events::Error::custom(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use wee_events::{RetryDiagnostics, Revision};

    #[test]
    fn sqlite_serialization_errors_display_includes_inner_message() {
        let sqlite_error = Error::Serialization(
            serde_json::from_str::<serde_json::Value>("{invalid json")
                .expect_err("invalid JSON should fail"),
        );

        let rendered = sqlite_error.to_string();
        assert!(
            rendered.starts_with("serialization error: "),
            "unexpected display: {rendered}"
        );
    }

    #[test]
    fn sqlite_retry_exhausted_errors_preserve_retry_diagnostics() {
        use wee_events::RetryExhausted;

        let diagnostics = RetryDiagnostics {
            last_attempted_revision: Revision::try_from("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            observed_max_revision: Revision::try_from("01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
            possible_clock_skew_ms: Some(17),
        };

        let error = Error::WeeEvents(wee_events::Error::retry_exhausted(5, diagnostics.clone()));

        let wee = match &error {
            Error::WeeEvents(e) => e,
            other => panic!("expected WeeEvents, got {other}"),
        };
        let recovered = wee
            .downcast_ref::<RetryExhausted>()
            .expect("RetryExhausted should be retrievable from Custom");
        assert_eq!(recovered.attempts, 5);
        assert_eq!(recovered.diagnostics, diagnostics);
    }
}
