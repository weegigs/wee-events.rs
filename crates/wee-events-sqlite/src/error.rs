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

impl From<Error> for wee_events::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::WeeEvents(inner) => inner,
            Error::Serialization(inner) => wee_events::Error::Serialization(inner),
            other => wee_events::Error::Store(Box::new(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn sqlite_serialization_errors_remain_domain_serialization_errors() {
        let sqlite_error = Error::Serialization(
            serde_json::from_str::<serde_json::Value>("{invalid json")
                .expect_err("invalid JSON should fail"),
        );

        let error: wee_events::Error = sqlite_error.into();

        assert!(matches!(error, wee_events::Error::Serialization(_)));
    }

    #[test]
    fn sqlite_internal_errors_become_domain_store_errors() {
        let error: wee_events::Error = Error::Internal("boom".to_string()).into();

        assert!(matches!(error, wee_events::Error::Store(_)));
        assert_eq!(error.to_string(), "internal error: boom");
    }
}
