use crate::id::Revision;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("revision conflict: expected {expected}, found {actual}")]
    RevisionConflict {
        expected: Revision,
        actual: Revision,
    },

    #[error("encoding mismatch: expected {expected}, actual {actual}")]
    EncodingMismatch { expected: String, actual: String },

    #[error("publish failed after {attempts} attempts")]
    RetryExhausted { attempts: usize },

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Store implementation errors (mutex poison, backend-specific failures).
    #[error("{0}")]
    Store(String),
}
