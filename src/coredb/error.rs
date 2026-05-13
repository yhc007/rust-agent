//! Errors for the CoreDB data layer.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoreDbError {
    #[error("CoreDB session build failed: {0}")]
    SessionBuild(String),

    #[error("CoreDB migration failed on statement `{stmt}`: {message}")]
    Migration { stmt: String, message: String },

    #[error("CoreDB query failed: {0}")]
    Query(String),
}
