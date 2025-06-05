use arrow::error::ArrowError;
use std::fmt;

// Forward declaration of AdbcError from operators.rs
// This is not ideal, but necessary if AdbcError remains in operators.rs
// and we want a specific EngineError variant for it without circular dependencies.
// A better solution would be to move AdbcError to its own file or into error.rs if it's generic enough.
// For now, we'll keep the Adbc(String) variant and handle conversion in ScanOperator.
// However, let's try to make a more specific AdbcError variant in EngineError
// by referencing the one from operators.rs for better type safety if possible.
// To do this cleanly, AdbcError would need to be moved or made into a more "global" type.

// Given the current structure, the simplest is Adbc(String) or Adbc(super::operators::AdbcError).
// Let's use super::operators::AdbcError for now and adjust imports in operators.rs if needed.
// This requires AdbcError to be pub in operators.rs.

// Re-evaluating: AdbcError is defined in operators.rs.
// error.rs cannot directly depend on operators.rs if operators.rs depends on error.rs (circular).
// So, EngineError::Adbc must be self-contained or use a string.
// Let's use a String for Adbc errors for simplicity now, and convert AdbcError from operators.rs to String.

#[derive(Debug)]
pub enum EngineError {
    Adbc { message: String },
    Arrow { source: ArrowError }, // Changed to be consistent with how it's used
    Projection { message: String },
    Filter { message: String },
    InvalidInput { message: String },
    Io { source: std::io::Error }, // Changed to be consistent
    Internal { message: String },
    PlanError { message: String }, // For errors during physical plan building
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::Adbc { message } => write!(f, "ADBC Error: {}", message),
            EngineError::Arrow { source } => write!(f, "Arrow Error: {}", source),
            EngineError::Projection { message } => write!(f, "Projection Error: {}", message),
            EngineError::Filter { message } => write!(f, "Filter Error: {}", message),
            EngineError::InvalidInput { message } => write!(f, "Invalid Input: {}", message),
            EngineError::Io { source } => write!(f, "IO Error: {}", source),
            EngineError::Internal { message } => write!(f, "Internal Engine Error: {}", message),
            EngineError::PlanError { message } => write!(f, "Query Plan Error: {}", message),
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EngineError::Arrow { source } => Some(source),
            EngineError::Io { source } => Some(source),
            _ => None, // AdbcError (if it were a nested error type), etc.
        }
    }
}

impl From<ArrowError> for EngineError {
    fn from(err: ArrowError) -> Self {
        EngineError::Arrow { source: err }
    }
}

impl From<std::io::Error> for EngineError {
    fn from(err: std::io::Error) -> Self {
        EngineError::Io { source: err }
    }
}

// We will handle the conversion from operators::AdbcError to EngineError::Adbc { message: String }
// within the ScanOperator logic, by formatting the AdbcError into a string.
// If operators::AdbcError were moved into this crate (e.g. crate::execution::adbc_error::AdbcError),
// then we could do:
// impl From<super::adbc_error::AdbcError> for EngineError {
//     fn from(err: super::adbc_error::AdbcError) -> Self {
//         EngineError::Adbc { source: err } // if Adbc variant stores AdbcError itself
//     }
// }
