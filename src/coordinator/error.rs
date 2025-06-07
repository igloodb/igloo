use std::fmt;

#[derive(Debug)]
pub enum PlannerError {
    NotSupported(String),
    PlanningError(String),
    Internal(String), // For other internal errors
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerError::NotSupported(op) => write!(f, "PlannerError: Operation not supported: {}", op),
            PlannerError::PlanningError(desc) => write!(f, "PlannerError: {}", desc),
            PlannerError::Internal(desc) => write!(f, "PlannerError (Internal): {}", desc),
        }
    }
}

impl std::error::Error for PlannerError {}
