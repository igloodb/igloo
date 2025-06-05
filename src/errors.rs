// src/errors.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IglooError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("Postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("ADBC Core error: {0}")]
    AdbcCore(#[from] adbc_core::error::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("FFI loading error: {0}")]
    LibLoading(#[from] libloading::Error),

    #[error("FFI call error: {0}")]
    Ffi(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("URL parsing error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("Incompatible data type for schema: field {field_name}, expected {expected_type}, got {actual_value}")]
    DataTypeMismatch {
        field_name: String,
        expected_type: String,
        actual_value: String,
    },

    #[error("Unsupported Arrow type in PostgresTable: {0:?}")]
    UnsupportedArrowType(datafusion::arrow::datatypes::DataType),

    #[error("Row does not contain field: {0}")]
    MissingField(String),
}

pub type Result<T> = std::result::Result<T, IglooError>;
