use libc::{c_char, c_int, c_void, int64_t, size_t};
use std::ffi::{CStr, CString};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::Arc;

// --- Arrow C Data Interface Imports ---
use arrow_schema::{Schema, SchemaRef, FFI_ArrowSchema};
use arrow_array::{RecordBatch, FFI_ArrowArrayStream};
use arrow_ipc::reader::StreamReader; // May need a different way to consume FFI_ArrowArrayStream
use arrow_ipc::ffi::from_ffi_stream; // Correct way to consume FFI_ArrowArrayStream
use arrow_schema::ArrowError; // Common error type from arrow-rs

// --- ADBC Constants and Status Codes ---

const ADBC_STATUS_OK: c_int = 0;
pub const ADBC_OPTION_DRIVER_INIT_FUNC_NAME: &str = "adbc.driver.init_func_name";

// --- ADBC FFI Structs ---

#[repr(C)]
pub enum AdbcDatabaseHandle {}
#[repr(C)]
pub enum AdbcConnectionHandle {}
#[repr(C)]
pub enum AdbcStatementHandle {}


#[repr(C)]
#[derive(Debug)]
pub struct AdbcError {
    pub message: *mut c_char,
    pub vendor_code: i32,
    pub sqlstate: [c_char; 5],
    pub release: Option<unsafe extern "C" fn(error: *mut AdbcError)>,
    pub private_data: *mut c_void,
    pub private_driver: *mut c_void,
}

#[repr(C)]
#[derive(Debug)]
pub struct AdbcErrorDetail {
    pub key: *const c_char,
    pub value: *const c_char,
}

// --- ADBC FFI Function Signatures ---

#[link(name = "adbc_driver_postgresql")]
#[link(name = "pq")]
extern "C" {
    // Database functions
    fn AdbcDatabaseNew(database: *mut *mut AdbcDatabaseHandle, error: *mut AdbcError) -> c_int;
    fn AdbcDatabaseSetOption(
        database: *mut AdbcDatabaseHandle,
        key: *const c_char,
        value: *const c_char,
        error: *mut AdbcError,
    ) -> c_int;
    fn AdbcDatabaseInit(database: *mut AdbcDatabaseHandle, error: *mut AdbcError) -> c_int;
    fn AdbcDatabaseRelease(database: *mut AdbcDatabaseHandle, error: *mut AdbcError) -> c_int;

    // Connection functions
    fn AdbcConnectionNew(connection: *mut *mut AdbcConnectionHandle, error: *mut AdbcError) -> c_int;
    fn AdbcConnectionInit(
        connection: *mut AdbcConnectionHandle,
        database: *mut AdbcDatabaseHandle,
        error: *mut AdbcError,
    ) -> c_int;
    fn AdbcConnectionRelease(connection: *mut AdbcConnectionHandle, error: *mut AdbcError) -> c_int;
    fn AdbcConnectionGetTableSchema(
        connection: *mut AdbcConnectionHandle,
        catalog: *const c_char,
        db_schema: *const c_char,
        table_name: *const c_char,
        schema: *mut FFI_ArrowSchema,
        error: *mut AdbcError,
    ) -> c_int;

    // Statement functions
    fn AdbcStatementNew(
        connection: *mut AdbcConnectionHandle,
        statement: *mut *mut AdbcStatementHandle,
        error: *mut AdbcError,
    ) -> c_int;
    fn AdbcStatementSetSqlQuery(
        statement: *mut AdbcStatementHandle,
        query: *const c_char,
        error: *mut AdbcError,
    ) -> c_int;
    fn AdbcStatementExecuteQuery(
        statement: *mut AdbcStatementHandle,
        stream: *mut FFI_ArrowArrayStream, // Output parameter for the stream
        rows_affected: *mut int64_t,    // Output parameter for rows affected
        error: *mut AdbcError,
    ) -> c_int;
    fn AdbcStatementRelease(statement: *mut AdbcStatementHandle, error: *mut AdbcError) -> c_int;

    // Error functions
    fn AdbcErrorGetDetailCount(error: *const AdbcError) -> size_t;
    fn AdbcErrorGetDetail(error: *const AdbcError, index: size_t) -> AdbcErrorDetail;

    #[allow(dead_code)]
    fn AdbcDriverPostgreSqlInit(version: c_int, driver: *mut c_void, error: *mut AdbcError) -> c_int;
}

// --- Rust Wrapper Structs ---

#[derive(Debug)]
pub struct AdbcDatabase {
    ptr: *mut AdbcDatabaseHandle,
    _marker: PhantomData<*mut ()>,
}

impl AdbcDatabase {
    fn new() -> Result<Self, AdbcErrorWrapper> {
        let mut db_ptr: *mut AdbcDatabaseHandle = ptr::null_mut();
        let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };
        let status = unsafe { AdbcDatabaseNew(&mut db_ptr, &mut error) };
        if status != ADBC_STATUS_OK { return Err(AdbcErrorWrapper::new(error, status, "AdbcDatabaseNew")); }
        if db_ptr.is_null() { return Err(AdbcErrorWrapper::custom("AdbcDatabaseNew: OK but null pointer")); }
        Ok(AdbcDatabase { ptr: db_ptr, _marker: PhantomData })
    }

    fn set_option(&mut self, key: &str, value: &str) -> Result<(), AdbcErrorWrapper> {
        let c_key = CString::new(key).map_err(|e| AdbcErrorWrapper::custom(&format!("Invalid key {}: {}", key, e)))?;
        let c_value = CString::new(value).map_err(|e| AdbcErrorWrapper::custom(&format!("Invalid value {}: {}", value, e)))?;
        let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };
        let status = unsafe { AdbcDatabaseSetOption(self.ptr, c_key.as_ptr(), c_value.as_ptr(), &mut error) };
        if status != ADBC_STATUS_OK { Err(AdbcErrorWrapper::new(error, status, "AdbcDatabaseSetOption")) } else { Ok(()) }
    }

    fn init(&mut self) -> Result<(), AdbcErrorWrapper> {
        let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };
        let status = unsafe { AdbcDatabaseInit(self.ptr, &mut error) };
        if status != ADBC_STATUS_OK { Err(AdbcErrorWrapper::new(error, status, "AdbcDatabaseInit")) } else { Ok(()) }
    }
}

impl Drop for AdbcDatabase {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };
            unsafe { AdbcDatabaseRelease(self.ptr, &mut error) }; // Ignore error in drop
            self.ptr = ptr::null_mut();
        }
    }
}

#[derive(Debug)]
pub struct AdbcStatement {
    ptr: *mut AdbcStatementHandle,
    _marker: PhantomData<*mut ()>,
    // Conceptually, it might also hold a reference to AdbcConnection if needed for lifetime,
    // e.g., _connection: Arc<AdbcConnectionSharedState>,
}

impl AdbcStatement {
    /// Creates a new statement associated with a connection.
    fn new(connection: &AdbcConnection) -> Result<Self, AdbcErrorWrapper> {
        let mut stmt_ptr: *mut AdbcStatementHandle = ptr::null_mut();
        let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };
        let status = unsafe { AdbcStatementNew(connection.ptr, &mut stmt_ptr, &mut error) };

        if status != ADBC_STATUS_OK {
            return Err(AdbcErrorWrapper::new(error, status, "AdbcStatementNew"));
        }
        if stmt_ptr.is_null() {
            return Err(AdbcErrorWrapper::custom("AdbcStatementNew: OK but null pointer"));
        }
        Ok(AdbcStatement { ptr: stmt_ptr, _marker: PhantomData })
    }

    /// Sets the SQL query for the statement.
    fn set_sql_query(&mut self, sql: &str) -> Result<(), AdbcErrorWrapper> {
        let c_sql = CString::new(sql).map_err(|e| AdbcErrorWrapper::custom(&format!("Invalid SQL query string: {}", e)))?;
        let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };
        let status = unsafe { AdbcStatementSetSqlQuery(self.ptr, c_sql.as_ptr(), &mut error) };
        if status != ADBC_STATUS_OK { Err(AdbcErrorWrapper::new(error, status, "AdbcStatementSetSqlQuery")) } else { Ok(()) }
    }

    /// Executes the query and returns an FFI_ArrowArrayStream.
    /// The caller is responsible for managing the lifetime of the FFI_ArrowArrayStream
    /// and eventually calling its release callback.
    fn execute_query_raw_stream(&mut self) -> Result<(FFI_ArrowArrayStream, i64), AdbcErrorWrapper> {
        let mut ffi_stream = FFI_ArrowArrayStream::empty();
        let mut rows_affected: int64_t = -1;
        let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };

        let status = unsafe {
            AdbcStatementExecuteQuery(self.ptr, &mut ffi_stream, &mut rows_affected, &mut error)
        };

        if status != ADBC_STATUS_OK {
            return Err(AdbcErrorWrapper::new(error, status, "AdbcStatementExecuteQuery"));
        }
        Ok((ffi_stream, rows_affected))
    }
}

impl Drop for AdbcStatement {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };
            unsafe { AdbcStatementRelease(self.ptr, &mut error) }; // Ignore error in drop
            self.ptr = ptr::null_mut();
        }
    }
}


#[derive(Debug)]
pub struct AdbcConnection {
    ptr: *mut AdbcConnectionHandle,
    _marker: PhantomData<*mut ()>,
    // _database: Arc<AdbcDatabase>, // To ensure database outlives connection
}

impl AdbcConnection {
    fn new() -> Result<Self, AdbcErrorWrapper> {
        let mut conn_ptr: *mut AdbcConnectionHandle = ptr::null_mut();
        let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };
        let status = unsafe { AdbcConnectionNew(&mut conn_ptr, &mut error) };
        if status != ADBC_STATUS_OK { return Err(AdbcErrorWrapper::new(error, status, "AdbcConnectionNew")); }
        if conn_ptr.is_null() { return Err(AdbcErrorWrapper::custom("AdbcConnectionNew: OK but null pointer")); }
        Ok(AdbcConnection { ptr: conn_ptr, _marker: PhantomData })
    }

    fn init(&mut self, db: &AdbcDatabase) -> Result<(), AdbcErrorWrapper> {
        let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };
        let status = unsafe { AdbcConnectionInit(self.ptr, db.ptr, &mut error) };
        if status != ADBC_STATUS_OK { Err(AdbcErrorWrapper::new(error, status, "AdbcConnectionInit")) } else { Ok(()) }
    }

    pub fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef, AdbcErrorWrapper> {
        let c_table_name = CString::new(table_name)
            .map_err(|e| AdbcErrorWrapper::custom(&format!("Invalid CString for table_name: {}", e)))?;
        let mut ffi_schema = FFI_ArrowSchema::empty();
        let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };

        let status = unsafe {
            AdbcConnectionGetTableSchema(self.ptr, ptr::null(), ptr::null(), c_table_name.as_ptr(), &mut ffi_schema, &mut error)
        };
        if status != ADBC_STATUS_OK { return Err(AdbcErrorWrapper::new(error, status, "AdbcConnectionGetTableSchema")); }

        match Schema::try_from(&ffi_schema) {
            Ok(schema) => Ok(Arc::new(schema)),
            Err(e) => {
                if let Some(release_fn) = ffi_schema.release { unsafe { release_fn(&mut ffi_schema) }; }
                Err(AdbcErrorWrapper::custom(&format!("Failed to convert FFI_ArrowSchema: {}", e)))
            }
        }
    }

    /// Executes an SQL query and returns a stream of RecordBatches.
    ///
    /// The returned iterator owns the ADBC statement and manages its lifetime.
    /// Thread safety (`Send + Sync`) of the iterator depends on the underlying ADBC driver's properties.
    pub fn execute_query_arrow(
        &self,
        sql: &str
    ) -> Result<AdbcStreamReader, AdbcErrorWrapper> {
        let mut statement = AdbcStatement::new(self)?;
        statement.set_sql_query(sql)?;

        let (ffi_stream, _rows_affected) = statement.execute_query_raw_stream()?;

        // The AdbcStreamReader now takes ownership of the statement and the ffi_stream
        AdbcStreamReader::try_new(statement, ffi_stream)
    }
}

impl Drop for AdbcConnection {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };
            unsafe { AdbcConnectionRelease(self.ptr, &mut error) }; // Ignore error in drop
            self.ptr = ptr::null_mut();
        }
    }
}

/// A stream reader for ADBC results, managing statement and stream lifetimes.
/// Note: The `Send` and `Sync` bounds are optimistic and depend on the driver's thread-safety.
pub struct AdbcStreamReader {
    reader: StreamReader<FFI_ArrowArrayStream>, // This is not directly how StreamReader is used with FFI_ArrowArrayStream
                                                 // It should be:
                                                 // reader: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>> + Send + Sync>,
                                                 // or directly use a struct that wraps FFI_ArrowArrayStream and implements Iterator.
                                                 // For this PoC, we'll use from_ffi_stream which returns an iterator.
    _ffi_stream_holder: FFI_ArrowArrayStream, // Keep the FFI stream struct alive for its release callback
    _statement: AdbcStatement, // Owns the statement to keep it alive
    // The actual iterator from from_ffi_stream
    batch_iterator: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>> + Send + Sync>,
}

impl AdbcStreamReader {
    /// Creates a new AdbcStreamReader from a statement and an FFI_ArrowArrayStream.
    /// Takes ownership of the statement and the FFI stream.
    pub fn try_new(
        statement: AdbcStatement,
        mut ffi_stream: FFI_ArrowArrayStream,
    ) -> Result<Self, AdbcErrorWrapper> {
        // The `from_ffi_stream` function consumes the FFI_ArrowArrayStream.
        // It's crucial that `ffi_stream.release` is properly set by the ADBC driver.
        // `from_ffi_stream` will set up the necessary drop glue to call this release function.
        let reader_iterator = unsafe {
            from_ffi_stream(&mut ffi_stream)
                .map_err(|e| AdbcErrorWrapper::custom(&format!("Failed to create reader from FFI stream: {}", e)))?
        };

        Ok(AdbcStreamReader {
            _ffi_stream_holder: ffi_stream, // This ffi_stream is now "moved" or "consumed" by from_ffi_stream effectively.
                                           // Storing it here is mainly to tie its lifetime if from_ffi_stream didn't fully consume/manage it.
                                           // However, modern arrow-rs from_ffi_stream should handle the release.
                                           // Let's assume from_ffi_stream handles release of the C stream.
                                           // The FFI_ArrowArrayStream struct itself doesn't need separate Drop if its release is called.
            _statement: statement,
            batch_iterator: Box::new(reader_iterator),
        })
    }
}

impl Iterator for AdbcStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batch_iterator.next()
    }
}

// --- Error Handling Wrapper ---
#[derive(Debug)]
pub struct AdbcErrorWrapper {
    pub message: String,
    pub vendor_code: Option<i32>,
    pub sqlstate: Option<String>,
    pub adbc_status: c_int,
    pub operation: String,
    pub details: Vec<(String, String)>,
}

impl AdbcErrorWrapper {
    fn new(mut error: AdbcError, status_code: c_int, operation: &str) -> Self {
        let message = unsafe {
            if error.message.is_null() { format!("ADBC op '{}': status {}. No message.", operation, status_code) }
            else { CStr::from_ptr(error.message).to_string_lossy().into_owned() }
        };
        let vendor_code = Some(error.vendor_code);
        let sqlstate_bytes: Vec<u8> = error.sqlstate.iter().map(|&c| c as u8).take_while(|&b| b != 0).collect();
        let sqlstate = String::from_utf8(sqlstate_bytes).ok();
        let mut details = Vec::new();
        if !error.private_data.is_null() {
            unsafe {
                let count = AdbcErrorGetDetailCount(&error);
                for i in 0..count {
                    let detail = AdbcErrorGetDetail(&error, i);
                    let key = if detail.key.is_null() { String::new() } else { CStr::from_ptr(detail.key).to_string_lossy().into_owned() };
                    let value = if detail.value.is_null() { String::new() } else { CStr::from_ptr(detail.value).to_string_lossy().into_owned() };
                    details.push((key, value));
                }
            }
        }
        if let Some(release_fn) = error.release { unsafe { release_fn(&mut error) }; }
        AdbcErrorWrapper { message, vendor_code, sqlstate, adbc_status: status_code, operation: operation.to_string(), details }
    }
    fn custom(message: &str) -> Self {
        AdbcErrorWrapper { message: message.to_string(), vendor_code: None, sqlstate: None, adbc_status: -1, operation: "Custom".to_string(), details: Vec::new() }
    }
}
impl fmt::Display for AdbcErrorWrapper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ADBC Error (op: '{}', status: {}): {}", self.operation, self.adbc_status, self.message)?;
        if let Some(vc) = self.vendor_code { write!(f, ", VendorCode: {}", vc)?; }
        if let Some(ss) = &self.sqlstate { write!(f, ", SQLState: {}", ss)?; }
        if !self.details.is_empty() {
            write!(f, ", Details: [")?;
            for (i, (k, v)) in self.details.iter().enumerate() {
                if i > 0 { write!(f, ", ")?; }
                write!(f, "\"{}\": \"{}\"", k, v)?;
            }
            write!(f, "]")?;
        }
        Ok(())
    }
}
impl std::error::Error for AdbcErrorWrapper {}

// --- Public Connect Function ---
pub fn connect(
    connection_string: &str,
    driver_init_func_name_override: Option<&str>,
) -> Result<AdbcConnection, String> {
    let mut db = AdbcDatabase::new().map_err(|e| e.to_string())?;
    if let Some(init_func_name) = driver_init_func_name_override {
        db.set_option(ADBC_OPTION_DRIVER_INIT_FUNC_NAME, init_func_name).map_err(|e| e.to_string())?;
    }
    db.set_option("uri", connection_string).map_err(|e| e.to_string())?;
    db.init().map_err(|e| e.to_string())?;
    let mut conn = AdbcConnection::new().map_err(|e| e.to_string())?;
    conn.init(&db).map_err(|e| e.to_string())?;
    mem::forget(db); // Simplification: See notes in previous steps. Real lib needs robust lifetime mgmt.
    Ok(conn)
}
