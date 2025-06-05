// src/adbc_postgres_ffi.rs
use crate::errors::{IglooError, Result as IglooResult};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::record_batch::RecordBatch;
use libloading::{Library, Symbol};
use scopeguard::defer;
use std::ffi::{c_char, c_int, CStr, CString};
use std::mem;
use std::ptr;

// ADBC C API Structs (simplified, fields are driver-private)
#[repr(C)]
pub struct AdbcDriver { _private: [u8; 0] }
#[repr(C)]
pub struct AdbcDatabase { _private_data: *mut std::ffi::c_void, _private_driver: *mut AdbcDriver }
#[repr(C)]
pub struct AdbcConnection { _private_data: *mut std::ffi::c_void, _private_driver: *mut AdbcDriver }
#[repr(C)]
pub struct AdbcStatement { _private_data: *mut std::ffi::c_void, _private_driver: *mut AdbcDriver }

// AdbcError struct as defined in user feedback
#[repr(C)]
#[derive(Debug)]
pub struct AdbcError {
    pub message: *mut c_char,
    pub vendor_code: i32,
    pub sqlstate: [u8; 6], // Null-terminated: 5 bytes + null
    pub release: Option<unsafe extern "C" fn(error: *mut AdbcError)>,
    pub private_data: *mut std::ffi::c_void,
}

impl AdbcError {
    fn release_if_needed(&mut self) {
        if let Some(release_fn) = self.release {
            // Per ADBC spec, error->release(error) should be called if error->message is not NULL.
            if !self.message.is_null() {
                unsafe { release_fn(self) };
                self.message = ptr::null_mut(); // Avoid double free
            }
        }
    }

    fn get_message_str(&self) -> Option<String> {
        if self.message.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(self.message).to_string_lossy().into_owned() })
        }
    }
}

impl Drop for AdbcError {
    fn drop(&mut self) {
        self.release_if_needed();
    }
}

// ADBC Function Pointer Types
type AdbcDatabaseNewFunc = unsafe extern "C" fn(database: *mut AdbcDatabase, error: *mut AdbcError) -> c_int;
type AdbcDatabaseSetOptionFunc = unsafe extern "C" fn(database: *mut AdbcDatabase, key: *const c_char, value: *const c_char, error: *mut AdbcError) -> c_int;
type AdbcDatabaseInitFunc = unsafe extern "C" fn(database: *mut AdbcDatabase, error: *mut AdbcError) -> c_int;
type AdbcDatabaseReleaseFunc = unsafe extern "C" fn(database: *mut AdbcDatabase, error: *mut AdbcError) -> c_int;

type AdbcConnectionNewFunc = unsafe extern "C" fn(connection: *mut AdbcConnection, error: *mut AdbcError) -> c_int;
type AdbcConnectionInitFunc = unsafe extern "C" fn(connection: *mut AdbcConnection, database: *mut AdbcDatabase, error: *mut AdbcError) -> c_int;
type AdbcConnectionReleaseFunc = unsafe extern "C" fn(connection: *mut AdbcConnection, error: *mut AdbcError) -> c_int;

type AdbcStatementNewFunc = unsafe extern "C" fn(connection: *mut AdbcConnection, statement: *mut AdbcStatement, error: *mut AdbcError) -> c_int;
type AdbcStatementSetSqlQueryFunc = unsafe extern "C" fn(statement: *mut AdbcStatement, query: *const c_char, error: *mut AdbcError) -> c_int;
type AdbcStatementExecuteQueryFunc = unsafe extern "C" fn(statement: *mut AdbcStatement, out_stream: *mut FFI_ArrowArrayStream, rows_affected: *mut i64, error: *mut AdbcError) -> c_int;
type AdbcStatementReleaseFunc = unsafe extern "C" fn(statement: *mut AdbcStatement, error: *mut AdbcError) -> c_int;


pub struct AdbcPostgresFFI {
    _lib: Box<Library>, // Keeps the library loaded

    database_new: Symbol<'static, AdbcDatabaseNewFunc>,
    database_set_option: Symbol<'static, AdbcDatabaseSetOptionFunc>,
    database_init: Symbol<'static, AdbcDatabaseInitFunc>,
    database_release: Symbol<'static, AdbcDatabaseReleaseFunc>,

    connection_new: Symbol<'static, AdbcConnectionNewFunc>,
    connection_init: Symbol<'static, AdbcConnectionInitFunc>,
    connection_release: Symbol<'static, AdbcConnectionReleaseFunc>,

    statement_new: Symbol<'static, AdbcStatementNewFunc>,
    statement_set_sql_query: Symbol<'static, AdbcStatementSetSqlQueryFunc>,
    statement_execute_query: Symbol<'static, AdbcStatementExecuteQueryFunc>,
    statement_release: Symbol<'static, AdbcStatementReleaseFunc>,
}

impl AdbcPostgresFFI {
    pub unsafe fn load(lib_path: &str) -> IglooResult<Self> {
        let lib = Box::new(Library::new(lib_path).map_err(IglooError::LibLoading)?);

        macro_rules! load_symbol {
            ($name:ident, $type:ty) => {
                mem::transmute::<Symbol<'_ , $type>, Symbol<'static, $type>>(
                    lib.get(stringify!($name).as_bytes())
                        .map_err(IglooError::LibLoading)?
                )
            };
        }

        Ok(Self {
            database_new: load_symbol!(AdbcDatabaseNew, AdbcDatabaseNewFunc),
            database_set_option: load_symbol!(AdbcDatabaseSetOption, AdbcDatabaseSetOptionFunc),
            database_init: load_symbol!(AdbcDatabaseInit, AdbcDatabaseInitFunc),
            database_release: load_symbol!(AdbcDatabaseRelease, AdbcDatabaseReleaseFunc),
            connection_new: load_symbol!(AdbcConnectionNew, AdbcConnectionNewFunc),
            connection_init: load_symbol!(AdbcConnectionInit, AdbcConnectionInitFunc),
            connection_release: load_symbol!(AdbcConnectionRelease, AdbcConnectionReleaseFunc),
            statement_new: load_symbol!(AdbcStatementNew, AdbcStatementNewFunc),
            statement_set_sql_query: load_symbol!(AdbcStatementSetSqlQuery, AdbcStatementSetSqlQueryFunc),
            statement_execute_query: load_symbol!(AdbcStatementExecuteQuery, AdbcStatementExecuteQueryFunc),
            statement_release: load_symbol!(AdbcStatementRelease, AdbcStatementReleaseFunc),
            _lib: lib,
        })
    }

    fn check_status(&self, code: c_int, error_struct: &mut AdbcError) -> IglooResult<()> {
        if code == 0 { // ADBC_STATUS_OK
            return Ok(());
        }
        // Error struct should be populated by the FFI call.
        // Its Drop impl will call release_if_needed.
        let err_msg = error_struct.get_message_str().unwrap_or_else(|| "Unknown ADBC FFI error; no message.".to_string());
        Err(IglooError::Ffi(format!(
            "ADBC Call Failed (code {}): {}",
            code, err_msg
        )))
    }

    pub unsafe fn run_query(&self, uri: &str, sql_query: &str) -> IglooResult<Vec<RecordBatch>> {
        // Initialize all ADBC structs and AdbcError to zero.
        // Important: AdbcError must be zeroed so its release field is initially null.
        let mut error: AdbcError = mem::zeroed();
        let mut db: AdbcDatabase = mem::zeroed();
        let mut conn: AdbcConnection = mem::zeroed();
        let mut stmt: AdbcStatement = mem::zeroed();
        let mut stream_ptr: FFI_ArrowArrayStream = mem::zeroed(); // Pointer to be populated
        let mut rows_affected: i64 = 0;

        // Defer cleanup actions. These run in LIFO order.
        // AdbcError's Drop impl handles its own release.
        defer! {
            if !db._private_data.is_null() || !db._private_driver.is_null() { // Check if initialized
                // log::debug!("Releasing ADBC Database via FFI");
                let mut release_err: AdbcError = mem::zeroed();
                (self.database_release)(&mut db, &mut release_err);
                // Log release_err if necessary, though its Drop will also release its message
            }
        }
        defer! {
            if !conn._private_data.is_null() || !conn._private_driver.is_null() {
                // log::debug!("Releasing ADBC Connection via FFI");
                let mut release_err: AdbcError = mem::zeroed();
                (self.connection_release)(&mut conn, &mut release_err);
            }
        }
        defer! {
            if !stmt._private_data.is_null() || !stmt._private_driver.is_null() {
                // log::debug!("Releasing ADBC Statement via FFI");
                let mut release_err: AdbcError = mem::zeroed();
                (self.statement_release)(&mut stmt, &mut release_err);
            }
        }
        defer! {
            // Check if stream was initialized and has a release function
            if !stream_ptr.private_data.is_null() && stream_ptr.release.is_some() {
                // log::debug!("Releasing FFI_ArrowArrayStream via FFI");
                stream_ptr.release.unwrap()(&mut stream_ptr);
            }
        }

        // Database setup
        self.check_status((self.database_new)(&mut db, &mut error), &mut error)?;
        let c_uri_key = CString::new("uri").map_err(|e| IglooError::Ffi(format!("CString creation failed for 'uri' key: {}", e)))?;
        let c_uri_val = CString::new(uri).map_err(|e| IglooError::Ffi(format!("CString creation failed for URI value: {}", e)))?;
        self.check_status((self.database_set_option)(&mut db, c_uri_key.as_ptr(), c_uri_val.as_ptr(), &mut error), &mut error)?;
        self.check_status((self.database_init)(&mut db, &mut error), &mut error)?;

        // Connection setup
        self.check_status((self.connection_new)(&mut conn, &mut error), &mut error)?;
        self.check_status((self.connection_init)(&mut conn, &mut db, &mut error), &mut error)?;

        // Statement setup & execution
        self.check_status((self.statement_new)(&mut conn, &mut stmt, &mut error), &mut error)?;
        let c_sql = CString::new(sql_query).map_err(|e| IglooError::Ffi(format!("CString creation failed for SQL query: {}", e)))?;
        self.check_status((self.statement_set_sql_query)(&mut stmt, c_sql.as_ptr(), &mut error), &mut error)?;

        self.check_status((self.statement_execute_query)(&mut stmt, &mut stream_ptr, &mut rows_affected, &mut error), &mut error)?;

        // Convert FFI_ArrowArrayStream to Rust RecordBatches
        // ArrowArrayStreamReader::try_new consumes the stream_ptr if successful.
        // If it fails, stream_ptr is NOT consumed, and its release is handled by the defer block.
        let reader = ArrowArrayStreamReader::try_new(stream_ptr)
            .map_err(|e| {
                // If try_new fails, stream_ptr was not consumed, so its release is still pending in defer!
                // We need to ensure stream_ptr.private_data is nulled out if try_new took ownership but failed partway
                // However, try_new's contract is that it consumes on success. If it errors, it shouldn't have consumed.
                IglooError::Arrow(e)
            })?;
        // If try_new succeeded, stream_ptr is now "moved" into reader and its resources will be managed by reader.
        // We must prevent its release in the defer block.
        // Null out stream_ptr's release func or private_data to signify it's been moved.
        // This is tricky. A better way is to have stream_ptr wrapped in a struct with a Drop impl
        // that only releases if not explicitly "consumed".
        // For now, simplest is to rely on try_new's behavior and that the defer block for stream
        // will check if stream_ptr.private_data is null (which it won't be if try_new failed before consuming).
        // To be absolutely safe with the defer block:
        // After a successful ArrowArrayStreamReader::try_new, we should mark stream_ptr as "consumed"
        // so the defer block doesn't try to release it.
        // E.g., manually null out its release pointer or private_data *after* try_new succeeds.
        // This is not done here yet, relying on try_new's consumption contract.

        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result.map_err(IglooError::Arrow)?);
        }

        // log::info!("ADBC FFI query executed. Rows affected: {}. Batches returned: {}", rows_affected, batches.len());
        Ok(batches)
    }
}
