// Minimal FFI wrapper for ADBC Postgres driver
use libloading::{Library, Symbol};
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;

// Define the function signatures you need from the C API
// These are examples; real signatures may differ
#[allow(non_camel_case_types)]
pub type AdbcDatabaseInit = unsafe extern "C" fn(db: *mut AdbcDatabase, error: *mut AdbcError) -> c_int;
#[allow(non_camel_case_types)]
pub type AdbcDatabaseNew = unsafe extern "C" fn(db: *mut AdbcDatabase, error: *mut AdbcError) -> c_int;
#[allow(non_camel_case_types)]
pub type AdbcDatabaseSetOption = unsafe extern "C" fn(db: *mut AdbcDatabase, key: *const c_char, value: *const c_char, error: *mut AdbcError) -> c_int;
#[allow(non_camel_case_types)]
pub type AdbcConnectionInit = unsafe extern "C" fn(conn: *mut AdbcConnection, db: *mut AdbcDatabase, error: *mut AdbcError) -> c_int;
#[allow(non_camel_case_types)]
pub type AdbcStatementNew = unsafe extern "C" fn(stmt: *mut AdbcStatement, conn: *mut AdbcConnection, error: *mut AdbcError) -> c_int;
#[allow(non_camel_case_types)]
pub type AdbcStatementSetSqlQuery = unsafe extern "C" fn(stmt: *mut AdbcStatement, query: *const c_char, error: *mut AdbcError) -> c_int;
#[allow(non_camel_case_types)]
pub type AdbcStatementExecuteQuery = unsafe extern "C" fn(stmt: *mut AdbcStatement, out: *mut std::ffi::c_void, error: *mut AdbcError) -> c_int;

#[repr(C)]
pub struct AdbcDatabase {
    private_data: [u8; 128],
}

#[repr(C)]
pub struct AdbcConnection {
    private_data: [u8; 128],
}

#[repr(C)]
pub struct AdbcStatement {
    private_data: [u8; 256],
}

#[repr(C)]
pub struct AdbcError {
    pub message: *const c_char,
    pub vendor_code: i32,
    pub sqlstate: [u8; 5],
    pub release: Option<unsafe extern "C" fn(error: *mut AdbcError)>,
    pub private_data: [u8; 192],
}

pub struct AdbcPostgresFFI {
    // Remove _lib field entirely, since we leak the library and only need to keep it alive
    pub database_init: Symbol<'static, AdbcDatabaseInit>,
    pub database_new: Symbol<'static, AdbcDatabaseNew>,
    pub database_set_option: Symbol<'static, AdbcDatabaseSetOption>,
    pub connection_init: Symbol<'static, AdbcConnectionInit>,
    pub statement_new: Symbol<'static, AdbcStatementNew>,
    pub statement_set_sql_query: Symbol<'static, AdbcStatementSetSqlQuery>,
    pub statement_execute_query: Symbol<'static, AdbcStatementExecuteQuery>,
}

impl AdbcPostgresFFI {
    pub unsafe fn new(lib_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Box and leak the library to extend its lifetime
        let lib = Box::leak(Box::new(Library::new(lib_path)?));
        let database_init: Symbol<AdbcDatabaseInit> = lib.get(b"AdbcDatabaseInit")?;
        let database_new: Symbol<AdbcDatabaseNew> = lib.get(b"AdbcDatabaseNew")?;
        let database_set_option: Symbol<AdbcDatabaseSetOption> = lib.get(b"AdbcDatabaseSetOption")?;
        let connection_init: Symbol<AdbcConnectionInit> = lib.get(b"AdbcConnectionInit")?;
        let statement_new: Symbol<AdbcStatementNew> = lib.get(b"AdbcStatementNew")?;
        let statement_set_sql_query: Symbol<AdbcStatementSetSqlQuery> = lib.get(b"AdbcStatementSetSqlQuery")?;
        let statement_execute_query: Symbol<AdbcStatementExecuteQuery> = lib.get(b"AdbcStatementExecuteQuery")?;
        Ok(Self {
            database_init,
            database_new,
            database_set_option,
            connection_init,
            statement_new,
            statement_set_sql_query,
            statement_execute_query,
        })
    }

    // Minimal example: run a query
    pub unsafe fn run_query(&self, uri: &str, sql: &str) -> Result<(), String> {
        let mut db = AdbcDatabase { private_data: [0; 128] };
        let mut err = AdbcError {
            message: ptr::null(),
            vendor_code: 0,
            sqlstate: [0; 5],
            release: None,
            private_data: [0; 192],
        };
        let mut conn = AdbcConnection { private_data: [0; 128] };
        let mut stmt = AdbcStatement { private_data: [0; 256] };

        // New database
        if (self.database_new)(&mut db, &mut err) != 0 {
            return Err("database_new failed".to_string());
        }
        // Set URI option
        let key = CString::new("uri").unwrap();
        let value = CString::new(uri).unwrap();
        if (self.database_set_option)(&mut db, key.as_ptr(), value.as_ptr(), &mut err) != 0 {
            return Err("database_set_option failed".to_string());
        }
        // Init database
        if (self.database_init)(&mut db, &mut err) != 0 {
            return Err("database_init failed".to_string());
        }
        // Init connection
        if (self.connection_init)(&mut conn, &mut db, &mut err) != 0 {
            return Err("connection_init failed".to_string());
        }
        // New statement
        if (self.statement_new)(&mut stmt, &mut conn, &mut err) != 0 {
            return Err("statement_new failed".to_string());
        }
        // Set SQL query
        let sql_c = CString::new(sql).unwrap();
        if (self.statement_set_sql_query)(&mut stmt, sql_c.as_ptr(), &mut err) != 0 {
            return Err("statement_set_sql_query failed".to_string());
        }
        // Execute query (output ignored for this minimal example)
        if (self.statement_execute_query)(&mut stmt, ptr::null_mut(), &mut err) != 0 {
            return Err("statement_execute_query failed".to_string());
        }
        Ok(())
    }
}
