use libc::{c_char, c_int, c_void};
use std::ffi::{CStr, CString};
use std::ptr;
use std::mem;

// ADBC Status Codes
const ADBC_STATUS_OK: c_int = 0;

// Opaque structs for ADBC handles
#[repr(C)] pub enum AdbcDatabase {}
#[repr(C)] pub enum AdbcConnection {}

// AdbcError struct
#[repr(C)]
pub struct AdbcError {
    message: *mut c_char,
    vendor_code: i32,
    sqlstate: [c_char; 5],
    release: Option<unsafe extern "C" fn(error: *mut AdbcError)>,
    private_data: *mut c_void,
    private_driver: *mut c_void,
}

#[link(name = "adbc_driver_postgresql")]
#[link(name = "pq")]
extern "C" {
    fn AdbcDatabaseNew(database: *mut *mut AdbcDatabase, error: *mut AdbcError) -> c_int;
    fn AdbcDatabaseSetOption(
        database: *mut AdbcDatabase,
        key: *const c_char,
        value: *const c_char,
        error: *mut AdbcError,
    ) -> c_int;
    fn AdbcDatabaseInit(database: *mut AdbcDatabase, error: *mut AdbcError) -> c_int;
    fn AdbcDatabaseRelease(database: *mut AdbcDatabase, error: *mut AdbcError) -> c_int;
    fn AdbcConnectionNew(connection: *mut *mut AdbcConnection, error: *mut AdbcError) -> c_int;
    fn AdbcConnectionInit(
        connection: *mut AdbcConnection,
        database: *mut AdbcDatabase,
        error: *mut AdbcError,
    ) -> c_int;
    fn AdbcConnectionRelease(connection: *mut AdbcConnection, error: *mut AdbcError) -> c_int;
}

fn main() {
    let mut db: *mut AdbcDatabase = ptr::null_mut();
    let mut conn: *mut AdbcConnection = ptr::null_mut();
    let mut error: AdbcError = unsafe { mem::MaybeUninit::zeroed().assume_init() };

    unsafe {
        println!("Attempting ADBC connection to PostgreSQL...");

        let mut status = AdbcDatabaseNew(&mut db, &mut error);
        if status != ADBC_STATUS_OK {
            handle_adbc_error("AdbcDatabaseNew", status, &mut error);
            return;
        }
        if db.is_null() {
            eprintln!("AdbcDatabaseNew returned OK but database pointer is null.");
            if let Some(release_fn) = error.release { release_fn(&mut error); }
            return;
        }
        println!("AdbcDatabaseNew successful.");

        let uri_key = CString::new("uri").unwrap();
        let uri_val = CString::new("postgresql://postgres@localhost/igloo_test_db").unwrap();

        status = AdbcDatabaseSetOption(db, uri_key.as_ptr(), uri_val.as_ptr(), &mut error);
        if status != ADBC_STATUS_OK {
            handle_adbc_error("AdbcDatabaseSetOption (uri)", status, &mut error);
            AdbcDatabaseRelease(db, &mut error);
            return;
        }
        println!("AdbcDatabaseSetOption (uri) successful.");

        status = AdbcDatabaseInit(db, &mut error);
        if status != ADBC_STATUS_OK {
            handle_adbc_error("AdbcDatabaseInit", status, &mut error);
            AdbcDatabaseRelease(db, &mut error);
            return;
        }
        println!("AdbcDatabaseInit successful.");

        status = AdbcConnectionNew(&mut conn, &mut error);
        if status != ADBC_STATUS_OK {
            handle_adbc_error("AdbcConnectionNew", status, &mut error);
            AdbcDatabaseRelease(db, &mut error);
            return;
        }
        if conn.is_null() {
            eprintln!("AdbcConnectionNew returned OK but connection pointer is null.");
            if let Some(release_fn) = error.release { release_fn(&mut error); }
            AdbcDatabaseRelease(db, &mut error);
            return;
        }
        println!("AdbcConnectionNew successful.");

        status = AdbcConnectionInit(conn, db, &mut error);
        if status != ADBC_STATUS_OK {
            handle_adbc_error("AdbcConnectionInit", status, &mut error);
            AdbcConnectionRelease(conn, &mut error);
            AdbcDatabaseRelease(db, &mut error);
            return;
        }
        println!("ADBC Connection to PostgreSQL successful!");

        println!("Releasing connection...");
        status = AdbcConnectionRelease(conn, &mut error);
        if status != ADBC_STATUS_OK {
            handle_adbc_error("AdbcConnectionRelease", status, &mut error);
        } else {
            println!("Connection released successfully.");
        }
        conn = ptr::null_mut();

        println!("Releasing database...");
        status = AdbcDatabaseRelease(db, &mut error);
        if status != ADBC_STATUS_OK {
            handle_adbc_error("AdbcDatabaseRelease", status, &mut error);
        } else {
            println!("Database released successfully.");
        }
        db = ptr::null_mut();
    }
}

unsafe fn handle_adbc_error(operation: &str, status_code: c_int, error: *mut AdbcError) {
    eprint!("[ERROR] Operation '{}' failed with status code: {}. ", operation, status_code);
    if !error.is_null() && !(*error).message.is_null() {
        let error_message = CStr::from_ptr((*error).message);
        eprintln!("Details: {}", error_message.to_string_lossy());
        if let Some(release_fn) = (*error).release {
            release_fn(error);
            (*error).message = ptr::null_mut();
        }
    } else {
        eprintln!("No detailed error message available from ADBCError struct.");
    }
}
