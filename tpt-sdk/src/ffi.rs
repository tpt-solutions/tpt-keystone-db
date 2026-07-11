//! `extern "C"` bindings for C/C++ interop — the "FFI bindings" checklist
//! item. Covers connect/query/read-result/free; there is no FFI surface for
//! streaming or async callbacks (see `lib.rs`'s module doc).
//!
//! Result cells are handed back as borrowed pointers into the
//! [`crate::keystone::QueryResult`] the handle owns (mirroring
//! [`crate::zerocopy::RowView`]) rather than copied into freshly allocated
//! buffers — the caller must not use them past `tpt_sdk_free_result`.
//!
//! Every function is safe to call only per its documented preconditions;
//! that's inherent to a C ABI, not something this module can enforce.

use std::ffi::{c_char, CStr, CString};
use std::sync::Mutex;

use crate::keystone::blocking::Client;
use crate::keystone::QueryResult;

thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<CString>> = std::cell::RefCell::new(None);
}

fn set_last_error(msg: impl std::fmt::Display) {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = CString::new(msg.to_string()).ok();
    });
}

/// Returns a pointer to the last error message set on this thread, or null
/// if there wasn't one. Valid until the next SDK call on this thread.
#[no_mangle]
pub extern "C" fn tpt_sdk_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| slot.borrow().as_ref().map_or(std::ptr::null(), |c| c.as_ptr()))
}

pub struct ClientHandle(Mutex<Client>);
pub struct ResultHandle(QueryResult);

/// Connect to a Keystone node. `addr` is a nul-terminated `"host:port"`
/// string. Returns null on failure (check [`tpt_sdk_last_error`]).
///
/// # Safety
/// `addr` must be a valid pointer to a nul-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn tpt_sdk_connect(addr: *const c_char) -> *mut ClientHandle {
    if addr.is_null() {
        set_last_error("addr is null");
        return std::ptr::null_mut();
    }
    let addr = match CStr::from_ptr(addr).to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return std::ptr::null_mut();
        }
    };
    match Client::connect(addr) {
        Ok(client) => Box::into_raw(Box::new(ClientHandle(Mutex::new(client)))),
        Err(e) => {
            set_last_error(e);
            std::ptr::null_mut()
        }
    }
}

/// Run a query. `sql` is a nul-terminated UTF-8 C string. Returns null on
/// failure (check [`tpt_sdk_last_error`]).
///
/// # Safety
/// `client` must be a live pointer returned by [`tpt_sdk_connect`]; `sql`
/// must be a valid nul-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn tpt_sdk_query(client: *mut ClientHandle, sql: *const c_char) -> *mut ResultHandle {
    if client.is_null() || sql.is_null() {
        set_last_error("client or sql is null");
        return std::ptr::null_mut();
    }
    let sql = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return std::ptr::null_mut();
        }
    };
    let handle = &*client;
    let mut guard = match handle.0.lock() {
        Ok(g) => g,
        Err(e) => {
            set_last_error(e);
            return std::ptr::null_mut();
        }
    };
    match guard.query(sql) {
        Ok(result) => Box::into_raw(Box::new(ResultHandle(result))),
        Err(e) => {
            set_last_error(e);
            std::ptr::null_mut()
        }
    }
}

/// # Safety
/// `result` must be a live pointer returned by [`tpt_sdk_query`].
#[no_mangle]
pub unsafe extern "C" fn tpt_sdk_result_row_count(result: *const ResultHandle) -> usize {
    if result.is_null() {
        return 0;
    }
    (*result).0.rows.len()
}

/// # Safety
/// `result` must be a live pointer returned by [`tpt_sdk_query`].
#[no_mangle]
pub unsafe extern "C" fn tpt_sdk_result_column_count(result: *const ResultHandle) -> usize {
    if result.is_null() {
        return 0;
    }
    (*result).0.columns.len()
}

/// Borrow cell `(row, col)`'s raw bytes. Writes the cell's length to
/// `out_len` and returns a pointer valid until `result` is freed, or null
/// if the cell is SQL NULL or out of bounds (`*out_len` is set to 0 in that
/// case).
///
/// # Safety
/// `result` must be a live pointer returned by [`tpt_sdk_query`]; `out_len`
/// must point to a valid, writable `usize`.
#[no_mangle]
pub unsafe extern "C" fn tpt_sdk_result_cell(
    result: *const ResultHandle,
    row: usize,
    col: usize,
    out_len: *mut usize,
) -> *const u8 {
    if result.is_null() || out_len.is_null() {
        return std::ptr::null();
    }
    *out_len = 0;
    let Some(row) = (*result).0.rows.get(row) else { return std::ptr::null() };
    let Some(cell) = row.get(col) else { return std::ptr::null() };
    *out_len = cell.len();
    cell.as_ptr()
}

/// # Safety
/// `result` must be a pointer previously returned by [`tpt_sdk_query`],
/// not already freed.
#[no_mangle]
pub unsafe extern "C" fn tpt_sdk_free_result(result: *mut ResultHandle) {
    if !result.is_null() {
        drop(Box::from_raw(result));
    }
}

/// # Safety
/// `client` must be a pointer previously returned by [`tpt_sdk_connect`],
/// not already freed.
#[no_mangle]
pub unsafe extern "C" fn tpt_sdk_free_client(client: *mut ClientHandle) {
    if !client.is_null() {
        drop(Box::from_raw(client));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_result_accessors_are_safe_and_zero() {
        assert_eq!(unsafe { tpt_sdk_result_row_count(std::ptr::null()) }, 0);
        assert_eq!(unsafe { tpt_sdk_result_column_count(std::ptr::null()) }, 0);
    }

    #[test]
    fn free_on_null_is_a_noop() {
        unsafe { tpt_sdk_free_result(std::ptr::null_mut()) };
        unsafe { tpt_sdk_free_client(std::ptr::null_mut()) };
    }

    #[test]
    fn last_error_is_null_before_any_call() {
        assert!(unsafe { tpt_sdk_last_error() }.is_null());
    }
}
