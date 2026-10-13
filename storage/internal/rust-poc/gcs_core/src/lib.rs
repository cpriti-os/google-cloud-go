//! gcs_core: a minimal C-ABI "thick client" over the Rust GCS SDK
//! (google-cloud-storage crate), intended to be wrapped by a thin Go layer
//! via cgo. POC only.
//!
//! Design notes (each of these is a real cost of the architecture):
//!  * A private tokio multi-thread runtime lives inside the library. Every FFI
//!    entry point does `block_on`, which parks the calling OS thread (in Go,
//!    that is an M pinned by the goroutine for the entire RPC).
//!  * Panics must never unwind across `extern "C"`; everything is wrapped in
//!    `catch_unwind`.
//!  * Go memory cannot be retained by Rust after the call returns (cgo pointer
//!    rules), so uploads copy Go `[]byte` -> `bytes::Bytes` (one extra copy) and
//!    downloads copy `Bytes` chunks -> Go buffer (one extra copy).
//!  * Errors are flattened to (code, message string); the Go side loses the
//!    structured `googleapi.Error` / `status.Status` it has today.

use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use bytes::Bytes;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model_ext::ReadRange;
use tokio::runtime::Runtime;

static RT: OnceLock<Runtime> = OnceLock::new();

pub(crate) fn rt() -> &'static Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("gcs-core-tokio")
            .build()
            .expect("tokio runtime")
    })
}

pub struct GcsClient {
    storage: Storage,
    control: StorageControl,
}

#[repr(C)]
pub struct GcsStatus {
    /// 0 = OK, 1 = generic error, 2 = panic, 3 = bad argument, 4 = cancelled
    pub code: i32,
    /// HTTP status code if the error came from the service (0 otherwise).
    pub http_code: i32,
    /// Heap-allocated C string owned by Rust; free with gcs_string_free. NULL on OK.
    pub message: *mut c_char,
}

fn ok() -> GcsStatus {
    GcsStatus { code: 0, http_code: 0, message: std::ptr::null_mut() }
}

fn err(code: i32, msg: impl std::fmt::Display) -> GcsStatus {
    let s = CString::new(msg.to_string().replace('\0', " ")).unwrap();
    GcsStatus { code, http_code: 0, message: s.into_raw() }
}

/// Error from the SDK: keep the HTTP status so the Go side can rebuild
/// googleapi.Error / storage.ErrObjectNotExist.
fn sdk_err(e: &google_cloud_storage::Error) -> GcsStatus {
    let mut st = err(1, e);
    st.http_code = e.http_status_code().map(|c| c as i32).unwrap_or(0);
    st
}

pub(crate) use stream::*;
mod stream;

pub(crate) fn guard<F: FnOnce() -> GcsStatus>(f: F) -> GcsStatus {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(s) => s,
        Err(p) => {
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".into());
            err(2, format!("rust panic: {msg}"))
        }
    }
}

pub(crate) unsafe fn cstr<'a>(p: *const c_char) -> Result<&'a str, GcsStatus> {
    if p.is_null() {
        return Err(err(3, "null string argument"));
    }
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|e| err(3, format!("invalid utf-8: {e}")))
}

pub(crate) fn bucket_name(b: &str) -> String {
    if b.starts_with("projects/") { b.to_string() } else { format!("projects/_/buckets/{b}") }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Creates a client using Application Default Credentials.
/// `endpoint` may be NULL for the default (https://storage.googleapis.com).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_client_new(
    endpoint: *const c_char,
    out: *mut *mut GcsClient,
) -> GcsStatus {
    guard(|| {
        if out.is_null() {
            return err(3, "null out pointer");
        }
        let endpoint = if endpoint.is_null() {
            None
        } else {
            match unsafe { cstr(endpoint) } {
                Ok(s) => Some(s.to_string()),
                Err(e) => return e,
            }
        };
        let res = rt().block_on(async {
            let mut sb = Storage::builder();
            let mut cb = StorageControl::builder();
            if let Some(e) = &endpoint {
                sb = sb.with_endpoint(e.clone());
                cb = cb.with_endpoint(e.clone());
            }
            let storage = sb.build().await.map_err(|e| format!("storage build: {e}"))?;
            let control = cb.build().await.map_err(|e| format!("control build: {e}"))?;
            Ok::<_, String>(GcsClient { storage, control })
        });
        match res {
            Ok(c) => {
                unsafe { *out = Box::into_raw(Box::new(c)) };
                ok()
            }
            Err(e) => err(1, e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_client_free(c: *mut GcsClient) {
    if !c.is_null() {
        drop(unsafe { Box::from_raw(c) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

/// No-op used to measure raw cgo call overhead.
#[unsafe(no_mangle)]
pub extern "C" fn gcs_noop(x: u64) -> u64 {
    x.wrapping_add(1)
}

/// Calls `cb` `n` times; used to measure Rust->Go callback overhead.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_callback_bench(
    n: u64,
    cb: Option<unsafe extern "C" fn(*mut c_void, u64) -> u64>,
    user: *mut c_void,
) -> u64 {
    let cb = match cb {
        Some(cb) => cb,
        None => return 0,
    };
    let mut acc = 0u64;
    for i in 0..n {
        acc = acc.wrapping_add(unsafe { cb(user, i) });
    }
    acc
}

// ---------------------------------------------------------------------------
// Data plane: JSON ReadObject / WriteObject
// ---------------------------------------------------------------------------

/// Downloads `[offset, offset+len)` (len == 0 => to EOF) into the caller
/// supplied buffer `buf[0..cap)`. Writes the number of bytes copied to
/// `*out_len`. Fails if the object is larger than `cap`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_read_object(
    c: *const GcsClient,
    bucket: *const c_char,
    object: *const c_char,
    offset: u64,
    len: u64,
    buf: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> GcsStatus {
    guard(|| {
        if c.is_null() || buf.is_null() || out_len.is_null() {
            return err(3, "null argument");
        }
        let (bucket, object) = match (unsafe { cstr(bucket) }, unsafe { cstr(object) }) {
            (Ok(b), Ok(o)) => (bucket_name(b), o.to_string()),
            (Err(e), _) | (_, Err(e)) => return e,
        };
        let client = unsafe { &*c };
        let dst = unsafe { std::slice::from_raw_parts_mut(buf, cap) };
        let res = rt().block_on(async {
            let mut req = client.storage.read_object(bucket, object);
            if len > 0 {
                req = req.set_read_range(ReadRange::segment(offset, len));
            } else if offset > 0 {
                req = req.set_read_range(ReadRange::offset(offset));
            }
            let mut reader = req.send().await.map_err(|e| e.to_string())?;
            let mut n = 0usize;
            while let Some(chunk) = reader.next().await.transpose().map_err(|e| e.to_string())? {
                if n + chunk.len() > dst.len() {
                    return Err(format!("buffer too small: cap={} need>{}", dst.len(), n + chunk.len()));
                }
                dst[n..n + chunk.len()].copy_from_slice(&chunk); // copy #2 (Bytes -> Go buf)
                n += chunk.len();
            }
            Ok::<usize, String>(n)
        });
        match res {
            Ok(n) => {
                unsafe { *out_len = n };
                ok()
            }
            Err(e) => err(1, e),
        }
    })
}

/// Uploads `data[0..len)` as `object`. `resumable_threshold` of 0 keeps the
/// library default.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_write_object(
    c: *const GcsClient,
    bucket: *const c_char,
    object: *const c_char,
    data: *const u8,
    len: usize,
    out_generation: *mut i64,
) -> GcsStatus {
    guard(|| {
        if c.is_null() || (data.is_null() && len > 0) {
            return err(3, "null argument");
        }
        let (bucket, object) = match (unsafe { cstr(bucket) }, unsafe { cstr(object) }) {
            (Ok(b), Ok(o)) => (bucket_name(b), o.to_string()),
            (Err(e), _) | (_, Err(e)) => return e,
        };
        let client = unsafe { &*c };
        // cgo rules: Go memory may not be retained after return and the upload
        // may be retried => must copy into Rust-owned memory. copy #1.
        let payload = Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len) });
        let res = rt().block_on(async {
            client
                .storage
                .write_object(bucket, object, payload)
                .send_buffered()
                .await
                .map_err(|e| e.to_string())
        });
        match res {
            Ok(obj) => {
                if !out_generation.is_null() {
                    unsafe { *out_generation = obj.generation };
                }
                ok()
            }
            Err(e) => err(1, e),
        }
    })
}

// ---------------------------------------------------------------------------
// Async FFI variant: spawn on tokio, complete via callback into Go.
// The calling OS thread is NOT blocked; the Go side parks a goroutine on a
// channel instead. Cancellation: Go calls gcs_cancel(op_id) when ctx is done.
// ---------------------------------------------------------------------------

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::AbortHandle;

/// Completion callback. `status.message` (if non-NULL) must be freed by the
/// callee with gcs_string_free. `n` = bytes read (reads) or generation (writes).
pub type GcsDoneCb = unsafe extern "C" fn(user: *mut c_void, status: GcsStatus, n: u64);

static NEXT_OP: AtomicU64 = AtomicU64::new(1);
static INFLIGHT: OnceLock<Mutex<HashMap<u64, AbortHandle>>> = OnceLock::new();
fn inflight() -> &'static Mutex<HashMap<u64, AbortHandle>> {
    INFLIGHT.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Raw pointers are !Send; the Go side guarantees (via runtime.Pinner) that
/// the buffer stays valid and unmoved until the callback fires.
pub(crate) struct SendPtr<T>(pub(crate) *mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

pub(crate) fn spawn_op<F, E>(fut: F, cb: GcsDoneCb, user: *mut c_void) -> u64
where
    F: std::future::Future<Output = Result<u64, E>> + Send + 'static,
    E: IntoStatus + Send + 'static,
{
    let id = NEXT_OP.fetch_add(1, Ordering::Relaxed);
    let user = SendPtr(user as *mut u8);
    let jh = rt().spawn(fut);
    inflight().lock().unwrap().insert(id, jh.abort_handle());
    rt().spawn(async move {
        let r = jh.await;
        inflight().lock().unwrap().remove(&id);
        let user = user; // move the whole wrapper
        let (st, n) = match r {
            Ok(Ok(n)) => (ok(), n),
            Ok(Err(e)) => (e.into_status(), 0),
            Err(je) if je.is_cancelled() => (err(4, "cancelled"), 0),
            Err(je) => (err(2, format!("rust panic: {je}")), 0),
        };
        // Callback into Go from a tokio worker thread.
        unsafe { cb(user.0 as *mut c_void, st, n) };
    });
    id
}

/// Cancels an in-flight async op. The completion callback still fires
/// (with code 4) so the caller knows the buffer is no longer in use.
#[unsafe(no_mangle)]
pub extern "C" fn gcs_cancel(op_id: u64) {
    if let Some(h) = inflight().lock().unwrap().get(&op_id) {
        h.abort();
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_read_object_async(
    c: *const GcsClient,
    bucket: *const c_char,
    object: *const c_char,
    offset: u64,
    len: u64,
    buf: *mut u8,
    cap: usize,
    cb: GcsDoneCb,
    user: *mut c_void,
) -> u64 {
    let (bucket, object) = match (unsafe { cstr(bucket) }, unsafe { cstr(object) }) {
        (Ok(b), Ok(o)) => (bucket_name(b), o.to_string()),
        _ => return 0,
    };
    let storage = unsafe { &*c }.storage.clone();
    let dst = SendPtr(buf);
    spawn_op(
        async move {
            let dst = dst;
            let dst = unsafe { std::slice::from_raw_parts_mut(dst.0, cap) };
            let mut req = storage.read_object(bucket, object);
            if len > 0 {
                req = req.set_read_range(ReadRange::segment(offset, len));
            } else if offset > 0 {
                req = req.set_read_range(ReadRange::offset(offset));
            }
            let mut reader = req.send().await.map_err(|e| e.to_string())?;
            let mut n = 0usize;
            while let Some(chunk) = reader.next().await.transpose().map_err(|e| e.to_string())? {
                if n + chunk.len() > dst.len() {
                    return Err(format!("buffer too small: cap={} need>{}", dst.len(), n + chunk.len()));
                }
                dst[n..n + chunk.len()].copy_from_slice(&chunk);
                n += chunk.len();
            }
            Ok::<u64, String>(n as u64)
        },
        cb,
        user,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_write_object_async(
    c: *const GcsClient,
    bucket: *const c_char,
    object: *const c_char,
    data: *const u8,
    len: usize,
    cb: GcsDoneCb,
    user: *mut c_void,
) -> u64 {
    let (bucket, object) = match (unsafe { cstr(bucket) }, unsafe { cstr(object) }) {
        (Ok(b), Ok(o)) => (bucket_name(b), o.to_string()),
        _ => return 0,
    };
    let storage = unsafe { &*c }.storage.clone();
    // Still a copy: the upload may be retried after Go's slice is reused, and
    // bytes::Bytes needs owned ('static) memory.
    let payload = Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len) });
    spawn_op(
        async move {
            let obj = storage
                .write_object(bucket, object, payload)
                .send_buffered()
                .await
                .map_err(|e| e.to_string())?;
            Ok::<u64, String>(obj.generation as u64)
        },
        cb,
        user,
    )
}

// ---------------------------------------------------------------------------
// ListObjects (StorageControl / gRPC in the Rust SDK)
// ---------------------------------------------------------------------------

/// Lists all objects under `prefix` (may be NULL). Returns a newline-separated
/// list of names in `*out_names` (Rust owned; free with gcs_string_free) and
/// the count in `*out_count`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_list_objects(
    c: *const GcsClient,
    bucket: *const c_char,
    prefix: *const c_char,
    out_names: *mut *mut c_char,
    out_count: *mut usize,
) -> GcsStatus {
    guard(|| {
        if c.is_null() || out_names.is_null() || out_count.is_null() {
            return err(3, "null argument");
        }
        let bucket = match unsafe { cstr(bucket) } {
            Ok(b) => bucket_name(b),
            Err(e) => return e,
        };
        let prefix = if prefix.is_null() {
            String::new()
        } else {
            match unsafe { cstr(prefix) } {
                Ok(p) => p.to_string(),
                Err(e) => return e,
            }
        };
        let client = unsafe { &*c };
        let res = rt().block_on(async {
            use google_cloud_storage::Result;
            let mut names = String::new();
            let mut count = 0usize;
            let mut page_token = String::new();
            loop {
                let resp = client
                    .control
                    .list_objects()
                    .set_parent(bucket.clone())
                    .set_prefix(prefix.clone())
                    .set_page_token(page_token.clone())
                    .send()
                    .await?;
                for o in resp.objects {
                    names.push_str(&o.name);
                    names.push('\n');
                    count += 1;
                }
                if resp.next_page_token.is_empty() {
                    break;
                }
                page_token = resp.next_page_token;
            }
            Result::Ok((names, count))
        });
        match res {
            Ok((names, count)) => {
                unsafe {
                    *out_names = CString::new(names).unwrap().into_raw();
                    *out_count = count;
                }
                ok()
            }
            Err(e) => err(1, e),
        }
    })
}
