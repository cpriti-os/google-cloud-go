//! Streaming handles so the Go side can offer the real `storage` API shape:
//! `NewReader` -> io.ReadCloser (pull), `NewWriter` -> io.WriteCloser (push),
//! `Objects` -> page-at-a-time iterator.
//!
//! All data-moving calls are async (tokio spawn + completion callback) so the
//! calling goroutine parks instead of pinning an OS thread.

use std::ffi::{CString, c_char, c_void};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::read_object::ReadObjectResponse;
use google_cloud_storage::streaming_source::{SizeHint, StreamingSource};
use tokio::sync::{Mutex, mpsc};

use crate::{GcsClient, GcsDoneCb, GcsStatus, bucket_name, cstr, err, ok, rt, sdk_err};

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

struct ReaderState {
    resp: ReadObjectResponse,
    leftover: Bytes, // tail of the last chunk that did not fit the Go buffer
    eof: bool,
}

pub struct GcsReader {
    state: Mutex<ReaderState>,
    // Snapshot of metadata for gcs_reader_attrs (sync, no lock needed).
    size: i64,
    generation: i64,
    metageneration: i64,
    content_type: CString,
    content_encoding: CString,
}

#[repr(C)]
pub struct GcsObjectAttrs {
    pub size: i64,
    pub generation: i64,
    pub metageneration: i64,
    /// Borrowed from the handle; valid until the handle is closed.
    pub content_type: *const c_char,
    pub content_encoding: *const c_char,
}

/// Opens a (range) read. Completion callback receives the handle pointer in
/// `n` (0 on error). `if_generation_match` < 0 means "not set".
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_reader_open_async(
    c: *const GcsClient,
    bucket: *const c_char,
    object: *const c_char,
    offset: i64,
    length: i64,
    generation: i64,
    if_generation_match: i64,
    cb: GcsDoneCb,
    user: *mut c_void,
) -> u64 {
    let (bucket, object) = match (unsafe { cstr(bucket) }, unsafe { cstr(object) }) {
        (Ok(b), Ok(o)) => (bucket_name(b), o.to_string()),
        _ => return 0,
    };
    let storage = unsafe { &*c }.storage.clone();
    crate::spawn_op(
        async move {
            let mut req = storage.read_object(bucket, object);
            // Mirror storage.NewRangeReader(ctx, offset, length) semantics.
            if length > 0 {
                req = req.set_read_range(ReadRange::segment(offset as u64, length as u64));
            } else if length < 0 && offset < 0 {
                req = req.set_read_range(ReadRange::tail((-offset) as u64));
            } else if offset > 0 {
                req = req.set_read_range(ReadRange::offset(offset as u64));
            }
            if generation > 0 {
                req = req.set_generation(generation);
            }
            if if_generation_match >= 0 {
                req = req.set_if_generation_match(if_generation_match);
            }
            let resp = req.send().await.map_err(SdkErr)?;
            let h = resp.object();
            let reader = GcsReader {
                size: h.size,
                generation: h.generation,
                metageneration: h.metageneration,
                content_type: CString::new(h.content_type.clone()).unwrap_or_default(),
                content_encoding: CString::new(h.content_encoding.clone()).unwrap_or_default(),
                state: Mutex::new(ReaderState { resp, leftover: Bytes::new(), eof: false }),
            };
            Ok::<u64, SdkErr>(Box::into_raw(Box::new(reader)) as u64)
        },
        cb,
        user,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_reader_attrs(r: *const GcsReader, out: *mut GcsObjectAttrs) {
    if r.is_null() || out.is_null() {
        return;
    }
    let r = unsafe { &*r };
    unsafe {
        *out = GcsObjectAttrs {
            size: r.size,
            generation: r.generation,
            metageneration: r.metageneration,
            content_type: r.content_type.as_ptr(),
            content_encoding: r.content_encoding.as_ptr(),
        }
    };
}

/// io.Reader semantics: fills `buf[0..cap)` as far as the stream allows
/// (returns early only at EOF) and reports `n`; `n == 0` with OK status means EOF.
/// Only one read may be in flight per handle (same rule as io.Reader).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_reader_read_async(
    r: *mut GcsReader,
    buf: *mut u8,
    cap: usize,
    cb: GcsDoneCb,
    user: *mut c_void,
) -> u64 {
    if r.is_null() || buf.is_null() || cap == 0 {
        return 0;
    }
    let r = unsafe { &*r };
    let dst = crate::SendPtr(buf);
    crate::spawn_op(
        async move {
            let dst = dst;
            let dst = unsafe { std::slice::from_raw_parts_mut(dst.0, cap) };
            let mut st = r.state.lock().await;
            let mut n = 0usize;
            loop {
                if !st.leftover.is_empty() {
                    let take = st.leftover.len().min(dst.len() - n);
                    dst[n..n + take].copy_from_slice(&st.leftover[..take]); // copy #2
                    st.leftover.advance(take);
                    n += take;
                }
                // Fill the caller's buffer (fewer FFI crossings) unless EOF.
                if n == dst.len() || st.eof {
                    return Ok(n as u64);
                }
                match st.resp.next().await {
                    Some(Ok(chunk)) => st.leftover = chunk,
                    Some(Err(e)) => return Err(SdkErr(e)),
                    None => st.eof = true,
                }
            }
        },
        cb,
        user,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_reader_close(r: *mut GcsReader) {
    if !r.is_null() {
        drop(unsafe { Box::from_raw(r) });
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Bridges Go `Write()` calls into the crate's StreamingSource.
struct ChanSource {
    rx: mpsc::Receiver<Bytes>,
    /// Exact size when the Go side already knows it (content ended inside the
    /// first ChunkSize, like storage.Writer) => crate picks single-shot.
    exact: Option<u64>,
}

impl StreamingSource for ChanSource {
    type Error = std::io::Error;
    async fn next(&mut self) -> Option<Result<Bytes, Self::Error>> {
        self.rx.recv().await.map(Ok)
    }
    async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
        Ok(match self.exact {
            Some(n) => SizeHint::with_exact(n),
            None => SizeHint::new(), // unknown => resumable upload
        })
    }
}

pub struct GcsWriter {
    tx: Mutex<Option<mpsc::Sender<Bytes>>>,
    upload: Mutex<Option<tokio::task::JoinHandle<Result<google_cloud_storage::model::Object, SdkErr>>>>,
    result: Mutex<Option<google_cloud_storage::model::Object>>,
}

/// Creates an upload. `chunk_size` sets the resumable buffer; 0 = default.
/// `if_generation_match` < 0 means "not set" (0 == DoesNotExist in Go).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_writer_open(
    c: *const GcsClient,
    bucket: *const c_char,
    object: *const c_char,
    content_type: *const c_char,
    if_generation_match: i64,
    chunk_size: usize,
    known_size: i64,
    out: *mut *mut GcsWriter,
) -> GcsStatus {
    crate::guard(|| {
        let (bucket, object) = match (unsafe { cstr(bucket) }, unsafe { cstr(object) }) {
            (Ok(b), Ok(o)) => (bucket_name(b), o.to_string()),
            (Err(e), _) | (_, Err(e)) => return e,
        };
        let content_type = if content_type.is_null() {
            None
        } else {
            unsafe { cstr(content_type) }.ok().filter(|s| !s.is_empty()).map(|s| s.to_string())
        };
        let storage = unsafe { &*c }.storage.clone();
        // Bounded: gives Write() backpressure similar to Go's ChunkSize buffer.
        let (tx, rx) = mpsc::channel::<Bytes>(4);
        let upload = rt().spawn(async move {
            let exact = if known_size >= 0 { Some(known_size as u64) } else { None };
            let mut req = storage.write_object::<_, _, _, ChanSource>(bucket, object, ChanSource { rx, exact });
            if let Some(ct) = content_type {
                req = req.set_content_type(ct);
            }
            if if_generation_match >= 0 {
                req = req.set_if_generation_match(if_generation_match);
            }
            if chunk_size > 0 {
                req = req.with_resumable_upload_buffer_size(chunk_size);
            }
            req.send_buffered().await.map_err(SdkErr)
        });
        let w = GcsWriter {
            tx: Mutex::new(Some(tx)),
            upload: Mutex::new(Some(upload)),
            result: Mutex::new(None),
        };
        unsafe { *out = Box::into_raw(Box::new(w)) };
        ok()
    })
}

/// Copies `data` (Go memory; cannot be retained) and queues it. Completes when
/// the chunk is accepted by the upload pipeline (backpressure), not when it
/// is on the wire — same contract as Go's Writer.Write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_writer_write_async(
    w: *mut GcsWriter,
    data: *const u8,
    len: usize,
    cb: GcsDoneCb,
    user: *mut c_void,
) -> u64 {
    if w.is_null() || (data.is_null() && len > 0) {
        return 0;
    }
    let w = unsafe { &*w };
    let chunk = Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len) }); // copy #1
    crate::spawn_op(
        async move {
            let tx = w.tx.lock().await;
            match tx.as_ref() {
                Some(tx) => tx
                    .send(chunk)
                    .await
                    .map(|_| len as u64)
                    .map_err(|_| SdkErrMsg("upload aborted (see Close for the cause)".into())),
                None => Err(SdkErrMsg("write after close".into())),
            }
        },
        cb,
        user,
    )
}

/// Finishes the upload; `n` = generation on success.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_writer_close_async(w: *mut GcsWriter, cb: GcsDoneCb, user: *mut c_void) -> u64 {
    if w.is_null() {
        return 0;
    }
    let w = unsafe { &*w };
    crate::spawn_op(
        async move {
            w.tx.lock().await.take(); // close channel => source returns None
            let jh = w.upload.lock().await.take();
            let obj = match jh {
                Some(jh) => jh.await.map_err(|e| SdkErrMsg(format!("upload task: {e}")))??,
                None => return Err(SdkErrMsg("close called twice".into())),
            };
            let generation = obj.generation;
            *w.result.lock().await = Some(obj);
            Ok(generation as u64)
        },
        cb,
        user,
    )
}

/// After a successful close: size/generation of the created object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_writer_result(w: *const GcsWriter, out_size: *mut i64, out_generation: *mut i64) -> i32 {
    if w.is_null() {
        return 0;
    }
    let w = unsafe { &*w };
    match rt().block_on(w.result.lock()).as_ref() {
        Some(o) => {
            unsafe {
                *out_size = o.size;
                *out_generation = o.generation;
            }
            1
        }
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_writer_free(w: *mut GcsWriter) {
    if !w.is_null() {
        drop(unsafe { Box::from_raw(w) });
    }
}

// ---------------------------------------------------------------------------
// List: one page per call (mirrors ObjectIterator paging). Sync/blocking.
// ---------------------------------------------------------------------------

/// Returns a JSON page: {"items":[{name,size,generation,metageneration,
/// content_type,updated,crc32c,storage_class}],"prefixes":[...],"next":""}.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_list_page(
    c: *const GcsClient,
    bucket: *const c_char,
    prefix: *const c_char,
    delimiter: *const c_char,
    start_offset: *const c_char,
    end_offset: *const c_char,
    page_token: *const c_char,
    page_size: i32,
    versions: bool,
    out_json: *mut *mut c_char,
) -> GcsStatus {
    crate::guard(|| {
        let s = |p: *const c_char| -> String {
            if p.is_null() { String::new() } else { unsafe { cstr(p) }.map(|s| s.to_string()).unwrap_or_default() }
        };
        let bucket = match unsafe { cstr(bucket) } {
            Ok(b) => bucket_name(b),
            Err(e) => return e,
        };
        let (prefix, delimiter, so, eo, tok) = (s(prefix), s(delimiter), s(start_offset), s(end_offset), s(page_token));
        let client = unsafe { &*c };
        let res = rt().block_on(async {
            let mut req = client
                .control
                .list_objects()
                .set_parent(bucket)
                .set_prefix(prefix)
                .set_delimiter(delimiter)
                .set_page_token(tok)
                .set_versions(versions);
            if !so.is_empty() {
                req = req.set_lexicographic_start(so);
            }
            if !eo.is_empty() {
                req = req.set_lexicographic_end(eo);
            }
            if page_size > 0 {
                req = req.set_page_size(page_size);
            }
            req.send().await
        });
        match res {
            Ok(resp) => {
                let items: Vec<serde_json::Value> = resp
                    .objects
                    .iter()
                    .map(|o| {
                        serde_json::json!({
                            "name": o.name,
                            "size": o.size,
                            "generation": o.generation,
                            "metageneration": o.metageneration,
                            "content_type": o.content_type,
                            "storage_class": o.storage_class,
                            "updated_secs": o.update_time.as_ref().map(|t| t.seconds()).unwrap_or(0),
                            "crc32c": o.checksums.as_ref().and_then(|c| c.crc32c).unwrap_or(0),
                        })
                    })
                    .collect();
                let page = serde_json::json!({
                    "items": items,
                    "prefixes": resp.prefixes,
                    "next": resp.next_page_token,
                });
                unsafe { *out_json = CString::new(page.to_string()).unwrap().into_raw() };
                ok()
            }
            Err(e) => sdk_err(&e),
        }
    })
}

// ---------------------------------------------------------------------------
// Error plumbing for spawn_op: carry HTTP status through the callback.
// ---------------------------------------------------------------------------

pub(crate) struct SdkErr(pub google_cloud_storage::Error);
pub(crate) struct SdkErrMsg(pub String);

pub(crate) trait IntoStatus {
    fn into_status(self) -> GcsStatus;
}
impl IntoStatus for SdkErr {
    fn into_status(self) -> GcsStatus {
        sdk_err(&self.0)
    }
}
impl IntoStatus for SdkErrMsg {
    fn into_status(self) -> GcsStatus {
        err(1, self.0)
    }
}
impl IntoStatus for String {
    fn into_status(self) -> GcsStatus {
        err(1, self)
    }
}
impl From<SdkErr> for SdkErrMsg {
    fn from(e: SdkErr) -> Self {
        SdkErrMsg(e.0.to_string())
    }
}

#[allow(dead_code)]
fn _assert_send(_: Arc<GcsReader>) {}
