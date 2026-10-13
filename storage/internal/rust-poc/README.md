# Go GCS SDK over a shared Rust core — proof of concept

**Not for production. Not part of the `cloud.google.com/go/storage` module.**

This directory holds the proof‑of‑concept and measurements for evaluating a
thin Go wrapper over the Rust `google-cloud-storage` crate, as part of the GCS
SDK consolidation investigation. Start with [EVALUATION.md](./EVALUATION.md).

Layout:

- `gcs_core/` — Rust C‑ABI core over the unmodified `google-cloud-storage` crate
  (async FFI, streaming reader/writer handles, paged list, cancellation).
- `gothin/gcsrust/` — cgo bindings; `gothin/rustthin/` — `storage`‑shaped Go
  facade; `gothin/bench2/` — one benchmark body compiled against both
  `*storage.Client` and `*rustthin.Client`; `gothin/cmd/` — binaries.
- `gothin/results/` — raw logs: `vm/`, `vm3/` (first POC), `vm4/`, `vm5/`
  (1:1 facade; `vm5` is the final pass with CPU/memory columns).
- `vm_startup*.sh`, `vm_round*.sh` — GCE startup scripts that build everything
  from source and run the matrices.

`gothin` is its own Go module (`poc/gothin`) so nothing here is imported by the
real SDK.
