# Runnable HTTP text-input probe

Build the example and run the explicit executable through the optional text profile:

```sh
cargo build -p b4-http --example serve_permission_reuse --locked --offline
python3 -B scripts/check_http_always_example.py --example "$PWD/target/debug/examples/serve_permission_reuse" --text-inputs
```

The probe uses distinct 2,048-byte ASCII note strings for original Always execution and saved-permission reuse. Each compact input is 2,060 bytes, two values, depth one. It checks the complete committed attempt/result, all nine durable tables, immutable original history and permission provenance, normal owner retirement/reopen, database integrity, and temporary cleanup. It measures the real parked snapshot after the first owner exits; that snapshot must exceed 4,096 bytes and stay within 8,192 bytes and 512 canonical nodes.

`--text-inputs` and `--nested-inputs` are mutually exclusive. Default and nested modes retain their existing checks and outputs. Exact default-ASCII/spaced transport-body bytes are measured separately from compact canonical input bytes. Process evidence establishes committed attempts/results; it is not instrumented callback counting.

Fresh checks: example build, text/nested/default Always probes, original embedded reuse probe, Python source compilation and separate contract metadata. All receipts bind the same source and final executable in [qualification.json](qualification.json). All seven checks passed. Executable SHA-256: `63635cdb6618ac8bbacd5b4671287455d3b75926feaeb81016ad2eb2fab54a01`. Actual text HTTP request bodies were 2,099/2,099 bytes; the parked snapshot was 5,033 bytes and 87 canonical nodes. Both owners retired normally, integrity passed, and temporary data was removed.

Scope is local macOS arm64, Rust 1.93.1 and Python 3.12.12, using owned temporary data and loopback TCP. This increment changes the Python probe and runbook only. It adds no Rust/API/schema/dependency change, cancellation/deletion allowance, cross-platform qualification, maximum-composition guarantee or new full-workspace runtime credit. Existing child cleanup and stopped query-only inspection behavior are retained. Broad release scenarios remain open.
