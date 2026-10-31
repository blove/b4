# Bounded text inputs through saved permissions

Representative 2 KiB ASCII note fields pass through original Always approval, one original callback, normal SQLite owner retirement/reopen, and one saved-permission callback. Original and reused inputs differ and are checked independently. The real authenticated HTTP test exercises the same complete flow.

The positive input is 2,060 JSON bytes, two values, depth one. Its embedded parked snapshot is 5,036 bytes. A separate 3,750-byte note produces an 8,440-byte parked snapshot and refuses with `UnsupportedProfile`: all nine tables and the unconsumed grant remain unchanged, with zero callbacks and normal cleanup.

Normal original before/resume snapshots permit 8,192 bytes, acceptance records 16,384 bytes, and the prepared encoded bundle 16,384 bytes. Node, depth, task and row bounds remain. The typed ReadyAuthorization writer alone receives a measured 16 MiB effective work allowance; stored 8 MiB identity, 65,536 nodes, sticky accounting and the separate future-read certificate remain. Saved-use raw/semantic history accepts larger original records only under exact normal Dispatch or trusted production renewal with retained Dispatch provenance. Cancellation keeps its prior bounds. Individual maxima need not compose; this does not qualify long-text cancellation or deletion.

Actual final checks are bound to the same 1,099-file isolated source in [qualification.json](qualification.json). Focused results: two embedded text cases, one HTTP text case, all 77 persistence cases, and seven HTTP approval cases plus one reuse case. Formatting, strict workspace lint and separate metadata are included. Full-workspace result: 1,668 reported passes, zero failures and 11 ignored entries across 153 summaries; process exit 0 after 972.777 seconds.

Scope: local macOS arm64, pinned Rust 1.93.1, locked offline dependencies, owned temporary SQLite stores and loopback TCP. Summary execution counts include child/helper runs and are not unique test inventory. Metadata, formatting and lint carry no runtime-test credit. Historical failures and disposable diagnostics remain separate from final passes. Broad release scenarios remain open.
