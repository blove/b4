# Cancel an entered permission callback over HTTP

Construct the host with `HttpHost::new_with_permission_cancellation(store, auth, policy, PermissionHostConfig)`. This explicit profile supports the original human Always-approved callback and subsequent saved-permission uses. It requires durable approval, original execution, reuse, and both entered-cancellation Store capabilities. Existing constructors retain their existing behavior.

Use the same catalog and approval configuration as the permission host: one native route, one batch, one required-approval Pure task, and one active run/task. The host maps callback Arcs while preserving application identity, current and historical definitions, input validators, output kinds, tool metadata, and shared constraints. A private registered wrapper records actual controlled callback entry. Dispatch and admission alone do not establish entry.

Send the existing authenticated `POST /threads/{thread_id}/cancel` request. Current CancelRun policy, origin, recorded route, captured run identity, source records and fences must authorize the operation. An effect constraint revoked after callback entry does not veto an authorized stop.

A successful request returns HTTP 200 with `{"status":"interrupted","thread_id":"..."}` only after the typed permission cancellation request commits. Original execution advances from revision 4 to CancelRequested5; NewUse advances from revision 3 to CancelRequested4. The host then signals the callback and publishes the existing interrupted/cancelled terminal response. It suppresses later transport output and publishes one cancelled Done for an attached stream.

Publication does not release the callback. The worker retains its claim, task capacity and Store ownership until actual quiescence, including when a client abandons the request or disconnects. Implement `NativeEffect::invoke_controlled` to cooperate with its observer. The default delegates the existing callback unchanged; its eventual Success or Failure remains the real result. A real acknowledgement may settle cancellation. No acknowledgement is synthesized. A late successful result retains its output and proposed writes; the cancelled execution keeps its prior values.

Ready and dispatched-but-unentered callbacks return HTTP 409 `run_not_cancellable` without a cancellation write or signal. This increment does not retire those HTTP timing states. Older permission host constructors continue returning HTTP 501 `permission_operation_unsupported`; permission deletion remains unsupported. Current authorization refusal preserves resource privacy.

A Store failure leaves cancellation unconfirmed and faults the owned host/control. A request, error or uncertain callback receipt does not prove settlement. There is no automatic callback replay, receipt-storage retry, recovery or deletion path in this profile. Retain ownership through the existing shutdown/drain procedure.

The V5 descriptor declares `durable_permission_entered_cancellation=true`, while generic `fenced_cancellation`, broad Always, recovery and deletion promises remain false. Its unchanged bounded input, output, readiness and control declarations are inherited from V4. Typed entered-management attempts use a fresh cumulative 24 MiB evidence allowance with the existing 65,536-node ceiling and read ledger; the existing conflict loop is capped at eight attempts. Old account defaults and input, record, history and physical reader limits are unchanged. Individual maxima are not a guarantee that every combined input fits an evidence allowance.

See the [local qualification](../../conformance/permissions/http-entered-cancellation/README.md) for actual execution evidence and its limits.
