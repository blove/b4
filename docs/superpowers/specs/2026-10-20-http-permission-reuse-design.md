# Opt-in HTTP permission reuse

## Goal

Run a real authenticated AP request through the existing durable permission-use authorization, Driver entry gate, receipt-backed callback completion and SQLite persistence. First support reuse of already-provisioned genuine permissions. Keep HTTP Always creation/original-human callback unsupported; unmatched permissions follow existing Once/Deny parking/resume. Existing native/scripted hosts retain current behavior.

## Opt-in API and profile

Introduce a separate configuration/constructor requiring a durable PermissionStore and one immutable PermissionCatalog. Derive the NativeApplication from that catalog via a readonly accessor, rather than accepting an unrelated callback registry. Reuse HostLimits and existing ApprovalHostConfig, including policy revision and missing-permission prompts. Restrict registration to one route with one batch and one Pure required-approval task. The new configuration defaults to and requires one active run, one concurrent task and one task per execution; use the existing bounded input/output envelope, with a modest default input limit. Validate actual request bytes/nodes/depth before admission, and preserve later record/account refusals. These are finite subset bounds, not a promise that all individual maxima fit together. Reject unsupported configuration before opening or mutating a run.

The first host profile should explicitly refuse cancellation, recovery and deletion before mutations, including operator confinement, until those paths have permission-specific evidence. Publish accurate capabilities instead of inheriting generic flags. Current V2 assembly validation equates recovery support with an enabled approval bridge, so use an explicit V3 descriptor/profile with the limited recovery/cancel/delete flags and narrow permission-use-reuse feature; preserve V1/V2 formats and validation behavior for existing hosts. Broad always_permissions remains false. Do not imply support merely by having an endpoint registered.

## Execution

Authenticate and authorize using existing host gates and current ResourcePolicy. Share the existing native approval Authority implementation/factory so actor, policy revision, origin and route/Definition checks do not diverge. For a new accepted run, persist normal Start1, capture a genuine kernel PermissionAuthorizationCommand with a fresh Use identity, and call try_authorize_permission_ready against actual current fences. Some produces real Ready2/Use and invokes drive_permission_use_execution through an owned bridge; Only Ok(None) passes actual Start1 to the existing Driver, which parks or executes ordinary Once/Deny continuation. Each run starts once. Errors, unavailable constraints, malformed source evidence and quota refusals do not fall back. Already-approved legacy resumes bypass auto-reuse; their existing Once/Deny authorization semantics remain unchanged. Catalog constraints govern the reusable-permission lane. Never infer the branch from a marker string.

Use monotonic host facts and fresh attempt identity. The Driver hook invokes RunControl local admission. Keep host/store/capacity/worker owners through callback quiescence and viewer disconnect. Reuse existing wait/SSE terminal projection from the actual committed snapshot. Driver failures retain their actual Work through inspection and any bounded storage-only retry of the same failure/account, never callback replay. Failed settlement cannot become a fabricated completion; infrastructure faults preserve actual state and latch existing host fault behavior.

## Tests and delivery

Use genuine embedded Always approval/cancel/reopen provisioning, then actual authenticated HTTP request on the reopened owner. Check exact Complete4/Use/journal and immutable original permission records, callback one, full typed/raw persistence, listener/worker retirement and owner reopening. Add wrong-origin/policy denial, verified permission miss -> real Once/Deny park/resume, unsupported operations with unchanged state, accurate capabilities and viewer detach/owned drain. Add focused storage refusal if bridge retries or maps faults. All relevant existing host tests stay literal and pass. No broad core, Linux or HTTP Always qualification is implied.

Use a dedicated ordinary source copy and locked/offline Rust 1.93.1. Review source before native runs, test first, then quality/results review and selective exact adoption after full checks. No Git writes or routine user approval loop; standing user authorization is continuous. Prefer this bounded opt-in reuse host over silently changing legacy Always semantics or implementing all original-human callback/recovery paths in one increment. The new capability descriptor must not change legacy V1/V2 formats or assembly rules.

## Qualified admission bounds

The implemented opt-in profile defaults to 4,096 serialized input bytes and checks actual input value depth 16 and at most 256 values before admission. The ordinary body parser retains 65,536 bytes and depth 64. Output must use the receipt Driver's fixed 65,536-byte cap; smaller configured limits are rejected during construction. These bounds remain separate from later source-record and cumulative-account refusals. See the [qualification record](../../../conformance/permissions/http-reuse/qualification.json) for executed checks and platform scope.
