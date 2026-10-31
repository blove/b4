# CP-001 current-HEAD create compatibility design

## Decision and evidence

The whole-runtime parity target is committed `b4run` HEAD
`f7d89dbc6588a12f7cb41f13d43cc8defa98b244`. The Rust runtime owns the
server behavior; existing TypeScript SDK, UI and build packages remain clients.
This design covers only `POST /threads` creation and the immediate durable and
public thread projection needed to preserve its metadata. It does not qualify
CP-001 as a whole, M0, `COMPAT`, the full core profile or whole-product parity.

The independently reviewed private attempt-06 TCP sequence used the same 19
requests against the committed TypeScript build and Rust. Its frozen manifest is
SHA-256 `8f20eb5e628ffcc8c35afdf637297359ccbf74679a0878bd4bebea1764b56207`;
its results review is SHA-256
`818609357c70cbd2a52d10f0829a94b72db7d4ccc6bcab8dd97f618e63d4daad`.
All 19 cases were comparable and failed. Every case differed in significant
headers because Rust adds `cache-control: no-store` and TypeScript omits it on
create. Other observed differences are request-error JSON, unknown and
duplicate-key acceptance, `metadata` array persistence, and Rust's 65,536-byte
and depth-64 limits. The attempt-06 receipts and collector report remain
immutable historical evidence; no result is retroactively upgraded.

The user selected exact TypeScript behavior for the observed metadata-array
case and approved this create-path design, including finite enlarged create
limits and a fresh 19-case comparison. Exactness here means the named executed
cases and fields, not unbounded inputs or untested downstream operations.

## Approaches considered

1. **Chosen: preserve create behavior through typed durable metadata.** Use an
   object-or-array metadata type, route-specific parsing and response handling,
   and a new bounded comparison. This can match the observed request, response
   and stored row without silently changing other routes.
2. **Gateway-only array conversion.** Accept arrays at HTTP but convert them to
   an object internally. This loses the TypeScript array in SQLite and thread
   reads, so it cannot meet the chosen behavior.
3. **Keep strict native parsing and record `CHANGE`.** This preserves existing
   native assumptions but leaves the observed create cases incompatible with
   the selected current-HEAD target.

## Create request and response

After the existing host readiness, authentication and operation dispatch
checks, a create-specific parser reads at most **131,072 body bytes** and
accepts JSON through **depth 96**, using the existing parser's container-depth
count with the root at zero. It accepts an empty or ASCII-whitespace-only
body as `{}` and ignores content type, as the recorded cases require. It parses
duplicate object keys with last-key-wins semantics at every nesting level. It
accepts a top-level JSON object or array; a top-level array has no `metadata`
field. Other top-level JSON values and malformed JSON return HTTP 400 with
`{"error":{"kind":"request_error","message":"Malformed request body"}}`.
Unknown top-level properties are ignored. A present `metadata` must be a JSON
object or array; null and scalar metadata return HTTP 400 with
`{"error":{"kind":"request_error","message":"metadata must be an object"}}`.
An absent `metadata` becomes `{}` in the created row. Preserve every accepted
metadata value, including an array, without object coercion. Keep the existing
server-owned origin separate from untrusted client metadata.

Successful create responses and create request-validation refusals use JSON
content type and omit `cache-control`, matching the recorded significant
TypeScript headers. A
create-only response helper owns this behavior; the shared JSON/error helper
and all other routes retain their existing cache policy. The existing request
ID and ordinary transport-generated headers are outside the significant-header
comparison and remain unchanged. Any host infrastructure, authentication,
authorization or store error not characterized by these create cases retains
its current response path and is not asserted to match TypeScript.

The create limit is finite. Bodies above 131,072 bytes return the existing
HTTP 413 `body_too_large` request-error JSON shape; nesting beyond 96 returns
the existing HTTP 400 `invalid_json` request-error JSON shape before storage.
Both create-limit refusals omit `cache-control` under the create request-error
header policy. Metadata accounting refusal returns HTTP 413 with
`{"error":{"kind":"request_error","message":"metadata_limit","details":{"code":"metadata_limit"}}}`,
omits `cache-control`, and creates no row. These remain
explicit `CHANGE` boundaries until separately characterized and adjudicated.
The higher create limits admit the observed 65,537-byte and depth-65 cases.
Run, stream, resume and other JSON routes retain 65,536 bytes and depth 64.
The parser must enforce the limits while reading/decoding, including duplicate
keys and malformed input; it must not first allocate an unbounded body or an
unbounded recursive value. Bound the resulting stored metadata using the
existing thread/storage resource accounting, and reject before any row write
if that accounting cannot admit it. Such resource refusals are separate from
the 19-case parity claim.

## Durable metadata and dependent operations

Introduce an explicit `ThreadMetadata` object-or-array representation in the
runtime thread seed and record, the in-memory store, SQLite serialization,
and the public `Thread` wire type. Existing object metadata round-trips with
the same JSON and semantics; legacy SQLite rows remain valid without a schema
migration because the metadata column already stores JSON. New arrays
round-trip through create, SQLite reopen, `GET /threads/{id}`, and the Rust
wire DTO. Update the authored create/thread JSON Schemas to describe the
object-or-array metadata extension, top-level create arrays, and ignored
unknown create-root properties; retain their evidence labels;
schema authorship alone grants no `COMPAT` credit.

Native permission and recovery code currently assumes a map in several
budget, canonicalization and profile checks. Every such consumer must either
charge and handle arrays explicitly or reject the array-bearing thread with
an explicit unsupported-profile result **before** effects, disclosures or
mutations. It must never reinterpret an array as `{}`, lose array elements,
or read identity/authority from metadata. This increment does not claim that
TypeScript run, resume, permission or deletion behavior on array-bearing
threads has been characterized. Those operations remain on the CP-002–009
queue and cannot receive parity credit from create success.

## Capability description

Capability descriptor versions 1–8 are already assigned. Current hosts assemble
a validated version-2 base and may extend it through version 8 as permission
features are installed. Their wire schemas, validators and feature meanings
remain unchanged. The new host publishes **version 9** as an envelope with
three fields: `descriptor_version: 9`, `base_descriptor` containing the exact
validated version-2–8 descriptor the host would otherwise publish, and
`create_limits: {"body_bytes":131072,"json_depth":96}`. The nested base retains
`limits.body_bytes: 65536` and `limits.json_depth: 64` for non-create routes,
along with every installed feature. Version-9 validation must validate the
nested descriptor with its existing version-specific rules, require the exact
create limits, and reject any unassigned base version or extra envelope field.
Author `contracts/agent-protocol/schemas/capabilities-v9.json` with a closed
envelope, a `oneOf` reference to the existing version-2–8 base schemas, and
the exact create-limit values. Validate actual published version-9 output
against this schema for every supported active base version; the old schema
files remain byte-identical. Version-1–8 decoding and validation remain
available unchanged. A client that
does not understand version 9 must reject that top-level version explicitly;
it must not silently treat the nested base's common limits as create limits.
New-host capability output and validation must agree with actual parser
behavior. No capability response is itself evidence of TypeScript parity.

## Qualification and acceptance

Author acceptance expectations independently of Rust production code. First
show meaningful failures against the unchanged Rust baseline. Exercise all
19 recorded create requests over real TCP with a durable store, including
empty/whitespace, content type variants, malformed and non-object roots,
array metadata, unknown and duplicate keys, 65,537 bytes and depth 65.
Compare status, significant headers, semantic JSON and durable row count and
metadata, normalizing only permitted generated IDs and timestamps. Separately
test SQLite reopen and public read of array metadata, legacy object rows,
reserved/server-owned origin separation, over-limit refusal without row
creation, other routes' unchanged limits, capability version/limit accuracy,
and fail-closed array behavior in map-dependent permission paths. Run the
relevant native regression and schema suites.

The attempt-06 comparator incorrectly treats every stored metadata value as
an object, even though the captured TypeScript create response and SQLite row
contain `[1,2]`. Correct this invariant in a **new** comparison source map:
allow an array only as the exact expected JSON metadata value, verify its
response/live/SQLite linkage, and continue rejecting malformed or unlinked
rows. Freeze and independently review that source and the new acceptance
before execution. Use fresh stores, case receipts, process ownership proofs,
manifest and result review. The old comparator, receipts and failure tally
remain unchanged. Only a newly reviewed, complete comparison can grant
`COMPAT` for its passing cases; failed or incomparable cases stay explicit.

Update CP-001 and the wire-decision register with the resulting evidence and
the finite-limit `CHANGE` boundary. Preserve T-001–T-018 receipts as claims
about their exact historical source; regression passes on changed source do
not retroactively requalify them. CP-001 and M0 remain open until their broader
required corpora and decisions are completed.
