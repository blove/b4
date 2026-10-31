# ADR-0002: Use explicit strict native DTOs

Status: accepted native draft; compatibility characterization unresolved.

Run requests require a literal registered route shape and reject unknown root
keys. Omitted input defaults to an object, while present null, scalar and array
values are preserved for future route-specific validation. Resume requires route
and a nonempty decision list; resolved/cancelled entries are strict and distinct.

Strict run roots are a deliberate `CHANGE`: pinned source ignores extra run
keys. The native `done` event also requires explicit `output`; pinned POST stream
source can omit it when JavaScript output is undefined, whereas wait and idle
attach have their own null behavior. Explicit native JSON null does not represent
every undefined case. These restrictions require later qualification and remain
visible in the compatibility register.

Preserve direct chunk payloads, named tool object payloads and the AP error
envelope/code placement. Do not derive HTTP behavior from Serde rejection defaults
or claim whole endpoint compatibility from serialization tests. CP-001–010 remain
unresolved. See `../compatibility.md`.
