function pendingProjection(entry) {
  const value = entry.value
  const grant = entry.grant
  return {
    has_interrupt_id: typeof entry.interruptId === "string" && entry.interruptId.length > 0,
    has_resume_key: typeof entry.resumeKey === "string" && entry.resumeKey.length > 0,
    grant_prefix: typeof grant === "string" ? grant.slice(0, 5) : null,
    grant_bytes: typeof grant === "string" ? Buffer.byteLength(grant) : null,
    envelope_matches: value?.interruptId === entry.interruptId && value?.grant === grant,
    prompt: { type: value?.type, kind: value?.kind, detail: value?.detail },
  }
}

// This is a deliberately limited comparison projection. Every observation
// retains raw bytes; no string substitution is applied to application values.
export function summarizeGrantObservation({ id, response, ledger }) {
  const summary = { id, status: response.status }
  let json
  try { json = JSON.parse(response.body_utf8) } catch { /* SSE is recorded separately. */ }
  if (json?.error) summary.error = { message: json.error.message, code: json.error.details?.code }
  if (Array.isArray(json?.interrupts)) summary.pending = json.interrupts.map(pendingProjection)
  if (typeof json?.status === "string") summary.thread_status = json.status
  if (response.sse_frames) {
    summary.events = response.sse_frames.map(frame => frame.event)
    summary.interrupts = response.sse_frames.filter(frame => frame.event === "interrupt").map(frame => ({
      type: frame.data?.type,
      kind: frame.data?.kind,
      detail: frame.data?.detail,
      has_grant: typeof frame.data?.grant === "string",
    }))
  }
  summary.ledger = ledger
  return summary
}
