export class RunnerError extends Error {
  constructor(code) { super(code); this.name = 'RunnerError'; this.code = code; }
}

export class CorpusContext {
  constructor({ ready, control, fetchImpl = globalThis.fetch }) {
    this.ready = ready; this.control = control; this.fetchImpl = fetchImpl;
    this.groups = []; this.transports = []; this.ordinal = 0;
    this.local = new AsyncLocalStorage(); this.pending = new Set();
    this.gates = new Map(); this.controllers = [];
  }
  beginGroup(name, functionName) {
    this.group = { name, function: functionName, observations: [], witnesses: [] };
    this.groups.push(this.group);
  }
  async call(method, caseName, args, operation) {
    if (!SELECTED_METHODS.includes(method) || !CASES[this.group?.name]?.includes(caseName)
      || this.ordinal >= 128 || !Array.isArray(args)) throw new RunnerError('invalid_invocation');
    const record = { ordinal: ++this.ordinal, case: caseName, method,
      arguments: args.map((arg) => arg === undefined ? { kind: 'undefined' }
        : arg instanceof AbortSignal ? { kind: 'AbortSignal', aborted_at_invocation: arg.aborted } : arg),
      outcome: { kind: 'pending' } };
    this.group.observations.push(record);
    const promise = this.local.run(record, () => Promise.resolve().then(operation));
    this.pending.add(promise);
    try {
      const value = await promise;
      record.outcome = value?.[Symbol.asyncIterator] ? { kind: 'iterable' } : { kind: 'returned', value };
      return { value, error: null, record };
    } catch (error) {
      if (error instanceof RunnerError) throw error;
      record.outcome = { kind: 'rejected', error: errorInfo(error) };
      return { value: undefined, error, record };
    } finally { this.pending.delete(promise); }
  }
  fetch(role) {
    if (!['controller', 'worker'].includes(role)) throw new RunnerError('invalid_transport');
    return async (url, init) => {
      const invocation = this.local.getStore();
      let target;
      try { target = new URL(url); } catch { throw new RunnerError('invalid_origin'); }
      if (!invocation || target.origin !== new URL(this.ready.base_url).origin || target.username || target.password) throw new RunnerError('invalid_origin');
      const original = new Headers(init.headers);
      let forwarded = init;
      if (role === 'controller') {
        const headers = new Headers(original); headers.set('authorization', `Bearer ${this.ready.token}`);
        forwarded = { ...init, headers, redirect: 'error' };
      }
      const row = { ordinal: invocation.ordinal, request: { url: String(url), method: init.method ?? 'GET',
        original_headers: [...original.entries()], forwarded_headers: [...new Headers(forwarded.headers).entries()],
        body: init.body ?? null, has_signal: init.signal !== undefined, redirect: forwarded.redirect ?? null } };
      this.transports.push(row);
      try {
        const response = await this.fetchImpl(url, forwarded);
        row.response = { status: response.status, headers: [...response.headers.entries()] };
        return response;
      } catch (error) { row.error = errorInfo(error); throw error; }
    };
  }
  returned(result) {
    if (result.error) throw new RunnerError('unexpected_client_refusal');
    return result.value;
  }
  httpError(result, name, status, code) {
    if (result.error?.name !== name || result.error.status !== status || result.error.code !== code
      || (this.errorClasses?.[name] && !(result.error instanceof this.errorClasses[name]))) throw new RunnerError('unexpected_client_error');
  }
  witness(name, value, controlOrdinal = null) {
    this.group.witnesses.push({ name, control_ordinal: controlOrdinal, value }); return value;
  }
  async inspect(threadId, pending = false) {
    const operation = pending ? 'inspect_pending' : 'inspect_thread';
    return this.witness(operation, await this.control.request(this.group.name, { [operation]: { thread_id: threadId } }), this.control.ordinal ?? null);
  }
  async arm(gate) {
    const result = this.witness(`arm-${gate}`, await this.control.request(this.group.name, { arm: { gate } }), this.control.ordinal ?? null);
    this.gates.set(gate, result.generation); return result.generation;
  }
  async entered(gate, generation) {
    const result = this.witness(`entered-${gate}`, await this.control.request(this.group.name, { await_entered: { gate, generation } }), this.control.ordinal ?? null);
    if (!result.entered || result.returned || result.expired || result.cleanup_release) throw new RunnerError('invalid_gate_witness');
    return result;
  }
  async held(gate, generation) {
    const result = this.witness(`held-${gate}`, await this.control.request(this.group.name, { gate_status: { gate, generation } }), this.control.ordinal ?? null);
    if (!result.entered || result.returned || result.expired || result.cleanup_release) throw new RunnerError('invalid_gate_witness');
    return result;
  }
  async release(gate, generation) {
    const result = this.witness(`release-${gate}`, await this.control.request(this.group.name, { release: { gate, generation } }), this.control.ordinal ?? null);
    if (!result.entered || !result.returned || !result.explicit_release || result.cleanup_release || result.expired) throw new RunnerError('invalid_gate_witness');
    this.gates.delete(gate); return result;
  }
  signal() { const controller = new AbortController(); this.controllers.push(controller); return controller; }
  async consume(result) {
    const value = this.returned(result);
    result.record.outcome = { kind: 'stream', ...await consumeToEof(value) };
    return result.record.outcome;
  }
  async cutFirst(result) {
    const iterator = this.returned(result)[Symbol.asyncIterator]();
    const first = await iterator.next();
    // The actual next has settled before return is queued, including a bad first frame.
    try {
      if (first.done || first.value.event !== 'state') throw new RunnerError('missing_state_frame');
    } finally { await iterator.return(); }
    result.record.outcome = { kind: 'stream', frames: [first.value], eof: false, cut: 'return',
      reader_polled: true, pending_settled: true, return_awaited: true };
  }
  async abortStream(result, iterator, pending, controller) {
    const cut = await abortPolledIterator(iterator, pending, controller);
    if (cut.error?.name !== 'AbortError') throw new RunnerError('unexpected_stream_abort');
    result.record.outcome = { kind: 'stream', frames: [], eof: false, cut: 'signal', reader_polled: true, ...cut };
  }
  inFlight(state) {
    if (!state.found || state.execution_status !== 'running' || !state.task_statuses.includes('in_flight')) throw new RunnerError('missing_inflight');
  }
  observations() {
    if (this.pending.size || this.gates.size || this.groups.length !== 8
      || this.groups.some((group) => group.observations.some((record) => ['pending', 'iterable'].includes(record.outcome.kind)))) throw new RunnerError('unfinished_work');
    const methods = new Set(this.groups.flatMap((group) => group.observations.map((record) => record.method)));
    if (methods.size !== SELECTED_METHODS.length || SELECTED_METHODS.some((method) => !methods.has(method))) throw new RunnerError('missing_method');
    return { version: 1, kind: 'original-ap-client-observations', groups: this.groups, transports: this.transports };
  }
  cleanup() { for (const controller of this.controllers) controller.abort(); this.local.disable(); }
}

function errorInfo(error) {
  return { name: error?.name ?? 'Error', status: error?.status ?? null, code: error?.code ?? null,
    message: error?.message ?? String(error), stack: error?.stack ?? null };
}
import { AsyncLocalStorage } from 'node:async_hooks';
import { CASES, SELECTED_METHODS } from './runner-cases.mjs';
import { consumeToEof, abortPolledIterator } from './runner-support.mjs';
