// Offline only. Private captured bytes never enter an original caller.
import { isDeepStrictEqual } from 'node:util';
import { CASES, SELECTED_METHODS } from './runner-cases.mjs';
import { readPrivateJson } from './artifacts.mjs';
import { decodeHttpRequests, decodeHttpConnection, decodeCapturedSse } from './wire.mjs';

const fail = (code) => { throw new Error(`Client traffic: ${code}`); };
const METHODS = Object.freeze({
  C1: SELECTED_METHODS.slice(0, 10),
  C2: ['controller.dispatch', 'controller.dispatch'],
  C3: ['controller.dispatch', 'controller.dispatch', 'controller.cancel'],
  C4: ['controller.dispatch', 'controller.dispatch', 'controller.dispatch', 'controller.interrupt',
    'controller.dispatch', 'controller.interrupt', 'controller.dispatch', 'controller.interrupt',
    'controller.interrupt', 'controller.dispatch', 'controller.interrupt'],
  W1: ['worker.createThread', 'worker.getThread', 'worker.getThread', 'worker.getThread', 'worker.getThread',
    'worker.deleteThread', 'worker.deleteThread', 'worker.createThread', 'worker.startRun', 'worker.deleteThread', 'worker.deleteThread'],
  W2: ['worker.createThread', 'worker.startRun', 'worker.createThread', 'worker.startRun', 'worker.createThread', 'worker.startRun'],
  W3: ['worker.createThread', 'worker.startRun', 'worker.reattach', 'worker.startRun', 'worker.startRun',
    'worker.createThread', 'worker.startRun', 'worker.cancel', 'worker.startRun', 'worker.reattach', 'worker.cancel', 'worker.cancel', 'worker.cancel'],
  W4: ['worker.createThread', 'worker.pendingInterrupts', 'worker.startRun', 'worker.pendingInterrupts', 'worker.resume', 'worker.resume', 'worker.resume'],
});
const ROUTES = Object.freeze({ create: '/work-orders/create#workflow', dispatch: '/work-orders/dispatch#workflow',
  approve: '/work-orders/approve#workflow', retry: '/work-orders/retry#workflow', deny: '/work-orders/deny#workflow',
  intake: '/work-orders/intake#workflow', approveIntake: '/work-orders/approve-intake#workflow',
  rejectIntake: '/work-orders/reject-intake#workflow', cancel: '/work-orders/cancel#workflow', reconcile: '/reconcile#workflow' });
const CUTS = new Set(['held-dispatch-abandon', 'attach-held-start', 'attach-live']);
const ERRORS = Object.freeze({
  'default-auth-refusal': [401, 'authentication_required'], 'wait-failure': [500, null],
  'held-busy-before-abort': [409, 'thread_busy'], 'held-busy-after-abort': [409, 'thread_busy'],
  'held-dispatch-cancel': [409, 'run_cancelled'], 'held-busy-after-cancel': [409, 'thread_busy'],
  'worker-get-wrong-token': [401, 'authentication_required'], 'worker-delete-held': [409, 'run_in_flight'],
  'attach-busy-after-viewer-cut': [409, 'thread_busy'], 'attach-busy-after-post-abort': [409, 'thread_busy'],
  'attach-busy-after-cancel': [409, 'thread_busy'], 'resume-forced-once': [400, 'grant_required'],
  'resume-forced-deny': [400, 'grant_required'], 'resume-empty': [400, 'invalid_request'],
});
function keys(v, required, optional = []) {
  if (!v || typeof v !== 'object' || Array.isArray(v) || ![Object.prototype, null].includes(Object.getPrototypeOf(v))) fail('object');
  const d = Object.getOwnPropertyDescriptors(v);
  if (required.some((k) => !Object.hasOwn(d, k)) || Reflect.ownKeys(d).some((k) => ![...required, ...optional].includes(k)) ||
      Object.values(d).some((x) => !Object.hasOwn(x, 'value') || !x.enumerable)) fail('fields');
}
function pure(value, depth = 0, counter = { n: 0 }) {
  if (++counter.n > 65536 || depth > 64) fail('JSON limits');
  if (value === null || typeof value === 'boolean' || (typeof value === 'number' && Number.isFinite(value))) return;
  if (typeof value === 'string') { if (Buffer.byteLength(value) > 1048576) fail('string limit'); return; }
  if (!value || typeof value !== 'object') fail('non-JSON value');
  const d = Object.getOwnPropertyDescriptors(value);
  if (Array.isArray(value)) {
    if (Object.getPrototypeOf(value) !== Array.prototype || value.length > 256 || Reflect.ownKeys(d).length !== value.length + 1) fail('array');
    for (let i = 0; i < value.length; i++) {
      if (!d[i] || !Object.hasOwn(d[i], 'value') || !d[i].enumerable) fail('array element');
      pure(d[i].value, depth + 1, counter);
    }
  } else {
    if (![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail('prototype');
    for (const k of Reflect.ownKeys(d)) {
      if (typeof k !== 'string' || !Object.hasOwn(d[k], 'value') || !d[k].enumerable) fail('JSON field');
      pure(d[k].value, depth + 1, counter);
    }
  }
}
const equal = (a, b, code) => { if (!isDeepStrictEqual(a, b)) fail(code); };
function string(v) { if (typeof v !== 'string' || !v.length || v.includes('\0') || Buffer.byteLength(v) > 65536) fail('string'); return v; }
function json(body) { return readPrivateJson(new Map([['body.json', body]]), 'body.json'); }
function headers(rows) {
  if (!Array.isArray(rows) || rows.length > 64) fail('headers');
  const result = Object.create(null);
  for (const row of rows) {
    if (!Array.isArray(row) || row.length !== 2 || typeof row[0] !== 'string' || typeof row[1] !== 'string' ||
        !/^[a-z0-9!#$%&'*+.^_`|~-]+$/.test(row[0]) || Object.hasOwn(result, row[0]) || /[\r\n\0]/.test(row[1])) fail('headers');
    result[row[0]] = row[1];
  }
  return result;
}
function expectedRequest(record, ready) {
  const a = record.arguments; const [family, method] = record.method.split('.');
  if (!SELECTED_METHODS.includes(record.method)) fail('method');
  let id = a[0]; let target; let body = null; let verb = 'POST'; let signal = false; let contentType = false;
  const signalAt = (index) => {
    if (a.length <= index) return false;
    keys(a[index], ['kind', 'aborted_at_invocation']);
    if (a[index].kind !== 'AbortSignal' || a[index].aborted_at_invocation !== false) fail('signal');
    return true;
  };
  const thread = (tail = '') => `/threads/${encodeURIComponent(string(id))}${tail}`;
  if (family === 'controller') {
    if (method === 'interrupt') { if (a.length !== 1) fail('arguments'); target = thread('/cancel'); }
    else {
      let input;
      if (method === 'create') {
        if (a.length !== 1) fail('arguments'); keys(a[0], ['taskId', 'operationKey']);
        string(a[0].taskId); id = `create:${string(a[0].operationKey)}`; input = a[0];
      } else if (method === 'reconcile') {
        if (a.length) fail('arguments'); id = 'controller'; input = {};
      } else if (['approve', 'approveIntake', 'rejectIntake'].includes(method)) {
        if (a.length !== 2) fail('arguments'); string(id); input = { id, ...a[1] };
      } else {
        if (a.length < 2 || a.length > 3) fail('arguments'); input = { id: string(id), operationKey: string(a[1]) }; signal = signalAt(2);
      }
      target = thread('/runs/wait'); body = JSON.stringify({ route: ROUTES[method], input }); contentType = true;
    }
  } else if (method === 'createThread') {
    if (a.length !== 1) fail('arguments'); keys(a[0], ['fixture_case']);
    if (a[0].fixture_case !== record.case) fail('create metadata');
    target = '/threads'; body = JSON.stringify({ metadata: a[0] }); contentType = true;
  } else if (method === 'startRun') {
    if (a.length < 3 || a.length > 4 || !Object.values(ready.routes).includes(a[1])) fail('arguments');
    target = thread('/runs/stream'); body = JSON.stringify({ route: a[1], input: { messages: [{ role: 'user', content: string(a[2]) }] } });
    signal = signalAt(3); contentType = true;
  } else if (method === 'resume') {
    if (a.length !== 3 || a[1] !== ready.routes.worker_park || !Array.isArray(a[2]) || a[2].length > 1) fail('resume');
    target = thread('/resume');
    const resume = a[2].map((r) => { keys(r, ['interruptId', 'payload']); string(r.interruptId); if (!['once', 'deny'].includes(r.payload)) fail('resume'); return { interruptId: r.interruptId, status: 'resolved', payload: r.payload }; });
    body = JSON.stringify({ resume, route: a[1] }); contentType = true;
  } else {
    const tails = { reattach: '/runs/stream', pendingInterrupts: '/pending_interrupts', cancel: '/cancel', getThread: '', deleteThread: '' };
    if (!Object.hasOwn(tails, method) || a.length < 1 || a.length > (method === 'reattach' ? 2 : 1)) fail('arguments');
    target = thread(tails[method]); verb = method === 'deleteThread' ? 'DELETE' : method === 'cancel' ? 'POST' : 'GET';
    signal = method === 'reattach' && signalAt(1); contentType = ['reattach', 'pendingInterrupts'].includes(method);
  }
  if (body !== null && Buffer.byteLength(body) > 65536) fail('request body limit');
  const original = Object.create(null);
  if (contentType) original['content-type'] = 'application/json';
  if (family === 'worker') original.authorization = `Bearer ${record.case === 'worker-get-wrong-token' ? 'fixture-invalid-token' : ready.token}`;
  const forwarded = Object.assign(Object.create(null), original);
  if (family === 'controller' && record.case !== 'default-auth-refusal') forwarded.authorization = `Bearer ${ready.token}`;
  return { url: ready.base_url + target, target, method: verb, body, has_signal: signal, original, forwarded };
}
function nativeInterrupt(v) {
  keys(v, ['interruptId', 'resumeKey', 'value', 'grant']); string(v.interruptId); string(v.resumeKey);
  if (!/^[a-f0-9]{64}$/.test(v.resumeKey) || !/^b4ag_[a-f0-9]{64}$/.test(v.grant)) fail('native interrupt');
  keys(v.value, ['question']); if (v.value.question !== 'fixture-approval') fail('native prompt');
}
function frameShape(frame) {
  keys(frame, ['event', 'data']);
  const d = frame.data;
  if (frame.event === 'done') {
    keys(d, [], ['output']);
    if (!Object.hasOwn(d, 'output')) return { event: 'done', shape: 'done-void' };
    if (d.output === null) return { event: 'done', shape: 'done-null' };
    if (isDeepStrictEqual(d.output, { fixture: 'value' })) return { event: 'done', shape: 'done-value' };
    if (isDeepStrictEqual(d.output, { cancelled: true })) return { event: 'done', shape: 'done-cancelled' };
    keys(d.output, ['error']); if (d.output.error !== 'fixture-known-failure') fail('failure terminal');
    return { event: 'done', shape: 'done-failure' };
  }
  if (frame.event === 'interrupt') { nativeInterrupt(d); return { event: 'interrupt', shape: 'native-permission-request' }; }
  if (frame.event === 'state') {
    keys(d, ['anchor', 'input', 'interrupts', 'live', 'resume', 'run_started_at', 'status', 'turn', 'values'], ['turn_truncated']);
    if (!['busy', 'idle', 'interrupted'].includes(d.status) || typeof d.live !== 'boolean' || typeof d.resume !== 'boolean' ||
        !Array.isArray(d.interrupts) || (Object.hasOwn(d, 'turn_truncated') && d.turn_truncated !== true)) fail('state frame');
    for (const interrupt of d.interrupts) nativeInterrupt(interrupt);
    return { event: 'state', shape: `state-${d.status}` };
  }
  fail('unexpected native frame');
}
function refusal(body) {
  keys(body, ['error']); keys(body.error, ['kind', 'message'], ['details']);
  if (!['request_error', 'execution_error'].includes(body.error.kind) || typeof body.error.message !== 'string') fail('native error');
  if (body.error.kind === 'execution_error') {
    if (Object.hasOwn(body.error, 'details') || body.error.message !== 'fixture-known-failure') fail('native execution failure');
    return null;
  }
  keys(body.error.details, ['code']);
  const code = string(body.error.details.code);
  if (body.error.message !== code) fail('native request error message');
  return code;
}
function errorMetadata(error) {
  keys(error, ['name', 'status', 'code', 'message', 'stack']);
  if (typeof error.name !== 'string' || typeof error.message !== 'string' ||
      (error.stack !== null && typeof error.stack !== 'string') ||
      (error.status !== null && (!Number.isSafeInteger(error.status) || error.status < 0)) ||
      (error.code !== null && typeof error.code !== 'string' && error.code !== 20)) fail('recorded error metadata');
}
function validateTransport(record, transport, request, expected, response) {
  if (record.case === 'default-auth-refusal') { if (transport !== null) fail('default fetch seam'); return; }
  if (!transport) fail('missing transport');
  keys(transport, ['ordinal', 'request'], ['response', 'error']);
  if (transport.ordinal !== record.ordinal || (Object.hasOwn(transport, 'response') === Object.hasOwn(transport, 'error'))) fail('transport result');
  keys(transport.request, ['url', 'method', 'original_headers', 'forwarded_headers', 'body', 'has_signal', 'redirect']);
  equal({ url: transport.request.url, method: transport.request.method, body: transport.request.body, has_signal: transport.request.has_signal },
    { url: expected.url, method: expected.method, body: expected.body, has_signal: expected.has_signal }, 'original request');
  equal(headers(transport.request.original_headers), expected.original, 'original headers');
  equal(headers(transport.request.forwarded_headers), expected.forwarded, 'forwarded headers');
  if (transport.request.redirect !== 'error') fail('redirect guard');
  if (Object.hasOwn(transport, 'response')) {
    keys(transport.response, ['status', 'headers']);
    if (transport.response.status !== response.status) fail('response status');
    const seen = headers(transport.response.headers);
    equal(seen, response.headers, 'original response headers');
  } else {
    errorMetadata(transport.error);
    if (record.case !== 'held-dispatch-abandon' || transport.error.name !== 'AbortError' ||
        transport.error.status !== null || ![null, 20].includes(transport.error.code)) fail('transport rejection');
  }
}
export function auditInvocationTraffic(record, transport, pair, ready) {
  pure(record); if (transport !== null) pure(transport);
  keys(record, ['ordinal', 'method', 'case', 'arguments', 'outcome']);
  if (!Number.isSafeInteger(record.ordinal) || record.ordinal < 1 || record.ordinal > 128 || !Array.isArray(record.arguments)) fail('invocation');
  const expected = expectedRequest(record, ready); const { request, response } = pair;
  if (request.method !== expected.method || request.target !== expected.target || !request.complete ||
      request.body.toString('utf8') !== (expected.body ?? '') || request.headers.host !== new URL(ready.base_url).host) fail('captured request');
  for (const [k, v] of Object.entries(expected.forwarded)) if (request.headers[k] !== v) fail('captured request headers');
  if (record.case === 'default-auth-refusal' && Object.hasOwn(request.headers, 'authorization')) fail('default auth');
  if (!response.complete && !CUTS.has(record.case)) fail('unplanned incomplete response');
  validateTransport(record, transport, request, expected, response);
  const out = { method: record.method, outcome: record.outcome.kind, http_status: response.status ?? 0, client_status: 0,
    error_class: 'none', error_code: 'none', result_shape: 'none', frames: [], eof: false, cut: 'none' };
  const o = record.outcome;
  if (o.kind === 'rejected') {
    keys(o, ['kind', 'error']); errorMetadata(o.error);
    out.error_class = o.error.name; out.client_status = o.error.status ?? 0;
    if (record.case === 'held-dispatch-abandon') {
      if (o.error.name !== 'AbortError' || o.error.status !== null || ![null, 20].includes(o.error.code)) fail('original abort');
      out.cut = 'signal-abort'; return out;
    }
    if (!response.complete || response.headers['content-type']?.split(';')[0] !== 'application/json') fail('error response');
    const body = json(response.body);
    if (record.case === 'pending-original-schema-refusal') {
      if (response.status !== 200 || o.error.name !== 'ZodError' || o.error.status !== null || o.error.code !== null) fail('original schema refusal');
      keys(body, ['interrupts']); if (!Array.isArray(body.interrupts) || body.interrupts.length !== 1) fail('pending native grant');
      nativeInterrupt(body.interrupts[0]); return out;
    }
    const code = refusal(body); const expectedClass = record.method.startsWith('controller.') ? 'ControllerHttpError' : 'WorkerHttpError';
    if (o.error.name !== expectedClass || o.error.status !== response.status || o.error.code !== code) fail('original HTTP error');
    const message = expectedClass === 'ControllerHttpError' ? body.error.message
      : `Worker responded ${response.status}${code ? ` (${code})` : ''}: ${body.error.message}`;
    if (o.error.message !== message) fail('original HTTP error message');
    const policy = ERRORS[record.case];
    if (!policy || response.status !== policy[0] || code !== policy[1]) fail('unexpected refusal');
    out.error_code = code ?? 'none'; return out;
  }
  if (o.kind === 'stream') {
    keys(o, ['kind', 'frames', 'eof', 'cut', 'reader_polled', 'pending_settled', 'return_awaited'], ['error']);
    if (!Array.isArray(o.frames) || o.frames.length > 64 || o.reader_polled !== true || o.pending_settled !== true ||
        response.status !== 200 || response.headers['content-type']?.split(';')[0] !== 'text/event-stream') fail('original stream');
    if (o.cut === null) {
      if (!response.complete || o.eof !== true || o.return_awaited !== false || Object.hasOwn(o, 'error')) fail('original EOF');
    } else if (o.cut === 'return') {
      if (record.case !== 'attach-live' || o.eof !== false || o.return_awaited !== true || o.frames.length !== 1 ||
          o.frames[0].event !== 'state' || o.frames[0].data.live !== true || o.frames[0].data.status !== 'busy' || Object.hasOwn(o, 'error')) fail('original viewer return');
      out.cut = 'viewer-return';
    } else if (o.cut === 'signal') {
      keys(o.error, ['name']);
      if (record.case !== 'attach-held-start' || o.eof !== false || o.return_awaited !== true || o.frames.length || o.error.name !== 'AbortError') fail('original stream abort');
      out.cut = 'signal-abort';
    } else fail('unexpected cut');
    const decoded = decodeCapturedSse(response.body, { allowPartial: !response.complete });
    for (const frame of decoded.frames) frameShape(frame);
    if (o.cut === null) equal(o.frames, decoded.frames, 'actual consumed SSE');
    else equal(o.frames, decoded.frames.slice(0, o.frames.length), 'actual consumed SSE prefix');
    out.frames = o.frames.map(frameShape); out.eof = o.eof; out.result_shape = 'frame-stream';
    if (o.cut === null && (out.frames.at(-1)?.event !== 'done' || out.frames.filter((f) => f.event === 'done').length !== 1)) fail('terminal EOF');
    return out;
  }
  if (o.kind !== 'returned') fail('unfinished invocation');
  keys(o, ['kind', 'value']); if (!response.complete) fail('incomplete returned response');
  const body = response.body.length ? json(response.body) : null;
  const method = record.method.split('.')[1];
  if (record.method.startsWith('controller.') && method !== 'interrupt') {
    if (response.status !== 200) fail('factory status');
    equal(o.value, body, 'actual factory value');
    if (method === 'cancel') { if (body !== null) fail('true Void'); out.result_shape = 'void-null'; }
    else { equal(body, { ok: true, message: 'fixture-value' }, 'finite factory value'); out.result_shape = 'route-outcome'; }
  } else if (method === 'interrupt' || method === 'cancel') {
    if (response.status === 200) {
      keys(body, ['status', 'thread_id']); if (body.status !== 'interrupted' || body.thread_id !== record.arguments[0] || o.value !== 'interrupted') fail('actual cancel');
    } else {
      const code = refusal(body);
      const result = method === 'interrupt' ? 'no_run_in_flight' : response.status === 404 ? 'thread_not_found' : 'no_run_in_flight';
      if (![404, 409].includes(response.status) || (response.status === 404 ? code !== 'resource_unavailable' : code !== 'no_run_in_flight') || o.value !== result) fail('original cancel reduction');
    }
    out.result_shape = method === 'interrupt' ? 'interrupt-result' : 'cancel-result';
  } else if (method === 'createThread' || method === 'getThread') {
    if (response.status === 404 && method === 'getThread') {
      if (refusal(body) !== 'resource_unavailable' || o.value !== null) fail('original missing read'); out.result_shape = 'null';
    } else {
      if (![200, 201].includes(response.status)) fail('thread status');
      keys(body, ['thread_id', 'status'], ['created_at', 'updated_at', 'metadata']); string(body.thread_id);
      if (!['idle', 'busy', 'interrupted'].includes(body.status)) fail('thread projection');
      equal(o.value, method === 'createThread' ? body.thread_id : { threadId: body.thread_id, status: body.status }, 'actual thread value');
      if (method === 'getThread' && body.thread_id !== record.arguments[0]) fail('thread identity');
      out.result_shape = method === 'createThread' ? 'thread-id' : 'thread-metadata';
    }
  } else if (method === 'deleteThread') {
    if (response.status === 204) { if (response.body.length || o.value !== 'deleted') fail('actual deletion'); out.result_shape = 'deleted'; }
    else if (response.status === 404 && o.value === 'not_found' && refusal(body) === 'resource_unavailable') out.result_shape = 'not-found';
    else fail('original deletion');
  } else if (method === 'pendingInterrupts') {
    if (response.status !== 200 || o.value?.length !== 0) fail('empty pending'); keys(body, ['interrupts']); equal(body.interrupts, [], 'actual empty pending');
    equal(o.value, [], 'actual pending result'); out.result_shape = 'pending-empty';
  } else fail('unexpected returned method');
  return out;
}

const FUNCTIONS = ['C1_factory_method_map', 'C2_controller_auth', 'C3_wait_terminals', 'C4_held_controller',
  'W1_worker_crud_auth', 'W2_finite_sse', 'W3_attachment_cancel', 'W4_durable_park_refusal'];

function selectedArguments(records, ready) {
  const byCase = new Map(records.map((r) => [r.case, r]));
  const check = (name, args) => equal(byCase.get(name).arguments, args, 'selected arguments');
  const signal = () => ({ kind: 'AbortSignal', aborted_at_invocation: false });
  check('factory-create', [{ taskId: 'fixture-catalog-task', operationKey: 'fixture-create' }]);
  for (const m of ['dispatch', 'retry', 'deny', 'intake', 'cancel']) check(`factory-${m}`, [`fixture-c1-${m}`, 'ordinary']);
  check('factory-approve', ['fixture-c1-approve', { revision: 1, bundleDigest: 'a'.repeat(64), operationKey: 'ordinary' }]);
  check('factory-approve-intake', ['fixture-c1-approve-intake', { revision: 1, taskDigest: 'b'.repeat(64), operationKey: 'ordinary' }]);
  check('factory-reject-intake', ['fixture-c1-reject-intake', { note: 'finite fixture note', operationKey: 'ordinary' }]);
  check('factory-reconcile', []);
  for (const [name, id, key] of [['default-auth-refusal', 'fixture-c2-direct', 'ordinary'],
    ['bearer-auth-positive', 'fixture-c2-positive', 'ordinary'], ['wait-value', 'fixture-c3-value', 'ordinary'],
    ['wait-failure', 'fixture-c3-failure', 'fixture-known-failure'], ['wait-void', 'fixture-c3-void', 'ordinary'],
    ['held-busy-before-abort', 'fixture-c4-abandon', 'ordinary'], ['held-busy-after-abort', 'fixture-c4-abandon', 'ordinary'],
    ['held-busy-after-cancel', 'fixture-c4-cancel', 'ordinary'], ['concealed-establish', 'fixture-concealed-controller', 'ordinary']]) check(name, [id, key]);
  for (const [name, id] of [['held-dispatch-abandon', 'fixture-c4-abandon'], ['held-dispatch-cancel', 'fixture-c4-cancel']]) check(name, [id, 'ordinary', signal()]);
  for (const [name, id] of [['interrupt-abandoned-held', 'fixture-c4-abandon'], ['interrupt-held', 'fixture-c4-cancel'],
    ['interrupt-idle', 'fixture-c3-value'], ['interrupt-missing', 'fixture-missing-controller'], ['interrupt-concealed', 'fixture-concealed-controller']]) check(name, [id]);
  const created = new Map();
  for (const r of records.filter((r) => r.method === 'worker.createThread')) {
    check(r.case, [{ fixture_case: r.case }]);
    if (r.outcome.kind !== 'returned') fail('selected create outcome');
    const id = string(r.outcome.value);
    if ([...created.values()].includes(id) || id.startsWith('fixture-') || id.startsWith('create:') || id === 'controller') fail('generated identity');
    created.set(r.case, id);
  }
  const crud = created.get('worker-create'); const held = created.get('worker-held-create');
  for (const name of ['worker-get', 'worker-get-wrong-token', 'worker-delete', 'worker-delete-missing']) check(name, [crud]);
  check('worker-get-missing', ['fixture-missing-worker']); check('worker-get-concealed', ['fixture-concealed-controller']);
  check('worker-held-start', [held, ready.routes.worker_held, 'finite held input', signal()]);
  for (const name of ['worker-delete-held', 'worker-delete-released']) check(name, [held]);
  for (const kind of ['value', 'void', 'failure']) check(`${kind}-start`, [created.get(`${kind}-create`), ready.routes[`worker_${kind}`], 'finite stream input']);
  const attach = created.get('attach-create'); const cancelled = created.get('attach-cancel-create'); const park = created.get('park-create');
  check('attach-held-start', [attach, ready.routes.worker_held, 'finite attachment input', signal()]);
  check('attach-live', [attach, signal()]);
  for (const name of ['attach-busy-after-viewer-cut', 'attach-busy-after-post-abort']) check(name, [attach, ready.routes.worker_value, 'busy']);
  check('attach-cancel-held-start', [cancelled, ready.routes.worker_held, 'finite cancelled input', signal()]);
  check('attach-busy-after-cancel', [cancelled, ready.routes.worker_value, 'busy']);
  for (const name of ['attach-settled', 'worker-cancel-held', 'worker-cancel-idle']) check(name, [cancelled]);
  check('worker-cancel-missing', ['fixture-missing-worker']); check('worker-cancel-concealed', ['fixture-concealed-controller']);
  for (const name of ['pending-empty', 'pending-original-schema-refusal']) check(name, [park]);
  check('park-start', [park, ready.routes.worker_park, 'finite approval input']);
  const parkFrames = byCase.get('park-start').outcome.frames;
  if (!Array.isArray(parkFrames) || parkFrames.length !== 2 || parkFrames[0].event !== 'interrupt') fail('selected park frames');
  nativeInterrupt(parkFrames[0].data);
  for (const payload of ['once', 'deny']) check(`resume-forced-${payload}`, [park, ready.routes.worker_park, [{ interruptId: parkFrames[0].data.interruptId, payload }]]);
  check('resume-empty', [park, ready.routes.worker_park, []]);
}

function selectedOutcome(record, row) {
  const name = record.case;
  if (Object.hasOwn(ERRORS, name)) { if (row.outcome !== 'rejected') fail('selected refusal outcome'); return; }
  if (name === 'held-dispatch-abandon') { if (row.outcome !== 'rejected' || row.cut !== 'signal-abort') fail('selected abort outcome'); return; }
  if (name === 'pending-original-schema-refusal') { if (row.outcome !== 'rejected' || row.error_class !== 'ZodError') fail('selected schema outcome'); return; }
  const frames = {
    'worker-held-start': ['done-value'], 'value-start': ['done-value'], 'void-start': ['done-void'],
    'failure-start': ['done-failure'], 'attach-held-start': [], 'attach-live': ['state-busy'],
    'attach-cancel-held-start': ['done-cancelled'], 'attach-settled': ['state-idle', 'done-null'],
    'park-start': ['native-permission-request', 'done-null'],
  };
  if (Object.hasOwn(frames, name)) {
    if (row.outcome !== 'stream') fail('selected stream outcome');
    equal(row.frames.map((f) => f.shape), frames[name], 'selected stream frames');
    const cut = name === 'attach-held-start' ? 'signal-abort' : name === 'attach-live' ? 'viewer-return' : 'none';
    if (row.cut !== cut || row.eof !== (cut === 'none')) fail('selected stream completion');
    if (name === 'attach-settled' && record.outcome.frames[0].data.live !== false) fail('settled state');
    return;
  }
  if (row.outcome !== 'returned') fail('selected return outcome');
  const status = name === 'worker-delete' || name === 'worker-delete-missing' || name === 'worker-delete-released' ? 204
    : ['worker-get-missing', 'worker-get-concealed', 'interrupt-missing', 'interrupt-concealed', 'worker-cancel-missing', 'worker-cancel-concealed'].includes(name) ? 404
      : ['interrupt-idle', 'worker-cancel-idle'].includes(name) ? 409 : 200;
  if (row.http_status !== status) fail('selected native status');
}

// HTTP/1 requests are ordered within each connection. Across connections, this
// bounded bipartite assignment proves multiset equality; it invents no timing or
// delivery ordering for observationally identical requests.
export function auditClientTraffic(rawClient, captures, ready) {
  pure(rawClient); pure(ready);
  keys(rawClient, ['version', 'kind', 'groups', 'transports']);
  keys(ready, ['base_url', 'token', 'routes']);
  if (rawClient.version !== 1 || rawClient.kind !== 'original-ap-client-observations' ||
      !/^http:\/\/127\.0\.0\.1:[1-9][0-9]{0,4}$/.test(ready.base_url) || new URL(ready.base_url).port > 65535 ||
      !/^[a-f0-9]{64}$/.test(ready.token)) fail('corpus scope');
  const routes = Object.fromEntries(['value', 'void', 'failure', 'held', 'park'].map((kind) => [`worker_${kind}`, `/fixture/worker-${kind}#workflow`]));
  equal(ready.routes, routes, 'fixture routes');
  if (!Array.isArray(rawClient.groups) || rawClient.groups.length !== 8 || !Array.isArray(rawClient.transports) || rawClient.transports.length !== 62) fail('corpus cardinality');
  const names = Object.keys(CASES); const records = []; let ordinal = 0;
  for (const [index, group] of rawClient.groups.entries()) {
    keys(group, ['name', 'function', 'observations', 'witnesses']);
    const name = names[index];
    if (group.name !== name || group.function !== FUNCTIONS[index] || !Array.isArray(group.observations) || group.observations.length !== CASES[name].length || !Array.isArray(group.witnesses)) fail('selected groups');
    for (const [position, record] of group.observations.entries()) {
      keys(record, ['ordinal', 'method', 'case', 'arguments', 'outcome']);
      if (record.ordinal !== ++ordinal || record.case !== CASES[name][position] || record.method !== METHODS[name][position]) fail('selected invocations');
      records.push(record);
    }
  }
  selectedArguments(records, ready);
  const transports = new Map();
  for (const transport of rawClient.transports) {
    if (!Number.isSafeInteger(transport.ordinal) || transport.ordinal < 1 || transport.ordinal > records.length || transports.has(transport.ordinal) || records[transport.ordinal - 1].case === 'default-auth-refusal') fail('transport ordinals');
    transports.set(transport.ordinal, transport);
  }
  if (!Array.isArray(captures) || Object.getPrototypeOf(captures) !== Array.prototype || captures.length < 1 || captures.length > 16 || Reflect.ownKeys(captures).length !== captures.length + 1) fail('capture slots');
  const pairs = [];
  const expected = records.map((r) => expectedRequest(r, ready));
  const cutRequests = expected.filter((_, index) => CUTS.has(records[index].case));
  for (let index = 0; index < captures.length; index++) {
    const descriptor = Object.getOwnPropertyDescriptor(captures, index);
    if (!descriptor || !Object.hasOwn(descriptor, 'value') || !descriptor.enumerable) fail('capture element');
    const capture = descriptor.value;
    keys(capture, ['ordinal', 'read', 'write', 'read_eof', 'write_shutdown']);
    if (capture.ordinal !== index + 1 || !Buffer.isBuffer(capture.read) || !Buffer.isBuffer(capture.write) ||
        typeof capture.read_eof !== 'boolean' || typeof capture.write_shutdown !== 'boolean') fail('capture metadata');
    const requests = decodeHttpRequests(capture.read);
    const allowedIncompleteResponses = requests.filter((r) => cutRequests.some((e) => r.method === e.method && r.target === e.target && r.body.toString('utf8') === (e.body ?? ''))).map((r) => r.index);
    const decoded = decodeHttpConnection({ read: capture.read, write: capture.write }, { allowedIncompleteResponses });
    pairs.push(...decoded.pairs);
    if (pairs.length > records.length) fail('extra captured request');
  }
  if (pairs.length !== records.length) fail('captured request cardinality');
  const edges = records.map((record) => {
    const matches = [];
    for (const [index, pair] of pairs.entries()) {
      try {
        const row = auditInvocationTraffic(record, transports.get(record.ordinal) ?? null, pair, ready);
        selectedOutcome(record, row); matches.push({ index, row });
      } catch (error) {
        if (!(error instanceof Error) || !/^(Client traffic:|Private artifacts:|Wire:|SSE:)/.test(error.message)) throw error;
      }
    }
    if (!matches.length) fail('unmatched invocation');
    return matches;
  });
  const owners = Array(pairs.length).fill(-1); const chosen = Array(records.length);
  const assign = (record, visited) => {
    for (const edge of edges[record]) {
      if (visited.has(edge.index)) continue;
      visited.add(edge.index);
      if (owners[edge.index] === -1 || assign(owners[edge.index], visited)) {
        owners[edge.index] = record; chosen[record] = edge.row; return true;
      }
    }
    return false;
  };
  for (let index = 0; index < records.length; index++) if (!assign(index, new Set())) fail('no complete one-to-one traffic assignment');
  let offset = 0;
  const groups = names.map((name) => {
    const observations = chosen.slice(offset, offset + CASES[name].length); offset += CASES[name].length;
    return { name, observations };
  });
  return { groups, requestCount: pairs.length, transportCount: transports.size };
}
