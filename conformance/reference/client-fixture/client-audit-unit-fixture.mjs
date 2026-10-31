// SYNTHETIC UNIT FIXTURE ONLY. No original caller, native host, socket, or file is
// executed here. Static IDs and authority values have no provenance or authority.
// The requests below follow the pinned controller/client.ts and worker/client.ts
// serialization; the replies follow the separately reviewed native fixture.
// This fixture supplies traffic facts, not the parent control/retirement proof.

const FUNCTIONS = [
  'C1_factory_method_map', 'C2_controller_auth', 'C3_wait_terminals', 'C4_held_controller',
  'W1_worker_crud_auth', 'W2_finite_sse', 'W3_attachment_cancel', 'W4_durable_park_refusal',
];
const TIME = '2026-10-02T00:00:00Z';
const SIGNAL = () => ({ kind: 'AbortSignal', aborted_at_invocation: false });
const VALUE = () => ({ ok: true, message: 'fixture-value' });
const DONE = (output) => ({ event: 'done', data: { output } });
const WORKER_VALUE = () => ({ fixture: 'value' });
const returned = (value) => ({ kind: 'returned', value });
const rejected = (name, status, code, message) => ({
  kind: 'rejected', error: { name, status, code, message, stack: null },
});
const stream = (frames) => ({ kind: 'stream', frames, eof: true, cut: null,
  reader_polled: true, pending_settled: true, return_awaited: false });
const nativeError = (code) => ({ error: { kind: 'request_error', message: code, details: { code } } });
const headerRows = (headers) => Object.entries(headers).sort(([a], [b]) => a.localeCompare(b));

// Content-Length is measured in encoded bytes. Chunked SSE includes a real last
// chunk on complete replies. The held POST deliberately ends after its headers.
function reply(status, value, { sse = false, incomplete = false, empty = false, retryAfterState = false } = {}) {
  const headers = empty ? {} : sse
    ? { 'cache-control': 'no-cache', 'content-type': 'text/event-stream', 'transfer-encoding': 'chunked' }
    : { 'content-type': 'application/json' };
  let body = Buffer.alloc(0);
  if (sse) {
    const frames = value.map((frame, index) => `event: ${frame.event}\ndata: ${JSON.stringify(frame.data)}\n\n`
      + (retryAfterState && index === 0 ? 'retry: 2000\n\n' : '')).join('');
    const bytes = Buffer.from(frames, 'utf8');
    const chunks = bytes.length ? Buffer.concat([
      Buffer.from(`${bytes.length.toString(16)}\r\n`, 'ascii'), bytes, Buffer.from('\r\n', 'ascii'),
    ]) : Buffer.alloc(0);
    body = incomplete ? chunks : Buffer.concat([chunks, Buffer.from('0\r\n\r\n', 'ascii')]);
  } else if (!empty) {
    body = Buffer.from(JSON.stringify(value), 'utf8');
    headers['content-length'] = String(body.length);
  }
  const names = { 200: 'OK', 204: 'No Content', 400: 'Bad Request',
    401: 'Unauthorized', 404: 'Not Found', 409: 'Conflict', 500: 'Internal Server Error' };
  const head = Buffer.from(`HTTP/1.1 ${status} ${names[status]}\r\n${headerRows(headers)
    .map(([k, v]) => `${k}: ${v}\r\n`).join('')}\r\n`, 'ascii');
  return { status, headers, bytes: Buffer.concat([head, body]) };
}

function request(ready, target, method, body, { contentType = false, worker = false,
  direct = false, wrongToken = false, signal = false } = {}) {
  const original = {};
  if (contentType) original['content-type'] = 'application/json';
  if (worker) original.authorization = `Bearer ${wrongToken ? 'fixture-invalid-token' : ready.token}`;
  const forwarded = { ...original };
  if (!worker && !direct) forwarded.authorization = `Bearer ${ready.token}`;
  const captureHeaders = { host: new URL(ready.base_url).host, ...forwarded };
  if (body !== null) captureHeaders['content-length'] = String(Buffer.byteLength(body));
  // The original caller supplies no body for these methods; Fetch may emit the
  // equivalent Content-Length: 0, so the synthetic capture includes that framing.
  else if (method === 'POST') captureHeaders['content-length'] = '0';
  const head = `${method} ${target} HTTP/1.1\r\n${headerRows(captureHeaders)
    .map(([k, v]) => `${k}: ${v}\r\n`).join('')}\r\n`;
  return { captured: Buffer.from(head + (body ?? ''), 'utf8'), transport: {
    url: ready.base_url + target, method, original_headers: headerRows(original),
    forwarded_headers: headerRows(forwarded), body, has_signal: signal, redirect: 'error',
  } };
}

export function makeClientTrafficUnitFixture() {
  const ready = { base_url: 'http://127.0.0.1:32123', token: 'c'.repeat(64), routes: {
    worker_value: '/fixture/worker-value#workflow', worker_void: '/fixture/worker-void#workflow',
    worker_failure: '/fixture/worker-failure#workflow', worker_held: '/fixture/worker-held#workflow',
    worker_park: '/fixture/worker-park#workflow',
  } };
  const rawClient = { version: 1, kind: 'original-ap-client-observations', groups: [], transports: [] };
  const pools = Array.from({ length: 6 }, () => ({ reads: [], writes: [] }));
  let ordinal = 0; let ordinary = 0; let group;
  const path = (id, tail = '') => `/threads/${encodeURIComponent(id)}${tail}`;
  const begin = (name) => {
    group = { name, function: FUNCTIONS[rawClient.groups.length], observations: [], witnesses: [] };
    rawClient.groups.push(group);
  };
  function add(method, caseName, args, req, out, res, cutPool = null) {
    const n = ++ordinal;
    group.observations.push({ ordinal: n, case: caseName, method, arguments: args, outcome: out });
    if (caseName !== 'default-auth-refusal') {
      const row = { ordinal: n, request: req.transport };
      if (caseName === 'held-dispatch-abandon') row.error = { ...out.error };
      else row.response = { status: res.status, headers: headerRows(res.headers) };
      rawClient.transports.push(row);
    }
    const pool = pools[cutPool ?? ordinary++ % 4];
    pool.reads.push(req.captured); pool.writes.push(res?.bytes ?? Buffer.alloc(0));
  }
  function controllerRun(method, caseName, args, route, input, options = {}) {
    const id = options.id ?? args[0];
    const req = request(ready, path(id, '/runs/wait'), 'POST', JSON.stringify({ route, input }),
      { contentType: true, direct: options.direct ?? false, signal: options.signal ?? false });
    const out = options.outcome ?? returned(options.void ? null : VALUE());
    const res = options.response === undefined ? reply(200, options.void ? null : VALUE()) : options.response;
    add(`controller.${method}`, caseName, args, req, out, res, options.cutPool ?? null);
  }
  function controllerDispatch(caseName, id, key = 'ordinary', options = {}) {
    controllerRun('dispatch', caseName, [id, key, ...(options.signal ? [SIGNAL()] : [])],
      '/work-orders/dispatch#workflow', { id, operationKey: key }, options);
  }
  function controllerRefusal(caseName, id, code, options = {}) {
    const status = options.status ?? 409;
    controllerDispatch(caseName, id, options.key ?? 'ordinary', { ...options,
      outcome: rejected('ControllerHttpError', status, code, code ?? 'fixture-known-failure'),
      response: reply(status, code === null
        ? { error: { kind: 'execution_error', message: 'fixture-known-failure' } } : nativeError(code)),
    });
  }
  function cancel(family, caseName, id, status, result, code = null) {
    const req = request(ready, path(id, '/cancel'), 'POST', null, { worker: family === 'worker' });
    const res = reply(status, status === 200 ? { thread_id: id, status: 'interrupted' } : nativeError(code));
    add(`${family}.${family === 'controller' ? 'interrupt' : 'cancel'}`, caseName,
      [id], req, returned(result), res);
  }
  function create(caseName, id) {
    const metadata = { fixture_case: caseName };
    const req = request(ready, '/threads', 'POST', JSON.stringify({ metadata }), { worker: true, contentType: true });
    add('worker.createThread', caseName, [metadata], req, returned(id), reply(200, {
      thread_id: id, created_at: TIME, updated_at: TIME, metadata, status: 'idle',
    }));
  }
  function workerRefusal(method, caseName, args, req, status, code) {
    add(`worker.${method}`, caseName, args, req, rejected('WorkerHttpError', status, code,
      `Worker responded ${status} (${code}): ${code}`), reply(status, nativeError(code)));
  }
  function get(caseName, id, { missing = false, wrong = false } = {}) {
    const req = request(ready, path(id), 'GET', null, { worker: true, wrongToken: wrong });
    if (wrong) workerRefusal('getThread', caseName, [id], req, 401, 'authentication_required');
    else add('worker.getThread', caseName, [id], req, returned(missing ? null : { threadId: id, status: 'idle' }),
      reply(missing ? 404 : 200, missing ? nativeError('resource_unavailable') : {
        thread_id: id, created_at: TIME, updated_at: TIME, metadata: { fixture_case: 'worker-create' }, status: 'idle',
      }));
  }
  function remove(caseName, id, held = false) {
    const req = request(ready, path(id), 'DELETE', null, { worker: true });
    if (held) workerRefusal('deleteThread', caseName, [id], req, 409, 'run_in_flight');
    else add('worker.deleteThread', caseName, [id], req, returned('deleted'), reply(204, null, { empty: true }));
  }
  function start(caseName, id, route, content, { signal = false, frames = [DONE(WORKER_VALUE())],
    refusal = false, cut = false } = {}) {
    const args = [id, route, content, ...(signal ? [SIGNAL()] : [])];
    const req = request(ready, path(id, '/runs/stream'), 'POST', JSON.stringify({ route,
      input: { messages: [{ role: 'user', content }] } }), { worker: true, contentType: true, signal });
    if (refusal) { workerRefusal('startRun', caseName, args, req, 409, 'thread_busy'); return; }
    const out = cut ? { kind: 'stream', frames: [], eof: false, cut: 'signal', reader_polled: true,
      pending_settled: true, return_awaited: true, error: { name: 'AbortError' } } : stream(frames);
    add('worker.startRun', caseName, args, req, out, reply(200, cut ? [] : frames,
      { sse: true, incomplete: cut }), cut ? 5 : null);
  }
  const userInput = (content) => ({ messages: [{ role: 'user', content }] });
  const live = { anchor: 'synthetic-attachment-execution', input: userInput('finite attachment input'),
    interrupts: [], live: true, resume: false, run_started_at: TIME, status: 'busy', turn: [],
    values: { input: userInput('finite attachment input') } };
  const idle = { anchor: null, input: null, interrupts: [], live: false, resume: false,
    run_started_at: null, status: 'idle', turn: null, values: { input: userInput('finite cancelled input') } };
  const grant = { interruptId: 'synthetic-private-interrupt', resumeKey: 'd'.repeat(64),
    value: { question: 'fixture-approval' }, grant: `b4ag_${'e'.repeat(64)}` };

  begin('C1');
  const createArgs = { taskId: 'fixture-catalog-task', operationKey: 'fixture-create' };
  controllerRun('create', 'factory-create', [createArgs], '/work-orders/create#workflow', createArgs,
    { id: 'create:fixture-create' });
  controllerDispatch('factory-dispatch', 'fixture-c1-dispatch');
  const approve = { revision: 1, bundleDigest: 'a'.repeat(64), operationKey: 'ordinary' };
  controllerRun('approve', 'factory-approve', ['fixture-c1-approve', approve], '/work-orders/approve#workflow',
    { id: 'fixture-c1-approve', ...approve });
  for (const method of ['retry', 'deny', 'intake']) {
    const id = `fixture-c1-${method}`;
    controllerRun(method, `factory-${method}`, [id, 'ordinary'], `/work-orders/${method}#workflow`, { id, operationKey: 'ordinary' });
  }
  const intake = { revision: 1, taskDigest: 'b'.repeat(64), operationKey: 'ordinary' };
  controllerRun('approveIntake', 'factory-approve-intake', ['fixture-c1-approve-intake', intake],
    '/work-orders/approve-intake#workflow', { id: 'fixture-c1-approve-intake', ...intake });
  const reject = { note: 'finite fixture note', operationKey: 'ordinary' };
  controllerRun('rejectIntake', 'factory-reject-intake', ['fixture-c1-reject-intake', reject],
    '/work-orders/reject-intake#workflow', { id: 'fixture-c1-reject-intake', ...reject });
  controllerRun('cancel', 'factory-cancel', ['fixture-c1-cancel', 'ordinary'], '/work-orders/cancel#workflow',
    { id: 'fixture-c1-cancel', operationKey: 'ordinary' }, { void: true });
  controllerRun('reconcile', 'factory-reconcile', [], '/reconcile#workflow', {}, { id: 'controller' });

  begin('C2');
  controllerRefusal('default-auth-refusal', 'fixture-c2-direct', 'authentication_required', { status: 401, direct: true });
  controllerDispatch('bearer-auth-positive', 'fixture-c2-positive');
  group.witnesses.push({ name: 'default-fetch-versus-bearer-only-injection', control_ordinal: null, value: true });

  begin('C3');
  controllerDispatch('wait-value', 'fixture-c3-value');
  controllerRefusal('wait-failure', 'fixture-c3-failure', null, { status: 500, key: 'fixture-known-failure' });
  controllerRun('cancel', 'wait-void', ['fixture-c3-void', 'ordinary'], '/work-orders/cancel#workflow',
    { id: 'fixture-c3-void', operationKey: 'ordinary' }, { void: true });

  begin('C4');
  controllerDispatch('held-dispatch-abandon', 'fixture-c4-abandon', 'ordinary', { signal: true, cutPool: 4,
    outcome: rejected('AbortError', null, 20, 'This operation was aborted'), response: null });
  controllerRefusal('held-busy-before-abort', 'fixture-c4-abandon', 'thread_busy');
  controllerRefusal('held-busy-after-abort', 'fixture-c4-abandon', 'thread_busy');
  cancel('controller', 'interrupt-abandoned-held', 'fixture-c4-abandon', 200, 'interrupted');
  controllerRefusal('held-dispatch-cancel', 'fixture-c4-cancel', 'run_cancelled', { signal: true });
  cancel('controller', 'interrupt-held', 'fixture-c4-cancel', 200, 'interrupted');
  controllerRefusal('held-busy-after-cancel', 'fixture-c4-cancel', 'thread_busy');
  cancel('controller', 'interrupt-idle', 'fixture-c3-value', 409, 'no_run_in_flight', 'no_run_in_flight');
  cancel('controller', 'interrupt-missing', 'fixture-missing-controller', 404, 'no_run_in_flight', 'resource_unavailable');
  controllerDispatch('concealed-establish', 'fixture-concealed-controller');
  cancel('controller', 'interrupt-concealed', 'fixture-concealed-controller', 404, 'no_run_in_flight', 'resource_unavailable');

  begin('W1');
  const crud = 'synthetic-worker-crud'; const held = 'synthetic-worker-held';
  create('worker-create', crud);
  get('worker-get', crud);
  get('worker-get-missing', 'fixture-missing-worker', { missing: true });
  get('worker-get-concealed', 'fixture-concealed-controller', { missing: true });
  get('worker-get-wrong-token', crud, { wrong: true });
  remove('worker-delete', crud); remove('worker-delete-missing', crud);
  create('worker-held-create', held);
  start('worker-held-start', held, ready.routes.worker_held, 'finite held input', { signal: true });
  remove('worker-delete-held', held, true); remove('worker-delete-released', held);

  begin('W2');
  for (const kind of ['value', 'void', 'failure']) {
    const id = `synthetic-worker-${kind}`;
    create(`${kind}-create`, id);
    const frames = kind === 'void' ? [{ event: 'done', data: {} }]
      : [DONE(kind === 'value' ? WORKER_VALUE() : { error: 'fixture-known-failure' })];
    start(`${kind}-start`, id, ready.routes[`worker_${kind}`], 'finite stream input', { frames });
  }

  begin('W3');
  const attach = 'synthetic-worker-attachment'; const cancelled = 'synthetic-worker-cancelled';
  create('attach-create', attach);
  start('attach-held-start', attach, ready.routes.worker_held, 'finite attachment input', { signal: true, cut: true });
  const viewer = request(ready, path(attach, '/runs/stream'), 'GET', null,
    { worker: true, contentType: true, signal: true });
  add('worker.reattach', 'attach-live', [attach, SIGNAL()], viewer, { kind: 'stream',
    frames: [{ event: 'state', data: live }], eof: false, cut: 'return', reader_polled: true,
    pending_settled: true, return_awaited: true },
  reply(200, [{ event: 'state', data: live }, DONE(WORKER_VALUE())], { sse: true }));
  for (const caseName of ['attach-busy-after-viewer-cut', 'attach-busy-after-post-abort']) {
    start(caseName, attach, ready.routes.worker_value, 'busy', { refusal: true });
  }
  create('attach-cancel-create', cancelled);
  start('attach-cancel-held-start', cancelled, ready.routes.worker_held, 'finite cancelled input',
    { signal: true, frames: [DONE({ cancelled: true })] });
  cancel('worker', 'worker-cancel-held', cancelled, 200, 'interrupted');
  start('attach-busy-after-cancel', cancelled, ready.routes.worker_value, 'busy', { refusal: true });
  add('worker.reattach', 'attach-settled', [cancelled], request(ready, path(cancelled, '/runs/stream'),
    'GET', null, { worker: true, contentType: true }), stream([{ event: 'state', data: idle }, DONE(null)]),
  reply(200, [{ event: 'state', data: idle }, DONE(null)], { sse: true, retryAfterState: true }));
  cancel('worker', 'worker-cancel-idle', cancelled, 409, 'no_run_in_flight', 'no_run_in_flight');
  // Pinned worker.cancel reduces every native 404 to thread_not_found, including
  // this missing thread. Controller.interrupt has a different 404 reduction.
  cancel('worker', 'worker-cancel-missing', 'fixture-missing-worker', 404, 'thread_not_found', 'resource_unavailable');
  cancel('worker', 'worker-cancel-concealed', 'fixture-concealed-controller', 404, 'thread_not_found', 'resource_unavailable');

  begin('W4');
  const park = 'synthetic-worker-park';
  create('park-create', park);
  const pending = () => request(ready, path(park, '/pending_interrupts'), 'GET', null, { worker: true, contentType: true });
  add('worker.pendingInterrupts', 'pending-empty', [park], pending(), returned([]), reply(200, { interrupts: [] }));
  start('park-start', park, ready.routes.worker_park, 'finite approval input',
    { frames: [{ event: 'interrupt', data: grant }, DONE(null)] });
  add('worker.pendingInterrupts', 'pending-original-schema-refusal', [park], pending(),
    rejected('ZodError', null, null, 'Synthetic schema refusal: native interrupt lacks the original permission-request fields'),
    reply(200, { interrupts: [grant] }));
  for (const payload of ['once', 'deny']) {
    const resolutions = [{ interruptId: grant.interruptId, payload }];
    const req = request(ready, path(park, '/resume'), 'POST', JSON.stringify({ resume: [
      { interruptId: grant.interruptId, status: 'resolved', payload },
    ], route: ready.routes.worker_park }), { worker: true, contentType: true });
    workerRefusal('resume', `resume-forced-${payload}`, [park, ready.routes.worker_park, resolutions], req, 400, 'grant_required');
  }
  workerRefusal('resume', 'resume-empty', [park, ready.routes.worker_park, []],
    request(ready, path(park, '/resume'), 'POST', JSON.stringify({ resume: [], route: ready.routes.worker_park }),
      { worker: true, contentType: true }), 400, 'invalid_request');
  group.witnesses.push({ name: 'forced-id-only-no-grant-and-metadata-unchanged', control_ordinal: null, value: true });

  const captures = pools.map((pool, index) => ({ ordinal: index + 1, read: Buffer.concat(pool.reads),
    write: Buffer.concat(pool.writes), read_eof: index < 4, write_shutdown: index < 4 }));
  return { rawClient, captures, ready };
}
