import test from 'node:test';
import assert from 'node:assert/strict';
import { auditInvocationTraffic, auditClientTraffic } from './client-audit.mjs';
import { decodeHttpConnection } from './wire.mjs';
import { makeClientTrafficUnitFixture } from './client-audit-unit-fixture.mjs';

// These are synthetic unit bytes and observations, never actual client evidence.
const ready = { base_url: 'http://127.0.0.1:12345', token: 'private-unit-token', routes: {
  worker_value: '/fixture/worker-value#workflow', worker_void: '/fixture/worker-void#workflow',
  worker_failure: '/fixture/worker-failure#workflow', worker_held: '/fixture/worker-held#workflow', worker_park: '/fixture/worker-park#workflow',
} };
const error = (name, status, code) => ({ name, status, code, message: code ?? 'private unit error', stack: 'private unit stack' });
function observation(method, caseName, args, value) {
  return { ordinal: 1, method, case: caseName, arguments: args, outcome: { kind: 'returned', value } };
}
function transport(record, target, body, role = 'controller', status = 200) {
  const original = role === 'controller' ? [['content-type', 'application/json']] : [['authorization', `Bearer ${ready.token}`], ['content-type', 'application/json']];
  const forwarded = role === 'controller' ? [['authorization', `Bearer ${ready.token}`], ...original] : original;
  return { ordinal: record.ordinal, request: { url: ready.base_url + target, method: 'POST', original_headers: original,
    forwarded_headers: forwarded, body, has_signal: false, redirect: 'error' }, response: { status, headers: [['content-type', 'application/json']] } };
}
function pair(t, responseBody, status = 200, contentType = 'application/json') {
  const requestBody = t.request.body ?? '';
  const request = Buffer.from(`${t.request.method} ${new URL(t.request.url).pathname} HTTP/1.1\r\nHost: 127.0.0.1:12345\r\n${t.request.forwarded_headers.map(([k,v])=>`${k}: ${v}\r\n`).join('')}Content-Length: ${Buffer.byteLength(requestBody)}\r\n\r\n${requestBody}`);
  const body = Buffer.from(responseBody);
  const response = Buffer.concat([Buffer.from(`HTTP/1.1 ${status} Unit\r\nContent-Type: ${contentType}\r\nContent-Length: ${body.length}\r\n\r\n`), body]);
  t.response.headers = [['content-type', contentType], ['content-length', String(body.length)]];
  return decodeHttpConnection({ read: request, write: response }).pairs[0];
}

test('actual captured request and returned value are correlated before safe facts exist', () => {
  const r = observation('controller.dispatch', 'factory-dispatch', ['unit-id', 'ordinary'], { ok: true, message: 'fixture-value' });
  const body = JSON.stringify({ route: '/work-orders/dispatch#workflow', input: { id: 'unit-id', operationKey: 'ordinary' } });
  const t = transport(r, '/threads/unit-id/runs/wait', body);
  const out = auditInvocationTraffic(r, t, pair(t, JSON.stringify(r.outcome.value)), ready);
  assert.equal(out.result_shape, 'route-outcome'); assert.equal(out.http_status, 200);
  const wrong = pair(t, '{"ok":true,"message":"other"}');
  assert.throws(() => auditInvocationTraffic(r, t, wrong, ready), /Client traffic/);
});

test('original createThread returns the captured thread_id string and the private request stays exact', () => {
  const r = observation('worker.createThread', 'worker-create', [{ fixture_case: 'worker-create' }], 'unit-created');
  const t = transport(r, '/threads', '{"metadata":{"fixture_case":"worker-create"}}', 'worker', 201);
  assert.equal(auditInvocationTraffic(r, t, pair(t, '{"thread_id":"unit-created","status":"idle"}', 201), ready).result_shape, 'thread-id');
  const changed = structuredClone(t); changed.request.body = '{"metadata":{"fixture_case":"rewritten"}}';
  assert.throws(() => auditInvocationTraffic(r, changed, pair(changed, '{"thread_id":"unit-created","status":"idle"}', 201), ready));
});

test('finite original SSE frames must match captured frames including a true Void terminal', () => {
  const r = observation('worker.startRun', 'void-start', ['unit-stream', ready.routes.worker_void, 'finite stream input']);
  r.outcome = { kind: 'stream', frames: [{ event: 'done', data: {} }], eof: true, cut: null,
    reader_polled: true, pending_settled: true, return_awaited: false };
  const body = JSON.stringify({ route: ready.routes.worker_void, input: { messages: [{ role: 'user', content: 'finite stream input' }] } });
  const t = transport(r, '/threads/unit-stream/runs/stream', body, 'worker'); t.response.headers = [['content-type', 'text/event-stream']];
  const out = auditInvocationTraffic(r, t, pair(t, 'event: done\ndata: {}\n\n', 200, 'text/event-stream'), ready);
  assert.deepEqual(out.frames, [{ event: 'done', shape: 'done-void' }]); assert.equal(out.eof, true);
  assert.throws(() => auditInvocationTraffic(r, t, pair(t, 'event: done\ndata: {"output":null}\n\n', 200, 'text/event-stream'), ready));
});

test('default original controller authentication refusal has no injected transport row', () => {
  const r = observation('controller.dispatch', 'default-auth-refusal', ['unit-direct', 'ordinary']);
  r.outcome = { kind: 'rejected', error: error('ControllerHttpError', 401, 'authentication_required') };
  const body = JSON.stringify({ route: '/work-orders/dispatch#workflow', input: { id: 'unit-direct', operationKey: 'ordinary' } });
  const t = transport(r, '/threads/unit-direct/runs/wait', body, 'controller', 401); t.request.forwarded_headers = [['content-type', 'application/json']];
  const captured = pair(t, '{"error":{"kind":"request_error","message":"authentication_required","details":{"code":"authentication_required"}}}', 401);
  const out = auditInvocationTraffic(r, null, captured, ready); assert.equal(out.error_code, 'authentication_required');
  assert.throws(() => auditInvocationTraffic(r, t, captured, ready), /Client traffic/);
});

test('a malformed or incomplete corpus is refused before publication', () => {
  assert.throws(() => auditClientTraffic({ version: 1, kind: 'original-ap-client-observations', groups: [], transports: [] }, [], ready));
});

test('private recorded error metadata must retain the actual typed Error fields', () => {
  const r = observation('controller.dispatch', 'default-auth-refusal', ['unit-direct', 'ordinary']);
  r.outcome = { kind: 'rejected', error: error('ControllerHttpError', 401, 'authentication_required') };
  const body = JSON.stringify({ route: '/work-orders/dispatch#workflow', input: { id: 'unit-direct', operationKey: 'ordinary' } });
  const t = transport(r, '/threads/unit-direct/runs/wait', body, 'controller', 401); t.request.forwarded_headers = [['content-type', 'application/json']];
  const captured = pair(t, '{"error":{"kind":"request_error","message":"authentication_required","details":{"code":"authentication_required"}}}', 401);
  for (const [field, invalid] of [['message', 42], ['stack', 42]]) {
    const changed = structuredClone(r); changed.outcome.error[field] = invalid;
    assert.throws(() => auditInvocationTraffic(changed, null, captured, ready), /Client traffic/);
  }
});

test('a declared signal cut cannot conceal an unexpected captured native frame', () => {
  const r = observation('worker.startRun', 'attach-held-start', ['unit-held', ready.routes.worker_held, 'finite attachment input', { kind: 'AbortSignal', aborted_at_invocation: false }]);
  r.outcome = { kind: 'stream', frames: [], eof: false, cut: 'signal', reader_polled: true,
    pending_settled: true, return_awaited: true, error: { name: 'AbortError' } };
  const body = JSON.stringify({ route: ready.routes.worker_held, input: { messages: [{ role: 'user', content: 'finite attachment input' }] } });
  const t = transport(r, '/threads/unit-held/runs/stream', body, 'worker'); t.request.has_signal = true; t.response.headers = [['content-type', 'text/event-stream']];
  assert.throws(() => auditInvocationTraffic(r, t, pair(t, 'event: unknown\ndata: {"private":"payload"}\n\n', 200, 'text/event-stream'), ready), /Client traffic/);
});

test('all 63 original-call shapes match pooled synthetic traffic without claiming a connection per call', () => {
  const f = makeClientTrafficUnitFixture();
  const out = auditClientTraffic(f.rawClient, f.captures, f.ready);
  assert.equal(out.requestCount, 63);
  assert.equal(out.transportCount, 62);
  assert.equal(out.groups.length, 8);
  assert.equal(out.groups.flatMap((g) => g.observations).length, 63);
  assert.deepEqual(out.groups[6].observations[11], {
    method: 'worker.cancel', outcome: 'returned', http_status: 404, client_status: 0,
    error_class: 'none', error_code: 'none', result_shape: 'cancel-result', frames: [], eof: false, cut: 'none',
  });
  assert.equal(out.groups[3].observations[0].cut, 'signal-abort');
  assert.equal(out.groups[6].observations[2].cut, 'viewer-return');
});

for (const [name, change] of [
  ['missing original invocation', (f) => f.rawClient.groups[0].observations.pop()],
  ['duplicated transport attribution', (f) => { f.rawClient.transports[1].ordinal = f.rawClient.transports[0].ordinal; }],
  ['injected direct-controller transport', (f) => { f.rawClient.transports[0].ordinal = 11; }],
  ['altered selected argument despite matching rewritten traffic', (f) => { f.rawClient.groups[0].observations[1].arguments[1] = 'changed'; }],
  ['unknown observation field', (f) => { f.rawClient.groups[0].observations[0].authority = 'private'; }],
  ['unknown method in a selected case', (f) => { f.rawClient.groups[0].observations[0].method = 'worker.createThread'; }],
  ['missing capture request', (f) => { f.captures[4].read = Buffer.alloc(0); }],
  ['extra captured request without a caller', (f) => {
    f.captures.push({ ...f.captures[4], ordinal: 7 });
  }],
  ['undeclared incomplete response', (f) => { f.captures[0].write = f.captures[0].write.subarray(0, f.captures[0].write.length - 1); }],
  ['a finite stream changed to a valid but wrong terminal shape', (f) => {
    f.rawClient.groups[5].observations[1].outcome.frames[0].data = {};
  }],
  ['missing generated thread identity linkage', (f) => { f.rawClient.groups[4].observations[1].arguments[0] = 'other-generated-id'; }],
  ['sparse invocation array', (f) => { delete f.rawClient.groups[0].observations[1]; }],
]) {
  test(`the aggregate refuses ${name}`, () => {
    const f = makeClientTrafficUnitFixture(); change(f);
    assert.throws(() => auditClientTraffic(f.rawClient, f.captures, f.ready));
  });
}

test('recorded response headers retain the full original response header list', () => {
  const r = observation('controller.dispatch', 'factory-dispatch', ['unit-id', 'ordinary'], { ok: true, message: 'fixture-value' });
  const body = JSON.stringify({ route: '/work-orders/dispatch#workflow', input: { id: 'unit-id', operationKey: 'ordinary' } });
  const t = transport(r, '/threads/unit-id/runs/wait', body);
  const captured = pair(t, JSON.stringify(r.outcome.value));
  t.response.headers = [];
  assert.throws(() => auditInvocationTraffic(r, t, captured, ready), /Client traffic/);
});

for (const [name, group, position] of [['controller', 1, 0], ['worker', 4, 4]]) {
  test(`the ${name} original HTTP error message must follow the captured error body`, () => {
    const f = makeClientTrafficUnitFixture();
    f.rawClient.groups[group].observations[position].outcome.error.message = 'contradictory private error';
    assert.throws(() => auditClientTraffic(f.rawClient, f.captures, f.ready), /Client traffic/);
  });
}

test('authentication refusal keeps the actual native request_error kind', () => {
  const r = observation('controller.dispatch', 'default-auth-refusal', ['unit-direct', 'ordinary']);
  r.outcome = { kind: 'rejected', error: { ...error('ControllerHttpError', 401, 'authentication_required'), message: 'authentication_required' } };
  const body = JSON.stringify({ route: '/work-orders/dispatch#workflow', input: { id: 'unit-direct', operationKey: 'ordinary' } });
  const t = transport(r, '/threads/unit-direct/runs/wait', body, 'controller', 401); t.request.forwarded_headers = [['content-type', 'application/json']];
  const captured = pair(t, '{"error":{"kind":"execution_error","message":"authentication_required","details":{"code":"authentication_required"}}}', 401);
  assert.throws(() => auditInvocationTraffic(r, null, captured, ready), /Client traffic/);
});

test('matching consumed and captured failure frames must still be the selected native failure', () => {
  const f = makeClientTrafficUnitFixture();
  f.rawClient.groups[5].observations[5].outcome.frames[0].data.output.error = 'fixture-wrong-failure';
  for (const capture of f.captures) {
    capture.write = Buffer.from(capture.write.toString().replace('"output":{"error":"fixture-known-failure"}', '"output":{"error":"fixture-wrong-failure"}'));
  }
  assert.throws(() => auditClientTraffic(f.rawClient, f.captures, f.ready), /Client traffic/);
});
