import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable, PassThrough, Writable } from 'node:stream';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { encodeCommand, controlLines, ControlClient } from './control.mjs';
import { PrivateWriter } from './private-output.mjs';
import { consumeToEof, abortPolledIterator } from './runner-support.mjs';
import { CorpusContext } from './runner-context.mjs';
import * as controllerGroups from './controller-cases.mjs';
import * as workerGroups from './worker-cases.mjs';
import { unitContext } from './runner-unit-fixture.mjs';
import { runGroups, verifyBundleManifest } from './runner.mjs';
import { SELECTED_METHODS } from './runner-cases.mjs';
import { createHash } from 'node:crypto';

test('control encodes the actual bounded version ordinal and tagged command', () => {
  const bytes = encodeCommand(1, 'setup', { ready: {} });
  assert.deepEqual(bytes, Buffer.from('{"version":1,"ordinal":1,"case":"setup","command":{"ready":{}}}\n'));
});

test('incremental control reader preserves a line across real fragmented chunks', async () => {
  const lines = [];
  for await (const line of controlLines(Readable.from([Buffer.from('{"version":'), Buffer.from('1}\n')])) ) lines.push(line);
  assert.deepEqual(lines, ['{"version":1}']);
});

test('private evidence writer persists exact reserved bytes with mode600', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'b4-client-writer-'));
  try {
    const writer = new PrivateWriter(directory, { limitBytes: 128 });
    const bytes = Buffer.alloc(128, 65);
    await writer.writeBytes('client-observations.json', bytes);
    const file = join(directory, 'client-observations.json');
    assert.equal(existsSync(file), true, 'authorized private bytes must actually be persisted');
    assert.deepEqual(await readFile(file), bytes);
    assert.equal((await stat(file)).mode & 0o777, 0o600);
    assert.deepEqual(await writer.verify(), { reserved_bytes: 128, persisted_bytes: 128 });
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('stream EOF helper consumes actual iterable values and records real completion', async () => {
  async function* stream() { yield { event: 'done', data: {} }; }
  assert.deepEqual(await consumeToEof(stream()), {
    frames: [{ event: 'done', data: {} }], eof: true, cut: null,
    reader_polled: true, pending_settled: true, return_awaited: false,
  });
});

test('blocked read is settled by its signal before iterator return is awaited', async () => {
  const events = [];
  const controller = new AbortController();
  const pending = new Promise((resolve, reject) => {
    controller.signal.addEventListener('abort', () => { events.push('abort'); reject(controller.signal.reason); }, { once: true });
  });
  // Unit ownership witness, not an import/execution of the original pinned parser.
  const iterator = { async return() { events.push('return'); return { done: true }; } };
  const actual = await abortPolledIterator(iterator, pending, controller);
  assert.equal(actual.pending_settled, true);
  assert.equal(actual.return_awaited, true);
  assert.deepEqual(events, ['abort', 'return']);
});

test('control rejects unknown commands instead of serializing arbitrary operations', () => {
  assert.throws(() => encodeCommand(1, 'setup', { arbitrary_sql: { sql: 'SELECT 1' } }), { code: 'invalid_command' });
});

test('control refuses encoded bytes beyond its newline inclusive cap', () => {
  assert.throws(() => encodeCommand(1, 'W1', { inspect_thread: { thread_id: 'a'.repeat(8192) } }), { code: 'line_limit' });
});

test('incremental control reader accepts exactly8192 bytes and refuses byte8193', async () => {
  const exact = `${'a'.repeat(8191)}\n`;
  const actual = [];
  for await (const line of controlLines(Readable.from([Buffer.from(exact)]))) actual.push(line);
  assert.equal(actual.length, 1);
  assert.equal(actual[0].length, 8191);
  await assert.rejects(async () => {
    for await (const line of controlLines(Readable.from([Buffer.from(`${'a'.repeat(8192)}\n`)]))) void line;
  }, { code: 'line_limit' });
});

test('incremental control reader rejects incomplete final lines and malformed UTF8', async () => {
  for (const [bytes, code] of [[Buffer.from('partial'), 'partial_line'], [Buffer.from([0xff, 10]), 'invalid_utf8']]) {
    await assert.rejects(async () => { for await (const line of controlLines(Readable.from([bytes]))) void line; }, { code });
  }
});

test('control request matches actual ordinal reply before returning metadata', async () => {
  const input = new PassThrough();
  const writes = [];
  const output = new Writable({ write(bytes, encoding, callback) { writes.push(Buffer.from(bytes)); callback(); } });
  const client = new ControlClient(input, output);
  input.end('{"version":1,"ordinal":1,"ok":true,"result":{"generation":1}}\n');
  assert.deepEqual(await client.request('C4', { arm: { gate: 'controller_dispatch' } }), { generation: 1 });
  assert.equal(JSON.parse(writes[0]).ordinal, 1);
});

test('control request refuses mismatched ordinal and unknown reply fields', async () => {
  for (const reply of [{ version: 1, ordinal: 2, ok: true, result: { generation: 1 } },
    { version: 1, ordinal: 1, ok: true, result: { generation: 1 }, extra: true }]) {
    const input = Readable.from([Buffer.from(`${JSON.stringify(reply)}\n`)]);
    const output = new Writable({ write(bytes, encoding, callback) { callback(); } });
    const client = new ControlClient(input, output);
    await assert.rejects(client.request('C4', { arm: { gate: 'controller_dispatch' } }), { code: 'invalid_reply' });
  }
});

test('private writer refuses its next byte before creating a file even with other role space', async () => {
  const exhausted = await mkdtemp(join(tmpdir(), 'b4-client-exhausted-'));
  const other = await mkdtemp(join(tmpdir(), 'b4-client-other-'));
  try {
    const writer = new PrivateWriter(exhausted, { limitBytes: 1 });
    const otherWriter = new PrivateWriter(other, { limitBytes: 128 });
    await writer.writeBytes('first.bin', Buffer.from('a'));
    await assert.rejects(writer.writeBytes('second.bin', Buffer.from('b')), { code: 'evidence_limit' });
    assert.equal(existsSync(join(exhausted, 'second.bin')), false);
    await otherWriter.writeBytes('unused-role.bin', Buffer.from('other role')); // Downward helper unit, no native proof.
    assert.deepEqual(await readFile(join(other, 'unused-role.bin')), Buffer.from('other role'));
  } finally { await rm(exhausted, { recursive: true, force: true }); await rm(other, { recursive: true, force: true }); }
});

test('private writer never overwrites an existing artifact and retains failed reservation', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'b4-client-collision-'));
  try {
    const writer = new PrivateWriter(directory, { limitBytes: 128 });
    await writer.writeBytes('same.bin', Buffer.from('first'));
    await assert.rejects(writer.writeBytes('same.bin', Buffer.from('second')), { code: 'write_failed' });
    assert.equal(writer.reservedBytes, 11);
    assert.deepEqual(await readFile(join(directory, 'same.bin')), Buffer.from('first'));
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('stream frame65 is refused and actual iterator cleanup is awaited', async () => {
  let closed = false;
  async function* stream() { try { for (let i = 0; i < 65; i++) yield { event: 'done', data: i }; } finally { closed = true; } }
  await assert.rejects(consumeToEof(stream()), { code: 'frame_limit' });
  assert.equal(closed, true);
});

test('final quota ledger includes its own exact bytes and closes the writer', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'b4-client-ledger-'));
  try {
    const writer = new PrivateWriter(directory, { limitBytes: 256 });
    await writer.writeJson('client-observations.json', { version: 1 });
    const result = await writer.finalize();
    const file = join(directory, 'quota-ledger.json');
    assert.equal(existsSync(file), true, 'positive ledger must actually exist');
    const ledger = JSON.parse(await readFile(file, 'utf8'));
    assert.equal(ledger.used_bytes, result.persisted_bytes);
    assert.equal(ledger.limit_bytes, 256);
    assert.equal(ledger.used_bytes, (await stat(file)).size + (await stat(join(directory, 'client-observations.json'))).size);
    await assert.rejects(writer.writeBytes('after.bin', Buffer.from('a')), { code: 'invalid_output' });
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('observation context invokes supplied operation and records real result privately', async () => {
  const context = new CorpusContext({ ready: {}, control: {} });
  context.beginGroup('C1', 'C1_factory_method_map');
  let invoked = false;
  const result = await context.call('controller.create', 'factory-create', [{ taskId: 'unit' }], async () => { invoked = true; return { ok: true }; });
  assert.equal(invoked, true);
  assert.deepEqual(result.value, { ok: true });
  assert.equal(context.groups[0].observations[0].ordinal, 1);
  assert.deepEqual(context.groups[0].observations[0].outcome, { kind: 'returned', value: { ok: true } });
});

test('controller header seam preserves original Response body and signal objects', async () => {
  const response = new Response('original body');
  const signal = new AbortController().signal;
  let forwarded;
  const context = new CorpusContext({ ready: { base_url: 'http://127.0.0.1:12345', token: 'private-unit-token' }, control: {},
    fetchImpl: async (url, init) => { forwarded = { url, init }; return response; } });
  context.beginGroup('C1', 'C1_factory_method_map');
  const originalBody = response.body;
  const result = await context.call('controller.dispatch', 'factory-dispatch', [], () =>
    context.fetch('controller')('http://127.0.0.1:12345/threads/unit/runs/wait', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"route":"unit"}', signal,
    }));
  assert.equal(result.value, response);
  assert.equal(response.body, originalBody);
  assert.equal(response.bodyUsed, false);
  assert.equal(forwarded.init.signal, signal);
  assert.equal(forwarded.init.body, '{"route":"unit"}');
  assert.equal(new Headers(forwarded.init.headers).get('authorization'), 'Bearer private-unit-token');
  assert.equal(forwarded.init.redirect, 'error');
  assert.equal(context.transports[0].ordinal, 1);
  assert.equal(new Headers(context.transports[0].request.original_headers).has('authorization'), false);
});

test('fetch observation attribution follows async invocation context across concurrency', async () => {
  const context = new CorpusContext({ ready: { base_url: 'http://127.0.0.1:12345', token: 'private-unit-token' }, control: {},
    fetchImpl: async () => new Response(null) });
  context.beginGroup('C3', 'C3_wait_terminals');
  let release;
  const held = new Promise((resolve) => { release = resolve; });
  const first = context.call('controller.dispatch', 'wait-value', [], async () => {
    await held;
    return context.fetch('controller')('http://127.0.0.1:12345/first', { method: 'POST' });
  });
  const second = context.call('controller.dispatch', 'wait-failure', [], () => context.fetch('controller')('http://127.0.0.1:12345/second', { method: 'POST' }));
  await second; release(); await first;
  assert.deepEqual(context.transports.map((row) => row.ordinal), [2, 1]);
});

test('fetch header seam refuses another origin before any actual fetch', async () => {
  let calls = 0;
  const context = new CorpusContext({ ready: { base_url: 'http://127.0.0.1:12345', token: 'private-unit-token' }, control: {},
    fetchImpl: async () => { calls++; return new Response(null); } });
  context.beginGroup('C1', 'C1_factory_method_map');
  await assert.rejects(context.call('controller.dispatch', 'factory-dispatch', [], () =>
    context.fetch('controller')('https://example.invalid/', {})), { code: 'invalid_origin' });
  assert.equal(calls, 0);
});

for (const [name, group] of Object.entries({ ...controllerGroups, ...workerGroups })) {
  test(`unit orchestration ${name} actually invokes supplied factory methods`, async () => {
    const context = unitContext();
    context.beginGroup(name.slice(0, 2), name);
    try {
      await group(context);
      assert.ok(context.actualMethodCalls.length > 0, 'compiling empty group cannot qualify an actual invocation');
      assert.ok(context.groups[0].observations.length > 0);
      assert.equal(context.pending.size, 0);
      assert.equal(context.gates.size, 0);
    } finally { context.cleanup(); }
  });
}

test('assembled unit driver executes eight groups and all19 distinct supplied methods', async () => {
  const context = unitContext();
  try {
    const observations = await runGroups(context);
    assert.equal(observations.groups.length, 8);
    assert.deepEqual(observations.groups.map((group) => group.name), ['C1', 'C2', 'C3', 'C4', 'W1', 'W2', 'W3', 'W4']);
    assert.deepEqual([...new Set(context.actualMethodCalls.map((row) => row.method))].sort(), [...SELECTED_METHODS].sort());
    assert.equal(observations.kind, 'original-ap-client-observations');
  } finally { context.cleanup(); }
});

test('bundle admission verifies actual bytes and fixed names without importing code', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'b4-client-bundle-unit-'));
  const bytes = Buffer.from('export const syntheticUnit = true;\n');
  const outputs = ['controller', 'worker'].map((entry) => ({ entry, file: `${entry}-client.mjs`, bytes: bytes.length,
    sha256: createHash('sha256').update(bytes).digest('hex') }));
  const manifest = { version: 1, provenance: 'original_pinned_client',
    source: { commit: '8acac534ba6eb960b34de217b0da45b8405023af' }, outputs };
  try {
    // Synthetic files test file admission only; never treated as original provenance or imported.
    for (const output of outputs) await writeFile(join(directory, output.file), bytes, { flag: 'wx', mode: 0o600 });
    await writeFile(join(directory, 'bundle-manifest.json'), `${JSON.stringify(manifest)}\n`, { flag: 'wx', mode: 0o600 });
    const accepted = await verifyBundleManifest(directory);
    assert.ok(accepted, 'compiling admission stub cannot substitute for byte verification');
    assert.equal(accepted.manifest.version, 1);
    await writeFile(join(directory, 'worker-client.mjs'), 'changed');
    await assert.rejects(verifyBundleManifest(directory), { code: 'invalid_bundle' });
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('private control witness binds the actual control ordinal for exact artifact correlation', async () => {
  const state = { found: true, execution_status: 'running', task_statuses: ['in_flight'], revision: 1, owner_generation: 1 };
  const context = new CorpusContext({ ready: {}, control: { ordinal: 7, async request() { return state; } } });
  context.beginGroup('C4', 'C4_held_controller');
  await context.inspect('unit-private-thread');
  assert.deepEqual(context.groups[0].witnesses[0], { name: 'inspect_thread', control_ordinal: 7, value: state });
});

test('control refuses duplicate decoded fields that could conceal gate expiry', async () => {
  for (const key of ['expired', '\\u0065xpired']) {
    const input = Readable.from([Buffer.from(`{"version":1,"ordinal":1,"ok":true,"result":{"generation":1,"entered":true,"returned":false,"explicit_release":false,"cleanup_release":false,"expired":true,"${key}":false}}\n`)]);
    const output = new Writable({ write(bytes, encoding, callback) { callback(); } });
    const client = new ControlClient(input, output);
    await assert.rejects(client.request('C4', { gate_status: { gate: 'controller_dispatch', generation: 1 } }), { code: 'invalid_reply' });
  }
});

test('control finite downward deadline refuses a real delayed reply and closes its input', async () => {
  const input = new PassThrough();
  const output = new Writable({ write(bytes, encoding, callback) { callback(); } });
  const client = new ControlClient(input, output, { replyTimeoutMs: 5 });
  const timer = setTimeout(() => input.end('{"version":1,"ordinal":1,"ok":true,"result":{"generation":1}}\n'), 30);
  try {
    await assert.rejects(client.request('C4', { arm: { gate: 'controller_dispatch' } }), { code: 'control_timeout' });
    assert.equal(input.destroyed, true);
  } finally { clearTimeout(timer); await client.dispose(); }
});

test('a wrong actual first attachment frame still awaits iterator cleanup', async () => {
  let closed = false;
  async function* stream() { try { yield { event: 'done', data: {} }; } finally { closed = true; } }
  const context = new CorpusContext({ ready: {}, control: {} });
  context.beginGroup('W3', 'W3_attachment_cancel');
  const result = await context.call('worker.reattach', 'attach-live', [], async () => stream());
  await assert.rejects(context.cutFirst(result), { code: 'missing_state_frame' });
  assert.equal(closed, true, 'negative frame checks must retain actual reader cleanup');
  context.cleanup();
});

for (const [name, group, client, method, errorName] of [
  ['controller', controllerGroups.C2_controller_auth, 'directController', 'dispatch', 'ControllerHttpError'],
  ['worker', workerGroups.W1_worker_crud_auth, 'wrongWorker', 'getThread', 'WorkerHttpError'],
]) {
  test(`${name} auth orchestration retains the native authentication_required code`, async () => {
    const context = unitContext();
    context[client][method] = async () => { throw Object.assign(new Error('authentication_required'), {
      name: errorName, status: 401, code: 'authentication_required',
    }); };
    context.beginGroup(group.name.slice(0, 2), group.name);
    try { await group(context); } finally { context.cleanup(); }
  });
}

test('full W3 orchestration preserves the original404 missing-thread cancellation reduction', async () => {
  const context = unitContext();
  const unitCancel = context.worker.cancel;
  const cancellations = [];
  // Transport returns404 before cancellation::cancel for an absent thread;
  // the pinned factory reduces that status to thread_not_found. This supplied
  // method double exercises orchestration, without importing or mocking HTTP.
  context.worker.cancel = async (id) => {
    cancellations.push(id);
    return id === 'fixture-missing-worker' ? 'thread_not_found' : unitCancel(id);
  };
  context.beginGroup('W3', 'W3_attachment_cancel');
  try {
    await workerGroups.W3_attachment_cancel(context);
    assert.ok(cancellations.includes('fixture-missing-worker'));
    const missing = context.groups[0].observations.find((row) => row.case === 'worker-cancel-missing');
    assert.deepEqual(missing.outcome, { kind: 'returned', value: 'thread_not_found' });
    assert.equal(context.pending.size, 0);
    assert.equal(context.gates.size, 0);
  } finally { context.cleanup(); }
});
