import test from 'node:test';
import assert from 'node:assert/strict';
import { auditNativeArtifacts } from './native-audit.mjs';

const LIMITS = { native: 24 * 1024 * 1024, client: 8 * 1024 * 1024 };
const FUNCTIONS = ['C1_factory_method_map', 'C2_controller_auth', 'C3_wait_terminals', 'C4_held_controller',
  'W1_worker_crud_auth', 'W2_finite_sse', 'W3_attachment_cancel', 'W4_durable_park_refusal'];
const encode = (value) => Buffer.from(`${JSON.stringify(value)}\n`);
const names = ['C1', 'C2', 'C3', 'C4', 'W1', 'W2', 'W3', 'W4'];
const flags = (generation, returned = false) => ({ generation, entered: true, returned,
  explicit_release: returned, cleanup_release: false, expired: false });
function view(id, status = 'running') {
  const approved = status === 'waiting_for_approval';
  const completed = status === 'completed';
  const spec = { id: 'fixture-task', effect_id: 'fixture-effect', recovery: 'pure', requires_approval: approved };
  const values = { input: { fixture: 'input' } };
  const snapshot = { schema_version: 1, revision: completed ? 3 : 2, execution_id: `execution-${id}`, turn_id: `turn-${id}`,
    turn_history: [`turn-${id}`], attempt_history: approved ? [] : [`attempt-${id}`], owner_generation: 1,
    definition: { route: approved ? '/fixture/worker-park#workflow' : id.startsWith('fixture-c4-') || id === 'fixture-concealed-controller'
      ? '/work-orders/dispatch#workflow' : '/fixture/worker-held#workflow', revision: 'fixture-v1',
      batches: [[spec]], max_attempts: 1 }, status, batch_index: completed ? 1 : 0, values,
    tasks: [{ spec, status: approved ? 'waiting_for_approval' : completed ? 'succeeded' : 'in_flight', input: values,
      attempt_id: approved ? null : `attempt-${id}`, result: completed ? { output: { fixture: 'value' }, writes: [] } : null,
      interrupt: approved ? { id: 'private-interrupt', prompt: { question: 'fixture-approval' } } : null, approved: false }],
    attempts_used: approved ? 0 : 1, updated_at_ms: 100, failure: null };
  if (completed) snapshot.output = values;
  return { thread: { id, generation: `generation-${id}`, active_execution_id: snapshot.execution_id, created_at_ms: 1,
    metadata: {}, origin: { application: 'ap-client-fixture', tenant: 'local', principal: 'owner' }, resource_revision: snapshot.revision + 1 },
  execution: { thread_id: id, snapshot } };
}
function approval(v, parked = false) {
  const s = v.execution.snapshot;
  return { thread: v.thread, execution: v.execution, grants: parked ? [{ thread_id: v.thread.id,
    thread_generation: v.thread.generation, origin: v.thread.origin, execution_id: s.execution_id,
    route: s.definition.route, definition_revision: 'fixture-v1', task_id: 'fixture-task', effect_id: 'fixture-effect',
    interrupt_id: 'private-interrupt', resume_key: 'private-resume', task_parking_revision: s.revision,
    policy_revision: 'fixture-policy-v1', proposal_digest: 'a'.repeat(64), prompt: { question: 'fixture-approval' },
    token: `b4ag_${'b'.repeat(64)}`, verifier: Array(32).fill(7), issued_at_ms: 100, expires_at_ms: 200, consumed: false }] : [], acceptances: [] };
}
function terminal(v, status) {
  const out = structuredClone(v); const s = out.execution.snapshot;
  const delta = status === 'completed' ? 1 : 2;
  s.revision += delta; out.thread.resource_revision += delta; s.updated_at_ms += 1; s.status = status;
  s.tasks[0].status = 'succeeded';
  s.tasks[0].result = { output: s.definition.route === '/work-orders/dispatch#workflow'
    ? { ok: true, message: 'fixture-value' } : { fixture: 'value' }, writes: [] };
  if (status === 'completed') { s.batch_index = 1; s.output = s.values; }
  return out;
}
function settleRole(role, files) {
  files.delete('quota-ledger.json');
  const before = [...files.values()].reduce((n, b) => n + b.length, 0);
  let used = before;
  for (;;) {
    const bytes = encode({ version: 1, limit_bytes: LIMITS[role], used_bytes: used });
    if (before + bytes.length === used) { files.set('quota-ledger.json', bytes); break; }
    used = before + bytes.length;
  }
  return { role, directory: `/private/synthetic/${role}`, files, usedBytes: used, limitBytes: LIMITS[role] };
}
// Entirely synthetic private artifacts, never original imports or native execution.
function fixture() {
  const native = new Map();
  const raw = { version: 1, kind: 'original-ap-client-observations',
    groups: names.map((name, i) => ({ name, function: FUNCTIONS[i], observations: [], witnesses: [] })), transports: [] };
  let observation = 0;
  const group = (name) => raw.groups.find((g) => g.name === name);
  const call = (name, method, caseName, args, value = null) => {
    const record = { ordinal: ++observation, method, case: caseName, arguments: args, outcome: { kind: 'returned', value } };
    group(name).observations.push(record); return record;
  };
  call('C4', 'controller.dispatch', 'held-dispatch-abandon', ['fixture-c4-abandon', 'ordinary']);
  call('C4', 'controller.dispatch', 'held-dispatch-cancel', ['fixture-c4-cancel', 'ordinary']);
  call('C4', 'controller.dispatch', 'concealed-establish', ['fixture-concealed-controller', 'ordinary']);
  call('W1', 'worker.createThread', 'worker-held-create', [{}], 'fixture-w1-held');
  call('W1', 'worker.startRun', 'worker-held-start', ['fixture-w1-held', '/fixture/worker-held#workflow', 'input']);
  call('W1', 'worker.deleteThread', 'worker-delete-released', ['fixture-w1-held'], 'deleted');
  call('W3', 'worker.createThread', 'attach-create', [{}], 'fixture-w3-abandon');
  call('W3', 'worker.startRun', 'attach-held-start', ['fixture-w3-abandon', '/fixture/worker-held#workflow', 'input']);
  call('W3', 'worker.createThread', 'attach-cancel-create', [{}], 'fixture-w3-cancel');
  call('W3', 'worker.startRun', 'attach-cancel-held-start', ['fixture-w3-cancel', '/fixture/worker-held#workflow', 'input']);
  call('W4', 'worker.createThread', 'park-create', [{}], 'fixture-w4-park');
  const observed = new Map(); const generations = new Map(); const inspections = [];
  const ready = { base_url: 'http://127.0.0.1:32123', token: 'c'.repeat(64), routes: {
    worker_value: '/fixture/worker-value#workflow', worker_void: '/fixture/worker-void#workflow',
    worker_failure: '/fixture/worker-failure#workflow', worker_held: '/fixture/worker-held#workflow', worker_park: '/fixture/worker-park#workflow' } };
  let commands = 0;
  function control(name, op, payload, result, witnessName) {
    const ordinal = ++commands; const suffix = String(ordinal).padStart(4, '0');
    native.set(`control-${suffix}-request.json`, encode({ version: 1, ordinal, case: name, command: { [op]: payload } }));
    native.set(`control-${suffix}-reply.json`, encode({ version: 1, ordinal, ok: true, result }));
    if (witnessName) group(name).witnesses.push({ name: witnessName, control_ordinal: ordinal, value: result });
    return ordinal;
  }
  control('setup', 'ready', {}, ready);
  function inspect(name, id, pending = false, status = 'running') {
    const v = view(id, status); const s = v.execution.snapshot;
    const result = { found: true, execution_status: s.status, task_statuses: s.tasks.map((t) => t.status), revision: s.revision, owner_generation: 1 };
    if (pending) result.interrupt_ids = ['private-interrupt'];
    const op = pending ? 'inspect_pending' : 'inspect_thread'; const ordinal = control(name, op, { thread_id: id }, result, op);
    const prefix = `snapshot-${String(ordinal).padStart(4, '0')}`;
    native.set(`${prefix}-thread.json`, encode(v));
    if (pending) native.set(`${prefix}-approval.json`, encode(approval(v, true)));
    observed.set(id, v); inspections.push(ordinal); return ordinal;
  }
  function held(name, gate, id, statuses = 1) {
    const generation = (generations.get(gate) ?? 0) + 1; generations.set(gate, generation);
    control(name, 'arm', { gate }, { generation }, `arm-${gate}`);
    control(name, 'await_entered', { gate, generation }, flags(generation), `entered-${gate}`);
    inspect(name, id);
    for (let i = 0; i < statuses; i++) control(name, 'gate_status', { gate, generation }, flags(generation), `held-${gate}`);
    control(name, 'release', { gate, generation }, flags(generation, true), `release-${gate}`);
  }
  group('C2').witnesses.push({ name: 'default-fetch-versus-bearer-only-injection', control_ordinal: null, value: true });
  held('C4', 'controller_dispatch', 'fixture-c4-abandon', 2);
  held('C4', 'controller_dispatch', 'fixture-c4-cancel');
  inspect('C4', 'fixture-concealed-controller', false, 'completed');
  held('W1', 'worker_held', 'fixture-w1-held', 0);
  held('W3', 'worker_held', 'fixture-w3-abandon', 2);
  held('W3', 'worker_held', 'fixture-w3-cancel');
  const parkOrdinals = [inspect('W4', 'fixture-w4-park', true, 'waiting_for_approval')];
  for (const payload of ['once', 'deny', null]) {
    const resolutions = payload === null ? [] : [{ interruptId: 'private-interrupt', payload }];
    const row = call('W4', 'worker.resume', payload === null ? 'resume-empty' : `resume-forced-${payload}`,
      ['fixture-w4-park', ready.routes.worker_park, resolutions]);
    row.outcome = { kind: 'rejected', error: { name: 'WorkerHttpError', status: 400, code: payload === null ? 'invalid_request' : 'grant_required', message: 'private error', stack: null } };
    raw.transports.push({ ordinal: row.ordinal, request: { url: `${ready.base_url}/threads/fixture-w4-park/resume`, method: 'POST',
      original_headers: [], forwarded_headers: [], body: JSON.stringify({ resume: resolutions.map((r) => ({ ...r, status: 'resolved' })), route: ready.routes.worker_park }),
      has_signal: false, redirect: 'error' }, response: { status: 400, headers: [] } });
    parkOrdinals.push(inspect('W4', 'fixture-w4-park', true, 'waiting_for_approval'));
  }
  group('W4').witnesses.push({ name: 'forced-id-only-no-grant-and-metadata-unchanged', control_ordinal: null, value: true });
  control('retirement', 'final_stop', {}, { finished: true });
  const ids = [...observed.keys()].sort();
  ids.forEach((id, i) => {
    const original = observed.get(id);
    const v = id === 'fixture-w1-held' ? null : id === 'fixture-w3-abandon' ? terminal(original, 'completed')
      : ['fixture-c4-abandon', 'fixture-c4-cancel', 'fixture-w3-cancel'].includes(id) ? terminal(original, 'cancelled') : original;
    const prefix = `reopened-${String(i + 1).padStart(4, '0')}`;
    native.set(`${prefix}-thread.json`, encode(v)); native.set(`${prefix}-approval.json`, encode(v === null ? null : approval(v, id === 'fixture-w4-park')));
  });
  const read = Buffer.from('GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n');
  const write = Buffer.from('HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}');
  native.set('conn-0001.read.bin', read); native.set('conn-0001.write.bin', write); native.set('stderr.bin', Buffer.alloc(0));
  native.set('capture-manifest.json', encode({ version: 1, accepted: 1, retired: 1, sticky_loss: false,
    connections: [{ ordinal: 1, read_file: 'conn-0001.read.bin', write_file: 'conn-0001.write.bin', read_bytes: read.length,
      write_bytes: write.length, read_eof: false, write_shutdown: true }] }));
  const client = settleRole('client', new Map([['client-observations.json', encode(raw)]]));
  const proof = { version: 1, child: { exit_code: 0, normal_reap: true },
    control: { commands, final_stop: true, pipe_eof: true, observed_threads: ids }, control_joined: true,
    stderr: { pipe_eof: true, joined: true, bytes: 0 }, server: { normal_join: true },
    shutdown: { outcome: 'quiescent', faulted: false, active_owners: 0 }, capture: { accepted: 1, retired: 1, sticky_loss: false },
    weak_store_absent: true, weak_application_absent: true,
    writer: { reopened: true, prior_owner_generation: 1, owner_generation: 2, ready: true },
    gates: { controller_dispatch: flags(2, true), worker_held: flags(3, true) },
    mode_checks: { private_root: true, store_at_open: true, store_at_reopen: true },
    evidence: { native_limit_bytes: LIMITS.native, client_limit_bytes: LIMITS.client, client_used_bytes: client.usedBytes } };
  native.set('native-proof.json', encode(proof));
  const nativeRole = settleRole('native', native);
  const refresh = () => {
    client.files.set('client-observations.json', encode(raw)); Object.assign(client, settleRole('client', client.files));
    proof.evidence.client_used_bytes = client.usedBytes; native.set('native-proof.json', encode(proof));
    Object.assign(nativeRole, settleRole('native', native));
  };
  const changeJson = (name, change) => { const value = JSON.parse(native.get(name)); change(value); native.set(name, encode(value)); };
  return { native: nativeRole, client, raw, ready, proof, parkOrdinals, inspections, refresh, changeJson };
}

test('audits full private history and actual persisted-state bytes into only the safe retirement projection', () => {
  const f = fixture(); const before = [...f.native.files].map(([n, b]) => [n, b.toString('hex')]);
  const result = auditNativeArtifacts(f.native, f.client, f.raw);
  assert.deepEqual(result.ready, f.ready);
  assert.deepEqual(result.retirement, { accepted: 1, retired: 1, capture_complete: true, child_exit_code: 0,
    control_eof: true, control_joined: true, stderr_joined: true, server_joined: true, quiescent: true, faulted: false,
    store_weak_absent: true, application_weak_absent: true, writer_reopened: true, owner_advanced: true,
    expired_gates: 0, cleanup_releases: 0 });
  assert.deepEqual([...f.native.files].map(([n, b]) => [n, b.toString('hex')]), before);
  assert.equal(JSON.stringify(result.retirement).includes('private-'), false);
});

test('rejects an expired earlier generation even when both latest native gate flags are positive', () => {
  const f = fixture(); f.changeJson('control-0003-reply.json', (v) => { v.result.expired = true; }); f.refresh();
  assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw));
});

const controlName = (f, operation, direction = 'reply') => {
  for (let n = 1; n <= f.proof.control.commands; n++) {
    const prefix = `control-${String(n).padStart(4, '0')}`;
    if (Object.hasOwn(JSON.parse(f.native.files.get(`${prefix}-request.json`)).command, operation)) return `${prefix}-${direction}.json`;
  }
  throw new Error('unit control operation missing');
};
const reopen = (f, id, kind) => `reopened-${String(f.proof.control.observed_threads.indexOf(id) + 1).padStart(4, '0')}-${kind}.json`;
const parkName = (f, index, kind) => `snapshot-${String(f.parkOrdinals[index]).padStart(4, '0')}-${kind}.json`;

for (const [name, change] of [
  ['normal child reap', (f) => { f.proof.child.normal_reap = false; }],
  ['child exit code', (f) => { f.proof.child.exit_code = 1; }],
  ['control pipe EOF', (f) => { f.proof.control.pipe_eof = false; }],
  ['control thread join', (f) => { f.proof.control_joined = false; }],
  ['stderr EOF and join', (f) => { f.proof.stderr.joined = false; }],
  ['normal server join', (f) => { f.proof.server.normal_join = false; }],
  ['Quiescent active owners', (f) => { f.proof.shutdown.active_owners = 1; }],
  ['Quiescent fault', (f) => { f.proof.shutdown.faulted = true; }],
  ['Store Weak absence', (f) => { f.proof.weak_store_absent = false; }],
  ['application Weak absence', (f) => { f.proof.weak_application_absent = false; }],
  ['actual writer reopen', (f) => { f.proof.writer.reopened = false; }],
  ['exact next writer owner', (f) => { f.proof.writer.owner_generation = 3; }],
  ['writer readiness', (f) => { f.proof.writer.ready = false; }],
  ['store mode witness', (f) => { f.proof.mode_checks.store_at_reopen = false; }],
  ['native evidence fixed fields', (f) => { f.proof.authority = 'private raw field'; }],
  ['latest proof actual generation', (f) => { f.proof.gates.controller_dispatch.generation = 1; }],
]) {
  test(`refuses missing or contradictory ${name} evidence`, () => {
    const f = fixture(); change(f); f.refresh(); assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw));
  });
}

for (const [name, change] of [
  ['unknown command fields', (f) => f.changeJson(controlName(f, 'arm', 'request'), (v) => { v.command.arm.extra = true; })],
  ['wrong version', (f) => f.changeJson('control-0001-request.json', (v) => { v.version = 2; })],
  ['wrong reply ordinal', (f) => f.changeJson('control-0002-reply.json', (v) => { v.ordinal = 3; })],
  ['backward case phase', (f) => f.changeJson(controlName(f, 'release', 'request'), (v) => { v.case = 'C1'; })],
  ['missing transcript half', (f) => { f.native.files.delete('control-0003-request.json'); }],
  ['cleanup release in earlier generation', (f) => f.changeJson(controlName(f, 'release'), (v) => { v.result.cleanup_release = true; })],
  ['stale gate generation', (f) => f.changeJson(controlName(f, 'await_entered', 'request'), (v) => { v.command.await_entered.generation = 2; })],
  ['missing first entry flag', (f) => f.changeJson(controlName(f, 'await_entered'), (v) => { v.result.entered = false; })],
  ['return before explicit release', (f) => f.changeJson(controlName(f, 'gate_status'), (v) => { v.result.returned = true; })],
  ['unreleased final generation', (f) => f.changeJson(`control-${String(f.parkOrdinals[0] - 1).padStart(4, '0')}-reply.json`, (v) => { v.result.explicit_release = false; })],
  ['client witness metadata mismatch', (f) => { f.raw.groups[3].witnesses.find((w) => w.name === 'inspect_thread').value = { found: false }; }],
  ['client witness duplicate', (f) => { f.raw.groups[3].witnesses.push(structuredClone(f.raw.groups[3].witnesses[0])); }],
  ['client witness missing', (f) => { f.raw.groups[3].witnesses.splice(0, 1); }],
  ['unrecognized null annotation', (f) => { f.raw.groups[1].witnesses[0].name = 'unverified-authority'; }],
]) {
  test(`refuses complete-history defect: ${name}`, () => {
    const f = fixture(); change(f); f.refresh(); assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw));
  });
}

test('refuses duplicate decoded parent flags before they can conceal earlier expiry', () => {
  const f = fixture(); const file = controlName(f, 'await_entered');
  f.native.files.set(file, Buffer.from(f.native.files.get(file).toString().replace('"expired":false', '"expired":true,"\\u0065xpired":false')));
  f.refresh(); assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /duplicate/);
});

test('refuses a control line without actual LF, a second LF or byte8193', () => {
  for (const body of [Buffer.from('{"version":1}'), Buffer.from('{}\n{}\n'), Buffer.alloc(8193, 10)]) {
    const f = fixture(); f.native.files.set('control-0001-request.json', body); f.refresh();
    assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /control line/);
  }
});

test('refuses hidden grant token and verifier changes while all control metadata remains identical', () => {
  for (const mutate of [(g) => { g.token = `b4ag_${'d'.repeat(64)}`; }, (g) => { g.verifier[0] = 8; }]) {
    const f = fixture(); f.changeJson(parkName(f, 1, 'approval'), (v) => mutate(v.grants[0])); f.refresh();
    assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /park authority/);
  }
});

test('refuses parked thread resource mutation invisible to the reply metadata', () => {
  const f = fixture(); f.changeJson(parkName(f, 2, 'thread'), (v) => { v.thread.resource_revision++; });
  f.changeJson(parkName(f, 2, 'approval'), (v) => { v.thread.resource_revision++; }); f.refresh();
  assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /park authority/);
});

test('requires parked authority byte identity as well as semantic equality', () => {
  const f = fixture(); const name = parkName(f, 3, 'approval');
  f.native.files.set(name, Buffer.from(`${JSON.stringify(JSON.parse(f.native.files.get(name)), null, 2)}\n`)); f.refresh();
  assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /park bytes/);
});

test('refuses consumed grants or fabricated acceptance audits even if later park snapshots agree', () => {
  for (const mutate of [(a) => { a.grants[0].consumed = true; }, (a) => { a.acceptances.push({ decisions: [] }); }]) {
    const f = fixture();
    for (let i = 0; i < 4; i++) f.changeJson(parkName(f, i, 'approval'), mutate);
    f.changeJson(reopen(f, 'fixture-w4-park', 'approval'), mutate); f.refresh();
    assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw));
  }
});

test('refuses forced IDs that differ from the actual unconsumed grant and any forwarded grant field', () => {
  for (const mutate of [(r) => { r.arguments[2][0].interruptId = 'fabricated-interrupt'; }, (r) => { r.arguments[2][0].grant = 'private-grant'; }]) {
    const f = fixture(); const row = f.raw.groups[7].observations.find((r) => r.case === 'resume-forced-once');
    mutate(row); f.refresh(); assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /grantless/);
  }
  const f = fixture(); const t = f.raw.transports[0]; const body = JSON.parse(t.request.body); body.grant = 'private-grant'; t.request.body = JSON.stringify(body);
  f.refresh(); assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /grantless/);
});

test('refuses a stale or permuted observed-thread to reopened-file correspondence', () => {
  const f = fixture(); const a = reopen(f, 'fixture-c4-abandon', 'thread'); const b = reopen(f, 'fixture-c4-cancel', 'thread');
  const one = f.native.files.get(a); f.native.files.set(a, f.native.files.get(b)); f.native.files.set(b, one); f.refresh();
  assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /thread identity/);
});

test('requires reopened full parked authority unchanged without inventing execution-owner adoption', () => {
  const f = fixture();
  for (const kind of ['thread', 'approval']) f.changeJson(reopen(f, 'fixture-w4-park', kind), (v) => { v.execution.snapshot.owner_generation = 2; });
  f.refresh(); assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /execution metadata/);
});

test('refuses reopened terminal identity mutation and missing deletion', () => {
  const f = fixture(); f.changeJson(reopen(f, 'fixture-w3-abandon', 'thread'), (v) => { v.execution.snapshot.turn_id = 'other-turn'; });
  f.refresh(); assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw));
  const d = fixture(); const v = terminal(view('fixture-w1-held'), 'completed');
  d.native.files.set(reopen(d, 'fixture-w1-held', 'thread'), encode(v)); d.native.files.set(reopen(d, 'fixture-w1-held', 'approval'), encode(approval(v)));
  d.refresh(); assert.throws(() => auditNativeArtifacts(d.native, d.client, d.raw), /deleted/);
});

test('refuses malformed full Store map fields even when all park/reopened bytes agree', () => {
  const f = fixture();
  const mutate = (v) => { v.execution.snapshot.values = []; v.execution.snapshot.tasks[0].input = []; };
  for (let i = 0; i < 4; i++) for (const kind of ['thread', 'approval']) f.changeJson(parkName(f, i, kind), mutate);
  for (const kind of ['thread', 'approval']) f.changeJson(reopen(f, 'fixture-w4-park', kind), mutate);
  f.refresh(); assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw));
});

test('requires exact file cardinality and preserves disjoint role limits and actual self-inclusive totals', () => {
  for (const mutate of [
    (f) => { f.native.files.set('reopened-0008-thread.json', encode(null)); },
    (f) => { f.native.files.set('conn-0002.read.bin', Buffer.alloc(0)); },
    (f) => { f.native.files.set('snapshot-0001-approval.json', encode(null)); },
  ]) {
    const f = fixture(); mutate(f); f.refresh(); assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /unaccounted/);
  }
  const f = fixture(); f.native.usedBytes--; assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /accounting/);
  const n = fixture(); n.native.limitBytes += n.client.limitBytes; assert.throws(() => auditNativeArtifacts(n.native, n.client, n.raw), /role/);
  const c = fixture(); c.proof.evidence.client_used_bytes--; c.native.files.set('native-proof.json', encode(c.proof)); Object.assign(c.native, settleRole('native', c.native.files));
  assert.throws(() => auditNativeArtifacts(c.native, c.client, c.raw), /evidence proof/);
});

test('checks complete capture counts against actual bytes independently of EOF/shutdown flags', () => {
  for (const mutate of [(m) => { m.retired = 0; }, (m) => { m.connections[0].read_bytes++; }, (m) => { m.connections[0].ordinal = 2; },
    (m) => { m.accepted = 17; }, (m) => { m.sticky_loss = true; }]) {
    const f = fixture(); f.changeJson('capture-manifest.json', mutate); f.refresh();
    assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw));
  }
  const f = fixture(); f.native.files.set('conn-0001.read.bin', Buffer.alloc(262144));
  f.changeJson('capture-manifest.json', (m) => { m.connections[0].read_bytes = 262144; m.connections[0].read_eof = false; m.connections[0].write_shutdown = false; });
  f.refresh(); assert.equal(auditNativeArtifacts(f.native, f.client, f.raw).retirement.capture_complete, true);
  f.native.files.set('conn-0001.read.bin', Buffer.alloc(262145)); f.changeJson('capture-manifest.json', (m) => { m.connections[0].read_bytes = 262145; });
  f.refresh(); assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /integer bound/);
});

test('checks stderr actual byte cardinality/cap and raw client argument-to-file binding', () => {
  const f = fixture(); f.native.files.set('stderr.bin', Buffer.from('private-stderr')); f.refresh();
  assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw), /stderr bytes/);
  const large = fixture(); large.native.files.set('stderr.bin', Buffer.alloc(1048577)); large.proof.stderr.bytes = 1048577; large.refresh();
  assert.throws(() => auditNativeArtifacts(large.native, large.client, large.raw), /integer bound/);
  const c = fixture(); c.raw.groups[0].function = 'different';
  assert.throws(() => auditNativeArtifacts(c.native, c.client, c.raw), /client file binding/);
});

test('reopened cancelled executions retain the fixed succeeded callback result after explicit held return', () => {
  // Actual run-A showed TaskFinished succeeding while the execution was already
  // CancelRequested. The unchanged kernel settles the execution as Cancelled,
  // preserving the callback result. Static portable values below follow the
  // reviewed fixture; no actual private identifier or authority is copied.
  const f = fixture();
  const drained = ['fixture-c4-abandon', 'fixture-c4-cancel', 'fixture-w3-cancel'];
  const changeTerminal = (id, change) => {
    f.changeJson(reopen(f, id, 'thread'), change);
    const v = JSON.parse(f.native.files.get(reopen(f, id, 'thread')));
    f.changeJson(reopen(f, id, 'approval'), (a) => { a.thread = v.thread; a.execution = v.execution; });
  };
  for (const id of drained) changeTerminal(id, (v) => {
    assert.equal(v.execution.snapshot.status, 'cancelled');
    v.execution.snapshot.tasks[0].status = 'succeeded';
    v.execution.snapshot.tasks[0].result = { output: id.startsWith('fixture-c4-')
      ? { ok: true, message: 'fixture-value' } : { fixture: 'value' }, writes: [] };
  });
  f.refresh();
  assert.equal(auditNativeArtifacts(f.native, f.client, f.raw).retirement.writer_reopened, true);
  for (const id of drained) {
    const name = reopen(f, id, 'thread'); const retained = f.native.files.get(name);
    const approvalName = reopen(f, id, 'approval'); const retainedApproval = f.native.files.get(approvalName);
    for (const corrupt of [
      (v) => { v.execution.snapshot.tasks[0].status = 'cancelled'; v.execution.snapshot.tasks[0].result = null; },
      (v) => { v.execution.snapshot.tasks[0].result.output = { private: 'wrong callback result' }; },
      (v) => { v.execution.snapshot.tasks[0].result.writes.push({ kind: 'replace', field: 'private', value: null }); },
      (v) => { v.execution.snapshot.batch_index = 1; },
      (v) => { v.execution.snapshot.output = v.execution.snapshot.values; },
      (v) => { v.thread.resource_revision++; },
    ]) {
      changeTerminal(id, corrupt); f.refresh();
      assert.throws(() => auditNativeArtifacts(f.native, f.client, f.raw));
      f.native.files.set(name, retained); f.native.files.set(approvalName, retainedApproval); f.refresh();
    }
  }
});
