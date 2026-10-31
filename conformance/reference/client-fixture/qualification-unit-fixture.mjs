// SYNTHETIC UNIT FILES ONLY. The native helpers below are copied from the frozen
// native-audit.test.mjs setup, then joined to the independently authored static
// traffic fixture. No original caller, native host or socket executes here.
import { mkdtemp, mkdir, chmod, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { makeClientTrafficUnitFixture } from './client-audit-unit-fixture.mjs';


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
function nativeFixture() {
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


const ID_MAP = [
  ['fixture-w1-held', 'synthetic-worker-held'],
  ['fixture-w3-abandon', 'synthetic-worker-attachment'],
  ['fixture-w3-cancel', 'synthetic-worker-cancelled'],
  ['fixture-w4-park', 'synthetic-worker-park'],
  ['private-interrupt', 'synthetic-private-interrupt'],
  ['private-resume', 'd'.repeat(64)],
  [`b4ag_${'b'.repeat(64)}`, `b4ag_${'e'.repeat(64)}`],
  ['execution-synthetic-worker-attachment', 'synthetic-attachment-execution'],
];
function remap(value) {
  if (typeof value === 'string') {
    for (const [before, after] of ID_MAP) value = value.replaceAll(before, after);
    return value;
  }
  if (Array.isArray(value)) return value.map(remap);
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, v]) => [key, remap(v)]));
  return value;
}
function alignView(value) {
  if (!value || !value.thread || !value.execution) return value;
  const id = value.thread.id;
  const contents = {
    'synthetic-worker-held': ['worker-held-create', 'finite held input'],
    'synthetic-worker-attachment': ['attach-create', 'finite attachment input'],
    'synthetic-worker-cancelled': ['attach-cancel-create', 'finite cancelled input'],
    'synthetic-worker-park': ['park-create', 'finite approval input'],
  };
  const input = contents[id]
    ? { messages: [{ role: 'user', content: contents[id][1] }] }
    : { id, operationKey: 'ordinary' };
  const s = value.execution.snapshot;
  s.values = { input }; s.tasks[0].input = { input };
  if (Object.hasOwn(s, 'output')) s.output = s.values;
  if (id === 'fixture-concealed-controller' && s.tasks[0].result) {
    s.tasks[0].result.output = { ok: true, message: 'fixture-value' };
  }
  if (contents[id]) value.thread.metadata = { fixture_case: contents[id][0] };
  return value;
}

export function makeQualificationUnitFixture() {
  const n = nativeFixture(); const traffic = makeClientTrafficUnitFixture();
  const proof = remap(n.proof);
  const oldIds = n.proof.control.observed_threads;
  proof.control.observed_threads = oldIds.map(remap).sort();
  const files = new Map();
  for (const [name, body] of n.native.files) {
    if (name === 'quota-ledger.json' || name === 'native-proof.json' || name === 'capture-manifest.json' || name.startsWith('conn-')) continue;
    let filename = name;
    if (name.startsWith('reopened-')) {
      const index = Number(name.slice(9, 13)) - 1;
      const nextIndex = proof.control.observed_threads.indexOf(remap(oldIds[index])) + 1;
      filename = `reopened-${String(nextIndex).padStart(4, '0')}${name.slice(13)}`;
    }
    files.set(filename, name.endsWith('.json') ? encode(alignView(remap(JSON.parse(body)))) : body);
  }
  for (const group of traffic.rawClient.groups) {
    const native = n.raw.groups.find((g) => g.name === group.name);
    group.witnesses.push(...native.witnesses.filter((w) => w.control_ordinal !== null).map(remap));
  }
  const connections = traffic.captures.map((c) => {
    const ordinal = String(c.ordinal).padStart(4, '0');
    const read_file = `conn-${ordinal}.read.bin`; const write_file = `conn-${ordinal}.write.bin`;
    files.set(read_file, c.read); files.set(write_file, c.write);
    return { ordinal: c.ordinal, read_file, write_file, read_bytes: c.read.length, write_bytes: c.write.length,
      read_eof: c.read_eof, write_shutdown: c.write_shutdown };
  });
  files.set('capture-manifest.json', encode({ version: 1, accepted: connections.length,
    retired: connections.length, sticky_loss: false, connections }));
  proof.capture = { accepted: connections.length, retired: connections.length, sticky_loss: false };
  const client = settleRole('client', new Map()); const native = settleRole('native', files);
  const refresh = (clientBytes = encode(traffic.rawClient)) => {
    client.files.set('client-observations.json', clientBytes);
    Object.assign(client, settleRole('client', client.files));
    proof.evidence.client_used_bytes = client.usedBytes;
    files.set('native-proof.json', encode(proof)); Object.assign(native, settleRole('native', files));
  };
  refresh();
  return { native, client, rawClient: traffic.rawClient, captures: traffic.captures, ready: traffic.ready, proof, refresh };
}

export async function writeQualificationUnitFixture(t, fixture = makeQualificationUnitFixture()) {
  const root = await mkdtemp(join(tmpdir(), 'b4-qualification-unit-')); await chmod(root, 0o700);
  t.after(() => rm(root, { recursive: true, force: true }));
  const nativeDir = join(root, 'native'); const clientDir = join(root, 'client');
  for (const [directory, role] of [[nativeDir, fixture.native], [clientDir, fixture.client]]) {
    await mkdir(directory, { mode: 0o700 });
    for (const [name, body] of role.files) await writeFile(join(directory, name), body, { mode: 0o600, flag: 'wx' });
  }
  return { ...fixture, nativeDir, clientDir };
}
