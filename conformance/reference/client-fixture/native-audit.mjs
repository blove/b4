// Offline private native evidence audit; no caller modules are imported here.
import { isDeepStrictEqual } from 'node:util';
import { readPrivateJson } from './artifacts.mjs';

const LIMITS = { native: 24 * 1024 * 1024, client: 8 * 1024 * 1024 };
const GROUPS = ['C1', 'C2', 'C3', 'C4', 'W1', 'W2', 'W3', 'W4'];
const FUNCTIONS = ['C1_factory_method_map', 'C2_controller_auth', 'C3_wait_terminals', 'C4_held_controller',
  'W1_worker_crud_auth', 'W2_finite_sse', 'W3_attachment_cancel', 'W4_durable_park_refusal'];
const GATES = ['controller_dispatch', 'worker_held'];
const ROUTES = Object.freeze(Object.fromEntries(['value', 'void', 'failure', 'held', 'park']
  .map((kind) => [`worker_${kind}`, `/fixture/worker-${kind}#workflow`])));
const EXECUTIONS = ['accepted', 'running', 'waiting_for_approval', 'completed', 'failed', 'cancel_requested', 'cancelled', 'recovery_blocked'];
const TASKS = ['ready', 'waiting_for_approval', 'in_flight', 'succeeded', 'failed', 'denied', 'cancelled', 'outcome_unknown'];
const FLAGS = ['generation', 'entered', 'returned', 'explicit_release', 'cleanup_release', 'expired'];
const ORIGIN = { application: 'ap-client-fixture', tenant: 'local', principal: 'owner' };
const fail = (code) => { throw new Error(`Native audit: ${code}`); };
function object(value, required, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
    || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail('object fields');
  const descriptors = Object.getOwnPropertyDescriptors(value);
  const names = Reflect.ownKeys(descriptors);
  if (required.some((key) => !names.includes(key)) || names.some((key) => ![...required, ...optional].includes(key))
    || names.some((key) => !Object.hasOwn(descriptors[key], 'value') || !descriptors[key].enumerable)) fail('object fields');
}
function array(value, min, max) {
  if (!Array.isArray(value) || value.length < min || value.length > max) fail('array bound');
  const descriptors = Object.getOwnPropertyDescriptors(value);
  if (Reflect.ownKeys(descriptors).length !== value.length + 1) fail('array fields');
  for (let i = 0; i < value.length; i++) if (!descriptors[i] || !Object.hasOwn(descriptors[i], 'value') || !descriptors[i].enumerable) fail('array fields');
}
function integer(n, min = 0, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(n) || n < min || n > max) fail('integer bound');
}
function string(s, max = 1024) {
  if (typeof s !== 'string' || !s.length || s.includes('\0') || Buffer.byteLength(s) > max) fail('string bound');
}
function storeMap(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
    || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail('Store map fields');
}
function equal(a, b, code) { if (!isDeepStrictEqual(a, b)) fail(code); }
function positive(value, fields) { for (const key of fields) if (value[key] !== true) fail('positive retirement'); }
const suffix = (n) => String(n).padStart(4, '0');

function role(value, name) {
  object(value, ['role', 'directory', 'files', 'usedBytes', 'limitBytes']);
  if (value.role !== name || typeof value.directory !== 'string' || !value.directory.startsWith('/')
    || !(value.files instanceof Map) || value.limitBytes !== LIMITS[name]) fail('role');
  integer(value.files.size, 1, name === 'native' ? 2048 : 2);
  let used = 0;
  for (const [file, body] of value.files) {
    if (typeof file !== 'string' || !Buffer.isBuffer(body)) fail('role file');
    used += body.length; integer(used, 0, LIMITS[name]);
  }
  if (used !== value.usedBytes) fail('role accounting');
  const ledger = readPrivateJson(value.files, 'quota-ledger.json');
  object(ledger, ['version', 'limit_bytes', 'used_bytes']);
  equal(ledger, { version: 1, limit_bytes: LIMITS[name], used_bytes: used }, 'role ledger');
  if (name === 'client' && (value.files.size !== 2 || !value.files.has('client-observations.json'))) fail('client files');
}

function capture(files, consumed) {
  const manifest = readPrivateJson(files, 'capture-manifest.json');
  object(manifest, ['version', 'accepted', 'retired', 'sticky_loss', 'connections']);
  integer(manifest.accepted, 1, 16);
  if (manifest.version !== 1 || manifest.retired !== manifest.accepted || manifest.sticky_loss !== false) fail('capture retirement');
  array(manifest.connections, manifest.accepted, manifest.accepted);
  consumed.add('capture-manifest.json');
  manifest.connections.forEach((entry, i) => {
    object(entry, ['ordinal', 'read_file', 'write_file', 'read_bytes', 'write_bytes', 'read_eof', 'write_shutdown']);
    const read = `conn-${suffix(i + 1)}.read.bin`; const write = `conn-${suffix(i + 1)}.write.bin`;
    if (entry.ordinal !== i + 1 || entry.read_file !== read || entry.write_file !== write) fail('capture ordinal');
    integer(entry.read_bytes, 0, 262144); integer(entry.write_bytes, 0, 262144);
    if (typeof entry.read_eof !== 'boolean' || typeof entry.write_shutdown !== 'boolean') fail('capture flags');
    if (!files.has(read) || !files.has(write) || files.get(read).length !== entry.read_bytes || files.get(write).length !== entry.write_bytes) fail('capture bytes');
    consumed.add(read); consumed.add(write);
  });
  return manifest;
}

function ready(result) {
  object(result, ['base_url', 'token', 'routes']); object(result.routes, Object.keys(ROUTES));
  equal(result.routes, ROUTES, 'ready routes');
  if (typeof result.token !== 'string' || !/^[a-f0-9]{64}$/.test(result.token)) fail('ready token');
  let url; try { url = new URL(result.base_url); } catch { fail('ready origin'); }
  if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || !url.port || url.username || url.password
    || url.pathname !== '/' || url.search || url.hash || result.base_url !== url.origin) fail('ready origin');
  return result;
}
function gateFlags(value, generation, returned) {
  object(value, FLAGS); integer(generation, 1, 256);
  if (value.generation !== generation || value.entered !== true || value.returned !== returned
    || value.explicit_release !== returned || value.cleanup_release !== false || value.expired !== false) fail('gate history');
}
function taskSpec(spec) {
  object(spec, ['id', 'effect_id', 'recovery', 'requires_approval']);
  if (spec.id !== 'fixture-task' || spec.effect_id !== 'fixture-effect' || spec.recovery !== 'pure'
    || typeof spec.requires_approval !== 'boolean') fail('fixture task');
}
function threadView(view, id, owner) {
  if (view === null) return;
  object(view, ['thread', 'execution']);
  const t = view.thread;
  object(t, ['id', 'generation', 'active_execution_id', 'created_at_ms', 'metadata', 'origin', 'resource_revision']);
  if (t.id !== id) fail('thread identity'); string(t.id); string(t.generation);
  integer(t.created_at_ms); integer(t.resource_revision);
  storeMap(t.metadata);
  equal(t.origin, ORIGIN, 'thread origin');
  if (view.execution === null) { if (t.active_execution_id !== null) fail('active execution'); return; }
  const e = view.execution; object(e, ['thread_id', 'snapshot']);
  if (e.thread_id !== id) fail('execution thread');
  const s = e.snapshot;
  object(s, ['schema_version', 'revision', 'execution_id', 'turn_id', 'turn_history', 'attempt_history', 'owner_generation',
    'definition', 'status', 'batch_index', 'values', 'tasks', 'attempts_used', 'updated_at_ms', 'failure'], ['output']);
  if (s.schema_version !== 1 || s.owner_generation !== owner || s.execution_id !== t.active_execution_id
    || !EXECUTIONS.includes(s.status) || (s.failure !== null && typeof s.failure !== 'string')) fail('execution metadata');
  string(s.execution_id); string(s.turn_id); integer(s.revision); integer(s.batch_index, 0, 1); integer(s.attempts_used, 0, 1); integer(s.updated_at_ms);
  array(s.turn_history, 1, 1); array(s.attempt_history, 0, 1);
  equal(s.turn_history, [s.turn_id], 'turn history'); s.attempt_history.forEach((v) => string(v));
  const d = s.definition; object(d, ['route', 'revision', 'batches', 'max_attempts']); string(d.route);
  if (d.revision !== 'fixture-v1' || d.max_attempts !== 1) fail('fixture definition');
  array(d.batches, 1, 1); array(d.batches[0], 1, 1); taskSpec(d.batches[0][0]); array(s.tasks, 1, 1);
  const task = s.tasks[0]; object(task, ['spec', 'status', 'input', 'attempt_id', 'result', 'interrupt', 'approved'], ['failure']);
  storeMap(s.values); storeMap(task.input);
  taskSpec(task.spec); equal(task.spec, d.batches[0][0], 'task definition');
  if (!TASKS.includes(task.status) || typeof task.approved !== 'boolean') fail('task metadata');
  if (task.attempt_id !== null) string(task.attempt_id);
  if (task.result !== null) { object(task.result, ['output', 'writes']); array(task.result.writes, 0, 0); }
  if (task.interrupt !== null) { object(task.interrupt, ['id', 'prompt']); string(task.interrupt.id); }
  if (Object.hasOwn(task, 'failure')) string(task.failure);
}
function approvalView(view, thread) {
  if (thread === null || thread.execution === null) { if (view !== null) fail('approval absence'); return; }
  object(view, ['thread', 'execution', 'grants', 'acceptances']);
  equal(view.thread, thread.thread, 'approval thread'); equal(view.execution, thread.execution, 'approval execution');
  array(view.grants, 0, 1); array(view.acceptances, 0, 0);
  for (const g of view.grants) {
    object(g, ['thread_id', 'thread_generation', 'origin', 'execution_id', 'route', 'definition_revision', 'task_id', 'effect_id',
      'interrupt_id', 'resume_key', 'task_parking_revision', 'policy_revision', 'proposal_digest', 'prompt', 'token', 'verifier',
      'issued_at_ms', 'expires_at_ms', 'consumed']);
    const s = thread.execution.snapshot; const task = s.tasks[0];
    if (g.thread_id !== thread.thread.id || g.thread_generation !== thread.thread.generation || g.execution_id !== s.execution_id
      || g.route !== s.definition.route || g.definition_revision !== s.definition.revision || g.task_id !== task.spec.id
      || g.effect_id !== task.spec.effect_id || g.interrupt_id !== task.interrupt?.id || g.task_parking_revision !== s.revision
      || g.policy_revision !== 'fixture-policy-v1' || g.consumed !== false) fail('grant binding');
    equal(g.origin, ORIGIN, 'grant origin'); equal(g.prompt, task.interrupt.prompt, 'grant prompt'); string(g.interrupt_id); string(g.resume_key);
    if (typeof g.proposal_digest !== 'string' || !/^[a-f0-9]{64}$/.test(g.proposal_digest)
      || typeof g.token !== 'string' || !/^b4ag_[a-f0-9]{64}$/.test(g.token)) fail('grant authority');
    array(g.verifier, 32, 32); g.verifier.forEach((n) => integer(n, 0, 255));
    integer(g.issued_at_ms, 0, Number.MAX_SAFE_INTEGER - 86400000); integer(g.expires_at_ms, g.issued_at_ms, g.issued_at_ms + 86400000);
  }
}
function metadata(view, pending, approval) {
  const s = view?.execution?.snapshot;
  const result = { found: view !== null, execution_status: s?.status ?? null, task_statuses: s?.tasks.map((t) => t.status) ?? [],
    revision: s?.revision ?? null, owner_generation: s?.owner_generation ?? null };
  if (pending) result.interrupt_ids = approval?.grants.filter((g) => !g.consumed).map((g) => g.interrupt_id) ?? [];
  return result;
}

function controls(files, count, owner, consumed) {
  integer(count, 2, 256);
  const entries = []; const inspections = []; const gates = new Map(GATES.map((gate) => [gate, []]));
  let phase = 0; let firstReady;
  for (let ordinal = 1; ordinal <= count; ordinal++) {
    const prefix = `control-${suffix(ordinal)}`;
    const parse = (name) => {
      const body = files.get(name);
      if (!Buffer.isBuffer(body) || body.length < 2 || body.length > 8192 || body.indexOf(10) !== body.length - 1) fail('control line');
      consumed.add(name); return readPrivateJson(files, name);
    };
    const request = parse(`${prefix}-request.json`); const reply = parse(`${prefix}-reply.json`);
    object(request, ['version', 'ordinal', 'case', 'command']); object(reply, ['version', 'ordinal', 'ok', 'result']);
    if (request.version !== 1 || request.ordinal !== ordinal || reply.version !== 1 || reply.ordinal !== ordinal || reply.ok !== true) fail('control ordinal or refusal');
    const nextPhase = ['setup', ...GROUPS, 'retirement'].indexOf(request.case);
    if (nextPhase < phase || nextPhase < 0) fail('control phase'); phase = nextPhase;
    if (!request.command || typeof request.command !== 'object' || Array.isArray(request.command) || Object.keys(request.command).length !== 1) fail('control command');
    const operation = Object.keys(request.command)[0]; const payload = request.command[operation];
    const commands = { ready: [], arm: ['gate'], await_entered: ['gate', 'generation'], gate_status: ['gate', 'generation'],
      release: ['gate', 'generation'], inspect_thread: ['thread_id'], inspect_pending: ['thread_id'], final_stop: [] };
    if (!Object.hasOwn(commands, operation)) fail('control command'); object(payload, commands[operation]);
    if ((ordinal === 1) !== (operation === 'ready') || (ordinal === count) !== (operation === 'final_stop')
      || (request.case === 'setup') !== (operation === 'ready') || (request.case === 'retirement') !== (operation === 'final_stop')) fail('control endpoints');
    const entry = { ordinal, case: request.case, operation, payload, result: reply.result };
    if (operation === 'ready') firstReady = ready(reply.result);
    else if (operation === 'final_stop') equal(reply.result, { finished: true }, 'final stop');
    else if (operation === 'inspect_thread' || operation === 'inspect_pending') {
      string(payload.thread_id);
      const name = `snapshot-${suffix(ordinal)}-thread.json`; consumed.add(name);
      entry.thread = readPrivateJson(files, name); threadView(entry.thread, payload.thread_id, owner);
      if (operation === 'inspect_pending') {
        const a = `snapshot-${suffix(ordinal)}-approval.json`; consumed.add(a);
        entry.approval = readPrivateJson(files, a); approvalView(entry.approval, entry.thread);
      }
      equal(reply.result, metadata(entry.thread, operation === 'inspect_pending', entry.approval), 'inspect full snapshot');
      inspections.push(entry);
    } else {
      if (!GATES.includes(payload.gate) || (payload.gate === 'controller_dispatch' ? request.case !== 'C4' : !['W1', 'W3'].includes(request.case))) fail('gate case');
      const history = gates.get(payload.gate); const last = history.at(-1);
      if (operation === 'arm') {
        object(reply.result, ['generation']);
        if (last && !last.released) fail('gate rearm');
        if (reply.result.generation !== history.length + 1) fail('gate generation');
        history.push({ generation: reply.result.generation, case: request.case, arm: ordinal, entered: null, released: null, status: null });
      } else {
        if (!last || payload.generation !== last.generation || last.case !== request.case || last.released) fail('gate phase');
        gateFlags(reply.result, last.generation, operation === 'release');
        if (operation === 'await_entered') { if (last.entered !== null) fail('duplicate gate entry'); last.entered = ordinal; }
        else if (!last.entered) fail('gate entry required');
        if (operation === 'release') last.released = ordinal;
        last.status = reply.result;
      }
    }
    entries.push(entry);
  }
  if (gates.get('controller_dispatch').length !== 2 || gates.get('worker_held').length !== 3) fail('gate cardinality');
  for (const history of gates.values()) for (const generation of history) if (!generation.entered || !generation.released) fail('gate completion');
  return { entries, inspections, gates, ready: firstReady };
}

function callerWitnesses(raw, control) {
  object(raw, ['version', 'kind', 'groups', 'transports']);
  if (raw.version !== 1 || raw.kind !== 'original-ap-client-observations') fail('client kind');
  array(raw.groups, 8, 8); array(raw.transports, 0, 128);
  const witnessed = new Set(); const observations = new Map(); const ordinals = new Set();
  const annotations = new Set(); let totalWitnesses = 0;
  raw.groups.forEach((group, index) => {
    object(group, ['name', 'function', 'observations', 'witnesses']);
    if (group.name !== GROUPS[index] || group.function !== FUNCTIONS[index]) fail('client group');
    array(group.observations, 0, 128); array(group.witnesses, 0, 256);
    for (const row of group.observations) {
      object(row, ['ordinal', 'method', 'case', 'arguments', 'outcome']); integer(row.ordinal, 1, 128); string(row.case, 128);
      array(row.arguments, 0, 8);
      if (ordinals.has(row.ordinal) || observations.has(`${group.name}/${row.case}`)) fail('client invocation duplicate');
      ordinals.add(row.ordinal); observations.set(`${group.name}/${row.case}`, row);
    }
    for (const witness of group.witnesses) {
      if (++totalWitnesses > 256) fail('witness bound');
      object(witness, ['name', 'control_ordinal', 'value']);
      if (witness.control_ordinal === null) {
        const name = group.name === 'C2' ? 'default-fetch-versus-bearer-only-injection'
          : group.name === 'W4' ? 'forced-id-only-no-grant-and-metadata-unchanged' : null;
        if (name === null || witness.name !== name || witness.value !== true || annotations.has(name)) fail('static witness');
        annotations.add(name); continue;
      }
      integer(witness.control_ordinal, 2, control.entries.length - 1);
      const entry = control.entries[witness.control_ordinal - 1];
      const name = { inspect_thread: 'inspect_thread', inspect_pending: 'inspect_pending', arm: `arm-${entry.payload.gate}`,
        await_entered: `entered-${entry.payload.gate}`, gate_status: `held-${entry.payload.gate}`, release: `release-${entry.payload.gate}` }[entry.operation];
      if (entry.case !== group.name || witness.name !== name || witnessed.has(entry.ordinal)) fail('witness command');
      equal(witness.value, entry.result, 'witness result'); witnessed.add(entry.ordinal);
    }
  });
  if (annotations.size !== 2 || witnessed.size !== control.entries.length - 2) fail('witness completeness');
  const get = (group, name, method) => {
    const row = observations.get(`${group}/${name}`); if (!row || row.method !== method) fail('required caller'); return row;
  };
  return { get };
}

function selectedState(files, proof, control, raw, callers, consumed) {
  const arg = (g, name, method) => { const row = callers.get(g, name, method); string(row.arguments[0]); return row.arguments[0]; };
  const create = (g, name) => {
    const row = callers.get(g, name, 'worker.createThread');
    object(row.outcome, ['kind', 'value']); if (row.outcome.kind !== 'returned') fail('created thread'); string(row.outcome.value); return row.outcome.value;
  };
  const held = [
    ['controller_dispatch', 1, 'C4', arg('C4', 'held-dispatch-abandon', 'controller.dispatch'), 'cancelled'],
    ['controller_dispatch', 2, 'C4', arg('C4', 'held-dispatch-cancel', 'controller.dispatch'), 'cancelled'],
    ['worker_held', 1, 'W1', create('W1', 'worker-held-create'), 'deleted'],
    ['worker_held', 2, 'W3', create('W3', 'attach-create'), 'completed'],
    ['worker_held', 3, 'W3', create('W3', 'attach-cancel-create'), 'cancelled'],
  ];
  const starts = [['W1', 'worker-held-start', held[2][3]], ['W3', 'attach-held-start', held[3][3]], ['W3', 'attach-cancel-held-start', held[4][3]]];
  for (const [g, name, id] of starts) {
    const row = callers.get(g, name, 'worker.startRun');
    if (row.arguments[0] !== id || row.arguments[1] !== ROUTES.worker_held) fail('held caller identity');
  }
  const concealed = arg('C4', 'concealed-establish', 'controller.dispatch'); const parked = create('W4', 'park-create');
  const ids = [...held.map((h) => h[3]), concealed, parked];
  if (new Set(ids).size !== 7) fail('selected thread cardinality');
  equal(proof.control.observed_threads, [...ids].sort(), 'observed threads');
  const latest = new Map();
  for (const entry of control.inspections) {
    if (!ids.includes(entry.payload.thread_id)) fail('unexpected inspected thread');
    latest.set(entry.payload.thread_id, entry);
  }
  if (latest.size !== 7) fail('inspected thread completeness');
  for (const [gate, generation, g, id] of held) {
    const state = control.gates.get(gate)[generation - 1];
    if (state.case !== g) fail('selected gate generation');
    const entry = control.inspections.find((e) => e.payload.thread_id === id && e.ordinal > state.entered && e.ordinal < state.released);
    if (!entry || entry.operation !== 'inspect_thread' || entry.case !== g || entry.result.execution_status !== 'running'
      || !entry.result.task_statuses.includes('in_flight')) fail('actual inflight witness');
    if (entry.thread.execution.snapshot.definition.route !== (gate === 'controller_dispatch' ? '/work-orders/dispatch#workflow' : ROUTES.worker_held)) fail('held native route');
  }
  const park = control.inspections.filter((e) => e.case === 'W4');
  if (park.length !== 4 || park.some((e) => e.operation !== 'inspect_pending' || e.payload.thread_id !== parked)) fail('park snapshot sequence');
  const baseline = park[0]; const s = baseline.thread?.execution?.snapshot;
  if (!s || s.status !== 'waiting_for_approval' || s.definition.route !== ROUTES.worker_park || s.tasks[0].status !== 'waiting_for_approval'
    || s.tasks[0].spec.requires_approval !== true || s.tasks[0].approved !== false || s.tasks[0].attempt_id !== null
    || s.tasks[0].result !== null || baseline.approval.grants.length !== 1 || baseline.approval.acceptances.length !== 0) fail('durable park');
  const interrupt = baseline.approval.grants[0].interrupt_id;
  for (const entry of park.slice(1)) for (const kind of ['thread', 'approval']) {
    const before = `snapshot-${suffix(baseline.ordinal)}-${kind}.json`; const after = `snapshot-${suffix(entry.ordinal)}-${kind}.json`;
    equal(entry[kind], baseline[kind], 'park authority changed');
    if (!files.get(before).equals(files.get(after))) fail('park bytes changed');
  }
  for (const [index, name, payload] of [[0, 'resume-forced-once', 'once'], [1, 'resume-forced-deny', 'deny'], [2, 'resume-empty', null]]) {
    const row = callers.get('W4', name, 'worker.resume');
    const resolutions = payload === null ? [] : [{ interruptId: interrupt, payload }];
    equal(row.arguments, [parked, ROUTES.worker_park, resolutions], 'forced grantless arguments');
    if (row.outcome.kind !== 'rejected' || row.outcome.error?.name !== 'WorkerHttpError' || row.outcome.error.status !== 400
      || row.outcome.error.code !== (payload === null ? 'invalid_request' : 'grant_required')) fail('forced refusal');
    const transports = raw.transports.filter((t) => t.ordinal === row.ordinal);
    if (transports.length !== 1) fail('forced transport cardinality');
    const request = transports[0].request;
    object(request, ['url', 'method', 'original_headers', 'forwarded_headers', 'body', 'has_signal', 'redirect']);
    if (request.url !== `${control.ready.base_url}/threads/${encodeURIComponent(parked)}/resume` || request.method !== 'POST'
      || typeof request.body !== 'string' || Buffer.byteLength(request.body) > 8192) fail('forced transport');
    const body = readPrivateJson(new Map([['resume.json', Buffer.from(request.body)]]), 'resume.json');
    equal(body, { resume: resolutions.map((r) => ({ interruptId: r.interruptId, status: 'resolved', payload: r.payload })), route: ROUTES.worker_park }, 'forwarded grantless body');
    if (index && row.ordinal <= callers.get('W4', index === 1 ? 'resume-forced-once' : 'resume-forced-deny', 'worker.resume').ordinal) fail('forced probe order');
  }
  const deletion = callers.get('W1', 'worker-delete-released', 'worker.deleteThread');
  if (deletion.arguments[0] !== held[2][3] || deletion.outcome.kind !== 'returned' || deletion.outcome.value !== 'deleted') fail('held deletion');
  proof.control.observed_threads.forEach((id, i) => {
    const prefix = `reopened-${suffix(i + 1)}`; const t = `${prefix}-thread.json`; const a = `${prefix}-approval.json`;
    consumed.add(t); consumed.add(a);
    const view = readPrivateJson(files, t); const approvals = readPrivateJson(files, a);
    threadView(view, id, proof.writer.prior_owner_generation); approvalView(approvals, view);
    const before = latest.get(id); const selected = held.find((h) => h[3] === id);
    if (id === parked) {
      equal(view, baseline.thread, 'reopened park thread'); equal(approvals, baseline.approval, 'reopened park authority');
      if (!files.get(t).equals(files.get(`snapshot-${suffix(baseline.ordinal)}-thread.json`))
        || !files.get(a).equals(files.get(`snapshot-${suffix(baseline.ordinal)}-approval.json`))) fail('reopened park bytes');
    } else if (selected?.[4] === 'deleted') { if (view !== null || approvals !== null) fail('reopened deleted thread'); }
    else if (selected) {
      if (!before.thread?.execution || !view?.execution) fail('reopened execution');
      const expected = structuredClone(before.thread); const old = expected.execution.snapshot; const after = view.execution.snapshot;
      if (old.status !== 'running' || old.tasks[0].status !== 'in_flight' || old.tasks[0].result !== null || old.tasks[0].interrupt !== null
        || old.failure !== null || Object.hasOwn(old, 'output')) fail('held snapshot state');
      integer(after.updated_at_ms, old.updated_at_ms);
      const completed = selected[4] === 'completed'; const delta = completed ? 1 : 2;
      old.revision += delta; expected.thread.resource_revision += delta; old.updated_at_ms = after.updated_at_ms;
      // These immutable held callbacks return success after explicit release,
      // even while cancellation drains. The kernel preserves that task result
      // while settling the execution as Cancelled without reducing its values.
      old.status = selected[4]; old.tasks[0].status = 'succeeded';
      old.tasks[0].result = { output: selected[0] === 'controller_dispatch'
        ? { ok: true, message: 'fixture-value' } : { fixture: 'value' }, writes: [] };
      if (completed) { old.batch_index = 1; old.output = old.values; }
      equal(view, expected, 'reopened selected terminal');
      if (approvals.grants.length || approvals.acceptances.length) fail('unexpected terminal authority');
    } else {
      if (id !== concealed || before.result.execution_status !== 'completed') fail('concealed terminal');
      equal(view, before.thread, 'reopened concealed thread');
      if (!files.get(t).equals(files.get(`snapshot-${suffix(before.ordinal)}-thread.json`)) || approvals.grants.length || approvals.acceptances.length) fail('reopened concealed bytes');
    }
  });
}

export function auditNativeArtifacts(nativeRole, clientRole, rawClient) {
  role(nativeRole, 'native'); role(clientRole, 'client');
  const raw = readPrivateJson(clientRole.files, 'client-observations.json'); equal(rawClient, raw, 'client file binding');
  const files = nativeRole.files; const consumed = new Set(['quota-ledger.json', 'native-proof.json', 'stderr.bin']);
  const manifest = capture(files, consumed); const proof = readPrivateJson(files, 'native-proof.json');
  object(proof, ['version', 'child', 'control', 'control_joined', 'stderr', 'server', 'shutdown', 'capture', 'weak_store_absent',
    'weak_application_absent', 'writer', 'gates', 'mode_checks', 'evidence']);
  object(proof.child, ['exit_code', 'normal_reap']); object(proof.control, ['commands', 'final_stop', 'pipe_eof', 'observed_threads']);
  object(proof.stderr, ['pipe_eof', 'joined', 'bytes']); object(proof.server, ['normal_join']);
  object(proof.shutdown, ['outcome', 'faulted', 'active_owners']); object(proof.capture, ['accepted', 'retired', 'sticky_loss']);
  object(proof.writer, ['reopened', 'prior_owner_generation', 'owner_generation', 'ready']); object(proof.gates, GATES);
  object(proof.mode_checks, ['private_root', 'store_at_open', 'store_at_reopen']);
  object(proof.evidence, ['native_limit_bytes', 'client_limit_bytes', 'client_used_bytes']);
  if (proof.version !== 1 || proof.child.exit_code !== 0 || proof.shutdown.outcome !== 'quiescent' || proof.shutdown.faulted !== false
    || proof.shutdown.active_owners !== 0) fail('native retirement');
  positive(proof, ['control_joined', 'weak_store_absent', 'weak_application_absent']); positive(proof.child, ['normal_reap']);
  positive(proof.control, ['final_stop', 'pipe_eof']); positive(proof.stderr, ['pipe_eof', 'joined']); positive(proof.server, ['normal_join']);
  positive(proof.writer, ['reopened', 'ready']); positive(proof.mode_checks, ['private_root', 'store_at_open', 'store_at_reopen']);
  integer(proof.stderr.bytes, 0, 1048576);
  if (!Buffer.isBuffer(files.get('stderr.bin')) || files.get('stderr.bin').length !== proof.stderr.bytes) fail('stderr bytes');
  integer(proof.writer.prior_owner_generation, 1, Number.MAX_SAFE_INTEGER - 1);
  if (proof.writer.owner_generation !== proof.writer.prior_owner_generation + 1) fail('writer owner advancement');
  equal(proof.capture, { accepted: manifest.accepted, retired: manifest.retired, sticky_loss: false }, 'capture proof');
  equal(proof.evidence, { native_limit_bytes: LIMITS.native, client_limit_bytes: LIMITS.client, client_used_bytes: clientRole.usedBytes }, 'evidence proof');
  array(proof.control.observed_threads, 7, 7); proof.control.observed_threads.forEach((id) => string(id));
  const control = controls(files, proof.control.commands, proof.writer.prior_owner_generation, consumed);
  for (const gate of GATES) equal(proof.gates[gate], control.gates.get(gate).at(-1).status, 'latest gate proof');
  const callers = callerWitnesses(raw, control); selectedState(files, proof, control, raw, callers, consumed);
  if (consumed.size !== files.size || [...files.keys()].some((name) => !consumed.has(name))) fail('unaccounted native files');
  return { ready: control.ready, retirement: {
    accepted: manifest.accepted, retired: manifest.retired, capture_complete: true, child_exit_code: 0,
    control_eof: true, control_joined: true, stderr_joined: true, server_joined: true, quiescent: true, faulted: false,
    store_weak_absent: true, application_weak_absent: true, writer_reopened: true, owner_advanced: true,
    expired_gates: 0, cleanup_releases: 0,
  } };
}
