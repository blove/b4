import test from 'node:test';
import assert from 'node:assert/strict';
import { projectClientObservations } from './projection.mjs';

const controller = ['create', 'dispatch', 'approve', 'retry', 'deny', 'intake', 'approveIntake', 'rejectIntake', 'cancel', 'reconcile', 'interrupt'];
const worker = ['createThread', 'getThread', 'deleteThread', 'pendingInterrupts', 'startRun', 'reattach', 'resume', 'cancel'];
function row(method, extra = {}) {
  return { method, outcome: 'returned', http_status: 200, client_status: 0, error_class: 'none', error_code: 'none',
    result_shape: 'fixture-value', frames: [], eof: false, cut: 'none', ...extra };
}
function facts() {
  return { version: 1, platform: 'macos-arm64', source_commit: '8acac534ba6eb960b34de217b0da45b8405023af', groups: [
    { name: 'C1', observations: controller.slice(0, 10).map((name) => row(`controller.${name}`)) },
    { name: 'C2', observations: [row('controller.dispatch', { outcome: 'rejected', http_status: 401, client_status: 401, error_class: 'ControllerHttpError', error_code: 'authentication_required', result_shape: 'none' })] },
    { name: 'C3', observations: [row('controller.cancel', { result_shape: 'void-null' })] },
    { name: 'C4', observations: [row('controller.interrupt', { result_shape: 'interrupt-result' })] },
    { name: 'W1', observations: worker.slice(0, 3).map((name, i) => row(`worker.${name}`, { result_shape: ['thread-id', 'thread-metadata', 'deleted'][i] })) },
    { name: 'W2', observations: [row('worker.startRun', { outcome: 'stream', result_shape: 'frame-stream', frames: [{ event: 'done', shape: 'done-value' }], eof: true })] },
    { name: 'W3', observations: [row('worker.reattach', { outcome: 'stream', result_shape: 'frame-stream', frames: [{ event: 'state', shape: 'state-busy' }], cut: 'viewer-return' }), row('worker.cancel', { result_shape: 'cancel-result' })] },
    { name: 'W4', observations: [row('worker.pendingInterrupts', { outcome: 'rejected', error_class: 'ZodError', result_shape: 'none' }), row('worker.resume', { outcome: 'rejected', http_status: 400, client_status: 400, error_class: 'WorkerHttpError', error_code: 'grant_required', result_shape: 'none' })] },
  ], retirement: { accepted: 5, retired: 5, capture_complete: true, child_exit_code: 0, control_eof: true, control_joined: true,
    stderr_joined: true, server_joined: true, quiescent: true, faulted: false, store_weak_absent: true,
    application_weak_absent: true, writer_reopened: true, owner_advanced: true, expired_gates: 0, cleanup_releases: 0 },
  };
}

test('constructs bounded group/method facts including counterexamples without exposing raw values', () => {
  const out = projectClientObservations(facts());
  assert.equal(out.groups.length, 8);
  assert.equal(out.selected_method_count, 19);
  assert.equal(out.core_operation_count, 9);
  assert.equal(out.gates.CP010, 'unresolved');
  assert.equal(out.groups[1].observations[0].error_code, 'authentication_required');
  assert.equal(out.groups[7].observations[0].error_class, 'ZodError');
  assert.equal(out.groups[5].observations[0].frames[0].shape, 'done-value');
  assert.match(JSON.stringify(out), /forced-private-interrupt-id/);
  assert.equal(JSON.stringify(out).includes('arguments'), false);
});

test('rejects every unknown raw authority, error, identifier and digest field instead of scrubbing', () => {
  for (const field of ['token', 'grant', 'thread_id', 'arguments', 'message', 'stack', 'raw_sha256', 'timestamp']) {
    const input = facts(); input.groups[0].observations[0][field] = 'private';
    assert.throws(() => projectClientObservations(input), /unknown/);
  }
  for (const field of ['raw_http', 'source_sha256', 'native_proof', 'private_directory']) {
    const input = facts(); input[field] = 'private';
    assert.throws(() => projectClientObservations(input), /unknown/);
  }
  const input = facts(); input.groups[5].observations[0].frames[0].data = { grant: 'private' };
  assert.throws(() => projectClientObservations(input), /unknown/);
});

test('refuses arbitrary strings in admitted fields and incomplete or duplicate corpus groups', () => {
  for (const [field, value] of [['method', 'controller.private-ID'], ['error_class', 'private-stack'], ['error_code', 'arbitrary'], ['result_shape', 'grant-bearing-object'], ['cut', 'process-kill']]) {
    const input = facts(); input.groups[0].observations[0][field] = value;
    assert.throws(() => projectClientObservations(input));
  }
  const missing = facts(); missing.groups.pop(); assert.throws(() => projectClientObservations(missing), /groups/);
  const repeated = facts(); repeated.groups[7].name = 'C1'; assert.throws(() => projectClientObservations(repeated), /groups/);
  const method = facts(); method.groups[3].observations = [row('controller.dispatch')]; assert.throws(() => projectClientObservations(method), /methods/);
});

test('separates a successful cut from EOF and refuses loss, forced cleanup or incomplete retirement', () => {
  for (const [field, value] of [['retired', 4], ['capture_complete', false], ['child_exit_code', 1], ['faulted', true], ['expired_gates', 1], ['cleanup_releases', 1], ['server_joined', false], ['writer_reopened', false], ['accepted', 17]]) {
    const input = facts(); input.retirement[field] = value;
    assert.throws(() => projectClientObservations(input), /retirement/);
  }
  const input = facts(); input.groups[6].observations[0].eof = true;
  assert.throws(() => projectClientObservations(input), /stream/);
});

test('bounds observations and frames and keeps repeat counts distinct from unique methods', () => {
  const input = facts(); input.groups[0].observations.push(row('controller.create'));
  const out = projectClientObservations(input);
  assert.equal(out.selected_method_count, 19);
  assert.equal(out.invocation_count, facts().groups.reduce((n, g) => n + g.observations.length, 0) + 1);
  const frames = facts(); frames.groups[5].observations[0].frames = Array.from({ length: 65 }, () => ({ event: 'done', shape: 'done-value' }));
  assert.throws(() => projectClientObservations(frames), /limit/);
  const rows = facts(); rows.groups[0].observations = Array.from({ length: 257 }, () => row('controller.create'));
  assert.throws(() => projectClientObservations(rows), /limit/);
});

test('admits the actual native held-thread deletion refusal code without translating it', () => {
  const input = facts();
  input.groups[4].observations.push(row('worker.deleteThread', { outcome: 'rejected', http_status: 409,
    client_status: 409, error_class: 'WorkerHttpError', error_code: 'run_in_flight', result_shape: 'none' }));
  assert.equal(projectClientObservations(input).groups[4].observations.at(-1).error_code, 'run_in_flight');
});

test('closed facts arrays require dense own elements so holes cannot conceal required groups or counted rows', () => {
  for (const target of ['groups', 'observations', 'frames']) {
    const input = facts();
    const array = target === 'groups' ? input.groups : target === 'observations'
      ? input.groups[1].observations : input.groups[5].observations[0].frames;
    delete array[target === 'groups' ? 1 : 0];
    assert.throws(() => projectClientObservations(input), /array|element/);
  }
});

test('closed facts arrays refuse getters and additional properties without invoking them', () => {
  const input = facts(); let reads = 0;
  const original = input.groups[0].observations[0];
  Object.defineProperty(input.groups[0].observations, '0', { enumerable: true, get() { reads++; return original; } });
  assert.throws(() => projectClientObservations(input), /array|element/);
  assert.equal(reads, 0);
  const extra = facts(); extra.groups.private_token = 'raw';
  assert.throws(() => projectClientObservations(extra), /array|element/);
});
