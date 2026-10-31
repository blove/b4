// Closed, safe facts only. Raw caller objects do not enter this projection.
// The offline qualifier must verify these facts against private artifacts first.
const COMMIT = '8acac534ba6eb960b34de217b0da45b8405023af';
const GROUPS = ['C1', 'C2', 'C3', 'C4', 'W1', 'W2', 'W3', 'W4'];
const CONTROLLER = ['create', 'dispatch', 'approve', 'retry', 'deny', 'intake', 'approveIntake', 'rejectIntake', 'cancel', 'reconcile', 'interrupt'].map((n) => `controller.${n}`);
const WORKER = ['createThread', 'getThread', 'deleteThread', 'pendingInterrupts', 'startRun', 'reattach', 'resume', 'cancel'].map((n) => `worker.${n}`);
const METHODS = [...CONTROLLER, ...WORKER];
const STATUSES = [0, 200, 201, 204, 400, 401, 404, 409, 500];
const ERRORS = ['none', 'ControllerHttpError', 'WorkerHttpError', 'ZodError', 'DOMException', 'AbortError', 'Error', 'TypeError'];
const CODES = ['none', 'authentication_required', 'resource_unavailable', 'thread_busy', 'execution_error', 'grant_required', 'invalid_request', 'no_run_in_flight', 'thread_not_found', 'run_cancelled', 'run_in_flight'];
const RESULTS = ['none', 'route-outcome', 'fixture-value', 'void-null', 'thread-id', 'thread-metadata', 'null', 'deleted', 'not-found', 'interrupt-result', 'pending-empty', 'cancel-result', 'frame-stream'];
const FRAME_SHAPES = Object.freeze({
  state: ['state-busy', 'state-idle', 'state-interrupted'],
  done: ['done-value', 'done-null', 'done-void', 'done-failure', 'done-cancelled'],
  interrupt: ['native-permission-request'],
  chunk: ['fixture-value'],
});
const ROW_KEYS = ['method', 'outcome', 'http_status', 'client_status', 'error_class', 'error_code', 'result_shape', 'frames', 'eof', 'cut'];
const RETIREMENT_KEYS = ['accepted', 'retired', 'capture_complete', 'child_exit_code', 'control_eof', 'control_joined', 'stderr_joined', 'server_joined', 'quiescent', 'faulted', 'store_weak_absent', 'application_weak_absent', 'writer_reopened', 'owner_advanced', 'expired_gates', 'cleanup_releases'];
const fail = (s) => { throw new Error(`Client projection: ${s}`); };

function object(value, keys) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail('invalid object');
  // Descriptors prevent getters from returning changing values during validation.
  const descriptors = Object.getOwnPropertyDescriptors(value);
  if (Reflect.ownKeys(descriptors).length !== keys.length || Reflect.ownKeys(descriptors).some((key) => !keys.includes(key))) fail('unknown or missing fields');
  for (const key of keys) if (!Object.hasOwn(descriptors[key], 'value') || !descriptors[key].enumerable) fail('invalid field');
  return value;
}
const member = (value, admitted, label) => { if (!admitted.includes(value)) fail(`invalid ${label}`); return value; };
const integer = (n, max, label) => { if (!Number.isSafeInteger(n) || n < 0 || n > max) fail(`${label} limit`); return n; };
const bool = (v, label) => { if (typeof v !== 'boolean') fail(`invalid ${label}`); return v; };

function array(value, min, max, label) {
  if (!Array.isArray(value) || Object.getPrototypeOf(value) !== Array.prototype ||
      value.length < min || value.length > max) fail(`${label} array limit`);
  const descriptors = Object.getOwnPropertyDescriptors(value);
  if (Reflect.ownKeys(descriptors).length !== value.length + 1) fail(`${label} array elements`);
  for (let i = 0; i < value.length; i++) {
    const d = descriptors[i];
    if (!d || !Object.hasOwn(d, 'value') || !d.enumerable) fail(`${label} array element`);
  }
  return value;
}

function retirement(value) {
  object(value, RETIREMENT_KEYS);
  if (integer(value.accepted, 16, 'retirement') < 1 || value.retired !== value.accepted || value.child_exit_code !== 0 ||
      value.faulted !== false || value.expired_gates !== 0 || value.cleanup_releases !== 0) fail('retirement is incomplete');
  for (const key of RETIREMENT_KEYS.filter((k) => !['accepted', 'retired', 'child_exit_code', 'faulted', 'expired_gates', 'cleanup_releases'].includes(k))) {
    if (value[key] !== true) fail('retirement is incomplete');
  }
  return Object.fromEntries(RETIREMENT_KEYS.map((k) => [k, value[k]]));
}

function observation(value, group) {
  object(value, ROW_KEYS);
  const method = member(value.method, group.startsWith('C') ? CONTROLLER : WORKER, 'method');
  const outcome = member(value.outcome, ['returned', 'rejected', 'stream'], 'outcome');
  const http_status = member(value.http_status, STATUSES, 'status');
  const client_status = member(value.client_status, STATUSES, 'client status');
  const error_class = member(value.error_class, ERRORS, 'error class');
  const error_code = member(value.error_code, CODES, 'error code');
  const result_shape = member(value.result_shape, RESULTS, 'result shape');
  const eof = bool(value.eof, 'EOF');
  const cut = member(value.cut, ['none', 'viewer-return', 'signal-abort'], 'cut');
  array(value.frames, 0, 64, 'frame');
  const frames = value.frames.map((frame) => {
    object(frame, ['event', 'shape']);
    member(frame.event, Object.keys(FRAME_SHAPES), 'event');
    member(frame.shape, FRAME_SHAPES[frame.event], 'frame shape');
    return { event: frame.event, shape: frame.shape };
  });
  if (outcome === 'rejected') {
    if (error_class === 'none' || result_shape !== 'none' || frames.length || eof) fail('inconsistent error observation');
  } else if (error_class !== 'none' || error_code !== 'none' || client_status !== 0) fail('inconsistent successful observation');
  if (outcome === 'stream') {
    if (result_shape !== 'frame-stream' || (cut === 'none' ? !eof : eof)) fail('inconsistent stream observation');
  } else if (frames.length || eof || (cut !== 'none' && !(outcome === 'rejected' && cut === 'signal-abort'))) fail('inconsistent stream observation');
  return { method, outcome, http_status, client_status, error_class, error_code, result_shape, frames, eof, cut };
}

export function projectClientObservations(facts) {
  object(facts, ['version', 'platform', 'source_commit', 'groups', 'retirement']);
  if (facts.version !== 1 || facts.platform !== 'macos-arm64' || facts.source_commit !== COMMIT) fail('unsupported provenance scope');
  array(facts.groups, GROUPS.length, GROUPS.length, 'groups');
  let invocation_count = 0;
  const seen = new Set(); const methods = new Set();
  const groups = facts.groups.map((g) => {
    object(g, ['name', 'observations']);
    member(g.name, GROUPS, 'group');
    if (seen.has(g.name)) fail('duplicate groups'); seen.add(g.name);
    array(g.observations, 1, 256, 'observation');
    invocation_count += g.observations.length;
    integer(invocation_count, 256, 'run observation');
    const observations = g.observations.map((row) => {
      const out = observation(row, g.name); methods.add(out.method); return out;
    });
    return { name: g.name, invocation_count: observations.length, observations };
  }).sort((a, b) => GROUPS.indexOf(a.name) - GROUPS.indexOf(b.name));
  if (methods.size !== METHODS.length || METHODS.some((m) => !methods.has(m))) fail('incomplete selected methods');
  return {
    version: 1, kind: 'unchanged-ap-client-native-summary', platform: facts.platform, source_commit: COMMIT,
    selected_method_count: METHODS.length, core_operation_count: 9,
    core_operations: ['AP-001', 'AP-002', 'AP-003', 'AP-005', 'AP-006', 'AP-007', 'AP-008', 'AP-009', 'AP-010'],
    invocation_count, groups, retirement: retirement(facts.retirement),
    seams: ['native-fixture-outcomes', 'controller-bearer-header-injection', 'controller-origin-and-redirect-guard', 'forced-private-interrupt-id', 'passive-socket-accepted-bytes'],
    gates: { M0: 'open', CP001_to009: 'partial', CP010: 'unresolved', M5: 'partial', broad56: 'not_run', core_profile: 'unfrozen' },
    limitations: ['selected-original-callers-only', 'initial-macos-arm64-only', 'factory-business-behavior-excluded', 'socket-acceptance-is-not-caller-consumption', 'pending-schema-and-grantless-resume-counterexamples-retained', 'method-calls-are-not-all-positive-successes'],
  };
}
