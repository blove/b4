// Scripted method doubles exercise caller orchestration only. They are never
// bundled/imported as original clients or counted as native/corpus observations.
import { CorpusContext } from './runner-context.mjs';

function http(name, status, code) {
  return Object.assign(new Error('unit refusal'), { name, status, code });
}

export function unitContext() {
  let generation = 0;
  const terminals = new Set();
  let heldWait;
  const context = new CorpusContext({ ready: { base_url: 'http://127.0.0.1:12345', token: 'unit', routes: {
    worker_value: '/fixture/worker-value#workflow', worker_void: '/fixture/worker-void#workflow',
    worker_failure: '/fixture/worker-failure#workflow', worker_held: '/fixture/worker-held#workflow', worker_park: '/fixture/worker-park#workflow',
  } }, control: { async request(group, command) {
    const [operation, payload] = Object.entries(command)[0];
    if (operation === 'arm') return { generation: ++generation };
    if (operation === 'release') {
      for (const resolve of terminals) resolve({}); terminals.clear();
      heldWait?.resolve({ ok: true, message: 'unit' });
      return { generation: payload.generation, entered: true, returned: true, explicit_release: true, cleanup_release: false, expired: false };
    }
    if (['await_entered', 'gate_status'].includes(operation)) return {
      generation: payload.generation, entered: true, returned: false, explicit_release: false, cleanup_release: false, expired: false,
    };
    const parked = payload.thread_id.includes('park-create');
    const state = { found: !payload.thread_id.includes('missing'), execution_status: parked ? 'waiting_for_approval' : 'running',
      task_statuses: [parked ? 'waiting_for_approval' : 'in_flight'], revision: 3, owner_generation: 1 };
    if (operation === 'inspect_pending') state.interrupt_ids = ['unit-interrupt'];
    return state;
  } } });
  context.actualMethodCalls = [];
  let currentCase;
  const originalCall = context.call.bind(context);
  context.call = (method, caseName, args, operation) => {
    const wrapped = () => { currentCase = caseName; return operation(); };
    return originalCall(method, caseName, args, wrapped);
  };
  const method = (name, callback) => (...args) => {
    context.actualMethodCalls.push({ method: name, case: currentCase, arguments: args });
    return callback(currentCase, args);
  };
  const controller = {};
  for (const name of ['create', 'dispatch', 'approve', 'retry', 'deny', 'intake', 'approveIntake', 'rejectIntake', 'cancel', 'reconcile']) {
    controller[name] = method(`controller.${name}`, (caseName, args) => {
      if (caseName === 'wait-failure') throw http('ControllerHttpError', 500, undefined);
      if (caseName.includes('busy')) throw http('ControllerHttpError', 409, 'thread_busy');
      if (caseName.startsWith('held-dispatch')) return new Promise((resolve, reject) => {
        heldWait = { resolve, reject };
        args[2]?.addEventListener('abort', () => reject(args[2].reason), { once: true });
      });
      if (name === 'cancel') return null;
      return { ok: true, message: 'unit' };
    });
  }
  controller.interrupt = method('controller.interrupt', (caseName) => {
    if (['interrupt-idle', 'interrupt-missing', 'interrupt-concealed'].includes(caseName)) return 'no_run_in_flight';
    heldWait?.reject(http('ControllerHttpError', 409, 'run_cancelled'));
    return 'interrupted';
  });
  context.controller = controller;
  context.directController = { dispatch: method('controller.dispatch', () => { throw http('ControllerHttpError', 401, 'authentication_required'); }) };

  function stream(route, signal, statePrefix = false) {
    const held = route.includes('held');
    const promise = held ? new Promise((resolve, reject) => {
      terminals.add(resolve);
      signal?.addEventListener('abort', () => reject(signal.reason), { once: true });
    }) : Promise.resolve();
    // Rejection is observed immediately; actual iterable next still awaits it.
    promise.catch(() => {});
    return (async function* () {
      if (statePrefix) yield { event: 'state', data: { unit: true } };
      if (held) {
        const terminal = await promise;
        yield { event: 'done', data: terminal?.cancelled ? { output: { cancelled: true } } : { output: { ok: true } } };
      } else if (route.includes('park')) {
        yield { event: 'interrupt', data: { interruptId: 'unit-interrupt' } };
        yield { event: 'done', data: { output: null } };
      } else yield { event: 'done', data: route.includes('void') ? {}
        : route.includes('failure') ? { output: { error: 'unit failure' } } : { output: { ok: true } } };
    })();
  }
  const worker = {
    createThread: method('worker.createThread', (caseName) => `unit-${caseName}`),
    getThread: method('worker.getThread', (caseName, args) => caseName.includes('missing') || caseName.includes('concealed') ? null : { threadId: args[0], status: 'idle' }),
    deleteThread: method('worker.deleteThread', (caseName) => { if (caseName === 'worker-delete-held') throw http('WorkerHttpError', 409, 'run_in_flight'); return 'deleted'; }),
    startRun: method('worker.startRun', (caseName, args) => {
      if (caseName.includes('busy')) throw http('WorkerHttpError', 409, 'thread_busy');
      return stream(args[1], args[3]);
    }),
    reattach: method('worker.reattach', (caseName, args) => stream(caseName === 'attach-live' ? 'held' : 'value', args[1], true)),
    cancel: method('worker.cancel', (caseName) => {
      if (caseName === 'worker-cancel-concealed' || caseName === 'worker-cancel-missing') return 'thread_not_found';
      if (caseName === 'worker-cancel-idle') return 'no_run_in_flight';
      for (const resolve of terminals) resolve({ cancelled: true }); terminals.clear();
      return 'interrupted';
    }),
    pendingInterrupts: method('worker.pendingInterrupts', (caseName) => {
      if (caseName === 'pending-empty') return [];
      throw Object.assign(new Error('unit original schema rejection'), { name: 'ZodError' });
    }),
    resume: method('worker.resume', (caseName) => { throw http('WorkerHttpError', 400, caseName === 'resume-empty' ? 'invalid_request' : 'grant_required'); }),
  };
  context.worker = worker;
  context.wrongWorker = { getThread: method('worker.getThread', () => { throw http('WorkerHttpError', 401, 'authentication_required'); }) };
  return context;
}
