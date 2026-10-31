import { RunnerError } from './runner-context.mjs';

function value(context, result) {
  const actual = context.returned(result);
  if (actual?.ok !== true || typeof actual.message !== 'string') throw new RunnerError('unexpected_factory_value');
}

export async function C1_factory_method_map(c) {
  const create = { taskId: 'fixture-catalog-task', operationKey: 'fixture-create' };
  value(c, await c.call('controller.create', 'factory-create', [create], () => c.controller.create(create)));
  const calls = [
    ['dispatch', 'factory-dispatch', ['fixture-c1-dispatch', 'ordinary']],
    ['approve', 'factory-approve', ['fixture-c1-approve', { revision: 1, bundleDigest: 'a'.repeat(64), operationKey: 'ordinary' }]],
    ['retry', 'factory-retry', ['fixture-c1-retry', 'ordinary']],
    ['deny', 'factory-deny', ['fixture-c1-deny', 'ordinary']],
    ['intake', 'factory-intake', ['fixture-c1-intake', 'ordinary']],
    ['approveIntake', 'factory-approve-intake', ['fixture-c1-approve-intake', { revision: 1, taskDigest: 'b'.repeat(64), operationKey: 'ordinary' }]],
    ['rejectIntake', 'factory-reject-intake', ['fixture-c1-reject-intake', { note: 'finite fixture note', operationKey: 'ordinary' }]],
  ];
  for (const [method, caseName, args] of calls) value(c, await c.call(`controller.${method}`, caseName, args, () => c.controller[method](...args)));
  const args = ['fixture-c1-cancel', 'ordinary'];
  if (c.returned(await c.call('controller.cancel', 'factory-cancel', args, () => c.controller.cancel(...args))) !== null) throw new RunnerError('unexpected_void');
  value(c, await c.call('controller.reconcile', 'factory-reconcile', [], () => c.controller.reconcile()));
}

export async function C2_controller_auth(c) {
  const args = ['fixture-c2-direct', 'ordinary'];
  c.httpError(await c.call('controller.dispatch', 'default-auth-refusal', args, () => c.directController.dispatch(...args)), 'ControllerHttpError', 401, 'authentication_required');
  const positive = ['fixture-c2-positive', 'ordinary'];
  value(c, await c.call('controller.dispatch', 'bearer-auth-positive', positive, () => c.controller.dispatch(...positive)));
  c.witness('default-fetch-versus-bearer-only-injection', true);
}

export async function C3_wait_terminals(c) {
  value(c, await c.call('controller.dispatch', 'wait-value', ['fixture-c3-value', 'ordinary'], () => c.controller.dispatch('fixture-c3-value', 'ordinary')));
  c.httpError(await c.call('controller.dispatch', 'wait-failure', ['fixture-c3-failure', 'fixture-known-failure'], () => c.controller.dispatch('fixture-c3-failure', 'fixture-known-failure')), 'ControllerHttpError', 500, undefined);
  if (c.returned(await c.call('controller.cancel', 'wait-void', ['fixture-c3-void', 'ordinary'], () => c.controller.cancel('fixture-c3-void', 'ordinary'))) !== null) throw new RunnerError('unexpected_void');
}

export async function C4_held_controller(c) {
  const gate = 'controller_dispatch';
  const abandonedId = 'fixture-c4-abandon';
  const firstGeneration = await c.arm(gate);
  const controller = c.signal();
  const wait = c.call('controller.dispatch', 'held-dispatch-abandon', [abandonedId, 'ordinary', controller.signal], () => c.controller.dispatch(abandonedId, 'ordinary', controller.signal));
  await c.entered(gate, firstGeneration);
  c.inFlight(await c.inspect(abandonedId));
  c.httpError(await c.call('controller.dispatch', 'held-busy-before-abort', [abandonedId, 'ordinary'], () => c.controller.dispatch(abandonedId, 'ordinary')), 'ControllerHttpError', 409, 'thread_busy');
  controller.abort();
  if ((await wait).error?.name !== 'AbortError') throw new RunnerError('unexpected_wait_abort');
  await c.held(gate, firstGeneration);
  c.httpError(await c.call('controller.dispatch', 'held-busy-after-abort', [abandonedId, 'ordinary'], () => c.controller.dispatch(abandonedId, 'ordinary')), 'ControllerHttpError', 409, 'thread_busy');
  if (c.returned(await c.call('controller.interrupt', 'interrupt-abandoned-held', [abandonedId], () => c.controller.interrupt(abandonedId))) !== 'interrupted') throw new RunnerError('unexpected_cancel');
  await c.held(gate, firstGeneration);
  await c.release(gate, firstGeneration);

  const cancelledId = 'fixture-c4-cancel';
  const secondGeneration = await c.arm(gate);
  const cancelledSignal = c.signal();
  const cancelledWait = c.call('controller.dispatch', 'held-dispatch-cancel', [cancelledId, 'ordinary', cancelledSignal.signal], () => c.controller.dispatch(cancelledId, 'ordinary', cancelledSignal.signal));
  await c.entered(gate, secondGeneration);
  c.inFlight(await c.inspect(cancelledId));
  if (c.returned(await c.call('controller.interrupt', 'interrupt-held', [cancelledId], () => c.controller.interrupt(cancelledId))) !== 'interrupted') throw new RunnerError('unexpected_cancel');
  c.httpError(await cancelledWait, 'ControllerHttpError', 409, 'run_cancelled');
  await c.held(gate, secondGeneration);
  c.httpError(await c.call('controller.dispatch', 'held-busy-after-cancel', [cancelledId, 'ordinary'], () => c.controller.dispatch(cancelledId, 'ordinary')), 'ControllerHttpError', 409, 'thread_busy');
  await c.release(gate, secondGeneration);

  for (const [caseName, id] of [['interrupt-idle', 'fixture-c3-value'], ['interrupt-missing', 'fixture-missing-controller']]) {
    if (c.returned(await c.call('controller.interrupt', caseName, [id], () => c.controller.interrupt(id))) !== 'no_run_in_flight') throw new RunnerError('unexpected_cancel_reduction');
  }
  const concealed = 'fixture-concealed-controller';
  value(c, await c.call('controller.dispatch', 'concealed-establish', [concealed, 'ordinary'], () => c.controller.dispatch(concealed, 'ordinary')));
  if (!(await c.inspect(concealed)).found) throw new RunnerError('missing_concealed_thread');
  if (c.returned(await c.call('controller.interrupt', 'interrupt-concealed', [concealed], () => c.controller.interrupt(concealed))) !== 'no_run_in_flight') throw new RunnerError('unexpected_cancel_reduction');
}
