import { RunnerError } from './runner-context.mjs';

async function create(c, caseName) {
  const metadata = { fixture_case: caseName };
  const id = c.returned(await c.call('worker.createThread', caseName, [metadata], () => c.worker.createThread(metadata)));
  if (typeof id !== 'string' || !id.length) throw new RunnerError('unexpected_thread_id');
  return id;
}

function done(stream) {
  const last = stream.frames.at(-1);
  if (!stream.eof || last?.event !== 'done') throw new RunnerError('missing_terminal_eof');
  return last.data;
}

export async function W1_worker_crud_auth(c) {
  const id = await create(c, 'worker-create');
  const thread = c.returned(await c.call('worker.getThread', 'worker-get', [id], () => c.worker.getThread(id)));
  if (thread?.threadId !== id || typeof thread.status !== 'string') throw new RunnerError('unexpected_thread_metadata');
  for (const [caseName, target] of [['worker-get-missing', 'fixture-missing-worker'], ['worker-get-concealed', 'fixture-concealed-controller']]) {
    if (c.returned(await c.call('worker.getThread', caseName, [target], () => c.worker.getThread(target))) !== null) throw new RunnerError('unexpected_concealed_read');
  }
  c.httpError(await c.call('worker.getThread', 'worker-get-wrong-token', [id], () => c.wrongWorker.getThread(id)), 'WorkerHttpError', 401, 'authentication_required');
  for (const caseName of ['worker-delete', 'worker-delete-missing']) {
    if (c.returned(await c.call('worker.deleteThread', caseName, [id], () => c.worker.deleteThread(id))) !== 'deleted') throw new RunnerError('unexpected_delete');
  }
  const heldId = await create(c, 'worker-held-create');
  const generation = await c.arm('worker_held');
  const controller = c.signal();
  const args = [heldId, c.ready.routes.worker_held, 'finite held input', controller.signal];
  const stream = await c.call('worker.startRun', 'worker-held-start', args, () => c.worker.startRun(...args));
  c.returned(stream);
  await c.entered('worker_held', generation);
  c.inFlight(await c.inspect(heldId));
  c.httpError(await c.call('worker.deleteThread', 'worker-delete-held', [heldId], () => c.worker.deleteThread(heldId)), 'WorkerHttpError', 409, 'run_in_flight');
  await c.release('worker_held', generation);
  done(await c.consume(stream));
  if (c.returned(await c.call('worker.deleteThread', 'worker-delete-released', [heldId], () => c.worker.deleteThread(heldId))) !== 'deleted') throw new RunnerError('unexpected_delete');
}

export async function W2_finite_sse(c) {
  for (const kind of ['value', 'void', 'failure']) {
    const id = await create(c, `${kind}-create`);
    const route = c.ready.routes[`worker_${kind}`];
    const args = [id, route, 'finite stream input'];
    const stream = await c.consume(await c.call('worker.startRun', `${kind}-start`, args, () => c.worker.startRun(...args)));
    const terminal = done(stream);
    if (kind === 'void' && (terminal === null || typeof terminal !== 'object' || Object.keys(terminal).length)) throw new RunnerError('unexpected_void');
    if (kind === 'failure' && typeof terminal?.output?.error !== 'string') throw new RunnerError('missing_known_failure');
    if (kind === 'value' && !Object.hasOwn(terminal, 'output')) throw new RunnerError('missing_value_output');
  }
}

export async function W3_attachment_cancel(c) {
  const gate = 'worker_held';
  const id = await create(c, 'attach-create');
  const firstGeneration = await c.arm(gate);
  const postController = c.signal();
  const args = [id, c.ready.routes.worker_held, 'finite attachment input', postController.signal];
  const post = await c.call('worker.startRun', 'attach-held-start', args, () => c.worker.startRun(...args));
  const iterator = c.returned(post)[Symbol.asyncIterator]();
  const pending = iterator.next();
  // Install an immediate rejection observer; abort settlement is awaited below.
  pending.catch(() => {});
  await c.entered(gate, firstGeneration);
  c.inFlight(await c.inspect(id));
  const viewerController = c.signal();
  const attach = await c.call('worker.reattach', 'attach-live', [id, viewerController.signal], () => c.worker.reattach(id, viewerController.signal));
  await c.cutFirst(attach);
  await c.held(gate, firstGeneration);
  c.httpError(await c.call('worker.startRun', 'attach-busy-after-viewer-cut', [id, c.ready.routes.worker_value, 'busy'], () => c.worker.startRun(id, c.ready.routes.worker_value, 'busy')), 'WorkerHttpError', 409, 'thread_busy');
  await c.abortStream(post, iterator, pending, postController);
  await c.held(gate, firstGeneration);
  c.httpError(await c.call('worker.startRun', 'attach-busy-after-post-abort', [id, c.ready.routes.worker_value, 'busy'], () => c.worker.startRun(id, c.ready.routes.worker_value, 'busy')), 'WorkerHttpError', 409, 'thread_busy');
  await c.release(gate, firstGeneration);

  const cancelledId = await create(c, 'attach-cancel-create');
  const secondGeneration = await c.arm(gate);
  const cancelController = c.signal();
  const cancelledArgs = [cancelledId, c.ready.routes.worker_held, 'finite cancelled input', cancelController.signal];
  const cancelledPost = await c.call('worker.startRun', 'attach-cancel-held-start', cancelledArgs, () => c.worker.startRun(...cancelledArgs));
  await c.entered(gate, secondGeneration);
  c.inFlight(await c.inspect(cancelledId));
  if (c.returned(await c.call('worker.cancel', 'worker-cancel-held', [cancelledId], () => c.worker.cancel(cancelledId))) !== 'interrupted') throw new RunnerError('unexpected_cancel');
  const terminal = done(await c.consume(cancelledPost));
  if (terminal?.output?.cancelled !== true) throw new RunnerError('missing_cancelled_terminal');
  await c.held(gate, secondGeneration);
  c.httpError(await c.call('worker.startRun', 'attach-busy-after-cancel', [cancelledId, c.ready.routes.worker_value, 'busy'], () => c.worker.startRun(cancelledId, c.ready.routes.worker_value, 'busy')), 'WorkerHttpError', 409, 'thread_busy');
  await c.release(gate, secondGeneration);
  const settled = await c.call('worker.reattach', 'attach-settled', [cancelledId], () => c.worker.reattach(cancelledId));
  const settledFrames = await c.consume(settled);
  if (settledFrames.frames[0]?.event !== 'state') throw new RunnerError('missing_state_frame');
  done(settledFrames);
  for (const [caseName, target, expected] of [['worker-cancel-idle', cancelledId, 'no_run_in_flight'],
    ['worker-cancel-missing', 'fixture-missing-worker', 'thread_not_found'],
    ['worker-cancel-concealed', 'fixture-concealed-controller', 'thread_not_found']]) {
    if (c.returned(await c.call('worker.cancel', caseName, [target], () => c.worker.cancel(target))) !== expected) throw new RunnerError('unexpected_cancel_reduction');
  }
}

export async function W4_durable_park_refusal(c) {
  const id = await create(c, 'park-create');
  const empty = c.returned(await c.call('worker.pendingInterrupts', 'pending-empty', [id], () => c.worker.pendingInterrupts(id)));
  if (!Array.isArray(empty) || empty.length) throw new RunnerError('unexpected_pending_empty');
  const args = [id, c.ready.routes.worker_park, 'finite approval input'];
  const parked = await c.consume(await c.call('worker.startRun', 'park-start', args, () => c.worker.startRun(...args)));
  if (!parked.frames.some((frame) => frame.event === 'interrupt')) throw new RunnerError('missing_native_interrupt');
  done(parked);
  const schemaRefusal = await c.call('worker.pendingInterrupts', 'pending-original-schema-refusal', [id], () => c.worker.pendingInterrupts(id));
  if (schemaRefusal.error?.name !== 'ZodError') throw new RunnerError('missing_original_schema_refusal');
  const before = await c.inspect(id, true);
  if (before.execution_status !== 'waiting_for_approval' || before.interrupt_ids.length !== 1) throw new RunnerError('missing_stable_park');
  for (const payload of ['once', 'deny']) {
    const resolutions = [{ interruptId: before.interrupt_ids[0], payload }];
    const result = await c.call('worker.resume', `resume-forced-${payload}`, [id, c.ready.routes.worker_park, resolutions], () => c.worker.resume(id, c.ready.routes.worker_park, resolutions));
    c.httpError(result, 'WorkerHttpError', 400, 'grant_required');
    if (JSON.stringify(await c.inspect(id, true)) !== JSON.stringify(before)) throw new RunnerError('changed_park_metadata');
  }
  c.httpError(await c.call('worker.resume', 'resume-empty', [id, c.ready.routes.worker_park, []], () => c.worker.resume(id, c.ready.routes.worker_park, [])), 'WorkerHttpError', 400, 'invalid_request');
  if (JSON.stringify(await c.inspect(id, true)) !== JSON.stringify(before)) throw new RunnerError('changed_park_metadata');
  c.witness('forced-id-only-no-grant-and-metadata-unchanged', true);
}
