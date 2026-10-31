import { constants } from 'node:fs';
import { lstat, open, readFile, realpath } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { isAbsolute, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { ControlClient } from './control.mjs';
import { PrivateWriter } from './private-output.mjs';
import { CorpusContext, RunnerError } from './runner-context.mjs';
import { C1_factory_method_map, C2_controller_auth, C3_wait_terminals, C4_held_controller } from './controller-cases.mjs';
import { W1_worker_crud_auth, W2_finite_sse, W3_attachment_cancel, W4_durable_park_refusal } from './worker-cases.mjs';

export async function runGroups(context) {
  for (const group of [C1_factory_method_map, C2_controller_auth, C3_wait_terminals, C4_held_controller,
    W1_worker_crud_auth, W2_finite_sse, W3_attachment_cancel, W4_durable_park_refusal]) {
    context.beginGroup(group.name.slice(0, 2), group.name);
    await group(context);
  }
  return context.observations();
}

export async function verifyBundleManifest(directory) {
  try {
    if (!isAbsolute(directory) || directory.includes('\0')) throw new RunnerError('invalid_bundle');
    const info = await lstat(directory);
    if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o777) !== 0o700 || info.uid !== process.getuid()) throw new RunnerError('invalid_bundle');
    directory = await realpath(directory);
    const manifestPath = join(directory, 'bundle-manifest.json');
    const manifestInfo = await lstat(manifestPath);
    if (!manifestInfo.isFile() || manifestInfo.isSymbolicLink() || manifestInfo.size > 1024 * 1024
      || (manifestInfo.mode & 0o777) !== 0o600 || manifestInfo.uid !== process.getuid()) throw new RunnerError('invalid_bundle');
    const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
    if (manifest.version !== 1 || manifest.provenance !== 'original_pinned_client'
      || manifest.source?.commit !== '8acac534ba6eb960b34de217b0da45b8405023af' || !Array.isArray(manifest.outputs)
      || manifest.outputs.length !== 2) throw new RunnerError('invalid_bundle');
    const entries = new Map();
    for (const output of manifest.outputs) {
      if (!['controller', 'worker'].includes(output.entry) || output.file !== `${output.entry}-client.mjs`
        || entries.has(output.entry) || !/^[a-f0-9]{64}$/.test(output.sha256) || !Number.isSafeInteger(output.bytes)
        || output.bytes < 1) throw new RunnerError('invalid_bundle');
      const file = join(directory, output.file);
      const handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        const source = await handle.stat();
        if (!source.isFile() || source.size !== output.bytes || (source.mode & 0o777) !== 0o600 || source.uid !== process.getuid()) throw new RunnerError('invalid_bundle');
        const hash = createHash('sha256');
        const chunk = Buffer.alloc(65536);
        let total = 0;
        while (true) {
          const { bytesRead } = await handle.read(chunk, 0, chunk.length, null);
          if (!bytesRead) break;
          total += bytesRead; if (total > output.bytes) throw new RunnerError('invalid_bundle');
          hash.update(chunk.subarray(0, bytesRead));
        }
        if (total !== output.bytes || hash.digest('hex') !== output.sha256) throw new RunnerError('invalid_bundle');
      } finally { await handle.close(); }
      entries.set(output.entry, pathToFileURL(file).href);
    }
    return { manifest, controllerURL: entries.get('controller'), workerURL: entries.get('worker') };
  } catch { throw new RunnerError('invalid_bundle'); }
}

export async function runOriginalClientCorpus(bundleDir, outputDir) {
  const admitted = await verifyBundleManifest(bundleDir);
  const writer = new PrivateWriter(outputDir);
  await writer.initialize();
  const control = new ControlClient(process.stdin, process.stdout);
  let context;
  try {
    const ready = await control.request('setup', { ready: {} });
    // These are the reviewed fresh original-client compiler outputs, not rewrites.
    // Unit tests never call this loader or import the pinned original factories.
    const controller = await import(admitted.controllerURL);
    const worker = await import(admitted.workerURL);
    if (typeof controller.createControllerClient !== 'function' || typeof controller.ControllerHttpError !== 'function'
      || typeof worker.createHttpWorkerClient !== 'function' || typeof worker.WorkerHttpError !== 'function') throw new RunnerError('invalid_bundle');
    context = new CorpusContext({ ready, control });
    context.controller = controller.createControllerClient(ready.base_url, context.fetch('controller'));
    context.directController = controller.createControllerClient(ready.base_url);
    context.worker = worker.createHttpWorkerClient(ready.base_url, { token: ready.token, fetch: context.fetch('worker') });
    context.wrongWorker = worker.createHttpWorkerClient(ready.base_url, { token: 'fixture-invalid-token', fetch: context.fetch('worker') });
    context.errorClasses = { ControllerHttpError: controller.ControllerHttpError, WorkerHttpError: worker.WorkerHttpError };
    const observations = await runGroups(context);
    await verifyBundleManifest(bundleDir);
    await writer.writeJson('client-observations.json', observations);
    await writer.finalize();
    await control.request('retirement', { final_stop: {} });
    return observations;
  } finally {
    context?.cleanup();
    await control.dispose();
    await new Promise((done) => process.stdout.end(done));
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    if (process.argv.length !== 4) throw new RunnerError('invalid_arguments');
    await runOriginalClientCorpus(process.argv[2], process.argv[3]);
  } catch {
    process.stderr.write('client_runner_failed\n');
    process.exitCode = 1;
  }
}
