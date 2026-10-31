import test from 'node:test';
import assert from 'node:assert/strict';
import { readdir, readFile, lstat, chmod, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { qualifyClientArtifacts } from './qualify.mjs';
import { auditNativeArtifacts } from './native-audit.mjs';
import { makeQualificationUnitFixture, writeQualificationUnitFixture } from './qualification-unit-fixture.mjs';

async function inventory(directory) {
  const records = [];
  for (const name of (await readdir(directory)).sort()) {
    const path = join(directory, name); const stat = await lstat(path);
    records.push({ name, mode: stat.mode & 0o7777, bytes: stat.size,
      sha256: createHash('sha256').update(await readFile(path)).digest('hex') });
  }
  return records;
}

test('complete synthetic native history and traffic are consistent before composer admission', () => {
  const f = makeQualificationUnitFixture();
  assert.equal(f.rawClient.groups.flatMap((g) => g.observations).length, 63);
  assert.equal(f.rawClient.transports.length, 62);
  assert.equal(auditNativeArtifacts(f.native, f.client, f.rawClient).retirement.accepted, 6);
  const snapshots = [...f.native.files].filter(([name]) => /^snapshot-.*-thread\.json$/.test(name))
    .map(([, body]) => JSON.parse(body));
  const concealed = snapshots.find((v) => v?.thread.id === 'fixture-concealed-controller');
  const returned = f.rawClient.groups[3].observations.find((r) => r.case === 'concealed-establish').outcome.value;
  assert.deepEqual(concealed.execution.snapshot.tasks[0].result.output, returned);
  const held = snapshots.find((v) => v?.thread.id === 'synthetic-worker-attachment');
  const state = f.rawClient.groups[6].observations.find((r) => r.case === 'attach-live').outcome.frames[0].data;
  assert.equal(held.execution.snapshot.execution_id, state.anchor);
  assert.deepEqual(held.execution.snapshot.values, state.values);
});

test('qualifies all eight synthetic groups into safe facts without changing private files', async (t) => {
  const f = await writeQualificationUnitFixture(t);
  const before = await Promise.all([inventory(f.nativeDir), inventory(f.clientDir)]);
  const result = await qualifyClientArtifacts(f.nativeDir, f.clientDir);
  assert.equal(result.kind, 'unchanged-ap-client-native-summary');
  assert.equal(result.invocation_count, 63);
  assert.equal(result.selected_method_count, 19);
  assert.equal(result.core_operation_count, 9);
  assert.equal(result.groups.length, 8);
  assert.equal(result.retirement.accepted, 6);
  assert.equal(result.retirement.retired, 6);
  assert.equal(result.gates.CP010, 'unresolved');
  assert.equal(result.gates.core_profile, 'unfrozen');
  assert.equal(result.groups[3].observations[0].cut, 'signal-abort');
  assert.equal(result.groups[6].observations[2].cut, 'viewer-return');
  const encoded = JSON.stringify(result);
  for (const privateValue of [f.ready.token, `b4ag_${'e'.repeat(64)}`, 'd'.repeat(64),
    'synthetic-private-interrupt', 'synthetic-worker-held', f.ready.base_url]) assert.equal(encoded.includes(privateValue), false);
  assert.deepEqual(await Promise.all([inventory(f.nativeDir), inventory(f.clientDir)]), before);
});

test('missing native files refuse before a summary can be returned', async (t) => {
  const f = await writeQualificationUnitFixture(t);
  await unlink(join(f.nativeDir, 'native-proof.json'));
  await assert.rejects(qualifyClientArtifacts(f.nativeDir, f.clientDir));
});

test('missing client observations refuse before a summary can be returned', async (t) => {
  const f = await writeQualificationUnitFixture(t);
  await unlink(join(f.clientDir, 'client-observations.json'));
  await assert.rejects(qualifyClientArtifacts(f.nativeDir, f.clientDir));
});

test('physical private modes are checked before artifact facts are used', async (t) => {
  const f = await writeQualificationUnitFixture(t);
  await chmod(join(f.nativeDir, 'native-proof.json'), 0o644);
  await assert.rejects(qualifyClientArtifacts(f.nativeDir, f.clientDir));
});

test('complete traffic cannot publish when native retirement is incomplete', async (t) => {
  const fixture = makeQualificationUnitFixture(); fixture.proof.child.exit_code = 1; fixture.refresh();
  const f = await writeQualificationUnitFixture(t, fixture);
  await assert.rejects(qualifyClientArtifacts(f.nativeDir, f.clientDir));
});

test('native retirement cannot publish when captured response values disagree with caller results', async (t) => {
  const fixture = makeQualificationUnitFixture();
  const name = 'conn-0001.write.bin'; const body = fixture.native.files.get(name);
  const changed = Buffer.from(body.toString().replace('fixture-value', 'fixture-other'));
  assert.notDeepEqual(changed, body); assert.equal(changed.length, body.length);
  fixture.native.files.set(name, changed); fixture.refresh();
  assert.equal(auditNativeArtifacts(fixture.native, fixture.client, fixture.rawClient).retirement.accepted, 6);
  const f = await writeQualificationUnitFixture(t, fixture);
  await assert.rejects(qualifyClientArtifacts(f.nativeDir, f.clientDir));
});

test('duplicate decoded client fields refuse even with matching actual quota ledgers', async (t) => {
  const fixture = makeQualificationUnitFixture();
  const raw = JSON.stringify(fixture.rawClient).replace('"version":1', '"version":2,"\\u0076ersion":1');
  fixture.refresh(Buffer.from(`${raw}\n`));
  const f = await writeQualificationUnitFixture(t, fixture);
  await assert.rejects(qualifyClientArtifacts(f.nativeDir, f.clientDir));
});

test('malformed client JSON refuses even with matching actual quota ledgers', async (t) => {
  const fixture = makeQualificationUnitFixture(); fixture.refresh(Buffer.from('{'));
  const f = await writeQualificationUnitFixture(t, fixture);
  await assert.rejects(qualifyClientArtifacts(f.nativeDir, f.clientDir));
});

test('unknown raw authority fields refuse instead of reaching public projection', async (t) => {
  const fixture = makeQualificationUnitFixture();
  fixture.rawClient.groups[0].observations[0].token = 'private-extra-authority'; fixture.refresh();
  const f = await writeQualificationUnitFixture(t, fixture);
  await assert.rejects(qualifyClientArtifacts(f.nativeDir, f.clientDir));
});
