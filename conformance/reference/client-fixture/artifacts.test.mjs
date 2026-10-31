import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, chmod, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadEvidenceRole, loadCapture, readPrivateJson } from './artifacts.mjs';

const LIMITS = { native: 24 * 1024 * 1024, client: 8 * 1024 * 1024 };
async function fixture(t, role = 'native', extra = {}) {
  const root = await mkdtemp(join(tmpdir(), 'b4-client-artifact-unit-')); await chmod(root, 0o700);
  t.after(() => rm(root, { recursive: true, force: true }));
  const dir = join(root, role); await mkdir(dir, { mode: 0o700 });
  const read = Buffer.from('GET /threads/a HTTP/1.1\r\nHost: localhost\r\n\r\n');
  const write = Buffer.from('HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}');
  const files = role === 'native' ? {
    'conn-0001.read.bin': read, 'conn-0001.write.bin': write,
    'capture-manifest.json': Buffer.from(JSON.stringify({ version: 1, accepted: 1, retired: 1, sticky_loss: false,
      connections: [{ ordinal: 1, read_file: 'conn-0001.read.bin', write_file: 'conn-0001.write.bin', read_bytes: read.length, write_bytes: write.length, read_eof: true, write_shutdown: true }] })),
    ...extra,
  } : { 'client-observations.json': Buffer.from('{"version":1}'), ...extra };
  for (const [name, data] of Object.entries(files)) await writeFile(join(dir, name), data, { mode: 0o600, flag: 'wx' });
  let used = Object.values(files).reduce((n, b) => n + b.length, 0); let ledger;
  for (;;) {
    ledger = Buffer.from(JSON.stringify({ version: 1, limit_bytes: LIMITS[role], used_bytes: used }));
    const actual = Object.values(files).reduce((n, b) => n + b.length, 0) + ledger.length;
    if (actual === used) break; used = actual;
  }
  await writeFile(join(dir, 'quota-ledger.json'), ledger, { mode: 0o600, flag: 'wx' });
  return { dir, files, used };
}

test('reads actual bounded private files, verifies self-inclusive ledger and complete connection cardinality', async (t) => {
  const f = await fixture(t);
  const role = await loadEvidenceRole(f.dir, 'native');
  assert.equal(role.usedBytes, f.used);
  assert.equal(role.limitBytes, LIMITS.native);
  const captures = loadCapture(role);
  assert.equal(captures.length, 1);
  assert.deepEqual(captures[0].read, f.files['conn-0001.read.bin']);
  assert.equal(captures[0].ordinal, 1);
});

test('refuses wrong privacy modes, symlinks, nesting and mismatched actual byte ledgers', async (t) => {
  const f = await fixture(t);
  await chmod(join(f.dir, 'conn-0001.read.bin'), 0o644);
  await assert.rejects(loadEvidenceRole(f.dir, 'native'), /mode/);
  await chmod(join(f.dir, 'conn-0001.read.bin'), 0o600);
  await mkdir(join(f.dir, 'nested'), { mode: 0o700 });
  await assert.rejects(loadEvidenceRole(f.dir, 'native'), /regular/);
  await rm(join(f.dir, 'nested'), { recursive: true });
  await symlink(join(f.dir, 'conn-0001.read.bin'), join(f.dir, 'stderr.bin'));
  await assert.rejects(loadEvidenceRole(f.dir, 'native'), /regular/);
  await rm(join(f.dir, 'stderr.bin'));
  const ledger = JSON.parse(await readFile(join(f.dir, 'quota-ledger.json'), 'utf8')); ledger.used_bytes--;
  await writeFile(join(f.dir, 'quota-ledger.json'), JSON.stringify(ledger));
  await assert.rejects(loadEvidenceRole(f.dir, 'native'), /ledger/);
});

test('refuses slot/byte loss, misnamed connection files, mismatched cardinality and unknown manifest fields', async (t) => {
  const f = await fixture(t); const original = JSON.parse(f.files['capture-manifest.json']);
  for (const change of [
    (d) => { d.sticky_loss = true; }, (d) => { d.retired = 0; }, (d) => { d.accepted = 17; },
    (d) => { d.connections[0].read_bytes++; }, (d) => { d.connections[0].read_file = '../private'; },
    (d) => { d.connections[0].ordinal = 2; }, (d) => { d.connections[0].grant = 'private'; },
    (d) => { d.unknown = true; },
  ]) {
    const d = structuredClone(original); change(d);
    const role = { role: 'native', files: new Map(Object.entries({ ...f.files, 'capture-manifest.json': Buffer.from(JSON.stringify(d)) })) };
    assert.throws(() => loadCapture(role));
  }
});

test('keeps disjoint role ceilings and refuses unexpected private filenames', async (t) => {
  const f = await fixture(t, 'client');
  const role = await loadEvidenceRole(f.dir, 'client'); assert.equal(role.limitBytes, LIMITS.client);
  await assert.rejects(loadEvidenceRole(f.dir, 'native'), /filename|ledger/);
  await assert.rejects(loadEvidenceRole(f.dir, 'foreign'), /role/);
  const n = await fixture(t, 'native', { 'extra-secret.json': Buffer.from('{}') });
  await assert.rejects(loadEvidenceRole(n.dir, 'native'), /filename/);
});

test('refuses duplicate decoded JSON keys including escaped names and nested connection flags', async (t) => {
  const f = await fixture(t); const original = f.files['capture-manifest.json'].toString();
  for (const altered of [
    original.replace('"sticky_loss":false', '"sticky_loss":true,"sticky_loss":false'),
    original.replace('"sticky_loss":false', '"sticky_loss":true,"\\u0073ticky_loss":false'),
    original.replace('"read_eof":true', '"read_eof":false,"read_eof":true'),
  ]) {
    const role = { role: 'native', files: new Map(Object.entries({ ...f.files, 'capture-manifest.json': Buffer.from(altered) })) };
    assert.throws(() => loadCapture(role), /duplicate/);
  }
});

test('JSON uniqueness scanning preserves nested values and bounds malformed UTF8, bytes and depth', () => {
  const value = { comma: ',:{}[]', quoted: 'a"b\\c', array: [{ id: 1 }, { id: 2 }, ['comma', 42, true, null]], nested: { id: { id: 3 } } };
  const files = new Map([['private.json', Buffer.from(JSON.stringify(value))]]);
  assert.deepEqual(readPrivateJson(files, 'private.json'), value);
  for (const [body, expected] of [[Buffer.from([0xff]), /invalid/], [Buffer.alloc(8 * 1024 * 1024 + 1), /limit/],
    [Buffer.from('['.repeat(65) + '0' + ']'.repeat(65)), /depth/], [Buffer.from('{"x":1'), /invalid/]]) {
    assert.throws(() => readPrivateJson(new Map([['private.json', body]]), 'private.json'), expected);
  }
});
