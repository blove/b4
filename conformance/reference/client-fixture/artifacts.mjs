// Private, offline artifact inspection. Never publishes raw evidence.
import { constants } from 'node:fs';
import { lstat, readdir, open } from 'node:fs/promises';
import { isAbsolute, normalize, join } from 'node:path';

const LIMITS = Object.freeze({ native: 24 * 1024 * 1024, client: 8 * 1024 * 1024 });
const NATIVE_FILE = /^(?:conn-[0-9]{4}\.(?:read|write)\.bin|control-[0-9]{4}-(?:request|reply)\.json|(?:snapshot|reopened)-[0-9]{4}-(?:thread|approval)\.json|stderr\.bin|native-proof\.json|capture-manifest\.json|quota-ledger\.json)$/;
const CLIENT_FILE = /^(?:client-observations|quota-ledger)\.json$/;
const fail = (s) => { throw new Error(`Private evidence: ${s}`); };
const integer = (n, cap) => Number.isSafeInteger(n) && n >= 0 && n <= cap;

function keys(value, names) {
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
    Object.keys(value).length !== names.length || Object.keys(value).some((k) => !names.includes(k))) fail('unknown or missing fields');
}
export function readPrivateJson(files, name) {
  const body = files.get(name);
  if (!Buffer.isBuffer(body)) fail('missing required file');
  if (body.length > 8 * 1024 * 1024) fail('JSON byte limit');
  let text; let value;
  try { text = new TextDecoder('utf-8', { fatal: true }).decode(body); value = JSON.parse(text); }
  catch { fail('invalid JSON artifact'); }
  // JSON.parse validates syntax; this separate scan verifies decoded key uniqueness
  // before any parsed field is used. Escaped equivalent names share the same Set.
  const stack = []; let keysSeen = 0;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (c === '{' || c === '[') {
      if (stack.length >= 64) fail('JSON depth limit');
      stack.push(c === '{' ? { keys: new Set(), expectingKey: true } : null);
    } else if (c === '}' || c === ']') stack.pop();
    else if (c === ',' && stack.at(-1)) stack.at(-1).expectingKey = true;
    else if (c === '"') {
      const start = i++;
      for (; i < text.length; i++) {
        if (text[i] === '\\') i++;
        else if (text[i] === '"') break;
      }
      const frame = stack.at(-1);
      if (frame?.expectingKey) {
        const key = JSON.parse(text.slice(start, i + 1));
        if (frame.keys.has(key)) fail('duplicate decoded JSON field');
        if (++keysSeen > 16384) fail('JSON key count limit');
        frame.keys.add(key); frame.expectingKey = false;
      }
    }
  }
  return value;
}
function mode(stat, expected) {
  if ((stat.mode & 0o7777) !== expected || (typeof process.getuid === 'function' && stat.uid !== process.getuid())) fail('private mode or owner mismatch');
}
function same(a, b) { return a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeMs === b.mtimeMs && a.ctimeMs === b.ctimeMs; }

export async function loadEvidenceRole(directory, role) {
  if (!Object.hasOwn(LIMITS, role)) fail('unknown role');
  if (typeof directory !== 'string' || !isAbsolute(directory) || normalize(directory) !== directory) fail('invalid directory');
  const before = await lstat(directory);
  if (!before.isDirectory() || before.isSymbolicLink()) fail('not a private directory');
  mode(before, 0o700);
  const entries = await readdir(directory, { withFileTypes: true });
  if (entries.length > (role === 'native' ? 2048 : 2)) fail('file count limit');
  const pattern = role === 'native' ? NATIVE_FILE : CLIENT_FILE;
  const files = new Map(); let usedBytes = 0;
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    if (!entry.isFile() || entry.isSymbolicLink()) fail('non-regular artifact');
    if (!pattern.test(entry.name)) fail('unexpected filename');
    const path = join(directory, entry.name);
    const witness = await lstat(path);
    if (!witness.isFile() || witness.isSymbolicLink()) fail('non-regular artifact');
    mode(witness, 0o600);
    if (!integer(witness.size, LIMITS[role] - usedBytes)) fail('role byte limit');
    const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const actual = await file.stat();
      mode(actual, 0o600);
      if (!actual.isFile() || !same(witness, actual)) fail('artifact changed before read');
      const body = await file.readFile();
      if (body.length !== actual.size || !same(actual, await file.stat())) fail('artifact changed during read');
      usedBytes += body.length; files.set(entry.name, body);
    } finally { await file.close(); }
  }
  if (!same(before, await lstat(directory))) fail('directory changed during read');
  const ledger = readPrivateJson(files, 'quota-ledger.json');
  keys(ledger, ['version', 'limit_bytes', 'used_bytes']);
  if (ledger.version !== 1 || ledger.limit_bytes !== LIMITS[role] || ledger.used_bytes !== usedBytes) fail('self-inclusive ledger mismatch');
  return { role, directory, files, usedBytes, limitBytes: LIMITS[role] };
}

export function loadCapture(nativeRole) {
  if (nativeRole?.role !== 'native' || !(nativeRole.files instanceof Map)) fail('wrong capture role');
  const manifest = readPrivateJson(nativeRole.files, 'capture-manifest.json');
  keys(manifest, ['version', 'accepted', 'retired', 'sticky_loss', 'connections']);
  if (manifest.version !== 1 || !integer(manifest.accepted, 16) || manifest.accepted < 1 || manifest.retired !== manifest.accepted ||
    manifest.sticky_loss !== false || !Array.isArray(manifest.connections) || manifest.connections.length !== manifest.accepted) fail('capture retirement or loss');
  const connectionFiles = [...nativeRole.files.keys()].filter((name) => /^conn-/.test(name));
  if (connectionFiles.length !== manifest.accepted * 2) fail('capture file cardinality');
  const captures = manifest.connections.map((record, i) => {
    keys(record, ['ordinal', 'read_file', 'write_file', 'read_bytes', 'write_bytes', 'read_eof', 'write_shutdown']);
    const suffix = String(i + 1).padStart(4, '0');
    if (record.ordinal !== i + 1 || record.read_file !== `conn-${suffix}.read.bin` || record.write_file !== `conn-${suffix}.write.bin`) fail('capture ordinal/filename mismatch');
    if (!integer(record.read_bytes, 262144) || !integer(record.write_bytes, 262144) || typeof record.read_eof !== 'boolean' || typeof record.write_shutdown !== 'boolean') fail('capture byte/flag limits');
    const read = nativeRole.files.get(record.read_file); const write = nativeRole.files.get(record.write_file);
    if (!read || !write || read.length !== record.read_bytes || write.length !== record.write_bytes) fail('capture actual byte mismatch');
    return { ordinal: record.ordinal, read, write, read_eof: record.read_eof, write_shutdown: record.write_shutdown };
  });
  return captures;
}
