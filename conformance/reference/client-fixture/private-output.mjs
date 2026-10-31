export const CLIENT_EVIDENCE_BYTES = 8 * 1024 * 1024;

export class PrivateOutputError extends Error {
  constructor(code) { super(code); this.name = 'PrivateOutputError'; this.code = code; }
}

export class PrivateWriter {
  constructor(directory, { limitBytes = CLIENT_EVIDENCE_BYTES } = {}) {
    if (!isAbsolute(directory) || directory.includes('\0') || !Number.isSafeInteger(limitBytes)
      || limitBytes < 1 || limitBytes > CLIENT_EVIDENCE_BYTES) throw new PrivateOutputError('invalid_output');
    this.directory = directory; this.limitBytes = limitBytes; this.reservedBytes = 0;
    this.initialized = false; this.failed = false; this.finished = false; this.files = new Map();
  }
  async initialize() {
    if (this.initialized) return;
    const info = await lstat(this.directory);
    if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o777) !== 0o700
      || info.uid !== process.getuid() || (await readdir(this.directory)).length) throw new PrivateOutputError('invalid_output');
    this.directory = await realpath(this.directory); this.initialized = true;
  }
  async writeBytes(name, bytes) {
    if (this.failed || this.finished || !/^[a-z][a-z0-9-]*\.(?:json|bin)$/.test(name) || !Buffer.isBuffer(bytes)) throw new PrivateOutputError('invalid_output');
    await this.initialize();
    if (bytes.length > this.limitBytes - this.reservedBytes) {
      this.failed = true; throw new PrivateOutputError('evidence_limit');
    }
    this.reservedBytes += bytes.length;
    let handle;
    try {
      handle = await open(join(this.directory, name), constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
      const info = await handle.stat();
      if (!info.isFile() || (info.mode & 0o777) !== 0o600 || info.uid !== process.getuid()) throw new Error('mode');
      await handle.writeFile(bytes);
      await handle.close(); handle = undefined;
      this.files.set(name, bytes.length);
    } catch {
      this.failed = true;
      if (handle) await handle.close().catch(() => {});
      throw new PrivateOutputError('write_failed');
    }
  }
  async writeJson(name, value) {
    const encoded = JSON.stringify(value);
    if (typeof encoded !== 'string') throw new PrivateOutputError('invalid_output');
    await this.writeBytes(name, Buffer.from(`${encoded}\n`));
  }
  async finalize() {
    let total = this.reservedBytes;
    for (let attempt = 0; attempt < 16; attempt++) {
      const bytes = Buffer.from(`${JSON.stringify({ version: 1, limit_bytes: this.limitBytes, used_bytes: total })}\n`);
      const measured = this.reservedBytes + bytes.length;
      if (measured === total) {
        await this.writeBytes('quota-ledger.json', bytes);
        this.finished = true;
        return this.verify();
      }
      total = measured;
    }
    throw new PrivateOutputError('invalid_output');
  }
  async verify() {
    if (this.failed) throw new PrivateOutputError('write_failed');
    const names = await readdir(this.directory);
    if (names.length !== this.files.size) throw new PrivateOutputError('invalid_output');
    let actual = 0;
    for (const name of names) {
      const info = await lstat(join(this.directory, name));
      if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o777) !== 0o600
        || info.uid !== process.getuid() || info.size !== this.files.get(name)) throw new PrivateOutputError('invalid_output');
      actual += info.size;
    }
    if (actual !== this.reservedBytes || actual > this.limitBytes) throw new PrivateOutputError('invalid_output');
    return { reserved_bytes: this.reservedBytes, persisted_bytes: actual };
  }
}
import { constants } from 'node:fs';
import { lstat, open, readdir, realpath } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
