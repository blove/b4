export const CONTROL_LINE_BYTES = 8192;
export const CONTROL_COMMANDS = 256;

export class ControlError extends Error {
  constructor(code) { super(code); this.name = 'ControlError'; this.code = code; }
}

export function encodeCommand(ordinal, caseName, command) {
  if (!Number.isSafeInteger(ordinal) || ordinal < 1 || ordinal > CONTROL_COMMANDS
    || !['setup', 'C1', 'C2', 'C3', 'C4', 'W1', 'W2', 'W3', 'W4', 'retirement'].includes(caseName)) {
    throw new ControlError('invalid_command');
  }
  const keys = command && typeof command === 'object' ? Object.keys(command) : [];
  if (keys.length !== 1) throw new ControlError('invalid_command');
  const operation = keys[0];
  const payload = command[operation];
  const fields = { ready: [], arm: ['gate'], await_entered: ['gate', 'generation'],
    inspect_thread: ['thread_id'], inspect_pending: ['thread_id'],
    gate_status: ['gate', 'generation'], release: ['gate', 'generation'], final_stop: [] }[operation];
  if (!fields || !exactKeys(payload, fields)) throw new ControlError('invalid_command');
  if ('gate' in payload && !['controller_dispatch', 'worker_held'].includes(payload.gate)) throw new ControlError('invalid_command');
  if ('generation' in payload && (!Number.isSafeInteger(payload.generation) || payload.generation < 1)) throw new ControlError('invalid_command');
  if ('thread_id' in payload && (typeof payload.thread_id !== 'string' || !payload.thread_id.length || payload.thread_id.includes('\0'))) throw new ControlError('invalid_command');
  if ((operation === 'ready' && caseName !== 'setup') || (operation === 'final_stop' && caseName !== 'retirement')) throw new ControlError('invalid_command');
  const bytes = Buffer.from(`${JSON.stringify({ version: 1, ordinal, case: caseName, command })}\n`);
  if (bytes.length > CONTROL_LINE_BYTES) throw new ControlError('line_limit');
  return bytes;
}

export async function* controlLines(readable) {
  const line = Buffer.alloc(CONTROL_LINE_BYTES);
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let filled = 0;
  let count = 0;
  for await (const chunk of readable) {
    if (!Buffer.isBuffer(chunk)) throw new ControlError('invalid_chunk');
    for (const byte of chunk) {
      if (byte === 10) {
        if (++count > CONTROL_COMMANDS) throw new ControlError('command_limit');
        let value;
        try { value = decoder.decode(line.subarray(0, filled)); }
        catch { throw new ControlError('invalid_utf8'); }
        filled = 0;
        yield value;
      } else {
        if (filled >= CONTROL_LINE_BYTES - 1) throw new ControlError('line_limit');
        line[filled++] = byte;
      }
    }
  }
  if (filled) throw new ControlError('partial_line');
}

export class ControlClient {
  constructor(input, output, { replyTimeoutMs = 10000 } = {}) {
    if (!Number.isSafeInteger(replyTimeoutMs) || replyTimeoutMs < 1 || replyTimeoutMs > 10000) throw new ControlError('invalid_deadline');
    this.input = input; this.output = output;
    this.lines = controlLines(input)[Symbol.asyncIterator]();
    this.ordinal = 0; this.busy = false; this.closed = false;
    this.replyTimeoutMs = replyTimeoutMs;
  }
  async request(caseName, command) {
    if (this.busy || this.closed) throw new ControlError('control_unavailable');
    const ordinal = this.ordinal + 1;
    const bytes = encodeCommand(ordinal, caseName, command);
    this.ordinal = ordinal; this.busy = true;
    const deadline = performance.now() + this.replyTimeoutMs;
    let timer;
    try {
      const receive = async () => {
        await new Promise((resolve, reject) => {
          this.output.write(bytes, (error) => error ? reject(new ControlError('write_failed')) : resolve());
        });
        if (performance.now() >= deadline) throw new ControlError('control_timeout');
        return this.lines.next();
      };
      const next = await Promise.race([receive(), new Promise((resolve, reject) => {
        timer = setTimeout(() => reject(new ControlError('control_timeout')), this.replyTimeoutMs);
      })]);
      // The timer is cooperative; a delayed event loop cannot turn a late reply into success.
      if (performance.now() >= deadline) throw new ControlError('control_timeout');
      if (next.done) throw new ControlError('unexpected_eof');
      let reply;
      try { reply = parseControlJson(next.value); } catch { throw new ControlError('invalid_reply'); }
      if (!reply || reply.version !== 1 || reply.ordinal !== ordinal || typeof reply.ok !== 'boolean'
        || !exactKeys(reply, reply.ok ? ['version', 'ordinal', 'ok', 'result'] : ['version', 'ordinal', 'ok', 'error'])) {
        throw new ControlError('invalid_reply');
      }
      if (!reply.ok) {
        if (typeof reply.error !== 'string' || !/^[a-z_]{1,64}$/.test(reply.error)) throw new ControlError('invalid_reply');
        throw new ControlError('control_refusal');
      }
      validateResult(Object.keys(command)[0], command[Object.keys(command)[0]], reply.result);
      return reply.result;
    } catch (error) {
      this.closed = true;
      this.input.destroy();
      throw error instanceof ControlError ? error : new ControlError('control_failed');
    } finally { clearTimeout(timer); this.busy = false; }
  }
  async dispose() {
    this.closed = true;
    this.input.destroy();
    await this.lines.return();
  }
}

// Bounded lines are scanned before JSON.parse so duplicate decoded keys cannot hide flags.
function parseControlJson(text) {
  let offset = 0;
  const fail = () => { throw new ControlError('invalid_reply'); };
  const space = () => { while (/[\x20\t\r\n]/.test(text[offset] ?? '') && offset < text.length) offset++; };
  const string = () => {
    const start = offset;
    if (text[offset++] !== '"') fail();
    while (offset < text.length) {
      const character = text[offset++];
      if (character === '"') return JSON.parse(text.slice(start, offset));
      if (character === '\\') offset++;
    }
    fail();
  };
  const value = (depth) => {
    if (depth > 64) fail();
    space();
    const first = text[offset];
    if (first === '"') { string(); return; }
    if (first === '{' || first === '[') {
      const object = first === '{'; const end = object ? '}' : ']'; const keys = new Set();
      offset++; space();
      if (text[offset] === end) { offset++; return; }
      while (true) {
        if (object) {
          space(); const key = string();
          if (keys.has(key)) fail(); keys.add(key);
          space(); if (text[offset++] !== ':') fail();
        }
        value(depth + 1); space();
        const delimiter = text[offset++];
        if (delimiter === end) return;
        if (delimiter !== ',') fail();
      }
    }
    const primitive = /^(?:true|false|null|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/.exec(text.slice(offset));
    if (!primitive) fail(); offset += primitive[0].length;
  };
  value(0); space(); if (offset !== text.length) fail();
  return JSON.parse(text);
}

function exactKeys(value, keys) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}

function validateResult(operation, payload, result) {
  const fail = () => { throw new ControlError('invalid_reply'); };
  if (operation === 'arm') {
    if (!exactKeys(result, ['generation']) || !Number.isSafeInteger(result.generation) || result.generation < 1) fail();
  } else if (['await_entered', 'gate_status', 'release'].includes(operation)) {
    const flags = ['entered', 'returned', 'explicit_release', 'cleanup_release', 'expired'];
    if (!exactKeys(result, ['generation', ...flags]) || result.generation !== payload.generation
      || flags.some((key) => typeof result[key] !== 'boolean')) fail();
  } else if (operation === 'inspect_thread' || operation === 'inspect_pending') {
    const fields = ['found', 'execution_status', 'task_statuses', 'revision', 'owner_generation'];
    if (operation === 'inspect_pending') fields.push('interrupt_ids');
    if (!exactKeys(result, fields) || typeof result.found !== 'boolean' || !Array.isArray(result.task_statuses)
      || result.task_statuses.length > 16) fail();
    const execution = ['accepted', 'running', 'waiting_for_approval', 'completed', 'failed', 'cancel_requested', 'cancelled', 'recovery_blocked'];
    const tasks = ['ready', 'waiting_for_approval', 'in_flight', 'succeeded', 'failed', 'denied', 'cancelled', 'outcome_unknown'];
    if (result.execution_status !== null && !execution.includes(result.execution_status)) fail();
    if (result.task_statuses.some((status) => !tasks.includes(status))) fail();
    for (const field of ['revision', 'owner_generation']) if (result[field] !== null && (!Number.isSafeInteger(result[field]) || result[field] < 0)) fail();
    if (operation === 'inspect_pending' && (!Array.isArray(result.interrupt_ids) || result.interrupt_ids.length > 16
      || result.interrupt_ids.some((id) => typeof id !== 'string' || !id.length || id.length > 1024))) fail();
  } else if (operation === 'ready') {
    const routes = { worker_value: '/fixture/worker-value#workflow', worker_void: '/fixture/worker-void#workflow',
      worker_failure: '/fixture/worker-failure#workflow', worker_held: '/fixture/worker-held#workflow', worker_park: '/fixture/worker-park#workflow' };
    if (!exactKeys(result, ['base_url', 'token', 'routes']) || !exactKeys(result.routes, Object.keys(routes))
      || Object.keys(routes).some((key) => result.routes[key] !== routes[key]) || typeof result.token !== 'string'
      || !/^[\x21-\x7e]{1,512}$/.test(result.token)) fail();
    let url;
    try { url = new URL(result.base_url); } catch { fail(); }
    if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || !url.port || url.username || url.password
      || url.pathname !== '/' || url.search || url.hash) fail();
  } else if (operation === 'final_stop') {
    if (!exactKeys(result, ['finished']) || result.finished !== true) fail();
  }
}
