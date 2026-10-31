// Offline qualification only. Captured bytes are never supplied to a caller.
const LIMITS = Object.freeze({
  maxCaptureBytes: 262144, maxHeaderBytes: 8192, maxHeaders: 64,
  maxBodyBytes: 262144, maxMessages: 256, maxChunks: 4096,
});
const TOKEN = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
const CHUNK_LINE = /^[0-9A-Fa-f]{1,8}(?:;[!#$%&'*+.^_`|~0-9A-Za-z-]+(?:=(?:[!#$%&'*+.^_`|~0-9A-Za-z-]+|"(?:[\x20-\x21\x23-\x5b\x5d-\x7e]|\\[\x20-\x7e])*"))?)*$/;
const fail = (message) => { throw new Error(`HTTP capture: ${message}`); };

function options(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input) || ![Object.prototype, null].includes(Object.getPrototypeOf(input))) fail('invalid options');
  for (const key of Object.keys(input)) {
    if (!Object.hasOwn(LIMITS, key) && key !== 'allowedIncompleteResponses') fail('unknown option');
  }
  const out = { ...LIMITS };
  for (const [key, ceiling] of Object.entries(LIMITS)) {
    const value = input[key] ?? ceiling;
    if (!Number.isSafeInteger(value) || value < 1 || value > ceiling) fail('invalid limit');
    out[key] = value;
  }
  const cuts = input.allowedIncompleteResponses ?? [];
  if (!Array.isArray(cuts) || cuts.length > out.maxMessages || new Set(cuts).size !== cuts.length ||
      cuts.some((n) => !Number.isSafeInteger(n) || n < 0 || n >= out.maxMessages)) fail('invalid partial designation');
  out.cuts = new Set(cuts);
  return out;
}

function bytes(input, cap) {
  const parts = Buffer.isBuffer(input) ? [input] : input;
  if (!Array.isArray(parts) || parts.length > cap) fail('invalid capture');
  let length = 0;
  for (const part of parts) {
    if (!Buffer.isBuffer(part)) fail('invalid capture fragment');
    length += part.length;
    if (length > cap) fail('capture limit');
  }
  return Buffer.concat(parts, length);
}

function fields(lines, limits, trailers = false) {
  if (lines.length > limits.maxHeaders) fail('header count limit');
  const headers = Object.create(null);
  for (const line of lines) {
    const colon = line.indexOf(':');
    if (colon <= 0 || !TOKEN.test(line.slice(0, colon)) || /[^\x09\x20-\x7e]/.test(line.slice(colon + 1))) fail('malformed header');
    const key = line.slice(0, colon).toLowerCase();
    const value = line.slice(colon + 1).trim();
    if (trailers && ['content-length', 'transfer-encoding', 'host', 'connection', 'authorization'].includes(key)) fail('forbidden trailer');
    if (Object.hasOwn(headers, key)) {
      if (['content-length', 'transfer-encoding', 'host'].includes(key)) fail('duplicate framing header');
      headers[key] += `, ${value}`;
    } else headers[key] = value;
  }
  return headers;
}

function fieldPrefix(prefix, limits, trailers = false) {
  if (/[^\x09\x0a\x0d\x20-\x7e]/.test(prefix) || /([^\r]|^)\n|\r[^\n]/.test(prefix)) fail('malformed field prefix');
  const lines = prefix.split('\r\n');
  let tail = lines.pop();
  const completed = fields(lines, limits, trailers);
  if (!trailers) framing(completed, limits);
  const heldCr = tail.endsWith('\r');
  if (heldCr) tail = tail.slice(0, -1);
  if (!tail) return { completed, tail, heldCr };
  if (lines.length + 1 > limits.maxHeaders) fail('header count limit');
  if (tail.includes(':')) {
    fields([...lines, tail], limits, trailers);
    if (!trailers) {
      const colon = tail.indexOf(':'); const name = tail.slice(0, colon).toLowerCase();
      const raw = tail.slice(colon + 1); const value = raw.trim();
      if ((name === 'content-length' && Object.hasOwn(completed, 'transfer-encoding')) ||
          (name === 'transfer-encoding' && Object.hasOwn(completed, 'content-length'))) fail('ambiguous framing');
      if (name === 'content-length') {
        if ((!value && heldCr) || (value && !/^(0|[1-9][0-9]*)$/.test(value))) fail('malformed length prefix');
        if (value.length > 16 || Number(value) > limits.maxBodyBytes) fail('body length limit');
      }
      if (name === 'transfer-encoding' && (!'chunked'.startsWith(value.toLowerCase()) ||
        (heldCr && value.toLowerCase() !== 'chunked') || (value && /[\t ]$/.test(raw) && value.toLowerCase() !== 'chunked'))) fail('malformed encoding prefix');
    }
  } else if (!TOKEN.test(tail) || heldCr) fail('malformed field prefix');
  return { completed, tail, heldCr };
}

function partialResponseSemantics(status, method, prefix, limits) {
  if (status === 101) fail('unsupported upgrade');
  if (!prefix) return;
  const { completed, tail, heldCr } = prefix;
  const frame = framing(completed, limits);
  const noBody = method === 'HEAD' || status < 200 || [204, 304].includes(status);
  const colon = tail.indexOf(':');
  const name = colon > 0 ? tail.slice(0, colon).toLowerCase() : '';
  const value = colon > 0 ? tail.slice(colon + 1).trim() : '';
  if (noBody && (frame.chunked || name === 'transfer-encoding' ||
      (status === 204 && ((frame.length !== undefined && frame.length !== 0) ||
        (name === 'content-length' && value && value !== '0'))))) fail('invalid no-body framing');
  if (heldCr && !tail && !noBody && frame.length === undefined && !frame.chunked) fail('unsupported unframed response');
}

function chunkPrefix(prefix) {
  if (prefix.endsWith('\r')) return CHUNK_LINE.test(prefix.slice(0, -1));
  if (!prefix) return true;
  let i = 0;
  while (/[0-9A-Fa-f]/.test(prefix[i] ?? '') && i < prefix.length) i++;
  if (i < 1 || i > 8) return false;
  const tokenChar = (c) => c !== undefined && TOKEN.test(c);
  while (i < prefix.length) {
    if (prefix[i++] !== ';') return false;
    if (i === prefix.length) return true;
    const start = i;
    while (tokenChar(prefix[i])) i++;
    if (i === start) return false;
    if (i === prefix.length) return true;
    if (prefix[i] === ';') continue;
    if (prefix[i++] !== '=') return false;
    if (i === prefix.length) return true;
    if (prefix[i] === '"') {
      i++; let closed = false;
      while (i < prefix.length) {
        const c = prefix[i++];
        if (c === '"') { closed = true; break; }
        if (c === '\\') {
          if (i === prefix.length) return true;
          const escaped = prefix[i++]; if (escaped < ' ' || escaped > '~') return false;
        } else if (c < ' ' || c > '~') return false;
      }
      if (!closed) return true;
    } else {
      const valueStart = i;
      while (tokenChar(prefix[i])) i++;
      if (i === valueStart) return false;
    }
    if (i < prefix.length && prefix[i] !== ';') return false;
  }
  return true;
}

function framing(headers, limits) {
  const cl = headers['content-length'];
  const te = headers['transfer-encoding'];
  if (cl !== undefined && te !== undefined) fail('ambiguous framing');
  if (te !== undefined && te.toLowerCase() !== 'chunked') fail('unsupported transfer encoding');
  let length;
  if (cl !== undefined) {
    if (!/^(0|[1-9][0-9]*)$/.test(cl) || cl.length > 16) fail('invalid length');
    length = Number(cl);
    if (!Number.isSafeInteger(length) || length > limits.maxBodyBytes) fail('body length limit');
  }
  return { length, chunked: te !== undefined };
}

function message(buffer, offset, kind, method, limits, allowPartial) {
  const start = offset;
  const incomplete = (reason, extra = {}) => {
    if (!allowPartial) fail(`incomplete ${kind} ${reason}`);
    return { byteStart: start, byteEnd: buffer.length, complete: false, partialReason: reason,
      headers: Object.create(null), trailers: Object.create(null), body: Buffer.alloc(0), ...extra };
  };
  const headerEnd = buffer.indexOf('\r\n\r\n', offset);
  if (headerEnd < 0) {
    if (buffer.length - offset > limits.maxHeaderBytes) fail('header limit');
    const prefix = buffer.subarray(offset).toString('latin1');
    if (/[^\x09\x0a\x0d\x20-\x7e]/.test(prefix) || /([^\r]|^)\n|\r[^\n]/.test(prefix)) fail('malformed partial header');
    const completeLines = prefix.split('\r\n');
    const first = completeLines.shift();
    if (completeLines.length) {
      if (kind === 'response' && !/^HTTP\/1\.[01] [1-5][0-9]{2}(?: [\x20-\x7e]*)?$/.test(first)) fail('malformed response line');
      const prefixFields = fieldPrefix(completeLines.join('\r\n'), limits);
      if (kind === 'response') partialResponseSemantics(Number(first.slice(9, 12)), method, prefixFields, limits);
    } else if (kind === 'response' && first) {
      if (first.endsWith('\r')) {
        if (!/^HTTP\/1\.[01] [1-5][0-9]{2}(?: [\x20-\x7e]*)?$/.test(first.slice(0, -1))) fail('malformed partial response line');
      } else if (!['HTTP/1.0 ', 'HTTP/1.1 '].some((p) => p.startsWith(first) ||
        (first.startsWith(p) && /^(?:[1-5][0-9]{0,2}|[1-5][0-9]{2} [\x20-\x7e]*)$/.test(first.slice(p.length))))) fail('malformed partial response line');
    }
    if (kind === 'response' && /^HTTP\/1\.[01] [1-5][0-9]{2}(?:[ \r]|$)/.test(first)) {
      partialResponseSemantics(Number(first.slice(9, 12)), method, null, limits);
    }
    return incomplete('headers');
  }
  if (headerEnd + 4 - offset > limits.maxHeaderBytes) fail('header limit');
  const lines = buffer.subarray(offset, headerEnd).toString('latin1').split('\r\n');
  const line = lines.shift();
  const headers = fields(lines, limits);
  const base = { byteStart: start, headers, trailers: Object.create(null), body: Buffer.alloc(0) };
  if (kind === 'request') {
    const match = /^([A-Z!#$%&'*+.^_`|~-]+) ([\x21-\x7e]+) HTTP\/1\.[01]$/.exec(line);
    if (!match) fail('malformed request line');
    base.method = match[1]; base.target = match[2];
  } else {
    const match = /^HTTP\/1\.[01] ([1-5][0-9]{2})(?: [\x20-\x7e]*)?$/.exec(line);
    if (!match) fail('malformed response line');
    base.status = Number(match[1]);
    if (base.status === 101) fail('unsupported upgrade');
  }
  const { length, chunked } = framing(headers, limits);
  offset = headerEnd + 4;
  const noBody = kind === 'response' && (method === 'HEAD' || base.status < 200 || [204, 304].includes(base.status));
  if (noBody) {
    if (chunked || (base.status === 204 && length !== undefined && length !== 0)) fail('invalid no-body framing');
    return { ...base, framing: 'none', complete: true, byteEnd: offset };
  }
  if (chunked) {
    const pieces = []; let total = 0; let chunks = 0;
    const partial = (reason, suffix = Buffer.alloc(0)) => incomplete(reason, {
      ...base, framing: 'chunked', body: Buffer.concat([...pieces, suffix], total + suffix.length),
    });
    for (;;) {
      const end = buffer.indexOf('\r\n', offset);
      if (end < 0) {
        if (buffer.length - offset > limits.maxHeaderBytes) fail('chunk line limit');
        const prefix = buffer.subarray(offset).toString('latin1');
        if (!chunkPrefix(prefix)) fail('malformed partial chunk line');
        const digits = /^[0-9A-Fa-f]{1,8}/.exec(prefix)?.[0];
        if (digits) {
          const knownSize = Number.parseInt(digits, 16);
          if (knownSize > limits.maxBodyBytes - total) fail('body limit');
          if (knownSize > 0 && chunks >= limits.maxChunks) fail('chunk count limit');
        }
        return partial('chunk-header');
      }
      if (end - offset > limits.maxHeaderBytes) fail('chunk line limit');
      const sizeLine = buffer.subarray(offset, end).toString('latin1');
      if (!CHUNK_LINE.test(sizeLine)) fail('malformed chunk line');
      const size = Number.parseInt(sizeLine.split(';', 1)[0], 16);
      if (size > limits.maxBodyBytes - total) fail('body limit');
      offset = end + 2;
      if (size === 0) {
        // Empty trailers still have a final CRLF; the zero-size line alone is insufficient.
        let trailerEnd;
        if (buffer[offset] === 13 && buffer[offset + 1] === 10) trailerEnd = offset;
        else trailerEnd = buffer.indexOf('\r\n\r\n', offset);
        if (trailerEnd < 0) {
          if (buffer.length - offset > limits.maxHeaderBytes) fail('trailer limit');
          fieldPrefix(buffer.subarray(offset).toString('latin1'), limits, true);
          return partial('trailers');
        }
        const trailerBytes = trailerEnd - offset;
        if (trailerBytes + (trailerBytes ? 4 : 2) > limits.maxHeaderBytes) fail('trailer limit');
        base.trailers = fields(trailerBytes ? buffer.subarray(offset, trailerEnd).toString('latin1').split('\r\n') : [], limits, true);
        return { ...base, body: Buffer.concat(pieces, total), framing: 'chunked', complete: true,
          byteEnd: trailerEnd + (trailerBytes ? 4 : 2) };
      }
      if (++chunks > limits.maxChunks) fail('chunk count limit');
      if (buffer.length - offset < size + 2) {
        const available = Math.min(size, buffer.length - offset);
        if (available === size && buffer[offset + size] !== undefined && buffer[offset + size] !== 13) fail('malformed chunk terminator');
        return partial('chunk-body', buffer.subarray(offset, offset + available));
      }
      if (buffer[offset + size] !== 13 || buffer[offset + size + 1] !== 10) fail('malformed chunk terminator');
      pieces.push(buffer.subarray(offset, offset + size)); total += size; offset += size + 2;
    }
  }
  if (length !== undefined) {
    if (length > buffer.length - offset) return incomplete('body', {
      ...base, framing: 'content-length', body: buffer.subarray(offset),
    });
    return { ...base, framing: 'content-length', complete: true, body: buffer.subarray(offset, offset + length), byteEnd: offset + length };
  }
  if (kind === 'request') return { ...base, framing: 'none', complete: true, byteEnd: offset };
  fail('unsupported unframed response');
}

export function decodeHttpConnection(capture, input = {}) {
  if (!capture || Object.keys(capture).sort().join(',') !== 'read,write') fail('invalid capture fields');
  const limits = options(input);
  const read = bytes(capture.read, limits.maxCaptureBytes);
  const write = bytes(capture.write, limits.maxCaptureBytes);
  const requests = requestMessages(read, limits); const responses = []; const pairs = [];
  if ([...limits.cuts].some((n) => n >= requests.length)) fail('unpaired partial designation');
  let offset = 0;
  let infoCount = 0;
  for (const req of requests) {
    const informational = [];
    let m;
    do {
      m = message(write, offset, 'response', req.method, limits, limits.cuts.has(req.index));
      offset = m.byteEnd;
      if (m.complete && m.status < 200) {
        if (++infoCount > limits.maxMessages) fail('informational message limit');
        informational.push(m);
      }
    } while (m.complete && m.status < 200);
    m.index = req.index; responses.push(m); pairs.push({ request: req, response: m, informational });
    if (!m.complete && req.index !== requests.length - 1) fail('incomplete nonfinal response');
  }
  if (offset !== write.length) fail('unpaired response bytes');
  return { requests, responses, pairs, readBytes: read.length, writeBytes: write.length };
}

function requestMessages(read, limits) {
  const requests = []; let offset = 0;
  while (offset < read.length) {
    if (requests.length >= limits.maxMessages) fail('message limit');
    const m = message(read, offset, 'request', undefined, limits, false);
    m.index = requests.length; requests.push(m); offset = m.byteEnd;
  }
  return requests;
}

export function decodeHttpRequests(read, input = {}) {
  const limits = options(input);
  if (limits.cuts.size) fail('response cuts cannot apply to request-only decoding');
  return requestMessages(bytes(read, limits.maxCaptureBytes), limits);
}

export function decodeCapturedSse(body, input = {}) {
  const admitted = ['allowPartial', 'maxFrames', 'maxFrameBytes'];
  if (!input || typeof input !== 'object' || Array.isArray(input) || Object.keys(input).some((k) => !admitted.includes(k))) fail('unknown SSE option');
  const allowPartial = input.allowPartial ?? false;
  if (typeof allowPartial !== 'boolean') fail('invalid partial flag');
  const maxFrames = input.maxFrames ?? 64;
  const maxFrameBytes = input.maxFrameBytes ?? 65536;
  if (!Number.isSafeInteger(maxFrames) || maxFrames < 1 || maxFrames > 64 || !Number.isSafeInteger(maxFrameBytes) || maxFrameBytes < 1 || maxFrameBytes > 65536) fail('SSE limit');
  body = bytes(body, LIMITS.maxCaptureBytes);
  let text; let unfinishedUtf8 = false;
  const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
  try {
    text = decoder.decode(body, { stream: allowPartial });
    if (allowPartial) {
      try { text += decoder.decode(); } catch { unfinishedUtf8 = true; }
    }
  } catch { fail('malformed UTF8'); }
  const heldCr = allowPartial && text.endsWith('\r');
  if (heldCr) text = text.slice(0, -1);
  text = text.replace(/\r\n|\r/g, '\n');
  const frames = []; let blocks = 0; let offset = 0;
  const parse = (block) => {
    if (++blocks > 4096 || Buffer.byteLength(block) > maxFrameBytes) fail('SSE block limit');
    let event = 'message'; const data = [];
    for (const line of block.split('\n')) {
      if (!line || line.startsWith(':')) continue;
      const colon = line.indexOf(':');
      const field = colon < 0 ? line : line.slice(0, colon);
      let value = colon < 0 ? '' : line.slice(colon + 1);
      if (value.startsWith(' ')) value = value.slice(1);
      if (field === 'event') event = value;
      if (field === 'data') data.push(value);
    }
    if (!data.length) return;
    if (frames.length >= maxFrames) fail('SSE frame limit');
    let parsed;
    try { parsed = JSON.parse(data.join('\n')); } catch { fail('incomplete or non-JSON native SSE payload'); }
    frames.push({ event, data: parsed });
  };
  let boundary;
  while ((boundary = text.indexOf('\n\n', offset)) >= 0) {
    parse(text.slice(offset, boundary)); offset = boundary + 2;
  }
  const tail = text.slice(offset);
  if (Buffer.byteLength(tail) > maxFrameBytes) fail('SSE block limit');
  const partialTail = allowPartial && (tail.length > 0 || heldCr || unfinishedUtf8);
  const eofTailFrame = !allowPartial && tail.length > 0;
  if (eofTailFrame) parse(tail);
  return { frames, partialTail, eofTailFrame };
}
