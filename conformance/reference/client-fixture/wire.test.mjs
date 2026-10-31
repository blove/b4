import test from 'node:test';
import assert from 'node:assert/strict';
import { decodeHttpConnection, decodeCapturedSse, decodeHttpRequests } from './wire.mjs';

const request = (method, target, body = '') =>
  Buffer.from(`${method} ${target} HTTP/1.1\r\nHost: localhost\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
const response = (status, body = '') =>
  Buffer.from(`HTTP/1.1 ${status} Fixture\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
const decode = (read, write, options) => decodeHttpConnection({ read, write }, options);

test('decodes two persistent requests and responses without conflating connection and message', () => {
  const d = decode(Buffer.concat([request('POST', '/threads/t/runs/wait', '{"route":"/x#workflow"}'), request('GET', '/threads/t')]),
    Buffer.concat([response(200, '{"ok":true}'), response(404, '{"error":{"code":"not_found"}}')]));
  assert.equal(d.pairs.length, 2);
  assert.equal(d.pairs[0].request.target, '/threads/t/runs/wait');
  assert.equal(d.pairs[0].response.body.toString(), '{"ok":true}');
  assert.equal(d.pairs[1].response.status, 404);
  assert.equal(d.pairs[0].response.complete, true);
});

test('decodes requests before assigning declared cuts using their actual private targets and bodies', () => {
  const requests = decodeHttpRequests(Buffer.concat([request('POST', '/threads/a%2Fb/runs/wait', '{"input":{"operationKey":"controlled-case"}}'), request('GET', '/threads/a%2Fb/runs/stream')]));
  assert.equal(requests.length, 2);
  assert.equal(requests[0].target, '/threads/a%2Fb/runs/wait');
  assert.equal(JSON.parse(requests[0].body.toString()).input.operationKey, 'controlled-case');
  assert.equal(requests[1].method, 'GET');
  assert.throws(() => decodeHttpRequests(Buffer.from('GET /a HTTP/1.1\r\n')), /incomplete/);
});

test('fragmented and vectored byte boundaries preserve chunked data, trailers, and next message', () => {
  const bytes = Buffer.from('HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4;kind=one\r\ndata\r\n3\r\nxyz\r\n0\r\nX-End: yes\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n');
  const fragments = Array.from(bytes, (b) => Buffer.from([b]));
  const d = decode([request('GET', '/a'), request('DELETE', '/b')], fragments);
  assert.equal(d.pairs[0].response.body.toString(), 'dataxyz');
  assert.equal(d.pairs[0].response.trailers['x-end'], 'yes');
  assert.equal(d.pairs[1].response.status, 204);
  assert.equal(d.pairs[1].response.body.length, 0);
});

test('terminal chunk requires the final trailer CRLF before another response', () => {
  const read = request('GET', '/events');
  const missing = Buffer.from('HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n');
  assert.throws(() => decode(read, missing), /incomplete/);
  const d = decode(read, missing, { allowedIncompleteResponses: [0] });
  assert.equal(d.pairs[0].response.complete, false);
  assert.equal(d.pairs[0].response.partialReason, 'trailers');
  assert.equal(d.pairs[0].response.body.toString(), 'x');
});

test('allows only designated response cuts and keeps missing headers distinct from EOF', () => {
  const read = Buffer.concat([request('GET', '/a'), request('GET', '/b')]);
  assert.throws(() => decode(read, response(200, 'a')), /incomplete/);
  const d = decode(read, response(200, 'a'), { allowedIncompleteResponses: [1] });
  assert.equal(d.pairs[0].response.complete, true);
  assert.equal(d.pairs[1].response.complete, false);
  assert.equal(d.pairs[1].response.partialReason, 'headers');
  assert.throws(() => decode(read, response(200, 'a'), { allowedIncompleteResponses: [0] }), /incomplete/);
});

test('informational responses do not consume the request response queue', () => {
  const d = decode(request('POST', '/a', 'x'), Buffer.concat([
    Buffer.from('HTTP/1.1 100 Continue\r\n\r\n'), response(200, 'done'),
  ]));
  assert.equal(d.pairs.length, 1);
  assert.equal(d.pairs[0].informational[0].status, 100);
  assert.equal(d.pairs[0].response.status, 200);
});

test('HEAD and 304 use no body despite a representation Content-Length', () => {
  const d = decode(Buffer.concat([request('HEAD', '/a'), request('GET', '/b')]), Buffer.from(
    'HTTP/1.1 200 OK\r\nContent-Length: 99\r\n\r\nHTTP/1.1 304 Not Modified\r\nContent-Length: 123\r\n\r\n'));
  assert.equal(d.pairs[0].response.body.length, 0);
  assert.equal(d.pairs[1].response.body.length, 0);
});

test('refuses ambiguous framing, malformed headers/chunks, unpaired responses, and unsupported upgrades', () => {
  for (const raw of [
    'HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx',
    'HTTP/1.1 200 OK\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n',
    'HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n0\r\n\r\n',
    'HTTP/1.1 200 OK\r\n folded: no\r\n\r\n',
    'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx!\r\n0\r\n\r\n',
    'HTTP/1.1 101 Switching Protocols\r\n\r\n',
    'HTTP/1.1 200 OK\r\n\r\nunknown length',
  ]) assert.throws(() => decode(request('GET', '/a'), Buffer.from(raw)));
  assert.throws(() => decode(Buffer.alloc(0), response(200, 'x')), /unpaired/);
  assert.throws(() => decode(Buffer.from('GET /a HTTP/1.1\nHost: a\n\n'), Buffer.alloc(0)), /incomplete|malformed/);
});

test('checks capture, header, body, message and chunk limits before accepting evidence', () => {
  assert.throws(() => decode(request('GET', '/a'), response(200, 'x'), { maxCaptureBytes: 1 }), /limit/);
  assert.throws(() => decode(request('GET', '/a'), response(200, 'x'), { maxHeaderBytes: 8 }), /limit/);
  assert.throws(() => decode(request('GET', '/a'), response(200, 'ab'), { maxBodyBytes: 1 }), /limit/);
  assert.throws(() => decode(Buffer.concat([request('GET', '/a'), request('GET', '/b')]), Buffer.concat([response(200), response(200)]), { maxMessages: 1 }), /limit/);
  assert.throws(() => decode(request('GET', '/a'), Buffer.from('HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n1\r\ny\r\n0\r\n\r\n'), { maxChunks: 1 }), /limit/);
  assert.throws(() => decode(request('GET', '/a'), response(200, 'x'), { arbitrary: true }), /unknown/);
  assert.throws(() => decode(request('GET', '/a'), response(200, 'x'), { maxCaptureBytes: 262145 }), /limit/);
});

test('partial Content-Length bodies retain actual accepted bytes without pretending message completion', () => {
  const d = decode(request('GET', '/a'), Buffer.from('HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nxy'), { allowedIncompleteResponses: [0] });
  assert.equal(d.pairs[0].response.body.toString(), 'xy');
  assert.equal(d.pairs[0].response.partialReason, 'body');
  assert.equal(d.pairs[0].response.complete, false);
  assert.throws(() => decode(request('GET', '/a'), Buffer.from('HTTP/1.1 200 OK\r\nContent-Length: 999999999999999999\r\n\r\n'), { allowedIncompleteResponses: [0] }), /length|limit/);
});

test('partial designations do not admit malformed header bytes or inherited option names', () => {
  assert.throws(() => decode(request('GET', '/a'), Buffer.from('HTTP/1.1 200 OK\r\nBad: \0'), { allowedIncompleteResponses: [0] }), /malformed/);
  assert.throws(() => decode(request('GET', '/a'), Buffer.alloc(0), { allowedIncompleteResponses: [0], constructor: 5 }), /unknown/);
  assert.throws(() => decode(request('GET', '/a'), Buffer.from('HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\0'), { allowedIncompleteResponses: [0] }), /malformed/);
  assert.throws(() => decode(request('GET', '/a'), Buffer.alloc(0), Object.create({ allowedIncompleteResponses: [0] })), /invalid/);
});

test('offline SSE distinguishes complete frames, comments and planned partial suffixes', () => {
  const body = Buffer.from(': keepalive\r\n\r\nevent: state\r\ndata: {"status":"busy"}\r\n\r\nevent: done\ndata: {"output":\ndata: null}\n\n');
  const d = decodeCapturedSse(body);
  assert.deepEqual(d.frames, [{ event: 'state', data: { status: 'busy' } }, { event: 'done', data: { output: null } }]);
  assert.equal(d.partialTail, false);
  const prefix = decodeCapturedSse(Buffer.concat([body, Buffer.from('event: done\ndata: {')]), { allowPartial: true });
  assert.equal(prefix.frames.length, 2);
  assert.equal(prefix.partialTail, true);
  assert.throws(() => decodeCapturedSse(Buffer.concat([body, Buffer.from('event: done\ndata: {')])), /incomplete/);
});

test('offline SSE enforces frame, block, UTF8 and captured-byte bounds', () => {
  const block = Buffer.from('event: done\ndata: {}\n\n');
  assert.throws(() => decodeCapturedSse(Buffer.concat([block, block]), { maxFrames: 1 }), /limit/);
  assert.throws(() => decodeCapturedSse(block, { maxFrameBytes: 8 }), /limit/);
  assert.throws(() => decodeCapturedSse(Buffer.from([0xff])), /UTF/);
  assert.throws(() => decodeCapturedSse(Buffer.alloc(262145)), /limit/);
  assert.throws(() => decodeCapturedSse(block, { maxFrames: 65 }), /limit/);
  assert.throws(() => decodeCapturedSse(block, { constructor: 1 }), /unknown/);
});

test('declared cuts admit viable field and extension prefixes but refuse impossible grammar', () => {
  const chunked = 'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n';
  for (const prefix of ['', 'HT', 'HTTP/1.1 200 OK\r\nX-', 'HTTP/1.1 200 OK\r\nX-Case: val',
    `${chunked}A`, `${chunked}1;kind="`, `${chunked}0\r\nX-End: ya`, `${chunked}0\r\n\r`]) {
    const d = decode(request('GET', '/a'), Buffer.from(prefix), { allowedIncompleteResponses: [0] });
    assert.equal(d.responses[0].complete, false);
  }
  for (const prefix of [`${chunked}Z`, `${chunked}000000000`, `${chunked}1;=`, `${chunked}0\r\nBad: \0`,
    'HTTP/1.1 200 OK\r\nBad Header: value', 'HTTP/1.1 200 OK\r\n folded', 'HTTP/1.1 2\r']) {
    assert.throws(() => decode(request('GET', '/a'), Buffer.from(prefix), { allowedIncompleteResponses: [0] }), /malformed/);
  }
});

test('partial fields cannot exceed header counts or duplicate already complete framing fields', () => {
  const read = Buffer.from('GET /a HTTP/1.1\r\nHost: localhost\r\n\r\n');
  for (const [prefix, maxHeaders] of [['HTTP/1.1 200 OK\r\nX-A: one\r\nX-B: two', 1],
    ['HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2', 64]]) {
    assert.throws(() => decode(read, Buffer.from(prefix), { allowedIncompleteResponses: [0], maxHeaders }), /limit|duplicate/);
  }
});

test('partial header syntax retains only viable Content-Length and transfer-encoding prefixes', () => {
  for (const prefix of ['Content-Length: ', 'Content-Length: 12', 'Transfer-Encoding: chun']) {
    assert.equal(decode(request('GET', '/a'), Buffer.from(`HTTP/1.1 200 OK\r\n${prefix}`), { allowedIncompleteResponses: [0] }).responses[0].complete, false);
  }
  for (const prefix of ['Content-Length: -1\r\n', 'Content-Length: -', 'Content-Length: 01', 'Transfer-Encoding: gzip\r\n',
    'Transfer-Encoding: gzip', 'Content-Length: 1\r\nTransfer-Encoding: chunked', 'Transfer-Encoding: chunked\r\nContent-Length: ']) {
    assert.throws(() => decode(request('GET', '/a'), Buffer.from(`HTTP/1.1 200 OK\r\n${prefix}`), { allowedIncompleteResponses: [0] }));
  }
});

test('known incomplete chunk sizes and counts must still fit their remaining caps', () => {
  const prefix = 'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n';
  for (const [tail, caps] of [['5', { maxBodyBytes: 4 }], ['FFFFFFFF', {}],
    ['1\r\nx\r\n1', { maxChunks: 1 }], ['1\r\nx\r\n4', { maxBodyBytes: 4 }]]) {
    assert.throws(() => decode(request('GET', '/a'), Buffer.from(prefix + tail), { allowedIncompleteResponses: [0], ...caps }), /limit/);
  }
  assert.equal(decode(request('GET', '/a'), Buffer.from(prefix + '1\r\nx\r\n0'), { allowedIncompleteResponses: [0], maxChunks: 1 }).responses[0].complete, false);
});

test('an incomplete status prefix cannot begin a reason before all three status digits', () => {
  for (const tail of ['2 ', '20 reason', '20 ']) {
    assert.throws(() => decode(request('GET', '/a'), Buffer.from('HTTP/1.1 ' + tail), { allowedIncompleteResponses: [0] }), /malformed/);
  }
  for (const tail of ['2', '20', '200', '200 O']) {
    assert.equal(decode(request('GET', '/a'), Buffer.from('HTTP/1.1 ' + tail), { allowedIncompleteResponses: [0] }).responses[0].complete, false);
  }
});

test('partial headers refuse response semantics that cannot become an admitted complete message', () => {
  for (const prefix of ['HTTP/1.1 101 Switching Protocols\r\n', 'HTTP/1.1 200 OK\r\n\r',
    'HTTP/1.1 204 No Content\r\nContent-Length: 1\r']) {
    assert.throws(() => decode(request('GET', '/a'), Buffer.from(prefix), { allowedIncompleteResponses: [0] }));
  }
  for (const prefix of ['HTTP/1.1 200 OK\r\nContent-Length: 1\r', 'HTTP/1.1 204 No Content\r\n\r',
    'HTTP/1.1 204 No Content\r\nContent-Length: 0\r']) {
    assert.equal(decode(request('GET', '/a'), Buffer.from(prefix), { allowedIncompleteResponses: [0] }).responses[0].complete, false);
  }
  assert.equal(decode(request('HEAD', '/a'), Buffer.from('HTTP/1.1 200 OK\r\n\r'), { allowedIncompleteResponses: [0] }).responses[0].complete, false);
});
