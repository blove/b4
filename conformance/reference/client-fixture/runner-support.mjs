export const STREAM_FRAMES = 64;

export async function consumeToEof(iterable) {
  const iterator = iterable[Symbol.asyncIterator]();
  const frames = [];
  try {
    while (true) {
      const next = await iterator.next();
      if (next.done) return { frames, eof: true, cut: null, reader_polled: true,
        pending_settled: true, return_awaited: false };
      if (frames.length >= STREAM_FRAMES) throw Object.assign(new Error('frame_limit'), { code: 'frame_limit' });
      frames.push(next.value);
    }
  } catch (error) {
    await iterator.return();
    throw error;
  }
}

export async function abortPolledIterator(iterator, pending, controller) {
  const settled = Promise.resolve(pending).then((value) => ({ value }), (error) => ({ error }));
  controller.abort();
  const result = await settled;
  await iterator.return();
  return { pending_settled: true, return_awaited: true,
    error: result.error ? { name: result.error.name ?? 'Error' } : null };
}
