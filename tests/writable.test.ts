// SPDX-License-Identifier: LGPL-3.0-or-later
import { Writable } from '@zenfs/streams';
import { Buffer } from 'node:buffer';
import assert from 'node:assert/strict';
import { suite, test } from 'node:test';

/** Collects everything written, with an optional per-write delay. */
function sink(options: { highWaterMark?: number; delay?: number } = {}) {
	const written: string[] = [];
	const w = new Writable({
		highWaterMark: options.highWaterMark,
		write(chunk, encoding, callback) {
			written.push(chunk.toString());
			if (options.delay === undefined) callback();
			else setTimeout(callback, options.delay);
		},
	});
	return { w, written };
}

suite('Writable', () => {
	test('write then end delivers every chunk', async () => {
		const { w, written } = sink();
		w.write('a');
		w.write('b');
		await new Promise(resolve => w.end('c', resolve));
		assert.deepEqual(written, ['a', 'b', 'c']);
		assert.equal(w.writableFinished, true);
	});

	test('decodes strings to Buffers by default', async () => {
		let chunk: unknown;
		const w = new Writable({
			write(c, e, cb) {
				chunk = c;
				cb();
			},
		});
		await new Promise(resolve => w.end('hi', resolve));
		assert.ok(chunk instanceof Buffer);
	});

	test('decodeStrings: false leaves strings alone', async () => {
		let chunk: unknown;
		const w = new Writable({
			decodeStrings: false,
			write(c, e, cb) {
				chunk = c;
				cb();
			},
		});
		await new Promise(resolve => w.end('hi', resolve));
		assert.equal(chunk, 'hi');
	});

	test('write returns false past the high water mark and drains', async () => {
		const { w } = sink({ highWaterMark: 4, delay: 1 });
		assert.equal(w.write(Buffer.alloc(2)), true);
		assert.equal(w.write(Buffer.alloc(4)), false);
		assert.equal(w.writableNeedDrain, true);
		await new Promise(resolve => w.on('drain', resolve));
		assert.equal(w.writableNeedDrain, false);
	});

	test('cork batches writes into _writev', async () => {
		const batches: string[][] = [];
		const w = new Writable({
			writev(chunks, callback) {
				batches.push(chunks.map(c => c.chunk.toString()));
				callback();
			},
		});
		w.cork();
		w.write('a');
		w.write('b');
		w.write('c');
		assert.equal(w.writableCorked, 1);
		w.uncork();
		await new Promise(resolve => w.end(resolve));
		assert.deepEqual(batches, [['a', 'b', 'c']]);
	});

	test('end() fully uncorks', async () => {
		const { w, written } = sink();
		w.cork();
		w.cork();
		w.write('a');
		await new Promise(resolve => w.end('b', resolve));
		assert.deepEqual(written, ['a', 'b']);
	});

	test('write after end errors', async () => {
		const { w } = sink();
		w.end('a');
		const error = new Promise(resolve => w.once('error', resolve));
		w.write('b');
		assert.equal(((await error) as { code: string }).code, 'ERR_STREAM_WRITE_AFTER_END');
	});

	test('a write error is passed to the callback and the stream', async () => {
		const failure = new Error('disk full');
		const w = new Writable({
			write(c, e, cb) {
				cb(failure);
			},
		});
		const fromCallback = new Promise(resolve => w.write('a', resolve as never));
		const fromStream = new Promise(resolve => w.once('error', resolve));
		assert.equal(await fromCallback, failure);
		assert.equal(await fromStream, failure);
		assert.equal(w.errored, failure);
	});

	test('_final runs before finish', async () => {
		const order: string[] = [];
		const w = new Writable({
			write(c, e, cb) {
				cb();
			},
			final(cb) {
				order.push('final');
				setTimeout(cb, 1);
			},
		});
		w.on('finish', () => order.push('finish'));
		await new Promise(resolve => w.end('a', resolve));
		assert.deepEqual(order, ['final', 'finish']);
	});

	test('null chunks are rejected', () => {
		const { w } = sink();
		assert.throws(() => w.write(null), { code: 'ERR_STREAM_NULL_VALUES' });
	});

	test('an unknown encoding is rejected', () => {
		const { w } = sink();
		assert.throws(() => w.write('x', 'nonsense' as never), { code: 'ERR_UNKNOWN_ENCODING' });
	});

	test('piping from a Writable errors', async () => {
		const { w } = sink();
		const error = new Promise(resolve => w.once('error', resolve));
		w.pipe(sink().w as never);
		assert.equal(((await error) as { code: string }).code, 'ERR_STREAM_CANNOT_PIPE');
	});

	test('destroy fails the writes still queued', async () => {
		const w = new Writable({
			write(c, e, cb) {
				cb();
			},
		});
		w.cork();
		const queued = new Promise<Error | null | undefined>(resolve => w.write('a', resolve));
		w.on('error', () => {});
		w.destroy();
		assert.equal(((await queued) as { code?: string })?.code, 'ERR_STREAM_DESTROYED');
		assert.equal(w.destroyed, true);
	});

	test('an end() callback after destroy reports ERR_STREAM_DESTROYED', async () => {
		const w = new Writable({
			write(c, e, cb) {
				cb();
			},
		});
		w.on('error', () => {});
		w.destroy();
		const err = await new Promise<Error | null | undefined>(resolve => w.end('a', resolve));
		assert.equal((err as { code?: string })?.code, 'ERR_STREAM_DESTROYED');
	});

	test('objectMode measures length in chunks', () => {
		const w = new Writable({ objectMode: true, highWaterMark: 2, write() {} });
		assert.equal(w.write({}), true);
		assert.equal(w.write({}), false);
		assert.equal(w.writableLength, 2);
	});
});
