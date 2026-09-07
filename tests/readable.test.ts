// SPDX-License-Identifier: LGPL-3.0-or-later
import { Readable } from '@zenfs/streams';
import { Buffer } from 'node:buffer';
import assert from 'node:assert/strict';
import { suite, test } from 'node:test';

suite('Readable', () => {
	test('pushes through to async iteration', async () => {
		const r = new Readable({
			read() {
				this.push('a');
				this.push('b');
				this.push(null);
			},
		});
		const out: string[] = [];
		for await (const chunk of r) out.push(chunk.toString());
		assert.deepEqual(out, ['a', 'b']);
	});

	test('yields Buffers without an encoding and strings with one', async () => {
		const bytes = new Readable({
			read() {
				this.push('hi');
				this.push(null);
			},
		});
		assert.ok((await bytes.toArray())[0] instanceof Buffer);

		const text = new Readable({
			encoding: 'utf8',
			read() {
				this.push('hi');
				this.push(null);
			},
		});
		assert.deepEqual(await text.toArray(), ['hi']);
	});

	test('setEncoding does not split multi-byte characters', async () => {
		const bytes = Buffer.from('héllo ✓', 'utf8');
		const r = new Readable({ read() {} });
		r.setEncoding('utf8');
		for (const byte of bytes) r.push(Buffer.from([byte]));
		r.push(null);
		assert.equal((await r.toArray()).join(''), 'héllo ✓');
	});

	test('read(n) returns exactly n bytes and buffers the rest', () => {
		const r = new Readable({ read() {} });
		r.push(Buffer.from('hello world'));
		r.push(null);
		assert.equal(r.read(5).toString(), 'hello');
		assert.equal(r.readableLength, 6);
		assert.equal(r.read(6).toString(), ' world');
		assert.equal(r.read(), null);
	});

	test('unshift puts a chunk back at the front', () => {
		const r = new Readable({ read() {} });
		r.push(Buffer.from('hello world'));
		r.push(null);
		r.unshift(r.read(5));
		assert.equal(r.read(11).toString(), 'hello world');
	});

	test('push after EOF errors', async () => {
		const r = new Readable({ read() {} });
		r.push(null);
		const error = new Promise(resolve => r.once('error', resolve));
		r.push('late');
		assert.equal(((await error) as { code: string }).code, 'ERR_STREAM_PUSH_AFTER_EOF');
	});

	test('push reports back-pressure at the high water mark', () => {
		const r = new Readable({ highWaterMark: 4, read() {} });
		assert.equal(r.push(Buffer.alloc(2)), true);
		assert.equal(r.push(Buffer.alloc(4)), false);
	});

	test('objectMode counts chunks rather than bytes', async () => {
		const r = Readable.from([{ a: 1 }, { b: 2 }]);
		assert.ok(r.readableObjectMode);
		assert.deepEqual(await r.toArray(), [{ a: 1 }, { b: 2 }]);
	});

	test('pause and resume drive flowing mode', async () => {
		const r = Readable.from(['a', 'b', 'c']);
		assert.equal(r.readableFlowing, null);
		r.pause();
		assert.equal(r.isPaused(), true);
		const out: string[] = [];
		r.on('data', c => out.push(c as string));
		r.resume();
		await new Promise(resolve => r.on('end', resolve));
		assert.deepEqual(out, ['a', 'b', 'c']);
	});

	test('emits end then close, and readableEnded flips', async () => {
		const order: string[] = [];
		const r = Readable.from(['x']);
		r.on('data', () => order.push('data'));
		r.on('end', () => order.push('end'));
		r.on('close', () => order.push('close'));
		await new Promise(resolve => r.on('close', resolve));
		assert.deepEqual(order, ['data', 'end', 'close']);
		assert.equal(r.readableEnded, true);
	});

	test('destroy emits the error and marks the stream destroyed', async () => {
		const r = new Readable({ read() {} });
		const error = new Error('nope');
		const seen = new Promise(resolve => r.once('error', resolve));
		r.destroy(error);
		assert.equal(await seen, error);
		assert.equal(r.destroyed, true);
		assert.equal(r.errored, error);
	});

	test('_construct defers reads until it completes', async () => {
		const order: string[] = [];
		const r = new Readable({
			construct(cb) {
				order.push('construct');
				setTimeout(cb, 1);
			},
			read() {
				order.push('read');
				this.push('c');
				this.push(null);
			},
		});
		assert.equal((await r.toArray()).join(''), 'c');
		assert.deepEqual(order, ['construct', 'read']);
	});

	test('_read errors destroy the stream', async () => {
		const r = new Readable({
			read() {
				throw new Error('read failed');
			},
		});
		await assert.rejects(r.toArray(), { message: 'read failed' });
	});

	test('Readable.from accepts strings, buffers and async iterables', async () => {
		assert.deepEqual(await Readable.from('abc').toArray(), ['abc']);
		assert.deepEqual(await Readable.from(Buffer.from('xy')).toArray(), [Buffer.from('xy')]);
		assert.deepEqual(
			await Readable.from(
				(async function* () {
					yield await Promise.resolve(1);
					yield 2;
				})()
			).toArray(),
			[1, 2]
		);
	});

	test('Readable.from rejects a null value', async () => {
		await assert.rejects(Readable.from([null] as never).toArray(), { code: 'ERR_STREAM_NULL_VALUES' });
	});

	test('an abort signal destroys the stream', async () => {
		const ac = new AbortController();
		const r = new Readable({ read() {}, signal: ac.signal });
		ac.abort();
		await assert.rejects(r.toArray(), { name: 'AbortError' });
	});

	test('leaving iteration early destroys the stream', async () => {
		const r = Readable.from(['a', 'b', 'c']);
		for await (const chunk of r) {
			assert.equal(chunk, 'a');
			break;
		}
		assert.equal(r.destroyed, true);
	});

	test('iterator({ destroyOnReturn: false }) leaves the stream alive', async () => {
		const r = Readable.from(['a', 'b', 'c']);
		for await (const chunk of r.iterator({ destroyOnReturn: false })) {
			assert.equal(chunk, 'a');
			break;
		}
		assert.equal(r.destroyed, false);
		assert.deepEqual(await r.toArray(), ['b', 'c']);
	});

	test('readable event pairs with read()', async () => {
		const r = Readable.from(['a', 'b']);
		const out: string[] = [];
		await new Promise<void>(resolve => {
			r.on('readable', () => {
				let chunk;
				while ((chunk = r.read()) !== null) out.push(chunk as string);
			});
			r.on('end', resolve);
		});
		assert.deepEqual(out, ['a', 'b']);
	});

	test('wrap consumes an old-style stream', async () => {
		const legacy = new (await import('node:events')).EventEmitter() as any;
		legacy.pause = () => {};
		legacy.resume = () => {};
		const r = new Readable({ objectMode: true }).wrap(legacy);
		queueMicrotask(() => {
			legacy.emit('data', 'a');
			legacy.emit('data', 'b');
			legacy.emit('end');
		});
		assert.deepEqual(await r.toArray(), ['a', 'b']);
	});

	test('rejects an invalid chunk type', async () => {
		const r = new Readable({ read() {} });
		const error = new Promise(resolve => r.once('error', resolve));
		r.push(42);
		assert.equal(((await error) as { code: string }).code, 'ERR_INVALID_ARG_TYPE');
	});
});
