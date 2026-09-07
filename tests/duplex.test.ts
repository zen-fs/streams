// SPDX-License-Identifier: LGPL-3.0-or-later
import { Duplex, duplexPair, PassThrough, Readable, Stream, Transform, Writable } from '@zenfs/streams';
import assert from 'node:assert/strict';
import { suite, test } from 'node:test';

suite('Duplex', () => {
	test('is an instance of every stream class', () => {
		const d = new Duplex({
			read() {},
			write(c, e, cb) {
				cb();
			},
		});
		assert.ok(d instanceof Duplex);
		assert.ok(d instanceof Readable);
		assert.ok(d instanceof Writable);
		assert.ok(d instanceof Stream);
	});

	test('carries the writable half of the prototype', () => {
		const d = new Duplex({
			read() {},
			write(c, e, cb) {
				cb();
			},
		});
		for (const key of ['write', 'end', 'cork', 'uncork', 'setDefaultEncoding', '_write'] as const) {
			assert.equal(typeof d[key], 'function', key);
		}
		for (const key of ['writable', 'writableEnded', 'writableFinished', 'writableLength', 'writableHighWaterMark'] as const) {
			assert.notEqual(d[key], undefined, key);
		}
	});

	test('both sides work independently', async () => {
		const written: string[] = [];
		const d = new Duplex({
			read() {
				this.push('out');
				this.push(null);
			},
			write(chunk, encoding, callback) {
				written.push(chunk.toString());
				callback();
			},
		});
		d.end('in');
		assert.equal((await d.toArray()).join(''), 'out');
		assert.deepEqual(written, ['in']);
	});

	test('is destroyed only once both sides are', () => {
		const d = new Duplex({
			read() {},
			write(c, e, cb) {
				cb();
			},
		});
		assert.equal(d.destroyed, false);
		d._readableState.destroyed = true;
		assert.equal(d.destroyed, false);
		d._writableState.destroyed = true;
		assert.equal(d.destroyed, true);
	});

	test('readable: false makes a write-only Duplex', async () => {
		const d = new Duplex({
			readable: false,
			write(c, e, cb) {
				cb();
			},
		});
		assert.equal(d.readable, false);
		assert.equal(d.writable, true);
		await new Promise(resolve => d.end('x', resolve));
	});

	test('allowHalfOpen: false ends the writable side with the readable one', async () => {
		const d = new Duplex({
			allowHalfOpen: false,
			read() {
				this.push(null);
			},
			write(c, e, cb) {
				cb();
			},
		});
		d.resume();
		await new Promise(resolve => d.on('finish', resolve));
		assert.equal(d.writableEnded, true);
	});

	test('Duplex.from wraps an async generator function', async () => {
		const d = Duplex.from(async function* (source: AsyncIterable<Buffer>) {
			for await (const chunk of source) yield chunk.toString().toUpperCase();
		});
		d.end('hey');
		assert.equal((await d.toArray()).join(''), 'HEY');
	});

	test('Duplex.from wraps an iterable', async () => {
		assert.deepEqual(await Duplex.from(['a', 'b']).toArray(), ['a', 'b']);
	});
});

suite('Transform', () => {
	test('maps every chunk', async () => {
		const t = new Transform({
			transform(chunk, encoding, callback) {
				callback(null, chunk.toString().toUpperCase());
			},
		});
		t.end('abc');
		assert.equal((await t.toArray()).join(''), 'ABC');
	});

	test('_flush appends a trailing chunk', async () => {
		const t = new Transform({
			transform(chunk, encoding, callback) {
				callback(null, chunk);
			},
			flush(callback) {
				callback(null, '!');
			},
		});
		t.end('hi');
		assert.equal((await t.toArray()).join(''), 'hi!');
	});

	test('a transform error destroys the stream', async () => {
		const t = new Transform({
			transform(chunk, encoding, callback) {
				callback(new Error('bad chunk'));
			},
		});
		t.end('x');
		await assert.rejects(t.toArray(), { message: 'bad chunk' });
	});

	test('an unimplemented _transform throws', () => {
		const t = new Transform({});
		assert.throws(() => t.end('x'), { code: 'ERR_METHOD_NOT_IMPLEMENTED' });
	});
});

suite('PassThrough', () => {
	test('passes chunks through unchanged', async () => {
		const p = new PassThrough();
		p.end('unchanged');
		assert.equal((await p.toArray()).join(''), 'unchanged');
	});
});

suite('duplexPair', () => {
	test('what is written to one side is read from the other', async () => {
		const [a, b] = duplexPair();
		a.end('ping');
		const out: string[] = [];
		for await (const chunk of b) out.push(chunk.toString());
		assert.deepEqual(out, ['ping']);
	});

	test('works in both directions', async () => {
		const [a, b] = duplexPair();
		a.write('from-a');
		b.write('from-b');
		a.end();
		b.end();
		assert.equal((await b.toArray()).join(''), 'from-a');
		assert.equal((await a.toArray()).join(''), 'from-b');
	});
});
