// SPDX-License-Identifier: LGPL-3.0-or-later
import { compose, finished, PassThrough, pipeline, promises, Readable, Transform, Writable } from '@zenfs/streams';
import assert from 'node:assert/strict';
import { suite, test } from 'node:test';

const upper = (): Transform =>
	new Transform({
		transform(chunk, encoding, callback) {
			callback(null, chunk.toString().toUpperCase());
		},
	});

function collector() {
	const out: string[] = [];
	const w = new Writable({
		write(chunk, encoding, callback) {
			out.push(chunk.toString());
			callback();
		},
	});
	return { w, out };
}

suite('pipeline', () => {
	test('connects a source, a transform and a sink', async () => {
		const { w, out } = collector();
		await new Promise<void>((resolve, reject) => pipeline(Readable.from(['a', 'b']), upper(), w, err => (err ? reject(err) : resolve())));
		assert.deepEqual(out, ['A', 'B']);
	});

	test('propagates an error and destroys every stage', async () => {
		const src = new Readable({
			read() {
				this.destroy(new Error('source failed'));
			},
		});
		const middle = new PassThrough();
		const { w } = collector();
		await assert.rejects(promises.pipeline(src, middle, w), { message: 'source failed' });
		assert.equal(middle.destroyed, true);
		assert.equal(w.destroyed, true);
	});

	test('accepts an async generator as a stage', async () => {
		const result = await promises.pipeline(
			Readable.from(['a', 'b']),
			async function* (source: AsyncIterable<string>) {
				for await (const chunk of source) yield chunk + '!';
			},
			async function (source: AsyncIterable<string>) {
				let all = '';
				for await (const chunk of source) all += chunk;
				return all;
			}
		);
		assert.equal(result, 'a!b!');
	});

	test('requires at least two stages', () => {
		assert.throws(() => pipeline(Readable.from(['a']), () => {}), { code: 'ERR_MISSING_ARGS' });
	});

	test('an abort signal cancels the pipeline', async () => {
		const ac = new AbortController();
		const src = new Readable({ read() {} });
		const { w } = collector();
		const done = promises.pipeline(src, w, { signal: ac.signal });
		ac.abort();
		await assert.rejects(done, { name: 'AbortError' });
	});

	test('pipes into a web WritableStream', async () => {
		const seen: string[] = [];
		const web = new WritableStream({
			write(chunk) {
				seen.push(chunk.toString());
			},
		});
		await promises.pipeline(Readable.from(['a', 'b']), web);
		assert.deepEqual(seen, ['a', 'b']);
	});
});

suite('finished', () => {
	test('resolves once a stream ends', async () => {
		const r = Readable.from(['a']);
		r.resume();
		await promises.finished(r);
		assert.equal(r.readableEnded, true);
	});

	test('resolves for an already-finished stream', async () => {
		const r = Readable.from(['a']);
		await r.toArray();
		await promises.finished(r);
	});

	test('rejects with the stream error', async () => {
		const r = new Readable({ read() {} });
		const done = promises.finished(r);
		r.destroy(new Error('gone'));
		await assert.rejects(done, { message: 'gone' });
	});

	test('the top-level finished is the callback form, not the promise one', () => {
		const r = Readable.from(['a']);
		r.resume();
		assert.equal(typeof finished(r, () => {}), 'function');
	});

	test('the callback form returns a cleanup function', async () => {
		const r = new Readable({ read() {} });
		let called = false;
		const cleanup = finished(r, () => (called = true));
		cleanup();
		r.destroy();
		await new Promise(resolve => setTimeout(resolve, 5));
		assert.equal(called, false);
	});

	test('reports a premature close', async () => {
		const r = new Readable({ read() {} });
		const done = promises.finished(r);
		r.destroy();
		await assert.rejects(done, { code: 'ERR_STREAM_PREMATURE_CLOSE' });
	});
});

suite('compose', () => {
	test('joins transforms into one Duplex', async () => {
		const c = compose(
			upper(),
			new Transform({
				transform(chunk, encoding, callback) {
					callback(null, chunk + '!');
				},
			})
		);
		c.end('hi');
		assert.equal((await c.toArray()).join(''), 'HI!');
	});

	test('Readable.prototype.compose is installed', async () => {
		const composed = Readable.from(['a', 'b']).compose(upper());
		assert.equal((await composed.toArray()).join(''), 'AB');
	});

	test('rejects a stage that cannot be written to', () => {
		assert.throws(() => compose(Readable.from(['a']), Readable.from(['b'])), { code: 'ERR_INVALID_ARG_VALUE' });
	});
});
