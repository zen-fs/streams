// SPDX-License-Identifier: LGPL-3.0-or-later
import { Readable, toAsyncStreamable } from '@zenfs/streams';
import { Buffer } from 'node:buffer';
import assert from 'node:assert/strict';
import { suite, test } from 'node:test';

suite('stream/iter protocol', () => {
	test('Readable implements toAsyncStreamable', () => {
		assert.equal(typeof Readable.prototype[toAsyncStreamable], 'function');
		assert.equal(toAsyncStreamable, Symbol.for('Stream.toAsyncStreamable'));
	});

	test('yields batches of Uint8Array for a byte stream', async () => {
		const r = new Readable({
			read() {
				this.push('a');
				this.push('b');
				this.push(null);
			},
		});

		const batches: Uint8Array[][] = [];
		for await (const batch of r[toAsyncStreamable]()) batches.push(batch);

		assert.ok(batches.length > 0);
		for (const batch of batches) {
			assert.ok(Array.isArray(batch));
			for (const chunk of batch) assert.ok(chunk instanceof Uint8Array);
		}
		assert.equal(Buffer.concat(batches.flat()).toString(), 'ab');
	});

	test('normalizes object-mode values to Uint8Array', async () => {
		const r = Readable.from(['x', 'y']);
		const batches: Uint8Array[][] = [];
		for await (const batch of r[toAsyncStreamable]()) batches.push(batch);
		assert.equal(Buffer.concat(batches.flat()).toString(), 'xy');
	});

	test('drains several buffered chunks into one batch', async () => {
		const r = new Readable({
			read() {
				for (let i = 0; i < 8; i++) this.push(Buffer.from([i]));
				this.push(null);
			},
		});
		const batches: Uint8Array[][] = [];
		for await (const batch of r[toAsyncStreamable]()) batches.push(batch);
		assert.ok(
			batches.some(b => b.length > 1),
			'expected at least one multi-chunk batch'
		);
	});

	test('surfaces a stream error', async () => {
		const r = new Readable({
			read() {
				this.destroy(new Error('iter failed'));
			},
		});
		await assert.rejects(
			async () => {
				for await (const batch of r[toAsyncStreamable]()) void batch;
			},
			{ message: 'iter failed' }
		);
	});

	test('exposes the source stream', () => {
		const r = Readable.from(['a']);
		assert.equal((r[toAsyncStreamable]() as { stream?: unknown }).stream, r);
	});
});
