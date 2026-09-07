// SPDX-License-Identifier: LGPL-3.0-or-later
import { Readable } from '@zenfs/streams';
import assert from 'node:assert/strict';
import { suite, test } from 'node:test';

const nums = (): Readable => Readable.from([1, 2, 3, 4, 5]);

suite('operators', () => {
	test('map returns a composable Readable', async () => {
		const mapped = nums().map(x => x * 2);
		assert.ok(mapped instanceof Readable);
		assert.deepEqual(await mapped.toArray(), [2, 4, 6, 8, 10]);
	});

	test('map awaits async functions', async () => {
		assert.deepEqual(
			await nums()
				.map(x => Promise.resolve(x + 1))
				.toArray(),
			[2, 3, 4, 5, 6]
		);
	});

	test('map honours concurrency', async () => {
		let inFlight = 0;
		let peak = 0;
		const out = await Readable.from([1, 2, 3, 4, 5, 6])
			.map(
				async x => {
					peak = Math.max(peak, ++inFlight);
					await new Promise(resolve => setTimeout(resolve, 2));
					inFlight--;
					return x;
				},
				{ concurrency: 3 }
			)
			.toArray();
		assert.deepEqual(out, [1, 2, 3, 4, 5, 6]);
		assert.ok(peak > 1, `expected concurrent calls, peak was ${peak}`);
	});

	test('filter keeps matching values', async () => {
		assert.deepEqual(
			await nums()
				.filter(x => x % 2 === 1)
				.toArray(),
			[1, 3, 5]
		);
	});

	test('flatMap flattens what the mapper yields', async () => {
		assert.deepEqual(
			await Readable.from([1, 2])
				.flatMap(x => [x, x])
				.toArray(),
			[1, 1, 2, 2]
		);
	});

	test('take and drop slice the stream', async () => {
		assert.deepEqual(await nums().take(2).toArray(), [1, 2]);
		assert.deepEqual(await nums().drop(3).toArray(), [4, 5]);
		assert.deepEqual(await nums().drop(1).take(2).toArray(), [2, 3]);
	});

	test('take(0) yields nothing', async () => {
		assert.deepEqual(await nums().take(0).toArray(), []);
	});

	test('reduce folds with and without an initial value', async () => {
		assert.equal(await nums().reduce((a, b) => a + b), 15);
		assert.equal(await nums().reduce((a, b) => a + b, 100), 115);
	});

	test('reduce of an empty stream without an initial value rejects', async () => {
		await assert.rejects(
			Readable.from([]).reduce((a, b) => a + b),
			/initial value/
		);
	});

	test('some, every and find short-circuit', async () => {
		assert.equal(await nums().some(x => x > 4), true);
		assert.equal(await nums().some(x => x > 9), false);
		assert.equal(await nums().every(x => x > 0), true);
		assert.equal(await nums().every(x => x > 1), false);
		assert.equal(await nums().find(x => x > 3), 4);
		assert.equal(await nums().find(x => x > 9), undefined);
	});

	test('forEach visits every value', async () => {
		const seen: number[] = [];
		await nums().forEach(x => void seen.push(x));
		assert.deepEqual(seen, [1, 2, 3, 4, 5]);
	});

	test('a stream error surfaces through an operator', async () => {
		const r = new Readable({
			objectMode: true,
			read() {
				this.destroy(new Error('mid-stream'));
			},
		});
		await assert.rejects(r.map(x => x).toArray(), { message: 'mid-stream' });
	});

	test('a mapper error surfaces', async () => {
		await assert.rejects(
			nums()
				.map(x => {
					if (x === 3) throw new Error('bad value');
					return x;
				})
				.toArray(),
			{ message: 'bad value' }
		);
	});

	test('operators reject a non-function', () => {
		assert.throws(() => nums().map(undefined as never), { code: 'ERR_INVALID_ARG_TYPE' });
	});

	test('operators are not constructors', () => {
		// @ts-expect-error 7009
		assert.throws(() => new Readable.prototype.map(), { code: 'ERR_ILLEGAL_CONSTRUCTOR' });
	});
});
