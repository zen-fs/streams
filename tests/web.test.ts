// SPDX-License-Identifier: LGPL-3.0-or-later
import { Duplex, promises, Readable, Writable } from '@zenfs/streams';
import assert from 'node:assert/strict';
import { suite, test } from 'node:test';

suite('web interop', () => {
	test('Readable.fromWeb consumes a ReadableStream', async () => {
		const web = new ReadableStream({
			start(controller) {
				controller.enqueue('a');
				controller.enqueue('b');
				controller.close();
			},
		});
		assert.deepEqual(await Readable.fromWeb(web, { objectMode: true }).toArray(), ['a', 'b']);
	});

	test('Readable.toWeb produces a readable ReadableStream', async () => {
		const web = Readable.toWeb(Readable.from(['x', 'y']));
		const out: unknown[] = [];
		for await (const chunk of web) out.push(chunk);
		assert.deepEqual(out, ['x', 'y']);
	});

	test('round-trips through the web and back', async () => {
		const web = Readable.toWeb(Readable.from(['a', 'b', 'c']));
		assert.equal((await Readable.fromWeb(web, { objectMode: true }).toArray()).join(''), 'abc');
	});

	test('a ReadableStream error reaches the Readable', async () => {
		const web = new ReadableStream({
			start(controller) {
				controller.error(new Error('web failed'));
			},
		});
		await assert.rejects(Readable.fromWeb(web).toArray(), { message: 'web failed' });
	});

	test('Writable.fromWeb writes into a WritableStream', async () => {
		const seen: string[] = [];
		const web = new WritableStream({
			write(chunk) {
				seen.push(String(chunk));
			},
		});
		const w = Writable.fromWeb(web);
		w.write('a');
		await new Promise(resolve => w.end('b', resolve));
		assert.deepEqual(seen, ['a', 'b']);
	});

	test('Writable.toWeb produces a writable WritableStream', async () => {
		const seen: string[] = [];
		const w = new Writable({
			write(chunk, encoding, callback) {
				seen.push(chunk.toString());
				callback();
			},
		});
		const web = Writable.toWeb(w);
		const writer = web.getWriter();
		await writer.write('a');
		await writer.write('b');
		await writer.close();
		assert.deepEqual(seen, ['a', 'b']);
	});

	test('Duplex.fromWeb bridges a readable/writable pair', async () => {
		const seen: string[] = [];
		const pair = {
			readable: new ReadableStream({
				start(controller) {
					controller.enqueue('down');
					controller.close();
				},
			}),
			writable: new WritableStream({
				write(chunk) {
					seen.push(String(chunk));
				},
			}),
		};
		const d = Duplex.fromWeb(pair, { objectMode: true });
		d.end('up');
		assert.deepEqual(await d.toArray(), ['down']);
		assert.deepEqual(seen, ['up']);
	});

	test('Duplex.toWeb produces both halves', async () => {
		const seen: string[] = [];
		const d = new Duplex({
			read() {
				this.push('r');
				this.push(null);
			},
			write(chunk, encoding, callback) {
				seen.push(chunk.toString());
				callback();
			},
		});
		const { readable, writable } = Duplex.toWeb(d);

		const writer = writable.getWriter();
		await writer.write('w');
		await writer.close();

		const out: unknown[] = [];
		for await (const chunk of readable) out.push(Buffer.from(chunk as Uint8Array).toString());

		assert.deepEqual(seen, ['w']);
		assert.deepEqual(out, ['r']);
	});

	test('Duplex.fromWeb rejects a bare ReadableStream', () => {
		assert.throws(() => Duplex.fromWeb(new ReadableStream() as never), { code: 'ERR_INVALID_ARG_TYPE' });
	});

	test('finished() works on a web stream produced by toWeb', async () => {
		const web = Readable.toWeb(Readable.from(['a']));
		for await (const chunk of web) void chunk;
		await promises.finished(web as never);
	});
});
