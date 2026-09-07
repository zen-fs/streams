// SPDX-License-Identifier: LGPL-3.0-or-later
import type { DestroyCallback } from './destroy.js';
import type { WriteCallback } from './writable.js';

import { addAbortSignalNoValidate } from './add-abort-signal.js';
import { destroyer } from './destroy.js';
import { Duplex } from './duplex.js';
import { eos } from './end-of-stream.js';
import { AbortError, ERR_INVALID_ARG_VALUE, ERR_MISSING_ARGS } from './errors.js';
import { pipeline, type PipelineStage } from './pipeline.js';
import { Readable } from './readable.js';
import { validateAbortSignal, validateObject } from './util.js';
import { isNodeStream, isReadable, isReadableStream, isTransformStream, isWebStream, isWritable, isWritableStream } from './utils.js';

/** Joins several streams into a single Duplex that writes into the first and reads from the last. */
export function compose(...streams: PipelineStage[]): Duplex {
	if (streams.length === 0) throw new ERR_MISSING_ARGS('streams');
	if (streams.length === 1) return Duplex.from(streams[0]);

	const orgStreams = streams.slice();

	if (typeof streams[0] === 'function') streams[0] = Duplex.from(streams[0]);
	if (typeof streams[streams.length - 1] === 'function') streams[streams.length - 1] = Duplex.from(streams[streams.length - 1]);

	for (let n = 0; n < streams.length; ++n) {
		if (!isNodeStream(streams[n]) && !isWebStream(streams[n])) continue;

		if (n < streams.length - 1 && !(isReadable(streams[n]) || isReadableStream(streams[n]) || isTransformStream(streams[n]))) {
			throw new ERR_INVALID_ARG_VALUE(`streams[${n}]`, orgStreams[n], 'must be readable');
		}
		if (n > 0 && !(isWritable(streams[n]) || isWritableStream(streams[n]) || isTransformStream(streams[n]))) {
			throw new ERR_INVALID_ARG_VALUE(`streams[${n}]`, orgStreams[n], 'must be writable');
		}
	}

	let ondrain: WriteCallback | null = null;
	let onfinish: WriteCallback | null = null;
	let onclose: DestroyCallback | null = null;

	function onfinished(err?: Error | null): void {
		const cb = onclose;
		onclose = null;

		if (cb) cb(err);
		else if (err) d.destroy(err);
		else if (!readable && !writable) d.destroy();
	}

	const head = streams[0];
	const tail = pipeline(streams as never, onfinished) as any;

	const writable = !!(isWritable(head) || isWritableStream(head) || isTransformStream(head));
	const readable = !!(isReadable(tail) || isReadableStream(tail) || isTransformStream(tail));

	const d = new Duplex({
		writableObjectMode: !!head?.writableObjectMode,
		readableObjectMode: !!tail?.readableObjectMode,
		writable,
		readable,
	});

	if (writable) {
		if (isNodeStream(head)) {
			d._write = function (chunk, encoding, callback) {
				if (head.write(chunk, encoding)) callback();
				else ondrain = callback;
			};

			d._final = function (callback) {
				head.end();
				onfinish = callback;
			};

			head.on('drain', () => {
				const cb = ondrain;
				ondrain = null;
				cb?.();
			});
		} else if (isWebStream(head)) {
			const writer = (isTransformStream(head) ? head.writable : (head as WritableStream)).getWriter();

			d._write = function (chunk, encoding, callback) {
				writer.ready.then(() => {
					writer.write(chunk).catch(() => {});
					callback();
				}, callback);
			};

			d._final = function (callback) {
				writer.ready.then(() => {
					writer.close().catch(() => {});
					onfinish = callback;
				}, callback);
			};
		}

		eos(isTransformStream(tail) ? tail.readable : tail, () => {
			const cb = onfinish;
			onfinish = null;
			cb?.();
		});
	}

	if (readable) {
		if (isNodeStream(tail)) {
			d._read = function () {
				(tail as Readable).resume();
			};

			tail.on('data', (chunk: unknown) => {
				if (!d.push(chunk)) (tail as Readable).pause();
			});

			tail.on('end', () => void d.push(null));
		} else if (isWebStream(tail)) {
			const reader = (isTransformStream(tail) ? tail.readable : (tail as ReadableStream)).getReader();

			d._read = function () {
				void (async () => {
					for (;;) {
						try {
							const { value, done } = await reader.read();
							if (done) {
								d.push(null);
								return;
							}
							if (!d.push(value)) return;
						} catch {
							return;
						}
					}
				})();
			};
		}
	}

	d._destroy = function (err, callback) {
		if (!err && onclose !== null) err = new AbortError();

		ondrain = null;
		onfinish = null;

		if (isNodeStream(tail)) destroyer(tail, err);

		if (onclose === null) callback(err);
		else onclose = callback;
	};

	return d;
}

// `Readable.prototype.compose` lives here rather than in `readable.js`: importing this module
// from there would evaluate `Duplex` while `Readable` is still being defined.
Object.defineProperty(Readable.prototype, 'compose', {
	value: function compose_(this: Readable, stream: PipelineStage, options?: { signal?: AbortSignal }): Duplex {
		if (options != null) validateObject(options, 'options');
		if (options?.signal != null) validateAbortSignal(options.signal, 'options.signal');

		const composed = compose(this, stream);
		if (options?.signal) addAbortSignalNoValidate(options.signal, composed);
		return composed;
	},
	enumerable: false,
	configurable: true,
	writable: true,
});
