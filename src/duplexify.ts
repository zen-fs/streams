// SPDX-License-Identifier: LGPL-3.0-or-later

/**
 * Backs `Duplex.from()`: turns iterables, async generator functions, promises, web streams
 * and `{ readable, writable }` pairs into a Duplex.
 * @module
 */

import type { DestroyCallback } from './destroy.js';
import type { ReadableWritablePair } from 'node:stream/web';
import type { WriteCallback } from './writable.js';

import { destroyer } from './destroy.js';
import { Duplex } from './duplex.js';
import { eos } from './end-of-stream.js';
import { AbortError, ERR_INVALID_ARG_TYPE, ERR_INVALID_RETURN_VALUE } from './errors.js';
import { from } from './from.js';
import { Readable } from './readable.js';
import { nextTick } from './util.js';
import {
	isDuplexNodeStream,
	isIterable,
	isNodeStream,
	isReadable,
	isReadableNodeStream,
	isReadableStream,
	isWritable,
	isWritableNodeStream,
	isWritableStream,
} from './utils.js';
import { Writable } from './writable.js';

export function duplexify(body: any, name: string): Duplex {
	if (isDuplexNodeStream(body)) return body as Duplex;

	if (isReadableNodeStream(body)) return fromPair({ readable: body as Readable });
	if (isWritableNodeStream(body)) return fromPair({ writable: body as Writable });
	if (isNodeStream(body)) return fromPair({});

	if (isReadableStream(body)) return fromPair({ readable: Readable.fromWeb(body) });
	if (isWritableStream(body)) return fromPair({ writable: Writable.fromWeb(body) });

	if (typeof body === 'function') {
		const { value, write, final, destroy } = fromAsyncGenerator(body);

		// The body may be a plain constructor rather than an async generator function.
		if (isDuplexNodeStream(value)) return value as Duplex;

		if (isIterable(value)) {
			return from(Duplex, value as AsyncIterable<unknown>, { objectMode: true, write, final, destroy } as never) as Duplex;
		}

		const then = (value as PromiseLike<unknown>)?.then;
		if (typeof then === 'function') {
			let d: Duplex;

			const promise = then.call(
				value,
				(val: unknown) => {
					if (val != null) throw new ERR_INVALID_RETURN_VALUE('nully', 'body', val);
				},
				(err: Error) => destroyer(d, err)
			) as Promise<void>;

			return (d = new Duplex({
				objectMode: true,
				readable: false,
				write,
				final(cb: WriteCallback) {
					final(() => {
						promise.then(
							() => nextTick(cb, null),
							(err: Error) => nextTick(cb, err)
						);
					});
				},
				destroy,
			}));
		}

		throw new ERR_INVALID_RETURN_VALUE('Iterable, AsyncIterable or AsyncFunction', name, value);
	}

	if (body instanceof Blob) return duplexify(body.arrayBuffer(), name);

	if (isIterable(body)) return from(Duplex, body as Iterable<unknown>, { objectMode: true, writable: false } as never) as Duplex;

	if (isReadableStream(body?.readable) && isWritableStream(body?.writable)) return Duplex.fromWeb(body as ReadableWritablePair);

	if (typeof body?.writable === 'object' || typeof body?.readable === 'object') {
		const readable = body.readable ? (isReadableNodeStream(body.readable) ? body.readable : duplexify(body.readable, name)) : undefined;
		const writable = body.writable ? (isWritableNodeStream(body.writable) ? body.writable : duplexify(body.writable, name)) : undefined;
		return fromPair({ readable, writable });
	}

	const then = body?.then;
	if (typeof then === 'function') {
		let d: Duplex;

		then.call(
			body,
			(val: unknown) => {
				if (val != null) d.push(val);
				d.push(null);
			},
			(err: Error) => destroyer(d, err)
		);

		return (d = new Duplex({ objectMode: true, writable: false, read() {} }));
	}

	throw new ERR_INVALID_ARG_TYPE(
		name,
		['Blob', 'ReadableStream', 'WritableStream', 'Stream', 'Iterable', 'AsyncIterable', 'Function', '{ readable, writable } pair', 'Promise'],
		body
	);
}

interface GenChannel {
	value: unknown;
	write(chunk: any, encoding: BufferEncoding, cb: WriteCallback): void;
	final(cb: WriteCallback): void;
	destroy(err: Error | null, cb: DestroyCallback): void;
}

/** Drives an async generator function from `write()`/`final()` calls, one chunk at a time. */
function fromAsyncGenerator(fn: (source: AsyncGenerator<unknown>, opts: { signal: AbortSignal }) => unknown): GenChannel {
	type Signal = { chunk?: unknown; done: boolean; cb: WriteCallback };
	let { promise, resolve } = Promise.withResolvers<Signal>();
	const ac = new AbortController();
	const signal = ac.signal;

	const value = fn(
		(async function* () {
			for (;;) {
				const pending = promise;
				const { chunk, done, cb } = await pending;
				nextTick(cb);
				if (done) return;
				if (signal.aborted) throw new AbortError(undefined, { cause: signal.reason });
				({ promise, resolve } = Promise.withResolvers<Signal>());
				yield chunk;
			}
		})(),
		{ signal }
	);

	return {
		value,
		write(chunk, encoding, cb) {
			resolve({ chunk, done: false, cb });
		},
		final(cb) {
			resolve({ done: true, cb });
		},
		destroy(err, cb) {
			ac.abort(err);
			// Unblock a generator waiting on the next write, so the readable side can see the abort.
			resolve({ done: true, cb: () => {} });
			cb(err);
		},
	};
}

/** Wraps a separate readable and writable into one Duplex, forwarding back-pressure both ways. */
function fromPair(pair: { readable?: Readable; writable?: Writable }): Duplex {
	const r = pair.readable && typeof pair.readable.read !== 'function' ? Readable.wrap(pair.readable) : pair.readable;
	const w = pair.writable;

	let readable = !!isReadable(r);
	let writable = !!isWritable(w);

	let ondrain: WriteCallback | null = null;
	let onfinish: WriteCallback | null = null;
	let onreadable: (() => void) | null = null;
	let onclose: DestroyCallback | null = null;

	function onfinished(err?: Error | null): void {
		const cb = onclose;
		onclose = null;

		if (cb) cb(err);
		else if (err) d.destroy(err);
	}

	const d = new Duplex({
		readableObjectMode: !!r?.readableObjectMode,
		writableObjectMode: !!w?.writableObjectMode,
		readable,
		writable,
	});

	if (writable) {
		eos(w, err => {
			writable = false;
			if (err) destroyer(r, err);
			onfinished(err);
		});

		d._write = function (chunk, encoding, callback) {
			if (w!.write(chunk, encoding)) callback();
			else ondrain = callback;
		};

		d._final = function (callback) {
			w!.end();
			onfinish = callback;
		};

		w!.on('drain', () => {
			const cb = ondrain;
			ondrain = null;
			cb?.();
		});

		w!.on('finish', () => {
			const cb = onfinish;
			onfinish = null;
			cb?.();
		});
	}

	if (readable) {
		eos(r, err => {
			readable = false;
			if (err) destroyer(w, err);
			onfinished(err);
		});

		r!.on('readable', () => {
			const cb = onreadable;
			onreadable = null;
			cb?.();
		});

		r!.on('end', () => void d.push(null));

		const read = (): void => {
			for (;;) {
				const buf = r!.read();
				if (buf === null) {
					onreadable = read;
					return;
				}
				if (!d.push(buf)) return;
			}
		};
		d._read = read;
	}

	d._destroy = function (err, callback) {
		if (!err && onclose !== null) err = new AbortError();

		onreadable = null;
		ondrain = null;
		onfinish = null;

		if (onclose === null) {
			callback(err);
		} else {
			onclose = callback;
			destroyer(w, err);
			destroyer(r, err);
		}
	};

	return d;
}
