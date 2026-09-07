// SPDX-License-Identifier: LGPL-3.0-or-later

/**
 * Ported from https://github.com/mafintosh/pump with permission from the author,
 * Mathias Buus (@mafintosh).
 * @module
 */

import type { Destroyable } from './destroy.js';
import type { PipelineOptions } from 'node:stream/promises';
import type { Writable } from './writable.js';

import { destroyer } from './destroy.js';
import { Duplex } from './duplex.js';
import { eos } from './end-of-stream.js';
import {
	AbortError,
	aggregateTwoErrors,
	ERR_INVALID_ARG_TYPE,
	ERR_INVALID_RETURN_VALUE,
	ERR_MISSING_ARGS,
	ERR_STREAM_DESTROYED,
	ERR_STREAM_PREMATURE_CLOSE,
	ERR_STREAM_UNABLE_TO_PIPE,
} from './errors.js';
import { PassThrough } from './passthrough.js';
import { Readable } from './readable.js';
import { addAbortListener, nextTick, once, validateAbortSignal, validateFunction } from './util.js';
import {
	isIterable,
	isNodeStream,
	isReadable,
	isReadableFinished,
	isReadableNodeStream,
	isReadableStream,
	isTransformStream,
	isWebStream,
} from './utils.js';

export type PipelineCallback = (err: Error | null, value?: unknown) => void;

/** Anything `pipeline()` accepts as one of its stages. */
export type PipelineStage = any;

function stageDestroyer(stream: unknown, reading: boolean, writing: boolean): { destroy: (err?: Error | null) => void; cleanup: () => void } {
	let finished = false;
	(stream as Destroyable).on('close', () => (finished = true));

	const cleanup = eos(stream, { readable: reading, writable: writing }, err => {
		finished = !err;
	});

	return {
		destroy: (err?: Error | null) => {
			if (finished) return;
			finished = true;
			destroyer(stream, err || new ERR_STREAM_DESTROYED('pipe'));
		},
		cleanup,
	};
}

function makeAsyncIterable(val: unknown): AsyncIterable<unknown> | Iterable<unknown> {
	if (isIterable(val)) return val as AsyncIterable<unknown>;
	// Legacy streams are not iterable on their own.
	if (isReadableNodeStream(val)) return fromReadable(val as Readable);
	throw new ERR_INVALID_ARG_TYPE('val', ['Readable', 'Iterable', 'AsyncIterable'], val);
}

async function* fromReadable(val: Readable): AsyncGenerator<unknown> {
	yield* Readable.prototype[Symbol.asyncIterator].call(val);
}

async function pumpToNode(
	iterable: AsyncIterable<unknown> | Iterable<unknown>,
	writable: Writable,
	finish: (err?: Error | null) => void,
	end: boolean
): Promise<void> {
	let error: Error | undefined;
	/** Settled by `drain`, or by the writable finishing or failing. */
	let pending: PromiseWithResolvers<void> | null = null;

	const resume = (err?: Error | null): void => {
		if (err) error = err;
		if (!pending) return;
		const gate = pending;
		pending = null;
		if (error) gate.reject(error);
		else gate.resolve();
	};

	const wait = (): Promise<void> => {
		if (error) return Promise.reject(error);
		pending = Promise.withResolvers();
		return pending.promise;
	};

	writable.on('drain', resume);
	const cleanup = eos(writable, { readable: false }, resume);

	try {
		if (writable.writableNeedDrain) await wait();

		for await (const chunk of iterable) {
			if (!writable.write(chunk)) await wait();
		}

		if (end) {
			writable.end();
			await wait();
		}

		finish();
	} catch (err: any) {
		finish(error !== err ? (aggregateTwoErrors(error, err as Error) ?? undefined) : (err as Error));
	} finally {
		cleanup();
		writable.off('drain', resume);
	}
}

async function pumpToWeb(
	readable: AsyncIterable<unknown> | Iterable<unknown>,
	target: WritableStream | TransformStream,
	finish: (err?: Error | null) => void,
	end: boolean
): Promise<void> {
	const writable = isTransformStream(target) ? target.writable : target;

	// https://streams.spec.whatwg.org/#example-manual-write-with-backpressure
	const writer = writable.getWriter();
	try {
		for await (const chunk of readable) {
			await writer.ready;
			writer.write(chunk).catch(() => {});
		}

		await writer.ready;
		if (end) await writer.close();

		finish();
	} catch (err: any) {
		try {
			await writer.abort(err);
		} finally {
			finish(err as Error);
		}
	}
}

/**
 * Pipes streams and transforms together, forwarding errors and destroying every stage when
 * one of them fails. The last argument is the completion callback.
 */
export function pipeline(...streams: [...PipelineStage[], PipelineCallback]): unknown {
	const callback = streams[streams.length - 1] as PipelineCallback;
	validateFunction(callback, 'streams[stream.length - 1]');
	streams.pop();
	return pipelineImpl(streams as PipelineStage[], once(callback));
}

/** @internal */
export function pipelineImpl(streams: PipelineStage[], callback: PipelineCallback, opts?: PipelineOptions): unknown {
	if (streams.length === 1 && Array.isArray(streams[0])) streams = streams[0];

	if (streams.length < 2) throw new ERR_MISSING_ARGS('streams');

	const ac = new AbortController();
	const signal = ac.signal;
	const outerSignal = opts?.signal;

	// The listeners on a readable final stage have to be cleaned up: nodejs/node#35452.
	const lastStreamCleanup: (() => void)[] = [];

	validateAbortSignal(outerSignal, 'options.signal');

	function abort(): void {
		finishImpl(new AbortError(undefined, { cause: outerSignal?.reason }), false);
	}

	const disposable = outerSignal ? addAbortListener(outerSignal, abort) : undefined;

	let error: Error | undefined;
	let value: unknown;
	const destroys: ((err?: Error | null) => void)[] = [];

	let finishCount = 0;

	function finish(err?: Error | null): void {
		finishImpl(err, --finishCount === 0);
	}

	function finishOnlyHandleError(err?: Error | null): void {
		finishImpl(err, false);
	}

	function finishImpl(err: Error | null | undefined, final: boolean): void {
		if (err && (!error || (error as { code?: string }).code === 'ERR_STREAM_PREMATURE_CLOSE' || error.name === 'AbortError')) error = err;

		if (!error && !final) return;

		while (destroys.length) destroys.shift()!(error);

		disposable?.[Symbol.dispose]();
		ac.abort();

		if (!final) return;

		if (!error) for (const fn of lastStreamCleanup) fn();
		nextTick(callback, error ?? null, value);
	}

	let ret: unknown;

	for (let i = 0; i < streams.length; i++) {
		const stream = streams[i];
		const reading = i < streams.length - 1;
		const writing = i > 0;
		const next = i + 1 < streams.length ? streams[i + 1] : null;
		const end = reading || opts?.end !== false;
		const isLastStream = i === streams.length - 1;

		if (isNodeStream(stream)) {
			if (next !== null && (next?.closed || next?.destroyed)) throw new ERR_STREAM_UNABLE_TO_PIPE();

			if (end) {
				const { destroy, cleanup } = stageDestroyer(stream, reading, writing);
				destroys.push(destroy);
				if (isReadable(stream) && isLastStream) lastStreamCleanup.push(cleanup);
			}

			// Catches errors that arrive after the pump has already completed.
			const onError = (err: Error): void => {
				if (err && err.name !== 'AbortError' && (err as { code?: string }).code !== 'ERR_STREAM_PREMATURE_CLOSE') finishOnlyHandleError(err);
			};
			stream.on('error', onError);
			if (isReadable(stream) && isLastStream) lastStreamCleanup.push(() => stream.removeListener('error', onError));
		}

		if (i === 0) {
			if (typeof stream === 'function') {
				ret = stream({ signal });
				if (!isIterable(ret)) throw new ERR_INVALID_RETURN_VALUE('Iterable, AsyncIterable or Stream', 'source', ret);
			} else if (isIterable(stream) || isReadableNodeStream(stream) || isTransformStream(stream)) {
				ret = stream;
			} else {
				ret = Duplex.from(stream);
			}
			continue;
		}

		if (typeof stream === 'function') {
			ret = isTransformStream(ret) ? makeAsyncIterable(ret.readable) : makeAsyncIterable(ret);
			ret = stream(ret, { signal });

			if (reading) {
				if (!isIterable(ret, true)) throw new ERR_INVALID_RETURN_VALUE('AsyncIterable', `transform[${i - 1}]`, ret);
				continue;
			}

			// The last stage has to be a stream so that pipeline(...) can still be piped onward.
			const pt = new PassThrough({ objectMode: true });

			// `then` may be a getter that throws on a second read, per Promises/A+.
			const then = (ret as PromiseLike<unknown>)?.then;
			if (typeof then === 'function') {
				finishCount++;
				then.call(
					ret,
					(val: unknown) => {
						value = val;
						if (val != null) pt.write(val);
						if (end) pt.end();
						nextTick(finish);
					},
					(err: Error) => {
						pt.destroy(err);
						nextTick(finish, err);
					}
				);
			} else if (isIterable(ret, true)) {
				finishCount++;
				void pumpToNode(ret as AsyncIterable<unknown>, pt, finish, end);
			} else if (isReadableStream(ret) || isTransformStream(ret)) {
				finishCount++;
				void pumpToNode(makeAsyncIterable((ret as TransformStream).readable ?? ret), pt, finish, end);
			} else {
				throw new ERR_INVALID_RETURN_VALUE('AsyncIterable or Promise', 'destination', ret);
			}

			ret = pt;

			const { destroy, cleanup } = stageDestroyer(ret, false, true);
			destroys.push(destroy);
			if (isLastStream) lastStreamCleanup.push(cleanup);
			continue;
		}

		if (isNodeStream(stream)) {
			if (isReadableNodeStream(ret)) {
				finishCount += 2;
				const cleanup = pipe(ret as Readable, stream as Writable, finish, finishOnlyHandleError, end);
				if (isReadable(stream) && isLastStream) lastStreamCleanup.push(cleanup);
			} else if (isTransformStream(ret) || isReadableStream(ret)) {
				finishCount++;
				void pumpToNode(makeAsyncIterable((ret as TransformStream).readable ?? ret), stream as Writable, finish, end);
			} else if (isIterable(ret)) {
				finishCount++;
				void pumpToNode(ret as AsyncIterable<unknown>, stream as Writable, finish, end);
			} else {
				throw new ERR_INVALID_ARG_TYPE('val', ['Readable', 'Iterable', 'AsyncIterable', 'ReadableStream', 'TransformStream'], ret);
			}
			ret = stream;
			continue;
		}

		if (isWebStream(stream)) {
			if (isReadableNodeStream(ret)) {
				finishCount++;
				void pumpToWeb(makeAsyncIterable(ret), stream as WritableStream, finish, end);
			} else if (isReadableStream(ret) || isIterable(ret)) {
				finishCount++;
				void pumpToWeb(ret as AsyncIterable<unknown>, stream as WritableStream, finish, end);
			} else if (isTransformStream(ret)) {
				finishCount++;
				void pumpToWeb(ret.readable, stream as WritableStream, finish, end);
			} else {
				throw new ERR_INVALID_ARG_TYPE('val', ['Readable', 'Iterable', 'AsyncIterable', 'ReadableStream', 'TransformStream'], ret);
			}
			ret = stream;
			continue;
		}

		ret = Duplex.from(stream);
	}

	if (signal.aborted || outerSignal?.aborted) nextTick(abort);

	return ret;
}

function pipe(
	src: Readable,
	dst: Writable,
	finish: (err?: Error | null) => void,
	finishOnlyHandleError: (err?: Error | null) => void,
	end: boolean
): () => void {
	let ended = false;

	dst.on('close', () => {
		// The destination closed before the source was done.
		if (!ended) finishOnlyHandleError(new ERR_STREAM_PREMATURE_CLOSE());
	});

	// With `end`, the listener below ends the destination instead.
	src.pipe(dst, { end: false });

	if (end) {
		const endFn = (): void => {
			ended = true;
			dst.end();
		};

		if (isReadableFinished(src)) nextTick(endFn);
		else src.once('end', endFn);
	} else {
		finish();
	}

	eos(src, { readable: true, writable: false }, err => {
		const rState = src._readableState;
		if (err && (err as { code?: string }).code === 'ERR_STREAM_PREMATURE_CLOSE' && rState?.ended && !rState.errored && !rState.errorEmitted) {
			// Some readables emit 'close' before 'end'. Since the stream is ended and did not
			// error, honour 'end' anyway: the destination cannot tell the difference.
			src.once('end', finish).once('error', finish);
		} else {
			finish(err);
		}
	});

	return eos(dst, { readable: false, writable: true }, finish);
}
