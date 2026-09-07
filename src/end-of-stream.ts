// SPDX-License-Identifier: LGPL-3.0-or-later

/**
 * Ported from https://github.com/mafintosh/end-of-stream with permission from the author,
 * Mathias Buus (@mafintosh).
 * @module
 */

import type { FinishedOptions } from 'node:stream';
import type { Stream } from './legacy.js';

import { AbortError, ERR_INVALID_ARG_TYPE, ERR_STREAM_PREMATURE_CLOSE } from './errors.js';
import {
	addAbortListener,
	kEmptyObject,
	nextTick,
	nop,
	once,
	validateAbortSignal,
	validateBoolean,
	validateFunction,
	validateObject,
} from './util.js';
import {
	isClosed,
	isNodeStream,
	isReadable,
	isReadableErrored,
	isReadableFinished,
	isReadableNodeStream,
	isReadableStream,
	isWritable,
	isWritableErrored,
	isWritableFinished,
	isWritableNodeStream,
	isWritableStream,
	kIsClosedPromise,
	willEmitClose as _willEmitClose,
} from './utils.js';

export type FinishedCallback = (this: unknown, err?: Error | null) => void;

type Probe = Record<string, any>;

function getErrored(stream: unknown): Error | null {
	const errored = isWritableErrored(stream) || isReadableErrored(stream);
	return (typeof errored !== 'boolean' && errored) || null;
}

function getOnCloseError(
	stream: unknown,
	readable: boolean,
	readableFinished: boolean | null,
	writable: boolean,
	writableFinished: boolean | null
): Error | null {
	const errored = getErrored(stream);
	if (errored) return errored;

	if (readable && !readableFinished && isReadableNodeStream(stream, true) && !isReadableFinished(stream, false))
		return new ERR_STREAM_PREMATURE_CLOSE();
	if (writable && !writableFinished && !isWritableFinished(stream, false)) return new ERR_STREAM_PREMATURE_CLOSE();

	return null;
}

/**
 * Calls `callback` when `stream` is no longer readable, writable, or has errored.
 * Returns a function that detaches the listeners.
 */
export function eos(stream: unknown, callback: FinishedCallback): () => void;
export function eos(stream: unknown, options: FinishedOptions | null | undefined, callback: FinishedCallback): () => void;
export function eos(stream: unknown, options: FinishedOptions | null | undefined | FinishedCallback, callback?: FinishedCallback): () => void {
	let opts: FinishedOptions;
	if (typeof options === 'function') {
		callback = options;
		opts = kEmptyObject;
	} else if (options == null) {
		opts = kEmptyObject;
	} else {
		validateObject(options, 'options');
		opts = options;
	}
	validateFunction(callback, 'callback');
	validateAbortSignal(opts.signal, 'options.signal');

	if (isReadableStream(stream) || isWritableStream(stream)) return eosWeb(stream, opts, callback);

	if (!isNodeStream(stream)) throw new ERR_INVALID_ARG_TYPE('stream', ['ReadableStream', 'WritableStream', 'Stream'], stream);

	const s = stream as Probe as Stream & Probe;

	const readable = opts.readable ?? isReadableNodeStream(stream);
	const writable = opts.writable ?? isWritableNodeStream(stream);

	let willEmitClose = !!_willEmitClose(stream) && isReadableNodeStream(stream) === readable && isWritableNodeStream(stream) === writable;
	let writableFinished = isWritableFinished(stream, false);
	let readableFinished = isReadableFinished(stream, false);

	const wState = s._writableState;
	const rState = s._readableState;

	/** `undefined` while undetermined, `null` for a clean settle, otherwise the error to report. */
	let immediate: Error | null | undefined;

	if (isClosed(stream)) {
		immediate = getOnCloseError(stream, readable, readableFinished, writable, writableFinished);
	} else if (wState?.errorEmitted || rState?.errorEmitted) {
		if (!willEmitClose) immediate = getErrored(stream);
	} else if (
		!readable
		&& (!willEmitClose || isReadable(stream))
		&& (writableFinished || isWritable(stream) === false)
		&& (wState == null || wState.pendingcb === undefined || wState.pendingcb === 0)
	) {
		immediate = getErrored(stream);
	} else if (!writable && (!willEmitClose || isWritable(stream)) && (readableFinished || isReadable(stream) === false)) {
		immediate = getErrored(stream);
	}

	let cleanup = (): void => {
		callback = nop;
	};

	if (immediate !== undefined) {
		if (opts.error !== false) {
			s.on('error', nop);
			cleanup = () => {
				callback = nop;
				s.removeListener('error', nop);
			};
		}
	} else if (opts.signal?.aborted) {
		immediate = new AbortError(undefined, { cause: opts.signal.reason });
	}

	if (immediate !== undefined) {
		const settled = immediate;
		nextTick(() => (settled === null ? callback!.call(stream) : callback!.call(stream, settled)));
		return cleanup;
	}

	callback = once(callback);

	const onlegacyfinish = (): void => {
		if (!s.writable) onfinish();
	};

	const onfinish = (): void => {
		writableFinished = true;
		// A destroyed stream means userland is doing something unusual, so `willEmitClose` can no longer be trusted.
		if (s.destroyed) willEmitClose = false;

		if (willEmitClose && (!s.readable || readable)) return;
		if (!readable || readableFinished) callback!.call(stream);
	};

	const onend = (): void => {
		readableFinished = true;
		if (s.destroyed) willEmitClose = false;

		if (willEmitClose && (!s.writable || writable)) return;
		if (!writable || writableFinished) callback!.call(stream);
	};

	const onerror = (err: Error): void => {
		callback!.call(stream, err);
	};

	const onclose = (): void => {
		const error = getOnCloseError(stream, readable, readableFinished, writable, writableFinished);
		callback!.call(stream, error ?? undefined);
	};

	if (writable && !wState) {
		s.on('end', onlegacyfinish);
		s.on('close', onlegacyfinish);
	}

	// Not every stream emits 'close' after 'aborted'.
	if (!willEmitClose && typeof s.aborted === 'boolean') s.on('aborted', onclose);

	s.on('end', onend);
	s.on('finish', onfinish);
	if (opts.error !== false) s.on('error', onerror);
	s.on('close', onclose);

	cleanup = () => {
		callback = nop;
		s.removeListener('aborted', onclose);
		s.removeListener('end', onlegacyfinish);
		s.removeListener('close', onlegacyfinish);
		s.removeListener('finish', onfinish);
		s.removeListener('end', onend);
		s.removeListener('error', onerror);
		s.removeListener('close', onclose);
	};

	if (opts.signal) {
		const abort = (): void => {
			// Held onto because cleanup() replaces it.
			const endCallback = callback!;
			cleanup();
			endCallback.call(stream, new AbortError(undefined, { cause: opts.signal!.reason }));
		};
		const disposable = addAbortListener(opts.signal, abort);
		const original = callback;
		callback = once((err?: Error | null) => {
			disposable[Symbol.dispose]();
			original.call(stream, err);
		});
	}

	return cleanup;
}

function eosWeb(stream: ReadableStream | WritableStream, options: FinishedOptions, callback: FinishedCallback): () => void {
	const closed = (stream as unknown as Record<symbol, { promise: Promise<unknown> } | undefined>)[kIsClosedPromise];
	if (!closed) {
		throw new ERR_INVALID_ARG_TYPE('stream', ['ReadableStream', 'WritableStream', 'Stream'], stream);
	}

	let done = once(callback);
	let aborted = false;

	if (options.signal) {
		const abort = (): void => {
			aborted = true;
			done.call(stream, new AbortError(undefined, { cause: options.signal!.reason }));
		};

		if (options.signal.aborted) {
			nextTick(abort);
		} else {
			const disposable = addAbortListener(options.signal, abort);
			const original = done;
			done = once((err?: Error | null) => {
				disposable[Symbol.dispose]();
				original.call(stream, err);
			});
		}
	}

	const settle = (err?: unknown): void => {
		if (!aborted) nextTick(() => done.call(stream, err as Error | undefined));
	};
	closed.promise.then(settle, settle);

	return nop;
}

export interface FinishedPromiseOptions extends FinishedOptions {
	/** Detach the listeners once the promise settles. */
	cleanup?: boolean;
}

/** Promise form of {@link eos}. */
export function finished(stream: unknown, opts?: FinishedPromiseOptions | null): Promise<void> {
	const options = opts ?? kEmptyObject;
	let autoCleanup = false;
	if ('cleanup' in options && options.cleanup !== undefined) {
		validateBoolean(options.cleanup, 'cleanup');
		autoCleanup = options.cleanup;
	}

	return new Promise<void>((resolve, reject) => {
		const cleanup = eos(stream, options, err => {
			if (autoCleanup) cleanup();
			if (err) reject(err);
			else resolve();
		});
	});
}
