// SPDX-License-Identifier: LGPL-3.0-or-later

/**
 * State flags shared by both stream sides and the duck-typing predicates that
 * `node:stream` exposes for classifying arbitrary values.
 * @module
 */

// `Symbol.for` so that node:stream and readable-stream can read this state too.
export const kIsDestroyed = Symbol.for('nodejs.stream.destroyed');
export const kIsErrored = Symbol.for('nodejs.stream.errored');
export const kIsReadable = Symbol.for('nodejs.stream.readable');
export const kIsWritable = Symbol.for('nodejs.stream.writable');
export const kIsDisturbed = Symbol.for('nodejs.stream.disturbed');

export const kState = Symbol('kState');
export const kOnConstructed = Symbol('kOnConstructed');

/* Flags common to ReadableState and WritableState. Each side adds its own from 1 << 9 up. */
export const kObjectMode = 1 << 0;
export const kErrorEmitted = 1 << 1;
export const kAutoDestroy = 1 << 2;
export const kEmitClose = 1 << 3;
export const kDestroyed = 1 << 4;
export const kClosed = 1 << 5;
export const kCloseEmitted = 1 << 6;
export const kErrored = 1 << 7;
export const kConstructed = 1 << 8;

/** The properties these predicates probe for. Everything is optional: the input may be any value. */
interface Probe {
	_readableState?: Probe;
	_writableState?: Probe;
	autoDestroy?: boolean;
	closed?: boolean;
	destroyed?: boolean;
	emitClose?: boolean;
	ended?: boolean;
	endEmitted?: boolean;
	errored?: Error | null;
	errorEmitted?: boolean;
	finished?: boolean;
	length?: number;
	on?: unknown;
	pendingcb?: number;
	pipe?: unknown;
	readable?: boolean;
	readableAborted?: boolean;
	readableDidRead?: boolean;
	readableEnded?: boolean;
	readableErrored?: Error | null;
	writable?: boolean;
	writableEnded?: boolean;
	writableErrored?: Error | null;
	writableFinished?: boolean;
	[kIsDestroyed]?: boolean;
	[kIsDisturbed]?: boolean;
	[kIsErrored]?: boolean;
	[kIsReadable]?: boolean | null;
	[kIsWritable]?: boolean | null;
	[key: string]: unknown;
}

const probe = (value: unknown): Probe => (value ?? {}) as Probe;

/** Whether `value` looks like a readable Node stream. `strict` additionally requires `pause`/`resume`. */
export function isReadableNodeStream(value: unknown, strict: boolean = false): boolean {
	const s = probe(value);
	return !!(
		value
		&& typeof s.pipe === 'function'
		&& typeof s.on === 'function'
		&& (!strict || (typeof s.pause === 'function' && typeof s.resume === 'function'))
		// A Duplex whose readable side was disabled is not readable; a plain Writable has `pipe` but no readable state.
		&& (!s._writableState || s._readableState?.readable !== false)
		&& (!s._writableState || !!s._readableState)
	);
}

export function isWritableNodeStream(value: unknown): boolean {
	const s = probe(value);
	return !!(value && typeof s.write === 'function' && typeof s.on === 'function' && (!s._readableState || s._writableState?.writable !== false));
}

export function isDuplexNodeStream(value: unknown): boolean {
	const s = probe(value);
	return !!(value && typeof s.pipe === 'function' && s._readableState && typeof s.on === 'function' && typeof s.write === 'function');
}

export function isNodeStream(value: unknown): boolean {
	const s = probe(value);
	return !!(
		value
		&& (s._readableState
			|| s._writableState
			|| (typeof s.write === 'function' && typeof s.on === 'function')
			|| (typeof s.pipe === 'function' && typeof s.on === 'function'))
	);
}

export function isReadableStream(value: unknown): value is ReadableStream {
	const s = probe(value);
	return !!(
		value
		&& !isNodeStream(value)
		&& typeof s.pipeThrough === 'function'
		&& typeof s.getReader === 'function'
		&& typeof s.cancel === 'function'
	);
}

export function isWritableStream(value: unknown): value is WritableStream {
	const s = probe(value);
	return !!(value && !isNodeStream(value) && typeof s.getWriter === 'function' && typeof s.abort === 'function');
}

export function isTransformStream(value: unknown): value is TransformStream {
	const s = probe(value);
	return !!(value && !isNodeStream(value) && typeof s.readable === 'object' && typeof s.writable === 'object');
}

export function isWebStream(value: unknown): boolean {
	return isReadableStream(value) || isWritableStream(value) || isTransformStream(value);
}

export function isIterable(value: unknown, isAsync?: boolean): boolean {
	if (value == null) return false;
	const s = value as Record<symbol, unknown>;
	if (isAsync === true) return typeof s[Symbol.asyncIterator] === 'function';
	if (isAsync === false) return typeof s[Symbol.iterator] === 'function';
	return typeof s[Symbol.asyncIterator] === 'function' || typeof s[Symbol.iterator] === 'function';
}

export function isDestroyed(stream: unknown): boolean | null {
	if (!isNodeStream(stream)) return null;
	const s = probe(stream);
	const state = s._writableState || s._readableState;
	return !!(s.destroyed || s[kIsDestroyed] || state?.destroyed);
}

/** Whether `end()` has been called. */
export function isWritableEnded(stream: unknown): boolean | null {
	if (!isWritableNodeStream(stream)) return null;
	const s = probe(stream);
	if (s.writableEnded === true) return true;
	if (s._writableState?.errored) return false;
	if (typeof s._writableState?.ended !== 'boolean') return null;
	return s._writableState.ended;
}

/** Whether `finish` has been emitted. With `strict === false`, an ended and drained stream counts. */
export function isWritableFinished(stream: unknown, strict?: boolean): boolean | null {
	if (!isWritableNodeStream(stream)) return null;
	const s = probe(stream);
	if (s.writableFinished === true) return true;
	const w = s._writableState;
	if (w?.errored) return false;
	if (typeof w?.finished !== 'boolean') return null;
	return !!(w.finished || (strict === false && w.ended === true && w.length === 0));
}

/** Whether `push(null)` has happened. */
export function isReadableEnded(stream: unknown): boolean | null {
	if (!isReadableNodeStream(stream)) return null;
	const s = probe(stream);
	if (s.readableEnded === true) return true;
	const r = s._readableState;
	if (!r || r.errored) return false;
	if (typeof r.ended !== 'boolean') return null;
	return r.ended;
}

/** Whether `end` has been emitted. With `strict === false`, an ended and drained stream counts. */
export function isReadableFinished(stream: unknown, strict?: boolean): boolean | null {
	if (!isReadableNodeStream(stream)) return null;
	const r = probe(stream)._readableState;
	if (r?.errored) return false;
	if (typeof r?.endEmitted !== 'boolean') return null;
	return !!(r.endEmitted || (strict === false && r.ended === true && r.length === 0));
}

export function isReadable(stream: unknown): boolean | null {
	const s = probe(stream);
	if (stream && s[kIsReadable] != null) return s[kIsReadable];
	if (typeof s.readable !== 'boolean') return null;
	if (isDestroyed(stream)) return false;
	return isReadableNodeStream(stream) && s.readable && !isReadableFinished(stream);
}

export function isWritable(stream: unknown): boolean | null {
	const s = probe(stream);
	if (stream && s[kIsWritable] != null) return s[kIsWritable];
	if (typeof s.writable !== 'boolean') return null;
	if (isDestroyed(stream)) return false;
	return isWritableNodeStream(stream) && s.writable && !isWritableEnded(stream);
}

export function isFinished(stream: unknown, opts?: { readable?: boolean; writable?: boolean }): boolean | null {
	if (!isNodeStream(stream)) return null;
	if (isDestroyed(stream)) return true;
	if (opts?.readable !== false && isReadable(stream)) return false;
	if (opts?.writable !== false && isWritable(stream)) return false;
	return true;
}

export function isWritableErrored(stream: unknown): Error | null {
	if (!isNodeStream(stream)) return null;
	const s = probe(stream);
	return s.writableErrored || s._writableState?.errored || null;
}

export function isReadableErrored(stream: unknown): Error | null {
	if (!isNodeStream(stream)) return null;
	const s = probe(stream);
	return s.readableErrored || s._readableState?.errored || null;
}

export function isClosed(stream: unknown): boolean | null {
	if (!isNodeStream(stream)) return null;
	const s = probe(stream);
	if (typeof s.closed === 'boolean') return s.closed;
	const w = s._writableState;
	const r = s._readableState;
	if (typeof w?.closed === 'boolean' || typeof r?.closed === 'boolean') return w?.closed || r?.closed || false;
	return null;
}

/** Whether `stream` is expected to emit `close` on its own, which `eos` relies on to decide what to wait for. */
export function willEmitClose(stream: unknown): boolean | null {
	if (!isNodeStream(stream)) return null;
	const s = probe(stream);
	const state = s._writableState || s._readableState;
	return !!(state?.autoDestroy && state.emitClose && state.closed === false);
}

/** Whether anything has been read from `stream`. */
export function isDisturbed(stream: unknown): boolean {
	const s = probe(stream);
	return !!(stream && (s[kIsDisturbed] ?? (s.readableDidRead || s.readableAborted)));
}

export function isErrored(stream: unknown): boolean {
	const s = probe(stream);
	return !!(
		stream
		&& (s[kIsErrored]
			?? s.readableErrored
			?? s.writableErrored
			?? s._readableState?.errorEmitted
			?? s._writableState?.errorEmitted
			?? s._readableState?.errored
			?? s._writableState?.errored)
	);
}

/** Read by `eos()` so `finished()` works on the web streams produced here. */
/** Set by `node:stream` on the web streams it creates; lets `finished()` observe one without consuming it. */
export const kIsClosedPromise = Symbol.for('nodejs.webstream.isClosedPromise');

export function trackClosed<T extends object>(stream: T, closed: Promise<void>): T {
	// A rejection is delivered through eos()'s callback; nothing else awaits it.
	closed.catch(() => {});
	Object.defineProperty(stream, kIsClosedPromise, { value: { promise: closed }, configurable: true });
	return stream;
}
