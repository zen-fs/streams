// SPDX-License-Identifier: LGPL-3.0-or-later
import type { Stream } from './legacy.js';
import type { ReadableState } from './readable.js';
import type { WritableState } from './writable.js';

import { AbortError, aggregateTwoErrors, ERR_MULTIPLE_CALLBACK } from './errors.js';
import { nextTick, type Callback } from './util.js';
import {
	isDestroyed,
	isFinished,
	kAutoDestroy,
	kClosed,
	kCloseEmitted,
	kConstructed,
	kDestroyed,
	kEmitClose,
	kErrored,
	kErrorEmitted,
	kIsDestroyed,
	kState,
} from './utils.js';

export const kDestroy = Symbol('kDestroy');
export const kConstruct = Symbol('kConstruct');

export type DestroyCallback = Callback;

/** The internals `destroy()` reaches for. Both state sides are optional so this covers Readable, Writable and Duplex. */
export interface Destroyable extends Stream {
	_readableState?: ReadableState;
	_writableState?: WritableState;
	_destroy(error: Error | null, callback: DestroyCallback): void;
	_construct?(callback: DestroyCallback): void;
	destroy(error?: Error | null, callback?: DestroyCallback): this;
}

function checkError(err: Error | null | undefined, w?: WritableState, r?: ReadableState): void {
	if (!err) return;

	// Reading `.stack` here avoids a V8 leak: https://github.com/nodejs/node/pull/34103
	void err.stack;

	if (w && !w.errored) w.errored = err;
	if (r && !r.errored) r.errored = err;
}

export function destroy(this: Destroyable, err?: Error | null, callback?: DestroyCallback): Destroyable {
	const r = this._readableState;
	const w = this._writableState;
	// A Duplex tracks construction on its writable side.
	const s = w || r;

	if ((w && (w[kState] & kDestroyed) !== 0) || (r && (r[kState] & kDestroyed) !== 0)) {
		callback?.();
		return this;
	}

	// Set destroyed before running any error callback, so re-entrant destroy() calls are safe.
	checkError(err, w, r);

	if (w) w[kState] |= kDestroyed;
	if (r) r[kState] |= kDestroyed;

	if (s && (s[kState] & kConstructed) === 0) {
		this.once(kDestroy, function (this: Destroyable, er: Error | null) {
			_destroy(this, aggregateTwoErrors(er, err) ?? null, callback);
		});
	} else {
		_destroy(this, err ?? null, callback);
	}

	return this;
}

function _destroy(self: Destroyable, err: Error | null, callback?: DestroyCallback): void {
	let called = false;

	function onDestroy(err?: Error | null): void {
		if (called) return;
		called = true;

		const r = self._readableState;
		const w = self._writableState;

		checkError(err, w, r);

		if (w) w[kState] |= kClosed;
		if (r) r[kState] |= kClosed;

		callback?.(err);

		if (err) nextTick(emitErrorCloseNT, self, err);
		else nextTick(emitCloseNT, self);
	}

	try {
		self._destroy(err, onDestroy);
	} catch (err: any) {
		onDestroy(err as Error);
	}
}

function emitErrorCloseNT(self: Destroyable, err: Error): void {
	emitErrorNT(self, err);
	emitCloseNT(self);
}

function emitCloseNT(self: Destroyable): void {
	const r = self._readableState;
	const w = self._writableState;

	if (w) w[kState] |= kCloseEmitted;
	if (r) r[kState] |= kCloseEmitted;

	if ((w && (w[kState] & kEmitClose) !== 0) || (r && (r[kState] & kEmitClose) !== 0)) self.emit('close');
}

function emitErrorNT(self: Destroyable, err: Error): void {
	const r = self._readableState;
	const w = self._writableState;

	if ((w && (w[kState] & kErrorEmitted) !== 0) || (r && (r[kState] & kErrorEmitted) !== 0)) return;

	if (w) w[kState] |= kErrorEmitted;
	if (r) r[kState] |= kErrorEmitted;

	self.emit('error', err);
}

/** Resets a destroyed stream so it can be used again. Exposed as `_undestroy()`. */
export function undestroy(this: Destroyable): void {
	const r = this._readableState;
	const w = this._writableState;

	if (r) {
		r.constructed = true;
		r.closed = false;
		r.closeEmitted = false;
		r.destroyed = false;
		r.errored = null;
		r.errorEmitted = false;
		r.reading = false;
		r.ended = r.readable === false;
		r.endEmitted = r.readable === false;
	}

	if (w) {
		w.constructed = true;
		w.destroyed = false;
		w.closed = false;
		w.closeEmitted = false;
		w.errored = null;
		w.errorEmitted = false;
		w.finalCalled = false;
		w.prefinished = false;
		w.ended = w.writable === false;
		w.ending = w.writable === false;
		w.finished = w.writable === false;
	}
}

/**
 * Fails `stream` with `err`, destroying it when `autoDestroy` is on and otherwise emitting `error` directly.
 * `sync` defers the emission by a tick, for callers already inside a synchronous stream callback.
 */
export function errorOrDestroy(stream: Destroyable, err?: Error | null, sync?: boolean): void {
	const r = stream._readableState;
	const w = stream._writableState;

	if ((w && (w[kState] & kDestroyed) !== 0) || (r && (r[kState] & kDestroyed) !== 0)) return;

	if ((r && (r[kState] & kAutoDestroy) !== 0) || (w && (w[kState] & kAutoDestroy) !== 0)) {
		stream.destroy(err);
	} else if (err) {
		void err.stack;

		if (w && (w[kState] & kErrored) === 0) w.errored = err;
		if (r && (r[kState] & kErrored) === 0) r.errored = err;

		if (sync) nextTick(emitErrorNT, stream, err);
		else emitErrorNT(stream, err);
	}
}

/** Runs the stream's `_construct()` hook, holding off `_destroy()` and reads until it completes. */
export function construct(stream: Destroyable, callback: () => void): void {
	if (typeof stream._construct !== 'function') return;

	const r = stream._readableState;
	const w = stream._writableState;

	if (r) r[kState] &= ~kConstructed;
	if (w) w[kState] &= ~kConstructed;

	stream.once(kConstruct, callback);

	// A Duplex registers both sides; only the first registration schedules the call.
	if (stream.listenerCount(kConstruct) > 1) return;

	nextTick(constructNT, stream);
}

function constructNT(stream: Destroyable): void {
	let called = false;

	function onConstruct(err?: Error | null): void {
		if (called) {
			errorOrDestroy(stream, err ?? new ERR_MULTIPLE_CALLBACK());
			return;
		}
		called = true;

		const r = stream._readableState;
		const w = stream._writableState;
		const s = w || r;

		if (r) r[kState] |= kConstructed;
		if (w) w[kState] |= kConstructed;

		if (s?.destroyed) stream.emit(kDestroy, err);
		else if (err) errorOrDestroy(stream, err, true);
		else stream.emit(kConstruct);
	}

	try {
		stream._construct!(err => nextTick(onConstruct, err));
	} catch (err: any) {
		nextTick(onConstruct, err as Error);
	}
}

function emitCloseLegacy(stream: Stream): void {
	stream.emit('close');
}

function emitErrorCloseLegacy(stream: Stream, err: Error): void {
	stream.emit('error', err);
	nextTick(emitCloseLegacy, stream);
}

/** Destroys any stream-like value, including legacy streams that only have `close()`. */
export function destroyer(stream: unknown, err?: Error | null): void {
	if (!stream || isDestroyed(stream)) return;

	if (!err && !isFinished(stream)) err = new AbortError();

	const s = stream as Stream & Partial<Destroyable> & { close?: () => void; destroyed?: boolean; [kIsDestroyed]?: boolean };

	if (typeof s.destroy === 'function') s.destroy(err);
	else if (typeof s.close === 'function') s.close();
	else if (err) nextTick(emitErrorCloseLegacy, s, err);
	else nextTick(emitCloseLegacy, s);

	if (!s.destroyed) s[kIsDestroyed] = true;
}
