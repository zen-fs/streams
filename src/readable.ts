// SPDX-License-Identifier: LGPL-3.0-or-later
import type {
	Readable as NodeReadable,
	ReadableOptions as NodeReadableOptions,
	ReadableIteratorOptions,
	ReadableOperatorOptions,
	ReadableToWebOptions,
} from 'node:stream';
import type { Destroyable, DestroyCallback } from './destroy.js';
import type { Duplex } from './duplex.js';
import type { EventKey, Listener } from './events.js';
import type { PipeDestination } from './legacy.js';
import type { WritableState } from './writable.js';

import { Buffer } from 'buffer';
import { addAbortSignal } from './add-abort-signal.js';
import { construct, destroyer, destroy as destroyImpl, errorOrDestroy, undestroy } from './destroy.js';
import { eos } from './end-of-stream.js';
import {
	AbortError,
	aggregateTwoErrors,
	ERR_INVALID_ARG_TYPE,
	ERR_METHOD_NOT_IMPLEMENTED,
	ERR_OUT_OF_RANGE,
	ERR_STREAM_PUSH_AFTER_EOF,
	ERR_STREAM_UNSHIFT_AFTER_END_EVENT,
	ERR_UNKNOWN_ENCODING,
} from './errors.js';
import { captureRejectionSymbol, type EventEmitterOptions } from './events.js';
import { from } from './from.js';
import { createBatchedAsyncIterator, normalizeBatch, toAsyncStreamable } from './iter.js';
import { prependListener, Stream } from './legacy.js';
import { installOperators } from './operators.js';
import { getDefaultHighWaterMark, getHighWaterMark } from './state.js';
import { StringDecoder } from './string_decoder.js';
import { nextTick, nop, validateObject } from './util.js';
import {
	isReadableStream,
	kAutoDestroy,
	kClosed,
	kCloseEmitted,
	kConstructed,
	kDestroyed,
	kEmitClose,
	kErrored,
	kErrorEmitted,
	kObjectMode,
	kOnConstructed,
	kState,
	trackClosed,
} from './utils.js';

export interface ReadableOptions<T extends Readable = Readable> extends NodeReadableOptions<T>, EventEmitterOptions {
	defaultEncoding?: BufferEncoding;
}

const kEnded = 1 << 9;
const kEndEmitted = 1 << 10;
const kReading = 1 << 11;
const kSync = 1 << 12;
const kNeedReadable = 1 << 13;
const kEmittedReadable = 1 << 14;
const kReadableListening = 1 << 15;
const kResumeScheduled = 1 << 16;
const kMultiAwaitDrain = 1 << 17;
const kReadingMore = 1 << 18;
const kDataEmitted = 1 << 19;
const kDefaultUTF8Encoding = 1 << 20;
const kDecoder = 1 << 21;
const kEncoding = 1 << 22;
const kHasFlowing = 1 << 23;
const kFlowing = 1 << 24;
const kHasPaused = 1 << 25;
const kPaused = 1 << 26;
const kDataListening = 1 << 27;
const kEndScheduled = 1 << 28;
const kEofReadablePending = 1 << 29;

const kErroredValue = Symbol('kErroredValue');
const kDefaultEncodingValue = Symbol('kDefaultEncodingValue');
const kDecoderValue = Symbol('kDecoderValue');
const kEncodingValue = Symbol('kEncodingValue');

/**
 * The readable half of a stream's bookkeeping, reachable as `stream._readableState`.
 *
 * As with `WritableState`, the flags share one bitfield and the named properties are
 * accessors over it.
 */
export class ReadableState {
	public [kState]: number;

	public [kErroredValue]: Error | null = null;
	public [kDefaultEncodingValue]: BufferEncoding = 'utf8';
	public [kDecoderValue]: StringDecoder | null = null;
	public [kEncodingValue]: BufferEncoding | null = null;

	/** The point at which `_read()` stops being called ahead of demand. */
	public highWaterMark: number;

	public buffer: any[] = [];

	public bufferIndex: number = 0;

	public length: number = 0;

	public pipes: PipeDestination[] = [];

	/** The destination(s) whose `drain` we are waiting on before resuming flow. */
	public awaitDrainWriters: PipeDestination | Set<PipeDestination> | null = null;

	/** `false` on a Duplex whose readable side was disabled at construction. */
	public readable?: boolean;

	public constructor(options: ReadableOptions<any> | undefined, stream: Readable, isDuplex: boolean) {
		this[kState] = kEmitClose | kAutoDestroy | kConstructed | kSync;

		if (options?.objectMode) this[kState] |= kObjectMode;
		if (isDuplex && (options as { readableObjectMode?: boolean } | undefined)?.readableObjectMode) this[kState] |= kObjectMode;

		this.highWaterMark = options ? getHighWaterMark(this.objectMode, options, 'readableHighWaterMark', isDuplex) : getDefaultHighWaterMark(false);

		if (options?.emitClose === false) this[kState] &= ~kEmitClose;
		if (options?.autoDestroy === false) this[kState] &= ~kAutoDestroy;

		const defaultEncoding = options?.defaultEncoding;
		if (defaultEncoding == null || defaultEncoding === 'utf8' || (defaultEncoding as string) === 'utf-8') {
			this[kState] |= kDefaultUTF8Encoding;
		} else if (Buffer.isEncoding(defaultEncoding)) {
			this.defaultEncoding = defaultEncoding;
		} else {
			throw new ERR_UNKNOWN_ENCODING(defaultEncoding);
		}

		if (options?.encoding) {
			this.decoder = new StringDecoder(options.encoding);
			this.encoding = options.encoding;
		}
	}

	public get objectMode(): boolean {
		return (this[kState] & kObjectMode) !== 0;
	}

	/** Set once `push(null)` has been seen. */
	public get ended(): boolean {
		return (this[kState] & kEnded) !== 0;
	}
	public set ended(value: boolean) {
		if (value) this[kState] |= kEnded;
		else this[kState] &= ~kEnded;
	}

	public get endEmitted(): boolean {
		return (this[kState] & kEndEmitted) !== 0;
	}
	public set endEmitted(value: boolean) {
		if (value) this[kState] |= kEndEmitted;
		else this[kState] &= ~kEndEmitted;
	}

	/** Whether a `_read()` call is outstanding. */
	public get reading(): boolean {
		return (this[kState] & kReading) !== 0;
	}
	public set reading(value: boolean) {
		if (value) this[kState] |= kReading;
		else this[kState] &= ~kReading;
	}

	public get constructed(): boolean {
		return (this[kState] & kConstructed) !== 0;
	}
	public set constructed(value: boolean) {
		if (value) this[kState] |= kConstructed;
		else this[kState] &= ~kConstructed;
	}

	public get sync(): boolean {
		return (this[kState] & kSync) !== 0;
	}
	public set sync(value: boolean) {
		if (value) this[kState] |= kSync;
		else this[kState] &= ~kSync;
	}

	public get needReadable(): boolean {
		return (this[kState] & kNeedReadable) !== 0;
	}
	public set needReadable(value: boolean) {
		if (value) this[kState] |= kNeedReadable;
		else this[kState] &= ~kNeedReadable;
	}

	public get emittedReadable(): boolean {
		return (this[kState] & kEmittedReadable) !== 0;
	}
	public set emittedReadable(value: boolean) {
		if (value) this[kState] |= kEmittedReadable;
		else this[kState] &= ~kEmittedReadable;
	}

	public get readableListening(): boolean {
		return (this[kState] & kReadableListening) !== 0;
	}

	public get resumeScheduled(): boolean {
		return (this[kState] & kResumeScheduled) !== 0;
	}

	public get errorEmitted(): boolean {
		return (this[kState] & kErrorEmitted) !== 0;
	}
	public set errorEmitted(value: boolean) {
		if (value) this[kState] |= kErrorEmitted;
		else this[kState] &= ~kErrorEmitted;
	}

	public get emitClose(): boolean {
		return (this[kState] & kEmitClose) !== 0;
	}

	public get autoDestroy(): boolean {
		return (this[kState] & kAutoDestroy) !== 0;
	}

	public get destroyed(): boolean {
		return (this[kState] & kDestroyed) !== 0;
	}
	public set destroyed(value: boolean) {
		if (value) this[kState] |= kDestroyed;
		else this[kState] &= ~kDestroyed;
	}

	public get closed(): boolean {
		return (this[kState] & kClosed) !== 0;
	}
	public set closed(value: boolean) {
		if (value) this[kState] |= kClosed;
		else this[kState] &= ~kClosed;
	}

	public get closeEmitted(): boolean {
		return (this[kState] & kCloseEmitted) !== 0;
	}
	public set closeEmitted(value: boolean) {
		if (value) this[kState] |= kCloseEmitted;
		else this[kState] &= ~kCloseEmitted;
	}

	public get multiAwaitDrain(): boolean {
		return (this[kState] & kMultiAwaitDrain) !== 0;
	}

	public get readingMore(): boolean {
		return (this[kState] & kReadingMore) !== 0;
	}

	/** Whether any `data` event has been emitted. */
	public get dataEmitted(): boolean {
		return (this[kState] & kDataEmitted) !== 0;
	}

	public get errored(): Error | null {
		return (this[kState] & kErrored) !== 0 ? this[kErroredValue] : null;
	}
	public set errored(value: Error | null) {
		if (value) {
			this[kErroredValue] = value;
			this[kState] |= kErrored;
		} else {
			this[kState] &= ~kErrored;
		}
	}

	public get defaultEncoding(): BufferEncoding {
		return (this[kState] & kDefaultUTF8Encoding) !== 0 ? 'utf8' : this[kDefaultEncodingValue];
	}
	public set defaultEncoding(value: BufferEncoding) {
		if (value === 'utf8' || (value as string) === 'utf-8') {
			this[kState] |= kDefaultUTF8Encoding;
		} else {
			this[kState] &= ~kDefaultUTF8Encoding;
			this[kDefaultEncodingValue] = value;
		}
	}

	public get decoder(): StringDecoder | null {
		return (this[kState] & kDecoder) !== 0 ? this[kDecoderValue] : null;
	}
	public set decoder(value: StringDecoder | null) {
		if (value) {
			this[kDecoderValue] = value;
			this[kState] |= kDecoder;
		} else {
			this[kState] &= ~kDecoder;
		}
	}

	public get encoding(): BufferEncoding | null {
		return (this[kState] & kEncoding) !== 0 ? this[kEncodingValue] : null;
	}
	public set encoding(value: BufferEncoding | null) {
		if (value) {
			this[kEncodingValue] = value;
			this[kState] |= kEncoding;
		} else {
			this[kState] &= ~kEncoding;
		}
	}

	/** `null` until the stream has been resumed or paused. */
	public get flowing(): boolean | null {
		return (this[kState] & kHasFlowing) !== 0 ? (this[kState] & kFlowing) !== 0 : null;
	}
	public set flowing(value: boolean | null) {
		if (value == null) {
			this[kState] &= ~(kHasFlowing | kFlowing);
		} else if (value) {
			this[kState] |= kHasFlowing | kFlowing;
		} else {
			this[kState] |= kHasFlowing;
			this[kState] &= ~kFlowing;
		}
	}

	public get paused(): boolean {
		return (this[kState] & kPaused) !== 0;
	}
	public set paused(value: boolean) {
		this[kState] |= kHasPaused;
		if (value) this[kState] |= kPaused;
		else this[kState] &= ~kPaused;
	}

	public get pipesCount(): number {
		return this.pipes.length;
	}

	public [kOnConstructed](stream: Readable): void {
		if ((this[kState] & kNeedReadable) !== 0) maybeReadMore(stream, this);
	}
}

/**
 * A source of data.
 *
 * Subclasses implement `_read()` (and optionally `_destroy()` and `_construct()`) and call
 * `push()`; the base class handles buffering, flow control and the `data`/`readable` events.
 */
export class Readable extends Stream implements Destroyable, NodeReadable {
	public _readableState: ReadableState;

	/** Present on a Duplex; `undefined` on a plain Readable. */
	declare public _writableState?: WritableState;

	public constructor(options?: ReadableOptions) {
		super(options);

		this._readableState = new ReadableState(options, this, false);

		if (options) {
			if (typeof options.read === 'function') this._read = options.read;
			if (typeof options.destroy === 'function') this._destroy = options.destroy;
			if (typeof options.construct === 'function') this._construct = options.construct;
			if (options.signal) addAbortSignal(options.signal, this);
		}

		if (this._construct != null) construct(this, () => this._readableState[kOnConstructed](this));
	}

	public destroy(error?: Error | null, callback?: DestroyCallback): this {
		destroyImpl.call(this, error, callback);
		return this;
	}

	public _undestroy(): void {
		undestroy.call(this);
	}

	public _destroy(error: Error | null, callback: DestroyCallback): void {
		callback(error);
	}

	public _construct?(callback: DestroyCallback): void;

	public [captureRejectionSymbol](err: Error): void {
		this.destroy(err);
	}

	public async [Symbol.asyncDispose](): Promise<void> {
		let error: Error | null = null;
		if (!this.destroyed) {
			error = this.readableEnded ? null : new AbortError();
			this.destroy(error);
		}
		await new Promise<void>((resolve, reject) => eos(this, err => (err && err !== error ? reject(err) : resolve())));
	}

	/**
	 * Adds a chunk to the read buffer, or ends the stream when passed `null`.
	 * Returns `false` once the high water mark is reached.
	 */
	public push(chunk: any, encoding?: BufferEncoding): boolean {
		const state = this._readableState;
		return (state[kState] & kObjectMode) === 0 ? pushByteMode(this, state, chunk, encoding) : pushObjectMode(this, state, chunk, encoding);
	}

	/** Returns a chunk to the front of the read buffer. Should only ever be a chunk that came out of `read()`. */
	public unshift(chunk: any, encoding?: BufferEncoding): boolean {
		const state = this._readableState;
		return (state[kState] & kObjectMode) === 0 ? unshiftByteMode(this, state, chunk, encoding) : unshiftObjectMode(this, state, chunk);
	}

	public isPaused(): boolean {
		const state = this._readableState[kState];
		return (state & kPaused) !== 0 || (state & (kHasFlowing | kFlowing)) === kHasFlowing;
	}

	/** Makes the stream emit strings in `encoding` rather than Buffers, without splitting characters. */
	public setEncoding(encoding: BufferEncoding): this {
		const state = this._readableState;

		const decoder = new StringDecoder(encoding);
		state.decoder = decoder;
		// setEncoding(null) leaves the decoder at utf8.
		state.encoding = state.decoder.encoding;

		// Re-decode whatever is already buffered.
		let content = '';
		for (const data of state.buffer.slice(state.bufferIndex)) content += decoder.write(data);
		if ((state[kState] & kEnded) !== 0) content += decoder.end();

		state.buffer.length = 0;
		state.bufferIndex = 0;
		if (content !== '') state.buffer.push(content);
		state.length = content.length;

		return this;
	}

	public read(n?: number | null): any {
		// Equivalent to parseInt(undefined, 10), avoiding a V8 slow path.
		if (n === undefined || n === null) n = NaN;
		else if (!Number.isInteger(n)) n = Number.parseInt(n as unknown as string, 10);

		const state = this._readableState;
		const nOrig = n;

		if (n > state.highWaterMark) state.highWaterMark = computeNewHighWaterMark(n);

		if (n !== 0) state[kState] &= ~(kEmittedReadable | kEofReadablePending);

		// read(0) is the idiom for triggering a 'readable' event without consuming.
		const stateLength = state.length;
		if (
			n === 0
			&& (state[kState] & kNeedReadable) !== 0
			&& ((state.highWaterMark !== 0 ? stateLength >= state.highWaterMark : stateLength > 0) || (state[kState] & kEnded) !== 0)
		) {
			if (stateLength === 0 && (state[kState] & kEnded) !== 0) endReadable(this);
			else emitReadable(this);
			return null;
		}

		n = howMuchToRead(n, state);

		if (n === 0 && (state[kState] & kEnded) !== 0) {
			if (state.length === 0) endReadable(this);
			return null;
		}

		// _read() has to run before the buffer is drained: for synchronous streams such as
		// PassThrough it can supply the very data this call is about to return.
		const wantMore = (state[kState] & kNeedReadable) !== 0 || state.length === 0 || state.length - n < state.highWaterMark;

		// No point reading when ended or already reading, and not allowed while constructing or destroyed.
		const canRead = (state[kState] & (kReading | kEnded | kDestroyed | kErrored | kConstructed)) === kConstructed;

		if (wantMore && canRead) {
			state[kState] |= kReading | kSync;
			if (state.length === 0) state[kState] |= kNeedReadable;

			try {
				this._read(state.highWaterMark);
			} catch (err: any) {
				errorOrDestroy(this, err as Error);
			}

			state[kState] &= ~kSync;

			// A synchronous push() clears `reading`, so re-evaluate how much is available.
			if ((state[kState] & kReading) === 0) n = howMuchToRead(nOrig, state);
		}

		const ret = n > 0 ? fromList(n, state) : null;

		if (ret === null) {
			state[kState] |= state.length <= state.highWaterMark ? kNeedReadable : 0;
			n = 0;
		} else {
			state.length -= n;
			if ((state[kState] & kMultiAwaitDrain) !== 0) (state.awaitDrainWriters as Set<PipeDestination>).clear();
			else state.awaitDrainWriters = null;
		}

		if (state.length === 0) {
			if ((state[kState] & kEnded) === 0) state[kState] |= kNeedReadable;
			if (nOrig !== n && (state[kState] & kEnded) !== 0) endReadable(this);
		}

		if (ret !== null && (state[kState] & (kErrorEmitted | kCloseEmitted)) === 0) {
			state[kState] |= kDataEmitted;
			this.emit('data', ret);
		}

		return ret;
	}

	// eslint-disable-next-line @typescript-eslint/no-unused-vars -- part of the contract subclasses implement
	public _read(size: number): void {
		throw new ERR_METHOD_NOT_IMPLEMENTED('_read()');
	}

	public override pipe<T extends PipeDestination>(dest: T, pipeOpts?: { end?: boolean }): T {
		const src = this;
		const state = this._readableState;

		if (state.pipes.length === 1 && (state[kState] & kMultiAwaitDrain) === 0) {
			state[kState] |= kMultiAwaitDrain;
			state.awaitDrainWriters = new Set(state.awaitDrainWriters ? [state.awaitDrainWriters as PipeDestination] : []);
		}

		state.pipes.push(dest);

		const doEnd = pipeOpts?.end !== false;
		const endFn = doEnd ? onend : unpipe;
		if ((state[kState] & kEndEmitted) !== 0) nextTick(endFn);
		else src.once('end', endFn);

		dest.on('unpipe', onunpipe);
		function onunpipe(readable: unknown, unpipeInfo?: { hasUnpiped: boolean }): void {
			if (readable !== src) return;
			if (unpipeInfo && unpipeInfo.hasUnpiped === false) {
				unpipeInfo.hasUnpiped = true;
				cleanup();
			}
		}

		function onend(): void {
			dest.end();
		}

		let ondrain: (() => void) | undefined;
		let cleanedUp = false;

		function cleanup(): void {
			dest.removeListener('close', onclose);
			dest.removeListener('finish', onfinish);
			if (ondrain) dest.removeListener('drain', ondrain);
			dest.removeListener('error', onerror);
			dest.removeListener('unpipe', onunpipe);
			src.removeListener('end', onend);
			src.removeListener('end', unpipe);
			src.removeListener('data', ondata);

			cleanedUp = true;

			// Releasing the drain wait unconditionally keeps a destination that errored
			// synchronously from starving the others piped from this same source.
			if (ondrain && state.awaitDrainWriters) ondrain();
		}

		function pause(): void {
			// Unpiping during dest.write() can otherwise leave the source paused forever.
			if (!cleanedUp) {
				if (state.pipes.length === 1 && state.pipes[0] === dest) {
					state.awaitDrainWriters = dest;
					state[kState] &= ~kMultiAwaitDrain;
				} else if (state.pipes.length > 1 && state.pipes.includes(dest)) {
					(state.awaitDrainWriters as Set<PipeDestination>).add(dest);
				}
				src.pause();
			}
			if (!ondrain) {
				// Attaching once per pipe rather than per chunk: adding and removing a
				// once() handler in flow() would be far more expensive.
				ondrain = pipeOnDrain(src, dest);
				dest.on('drain', ondrain);
			}
		}

		src.on('data', ondata);
		function ondata(chunk: unknown): void {
			try {
				if (dest.write(chunk) === false) pause();
			} catch (error: any) {
				dest.destroy?.(error as Error);
			}
		}

		// Stop piping on a destination error, without suppressing the throw.
		function onerror(this: unknown, er: Error): void {
			unpipe();
			dest.removeListener('error', onerror);
			if (dest.listenerCount?.('error') !== 0) return;

			const s = (dest as Partial<Destroyable>)._writableState || (dest as Partial<Destroyable>)._readableState;
			// A stream that emitted 'error' directly rather than going through destroy().
			if (s && !s.errorEmitted) errorOrDestroy(dest as unknown as Destroyable, er);
			else dest.emit('error', er);
		}

		prependListener(dest, 'error', onerror);

		// 'close' and 'finish' both unpipe, but only the first to arrive does the work.
		function onclose(): void {
			dest.removeListener('finish', onfinish);
			unpipe();
		}
		(dest as { once?: (e: string, l: Listener) => void }).once?.('close', onclose);

		function onfinish(): void {
			dest.removeListener('close', onclose);
			unpipe();
		}
		(dest as { once?: (e: string, l: Listener) => void }).once?.('finish', onfinish);

		function unpipe(): void {
			src.unpipe(dest);
		}

		dest.emit('pipe', src);

		if ((dest as { writableNeedDrain?: boolean }).writableNeedDrain === true) pause();
		else if ((state[kState] & kFlowing) === 0) src.resume();

		return dest;
	}

	public unpipe(dest?: PipeDestination): this {
		const state = this._readableState;

		if (state.pipes.length === 0) return this;

		if (!dest) {
			const dests = state.pipes;
			state.pipes = [];
			this.pause();
			for (const d of dests) d.emit('unpipe', this, { hasUnpiped: false });
			return this;
		}

		const index = state.pipes.indexOf(dest);
		if (index === -1) return this;

		state.pipes.splice(index, 1);
		if (state.pipes.length === 0) this.pause();

		dest.emit('unpipe', this, { hasUnpiped: false });

		return this;
	}

	public override on(ev: EventKey, fn: Listener): this {
		const res = super.on(ev, fn);
		const state = this._readableState;

		if (ev === 'data') {
			state[kState] |= kDataListening;

			// Keeping readableListening current makes the resume() below a no-op where it
			// needs to be, which is what allows once('readable') to work.
			state[kState] |= this.listenerCount('readable') > 0 ? kReadableListening : 0;

			if ((state[kState] & (kHasFlowing | kFlowing)) !== kHasFlowing) this.resume();
		} else if (ev === 'readable' && (state[kState] & (kEndEmitted | kReadableListening)) === 0) {
			state[kState] |= kReadableListening | kNeedReadable | kHasFlowing;
			state[kState] &= ~(kFlowing | kEmittedReadable);

			if (state.length) {
				emitReadable(this);
			} else {
				if ((state[kState] & kEofReadablePending) !== 0) {
					// The end-of-stream 'readable' was skipped because nobody was listening; redeem it now.
					state[kState] &= ~kEofReadablePending;
					emitReadable(this);
				}
				if ((state[kState] & kReading) === 0) nextTick(nReadingNextTick, this);
			}
		}

		return res;
	}

	public override addListener(ev: EventKey, fn: Listener): this {
		return this.on(ev, fn);
	}

	public override removeListener(ev: EventKey, fn: Listener): this {
		const res = super.removeListener(ev, fn);

		if (ev === 'readable') {
			// Deferred so that once('readable', fn) cycles still work: the reset has to land
			// after 'readable' was emitted but before any I/O.
			nextTick(updateReadableListening, this);
		} else if (ev === 'data' && this.listenerCount('data') === 0) {
			this._readableState[kState] &= ~kDataListening;
		}

		return res;
	}

	public override off(ev: EventKey, fn: Listener): this {
		return this.removeListener(ev, fn);
	}

	public override removeAllListeners(ev?: EventKey): this {
		const res = super.removeAllListeners(ev);
		if (ev === 'readable' || ev === undefined) nextTick(updateReadableListening, this);
		return res;
	}

	/** Switches the stream into flowing mode. */
	public resume(): this {
		const state = this._readableState;
		if ((state[kState] & kDestroyed) !== 0) return this;

		if ((state[kState] & kFlowing) === 0) {
			// Only actually flow when nobody is listening for 'readable', but resume() regardless.
			state[kState] |= kHasFlowing;
			if ((state[kState] & kReadableListening) === 0) state[kState] |= kFlowing;
			else state[kState] &= ~kFlowing;
			resume(this, state);
		}

		state[kState] |= kHasPaused;
		state[kState] &= ~kPaused;
		return this;
	}

	public pause(): this {
		const state = this._readableState;
		if ((state[kState] & kDestroyed) !== 0) return this;

		if ((state[kState] & (kHasFlowing | kFlowing)) !== kHasFlowing) {
			state[kState] |= kHasFlowing;
			state[kState] &= ~kFlowing;
			this.emit('pause');
		}

		state[kState] |= kHasPaused | kPaused;
		return this;
	}

	/** Uses an old-style (pre-streams2) stream as this stream's data source. */
	public wrap(stream: any): this {
		let paused = false;

		stream.on('data', (chunk: unknown) => {
			if (!this.push(chunk) && stream.pause) {
				paused = true;
				stream.pause();
			}
		});

		stream.on('end', () => void this.push(null));
		stream.on('error', (err: Error) => errorOrDestroy(this, err));
		stream.on('close', () => void this.destroy());
		stream.on('destroy', () => void this.destroy());

		this._read = () => {
			if (paused && stream.resume) {
				paused = false;
				stream.resume();
			}
		};

		// Proxy the wrapped stream's own methods, which matters for filters and duplexes.
		for (const key of Object.keys(stream)) {
			if ((this as any)[key] === undefined && typeof stream[key] === 'function') {
				(this as any)[key] = stream[key].bind(stream);
			}
		}

		return this;
	}

	public [Symbol.asyncIterator](): NodeJS.AsyncIterator<any> {
		return streamToAsyncIterator(this);
	}

	public iterator(options?: ReadableIteratorOptions): NodeJS.AsyncIterator<any> {
		if (options !== undefined) validateObject(options, 'options');
		return streamToAsyncIterator(this, options);
	}

	public get readable(): boolean {
		const r = this._readableState;
		return !!r && r.readable !== false && !r.destroyed && !r.errorEmitted && !r.endEmitted;
	}
	public set readable(value: boolean) {
		if (this._readableState) this._readableState.readable = !!value;
	}

	public get readableDidRead(): boolean {
		return this._readableState.dataEmitted;
	}

	public get readableAborted(): boolean {
		const r = this._readableState;
		return !!(r.readable !== false && (r.destroyed || r.errored) && !r.endEmitted);
	}

	public get readableHighWaterMark(): number {
		return this._readableState.highWaterMark;
	}

	public get readableBuffer(): any[] | undefined {
		return this._readableState?.buffer;
	}

	public get readableFlowing(): boolean | null {
		return this._readableState.flowing;
	}
	public set readableFlowing(value: boolean | null) {
		if (this._readableState) this._readableState.flowing = value;
	}

	public get readableLength(): number {
		return this._readableState.length;
	}

	public get readableObjectMode(): boolean {
		return this._readableState ? this._readableState.objectMode : false;
	}

	public get readableEncoding(): BufferEncoding | null {
		return this._readableState ? this._readableState.encoding : null;
	}

	public get errored(): Error | null {
		return this._readableState ? this._readableState.errored : null;
	}

	public get closed(): boolean {
		return this._readableState ? this._readableState.closed : false;
	}

	public get destroyed(): boolean {
		return this._readableState ? this._readableState.destroyed : false;
	}
	public set destroyed(value: boolean) {
		if (this._readableState) this._readableState.destroyed = value;
	}

	public get readableEnded(): boolean {
		return this._readableState ? this._readableState.endEmitted : false;
	}

	/** Builds a Readable from an iterable, async iterable, string or Buffer. */
	public static from(iterable: string | Buffer | Iterable<unknown> | AsyncIterable<unknown>, options?: ReadableOptions): Readable {
		return from(Readable, iterable, options);
	}

	/** Wraps an old-style stream, or any object with a compatible `data`/`end` event surface. */
	public static wrap(src: any, options?: ReadableOptions): Readable {
		return new Readable({
			objectMode: src.readableObjectMode ?? src.objectMode ?? true,
			...options,
			destroy(err, callback) {
				destroyer(src, err);
				callback(err);
			},
		}).wrap(src);
	}

	/** Wraps a WHATWG `ReadableStream`. */
	public static fromWeb(readableStream: ReadableStream, options?: ReadableOptions): Readable {
		if (!isReadableStream(readableStream)) throw new ERR_INVALID_ARG_TYPE('readableStream', 'ReadableStream', readableStream);

		const reader = readableStream.getReader();
		let closed = false;

		const readable: Readable = new Readable({
			objectMode: false,
			...options,
			read() {
				reader.read().then(
					chunk => {
						if (chunk.done) readable.push(null);
						else readable.push(chunk.value);
					},
					(error: Error) => destroyer(readable, error)
				);
			},
			destroy(error, callback) {
				const done = (): void => callback(error);
				if (closed) done();
				else reader.cancel(error ?? undefined).then(done, done);
			},
		});

		reader.closed.then(
			() => (closed = true),
			(error: Error) => {
				closed = true;
				destroyer(readable, error);
			}
		);

		return readable;
	}

	/** Exposes this stream as a WHATWG `ReadableStream`. */
	public static toWeb(streamReadable: Readable, options: ReadableToWebOptions = {}): ReadableStream {
		const objectMode = streamReadable.readableObjectMode;
		const highWaterMark = streamReadable.readableHighWaterMark;

		const strategy: QueuingStrategy =
			options.strategy ?? (objectMode ? new CountQueuingStrategy({ highWaterMark }) : new ByteLengthQueuingStrategy({ highWaterMark }));

		let controller: ReadableStreamDefaultController;
		const closed = Promise.withResolvers<void>();

		// Paused until the web side pulls, so back-pressure crosses the boundary.
		streamReadable.pause();

		const cleanup = eos(streamReadable, (error?: Error | null) => {
			if ((error as { code?: string })?.code === 'ERR_STREAM_PREMATURE_CLOSE') {
				error = new AbortError(undefined, { cause: error });
			}
			cleanup();
			// Keep a late 'error' from becoming an unhandled throw now that we own the stream.
			streamReadable.on('error', () => {});
			if (error) {
				controller.error(error);
				closed.reject(error);
			} else {
				controller.close();
				closed.resolve();
			}
		});

		streamReadable.on('data', (chunk: any) => {
			controller.enqueue(objectMode || typeof chunk === 'string' ? chunk : new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength));
			if ((controller.desiredSize ?? 0) <= 0) streamReadable.pause();
		});

		const stream = new ReadableStream(
			{
				start(c) {
					controller = c;
				},
				pull() {
					streamReadable.resume();
				},
				cancel(reason) {
					destroyer(streamReadable, reason as Error);
				},
			},
			strategy
		);

		return trackClosed(stream, closed.promise);
	}

	public static readonly ReadableState: typeof ReadableState = ReadableState;

	/** Exposed for tests. */
	public static _fromList: typeof fromList = fromList;

	[toAsyncStreamable]() {
		const state = this._readableState;
		const iter = createBatchedAsyncIterator(this, state.objectMode || state.encoding ? normalizeBatch : null);
		(iter as any).stream = this;
		return iter;
	}
}

/**
 * Installed onto the prototype rather than declared in the class body: the operators live in
 * `operators.js` and `compose` in `compose.js`, which the package entry point loads.
 */
export interface Readable {
	map(fn: (value: any, options: { signal: AbortSignal }) => any, options?: ReadableOperatorOptions): Readable;
	filter(fn: (value: any, options: { signal: AbortSignal }) => any, options?: ReadableOperatorOptions): Readable;
	flatMap(fn: (value: any, options: { signal: AbortSignal }) => any, options?: ReadableOperatorOptions): Readable;
	drop(limit: number, options?: ReadableOperatorOptions): Readable;
	take(limit: number, options?: ReadableOperatorOptions): Readable;

	every(fn: (value: any, options: { signal: AbortSignal }) => any, options?: ReadableOperatorOptions): Promise<boolean>;
	some(fn: (value: any, options: { signal: AbortSignal }) => any, options?: ReadableOperatorOptions): Promise<boolean>;
	find(fn: (value: any, options: { signal: AbortSignal }) => any, options?: ReadableOperatorOptions): Promise<any>;
	forEach(fn: (value: any, options: { signal: AbortSignal }) => any, options?: ReadableOperatorOptions): Promise<void>;
	toArray(options?: ReadableOperatorOptions): Promise<any[]>;
	reduce(
		reducer: (previous: any, value: any, options: { signal: AbortSignal }) => any,
		initialValue?: any,
		options?: ReadableOperatorOptions
	): Promise<any>;

	/** Available once `@zenfs/streams` (or `@zenfs/streams/compose`) has been imported. */
	compose(stream: any, options?: { signal?: AbortSignal }): Duplex;
}

installOperators(Readable);

function unshiftByteMode(stream: Readable, state: ReadableState, chunk: any, encoding?: BufferEncoding): boolean {
	if (chunk === null) {
		state[kState] &= ~kReading;
		onEofChunk(stream, state);
		return false;
	}

	if (typeof chunk === 'string') {
		encoding ||= state.defaultEncoding;
		if (state.encoding !== encoding) {
			// With an encoding set, the buffer holds strings, so keep the chunk in that encoding.
			chunk = state.encoding ? Buffer.from(chunk, encoding).toString(state.encoding) : Buffer.from(chunk, encoding);
		}
	} else if (ArrayBuffer.isView(chunk)) {
		chunk = Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
	} else if (chunk !== undefined && !(chunk instanceof Buffer)) {
		errorOrDestroy(stream, new ERR_INVALID_ARG_TYPE('chunk', ['string', 'Buffer', 'TypedArray', 'DataView'], chunk));
		return false;
	}

	if (!(chunk && chunk.length > 0)) return canPushMore(state);

	return unshiftValue(stream, state, chunk);
}

function unshiftObjectMode(stream: Readable, state: ReadableState, chunk: any): boolean {
	if (chunk === null) {
		state[kState] &= ~kReading;
		onEofChunk(stream, state);
		return false;
	}

	return unshiftValue(stream, state, chunk);
}

function unshiftValue(stream: Readable, state: ReadableState, chunk: any): boolean {
	if ((state[kState] & kEndEmitted) !== 0) errorOrDestroy(stream, new ERR_STREAM_UNSHIFT_AFTER_END_EVENT());
	else if ((state[kState] & (kDestroyed | kErrored)) !== 0) return false;
	else addChunk(stream, state, chunk, true);

	return canPushMore(state);
}

function pushByteMode(stream: Readable, state: ReadableState, chunk: any, encoding?: BufferEncoding | ''): boolean {
	if (chunk === null) {
		state[kState] &= ~kReading;
		onEofChunk(stream, state);
		return false;
	}

	if (typeof chunk === 'string') {
		encoding ||= state.defaultEncoding;
		if (state.encoding !== encoding) {
			chunk = Buffer.from(chunk, encoding);
			encoding = '';
		}
	} else if (chunk instanceof Buffer) {
		encoding = '';
	} else if (ArrayBuffer.isView(chunk)) {
		chunk = Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
		encoding = '';
	} else if (chunk !== undefined) {
		errorOrDestroy(stream, new ERR_INVALID_ARG_TYPE('chunk', ['string', 'Buffer', 'TypedArray', 'DataView'], chunk));
		return false;
	}

	if (!chunk || chunk.length <= 0) {
		state[kState] &= ~kReading;
		maybeReadMore(stream, state);
		return canPushMore(state);
	}

	if ((state[kState] & kEnded) !== 0) {
		errorOrDestroy(stream, new ERR_STREAM_PUSH_AFTER_EOF());
		return false;
	}

	if ((state[kState] & (kDestroyed | kErrored)) !== 0) return false;

	state[kState] &= ~kReading;

	if ((state[kState] & kDecoder) !== 0 && !encoding) {
		chunk = state[kDecoderValue]!.write(chunk);
		if (chunk.length === 0) {
			maybeReadMore(stream, state);
			return canPushMore(state);
		}
	}

	addChunk(stream, state, chunk, false);
	return canPushMore(state);
}

function pushObjectMode(stream: Readable, state: ReadableState, chunk: any, encoding?: BufferEncoding): boolean {
	if (chunk === null) {
		state[kState] &= ~kReading;
		onEofChunk(stream, state);
		return false;
	}

	if ((state[kState] & kEnded) !== 0) {
		errorOrDestroy(stream, new ERR_STREAM_PUSH_AFTER_EOF());
		return false;
	}

	if ((state[kState] & (kDestroyed | kErrored)) !== 0) return false;

	state[kState] &= ~kReading;

	if ((state[kState] & kDecoder) !== 0 && !encoding) chunk = state[kDecoderValue]!.write(chunk);

	addChunk(stream, state, chunk, false);
	return canPushMore(state);
}

function canPushMore(state: ReadableState): boolean {
	// The `length === 0` case keeps a hwm of 0 (as the repl uses) from wedging.
	return (state[kState] & kEnded) === 0 && (state.length < state.highWaterMark || state.length === 0);
}

function addChunk(stream: Readable, state: ReadableState, chunk: any, addToFront: boolean): void {
	if ((state[kState] & (kFlowing | kSync | kDataListening)) === (kFlowing | kDataListening) && state.length === 0) {
		// Guarded so the Set is not rebuilt for every pipe.
		if ((state[kState] & kMultiAwaitDrain) !== 0) (state.awaitDrainWriters as Set<PipeDestination>).clear();
		else state.awaitDrainWriters = null;

		state[kState] |= kDataEmitted;
		stream.emit('data', chunk);
	} else {
		state.length += (state[kState] & kObjectMode) !== 0 ? 1 : chunk.length;
		if (addToFront) {
			if (state.bufferIndex > 0) state.buffer[--state.bufferIndex] = chunk;
			else state.buffer.unshift(chunk);
		} else {
			state.buffer.push(chunk);
		}

		if ((state[kState] & kNeedReadable) !== 0) emitReadable(stream);
	}

	maybeReadMore(stream, state);
}

const MAX_HWM = 0x40000000;

function computeNewHighWaterMark(n: number): number {
	if (n > MAX_HWM) throw new ERR_OUT_OF_RANGE('size', '<= 1GiB', n);

	// Round up to the next power of two, so the hwm does not creep up in tiny steps.
	n--;
	n |= n >>> 1;
	n |= n >>> 2;
	n |= n >>> 4;
	n |= n >>> 8;
	n |= n >>> 16;
	return n + 1;
}

function howMuchToRead(n: number, state: ReadableState): number {
	if (n <= 0 || (state.length === 0 && (state[kState] & kEnded) !== 0)) return 0;
	if ((state[kState] & kObjectMode) !== 0) return 1;

	if (Number.isNaN(n)) {
		if ((state[kState] & kDecoder) === 0 && state.length) return state.buffer[state.bufferIndex].length;
		// Only flow one buffer at a time.
		if ((state[kState] & kFlowing) !== 0 && state.length) return state.buffer[state.bufferIndex].length;
		return state.length;
	}

	if (n <= state.length) return n;
	return (state[kState] & kEnded) !== 0 ? state.length : 0;
}

function onEofChunk(stream: Readable, state: ReadableState): void {
	if ((state[kState] & kEnded) !== 0) return;

	const decoder = (state[kState] & kDecoder) !== 0 ? state[kDecoderValue] : null;
	if (decoder) {
		const chunk = decoder.end();
		if (chunk?.length) {
			state.buffer.push(chunk);
			state.length += (state[kState] & kObjectMode) !== 0 ? 1 : chunk.length;
		}
	}

	state[kState] |= kEnded;

	if ((state[kState] & kSync) !== 0) {
		// Wait a tick: emitting now risks doing so from inside the read() that triggered this.
		if ((state[kState] & (kFlowing | kNeedReadable)) !== 0 || stream.listenerCount('readable') > 0) {
			emitReadable(stream);
		} else {
			// Nobody is watching, so skip scheduling 'readable' entirely. A listener attached
			// later redeems it (see Readable#on), and read()/resume() reach the end on their own.
			state[kState] |= kEofReadablePending;
		}
	} else {
		state[kState] &= ~kNeedReadable;
		state[kState] |= kEmittedReadable;
		// Parts of the ecosystem rely on this emission being synchronous at EOF.
		emitReadable_(stream);
	}
}

function emitReadable(stream: Readable): void {
	const state = stream._readableState;
	state[kState] &= ~kNeedReadable;
	if ((state[kState] & kEmittedReadable) === 0) {
		state[kState] |= kEmittedReadable;
		nextTick(emitReadable_, stream);
	}
}

function emitReadable_(stream: Readable): void {
	const state = stream._readableState;

	if ((state[kState] & (kDestroyed | kErrored)) === 0 && (state.length || (state[kState] & kEnded) !== 0)) {
		stream.emit('readable');
		state[kState] &= ~kEmittedReadable;
	}

	// Another 'readable' is needed when the stream is not flowing (flow handles it itself),
	// has not ended, and is below the high water mark.
	state[kState] |= (state[kState] & (kFlowing | kEnded)) === 0 && state.length <= state.highWaterMark ? kNeedReadable : 0;

	flow(stream);
}

function maybeReadMore(stream: Readable, state: ReadableState): void {
	if ((state[kState] & (kReadingMore | kReading | kConstructed)) === kConstructed) {
		state[kState] |= kReadingMore;
		nextTick(maybeReadMore_, stream, state);
	}
}

function maybeReadMore_(stream: Readable, state: ReadableState): void {
	// Keep reading while the buffer is below the high water mark, or while flowing with an
	// empty buffer -- in flowing mode nothing else drives read(), so stopping here would
	// stall a consumer that just subscribed to 'data'.
	//
	// `reading` means a _read() is outstanding and has not pushed yet; this runs again once it does.
	while (
		(state[kState] & (kReading | kEnded)) === 0
		&& (state.length < state.highWaterMark || ((state[kState] & kFlowing) !== 0 && state.length === 0))
	) {
		const len = state.length;
		stream.read(0);
		// No progress, so stop spinning.
		if (len === state.length) break;
	}
	state[kState] &= ~kReadingMore;
}

function pipeOnDrain(src: Readable, dest: PipeDestination): () => void {
	return function pipeOnDrainFunctionResult() {
		const state = src._readableState;

		// `ondrain` may be called directly, so use the captured dest rather than `this`.
		if (state.awaitDrainWriters === dest) state.awaitDrainWriters = null;
		else if ((state[kState] & kMultiAwaitDrain) !== 0) (state.awaitDrainWriters as Set<PipeDestination>).delete(dest);

		if ((!state.awaitDrainWriters || (state.awaitDrainWriters as Set<PipeDestination>).size === 0) && (state[kState] & kDataListening) !== 0) {
			src.resume();
		}
	};
}

function updateReadableListening(self: Readable): void {
	const state = self._readableState;

	if (self.listenerCount('readable') > 0) state[kState] |= kReadableListening;
	else state[kState] &= ~kReadableListening;

	if ((state[kState] & (kHasPaused | kPaused | kResumeScheduled)) === (kHasPaused | kResumeScheduled)) {
		// Flowing has to be set now, or the scheduled resume will not flow.
		state[kState] |= kHasFlowing | kFlowing;
	} else if ((state[kState] & kDataListening) !== 0) {
		self.resume();
	} else if ((state[kState] & kReadableListening) === 0) {
		state[kState] &= ~(kHasFlowing | kFlowing);
	}
}

function nReadingNextTick(self: Readable): void {
	self.read(0);
}

function resume(stream: Readable, state: ReadableState): void {
	if ((state[kState] & kResumeScheduled) === 0) {
		state[kState] |= kResumeScheduled;
		nextTick(resume_, stream, state);
	}
}

function resume_(stream: Readable, state: ReadableState): void {
	if ((state[kState] & kReading) === 0) stream.read(0);

	state[kState] &= ~kResumeScheduled;
	stream.emit('resume');
	flow(stream);
	if ((state[kState] & (kFlowing | kReading)) === kFlowing) stream.read(0);
}

function flow(stream: Readable): void {
	const state = stream._readableState;
	while ((state[kState] & kFlowing) !== 0 && stream.read() !== null);
}

/** Takes `n` bytes (or one object) off the front of the buffer. */
function fromList(n: number, state: ReadableState): any {
	// state.length cannot change while this runs, and neither can the buffered chunk
	// lengths, so every repeated property load is hoisted.
	const stateLength = state.length;

	if (stateLength === 0) return null;

	let idx = state.bufferIndex;
	let ret;

	const buf = state.buffer;
	const len = buf.length;

	if ((state[kState] & kObjectMode) !== 0) {
		ret = buf[idx];
		buf[idx++] = null;
	} else if (!n || n >= stateLength) {
		// Take everything and truncate the list.
		if ((state[kState] & kDecoder) !== 0) {
			ret = '';
			while (idx < len) {
				ret += buf[idx];
				buf[idx++] = null;
			}
		} else if (len - idx === 0) {
			ret = Buffer.alloc(0);
		} else if (len - idx === 1) {
			ret = buf[idx];
			buf[idx++] = null;
		} else {
			ret = Buffer.allocUnsafe(stateLength);

			let i = 0;
			while (idx < len) {
				const data = buf[idx];
				ret.set(data, i);
				i += data.length;
				buf[idx++] = null;
			}
		}
	} else {
		const first = buf[idx];
		const firstLength = first.length;

		if (n < firstLength) {
			// slice() behaves the same for buffers and strings.
			ret = first.slice(0, n);
			buf[idx] = first.slice(n);
		} else if (n === firstLength) {
			ret = first;
			buf[idx++] = null;
		} else if ((state[kState] & kDecoder) !== 0) {
			ret = '';
			while (idx < len) {
				const str = buf[idx];
				const strLength = str.length;
				if (n > strLength) {
					ret += str;
					n -= strLength;
					buf[idx++] = null;
					continue;
				}
				if (n === strLength) {
					ret += str;
					buf[idx++] = null;
				} else {
					ret += str.slice(0, n);
					buf[idx] = str.slice(n);
				}
				break;
			}
		} else {
			ret = Buffer.allocUnsafe(n);

			const retLen = n;
			while (idx < len) {
				const data = buf[idx];
				const dataLength = data.length;
				if (n > dataLength) {
					ret.set(data, retLen - n);
					n -= dataLength;
					buf[idx++] = null;
					continue;
				}
				if (n === dataLength) {
					ret.set(data, retLen - n);
					buf[idx++] = null;
				} else {
					ret.set(Buffer.from(data.buffer, data.byteOffset, n), retLen - n);
					buf[idx] = Buffer.from(data.buffer, data.byteOffset + n, dataLength - n);
				}
				break;
			}
		}
	}

	if (idx === len) {
		state.buffer.length = 0;
		state.bufferIndex = 0;
	} else if (idx > 1024) {
		state.buffer.splice(0, idx);
		state.bufferIndex = 0;
	} else {
		state.bufferIndex = idx;
	}

	return ret;
}

function endReadable(stream: Readable): void {
	const state = stream._readableState;

	if ((state[kState] & (kEndEmitted | kEndScheduled)) === 0) {
		state[kState] |= kEnded | kEndScheduled;
		nextTick(endReadableNT, state, stream);
	}
}

function endReadableNT(state: ReadableState, stream: Readable): void {
	// Clearing this lets endReadable() schedule again, both when the emission below is
	// skipped (after an unshift) and when the stream is later reset by undestroy().
	state[kState] &= ~kEndScheduled;

	// Check for one last unshift.
	if ((state[kState] & (kErrored | kCloseEmitted | kEndEmitted)) !== 0 || state.length !== 0) return;

	state[kState] |= kEndEmitted;
	stream.emit('end');

	const s = stream as Readable & { allowHalfOpen?: boolean; writable?: boolean };

	if (s.writable && s.allowHalfOpen === false) {
		nextTick(endWritableNT, stream);
	} else if (state.autoDestroy) {
		// A Duplex may only be destroyed once its writable side is done too.
		const wState = stream._writableState;
		if (!wState || (wState.autoDestroy && (wState.finished || wState.writable === false))) stream.destroy();
	}
}

function endWritableNT(stream: Readable): void {
	const s = stream as Readable & { writable?: boolean; writableEnded?: boolean; end?: () => void };
	if (s.writable && !s.writableEnded && !s.destroyed) s.end!();
}

/** %AsyncIteratorPrototype%, so the iterators handed out inherit the same helpers Node's do. */
const AsyncIteratorPrototype = Object.getPrototypeOf(Object.getPrototypeOf(async function* () {}).prototype) as object;

function streamToAsyncIterator(stream: Readable, options?: ReadableIteratorOptions): NodeJS.AsyncIterator<any> {
	if (typeof stream.read !== 'function') stream = Readable.wrap(stream, { objectMode: true });

	const iter = createAsyncIterator(stream, options) as NodeJS.AsyncIterator<any> & { stream: Readable };
	iter.stream = stream;
	return iter;
}

type Request = { type: 'next' | 'return' | 'throw'; value: unknown; resolve: (r: IteratorResult<any>) => void; reject: (e: unknown) => void };

/** Async iteration over a Readable. Requests arriving while another is outstanding are queued and served in order. */
function createAsyncIterator(stream: Readable, options?: ReadableIteratorOptions): NodeJS.AsyncIterator<any> {
	let callback: () => void = nop;
	/** `undefined` while active, `null` once ended cleanly, otherwise the error. */
	let error: Error | null | undefined;
	let started = false;
	let completed = false;
	/** Whether an asynchronous request is outstanding. */
	let inFlight = false;
	let queue: Request[] | null = null;
	let draining = false;
	let cleanup: () => void;

	// Serves both as the 'readable' listener (where `this === stream`) and as a promise
	// executor that stores the resolver waking a pending pump().
	function wakeup(this: unknown, resolve: () => void): void {
		if (this === stream) {
			callback();
			callback = nop;
		} else {
			callback = resolve;
		}
	}

	function start(): void {
		started = true;
		stream.on('readable', wakeup);
		cleanup = eos(stream, { writable: false }, err => {
			error = err ? (aggregateTwoErrors(error, err) ?? null) : null;
			callback();
			callback = nop;
		});
	}

	function finalize(): void {
		completed = true;

		const s = stream as Readable & { allowHalfOpen?: boolean; writable?: boolean; writableEnded?: boolean };
		const preserveHalfOpenDuplex = error === null && s.allowHalfOpen === true && s.writable === true && s.writableEnded !== true;

		if ((error || options?.destroyOnReturn !== false) && (error === undefined || stream._readableState.autoDestroy) && !preserveHalfOpenDuplex) {
			destroyer(stream, null);
		} else {
			stream.off('readable', wakeup);
			cleanup();
		}
	}

	function settleError(err: unknown, reject: (e: unknown) => void): void {
		error = aggregateTwoErrors(error, err as Error) ?? null;
		finalize();
		reject(error);
	}

	function drain(): void {
		// Requests that settle synchronously call back into drain(); the guard keeps one
		// loop running rather than recursing once per request.
		if (draining) return;
		draining = true;
		try {
			while (!inFlight && queue!.length) {
				const req = queue!.shift()!;
				if (req.type === 'next') processNext(req.resolve, req.reject);
				else if (req.type === 'return') processReturn(req.value, req.resolve);
				else processThrow(req.value, req.reject);
			}
		} finally {
			draining = false;
		}
	}

	// Runs with inFlight === true; settles this request and hands over to anything queued behind it.
	function pump(resolve: (r: IteratorResult<any>) => void, reject: (e: unknown) => void): void {
		const chunk = stream.destroyed ? null : stream.read();

		if (chunk !== null) {
			// Read `then` once, so a getter cannot observe or throw on a second access.
			const then = (chunk as PromiseLike<unknown>).then;
			if (typeof then === 'function') {
				then.call(
					chunk,
					(value: unknown) => {
						inFlight = false;
						resolve({ done: false, value });
						if (queue !== null) drain();
					},
					(err: unknown) => {
						inFlight = false;
						settleError(err, reject);
						if (queue !== null) drain();
					}
				);
				return;
			}
			inFlight = false;
			resolve({ done: false, value: chunk });
		} else if (error) {
			inFlight = false;
			settleError(error, reject);
		} else if (error === null) {
			inFlight = false;
			finalize();
			resolve({ done: true, value: undefined });
		} else {
			// Nothing buffered yet: wait for 'readable' or end-of-stream, then retry.
			void new Promise<void>(wakeup).then(() => pump(resolve, reject));
			return;
		}

		if (queue !== null) drain();
	}

	function processNext(resolve: (r: IteratorResult<any>) => void, reject: (e: unknown) => void): void {
		if (completed) {
			resolve({ done: true, value: undefined });
			return;
		}
		if (!started) start();
		inFlight = true;
		pump(resolve, reject);
	}

	function processReturn(value: unknown, resolve: (r: IteratorResult<any>) => void): void {
		if (!completed) {
			if (started) finalize();
			else completed = true;
		}
		resolve({ done: true, value });
	}

	function processThrow(err: unknown, reject: (e: unknown) => void): void {
		if (completed || !started) {
			completed = true;
			reject(err);
			return;
		}
		settleError(err, reject);
	}

	function enqueue(type: Request['type'], value: unknown): Promise<IteratorResult<any>> {
		return new Promise<IteratorResult<any>>((resolve, reject) => {
			if (inFlight) {
				queue ??= [];
				queue.push({ type, value, resolve, reject });
			} else if (type === 'next') {
				resolve({ done: true, value: undefined });
			} else if (type === 'return') {
				processReturn(value, resolve);
			} else {
				processThrow(value, reject);
			}
		});
	}

	return Object.assign(Object.create(AsyncIteratorPrototype) as object, {
		[Symbol.asyncIterator](): NodeJS.AsyncIterator<any> {
			return this;
		},

		async [Symbol.asyncDispose](): Promise<void> {
			await this.return(undefined);
		},

		next(): Promise<IteratorResult<any>> {
			if (inFlight || completed) return enqueue('next', undefined);

			if (!started) start();

			const chunk = stream.destroyed ? null : stream.read();
			if (chunk !== null) {
				const then = (chunk as PromiseLike<unknown>).then;
				if (typeof then === 'function') {
					inFlight = true;
					return then.call(
						chunk,
						(value: unknown) => {
							inFlight = false;
							if (queue !== null) drain();
							return { done: false, value };
						},
						(err: unknown) => {
							inFlight = false;
							error = aggregateTwoErrors(error, err as Error);
							finalize();
							if (queue !== null) drain();
							throw error;
						}
					) as Promise<IteratorResult<any>>;
				}
				return Promise.resolve({ done: false, value: chunk });
			}

			if (error) {
				finalize();
				return Promise.reject(error);
			}
			if (error === null) {
				finalize();
				return Promise.resolve({ done: true, value: undefined });
			}

			inFlight = true;
			return new Promise((resolve, reject) => {
				void new Promise<void>(wakeup).then(() => pump(resolve, reject));
			});
		},

		return(value?: unknown): Promise<IteratorResult<any>> {
			return enqueue('return', value);
		},

		throw(err?: unknown): Promise<IteratorResult<any>> {
			return enqueue('throw', err);
		},
	}) as NodeJS.AsyncIterator<any>;
}
