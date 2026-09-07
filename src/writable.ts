// SPDX-License-Identifier: LGPL-3.0-or-later
import type { DestroyCallback, Destroyable } from './destroy.js';
import type { PipeDestination } from './legacy.js';
import type { ReadableState } from './readable.js';
import type { WritableOptions as NodeWritableOptions, Writable as NodeWritable } from 'node:stream';

import { Buffer } from 'buffer';
import { addAbortSignal } from './add-abort-signal.js';
import { construct, destroyer, destroy as destroyImpl, errorOrDestroy, undestroy } from './destroy.js';
import { eos } from './end-of-stream.js';
import { captureRejectionSymbol, type EventEmitterOptions } from './events.js';
import {
	AbortError,
	ERR_INVALID_ARG_TYPE,
	ERR_METHOD_NOT_IMPLEMENTED,
	ERR_MULTIPLE_CALLBACK,
	ERR_STREAM_ALREADY_FINISHED,
	ERR_STREAM_CANNOT_PIPE,
	ERR_STREAM_DESTROYED,
	ERR_STREAM_NULL_VALUES,
	ERR_STREAM_WRITE_AFTER_END,
	ERR_UNKNOWN_ENCODING,
} from './errors.js';
import { Stream } from './legacy.js';
import { getDefaultHighWaterMark, getHighWaterMark } from './state.js';
import { nextTick, nop, type Callback } from './util.js';
import {
	isDestroyed,
	isWritable,
	isWritableStream,
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

/** `'buffer'` marks an already-decoded chunk; `''` is used where no encoding applies. */
export type ChunkEncoding = BufferEncoding | 'buffer' | '';

export type WriteCallback = Callback;

export interface WritableOptions<T extends Writable = Writable> extends NodeWritableOptions<T>, EventEmitterOptions {}

export interface WriteRequest {
	chunk: any;
	encoding: ChunkEncoding;
}

interface BufferedWrite extends WriteRequest {
	callback: WriteCallback;
}

interface AfterWriteTickInfo {
	count: number;
	cb: WriteCallback;
	stream: Writable;
	state: WritableState;
}

const kSync = 1 << 9;
const kFinalCalled = 1 << 10;
const kNeedDrain = 1 << 11;
const kEnding = 1 << 12;
const kFinished = 1 << 13;
const kDecodeStrings = 1 << 14;
const kWriting = 1 << 15;
const kBufferProcessing = 1 << 16;
const kPrefinished = 1 << 17;
const kAllBuffers = 1 << 18;
const kAllNoop = 1 << 19;
const kOnFinished = 1 << 20;
const kHasWritable = 1 << 21;
const kWritable = 1 << 22;
const kCorked = 1 << 23;
const kDefaultUTF8Encoding = 1 << 24;
const kWriteCb = 1 << 25;
const kExpectWriteCb = 1 << 26;
const kAfterWriteTickInfo = 1 << 27;
const kAfterWritePending = 1 << 28;
const kBuffered = 1 << 29;
const kEnded = 1 << 30;

const kErroredValue = Symbol('kErroredValue');
const kDefaultEncodingValue = Symbol('kDefaultEncodingValue');
const kWriteCbValue = Symbol('kWriteCbValue');
const kAfterWriteTickInfoValue = Symbol('kAfterWriteTickInfoValue');
const kBufferedValue = Symbol('kBufferedValue');
const kOnFinishedValue = Symbol('kOnFinishedValue');

/**
 * The writable half of a stream's bookkeeping, reachable as `stream._writableState`.
 *
 * Flags live in a single bitfield so the hot paths can test several at once; the named
 * properties below are accessors over that field, kept for compatibility with code that
 * inspects stream state.
 */
export class WritableState {
	public [kState]: number;

	public [kErroredValue]: Error | null = null;
	public [kDefaultEncodingValue]: BufferEncoding = 'utf8';
	public [kWriteCbValue]: WriteCallback | null = null;
	public [kAfterWriteTickInfoValue]: AfterWriteTickInfo | null = null;
	public [kBufferedValue]: BufferedWrite[] | null = null;
	public [kOnFinishedValue]: WriteCallback[] | null = null;

	/** The point at which `write()` starts returning false. */
	public highWaterMark: number;

	/** Bytes (or, in object mode, chunks) waiting to reach `_write()`. */
	public length: number = 0;

	public corked: number = 0;

	public writelen: number = 0;

	/** Number of pending user write callbacks; must reach 0 before `finish`. */
	public pendingcb: number = 0;

	public bufferedIndex: number = 0;

	public readonly onwrite: (er?: Error | null) => void;

	public constructor(options: WritableOptions<any> | undefined, stream: Writable, isDuplex: boolean) {
		this[kState] = kSync | kConstructed | kEmitClose | kAutoDestroy;

		if (options?.objectMode) this[kState] |= kObjectMode;
		if (isDuplex && (options as { writableObjectMode?: boolean } | undefined)?.writableObjectMode) this[kState] |= kObjectMode;

		this.highWaterMark = options ? getHighWaterMark(this.objectMode, options, 'writableHighWaterMark', isDuplex) : getDefaultHighWaterMark(false);

		if (!options || options.decodeStrings !== false) this[kState] |= kDecodeStrings;
		if (options?.emitClose === false) this[kState] &= ~kEmitClose;
		if (options?.autoDestroy === false) this[kState] &= ~kAutoDestroy;

		const defaultEncoding = options?.defaultEncoding;
		if (defaultEncoding == null || defaultEncoding === 'utf8' || (defaultEncoding as string) === 'utf-8') {
			this[kState] |= kDefaultUTF8Encoding;
		} else if (Buffer.isEncoding(defaultEncoding)) {
			this[kState] &= ~kDefaultUTF8Encoding;
			this[kDefaultEncodingValue] = defaultEncoding;
		} else {
			throw new ERR_UNKNOWN_ENCODING(defaultEncoding);
		}

		this.onwrite = (er?: Error | null) => onwrite(stream, er);

		resetBuffer(this);
	}

	public get objectMode(): boolean {
		return (this[kState] & kObjectMode) !== 0;
	}

	public get finalCalled(): boolean {
		return (this[kState] & kFinalCalled) !== 0;
	}
	public set finalCalled(value: boolean) {
		if (value) this[kState] |= kFinalCalled;
		else this[kState] &= ~kFinalCalled;
	}

	public get needDrain(): boolean {
		return (this[kState] & kNeedDrain) !== 0;
	}
	public set needDrain(value: boolean) {
		if (value) this[kState] |= kNeedDrain;
		else this[kState] &= ~kNeedDrain;
	}

	/** Set as soon as `end()` is entered. */
	public get ending(): boolean {
		return (this[kState] & kEnding) !== 0;
	}
	public set ending(value: boolean) {
		if (value) this[kState] |= kEnding;
		else this[kState] &= ~kEnding;
	}

	/** Set once `end()` has returned. */
	public get ended(): boolean {
		return (this[kState] & kEnded) !== 0;
	}
	public set ended(value: boolean) {
		if (value) this[kState] |= kEnded;
		else this[kState] &= ~kEnded;
	}

	public get finished(): boolean {
		return (this[kState] & kFinished) !== 0;
	}
	public set finished(value: boolean) {
		if (value) this[kState] |= kFinished;
		else this[kState] &= ~kFinished;
	}

	public get destroyed(): boolean {
		return (this[kState] & kDestroyed) !== 0;
	}
	public set destroyed(value: boolean) {
		if (value) this[kState] |= kDestroyed;
		else this[kState] &= ~kDestroyed;
	}

	public get decodeStrings(): boolean {
		return (this[kState] & kDecodeStrings) !== 0;
	}

	public get writing(): boolean {
		return (this[kState] & kWriting) !== 0;
	}

	/** Whether the `_write()` callback fired synchronously. */
	public get sync(): boolean {
		return (this[kState] & kSync) !== 0;
	}

	public get bufferProcessing(): boolean {
		return (this[kState] & kBufferProcessing) !== 0;
	}

	public get constructed(): boolean {
		return (this[kState] & kConstructed) !== 0;
	}
	public set constructed(value: boolean) {
		if (value) this[kState] |= kConstructed;
		else this[kState] &= ~kConstructed;
	}

	public get prefinished(): boolean {
		return (this[kState] & kPrefinished) !== 0;
	}
	public set prefinished(value: boolean) {
		if (value) this[kState] |= kPrefinished;
		else this[kState] &= ~kPrefinished;
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

	public get allBuffers(): boolean {
		return (this[kState] & kAllBuffers) !== 0;
	}

	public get allNoop(): boolean {
		return (this[kState] & kAllNoop) !== 0;
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

	/** `false` on a Duplex whose writable side was disabled at construction. */
	public get writable(): boolean | undefined {
		return (this[kState] & kHasWritable) !== 0 ? (this[kState] & kWritable) !== 0 : undefined;
	}
	public set writable(value: boolean | undefined | null) {
		if (value == null) {
			this[kState] &= ~(kHasWritable | kWritable);
		} else if (value) {
			this[kState] |= kHasWritable | kWritable;
		} else {
			this[kState] |= kHasWritable;
			this[kState] &= ~kWritable;
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

	public get writecb(): WriteCallback {
		return (this[kState] & kWriteCb) !== 0 ? this[kWriteCbValue]! : nop;
	}
	public set writecb(value: WriteCallback | null) {
		this[kWriteCbValue] = value;
		if (value) this[kState] |= kWriteCb;
		else this[kState] &= ~kWriteCb;
	}

	public get afterWriteTickInfo(): AfterWriteTickInfo | null {
		return (this[kState] & kAfterWriteTickInfo) !== 0 ? this[kAfterWriteTickInfoValue] : null;
	}

	public get buffered(): BufferedWrite[] {
		return (this[kState] & kBuffered) !== 0 ? this[kBufferedValue]! : [];
	}
	public set buffered(value: BufferedWrite[] | null) {
		this[kBufferedValue] = value;
		if (value) this[kState] |= kBuffered;
		else this[kState] &= ~kBuffered;
	}

	public get bufferedRequestCount(): number {
		return (this[kState] & kBuffered) === 0 ? 0 : this[kBufferedValue]!.length - this.bufferedIndex;
	}

	public getBuffer(): BufferedWrite[] {
		return (this[kState] & kBuffered) === 0 ? [] : this[kBufferedValue]!.slice(this.bufferedIndex);
	}

	public [kOnConstructed](stream: Writable): void {
		if ((this[kState] & kWriting) === 0) clearBuffer(stream, this);
		if ((this[kState] & kEnding) !== 0) finishMaybe(stream, this);
	}
}

function resetBuffer(state: WritableState): void {
	state[kBufferedValue] = null;
	state.bufferedIndex = 0;
	state[kState] |= kAllBuffers | kAllNoop;
	state[kState] &= ~kBuffered;
}

/**
 * `_final` is declared here rather than in the class body because it has no default
 * implementation: `node:stream` types it as always present, and `prefinish()` checks for it
 * at runtime before calling it.
 */
export interface Writable {
	_final(callback: WriteCallback): void;
}

/**
 * A destination for data.
 *
 * Subclasses implement `_write()` (and optionally `_writev()`, `_final()`, `_destroy()`
 * and `_construct()`); the base class handles buffering, back-pressure and `drain`.
 */
export class Writable extends Stream implements Destroyable, NodeWritable {
	public _writableState: WritableState;

	/** Present on a Duplex; `undefined` on a plain Writable. */
	declare public _readableState?: ReadableState;

	public constructor(options?: WritableOptions) {
		super(options);

		this._writableState = new WritableState(options, this, false);

		if (options) {
			if (typeof options.write === 'function') this._write = options.write;
			if (typeof options.writev === 'function') this._writev = options.writev;
			if (typeof options.destroy === 'function') this._destroy = options.destroy;
			if (typeof options.final === 'function') this._final = options.final;
			if (typeof options.construct === 'function') this._construct = options.construct;
			if (options.signal) addAbortSignal(options.signal, this);
		}

		if (this._construct != null) construct(this, () => this._writableState[kOnConstructed](this));
	}

	public static readonly WritableState: typeof WritableState = WritableState;

	/** Recognizes a Duplex, which inherits from Readable and so is not a structural subclass. */
	public static [Symbol.hasInstance](instance: unknown): boolean {
		if (Function.prototype[Symbol.hasInstance].call(this, instance)) return true;
		if ((this as unknown) !== Writable) return false;
		return !!instance && (instance as Writable)._writableState instanceof WritableState;
	}

	/** Writables are not readable, so piping from one is always an error. */
	public override pipe<T extends PipeDestination>(dest: T): T {
		errorOrDestroy(this, new ERR_STREAM_CANNOT_PIPE());
		return dest;
	}

	public write(chunk: any, encoding?: BufferEncoding | null, callback?: WriteCallback): boolean;
	public write(chunk: any, callback?: WriteCallback): boolean;
	public write(chunk: any, encoding?: BufferEncoding | null | WriteCallback, callback?: WriteCallback): boolean {
		if (typeof encoding === 'function') {
			callback = encoding;
			encoding = null;
		}
		return writeChunk(this, chunk, encoding ?? null, callback) === true;
	}

	/** Buffers writes until a matching number of `uncork()` calls. */
	public cork(): void {
		const state = this._writableState;
		state[kState] |= kCorked;
		state.corked++;
	}

	public uncork(): void {
		const state = this._writableState;
		if (!state.corked) return;

		state.corked--;
		if (!state.corked) state[kState] &= ~kCorked;
		if ((state[kState] & kWriting) === 0) clearBuffer(this, state);
	}

	public setDefaultEncoding(encoding: BufferEncoding): this {
		if (typeof encoding === 'string') encoding = encoding.toLowerCase() as BufferEncoding;
		if (!Buffer.isEncoding(encoding)) throw new ERR_UNKNOWN_ENCODING(encoding);
		this._writableState.defaultEncoding = encoding;
		return this;
	}

	public _write(chunk: any, encoding: BufferEncoding, callback: WriteCallback): void {
		if (this._writev) this._writev([{ chunk, encoding }], callback);
		else throw new ERR_METHOD_NOT_IMPLEMENTED('_write()');
	}

	public _writev?(chunks: WriteRequest[], callback: WriteCallback): void;

	public _construct?(callback: DestroyCallback): void;

	public end(callback?: WriteCallback): this;
	public end(chunk: any, callback?: WriteCallback): this;
	public end(chunk: any, encoding?: BufferEncoding | null, callback?: WriteCallback): this;
	public end(chunk?: any, encoding?: BufferEncoding | null | WriteCallback, callback?: WriteCallback): this {
		const state = this._writableState;

		if (typeof chunk === 'function') {
			callback = chunk as WriteCallback;
			chunk = null;
			encoding = null;
		} else if (typeof encoding === 'function') {
			callback = encoding;
			encoding = null;
		}

		let err: Error | undefined;

		if (chunk != null) {
			const ret = writeChunk(this, chunk, (encoding as BufferEncoding | null) ?? null);
			if (ret instanceof Error) err = ret;
		}

		// end() fully uncorks.
		if ((state[kState] & kCorked) !== 0) {
			state.corked = 1;
			this.uncork();
		}

		if (err) {
			// Reported below.
		} else if ((state[kState] & (kEnding | kErrored)) === 0) {
			// Forgiving of redundant end() calls: erroring here would be disproportionately destructive.
			state[kState] |= kEnding;
			finishMaybe(this, state, true);
			state[kState] |= kEnded;
		} else if ((state[kState] & kFinished) !== 0) {
			err = new ERR_STREAM_ALREADY_FINISHED('end');
		} else if ((state[kState] & kDestroyed) !== 0) {
			err = new ERR_STREAM_DESTROYED('end');
		}

		if (typeof callback === 'function') {
			if (err) nextTick(callback, err);
			else if ((state[kState] & kErrored) !== 0) nextTick(callback, state[kErroredValue]);
			else if ((state[kState] & kFinished) !== 0) nextTick(callback, null);
			else {
				state[kState] |= kOnFinished;
				state[kOnFinishedValue] ??= [];
				state[kOnFinishedValue].push(callback);
			}
		}

		return this;
	}

	public get closed(): boolean {
		return this._writableState ? (this._writableState[kState] & kClosed) !== 0 : false;
	}

	public get destroyed(): boolean {
		return this._writableState ? (this._writableState[kState] & kDestroyed) !== 0 : false;
	}
	public set destroyed(value: boolean) {
		if (!this._writableState) return;
		if (value) this._writableState[kState] |= kDestroyed;
		else this._writableState[kState] &= ~kDestroyed;
	}

	public get writable(): boolean {
		const w = this._writableState;
		return !!w && w.writable !== false && (w[kState] & (kEnding | kEnded | kDestroyed | kErrored)) === 0;
	}
	public set writable(value: boolean) {
		if (this._writableState) this._writableState.writable = !!value;
	}

	public get writableFinished(): boolean {
		return this._writableState ? (this._writableState[kState] & kFinished) !== 0 : false;
	}

	public get writableObjectMode(): boolean {
		return this._writableState ? (this._writableState[kState] & kObjectMode) !== 0 : false;
	}

	public get writableBuffer(): BufferedWrite[] | undefined {
		return this._writableState?.getBuffer();
	}

	public get writableEnded(): boolean {
		return this._writableState ? (this._writableState[kState] & kEnding) !== 0 : false;
	}

	public get writableNeedDrain(): boolean {
		return this._writableState ? (this._writableState[kState] & (kDestroyed | kEnding | kNeedDrain)) === kNeedDrain : false;
	}

	public get writableHighWaterMark(): number {
		return this._writableState?.highWaterMark;
	}

	public get writableCorked(): number {
		return this._writableState ? this._writableState.corked : 0;
	}

	public get writableLength(): number {
		return this._writableState?.length;
	}

	public get errored(): Error | null {
		return this._writableState ? this._writableState.errored : null;
	}

	public get writableAborted(): boolean {
		const state = this._writableState[kState];
		return (state & (kHasWritable | kWritable)) !== kHasWritable && (state & (kDestroyed | kErrored)) !== 0 && (state & kFinished) === 0;
	}

	public destroy(error?: Error | null, callback?: DestroyCallback): this {
		const state = this._writableState;

		// Pending write and end callbacks still have to be invoked.
		if ((state[kState] & (kBuffered | kOnFinished)) !== 0 && (state[kState] & kDestroyed) === 0) nextTick(errorBuffer, state);

		destroyImpl.call(this, error, callback);
		return this;
	}

	public _undestroy(): void {
		undestroy.call(this);
	}

	public _destroy(error: Error | null, callback: DestroyCallback): void {
		callback(error);
	}

	public [captureRejectionSymbol](err: Error): void {
		this.destroy(err);
	}

	/** Wraps a WHATWG `WritableStream`. */
	public static fromWeb(writableStream: WritableStream, options: WritableOptions = {}): Writable {
		if (!isWritableStream(writableStream)) throw new ERR_INVALID_ARG_TYPE('writableStream', 'WritableStream', writableStream);

		const writer = writableStream.getWriter();
		let closed = false;

		const writable: Writable = new Writable({
			decodeStrings: false,
			...options,
			write(chunk, encoding, callback) {
				writer.ready.then(() => writer.write(chunk).then(() => callback(), callback), callback);
			},
			final(callback) {
				if (closed) {
					callback();
					return;
				}
				writer.close().then(() => callback(), callback);
			},
			destroy(error, callback) {
				const done = (): void => callback(error);
				if (closed) done();
				else writer.abort(error ?? undefined).then(done, done);
			},
		});

		writer.closed.then(
			() => (closed = true),
			(error: Error) => {
				closed = true;
				destroyer(writable, error);
			}
		);

		return writable;
	}

	/** Exposes this stream as a WHATWG `WritableStream`. */
	public static toWeb(streamWritable: Writable): WritableStream {
		if (isDestroyed(streamWritable) || !isWritable(streamWritable)) {
			const closedStream = new WritableStream();
			void closedStream.close();
			return trackClosed(closedStream, Promise.resolve());
		}

		const highWaterMark = streamWritable.writableHighWaterMark;
		const strategy: QueuingStrategy = streamWritable.writableObjectMode
			? new CountQueuingStrategy({ highWaterMark })
			: // Strings are sized by length; anything unmeasurable counts as one, as in node.
				{ highWaterMark, size: (chunk: { byteLength?: number; length?: number }) => chunk?.byteLength ?? chunk?.length ?? 1 };

		let controller: WritableStreamDefaultController;
		const closed = Promise.withResolvers<void>();
		let backpressure: PromiseWithResolvers<void> | null = null;

		const cleanup = eos(streamWritable, (error?: Error | null) => {
			if ((error as { code?: string })?.code === 'ERR_STREAM_PREMATURE_CLOSE') {
				error = new AbortError(undefined, { cause: error });
			}
			cleanup();
			streamWritable.on('error', () => {});
			if (error) {
				controller.error(error);
				closed.reject(error);
			} else {
				closed.resolve();
			}
			backpressure?.resolve();
			backpressure = null;
		});

		streamWritable.on('drain', () => {
			backpressure?.resolve();
			backpressure = null;
		});

		const stream = new WritableStream(
			{
				start(c) {
					controller = c;
				},
				async write(chunk) {
					if (streamWritable.writableNeedDrain || !streamWritable.write(chunk)) {
						backpressure ??= Promise.withResolvers<void>();
						await backpressure.promise;
					}
				},
				close() {
					if (streamWritable.destroyed) return Promise.resolve();
					const ended = Promise.withResolvers<void>();
					streamWritable.end((err?: Error | null) => (err ? ended.reject(err) : ended.resolve()));
					return ended.promise;
				},
				abort(reason) {
					destroyer(streamWritable, reason as Error);
				},
			},
			strategy
		);

		return trackClosed(stream, closed.promise);
	}

	public async [Symbol.asyncDispose](): Promise<void> {
		if (!this.destroyed) this.destroy(this.writableFinished ? null : new AbortError());
		await new Promise<void>((resolve, reject) => eos(this, err => (err && err.name !== 'AbortError' ? reject(err) : resolve())));
	}
}

/** Returns `true` when more writes are welcome, or the `Error` that stopped this one. */
function writeChunk(stream: Writable, chunk: any, encoding: string | null, callback?: WriteCallback): boolean | Error {
	const state = stream._writableState;

	if (typeof callback !== 'function') callback = nop;

	if (chunk === null) throw new ERR_STREAM_NULL_VALUES();

	let enc: ChunkEncoding;

	if ((state[kState] & kObjectMode) === 0) {
		enc = (encoding as ChunkEncoding) || ((state[kState] & kDefaultUTF8Encoding) !== 0 ? 'utf8' : state.defaultEncoding);
		if (encoding && encoding !== 'buffer' && !Buffer.isEncoding(encoding)) throw new ERR_UNKNOWN_ENCODING(encoding);

		if (typeof chunk === 'string') {
			if (enc === 'buffer') throw new ERR_UNKNOWN_ENCODING(enc);
			if ((state[kState] & kDecodeStrings) !== 0) {
				chunk = Buffer.from(chunk, enc);
				enc = 'buffer';
			}
		} else if (chunk instanceof Buffer) {
			enc = 'buffer';
		} else if (ArrayBuffer.isView(chunk)) {
			chunk = Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
			enc = 'buffer';
		} else {
			throw new ERR_INVALID_ARG_TYPE('chunk', ['string', 'Buffer', 'TypedArray', 'DataView'], chunk);
		}
	} else {
		enc = (encoding as ChunkEncoding) ?? '';
	}

	let err: Error | undefined;
	if ((state[kState] & kEnding) !== 0) err = new ERR_STREAM_WRITE_AFTER_END();
	else if ((state[kState] & kDestroyed) !== 0) err = new ERR_STREAM_DESTROYED('write');

	if (err) {
		nextTick(callback, err);
		errorOrDestroy(stream, err, true);
		return err;
	}

	state.pendingcb++;
	return writeOrBuffer(stream, state, chunk, enc, callback);
}

/**
 * Hands the chunk to `_write()` when the stream is idle, otherwise queues it.
 * Returns `false` when the caller should wait for `drain`.
 */
function writeOrBuffer(stream: Writable, state: WritableState, chunk: any, encoding: ChunkEncoding, callback: WriteCallback): boolean {
	const len = (state[kState] & kObjectMode) !== 0 ? 1 : chunk.length;

	state.length += len;

	if ((state[kState] & (kWriting | kErrored | kCorked | kConstructed)) !== kConstructed) {
		if ((state[kState] & kBuffered) === 0) {
			state[kState] |= kBuffered;
			state[kBufferedValue] = [];
		}

		state[kBufferedValue]!.push({ chunk, encoding, callback });
		if ((state[kState] & kAllBuffers) !== 0 && encoding !== 'buffer') state[kState] &= ~kAllBuffers;
		if ((state[kState] & kAllNoop) !== 0 && callback !== nop) state[kState] &= ~kAllNoop;
	} else {
		state.writelen = len;
		if (callback !== nop) state.writecb = callback;
		state[kState] |= kWriting | kSync | kExpectWriteCb;
		stream._write(chunk, encoding as BufferEncoding, state.onwrite);
		state[kState] &= ~kSync;
	}

	const ret = state.length < state.highWaterMark || state.length === 0;

	if (!ret) state[kState] |= kNeedDrain;

	// A false return also breaks synchronous `while (stream.write(data))` loops on failure.
	return ret && (state[kState] & (kDestroyed | kErrored)) === 0;
}

function doWrite(stream: Writable, state: WritableState, writev: boolean, len: number, chunk: any, encoding: ChunkEncoding, cb: WriteCallback): void {
	state.writelen = len;
	if (cb !== nop) state.writecb = cb;
	state[kState] |= kWriting | kSync | kExpectWriteCb;

	if ((state[kState] & kDestroyed) !== 0) state.onwrite(new ERR_STREAM_DESTROYED('write'));
	else if (writev) stream._writev!(chunk, state.onwrite);
	else stream._write(chunk, encoding as BufferEncoding, state.onwrite);

	state[kState] &= ~kSync;
}

function onwriteError(stream: Writable, state: WritableState, er: Error, cb: WriteCallback): void {
	--state.pendingcb;

	cb(er);
	// The buffered writes fail too, but with a generic error: `er` belongs to one specific write.
	errorBuffer(state);
	// May emit 'error', which must always follow cb.
	errorOrDestroy(stream, er);
}

function onwrite(stream: Writable, er?: Error | null): void {
	const state = stream._writableState;

	if ((state[kState] & kExpectWriteCb) === 0) {
		errorOrDestroy(stream, new ERR_MULTIPLE_CALLBACK());
		return;
	}

	const sync = (state[kState] & kSync) !== 0;
	const cb = (state[kState] & kWriteCb) !== 0 ? state[kWriteCbValue]! : nop;

	state.writecb = null;
	state[kState] &= ~(kWriting | kExpectWriteCb);
	state.length -= state.writelen;
	state.writelen = 0;

	if (er) {
		void er.stack;

		if ((state[kState] & kErrored) === 0) {
			state[kErroredValue] = er;
			state[kState] |= kErrored;
		}

		// A Duplex has to fail its readable side too.
		if (stream._readableState && !stream._readableState.errored) stream._readableState.errored = er;

		if (sync) nextTick(onwriteError, stream, state, er, cb);
		else onwriteError(stream, state, er, cb);
		return;
	}

	if ((state[kState] & kBuffered) !== 0) clearBuffer(stream, state);

	if (!sync) {
		afterWrite(stream, state, 1, cb);
		return;
	}

	const needDrain = (state[kState] & kNeedDrain) !== 0 && state.length === 0;
	const needTick = needDrain || (state[kState] & kDestroyed) !== 0 || cb !== nop;

	// The same callback is usually passed to every write(), so consecutive completions
	// are coalesced into one scheduled tick with a counter.
	if (cb === nop) {
		if ((state[kState] & kAfterWritePending) === 0 && needTick) {
			nextTick(afterWrite, stream, state, 1, cb);
			state[kState] |= kAfterWritePending;
		} else {
			state.pendingcb--;
			if ((state[kState] & kEnding) !== 0) finishMaybe(stream, state, true);
		}
	} else if ((state[kState] & kAfterWriteTickInfo) !== 0 && state[kAfterWriteTickInfoValue]!.cb === cb) {
		state[kAfterWriteTickInfoValue]!.count++;
	} else if (needTick) {
		state[kAfterWriteTickInfoValue] = { count: 1, cb, stream, state };
		nextTick(afterWriteTick, state[kAfterWriteTickInfoValue]);
		state[kState] |= kAfterWritePending | kAfterWriteTickInfo;
	} else {
		state.pendingcb--;
		if ((state[kState] & kEnding) !== 0) finishMaybe(stream, state, true);
	}
}

function afterWriteTick({ stream, state, count, cb }: AfterWriteTickInfo): void {
	state[kState] &= ~kAfterWriteTickInfo;
	state[kAfterWriteTickInfoValue] = null;
	afterWrite(stream, state, count, cb);
}

function afterWrite(stream: Writable, state: WritableState, count: number, cb: WriteCallback): void {
	state[kState] &= ~kAfterWritePending;

	const needDrain = (state[kState] & (kEnding | kNeedDrain | kDestroyed)) === kNeedDrain && state.length === 0;
	if (needDrain) {
		state[kState] &= ~kNeedDrain;
		stream.emit('drain');
	}

	while (count-- > 0) {
		state.pendingcb--;
		cb(null);
	}

	if ((state[kState] & kDestroyed) !== 0) errorBuffer(state);

	if ((state[kState] & kEnding) !== 0) finishMaybe(stream, state, true);
}

/** Fails every queued write and end callback. */
function errorBuffer(state: WritableState): void {
	if ((state[kState] & kWriting) !== 0) return;

	if ((state[kState] & kBuffered) !== 0) {
		const buffered = state[kBufferedValue]!;
		for (let n = state.bufferedIndex; n < buffered.length; ++n) {
			const { chunk, callback } = buffered[n];
			state.length -= (state[kState] & kObjectMode) !== 0 ? 1 : chunk.length;
			callback(state.errored ?? new ERR_STREAM_DESTROYED('write'));
		}
	}

	callFinishedCallbacks(state, state.errored ?? new ERR_STREAM_DESTROYED('end'));

	resetBuffer(state);
}

/** Flushes queued writes, coalescing them through `_writev()` when the stream provides one. */
function clearBuffer(stream: Writable, state: WritableState): void {
	if ((state[kState] & (kDestroyed | kBufferProcessing | kCorked | kBuffered | kConstructed)) !== (kBuffered | kConstructed)) return;

	const objectMode = (state[kState] & kObjectMode) !== 0;
	const buffered = state[kBufferedValue]!;
	const { bufferedIndex } = state;
	const bufferedLength = buffered.length - bufferedIndex;

	if (!bufferedLength) return;

	let i = bufferedIndex;

	state[kState] |= kBufferProcessing;

	if (bufferedLength > 1 && stream._writev) {
		state.pendingcb -= bufferedLength - 1;

		const callback =
			(state[kState] & kAllNoop) !== 0
				? nop
				: (err?: Error | null) => {
						for (let n = i; n < buffered.length; ++n) buffered[n].callback(err);
					};

		// `doWrite` mutates `buffered`, so hand the callback its own copy when it needs one.
		const chunks = (state[kState] & kAllNoop) !== 0 && i === 0 ? buffered : buffered.slice(i);
		(chunks as BufferedWrite[] & { allBuffers?: boolean }).allBuffers = (state[kState] & kAllBuffers) !== 0;

		doWrite(stream, state, true, state.length, chunks, '', callback);

		resetBuffer(state);
	} else {
		do {
			const { chunk, encoding, callback } = buffered[i];
			(buffered as (BufferedWrite | null)[])[i++] = null;
			doWrite(stream, state, false, objectMode ? 1 : chunk.length, chunk, encoding, callback);
		} while (i < buffered.length && (state[kState] & kWriting) === 0);

		if (i === buffered.length) resetBuffer(state);
		else if (i > 256) {
			buffered.splice(0, i);
			state.bufferedIndex = 0;
		} else {
			state.bufferedIndex = i;
		}
	}

	state[kState] &= ~kBufferProcessing;
}

function needFinish(state: WritableState): boolean {
	// Ending and constructed, but not destroyed, finished, writing, errored or closed, with nothing left buffered.
	return (
		(state[kState] & (kEnding | kDestroyed | kConstructed | kFinished | kWriting | kErrorEmitted | kCloseEmitted | kErrored | kBuffered))
			=== (kEnding | kConstructed) && state.length === 0
	);
}

function onFinish(stream: Writable, state: WritableState, err?: Error | null): void {
	if ((state[kState] & kPrefinished) !== 0) {
		errorOrDestroy(stream, err ?? new ERR_MULTIPLE_CALLBACK());
		return;
	}

	state.pendingcb--;

	if (err) {
		callFinishedCallbacks(state, err);
		errorOrDestroy(stream, err, (state[kState] & kSync) !== 0);
	} else if (needFinish(state)) {
		state[kState] |= kPrefinished;
		stream.emit('prefinish');
		// Some streams assume 'finish' arrives asynchronously relative to the _final callback.
		state.pendingcb++;
		nextTick(finish, stream, state);
	}
}

function prefinish(stream: Writable, state: WritableState): void {
	if ((state[kState] & (kPrefinished | kFinalCalled)) !== 0) return;

	if (typeof stream._final === 'function' && (state[kState] & kDestroyed) === 0) {
		state[kState] |= kFinalCalled | kSync;
		state.pendingcb++;

		try {
			stream._final(err => onFinish(stream, state, err));
		} catch (err: any) {
			onFinish(stream, state, err as Error);
		}

		state[kState] &= ~kSync;
	} else {
		state[kState] |= kFinalCalled | kPrefinished;
		stream.emit('prefinish');
	}
}

function finishMaybe(stream: Writable, state: WritableState, sync?: boolean): void {
	if (!needFinish(state)) return;

	prefinish(stream, state);
	if (state.pendingcb !== 0) return;

	if (sync) {
		state.pendingcb++;
		nextTick(
			(stream: Writable, state: WritableState) => {
				if (needFinish(state)) finish(stream, state);
				else state.pendingcb--;
			},
			stream,
			state
		);
	} else if (needFinish(state)) {
		state.pendingcb++;
		finish(stream, state);
	}
}

function finish(stream: Writable, state: WritableState): void {
	state.pendingcb--;
	state[kState] |= kFinished;

	callFinishedCallbacks(state, null);

	stream.emit('finish');

	if ((state[kState] & kAutoDestroy) === 0) return;

	// A Duplex may only be destroyed once its readable side is done too.
	const rState = stream._readableState;
	if (!rState || (rState.autoDestroy && (rState.endEmitted || rState.readable === false))) stream.destroy();
}

function callFinishedCallbacks(state: WritableState, err: Error | null): void {
	if ((state[kState] & kOnFinished) === 0) return;

	const callbacks = state[kOnFinishedValue]!;
	state[kOnFinishedValue] = null;
	state[kState] &= ~kOnFinished;
	for (const callback of callbacks) callback(err);
}

// Present but null on the prototype, as in node, so `_writev` reads as absent until a
// subclass or the `writev` option provides one.
Writable.prototype._writev = null as unknown as Writable['_writev'];
