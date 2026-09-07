// SPDX-License-Identifier: LGPL-3.0-or-later
import type { DuplexOptions as NodeDuplexOptions, Duplex as NodeDuplex } from 'node:stream';

import { addAbortSignal } from './add-abort-signal.js';
import { construct, destroyer } from './destroy.js';
import { duplexify } from './duplexify.js';
import { ERR_INVALID_ARG_TYPE } from './errors.js';
import type { EventEmitterOptions } from './events.js';
import { Readable, ReadableState, type ReadableOptions } from './readable.js';
import { isReadableStream, isWritableStream, kOnConstructed } from './utils.js';
import type { ReadableWritablePair } from 'node:stream/web';
import { Writable, WritableState, type WriteCallback, type WriteRequest } from './writable.js';

/** `node:stream`'s options, plus the flags it accepts at runtime but does not declare. */
export interface DuplexOptions<T extends Duplex = Duplex> extends NodeDuplexOptions<T>, EventEmitterOptions {
	/** Set to `false` to build a Duplex that is only writable. */
	readable?: boolean;
	/** Set to `false` to build a Duplex that is only readable. */
	writable?: boolean;
	/** Encoding assumed for string chunks passed to `push()`. Defaults to `utf8`. */
	defaultEncoding?: BufferEncoding;
}

/** The writable half, taken from `Writable.prototype` at the bottom of this module. */
export interface Duplex {
	_writableState: WritableState;

	write(chunk: any, encoding?: BufferEncoding | null, callback?: WriteCallback): boolean;
	write(chunk: any, callback?: WriteCallback): boolean;
	end(callback?: WriteCallback): this;
	end(chunk: any, callback?: WriteCallback): this;
	end(chunk: any, encoding?: BufferEncoding | null, callback?: WriteCallback): this;
	cork(): void;
	uncork(): void;
	setDefaultEncoding(encoding: BufferEncoding): this;
	_write(chunk: any, encoding: BufferEncoding, callback: WriteCallback): void;
	_writev?(chunks: WriteRequest[], callback: WriteCallback): void;
	_final(callback: WriteCallback): void;

	writable: boolean;
	readonly writableAborted: boolean;
	readonly writableBuffer: ReturnType<WritableState['getBuffer']> | undefined;
	readonly writableCorked: number;
	readonly writableEnded: boolean;
	readonly writableFinished: boolean;
	readonly writableHighWaterMark: number;
	readonly writableLength: number;
	readonly writableNeedDrain: boolean;
	readonly writableObjectMode: boolean;
}

/**
 * A stream that is both readable and writable.
 */
export class Duplex extends Readable implements NodeDuplex {
	/** When `false`, ending the readable side also ends the writable side. Defaults to `true`. */
	public allowHalfOpen: boolean = true;

	public constructor(options?: DuplexOptions) {
		// A Duplex hook is handed a Duplex, which is a Readable; only TypeScript's contravariant
		// check on `this` parameters objects to forwarding these on.
		super(options as ReadableOptions);

		this._readableState = new ReadableState(options, this, true);
		this._writableState = new WritableState(options, this, true);

		if (options) {
			this.allowHalfOpen = options.allowHalfOpen !== false;

			if (options.readable === false) {
				this._readableState.readable = false;
				this._readableState.ended = true;
				this._readableState.endEmitted = true;
			}

			if (options.writable === false) {
				this._writableState.writable = false;
				this._writableState.ending = true;
				this._writableState.ended = true;
				this._writableState.finished = true;
			}

			if (typeof options.read === 'function') this._read = options.read;
			if (typeof options.write === 'function') this._write = options.write;
			if (typeof options.writev === 'function') this._writev = options.writev;
			if (typeof options.destroy === 'function') this._destroy = options.destroy;
			if (typeof options.final === 'function') this._final = options.final;
			if (typeof options.construct === 'function') this._construct = options.construct;
			if (options.signal) addAbortSignal(options.signal, this);
		}

		if (this._construct != null) {
			construct(this, () => {
				this._readableState[kOnConstructed](this);
				this._writableState[kOnConstructed](this);
			});
		}
	}

	/** Both sides have to be destroyed for a Duplex to count as destroyed. */
	public override get destroyed(): boolean {
		if (this._readableState === undefined || this._writableState === undefined) return false;
		return this._readableState.destroyed && this._writableState.destroyed;
	}
	public override set destroyed(value: boolean) {
		if (this._readableState && this._writableState) {
			this._readableState.destroyed = value;
			this._writableState.destroyed = value;
		}
	}

	/** Builds a Duplex from an iterable, async generator function, promise, web stream or stream pair. */
	public static from(body: unknown): Duplex {
		return duplexify(body, 'body');
	}

	/**
	 * Wraps a WHATWG `{ readable, writable }` pair.
	 *
	 * The trailing overload only exists so this static stays assignable to the one it shadows
	 * on `Readable`; passing a bare `ReadableStream` throws.
	 */
	public static override fromWeb(pair: ReadableWritablePair, options?: DuplexOptions): Duplex;
	public static override fromWeb(readableStream: ReadableStream, options?: ReadableOptions): Readable;
	public static override fromWeb(input: ReadableWritablePair | ReadableStream, options?: DuplexOptions & ReadableOptions): Duplex {
		const pair = input as ReadableWritablePair;
		if (!isReadableStream(pair.readable) || !isWritableStream(pair.writable)) {
			throw new ERR_INVALID_ARG_TYPE('pair', '{ readable, writable } pair', input);
		}
		const writer = pair.writable.getWriter();
		const reader = pair.readable.getReader();
		let writableClosed = false;
		let readableClosed = false;

		const duplex: Duplex = new Duplex({
			...options,
			decodeStrings: false,
			read() {
				reader.read().then(
					chunk => {
						if (chunk.done) duplex.push(null);
						else duplex.push(chunk.value);
					},
					(error: Error) => destroyer(duplex, error)
				);
			},
			write(chunk, encoding, callback) {
				writer.ready.then(() => writer.write(chunk).then(() => callback(), callback), callback);
			},
			final(callback) {
				if (writableClosed) {
					callback();
					return;
				}
				writer.close().then(() => callback(), callback);
			},
			destroy(error, callback) {
				let pending = 2;
				const done = (): void => void (--pending === 0 && callback(error));

				if (readableClosed) done();
				else reader.cancel(error ?? undefined).then(done, done);

				if (writableClosed) done();
				else writer.abort(error ?? undefined).then(done, done);
			},
		});

		writer.closed.then(
			() => (writableClosed = true),
			(error: Error) => {
				writableClosed = true;
				destroyer(duplex, error);
			}
		);

		reader.closed.then(
			() => (readableClosed = true),
			(error: Error) => {
				readableClosed = true;
				destroyer(duplex, error);
			}
		);

		return duplex;
	}

	/**
	 * Exposes a Duplex as a WHATWG `{ readable, writable }` pair.
	 *
	 * As with {@link Duplex.fromWeb}, the trailing overload is only there for assignability;
	 * a Readable that is not a Duplex throws.
	 */
	public static override toWeb(duplex: Duplex): ReadableWritablePair;
	public static override toWeb(streamReadable: Readable, options?: { strategy?: QueuingStrategy }): ReadableStream;
	public static override toWeb(stream: Duplex | Readable): ReadableWritablePair | ReadableStream {
		if (!(stream instanceof Duplex)) throw new ERR_INVALID_ARG_TYPE('duplex', 'Duplex', stream);

		return {
			writable: Writable.toWeb(stream._writableState ? stream : new Writable({ write: (chunk, encoding, cb) => cb() })),
			readable: Readable.toWeb(stream._readableState ? stream : new Readable({ read: () => {} })),
		};
	}
}

// The writable half. Anything Readable already provides (`pipe`, `destroy`, `errored`, ...) is
// left alone; `destroy` is then explicitly taken from Writable, which knows to flush pending
// write callbacks.
for (const key of Object.getOwnPropertyNames(Writable.prototype)) {
	if (key === 'constructor' || key in Duplex.prototype) continue;
	Object.defineProperty(Duplex.prototype, key, Object.getOwnPropertyDescriptor(Writable.prototype, key)!);
}

Object.defineProperty(Duplex.prototype, 'destroy', Object.getOwnPropertyDescriptor(Writable.prototype, 'destroy')!);
