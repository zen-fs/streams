// SPDX-License-Identifier: LGPL-3.0-or-later
import type { TransformCallback, TransformOptions as NodeTransformOptions, Transform as NodeTransform } from 'node:stream';
import type { WriteCallback } from './writable.js';

import { Duplex, type DuplexOptions } from './duplex.js';
import { ERR_METHOD_NOT_IMPLEMENTED } from './errors.js';
import { getHighWaterMark } from './state.js';
import { nextTick } from './util.js';

/** `node:stream`'s options, plus the flags {@link DuplexOptions} adds. */
export interface TransformOptions<T extends Transform = Transform>
	extends NodeTransformOptions<T>, Pick<DuplexOptions, 'readable' | 'writable' | 'defaultEncoding' | 'captureRejections'> {}

const kCallback = Symbol('kCallback');

/**
 * As with `Writable._final`, `_flush` has no default implementation but `node:stream` types it
 * as always present; `final()` checks for it at runtime before calling it.
 */
export interface Transform {
	_flush(callback: TransformCallback): void;
}

/**
 * A Duplex where the output is derived from the input.
 *
 * Back-pressure is driven by the reading side: `_transform()` is only called again once the
 * previous output has been consumed, so an inflating transform cannot run away with memory.
 */
export class Transform extends Duplex implements NodeTransform {
	private [kCallback]: WriteCallback | null = null;

	public constructor(options?: TransformOptions) {
		// A Duplex buffers on both sides while a Transform only wants `highWaterMark` elements
		// in total, so a readable hwm of 0 disables buffering on the writable side too.
		const readableHighWaterMark = options ? getHighWaterMark(false, options, 'readableHighWaterMark', true) : null;
		if (readableHighWaterMark === 0) {
			options = {
				...options,
				highWaterMark: undefined,
				readableHighWaterMark,
				writableHighWaterMark: options!.writableHighWaterMark || 0,
			};
		}

		// As in Duplex, only TypeScript's contravariant `this` check objects to forwarding these.
		super(options as DuplexOptions);

		// _read is implemented below, so the guard against emitting before the first read can go.
		this._readableState.sync = false;

		if (options) {
			if (typeof options.transform === 'function') this._transform = options.transform;
			if (typeof options.flush === 'function') this._flush = options.flush;
		}

		// Using 'prefinish' rather than _final keeps working for transforms that implement
		// _final themselves instead of, or as well as, _flush.
		this.on('prefinish', prefinish);
	}

	// eslint-disable-next-line @typescript-eslint/no-unused-vars -- part of the contract subclasses implement
	public _transform(chunk: any, encoding: BufferEncoding, callback: TransformCallback): void {
		throw new ERR_METHOD_NOT_IMPLEMENTED('_transform()');
	}

	public override _final(callback?: WriteCallback): void {
		final.call(this, callback);
	}

	public override _write(chunk: any, encoding: BufferEncoding, callback: WriteCallback): void {
		const rState = this._readableState;
		const wState = this._writableState;
		const length = rState.length;

		this._transform(chunk, encoding, (err, val) => {
			if (err) {
				callback(err);
				return;
			}

			if (val != null) this.push(val);

			if (rState.ended) {
				// push(null) during the transform: let the new state settle before continuing.
				nextTick(callback);
			} else if (wState.ended || length === rState.length || rState.length < rState.highWaterMark) {
				callback();
			} else {
				this[kCallback] = callback;
			}
		});
	}

	public override _read(): void {
		const callback = this[kCallback];
		if (!callback) return;
		this[kCallback] = null;
		callback();
	}
}

function final(this: Transform, cb?: WriteCallback): void {
	if (typeof this._flush !== 'function' || this.destroyed) {
		this.push(null);
		cb?.();
		return;
	}

	this._flush((er, data) => {
		if (er) {
			if (cb) cb(er);
			else this.destroy(er);
			return;
		}

		if (data != null) this.push(data);
		this.push(null);
		cb?.();
	});
}

function prefinish(this: Transform): void {
	if (this._final !== Transform.prototype._final) final.call(this);
}
