// SPDX-License-Identifier: LGPL-3.0-or-later
import type { Readable, ReadableOptions } from './readable.js';

import { Buffer } from 'buffer';
import { aggregateTwoErrors, ERR_INVALID_ARG_TYPE, ERR_STREAM_NULL_VALUES } from './errors.js';
import { nextTick } from './util.js';

/**
 * Backs `Readable.from()`. The constructor is passed in rather than imported to keep
 * this module out of a cycle with `readable.js`.
 */
export function from(
	Readable: new (options?: ReadableOptions) => Readable,
	iterable: string | Buffer | Iterable<unknown> | AsyncIterable<unknown>,
	opts?: ReadableOptions
): Readable {
	if (typeof iterable === 'string' || iterable instanceof Buffer) {
		return new Readable({
			objectMode: true,
			...opts,
			read(this: Readable) {
				this.push(iterable);
				this.push(null);
			},
		});
	}

	let iterator: Iterator<unknown> | AsyncIterator<unknown>;
	let isAsync: boolean;

	if ((iterable as AsyncIterable<unknown>)?.[Symbol.asyncIterator]) {
		isAsync = true;
		iterator = (iterable as AsyncIterable<unknown>)[Symbol.asyncIterator]();
	} else if ((iterable as Iterable<unknown>)?.[Symbol.iterator]) {
		isAsync = false;
		iterator = (iterable as Iterable<unknown>)[Symbol.iterator]();
	} else {
		throw new ERR_INVALID_ARG_TYPE('iterable', ['Iterable'], iterable);
	}

	const readable = new Readable({ objectMode: true, highWaterMark: 1, ...opts });
	const originalDestroy = readable._destroy;

	/** Guards against `_read()` re-entering before the previous iteration settles. */
	let reading = false;
	let isAsyncValues = false;

	readable._read = function () {
		if (reading) return;
		reading = true;

		if (isAsync) void nextAsync();
		else if (isAsyncValues) void nextSyncWithAsyncValues();
		else
			for (;;) {
				try {
					const { value, done } = (iterator as Iterator<unknown>).next();

					if (done) {
						readable.push(null);
						return;
					}

					if (value && typeof (value as PromiseLike<unknown>).then === 'function') {
						void changeToAsyncValues(value as PromiseLike<unknown>);
						return;
					}

					if (value === null) {
						reading = false;
						throw new ERR_STREAM_NULL_VALUES();
					}

					if (readable.push(value)) continue;

					reading = false;
				} catch (err: any) {
					readable.destroy(err as Error);
				}
				break;
			}
	};

	readable._destroy = function (error, cb) {
		originalDestroy.call(this, error, destroyError => {
			const combined = destroyError || error;
			close(combined).then(
				// nextTick in case cb throws
				() => nextTick(cb, combined),
				(closeError: Error) => nextTick(cb, aggregateTwoErrors(combined, closeError))
			);
		});
	};

	async function close(error: Error | null): Promise<void> {
		if (error != null && typeof iterator.throw === 'function') {
			const { value, done } = await iterator.throw(error);
			await value;
			if (done) return;
		}
		if (typeof iterator.return === 'function') {
			const { value } = await iterator.return();
			await value;
		}
	}

	async function changeToAsyncValues(value: PromiseLike<unknown>): Promise<void> {
		isAsyncValues = true;

		try {
			const res = await value;

			if (res === null) {
				reading = false;
				throw new ERR_STREAM_NULL_VALUES();
			}

			if (readable.push(res)) {
				await nextSyncWithAsyncValues();
				return;
			}

			reading = false;
		} catch (err: any) {
			readable.destroy(err as Error);
		}
	}

	async function nextSyncWithAsyncValues(): Promise<void> {
		for (;;) {
			try {
				const { value, done } = (iterator as Iterator<unknown>).next();

				if (done) {
					readable.push(null);
					return;
				}

				// eslint-disable-next-line @typescript-eslint/await-thenable
				const res = value && typeof (value as PromiseLike<unknown>).then === 'function' ? await value : value;

				if (res === null) {
					reading = false;
					throw new ERR_STREAM_NULL_VALUES();
				}

				if (readable.push(res)) continue;

				reading = false;
			} catch (err: any) {
				readable.destroy(err as Error);
			}
			break;
		}
	}

	async function nextAsync(): Promise<void> {
		for (;;) {
			try {
				const { value, done } = await iterator.next();

				if (done) {
					readable.push(null);
					return;
				}

				if (value === null) {
					reading = false;
					throw new ERR_STREAM_NULL_VALUES();
				}

				if (readable.push(value)) continue;

				reading = false;
			} catch (err: any) {
				readable.destroy(err as Error);
			}
			break;
		}
	}

	return readable;
}
