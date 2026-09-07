/* eslint-disable @typescript-eslint/prefer-promise-reject-errors */
// SPDX-License-Identifier: LGPL-3.0-or-later

/**
 * The array-like methods on `Readable.prototype` (`map`, `filter`, `take`, `reduce`, ...).
 * Importing this module installs them; the package entry point does so for you.
 * @module
 */

import type { ReadableOperatorOptions } from 'node:stream';
import type { Readable, Readable as ReadableClass } from './readable.js';

import { destroyer } from './destroy.js';
import { finished } from './end-of-stream.js';
import { AbortError, ERR_ILLEGAL_CONSTRUCTOR, ERR_MISSING_ARGS, ERR_OUT_OF_RANGE } from './errors.js';
import { validateAbortSignal, validateFunction, validateInteger, validateObject } from './util.js';

type Fn = (value: any, options: { signal: AbortSignal }) => any;

/** Returned by a mapper to drop the value rather than yield it. */
const kEmpty = Symbol('kEmpty');
const kEof = Symbol('kEof');

export function map(this: Readable, fn: Fn, options?: ReadableOperatorOptions): AsyncGenerator<any> {
	validateFunction(fn, 'fn');
	if (options != null) validateObject(options, 'options');
	if (options?.signal != null) validateAbortSignal(options.signal, 'options.signal');

	const concurrency = options?.concurrency != null ? Math.floor(options.concurrency) : 1;
	let highWaterMark = options?.highWaterMark != null ? Math.floor(options.highWaterMark) : concurrency - 1;

	validateInteger(concurrency, 'options.concurrency', 1);
	validateInteger(highWaterMark, 'options.highWaterMark', 0);

	highWaterMark += concurrency;

	return async function* map(this: Readable) {
		const signal = AbortSignal.any(options?.signal ? [options.signal] : []);
		const stream = this;
		const queue: (Promise<unknown> | typeof kEof)[] = [];
		const signalOpt = { signal };

		/** Gates: the consumer waits on `next` for a value, the pump waits on `resume` for room. */
		let next: PromiseWithResolvers<void> | null = null;
		let resume: PromiseWithResolvers<void> | null = null;
		let done = false;
		let count = 0;

		function afterItemProcessed(): void {
			count -= 1;
			maybeResume();
		}

		function onCatch(): void {
			done = true;
			afterItemProcessed();
		}

		function releaseResume(): void {
			resume?.resolve();
			resume = null;
		}

		function maybeResume(): void {
			if (resume && !done && count < concurrency && queue.length < highWaterMark) releaseResume();
		}

		async function pump(): Promise<void> {
			try {
				for await (let val of stream) {
					if (done) return;
					if (signal.aborted) throw new AbortError();

					let promise: Promise<unknown>;
					try {
						val = fn(val, signalOpt);
						if (val === kEmpty) continue;
						promise = Promise.resolve(val);
					} catch (err) {
						promise = Promise.reject(err);
					}

					count += 1;
					promise.then(afterItemProcessed, onCatch);
					queue.push(promise);

					next?.resolve();
					next = null;

					if (!done && (queue.length >= highWaterMark || count >= concurrency)) {
						resume = Promise.withResolvers();
						await resume.promise;
					}
				}
				queue.push(kEof);
			} catch (err) {
				const val = Promise.reject(err);
				val.then(afterItemProcessed, onCatch);
				queue.push(val);
			} finally {
				done = true;
				next?.resolve();
				next = null;
			}
		}

		void pump();

		try {
			for (;;) {
				while (queue.length > 0) {
					const val = await queue[0];

					if (val === kEof) return;
					if (signal.aborted) throw new AbortError();
					if (val !== kEmpty) yield val;

					void queue.shift();
					maybeResume();
				}

				next = Promise.withResolvers();
				await next.promise;
			}
		} finally {
			done = true;
			releaseResume();
			destroyer(stream, null);
		}
	}.call(this);
}

export function filter(this: Readable, fn: Fn, options?: ReadableOperatorOptions): AsyncGenerator<any> {
	validateFunction(fn, 'fn');
	return map.call(this, async (value, opts) => ((await fn(value, opts)) ? value : kEmpty), options);
}

export function flatMap(this: Readable, fn: Fn, options?: ReadableOperatorOptions): AsyncGenerator<any> {
	const values = map.call(this, fn, options);
	return (async function* flatMap() {
		for await (const val of values) yield* val as Iterable<unknown>;
	})();
}

function toIntegerOrInfinity(number: unknown): number {
	// Coerced to match https://github.com/tc39/proposal-iterator-helpers/issues/169
	const n = Number(number);
	if (Number.isNaN(n)) return 0;
	if (n < 0) throw new ERR_OUT_OF_RANGE('number', '>= 0', n);
	return n;
}

export function drop(this: Readable, number: number, options?: ReadableOperatorOptions): AsyncGenerator<any> {
	if (options != null) validateObject(options, 'options');
	if (options?.signal != null) validateAbortSignal(options.signal, 'options.signal');

	let remaining = toIntegerOrInfinity(number);

	return async function* drop(this: Readable) {
		if (options?.signal?.aborted) throw new AbortError();
		for await (const val of this) {
			if (options?.signal?.aborted) throw new AbortError();
			if (remaining-- <= 0) yield val;
		}
	}.call(this);
}

export function take(this: Readable, number: number, options?: ReadableOperatorOptions): AsyncGenerator<any> {
	if (options != null) validateObject(options, 'options');
	if (options?.signal != null) validateAbortSignal(options.signal, 'options.signal');

	let remaining = toIntegerOrInfinity(number);

	return async function* take(this: Readable) {
		if (options?.signal?.aborted) throw new AbortError();
		for await (const val of this) {
			if (options?.signal?.aborted) throw new AbortError();
			if (remaining-- > 0) yield val;
			// Stop rather than pulling one more value we would discard.
			if (remaining <= 0) return;
		}
	}.call(this);
}

export async function some(this: Readable, fn: Fn, options?: ReadableOperatorOptions): Promise<boolean> {
	for await (const value of filter.call(this, fn, options)) {
		void value;
		return true;
	}
	return false;
}

export async function every(this: Readable, fn: Fn, options?: ReadableOperatorOptions): Promise<boolean> {
	validateFunction(fn, 'fn');
	// De Morgan: not any that fail.
	return !(await some.call(this, async (...args: Parameters<Fn>) => !(await fn(...args)), options));
}

export async function find(this: Readable, fn: Fn, options?: ReadableOperatorOptions): Promise<any> {
	for await (const result of filter.call(this, fn, options)) return result;
	return undefined;
}

export async function forEach(this: Readable, fn: Fn, options?: ReadableOperatorOptions): Promise<void> {
	validateFunction(fn, 'fn');
	const mapped = map.call(
		this,
		async (value, opts) => {
			await fn(value, opts);
			return kEmpty;
		},
		options
	);
	for await (const value of mapped) void value;
}

export async function toArray(this: Readable, options?: ReadableOperatorOptions): Promise<any[]> {
	if (options != null) validateObject(options, 'options');
	if (options?.signal != null) validateAbortSignal(options.signal, 'options.signal');

	const result: any[] = [];
	for await (const val of this) {
		if (options?.signal?.aborted) throw new AbortError(undefined, { cause: options.signal.reason });
		result.push(val);
	}
	return result;
}

export async function reduce(
	this: Readable,
	reducer: (previous: any, value: any, options: { signal: AbortSignal }) => any,
	initialValue?: any,
	options?: ReadableOperatorOptions
): Promise<any> {
	validateFunction(reducer, 'reducer');
	if (options != null) validateObject(options, 'options');
	if (options?.signal != null) validateAbortSignal(options.signal, 'options.signal');

	let hasInitialValue = arguments.length > 1;

	if (options?.signal?.aborted) {
		const err = new AbortError(undefined, { cause: options.signal.reason });
		// The error reaches the caller through the throw below.
		this.once('error', () => {});
		await finished(this.destroy(err));
		throw err;
	}

	const ac = new AbortController();
	const signal = ac.signal;
	if (options?.signal) options.signal.addEventListener('abort', () => ac.abort(), { once: true });

	let gotAnyItem = false;
	try {
		for await (const value of this) {
			gotAnyItem = true;
			if (options?.signal?.aborted) throw new AbortError();

			if (!hasInitialValue) {
				initialValue = value;
				hasInitialValue = true;
			} else {
				initialValue = await reducer(initialValue, value, { signal });
			}
		}
		if (!gotAnyItem && !hasInitialValue) {
			const err = new ERR_MISSING_ARGS('reduce');
			err.message = 'Reduce of an empty stream requires an initial value';
			throw err;
		}
	} finally {
		ac.abort();
	}

	return initialValue;
}

/** Operators that produce another Readable. */
export const streamReturningOperators = { drop, filter, flatMap, map, take };

/** Operators that produce a promise. */
export const promiseReturningOperators = { every, forEach, reduce, toArray, some, find };

function define(Readable: typeof ReadableClass, key: string, fn: (this: Readable, ...args: any[]) => unknown): void {
	Object.defineProperty(fn, 'name', { value: key });
	Object.defineProperty(Readable.prototype, key, { value: fn, enumerable: false, configurable: true, writable: true });
}

/** Installs the operators on `Readable.prototype`. The package entry point calls this. */
export function installOperators(Readable: typeof ReadableClass): void {
	for (const [key, op] of Object.entries(streamReturningOperators)) {
		define(Readable, key, function (this: Readable, ...args: any[]) {
			if (new.target) throw new ERR_ILLEGAL_CONSTRUCTOR();
			// Wrapped back into a Readable so the result stays composable.
			return Readable.from((op as (this: Readable, ...a: any[]) => unknown).apply(this, args) as AsyncGenerator<unknown>);
		});
	}

	for (const [key, op] of Object.entries(promiseReturningOperators)) {
		define(Readable, key, function (this: Readable, ...args: any[]) {
			if (new.target) throw new ERR_ILLEGAL_CONSTRUCTOR();
			return (op as (this: Readable, ...a: any[]) => unknown).apply(this, args);
		});
	}
}
