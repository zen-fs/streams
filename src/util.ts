// SPDX-License-Identifier: LGPL-3.0-or-later
import { ERR_INVALID_ARG_TYPE, ERR_OUT_OF_RANGE } from './errors.js';

/** The `(error?) => void` shape every stream hook completes through. */
export type Callback = (error?: Error | null) => void;

export function nop(): void {}

export const kEmptyObject = Object.freeze(Object.create(null) as object);

export interface NextTick {
	<const A extends unknown[]>(callback: (...args: A) => unknown, ...args: A): void;
}

function queueTick(callback: (...args: any[]) => unknown, a?: unknown, b?: unknown, c?: unknown, d?: unknown): void {
	switch (arguments.length) {
		case 1:
			return queueMicrotask(callback as () => void);
		case 2:
			return queueMicrotask(() => callback(a));
		case 3:
			return queueMicrotask(() => callback(a, b));
		case 4:
			return queueMicrotask(() => callback(a, b, c));
		case 5:
			return queueMicrotask(() => callback(a, b, c, d));
		default: {
			// eslint-disable-next-line prefer-rest-params
			const args = Array.prototype.slice.call(arguments, 1);
			return queueMicrotask(() => callback(...args));
		}
	}
}

/**
 * `process.nextTick` where a process exists, otherwise `queueMicrotask`.
 *
 * Node drains the next-tick queue ahead of the microtask queue; the fallback has no
 * equivalent, so callbacks scheduled here interleave with promise continuations instead
 * of preceding them.
 */
export const nextTick: NextTick = typeof globalThis.process?.nextTick == 'function' ? globalThis.process.nextTick : queueTick;

/** Wraps `callback` so that only its first invocation is forwarded. */
export function once<const A extends unknown[]>(callback: (...args: A) => unknown): (this: unknown, ...args: A) => void {
	let called = false;
	return function (this: unknown, ...args: A) {
		if (called) return;
		called = true;
		callback.apply(this, args);
	};
}

export function validateFunction(value: unknown, name: string): asserts value is (...args: any[]) => unknown {
	if (typeof value !== 'function') throw new ERR_INVALID_ARG_TYPE(name, 'Function', value);
}

export function validateObject(value: unknown, name: string): void {
	if (value === null || Array.isArray(value) || typeof value !== 'object') throw new ERR_INVALID_ARG_TYPE(name, 'Object', value);
}

export function validateBoolean(value: unknown, name: string): asserts value is boolean {
	if (typeof value !== 'boolean') throw new ERR_INVALID_ARG_TYPE(name, 'boolean', value);
}

export function validateAbortSignal(signal: unknown, name: string): void {
	if (signal !== undefined && (signal === null || typeof signal !== 'object' || !('aborted' in signal))) {
		throw new ERR_INVALID_ARG_TYPE(name, 'AbortSignal', signal);
	}
}

export function validateInteger(value: unknown, name: string, min: number = Number.MIN_SAFE_INTEGER, max: number = Number.MAX_SAFE_INTEGER): void {
	if (typeof value !== 'number') throw new ERR_INVALID_ARG_TYPE(name, 'number', value);
	if (!Number.isInteger(value)) throw new ERR_OUT_OF_RANGE(name, 'an integer', value);
	if (value < min || value > max) throw new ERR_OUT_OF_RANGE(name, `>= ${min} && <= ${max}`, value);
}

/**
 * Runs `listener` the next time `signal` aborts, returning a disposable that detaches it.
 * An already-aborted signal never fires; callers check `signal.aborted` themselves.
 */
export function addAbortListener(signal: AbortSignal, listener: () => void): Disposable {
	signal.addEventListener('abort', listener, { once: true });
	return { [Symbol.dispose]: () => signal.removeEventListener('abort', listener) };
}
