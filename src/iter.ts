// SPDX-License-Identifier: LGPL-3.0-or-later

/**
 * The `Stream.toAsyncStreamable` protocol, which lets a `Readable` be consumed by the
 * `stream/iter` API as batches of `Uint8Array`.
 * @module
 */

import type { toAsyncStreamable as NodeToAsyncStreamable, toStreamable as NodeToStreamable } from 'node:stream/iter';
import type { Readable as ReadableClass } from './readable.js';

import { destroyer } from './destroy.js';
import { eos } from './end-of-stream.js';
import { aggregateTwoErrors, ERR_INVALID_ARG_TYPE } from './errors.js';
import { nop } from './util.js';

/*
 * Registered symbols, so these are the same keys `node:stream/iter` uses. The annotations
 * adopt node's `unique symbol` types; without them TypeScript widens to plain `symbol`, which
 * turns every member keyed by one into an index signature.
 */

/** Sync value-to-streamable protocol: `[Symbol.for('Stream.toStreamable')]()`. */
export const toStreamable: typeof NodeToStreamable = Symbol.for('Stream.toStreamable') as typeof NodeToStreamable;

/** Async value-to-streamable protocol: `[Symbol.for('Stream.toAsyncStreamable')]()`. */
export const toAsyncStreamable: typeof NodeToAsyncStreamable = Symbol.for('Stream.toAsyncStreamable') as typeof NodeToAsyncStreamable;

/** Caps how many buffered chunks go into one batch, bounding peak memory when `_read()` pushes many at once. */
const MAX_DRAIN_BATCH = 128;

const encoder = new TextEncoder();

type Protocol = Record<symbol, (() => unknown) | undefined>;

function hasProtocol(value: unknown, symbol: symbol): boolean {
	return value !== null && typeof value === 'object' && symbol in value && typeof (value as Protocol)[symbol] === 'function';
}

function toUint8Array(chunk: string | ArrayBuffer | ArrayBufferView): Uint8Array {
	if (typeof chunk === 'string') return encoder.encode(chunk);
	if (ArrayBuffer.isView(chunk)) return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
	return new Uint8Array(chunk);
}

/** Flattens a value into `Uint8Array`s, following the streamable protocols, promises and iterables. */
export async function* normalizeAsyncValue(value: unknown, allowNested: boolean = true): AsyncGenerator<Uint8Array> {
	if (value != null && typeof (value as PromiseLike<unknown>).then === 'function') {
		// eslint-disable-next-line @typescript-eslint/await-thenable
		yield* normalizeAsyncValue(await value, allowNested);
		return;
	}

	if (typeof value === 'string' || value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
		yield toUint8Array(value);
		return;
	}

	const isAsyncIterable = typeof (value as AsyncIterable<unknown>)?.[Symbol.asyncIterator] === 'function';

	if (!allowNested && (isAsyncIterable || hasProtocol(value, toAsyncStreamable))) {
		throw new ERR_INVALID_ARG_TYPE('value', ['string', 'ArrayBuffer', 'ArrayBufferView', 'Iterable', 'toStreamable'], value);
	}

	// Checked before toStreamable, which it takes precedence over.
	if (hasProtocol(value, toAsyncStreamable)) {
		yield* normalizeAsyncValue(await (value as Protocol)[toAsyncStreamable]!.call(value), allowNested);
		return;
	}

	if (hasProtocol(value, toStreamable)) {
		yield* normalizeAsyncValue((value as Protocol)[toStreamable]!.call(value), allowNested);
		return;
	}

	if (Array.isArray(value)) {
		for (const item of value) yield* normalizeAsyncValue(item, allowNested);
		return;
	}

	// Before sync iterables, since a value may be both.
	if (isAsyncIterable) {
		for await (const item of value as AsyncIterable<unknown>) yield* normalizeAsyncValue(item, allowNested);
		return;
	}

	if (typeof value !== 'string' && typeof (value as Iterable<unknown>)?.[Symbol.iterator] === 'function') {
		for (const item of value as Iterable<unknown>) yield* normalizeAsyncValue(item, allowNested);
		return;
	}

	throw new ERR_INVALID_ARG_TYPE(
		'value',
		['string', 'ArrayBuffer', 'ArrayBufferView', 'Iterable', 'AsyncIterable', 'toStreamable', 'toAsyncStreamable'],
		value
	);
}

/** Converts a batch from an object-mode or encoded stream to `Uint8Array`s, or `null` if it produced nothing. */
export async function normalizeBatch(raw: unknown[]): Promise<Uint8Array[] | null> {
	const batch: Uint8Array[] = [];
	for (const value of raw) {
		if (value instanceof Uint8Array) {
			batch.push(value);
			continue;
		}
		// Awaiting here can suspend; stream events during the suspension are queued rather
		// than lost, so an error surfaces on the next loop iteration.
		for await (const normalized of normalizeAsyncValue(value)) batch.push(normalized);
	}
	return batch.length > 0 ? batch : null;
}

/**
 * Like the plain async iterator, but drains everything currently buffered into a single batch
 * per yield, amortizing the promise and microtask cost over several chunks.
 *
 * `normalize` is only needed for object-mode and encoded streams; byte-mode chunks are already
 * `Uint8Array` subclasses and are yielded as they are.
 */
export async function* createBatchedAsyncIterator(
	stream: ReadableClass,
	normalize: ((raw: unknown[]) => Promise<Uint8Array[] | null>) | null
): AsyncGenerator<Uint8Array[]> {
	let callback: () => void = nop;

	function next(this: unknown, resolve: () => void): void {
		if (this === stream) {
			callback();
			callback = nop;
		} else {
			callback = resolve;
		}
	}

	stream.on('readable', next);

	/** `undefined` while active, `null` once ended cleanly, otherwise the error. */
	let error: Error | null | undefined;

	const cleanup = eos(stream, { writable: false }, err => {
		error = err ? (aggregateTwoErrors(error, err) ?? null) : null;
		callback();
		callback = nop;
	});

	try {
		for (;;) {
			const chunk = stream.destroyed ? null : stream.read();

			if (chunk !== null) {
				const batch = [chunk];
				while (batch.length < MAX_DRAIN_BATCH && stream._readableState.length > 0) {
					const c = stream.read();
					if (c === null) break;
					batch.push(c);
				}

				if (normalize === null) {
					yield batch as Uint8Array[];
				} else {
					const result = await normalize(batch);
					if (result !== null) yield result;
				}
			} else if (error) {
				throw error;
			} else if (error === null) {
				return;
			} else {
				await new Promise<void>(next);
			}
		}
	} catch (err: any) {
		error = aggregateTwoErrors(error, err as Error);
		throw error;
	} finally {
		if (error === undefined || stream._readableState.autoDestroy) {
			destroyer(stream, null);
		} else {
			stream.off('readable', next);
			cleanup();
		}
	}
}
