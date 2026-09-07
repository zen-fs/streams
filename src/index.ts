/*!
 * @zenfs/streams — https://npmjs.com/package/@zenfs/streams
 * Copyright © James Prevett and other ZenFS contributors.
 * SPDX-License-Identifier: LGPL-3.0-or-later
 */

/**
 * A modern polyfill for `node:stream`.
 *
 * This package exists to replace `readable-stream`, which accounts for most of `@zenfs/core`'s
 * dependency tree and has long-standing problems with its types and legacy code.
 *
 * Importing this module is what installs `Readable.prototype.compose`; everything else is
 * available from the individual modules too.
 *
 * @see https://github.com/zen-fs/core/issues/294
 *
 * @module
 */

import { destroyer } from './destroy.js';
import { Duplex } from './duplex.js';
import { duplexPair } from './duplexpair.js';
import { eos, finished } from './end-of-stream.js';
import { Stream } from './legacy.js';
import { PassThrough } from './passthrough.js';
import { pipeline } from './pipeline.js';
import * as promises from './promises.js';
import { Readable } from './readable.js';
import { getDefaultHighWaterMark, setDefaultHighWaterMark } from './state.js';
import { Transform } from './transform.js';
import { isDestroyed, isDisturbed, isErrored, isReadable, isWritable } from './utils.js';
import { Writable } from './writable.js';

import { addAbortSignal } from './add-abort-signal.js';
import { compose } from './compose.js';

export { addAbortSignal } from './add-abort-signal.js';
export { compose } from './compose.js';
export { destroyer as destroy } from './destroy.js';
export { Duplex } from './duplex.js';
export { duplexPair } from './duplexpair.js';
export { eos } from './end-of-stream.js';
export { EventEmitter } from './events.js';
export { createBatchedAsyncIterator, normalizeAsyncValue, normalizeBatch, toAsyncStreamable, toStreamable } from './iter.js';
export { PassThrough } from './passthrough.js';
export { Readable, ReadableState } from './readable.js';
export { getDefaultHighWaterMark, setDefaultHighWaterMark } from './state.js';
export { StringDecoder } from './string_decoder.js';
export { Transform } from './transform.js';
export { isDestroyed, isDisturbed, isErrored, isReadable, isWritable } from './utils.js';
export { Writable, WritableState } from './writable.js';
export { promises };

export type { DuplexOptions } from './duplex.js';
export type {
	DuplexToWebOptions,
	ReadableToWebOptions,
	ReadableIteratorOptions,
	FinishedOptions,
	ReadableOperatorOptions,
	TransformCallback,
} from 'node:stream';
export type { ReadableWritablePair } from 'node:stream/web';
export type { PipelineOptions } from 'node:stream/promises';
export type { FinishedCallback, FinishedPromiseOptions } from './end-of-stream.js';
export type { EventEmitterOptions, EventKey, Listener } from './events.js';
export type { PipelineCallback, PipelineStage } from './pipeline.js';
export type { ReadableOptions } from './readable.js';
export type { TransformOptions } from './transform.js';
export type { ChunkEncoding, WritableOptions, WriteCallback, WriteRequest } from './writable.js';

const kPromisifyCustom = Symbol.for('nodejs.util.promisify.custom');

const _finished = Object.assign(eos, { [kPromisifyCustom]: finished, __promisify__: finished });
const _pipeline = Object.assign(pipeline, { [kPromisifyCustom]: promises.pipeline, __promisify__: promises.pipeline });

export { _finished as finished, _pipeline as pipeline };

/* `node:stream` exposes everything as properties of `Stream` as well as as named exports, and
   its default export is `Stream` itself. Both spellings are kept so existing code ports over. */
const _Stream = Object.assign(Stream, {
	Stream,
	Readable,
	Writable,
	Duplex,
	Transform,
	PassThrough,
	pipeline: _pipeline,
	finished: _finished,
	duplexPair,
	addAbortSignal,
	compose,
	destroy: destroyer,
	getDefaultHighWaterMark,
	setDefaultHighWaterMark,
	isDestroyed,
	isDisturbed,
	isErrored,
	isReadable,
	isWritable,
	promises,
});

export { _Stream as Stream };

export default _Stream;
