// SPDX-License-Identifier: LGPL-3.0-or-later
import type { DuplexOptions } from 'node:stream';
import { ERR_INVALID_ARG_VALUE } from './errors.js';
import { validateInteger } from './util.js';

let defaultBytes = 64 * 1024;
let defaultObjects = 16;

/** The `highWaterMark` used by streams that do not specify one. */
export function getDefaultHighWaterMark(objectMode?: boolean): number {
	return objectMode ? defaultObjects : defaultBytes;
}

/** Changes the process-wide default `highWaterMark` for object-mode or byte streams. */
export function setDefaultHighWaterMark(objectMode: boolean, value: number): void {
	validateInteger(value, 'value', 0);
	if (objectMode) defaultObjects = value;
	else defaultBytes = value;
}

/**
 * Resolves the `highWaterMark` for one side of a stream.
 * `duplexKey` is only consulted when `isDuplex`, matching how a Duplex configures its sides separately.
 */
export function getHighWaterMark(
	objectMode: boolean,
	options: Pick<DuplexOptions, 'highWaterMark' | 'readableHighWaterMark' | 'writableHighWaterMark'>,
	duplexKey: 'readableHighWaterMark' | 'writableHighWaterMark',
	isDuplex: boolean
): number {
	const hwm = options.highWaterMark != null ? options.highWaterMark : isDuplex ? options[duplexKey] : null;

	if (hwm == null) return getDefaultHighWaterMark(objectMode);

	if (!Number.isInteger(hwm) || hwm < 0) throw new ERR_INVALID_ARG_VALUE(isDuplex ? `options.${duplexKey}` : 'options.highWaterMark', hwm);

	return Math.floor(hwm);
}
