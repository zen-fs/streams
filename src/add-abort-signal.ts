// SPDX-License-Identifier: LGPL-3.0-or-later
import type { Destroyable } from './destroy.js';

import { eos } from './end-of-stream.js';
import { AbortError, ERR_INVALID_ARG_TYPE } from './errors.js';
import { addAbortListener } from './util.js';
import { isNodeStream, isWebStream } from './utils.js';

/** Set by `node:stream` on the web streams it creates, to let an abort reach the underlying controller. */
const kControllerErrorFunction = Symbol.for('nodejs.webstream.controllerErrorFunction');

/** Destroys `stream` when `signal` aborts. */
export function addAbortSignal<T>(signal: AbortSignal, stream: T): T {
	if (typeof signal !== 'object' || !('aborted' in signal)) throw new ERR_INVALID_ARG_TYPE('signal', 'AbortSignal', signal);

	if (!isNodeStream(stream) && !isWebStream(stream)) {
		throw new ERR_INVALID_ARG_TYPE('stream', ['ReadableStream', 'WritableStream', 'Stream'], stream);
	}

	return addAbortSignalNoValidate(signal, stream);
}

/** @internal */
export function addAbortSignalNoValidate<T>(signal: AbortSignal, stream: T): T {
	if (typeof signal !== 'object' || !('aborted' in signal)) return stream;

	const onAbort = isNodeStream(stream)
		? () => (stream as Destroyable).destroy(new AbortError(undefined, { cause: signal.reason }))
		: () => {
				const error = (stream as Record<symbol, ((err: Error) => void) | undefined>)[kControllerErrorFunction];
				error?.(new AbortError(undefined, { cause: signal.reason }));
			};

	if (signal.aborted) {
		onAbort();
	} else {
		const disposable = addAbortListener(signal, onAbort);
		eos(stream, () => disposable[Symbol.dispose]());
	}

	return stream;
}
