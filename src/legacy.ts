// SPDX-License-Identifier: LGPL-3.0-or-later
import type { EventKey, Listener } from './events.js';
import { EventEmitter } from './events.js';

/** The minimum a `pipe()` destination has to provide. */
export interface PipeDestination {
	writable?: boolean;
	write(chunk: any): boolean;
	end(): unknown;
	destroy?(error?: Error): unknown;
	on(event: string, listener: Listener): unknown;
	removeListener(event: string, listener: Listener): unknown;
	emit(event: string, ...args: any[]): unknown;
	prependListener?(event: string, listener: Listener): unknown;
	listenerCount?(event: string): number;
	_isStdio?: boolean;
}

/** Adds `listener` ahead of the existing ones, falling back to `on` for emitters that cannot prepend. */
export function prependListener(emitter: PipeDestination | EventEmitter, event: EventKey, listener: Listener): void {
	if (typeof emitter.prependListener === 'function') emitter.prependListener(event as string, listener);
	else emitter.on(event as string, listener);
}

/**
 * The base of every stream class, providing only the pre-`Readable` `pipe()`.
 * `Readable` overrides `pipe()` with the back-pressure aware implementation.
 */
export class Stream extends EventEmitter {
	public pipe<T extends PipeDestination>(dest: T, options?: { end?: boolean }): T {
		const source = this;

		function ondata(chunk: unknown): void {
			if (dest.writable && dest.write(chunk) === false && (source as { pause?: () => void }).pause) {
				(source as unknown as { pause: () => void }).pause();
			}
		}

		source.on('data', ondata);

		function ondrain(): void {
			const src = source as unknown as { readable?: boolean; resume?: () => void };
			if (src.readable && src.resume) src.resume();
		}

		dest.on('drain', ondrain);

		let didOnEnd = false;

		function onend(): void {
			if (didOnEnd) return;
			didOnEnd = true;
			dest.end();
		}

		function onclose(): void {
			if (didOnEnd) return;
			didOnEnd = true;
			if (typeof dest.destroy === 'function') dest.destroy();
		}

		// Without `end: false`, the destination is ended once the source is done.
		if (!dest._isStdio && options?.end !== false) {
			source.on('end', onend);
			source.on('close', onclose);
		}

		function onerror(this: PipeDestination | Stream, err: Error): void {
			cleanup();
			// Re-emit only if we just removed the last 'error' handler, so the error is not swallowed.
			if (this.listenerCount?.('error') === 0) this.emit('error', err);
		}

		prependListener(source, 'error', onerror);
		prependListener(dest, 'error', onerror);

		function cleanup(): void {
			source.removeListener('data', ondata);
			dest.removeListener('drain', ondrain);

			source.removeListener('end', onend);
			source.removeListener('close', onclose);

			source.removeListener('error', onerror);
			dest.removeListener('error', onerror);

			source.removeListener('end', cleanup);
			source.removeListener('close', cleanup);

			dest.removeListener('close', cleanup);
		}

		source.on('end', cleanup);
		source.on('close', cleanup);
		dest.on('close', cleanup);

		dest.emit('pipe', source);

		return dest;
	}
}
