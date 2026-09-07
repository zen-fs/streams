// SPDX-License-Identifier: LGPL-3.0-or-later
import { ERR_INVALID_ARG_TYPE, ERR_UNHANDLED_ERROR } from './errors.js';
import { nextTick } from './util.js';

export type EventKey = string | symbol;

export type Listener = (...args: any[]) => unknown;

interface WrappedListener extends Listener {
	listener: Listener;
}

type Listeners = Record<EventKey, Listener | Listener[] | undefined>;

/** Emitted before an error is delivered to `error` listeners, and even when there are none. */
export const errorMonitor = Symbol('events.errorMonitor');

/** Implemented by an emitter to handle promise rejections returned from its listeners. */
export const captureRejectionSymbol = Symbol.for('nodejs.rejection');

const kCapture = Symbol('kCapture');

export interface EventEmitterOptions {
	/** Route rejections from listeners returning promises to the emitter's `Symbol.for('nodejs.rejection')` handler. */
	captureRejections?: boolean;
}

function checkListener(listener: unknown): asserts listener is Listener {
	if (typeof listener !== 'function') throw new ERR_INVALID_ARG_TYPE('listener', 'Function', listener);
}

/**
 * The subset of `node:events` that streams are built on, with Node's semantics:
 * an unhandled `error` event throws, listeners fire in insertion order against a
 * snapshot of the list, and `once()` wrappers expose their original via `.listener`.
 *
 * `setMaxListeners` is honored as state but never produces Node's
 * `MaxListenersExceededWarning`.
 */
export class EventEmitter {
	public static defaultMaxListeners: number = 10;

	/** The default for the `captureRejections` option of newly created emitters. */
	public static captureRejections: boolean = false;

	public static readonly captureRejectionSymbol: typeof captureRejectionSymbol = captureRejectionSymbol;

	public static readonly errorMonitor: typeof errorMonitor = errorMonitor;

	public _events: Listeners = Object.create(null) as Listeners;

	public _eventsCount: number = 0;

	public _maxListeners?: number;

	protected [kCapture]: boolean;

	public constructor(options?: EventEmitterOptions) {
		this[kCapture] = options?.captureRejections ?? EventEmitter.captureRejections;
	}

	public setMaxListeners(n: number): this {
		if (typeof n !== 'number' || n < 0 || Number.isNaN(n)) throw new ERR_INVALID_ARG_TYPE('n', 'a non-negative number', n);
		this._maxListeners = n;
		return this;
	}

	public getMaxListeners(): number {
		return this._maxListeners ?? EventEmitter.defaultMaxListeners;
	}

	public emit(type: EventKey, ...args: any[]): boolean {
		const events = this._events;

		let doError = type === 'error';
		if (doError) {
			if (events[errorMonitor] !== undefined) this.emit(errorMonitor, ...args);
			doError = events.error === undefined;
		}

		if (doError) {
			const er = args[0];
			if (er instanceof Error) throw er;
			const err = new ERR_UNHANDLED_ERROR(er);
			(err as Error & { context?: unknown }).context = er;
			throw err;
		}

		const handler = events[type];
		if (handler === undefined) return false;

		if (typeof handler === 'function') {
			const result = handler.apply(this, args);
			if (result != null) this.#capture(result, type, args);
			return true;
		}

		for (const listener of handler.slice()) {
			const result = listener.apply(this, args);
			if (result != null) this.#capture(result, type, args);
		}
		return true;
	}

	#capture(result: unknown, type: EventKey, args: unknown[]): void {
		if (!this[kCapture]) return;

		try {
			const then = (result as PromiseLike<unknown>).then;
			if (typeof then !== 'function') return;
			then.call(result, undefined, (err: unknown) => {
				nextTick(() => {
					const capture = (this as unknown as Record<symbol, ((err: unknown, type: EventKey, args: unknown[]) => void) | undefined>)[
						captureRejectionSymbol
					];
					if (capture) capture.call(this, err, type, args);
					else this.emit('error', err);
				});
			});
		} catch (err) {
			this.emit('error', err);
		}
	}

	#add(type: EventKey, listener: Listener, prepend: boolean): this {
		checkListener(listener);

		const events = this._events;

		if (events.newListener !== undefined) this.emit('newListener', type, (listener as WrappedListener).listener ?? listener);

		const existing = events[type];

		if (existing === undefined) {
			events[type] = listener;
			++this._eventsCount;
		} else if (typeof existing === 'function') {
			events[type] = prepend ? [listener, existing] : [existing, listener];
		} else if (prepend) {
			existing.unshift(listener);
		} else {
			existing.push(listener);
		}

		return this;
	}

	public addListener(type: EventKey, listener: Listener): this {
		return this.#add(type, listener, false);
	}

	public on(type: EventKey, listener: Listener): this {
		return this.#add(type, listener, false);
	}

	public prependListener(type: EventKey, listener: Listener): this {
		return this.#add(type, listener, true);
	}

	#wrapOnce(type: EventKey, listener: Listener): WrappedListener {
		let fired = false;
		const wrapped = ((...args: unknown[]) => {
			if (fired) return;
			fired = true;
			this.removeListener(type, wrapped);
			return listener.apply(this, args);
		}) as WrappedListener;
		wrapped.listener = listener;
		return wrapped;
	}

	public once(type: EventKey, listener: Listener): this {
		checkListener(listener);
		return this.#add(type, this.#wrapOnce(type, listener), false);
	}

	public prependOnceListener(type: EventKey, listener: Listener): this {
		checkListener(listener);
		return this.#add(type, this.#wrapOnce(type, listener), true);
	}

	public removeListener(type: EventKey, listener: Listener): this {
		checkListener(listener);

		const events = this._events;
		const list = events[type];
		if (list === undefined) return this;

		if (list === listener || (list as WrappedListener).listener === listener) {
			if (--this._eventsCount === 0) this._events = Object.create(null) as Listeners;
			else delete events[type];
			if (events.removeListener !== undefined) this.emit('removeListener', type, (list as WrappedListener).listener ?? listener);
			return this;
		}

		if (typeof list === 'function') return this;

		let position = -1;
		let original: Listener | undefined;
		for (let i = list.length - 1; i >= 0; i--) {
			if (list[i] === listener || (list[i] as WrappedListener).listener === listener) {
				original = (list[i] as WrappedListener).listener;
				position = i;
				break;
			}
		}
		if (position < 0) return this;

		list.splice(position, 1);
		if (list.length === 1) events[type] = list[0];

		if (events.removeListener !== undefined) this.emit('removeListener', type, original ?? listener);
		return this;
	}

	public off(type: EventKey, listener: Listener): this {
		return this.removeListener(type, listener);
	}

	public removeAllListeners(type?: EventKey): this {
		const events = this._events;

		// Emitting 'removeListener' is only observable when someone is listening for it.
		if (events.removeListener === undefined) {
			if (type === undefined) {
				this._events = Object.create(null) as Listeners;
				this._eventsCount = 0;
			} else if (events[type] !== undefined) {
				if (--this._eventsCount === 0) this._events = Object.create(null) as Listeners;
				else delete events[type];
			}
			return this;
		}

		if (type === undefined) {
			for (const key of Reflect.ownKeys(events)) {
				if (key === 'removeListener') continue;
				this.removeAllListeners(key);
			}
			this.removeAllListeners('removeListener');
			this._events = Object.create(null) as Listeners;
			this._eventsCount = 0;
			return this;
		}

		const listeners = events[type];
		if (typeof listeners === 'function') this.removeListener(type, listeners);
		else if (listeners !== undefined) for (let i = listeners.length - 1; i >= 0; i--) this.removeListener(type, listeners[i]);

		return this;
	}

	#listeners(type: EventKey, unwrap: boolean): Listener[] {
		const handler = this._events[type];
		if (handler === undefined) return [];
		if (typeof handler === 'function') return [unwrap ? ((handler as WrappedListener).listener ?? handler) : handler];
		return handler.map(listener => (unwrap ? ((listener as WrappedListener).listener ?? listener) : listener));
	}

	public listeners(type: EventKey): Listener[] {
		return this.#listeners(type, true);
	}

	public rawListeners(type: EventKey): Listener[] {
		return this.#listeners(type, false);
	}

	public listenerCount(type: EventKey, listener?: Listener): number {
		const handler = this._events[type];
		if (handler === undefined) return 0;

		if (typeof handler === 'function')
			return listener === undefined || handler === listener || (handler as WrappedListener).listener === listener ? 1 : 0;

		if (listener === undefined) return handler.length;

		let count = 0;
		for (const current of handler) {
			if (current === listener || (current as WrappedListener).listener === listener) count++;
		}
		return count;
	}

	public eventNames(): EventKey[] {
		return this._eventsCount > 0 ? Reflect.ownKeys(this._events) : [];
	}
}
