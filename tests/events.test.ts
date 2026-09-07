// SPDX-License-Identifier: LGPL-3.0-or-later
import { EventEmitter } from '@zenfs/streams';
import assert from 'node:assert/strict';
import { suite, test } from 'node:test';

suite('EventEmitter', () => {
	test('calls listeners in order with all arguments', () => {
		const emitter = new EventEmitter();
		const seen: string[] = [];
		emitter.on('x', (...args: unknown[]) => seen.push('a' + args.join('')));
		emitter.on('x', (...args: unknown[]) => seen.push('b' + args.join('')));
		assert.equal(emitter.emit('x', 1, 2), true);
		assert.deepEqual(seen, ['a12', 'b12']);
	});

	test('emit returns false with no listeners', () => {
		assert.equal(new EventEmitter().emit('nobody'), false);
	});

	test('prependListener puts a listener first', () => {
		const emitter = new EventEmitter();
		const seen: string[] = [];
		emitter.on('x', () => seen.push('second'));
		emitter.prependListener('x', () => seen.push('first'));
		emitter.emit('x');
		assert.deepEqual(seen, ['first', 'second']);
	});

	test('once fires exactly once and is removed', () => {
		const emitter = new EventEmitter();
		let count = 0;
		emitter.once('x', () => count++);
		emitter.emit('x');
		emitter.emit('x');
		assert.equal(count, 1);
		assert.equal(emitter.listenerCount('x'), 0);
	});

	test('removeListener finds the original behind a once wrapper', () => {
		const emitter = new EventEmitter();
		const listener = (): void => {};
		emitter.once('x', listener);
		assert.deepEqual(emitter.listeners('x'), [listener]);
		emitter.removeListener('x', listener);
		assert.equal(emitter.listenerCount('x'), 0);
	});

	test('rawListeners exposes the once wrapper', () => {
		const emitter = new EventEmitter();
		const listener = (): void => {};
		emitter.once('x', listener);
		assert.notEqual(emitter.rawListeners('x')[0], listener);
		assert.equal((emitter.rawListeners('x')[0] as { listener?: unknown }).listener, listener);
	});

	test('an unhandled error event throws the error', () => {
		const emitter = new EventEmitter();
		const error = new Error('unhandled');
		assert.throws(() => emitter.emit('error', error), error);
	});

	test('an unhandled non-Error error event throws ERR_UNHANDLED_ERROR', () => {
		assert.throws(() => new EventEmitter().emit('error', 'oops'), { code: 'ERR_UNHANDLED_ERROR' });
	});

	test('errorMonitor sees errors without handling them', () => {
		const emitter = new EventEmitter();
		let monitored: unknown;
		emitter.on(EventEmitter.errorMonitor, (err: unknown) => (monitored = err));
		const error = new Error('watched');
		assert.throws(() => emitter.emit('error', error), error);
		assert.equal(monitored, error);
	});

	test('removing a listener mid-emit does not skip the next one', () => {
		const emitter = new EventEmitter();
		const seen: string[] = [];
		const first = (): void => {
			seen.push('first');
			emitter.removeListener('x', first);
		};
		emitter.on('x', first);
		emitter.on('x', () => seen.push('second'));
		emitter.emit('x');
		assert.deepEqual(seen, ['first', 'second']);
	});

	test('removeAllListeners clears one event or all of them', () => {
		const emitter = new EventEmitter();
		emitter.on('a', () => {});
		emitter.on('b', () => {});
		emitter.removeAllListeners('a');
		assert.deepEqual(emitter.eventNames(), ['b']);
		emitter.removeAllListeners();
		assert.deepEqual(emitter.eventNames(), []);
	});

	test('newListener and removeListener meta-events fire', () => {
		const emitter = new EventEmitter();
		const seen: string[] = [];
		emitter.on('newListener', (type: string) => seen.push('+' + type));
		emitter.on('removeListener', (type: string) => seen.push('-' + type));
		const listener = (): void => {};
		emitter.on('x', listener);
		emitter.removeListener('x', listener);
		assert.deepEqual(seen, ['+removeListener', '+x', '-x']);
	});

	test('rejects a non-function listener', () => {
		assert.throws(() => new EventEmitter().on('x', undefined as never), { code: 'ERR_INVALID_ARG_TYPE' });
	});
});
