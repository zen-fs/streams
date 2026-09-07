# ZenFS Streams

A modern polyfill for [`node:stream`](https://nodejs.org/api/stream.html).

This package replaces [`readable-stream`](https://github.com/nodejs/readable-stream) in [ZenFS](https://github.com/zen-fs/core). `readable-stream` made up 10 of the 17 dependencies in the `@zenfs/core` tree and totaled 703 KB unpacked, and it has a number of unresolved problems with its types and legacy code.

It is written in TypeScript against Node's current implementation, and every exported class is declared `implements` its `node:stream` counterpart.

## Usage

```js
import { Readable, Writable, pipeline } from '@zenfs/streams';

await pipeline(
	Readable.from(['hello', ' ', 'world']),
	new Writable({
		write(chunk, encoding, callback) {
			console.log(chunk.toString());
			callback();
		},
	})
);
```

## Notes

- The only runtime dependency is [`buffer`](https://github.com/feross/buffer), which resolves to the built-in module on Node. `events`, `process`, `abort-controller` and `string_decoder` are not needed: `EventEmitter` and `StringDecoder` are implemented here, and `AbortController` and `queueMicrotask` are used directly.
- `StringDecoder` decodes UTF-8 with a streaming `TextDecoder`. UTF-16LE is handled separately, since Node drops a trailing odd byte and preserves lone surrogates where the WHATWG decoder replaces both with U+FFFD.
- Outside Node there is no `process.nextTick`, so callbacks fall back to `queueMicrotask`. Node drains its next-tick queue ahead of the microtask queue; the fallback has no equivalent, so those callbacks interleave with promise continuations rather than preceding them.
- `finished()` on a WHATWG stream needs the closed-promise symbol that `node:stream` (and this package's `toWeb()`) attaches. A plain `ReadableStream` cannot be observed without consuming it, so it is rejected.
- `setMaxListeners` is honored as state but never produces Node's `MaxListenersExceededWarning`.
