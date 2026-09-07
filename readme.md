# ZenFS Streams

A modern polyfill for [`node:stream`](https://nodejs.org/api/stream.html).

This package replaces [`readable-stream`](https://github.com/nodejs/readable-stream) in [ZenFS](https://github.com/zen-fs/core). `readable-stream` made up 10 of the 17 dependencies in the `@zenfs/core` tree and totaled 703 KB unpacked, and it has a number of unresolved problems with its types and legacy code.

> [!IMPORTANT]
> This package is a work in progress and is not yet usable. See [zen-fs/core#294](https://github.com/zen-fs/core/issues/294).

## Usage

```js
import { Readable, Writable, finished } from '@zenfs/streams';
```

For more information, see the [API documentation](https://zenfs.dev/streams/).
