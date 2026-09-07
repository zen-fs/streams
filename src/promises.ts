// SPDX-License-Identifier: LGPL-3.0-or-later

/**
 * The promise-returning forms of `finished()` and `pipeline()`, mirroring `node:stream/promises`.
 * @module
 */

import { finished } from './end-of-stream.js';
import type { PipelineOptions } from 'node:stream/promises';
import { pipelineImpl, type PipelineStage } from './pipeline.js';
import { isIterable, isNodeStream, isWebStream } from './utils.js';

export { finished };

/** Promise form of `pipeline()`. An options object may be passed as the final argument. */
export function pipeline(...streams: PipelineStage[]): Promise<any> {
	return new Promise<any>((resolve, reject) => {
		let options: PipelineOptions | undefined;

		const lastArg = streams[streams.length - 1];
		if (lastArg && typeof lastArg === 'object' && !isNodeStream(lastArg) && !isIterable(lastArg) && !isWebStream(lastArg)) {
			options = streams.pop() as PipelineOptions;
		}

		pipelineImpl(streams, (err, value) => (err ? reject(err) : resolve(value)), options);
	});
}
