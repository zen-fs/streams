// SPDX-License-Identifier: LGPL-3.0-or-later
import type { PassThrough as NodePassThrough, TransformCallback } from 'node:stream';
import type { TransformOptions } from './transform.js';

import { Transform } from './transform.js';

/** The most minimal Transform: every chunk written comes back out unchanged. */
export class PassThrough extends Transform implements NodePassThrough {
	public constructor(options?: TransformOptions) {
		super(options);
	}

	public override _transform(chunk: any, encoding: BufferEncoding, callback: TransformCallback): void {
		callback(null, chunk);
	}
}
