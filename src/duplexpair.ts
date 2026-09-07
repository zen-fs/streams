// SPDX-License-Identifier: LGPL-3.0-or-later
import type { DestroyCallback } from './destroy.js';
import type { WriteCallback } from './writable.js';

import { Duplex } from './duplex.js';
import { nextTick } from './util.js';
import type { DuplexOptions } from 'node:stream';

const kCallback = Symbol('kCallback');
const kOtherSide = Symbol('kOtherSide');

class DuplexSide extends Duplex {
	private [kCallback]: WriteCallback | null = null;
	private [kOtherSide]: DuplexSide | null = null;

	/** Can only be set once, which is what keeps the pairing encapsulated. */
	public link(otherSide: DuplexSide): void {
		this[kOtherSide] ??= otherSide;
	}

	public override _read(): void {
		const callback = this[kCallback];
		if (!callback) return;
		this[kCallback] = null;
		callback();
	}

	public override _write(chunk: any, encoding: BufferEncoding, callback: WriteCallback): void {
		const other = this[kOtherSide]!;
		if (chunk.length === 0) {
			nextTick(callback);
			return;
		}
		other.push(chunk);
		other[kCallback] = callback;
	}

	public override _final(callback: WriteCallback): void {
		const other = this[kOtherSide]!;
		other.on('end', callback);
		other.push(null);
	}

	public override _destroy(err: Error | null, callback: DestroyCallback): void {
		const other = this[kOtherSide];

		if (other !== null && !other.destroyed) {
			// Deferred so the current execution stack is not torn down underneath us.
			nextTick(() => {
				if (other.destroyed) return;
				// Closed without the error, so the other side finishes rather than hanging,
				// and no unhandled 'error' is raised on a stream nobody is watching.
				if (err) other.destroy();
				else other.push(null);
			});
		}

		callback(err);
	}
}

/** Two Duplexes joined so that what is written to one can be read from the other. */
export function duplexPair(options?: DuplexOptions): [Duplex, Duplex] {
	const side0 = new DuplexSide(options);
	const side1 = new DuplexSide(options);
	side0.link(side1);
	side1.link(side0);
	return [side0, side1];
}
