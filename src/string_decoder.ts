// SPDX-License-Identifier: LGPL-3.0-or-later
import { Buffer } from 'buffer';
import { ERR_INVALID_ARG_TYPE, ERR_UNKNOWN_ENCODING } from './errors.js';

/** Resolves an encoding alias to the spelling Node reports, or `undefined` if unknown. */
export function normalizeEncoding(encoding?: string | null): BufferEncoding | undefined {
	if (encoding == null || encoding === 'utf8' || encoding === 'utf-8') return 'utf8';

	switch (encoding.length > 8 ? encoding.toLowerCase() : encoding) {
		case 'utf8':
		case 'utf-8':
			return 'utf8';
		case 'ucs2':
		case 'ucs-2':
		case 'utf16le':
		case 'utf-16le':
			return 'utf16le';
		case 'latin1':
		case 'binary':
			return 'latin1';
		case 'base64':
			return 'base64';
		case 'base64url':
			return 'base64url';
		case 'ascii':
			return 'ascii';
		case 'hex':
			return 'hex';
		default:
			return encoding.toLowerCase() === encoding ? undefined : normalizeEncoding(encoding.toLowerCase());
	}
}

/** Bytes per group for the encodings whose characters span a fixed number of bytes. */
const groupSize = { base64: 3, base64url: 3 } as const;

/**
 * Decodes byte chunks into strings without splitting multi-byte characters across chunks.
 *
 * `utf8` uses a streaming `TextDecoder`. `utf16le` is handled here instead, because Node
 * drops a trailing odd byte and preserves lone surrogates where the WHATWG decoder replaces
 * both with U+FFFD. The remaining encodings only need their fixed-size groups buffered.
 */
export class StringDecoder {
	public readonly encoding: BufferEncoding;

	readonly #decoder?: TextDecoder;

	/** Bytes held back because they do not yet form a whole character. */
	#pending?: Buffer;

	public constructor(encoding?: string | null) {
		const normalized = normalizeEncoding(encoding);
		if (normalized === undefined) throw new ERR_UNKNOWN_ENCODING(encoding);
		this.encoding = normalized;

		// `ignoreBOM` keeps a leading BOM in the output, which is what Node does.
		if (normalized === 'utf8') this.#decoder = new TextDecoder('utf-8', { ignoreBOM: true });
	}

	public write(buf: string | ArrayBufferView): string {
		if (typeof buf === 'string') return buf;
		if (!ArrayBuffer.isView(buf)) throw new ERR_INVALID_ARG_TYPE('buf', ['string', 'Buffer', 'TypedArray', 'DataView'], buf);

		if (this.#decoder) return this.#decoder.decode(buf as Uint8Array, { stream: true });

		let chunk = Buffer.from(buf.buffer, buf.byteOffset, buf.byteLength);
		if (this.#pending) {
			chunk = Buffer.concat([this.#pending, chunk]);
			this.#pending = undefined;
		}

		if (this.encoding === 'utf16le') return this.#writeUTF16(chunk);

		const size = groupSize[this.encoding as keyof typeof groupSize];
		if (!size) return chunk.toString(this.encoding);

		const partial = chunk.length % size;
		if (!partial) return chunk.toString(this.encoding);

		this.#pending = Buffer.from(chunk.subarray(chunk.length - partial));
		return chunk.toString(this.encoding, 0, chunk.length - partial);
	}

	#writeUTF16(chunk: Buffer): string {
		if (chunk.length % 2 !== 0) {
			this.#pending = Buffer.from(chunk.subarray(chunk.length - 1));
			return chunk.toString('utf16le', 0, chunk.length - 1);
		}

		const text = chunk.toString('utf16le');

		// A trailing high surrogate may be completed by the next chunk.
		const last = text.charCodeAt(text.length - 1);
		if (last >= 0xd800 && last <= 0xdbff) {
			this.#pending = Buffer.from(chunk.subarray(chunk.length - 2));
			return text.slice(0, -1);
		}

		return text;
	}

	public end(buf?: string | ArrayBufferView): string {
		let result = buf === undefined ? '' : this.write(buf);

		// A final `decode()` without `stream` flushes any incomplete sequence and resets the decoder.
		if (this.#decoder) return result + this.#decoder.decode();

		if (this.#pending) {
			// For utf16le a held pair is a lone surrogate, which Node emits as-is; a single
			// held byte cannot form a code unit and is dropped.
			if (this.encoding !== 'utf16le') result += this.#pending.toString(this.encoding);
			else if (this.#pending.length === 2) result += this.#pending.toString('utf16le');

			this.#pending = undefined;
		}

		return result;
	}
}
