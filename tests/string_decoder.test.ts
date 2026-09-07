// SPDX-License-Identifier: LGPL-3.0-or-later
import { StringDecoder } from '@zenfs/streams';
import assert from 'node:assert/strict';
import { StringDecoder as NodeStringDecoder } from 'node:string_decoder';
import { suite, test } from 'node:test';

const samples = ['plain ascii', 'héllo wörld', '✓ ünïcödé ✗', '😀😃😄 emoji beyond the BMP', 'mixed: aé中😀z', ''];

const encodings = ['utf8', 'utf16le', 'latin1', 'ascii', 'base64', 'base64url', 'hex'] as const;

/** Every way of cutting `buf` into chunks of at most `size` bytes. */
function chunk(buf: Buffer, size: number): Buffer[] {
	const out: Buffer[] = [];
	for (let i = 0; i < buf.length; i += size) out.push(buf.subarray(i, i + size));
	return out;
}

/** Byte sequences that are not valid text, where replacement behaviour differs between decoders. */
const rawSamples: Buffer[] = [
	Buffer.from([0xe2, 0x82]), // truncated 3-byte utf8 sequence
	Buffer.from([0xff, 0xfe, 0x41]),
	Buffer.from([0x00, 0xd8]), // lone utf16le high surrogate
	Buffer.from([0x00, 0xd8, 0x41, 0x00]),
	Buffer.from([0x61, 0x62, 0x63]), // odd length
	Buffer.from([0xc0, 0x80]),
	Buffer.from([0xf0, 0x9f, 0x98, 0x80]),
];

suite('StringDecoder', () => {
	test('matches node:string_decoder across encodings and chunk sizes', () => {
		for (const encoding of encodings) {
			for (const sample of samples) {
				const bytes = Buffer.from(sample, 'utf8');
				for (const size of [1, 2, 3, 5, 7, bytes.length || 1]) {
					const ours = new StringDecoder(encoding);
					const theirs = new NodeStringDecoder(encoding);

					let a = '';
					let b = '';
					for (const part of chunk(bytes, size)) {
						a += ours.write(part);
						b += theirs.write(part);
					}
					a += ours.end();
					b += theirs.end();

					assert.equal(a, b, `${encoding} size=${size} sample=${JSON.stringify(sample)}`);
				}
			}
		}
	});

	test('matches node:string_decoder on byte sequences that are not valid text', () => {
		for (const encoding of encodings) {
			for (const bytes of rawSamples) {
				for (const size of [1, 2, 3, bytes.length]) {
					const ours = new StringDecoder(encoding);
					const theirs = new NodeStringDecoder(encoding);

					let a = '';
					let b = '';
					for (const part of chunk(bytes, size)) {
						a += ours.write(part);
						b += theirs.write(part);
					}
					a += ours.end();
					b += theirs.end();

					assert.equal(a, b, `${encoding} size=${size} bytes=${bytes.toString('hex')}`);
				}
			}
		}
	});

	test('normalizes encoding aliases the way node does', () => {
		for (const [alias, expected] of [
			['utf-8', 'utf8'],
			['UTF8', 'utf8'],
			['ucs2', 'utf16le'],
			['ucs-2', 'utf16le'],
			['utf-16le', 'utf16le'],
			['binary', 'latin1'],
			['HEX', 'hex'],
		] as const) {
			assert.equal(new StringDecoder(alias).encoding, expected);
			assert.equal(
				new StringDecoder(alias).encoding,
				(new NodeStringDecoder(alias as BufferEncoding) as unknown as { encoding: string }).encoding
			);
		}
	});

	test('rejects an unknown encoding', () => {
		assert.throws(() => new StringDecoder('nonsense'), { code: 'ERR_UNKNOWN_ENCODING' });
	});

	test('leaves a BOM in place', () => {
		const withBom = Buffer.from('﻿hi', 'utf8');
		assert.equal(new StringDecoder('utf8').end(withBom), new NodeStringDecoder('utf8').end(withBom));
	});

	test('passes strings through untouched', () => {
		assert.equal(new StringDecoder('utf8').write('already a string'), 'already a string');
	});

	test('is reusable after end()', () => {
		const decoder = new StringDecoder('utf8');
		const bytes = Buffer.from('é', 'utf8');
		assert.equal(decoder.write(bytes.subarray(0, 1)), '');
		assert.equal(decoder.end(), '�');
		assert.equal(decoder.end(Buffer.from('ok')), 'ok');
	});
});
