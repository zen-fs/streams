// SPDX-License-Identifier: LGPL-3.0-or-later

/**
 * Node's error codes, reproduced closely enough that `err.code` checks and
 * message assertions written against `node:stream` keep working.
 * @module
 */

const kTypes = ['string', 'function', 'number', 'object', 'Function', 'Object', 'boolean', 'bigint', 'symbol'];

const classRegExp = /^([A-Z][a-z0-9]*)+$/;

function formatList(array: string[], type: string = 'and'): string {
	switch (array.length) {
		case 0:
			return '';
		case 1:
			return array[0];
		case 2:
			return `${array[0]} ${type} ${array[1]}`;
		case 3:
			return `${array[0]}, ${array[1]}, ${type} ${array[2]}`;
		default:
			return `${array.slice(0, -1).join(', ')}, ${type} ${array[array.length - 1]}`;
	}
}

/** Describes a value the way Node's `ERR_INVALID_ARG_TYPE` messages do. */
export function determineSpecificType(value: unknown): string {
	if (value === null) return 'null';
	if (value === undefined) return 'undefined';

	switch (typeof value) {
		case 'bigint':
			return `type bigint (${value}n)`;
		case 'number':
			if (value === 0) return 1 / value === -Infinity ? 'type number (-0)' : 'type number (0)';
			if (Number.isNaN(value)) return 'type number (NaN)';
			if (value === Infinity || value === -Infinity) return `type number (${value})`;
			return `type number (${value})`;
		case 'boolean':
			return `type boolean (${value})`;
		case 'symbol':
			return `type symbol (${String(value)})`;
		case 'function':
			return `function ${value.name}`;
		case 'string':
			return `type string (${JSON.stringify(value.length > 28 ? value.slice(0, 25) + '...' : value)})`;
		default: {
			const name = value.constructor?.name;
			return name ? `an instance of ${name}` : 'an instance of Object';
		}
	}
}

/* Node reports its errors as `Name [CODE]: message` while leaving `name` alone, and carries
   the code on a `code` property. These three bases supply that over each built-in error type. */

abstract class CodedError extends Error {
	public abstract readonly code: string;

	public override toString(): string {
		return `${this.name} [${this.code}]: ${this.message}`;
	}
}

abstract class CodedTypeError extends TypeError {
	public abstract readonly code: string;

	public override toString(): string {
		return `${this.name} [${this.code}]: ${this.message}`;
	}
}

abstract class CodedRangeError extends RangeError {
	public abstract readonly code: string;

	public override toString(): string {
		return `${this.name} [${this.code}]: ${this.message}`;
	}
}

export class AbortError extends Error {
	public readonly code = 'ABORT_ERR';
	public override readonly name = 'AbortError';

	public constructor(message: string = 'The operation was aborted', options?: ErrorOptions) {
		super(message, options);
	}
}

/**
 * Combines two errors into an `AggregateError`, preferring `outerError`'s message and code.
 * Returns whichever error is present when only one is.
 */
export function aggregateTwoErrors(innerError: Error | null | undefined, outerError: Error): Error;
export function aggregateTwoErrors(innerError?: Error | null, outerError?: Error | null): Error | null | undefined;
export function aggregateTwoErrors(innerError?: Error | null, outerError?: Error | null): Error | null | undefined {
	if (!innerError || !outerError || innerError === outerError) return innerError || outerError;

	if (Array.isArray((outerError as AggregateError).errors)) {
		(outerError as AggregateError).errors.push(innerError);
		return outerError;
	}

	const error = new AggregateError([outerError, innerError], outerError.message);
	(error as Error & { code?: string }).code = (outerError as Error & { code?: string }).code;
	return error;
}

export class ERR_INVALID_ARG_TYPE extends CodedTypeError {
	public readonly code = 'ERR_INVALID_ARG_TYPE';
	public constructor(name: string, expected: string | string[], actual: unknown) {
		if (!Array.isArray(expected)) expected = [expected];

		let msg = 'The ';
		if (name.endsWith(' argument')) msg += `${name} `;
		else msg += `"${name}" ${name.includes('.') ? 'property' : 'argument'} `;
		msg += 'must be ';

		const types: string[] = [];
		const instances: string[] = [];
		const other: string[] = [];

		for (const value of expected) {
			if (kTypes.includes(value)) types.push(value.toLowerCase());
			else if (classRegExp.test(value)) instances.push(value);
			else other.push(value);
		}

		if (instances.length) {
			const pos = types.indexOf('object');
			if (pos !== -1) {
				types.splice(pos, 1);
				instances.push('Object');
			}
		}

		if (types.length) {
			msg += `${types.length > 1 ? 'one of type' : 'of type'} ${formatList(types, 'or')}`;
			if (instances.length || other.length) msg += ' or ';
		}

		if (instances.length) {
			msg += `an instance of ${formatList(instances, 'or')}`;
			if (other.length) msg += ' or ';
		}

		if (other.length > 1) msg += `one of ${formatList(other, 'or')}`;
		else if (other.length) msg += (other[0].toLowerCase() !== other[0] ? 'an ' : '') + other[0];

		super(`${msg}. Received ${determineSpecificType(actual)}`);
	}
}

export class ERR_INVALID_ARG_VALUE extends CodedTypeError {
	public readonly code = 'ERR_INVALID_ARG_VALUE';
	public constructor(name: string, value: unknown, reason: string = 'is invalid') {
		super(`The ${name.includes('.') ? 'property' : 'argument'} '${name}' ${reason}. Received ${determineSpecificType(value)}`);
	}
}

export class ERR_INVALID_RETURN_VALUE extends CodedTypeError {
	public readonly code = 'ERR_INVALID_RETURN_VALUE';
	public constructor(input: string, name: string, value: unknown) {
		super(`Expected ${input} to be returned from the "${name}" function but got ${determineSpecificType(value)}.`);
	}
}

export class ERR_MISSING_ARGS extends CodedTypeError {
	public readonly code = 'ERR_MISSING_ARGS';
	public constructor(...args: (string | string[])[]) {
		const names = args.map(arg => (Array.isArray(arg) ? arg.map(a => `"${a}"`).join(' or ') : `"${arg}"`));
		super(`The ${formatList(names)} argument${args.length > 1 ? 's' : ''} must be specified`);
	}
}

export class ERR_OUT_OF_RANGE extends CodedRangeError {
	public readonly code = 'ERR_OUT_OF_RANGE';
	public constructor(name: string, range: string, input: unknown) {
		super(`The value of "${name}" is out of range. It must be ${range}. Received ${determineSpecificType(input)}`);
	}
}

export class ERR_UNKNOWN_ENCODING extends CodedTypeError {
	public readonly code = 'ERR_UNKNOWN_ENCODING';
	public constructor(encoding: unknown) {
		super(`Unknown encoding: ${String(encoding)}`);
	}
}

export class ERR_METHOD_NOT_IMPLEMENTED extends CodedError {
	public readonly code = 'ERR_METHOD_NOT_IMPLEMENTED';
	public constructor(method: string) {
		super(`The ${method} method is not implemented`);
	}
}

export class ERR_MULTIPLE_CALLBACK extends CodedError {
	public readonly code = 'ERR_MULTIPLE_CALLBACK';
	public constructor() {
		super('Callback called multiple times');
	}
}

export class ERR_ILLEGAL_CONSTRUCTOR extends CodedTypeError {
	public readonly code = 'ERR_ILLEGAL_CONSTRUCTOR';
	public constructor() {
		super('Illegal constructor');
	}
}

export class ERR_UNHANDLED_ERROR extends CodedError {
	public readonly code = 'ERR_UNHANDLED_ERROR';
	public constructor(context?: unknown) {
		// eslint-disable-next-line @typescript-eslint/no-base-to-string
		super('Unhandled error.' + (context === undefined ? '' : ` (${String(context)})`));
	}
}

export class ERR_STREAM_ALREADY_FINISHED extends CodedError {
	public readonly code = 'ERR_STREAM_ALREADY_FINISHED';
	public constructor(method: string) {
		super(`Cannot call ${method} after a stream was finished`);
	}
}

export class ERR_STREAM_CANNOT_PIPE extends CodedError {
	public readonly code = 'ERR_STREAM_CANNOT_PIPE';
	public constructor() {
		super('Cannot pipe, not readable');
	}
}

export class ERR_STREAM_DESTROYED extends CodedError {
	public readonly code = 'ERR_STREAM_DESTROYED';
	public constructor(method: string) {
		super(`Cannot call ${method} after a stream was destroyed`);
	}
}

export class ERR_STREAM_NULL_VALUES extends CodedTypeError {
	public readonly code = 'ERR_STREAM_NULL_VALUES';
	public constructor() {
		super('May not write null values to stream');
	}
}

export class ERR_STREAM_PREMATURE_CLOSE extends CodedError {
	public readonly code = 'ERR_STREAM_PREMATURE_CLOSE';
	public constructor() {
		super('Premature close');
	}
}

export class ERR_STREAM_PUSH_AFTER_EOF extends CodedError {
	public readonly code = 'ERR_STREAM_PUSH_AFTER_EOF';
	public constructor() {
		super('stream.push() after EOF');
	}
}

export class ERR_STREAM_UNABLE_TO_PIPE extends CodedError {
	public readonly code = 'ERR_STREAM_UNABLE_TO_PIPE';
	public constructor() {
		super('Cannot pipe to a closed or destroyed stream');
	}
}

export class ERR_STREAM_UNSHIFT_AFTER_END_EVENT extends CodedError {
	public readonly code = 'ERR_STREAM_UNSHIFT_AFTER_END_EVENT';
	public constructor() {
		super('stream.unshift() after end event');
	}
}

export class ERR_STREAM_WRITE_AFTER_END extends CodedError {
	public readonly code = 'ERR_STREAM_WRITE_AFTER_END';
	public constructor() {
		super('write after end');
	}
}
