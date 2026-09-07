import shared from 'utilium/eslint';

export default [
	{ ignores: ['**/.*.ts'] },
	...shared(import.meta.dirname),
	{
		rules: {
			'@typescript-eslint/no-explicit-any': 'off',
			'@typescript-eslint/no-unsafe-call': 'off',
			'@typescript-eslint/no-unsafe-declaration-merging': 'off',
			'@typescript-eslint/unbound-method': 'off',
		},
	},
];
