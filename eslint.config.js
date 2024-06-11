const globals = require('globals');
const js = require('@eslint/js');
const eslintPluginPrettierRecommended = require('eslint-plugin-prettier/recommended');

module.exports = [
    js.configs.recommended,
    eslintPluginPrettierRecommended,
    {
        languageOptions: {
            sourceType: 'commonjs',
            globals: {
                ...globals.node
            },
            parserOptions: {
                ecmaVersion: 2022
            }
        }
    },
    {
        rules: {
            'no-console': 'warn',
            'no-unused-vars': 'off',
            'require-atomic-updates': 'off',
            'no-prototype-builtins': 'off'
        }
    }
];
