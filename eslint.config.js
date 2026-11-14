import js from '@eslint/js';
import jsxA11y from 'eslint-plugin-jsx-a11y';
import reactHooks from 'eslint-plugin-react-hooks';
import globals from 'globals';
import tseslint from 'typescript-eslint';

// Renderer code must call the core through `@mrl/api-client`, never a hand-written fetch.
const noRawApiFetch = {
  'no-restricted-syntax': [
    'error',
    ...[
      "CallExpression[callee.name='fetch'][arguments.0.value=/^\\/api/]",
      "CallExpression[callee.property.name='fetch'][arguments.0.value=/^\\/api/]",
      "CallExpression[callee.name='fetch'][arguments.0.quasis.0.value.raw=/^\\/api/]",
      "CallExpression[callee.property.name='fetch'][arguments.0.quasis.0.value.raw=/^\\/api/]",
    ].map((selector) => ({
      selector,
      message: "Use the @mrl/api-client hooks instead of fetch('/api…').",
    })),
  ],
};

export default tseslint.config(
  { ignores: ['**/dist/**', '**/release/**', '**/test-results/**', '**/playwright-report/**'] },
  js.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  {
    languageOptions: {
      parserOptions: { projectService: true, tsconfigRootDir: import.meta.dirname },
    },
    rules: {
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/restrict-template-expressions': ['error', { allowNumber: true }],
    },
  },
  { files: ['**/*.js'], ...tseslint.configs.disableTypeChecked },
  { files: ['**/*.js'], languageOptions: { globals: globals.node } },
  {
    files: ['apps/web/**/*.{ts,tsx}', 'packages/ui/**/*.{ts,tsx}'],
    plugins: { 'react-hooks': reactHooks, 'jsx-a11y': jsxA11y },
    languageOptions: { ...jsxA11y.flatConfigs.strict.languageOptions, globals: globals.browser },
    rules: {
      ...reactHooks.configs.flat['recommended-latest'].rules,
      ...jsxA11y.flatConfigs.strict.rules,
      ...noRawApiFetch,
    },
  },
  {
    files: ['apps/desktop/**/*.ts', '**/vite.config.ts', '**/playwright.config.ts'],
    languageOptions: { globals: globals.node },
  },
);
