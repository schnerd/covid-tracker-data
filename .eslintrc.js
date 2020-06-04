module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended',
    'plugin:prettier/recommended',
  ],
  plugins: ['@typescript-eslint'],
  rules: {
    '@typescript-eslint/ban-ts-comment': 'off',
    '@typescript-eslint/no-explicit-any': 'off',
    '@typescript-eslint/no-empty-function': 'off',
    '@typescript-eslint/no-empty-interface': 'off',
    '@typescript-eslint/no-unused-vars': 'off',
    '@typescript-eslint/no-unused-vars-experimental': 'warn',
    // Allow funcs to be out of order since they get hoisted
    '@typescript-eslint/no-use-before-define': ['error', 'nofunc'],
    // Disable for now, maybe add back later
    '@typescript-eslint/explicit-function-return-type': 'off',
    // Seems overly perscriptive
    '@typescript-eslint/interface-name-prefix': 'off',
  },
};
