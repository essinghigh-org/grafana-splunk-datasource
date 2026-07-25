import { defineConfig } from 'eslint/config';
import baseConfig from './.config/eslint.config.mjs';

export default defineConfig([
  {
    ignores: [
      '**/.cache/',
      '**/.npm/',
      '**/coverage/',
      '**/dist/',
      '**/node_modules/',
      '**/*~',
      '**/\\#*',
      '**/.\\#*',
      '**/yarn-error.log',
      '**/.eslintcache',
    ],
  },
  ...baseConfig,
]);
