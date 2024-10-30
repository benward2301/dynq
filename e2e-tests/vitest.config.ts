import { defineConfig } from 'vite';

export default defineConfig({
  test: {
    sequence: {
      concurrent: true
    },
    maxConcurrency: 10,
    testTimeout: 30000
  }
});