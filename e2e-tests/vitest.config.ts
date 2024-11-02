import { defineConfig } from 'vite';

export default defineConfig({
  test: {
    sequence: {
      concurrent: true
    },
    testTimeout: 30000
  }
});