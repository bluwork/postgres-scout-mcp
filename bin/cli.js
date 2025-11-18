#!/usr/bin/env node

import('../dist/index.js').catch((error) => {
  console.error('Failed to start postgres-scout-mcp:', error);
  process.exit(1);
});
