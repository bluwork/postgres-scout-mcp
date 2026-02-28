import { vi } from 'vitest';
import type { DatabaseConnection, ServerConfig } from '../../src/types.js';
import { Logger } from '../../src/utils/logger.js';
import { tmpdir } from 'os';
import { mkdtempSync } from 'fs';
import { join } from 'path';

export function createMockClient() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
    release: vi.fn(),
  };
}

export function createMockConnection(
  mockClient: ReturnType<typeof createMockClient>,
  mode: 'read-only' | 'read-write' = 'read-write'
): DatabaseConnection {
  const mockPool = {
    connect: vi.fn().mockResolvedValue(mockClient),
    options: { database: 'test' },
  };

  const config: ServerConfig = {
    mode,
    connectionString: 'postgresql://test:test@localhost:5432/test',
    queryTimeout: 30000,
    maxResultRows: 10000,
    enableRateLimit: false,
    rateLimitMaxRequests: 100,
    rateLimitWindowMs: 60000,
    logDir: mkdtempSync(join(tmpdir(), 'pg-scout-test-')),
    logLevel: 'error',
  };

  return { pool: mockPool as any, config };
}

export function createTestLogger(): Logger {
  const dir = mkdtempSync(join(tmpdir(), 'pg-scout-test-'));
  return new Logger(dir, 'error');
}
