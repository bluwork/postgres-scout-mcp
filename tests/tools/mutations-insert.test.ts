import { describe, it, expect, vi, beforeEach } from 'vitest';
import { safeInsert } from '../../src/tools/mutations.js';
import { createMockClient, createMockConnection, createTestLogger } from '../helpers/mock-connection.js';
import type { DatabaseConnection } from '../../src/types.js';
import type { Logger } from '../../src/utils/logger.js';

describe('safeInsert', () => {
  let mockClient: ReturnType<typeof createMockClient>;
  let connection: DatabaseConnection;
  let logger: Logger;

  beforeEach(() => {
    mockClient = createMockClient();
    connection = createMockConnection(mockClient);
    logger = createTestLogger();
  });

  describe('validation guards', () => {
    it('rejects empty columns', async () => {
      const result = await safeInsert(connection, logger, {
        table: 'users',
        schema: 'public',
        columns: [],
        rows: ['["Alice"]'],
        dryRun: false,
        maxRows: 1000,
        onConflict: 'error' as const,
      });
      expect(result.blocked).toBe(true);
      expect(result.reason).toMatch(/columns/i);
    });

    it('rejects empty rows', async () => {
      const result = await safeInsert(connection, logger, {
        table: 'users',
        schema: 'public',
        columns: ['name'],
        rows: [],
        dryRun: false,
        maxRows: 1000,
        onConflict: 'error' as const,
      });
      expect(result.blocked).toBe(true);
      expect(result.reason).toMatch(/rows/i);
    });

    it('rejects rows exceeding maxRows', async () => {
      const rows = Array.from({ length: 5 }, () => '["Alice"]');
      const result = await safeInsert(connection, logger, {
        table: 'users',
        schema: 'public',
        columns: ['name'],
        rows,
        dryRun: false,
        maxRows: 3,
        onConflict: 'error' as const,
      });
      expect(result.blocked).toBe(true);
      expect(result.reason).toMatch(/exceeds maxRows/i);
    });

    it('rejects invalid JSON in rows', async () => {
      const result = await safeInsert(connection, logger, {
        table: 'users',
        schema: 'public',
        columns: ['name'],
        rows: ['not valid json'],
        dryRun: false,
        maxRows: 1000,
        onConflict: 'error' as const,
      });
      expect(result.blocked).toBe(true);
      expect(result.reason).toMatch(/invalid row json/i);
    });

    it('rejects row with wrong column count', async () => {
      const result = await safeInsert(connection, logger, {
        table: 'users',
        schema: 'public',
        columns: ['name', 'email'],
        rows: ['["Alice"]'],
        dryRun: false,
        maxRows: 1000,
        onConflict: 'error' as const,
      });
      expect(result.blocked).toBe(true);
      expect(result.reason).toMatch(/1 values but 2 columns/);
    });
  });

  describe('dry run', () => {
    it('returns preview without calling executeQuery', async () => {
      const result = await safeInsert(connection, logger, {
        table: 'users',
        schema: 'public',
        columns: ['name', 'email'],
        rows: ['["Alice", "alice@example.com"]', '["Bob", "bob@example.com"]'],
        dryRun: true,
        maxRows: 1000,
        onConflict: 'error' as const,
      });
      expect(result.dryRun).toBe(true);
      expect(result.operation).toBe('INSERT');
      expect(result.wouldInsert).toBe(2);
      expect(result.columns).toEqual(['name', 'email']);
      expect(result.sampleRows).toHaveLength(2);
      expect(mockClient.query).not.toHaveBeenCalled();
    });
  });
});
