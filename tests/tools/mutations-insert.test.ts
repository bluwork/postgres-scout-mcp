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

  describe('query building', () => {
    it('builds correct parameterized INSERT query', async () => {
      mockClient.query.mockResolvedValue({ rows: [{ id: 1, name: 'Alice', email: 'alice@example.com' }], rowCount: 1 });

      const result = await safeInsert(connection, logger, {
        table: 'users',
        schema: 'public',
        columns: ['name', 'email'],
        rows: ['["Alice", "alice@example.com"]'],
        dryRun: false,
        maxRows: 1000,
        onConflict: 'error' as const,
      });

      // Find the INSERT call (skip the SET statement_timeout call)
      const calls = mockClient.query.mock.calls;
      const insertCall = calls.find((c: any) =>
        (typeof c[0] === 'object' ? c[0].text : c[0]).includes('INSERT')
      );

      expect(insertCall).toBeDefined();
      const queryObj = insertCall![0];
      expect(queryObj.text).toContain('INSERT INTO public.users');
      expect(queryObj.text).toContain('(name, email)');
      expect(queryObj.text).toContain('VALUES ($1, $2)');
      expect(queryObj.text).toContain('RETURNING *');
      expect(queryObj.text).not.toContain('ON CONFLICT');
      expect(queryObj.values).toEqual(['Alice', 'alice@example.com']);

      expect(result.success).toBe(true);
      expect(result.rowsInserted).toBe(1);
    });

    it('adds ON CONFLICT DO NOTHING when onConflict is skip', async () => {
      mockClient.query.mockResolvedValue({ rows: [], rowCount: 0 });

      await safeInsert(connection, logger, {
        table: 'users',
        schema: 'public',
        columns: ['name'],
        rows: ['["Alice"]'],
        dryRun: false,
        maxRows: 1000,
        onConflict: 'skip' as const,
      });

      const calls = mockClient.query.mock.calls;
      const insertCall = calls.find((c: any) =>
        (typeof c[0] === 'object' ? c[0].text : c[0]).includes('INSERT')
      );

      expect(insertCall).toBeDefined();
      expect(insertCall![0].text).toContain('ON CONFLICT DO NOTHING');
    });

    it('batches rows in groups of 500', async () => {
      mockClient.query.mockResolvedValue({ rows: [], rowCount: 250 });

      const rows = Array.from({ length: 750 }, (_, i) => `["user${i}"]`);
      await safeInsert(connection, logger, {
        table: 'users',
        schema: 'public',
        columns: ['name'],
        rows,
        dryRun: false,
        maxRows: 1000,
        onConflict: 'error' as const,
      });

      const insertCalls = mockClient.query.mock.calls.filter((c: any) =>
        (typeof c[0] === 'object' ? c[0].text : c[0]).includes('INSERT')
      );
      expect(insertCalls).toHaveLength(2);
    });
  });
});
