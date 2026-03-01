import { describe, it, expect, vi, beforeEach } from 'vitest';
import { optimizeQuery } from '../src/tools/optimization.js';
import { DatabaseConnection } from '../src/types.js';
import { Logger } from '../src/utils/logger.js';

// EXPLAIN without ANALYZE returns no Execution Time, no Actual Rows
const ESTIMATED_PLAN_RESULT = {
  rows: [{
    'QUERY PLAN': [{
      'Planning Time': 0.5,
      Plan: {
        'Node Type': 'Seq Scan',
        'Relation Name': 'users',
        'Plan Rows': 50000,
        'Plan Width': 120,
        'Total Cost': 1234.56,
        'Startup Cost': 0.00
        // No 'Actual Rows', 'Actual Total Time', etc.
      }
    }]
  }],
  rowCount: 1
};

// EXPLAIN ANALYZE returns full actual data
const ANALYZED_PLAN_RESULT = {
  rows: [{
    'QUERY PLAN': [{
      'Planning Time': 0.5,
      'Execution Time': 45.2,
      Plan: {
        'Node Type': 'Seq Scan',
        'Relation Name': 'users',
        'Plan Rows': 50000,
        'Plan Width': 120,
        'Total Cost': 1234.56,
        'Startup Cost': 0.00,
        'Actual Rows': 48000,
        'Actual Total Time': 44.8,
        'Actual Loops': 1
      }
    }]
  }],
  rowCount: 1
};

function createMockConnection(mode: 'read-only' | 'read-write'): DatabaseConnection {
  return {
    pool: {} as any,
    config: {
      mode,
      connectionString: 'postgresql://localhost/test',
      queryTimeout: 30000,
      maxResultRows: 1000,
      enableRateLimit: false,
      rateLimitMaxRequests: 100,
      rateLimitWindowMs: 60000,
      logDir: '/tmp',
      logLevel: 'error'
    }
  };
}

function createMockLogger(): Logger {
  return {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn()
  } as any;
}

describe('optimizeQuery in read-only mode', () => {
  let roConnection: DatabaseConnection;
  let rwConnection: DatabaseConnection;
  let logger: Logger;
  let mockExecuteQuery: any;

  beforeEach(async () => {
    roConnection = createMockConnection('read-only');
    rwConnection = createMockConnection('read-write');
    logger = createMockLogger();

    // Mock executeInternalQuery at the module level
    const dbModule = await import('../src/utils/database.js');
    mockExecuteQuery = vi.spyOn(dbModule, 'executeInternalQuery');
  });

  it('does not crash when Execution Time is missing (RO mode)', async () => {
    mockExecuteQuery.mockResolvedValueOnce(ESTIMATED_PLAN_RESULT);

    const result = await optimizeQuery(roConnection, logger, {
      query: 'SELECT * FROM users WHERE status = $1',
      includeRewrite: true,
      includeIndexes: true
    });

    expect(result.error).toBeUndefined();
    expect(result.executionPlan).toBeDefined();
  });

  it('sets estimatedPlanOnly flag in RO mode', async () => {
    mockExecuteQuery.mockResolvedValueOnce(ESTIMATED_PLAN_RESULT);

    const result = await optimizeQuery(roConnection, logger, {
      query: 'SELECT * FROM users WHERE status = $1',
      includeRewrite: true,
      includeIndexes: true
    });

    expect(result.estimatedPlanOnly).toBe(true);
  });

  it('does not set estimatedPlanOnly in RW mode', async () => {
    mockExecuteQuery.mockResolvedValueOnce(ANALYZED_PLAN_RESULT);

    const result = await optimizeQuery(rwConnection, logger, {
      query: 'SELECT * FROM users WHERE status = $1',
      includeRewrite: true,
      includeIndexes: true
    });

    expect(result.estimatedPlanOnly).toBeUndefined();
  });

  it('includes a note explaining limited data in RO mode', async () => {
    mockExecuteQuery.mockResolvedValueOnce(ESTIMATED_PLAN_RESULT);

    const result = await optimizeQuery(roConnection, logger, {
      query: 'SELECT * FROM users WHERE status = $1',
      includeRewrite: true,
      includeIndexes: true
    });

    expect(result.note).toBeDefined();
    expect(result.note).toMatch(/read-only|estimated|ANALYZE/i);
  });

  it('reports planningTime but marks executionTime as unavailable in RO', async () => {
    mockExecuteQuery.mockResolvedValueOnce(ESTIMATED_PLAN_RESULT);

    const result = await optimizeQuery(roConnection, logger, {
      query: 'SELECT * FROM users WHERE status = $1',
      includeRewrite: true,
      includeIndexes: true
    });

    expect(result.executionPlan.planningTime).toMatch(/0\.50/);
    expect(result.executionPlan.executionTime).toMatch(/unavailable|N\/A/i);
    // totalTime should not be NaN
    expect(result.executionPlan.totalTime).not.toMatch(/NaN/);
  });

  it('detects seq scans using Plan Rows when Actual Rows missing', async () => {
    mockExecuteQuery.mockResolvedValueOnce(ESTIMATED_PLAN_RESULT);

    const result = await optimizeQuery(roConnection, logger, {
      query: 'SELECT * FROM users WHERE status = $1',
      includeRewrite: true,
      includeIndexes: true
    });

    // Plan Rows is 50000 — should still detect the seq scan
    const seqScanIssue = result.issues.find((i: any) => i.type === 'sequential_scan');
    expect(seqScanIssue).toBeDefined();
    expect(seqScanIssue.table).toBe('users');
  });

  it('reports actual times in RW mode (with ANALYZE)', async () => {
    mockExecuteQuery.mockResolvedValueOnce(ANALYZED_PLAN_RESULT);

    const result = await optimizeQuery(rwConnection, logger, {
      query: 'SELECT * FROM users WHERE status = $1',
      includeRewrite: true,
      includeIndexes: true
    });

    expect(result.executionPlan.planningTime).toMatch(/0\.50/);
    expect(result.executionPlan.executionTime).toMatch(/45\.20/);
    expect(result.executionPlan.totalTime).toMatch(/45\.70/);
    expect(result.estimatedPlanOnly).toBeUndefined();
  });
});
