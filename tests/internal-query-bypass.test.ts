import { describe, it, expect, vi } from 'vitest';
import { sanitizeQuery } from '../src/utils/sanitize.js';

/**
 * These tests verify the contract: internal tool queries containing blocked
 * patterns (pg_backend_pid, pg_stat_activity, etc.) must be allowed when
 * executeQuery is called with options.internal = true.
 *
 * Since executeQuery requires a real DB pool, we test the sanitizeQuery
 * function directly — it's the gate that executeQuery calls.
 */

describe('sanitizeQuery: internal tool queries must be allowed', () => {
  // getCurrentActivity uses pg_backend_pid() — blocked in QUERY_DANGEROUS_FUNCTIONS
  it('should allow pg_backend_pid() for internal queries', () => {
    const query = `
      SELECT pid, usename as user, state, query
      FROM pg_stat_activity
      WHERE pid != pg_backend_pid()
    `;
    // Internal queries should bypass the blocklist
    expect(() => sanitizeQuery(query, 'read-only', { internal: true })).not.toThrow();
  });

  // getCurrentActivity stats query
  it('should allow pg_stat_activity stats query for internal queries', () => {
    const query = `
      SELECT COUNT(*) as total_connections,
        COUNT(*) FILTER (WHERE state = 'active') as active_queries
      FROM pg_stat_activity
      WHERE pid != pg_backend_pid()
    `;
    expect(() => sanitizeQuery(query, 'read-only', { internal: true })).not.toThrow();
  });

  // Tools using pg_stat_database (getDatabaseStats, collectMetricSnapshot, etc.)
  it('should allow pg_stat_database for internal queries', () => {
    const query = `SELECT * FROM pg_stat_database WHERE datname = 'mydb'`;
    expect(() => sanitizeQuery(query, 'read-only', { internal: true })).not.toThrow();
  });

  // Tools using pg_stat_user_tables (suggestIndexes, analyzeTableBloat, etc.)
  it('should allow pg_stat_user_tables for internal queries', () => {
    const query = `SELECT * FROM pg_stat_user_tables`;
    expect(() => sanitizeQuery(query, 'read-only', { internal: true })).not.toThrow();
  });

  // analyzeLocks uses pg_stat_activity
  it('should allow pg_stat_activity JOIN for internal queries', () => {
    const query = `
      SELECT a.pid, a.usename
      FROM pg_locks l
      LEFT JOIN pg_stat_activity a ON a.pid = l.pid
    `;
    expect(() => sanitizeQuery(query, 'read-only', { internal: true })).not.toThrow();
  });
});

describe('sanitizeQuery: internal flag must NOT disable basic safety', () => {
  // Internal queries must still enforce operation whitelist
  it('should still reject DROP even with internal: true', () => {
    expect(() =>
      sanitizeQuery('DROP TABLE users', 'read-only', { internal: true })
    ).toThrow();
  });

  // Internal queries must still reject multi-statement
  it('should still reject multi-statement even with internal: true', () => {
    expect(() =>
      sanitizeQuery('SELECT 1; DROP TABLE users', 'read-only', { internal: true })
    ).toThrow();
  });

  // Internal queries must still enforce operation mode
  it('should still reject INSERT in read-only mode with internal: true', () => {
    expect(() =>
      sanitizeQuery('INSERT INTO t VALUES (1)', 'read-only', { internal: true })
    ).toThrow();
  });
});

describe('sanitizeQuery: findRecent query templates must pass', () => {
  // Exact query templates that findRecent builds
  it('should allow findRecent main query (internal)', () => {
    const query = `
      SELECT *
      FROM "public"."orders"
      WHERE "created_at" >= NOW() - INTERVAL '7 days'
      ORDER BY "created_at" DESC
      LIMIT $1
    `;
    expect(() => sanitizeQuery(query, 'read-only', { internal: true })).not.toThrow();
  });

  it('should allow findRecent count query (internal)', () => {
    const query = `
      SELECT
        COUNT(*) as rows_found,
        NOW() - INTERVAL '7 days' as threshold
      FROM "public"."orders"
      WHERE "created_at" >= NOW() - INTERVAL '7 days'
    `;
    expect(() => sanitizeQuery(query, 'read-only', { internal: true })).not.toThrow();
  });

  it('should allow findRecent main query even WITHOUT internal flag', () => {
    const query = `
      SELECT *
      FROM "public"."orders"
      WHERE "created_at" >= NOW() - INTERVAL '7 days'
      ORDER BY "created_at" DESC
      LIMIT $1
    `;
    expect(() => sanitizeQuery(query, 'read-only')).not.toThrow();
  });

  it('should allow findRecent count query even WITHOUT internal flag', () => {
    const query = `
      SELECT
        COUNT(*) as rows_found,
        NOW() - INTERVAL '7 days' as threshold
      FROM "public"."orders"
      WHERE "created_at" >= NOW() - INTERVAL '7 days'
    `;
    expect(() => sanitizeQuery(query, 'read-only')).not.toThrow();
  });
});

describe('sanitizeQuery: user queries must still be blocked', () => {
  // Without internal flag, blocked patterns must still throw
  it('should reject pg_backend_pid() for user queries (no internal flag)', () => {
    expect(() =>
      sanitizeQuery('SELECT pg_backend_pid()', 'read-only')
    ).toThrow();
  });

  it('should reject pg_backend_pid() when internal is false', () => {
    expect(() =>
      sanitizeQuery('SELECT pg_backend_pid()', 'read-only', { internal: false })
    ).toThrow();
  });

  it('should reject version() for user queries', () => {
    expect(() =>
      sanitizeQuery('SELECT version()', 'read-only')
    ).toThrow();
  });

  it('should reject current_user for user queries', () => {
    expect(() =>
      sanitizeQuery('SELECT current_user', 'read-only')
    ).toThrow();
  });
});
