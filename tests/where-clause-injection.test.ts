import { describe, it, expect } from 'vitest';
import { validateUserWhereClause } from '../src/utils/sanitize.js';

describe('validateUserWhereClause: injection prevention', () => {
  // --- Subquery injection (must block SELECT keyword) ---

  it('should reject subquery via scalar SELECT', () => {
    expect(() =>
      validateUserWhereClause('id = (SELECT 1)')
    ).toThrow();
  });

  it('should reject EXISTS subquery', () => {
    expect(() =>
      validateUserWhereClause('EXISTS (SELECT 1 FROM users)')
    ).toThrow();
  });

  it('should reject IN subquery', () => {
    expect(() =>
      validateUserWhereClause("id IN (SELECT id FROM admin_users)")
    ).toThrow();
  });

  it('should reject boolean-blind subquery extraction', () => {
    expect(() =>
      validateUserWhereClause("id = 1 AND (SELECT password FROM users LIMIT 1) IS NOT NULL")
    ).toThrow();
  });

  it('should reject case-varied SELECT subquery', () => {
    expect(() =>
      validateUserWhereClause('id = (sElEcT 1)')
    ).toThrow();
  });

  // --- Dangerous PostgreSQL functions ---

  it('should reject pg_sleep time-based injection', () => {
    expect(() =>
      validateUserWhereClause('id = 1 OR pg_sleep(5) IS NOT NULL')
    ).toThrow();
  });

  it('should reject pg_read_file', () => {
    expect(() =>
      validateUserWhereClause("id = 1 OR pg_read_file('/etc/passwd') IS NOT NULL")
    ).toThrow();
  });

  it('should reject pg_ls_dir', () => {
    expect(() =>
      validateUserWhereClause("id = 1 OR pg_ls_dir('/tmp') IS NOT NULL")
    ).toThrow();
  });

  it('should reject lo_import', () => {
    expect(() =>
      validateUserWhereClause("id = lo_import('/etc/passwd')")
    ).toThrow();
  });

  it('should reject lo_export', () => {
    expect(() =>
      validateUserWhereClause("id = lo_export(1234, '/tmp/out')")
    ).toThrow();
  });

  it('should reject dblink', () => {
    expect(() =>
      validateUserWhereClause("id = dblink('host=evil', 'SELECT 1')")
    ).toThrow();
  });

  it('should reject current_setting for config extraction', () => {
    expect(() =>
      validateUserWhereClause("id = 1 OR current_setting('port')::int > 0")
    ).toThrow();
  });

  it('should reject set_config', () => {
    expect(() =>
      validateUserWhereClause("id = 1 OR set_config('log_statement', 'none', false) IS NOT NULL")
    ).toThrow();
  });

  // --- COPY / EXECUTE ---

  it('should reject COPY keyword', () => {
    expect(() =>
      validateUserWhereClause("id = 1 OR COPY users TO '/tmp/dump'")
    ).toThrow();
  });

  it('should reject EXECUTE keyword', () => {
    expect(() =>
      validateUserWhereClause("id = 1 OR EXECUTE 'DROP TABLE users'")
    ).toThrow();
  });

  // --- Legitimate WHERE clauses must still pass ---

  it('should allow simple equality', () => {
    expect(() =>
      validateUserWhereClause("id = 1")
    ).not.toThrow();
  });

  it('should allow comparison operators', () => {
    expect(() =>
      validateUserWhereClause("age > 18 AND status = 'active'")
    ).not.toThrow();
  });

  it('should allow IN with literal values', () => {
    expect(() =>
      validateUserWhereClause("status IN ('active', 'pending')")
    ).not.toThrow();
  });

  it('should allow BETWEEN', () => {
    expect(() =>
      validateUserWhereClause("created_at BETWEEN '2024-01-01' AND '2024-12-31'")
    ).not.toThrow();
  });

  it('should allow IS NULL / IS NOT NULL', () => {
    expect(() =>
      validateUserWhereClause("deleted_at IS NULL AND name IS NOT NULL")
    ).not.toThrow();
  });

  it('should allow LIKE patterns', () => {
    expect(() =>
      validateUserWhereClause("name LIKE '%john%'")
    ).not.toThrow();
  });

  it('should allow ILIKE patterns', () => {
    expect(() =>
      validateUserWhereClause("email ILIKE '%@example.com'")
    ).not.toThrow();
  });

  it('should allow NOT operator', () => {
    expect(() =>
      validateUserWhereClause("NOT deleted AND active = true")
    ).not.toThrow();
  });

  it('should allow OR conditions', () => {
    expect(() =>
      validateUserWhereClause("role = 'admin' OR role = 'moderator'")
    ).not.toThrow();
  });

  it('should allow parenthesized grouping', () => {
    expect(() =>
      validateUserWhereClause("(age > 18 AND status = 'active') OR role = 'admin'")
    ).not.toThrow();
  });

  it('should allow numeric comparisons', () => {
    expect(() =>
      validateUserWhereClause("price >= 10.5 AND quantity < 100")
    ).not.toThrow();
  });

  it('should allow array ANY operator', () => {
    expect(() =>
      validateUserWhereClause("id = ANY('{1,2,3}')")
    ).not.toThrow();
  });
});
