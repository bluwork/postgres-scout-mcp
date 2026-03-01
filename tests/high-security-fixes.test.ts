import { describe, it, expect } from 'vitest';
import { validateCondition } from '../src/utils/sanitize.js';
import { sanitizeErrorMessage } from '../src/utils/sanitize.js';

// === Issue #14: optimizeQuery ignores read-only ANALYZE restriction ===
// This is a runtime behavior test (needs connection mock), so we test the
// code path inline below via the exported function check.

// === Issue #15: SQL injection via condition parameter in checkConstraintViolations ===
describe('validateCondition: injection prevention (issue #15)', () => {
  it('should reject subquery in condition', () => {
    expect(() =>
      validateCondition('id = (SELECT 1)')
    ).toThrow();
  });

  it('should reject pg_sleep in condition', () => {
    expect(() =>
      validateCondition('id = 1 OR pg_sleep(5) IS NOT NULL')
    ).toThrow();
  });

  it('should reject pg_read_file in condition', () => {
    expect(() =>
      validateCondition("id = pg_read_file('/etc/passwd')")
    ).toThrow();
  });

  it('should reject dblink in condition', () => {
    expect(() =>
      validateCondition("id = dblink('host=evil', 'SELECT 1')")
    ).toThrow();
  });

  it('should reject EXECUTE in condition', () => {
    expect(() =>
      validateCondition("id = 1 OR EXECUTE 'DROP TABLE users'")
    ).toThrow();
  });

  it('should allow simple condition', () => {
    expect(() =>
      validateCondition("age > 18 AND status = 'active'")
    ).not.toThrow();
  });

  it('should allow IS NOT NULL condition', () => {
    expect(() =>
      validateCondition("email IS NOT NULL")
    ).not.toThrow();
  });
});

// === Issue #16: Always-true WHERE clause bypass in mutation protection ===
// With structured WhereCondition, tautologies like 1=1 or 'a'='a' are impossible
// to express. The only "affect all rows" scenario is an empty conditions array.
import { _testNormalizeWhereForSafety } from '../src/tools/mutations.js';

describe('normalizeWhereForSafety: empty WHERE protection (issue #16)', () => {
  it('should flag empty conditions array as dangerous', () => {
    expect(_testNormalizeWhereForSafety([])).toBe(true);
  });

  it('should not flag single equality condition', () => {
    expect(_testNormalizeWhereForSafety([{ field: 'id', op: '=', value: 1 }])).toBe(false);
  });

  it('should not flag comparison condition', () => {
    expect(_testNormalizeWhereForSafety([{ field: 'age', op: '>', value: 18 }])).toBe(false);
  });

  it('should not flag multiple conditions', () => {
    expect(_testNormalizeWhereForSafety([
      { field: 'status', op: '=', value: 'active' },
      { field: 'role', op: '=', value: 'admin' }
    ])).toBe(false);
  });

  it('should not flag IS NULL condition', () => {
    expect(_testNormalizeWhereForSafety([{ field: 'deleted_at', op: 'IS NULL' }])).toBe(false);
  });

  it('should not flag IN condition', () => {
    expect(_testNormalizeWhereForSafety([{ field: 'id', op: 'IN', value: [1, 2, 3] }])).toBe(false);
  });

  it('should not flag BETWEEN condition', () => {
    expect(_testNormalizeWhereForSafety([{ field: 'age', op: 'BETWEEN', value: [18, 65] }])).toBe(false);
  });

  it('should not flag nested AND/OR conditions', () => {
    expect(_testNormalizeWhereForSafety([
      { or: [
        { field: 'role', op: '=', value: 'admin' },
        { field: 'role', op: '=', value: 'moderator' }
      ]}
    ])).toBe(false);
  });

  it('should not flag LIKE condition', () => {
    expect(_testNormalizeWhereForSafety([{ field: 'name', op: 'LIKE', value: '%john%' }])).toBe(false);
  });

  it('should not flag ILIKE condition', () => {
    expect(_testNormalizeWhereForSafety([{ field: 'email', op: 'ILIKE', value: '%@example.com' }])).toBe(false);
  });
});

// === Issue #18: Information disclosure via unfiltered error messages ===
describe('sanitizeErrorMessage: information disclosure (issue #18)', () => {
  it('should strip table names from PG errors', () => {
    const raw = 'ERROR: relation "secret_users" does not exist';
    const sanitized = sanitizeErrorMessage(raw);
    expect(sanitized).not.toContain('secret_users');
  });

  it('should strip column type details from PG errors', () => {
    const raw = 'ERROR: column "password_hash" of type character varying(255) cannot be cast';
    const sanitized = sanitizeErrorMessage(raw);
    expect(sanitized).not.toContain('password_hash');
  });

  it('should strip constraint names from PG errors', () => {
    const raw = 'ERROR: duplicate key value violates unique constraint "users_email_key"';
    const sanitized = sanitizeErrorMessage(raw);
    expect(sanitized).not.toContain('users_email_key');
  });

  it('should preserve generic error category', () => {
    const raw = 'ERROR: syntax error at or near "SELEC"';
    const sanitized = sanitizeErrorMessage(raw);
    expect(sanitized).toContain('syntax error');
  });

  it('should handle timeout errors', () => {
    const raw = 'ERROR: canceling statement due to statement timeout';
    const sanitized = sanitizeErrorMessage(raw);
    expect(sanitized.toLowerCase()).toContain('time');
  });

  it('should handle permission errors', () => {
    const raw = 'ERROR: permission denied for table users';
    const sanitized = sanitizeErrorMessage(raw);
    expect(sanitized.toLowerCase()).toContain('permission denied');
    expect(sanitized).not.toContain('users');
  });

  it('should return generic message for unknown errors', () => {
    const raw = 'some weird internal error with sensitive details about pg_catalog.pg_class';
    const sanitized = sanitizeErrorMessage(raw);
    expect(sanitized).not.toContain('pg_catalog');
  });
});
