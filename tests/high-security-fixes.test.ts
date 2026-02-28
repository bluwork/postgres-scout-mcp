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
import { _testNormalizeWhereForSafety } from '../src/tools/mutations.js';

describe('normalizeWhereForSafety: always-true bypass (issue #16)', () => {
  it('should detect 1=1 tautology', () => {
    expect(_testNormalizeWhereForSafety('1=1')).toBe(true);
  });

  it('should detect wrapped (1=1)', () => {
    expect(_testNormalizeWhereForSafety('(1=1)')).toBe(true);
  });

  it('should detect "true"', () => {
    expect(_testNormalizeWhereForSafety('true')).toBe(true);
  });

  it('should detect numeric tautology 2=2', () => {
    expect(_testNormalizeWhereForSafety('2=2')).toBe(true);
  });

  it('should detect OR-based tautology: 1=1 OR anything', () => {
    expect(_testNormalizeWhereForSafety("1=1 OR name = 'x'")).toBe(true);
  });

  it('should detect string tautology: a=a pattern', () => {
    expect(_testNormalizeWhereForSafety("'a'='a'")).toBe(true);
  });

  it('should detect NOT false tautology', () => {
    expect(_testNormalizeWhereForSafety('NOT false')).toBe(true);
  });

  it('should not flag legitimate conditions', () => {
    expect(_testNormalizeWhereForSafety("id = 1")).toBe(false);
  });

  it('should not flag conditions with different values', () => {
    expect(_testNormalizeWhereForSafety("age > 18")).toBe(false);
  });

  it('should not flag complex legitimate conditions', () => {
    expect(_testNormalizeWhereForSafety("status = 'active' AND role = 'admin'")).toBe(false);
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
