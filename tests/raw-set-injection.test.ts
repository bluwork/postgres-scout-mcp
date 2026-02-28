import { describe, it, expect } from 'vitest';
import { validateRawSetClause } from '../src/utils/sanitize.js';

describe('validateRawSetClause: injection prevention', () => {
  // --- Subquery injection ---

  it('should reject subquery in SET value', () => {
    expect(() =>
      validateRawSetClause("name = (SELECT password FROM users LIMIT 1)")
    ).toThrow();
  });

  it('should reject EXISTS subquery in SET', () => {
    expect(() =>
      validateRawSetClause("active = EXISTS (SELECT 1 FROM admin_users)")
    ).toThrow();
  });

  // --- Dangerous function injection ---

  it('should reject pg_sleep in SET', () => {
    expect(() =>
      validateRawSetClause("name = pg_sleep(5)")
    ).toThrow();
  });

  it('should reject pg_read_file in SET', () => {
    expect(() =>
      validateRawSetClause("name = pg_read_file('/etc/passwd')")
    ).toThrow();
  });

  it('should reject dblink in SET', () => {
    expect(() =>
      validateRawSetClause("name = dblink('host=evil', 'SELECT 1')")
    ).toThrow();
  });

  it('should reject set_config in SET', () => {
    expect(() =>
      validateRawSetClause("name = set_config('log_statement', 'none', false)")
    ).toThrow();
  });

  // --- Comment / multi-statement injection ---

  it('should reject SQL comment in SET', () => {
    expect(() =>
      validateRawSetClause("name = 'x' -- rest is comment")
    ).toThrow();
  });

  it('should reject block comment in SET', () => {
    expect(() =>
      validateRawSetClause("name = 'x' /* comment */")
    ).toThrow();
  });

  it('should reject semicolon multi-statement in SET', () => {
    expect(() =>
      validateRawSetClause("name = 'x'; DROP TABLE users")
    ).toThrow();
  });

  // --- UNION injection ---

  it('should reject UNION SELECT in SET', () => {
    expect(() =>
      validateRawSetClause("name = 'x' UNION SELECT password FROM users")
    ).toThrow();
  });

  // --- Empty/whitespace ---

  it('should reject empty SET clause', () => {
    expect(() =>
      validateRawSetClause('')
    ).toThrow();
  });

  it('should reject whitespace-only SET clause', () => {
    expect(() =>
      validateRawSetClause('   ')
    ).toThrow();
  });

  // --- Unbalanced quotes/parens ---

  it('should reject unbalanced parentheses', () => {
    expect(() =>
      validateRawSetClause("name = (CASE WHEN 1=1 THEN 'x'")
    ).toThrow();
  });

  it('should reject unbalanced quotes', () => {
    expect(() =>
      validateRawSetClause("name = 'unfinished")
    ).toThrow();
  });

  // --- Legitimate SET clauses must pass ---

  it('should allow simple column = string value', () => {
    expect(() =>
      validateRawSetClause("name = 'John'")
    ).not.toThrow();
  });

  it('should allow column = number', () => {
    expect(() =>
      validateRawSetClause("age = 30")
    ).not.toThrow();
  });

  it('should allow multiple assignments', () => {
    expect(() =>
      validateRawSetClause("name = 'John', age = 30, active = true")
    ).not.toThrow();
  });

  it('should allow NULL assignment', () => {
    expect(() =>
      validateRawSetClause("deleted_at = NULL")
    ).not.toThrow();
  });

  it('should allow column = column arithmetic', () => {
    expect(() =>
      validateRawSetClause("counter = counter + 1")
    ).not.toThrow();
  });

  it('should allow NOW() function', () => {
    expect(() =>
      validateRawSetClause("updated_at = NOW()")
    ).not.toThrow();
  });

  it('should allow COALESCE', () => {
    expect(() =>
      validateRawSetClause("name = COALESCE(name, 'default')")
    ).not.toThrow();
  });
});
