import { describe, it, expect } from 'vitest';
import { sanitizeQuery } from '../src/utils/sanitize.js';

describe('sanitizeQuery: CTE with data-modifying main statement in read-only mode', () => {
  // --- Rejection cases: CTE + data-modifying main statement ---

  it('should reject CTE followed by DELETE in read-only mode', () => {
    expect(() =>
      sanitizeQuery('WITH cte AS (SELECT 1) DELETE FROM users WHERE id > 0', 'read-only')
    ).toThrow();
  });

  it('should reject CTE followed by UPDATE in read-only mode', () => {
    expect(() =>
      sanitizeQuery('WITH cte AS (SELECT 1) UPDATE users SET name = \'x\'', 'read-only')
    ).toThrow();
  });

  it('should reject CTE followed by INSERT in read-only mode', () => {
    expect(() =>
      sanitizeQuery('WITH cte AS (SELECT 1) INSERT INTO users (name) VALUES (\'x\')', 'read-only')
    ).toThrow();
  });

  it('should reject CTE followed by DROP in read-only mode', () => {
    expect(() =>
      sanitizeQuery('WITH cte AS (SELECT 1) DROP TABLE users', 'read-only')
    ).toThrow();
  });

  it('should reject CTE followed by TRUNCATE in read-only mode', () => {
    expect(() =>
      sanitizeQuery('WITH cte AS (SELECT 1) TRUNCATE users', 'read-only')
    ).toThrow();
  });

  it('should reject multiple CTEs followed by DELETE in read-only mode', () => {
    expect(() =>
      sanitizeQuery(
        'WITH a AS (SELECT 1), b AS (SELECT 2) DELETE FROM users',
        'read-only'
      )
    ).toThrow();
  });

  it('should reject CTE with extra whitespace/newlines before DELETE in read-only mode', () => {
    expect(() =>
      sanitizeQuery(
        'WITH cte AS (SELECT 1)\n  DELETE FROM users WHERE id = 1',
        'read-only'
      )
    ).toThrow();
  });

  // --- Allow cases: legitimate read-only CTE usage ---

  it('should still allow CTE followed by SELECT in read-only mode', () => {
    expect(() =>
      sanitizeQuery('WITH cte AS (SELECT 1) SELECT * FROM cte', 'read-only')
    ).not.toThrow();
  });

  it('should still allow simple SELECT in read-only mode', () => {
    expect(() =>
      sanitizeQuery('SELECT * FROM users', 'read-only')
    ).not.toThrow();
  });

  it('should allow CTE with DELETE in read-write mode', () => {
    expect(() =>
      sanitizeQuery('WITH cte AS (SELECT 1) DELETE FROM users WHERE id = 1', 'read-write')
    ).not.toThrow();
  });

  // --- CTE column list syntax (review comment #6) ---

  it('should allow CTE with column list followed by SELECT in read-only mode', () => {
    expect(() =>
      sanitizeQuery('WITH cte(id, name) AS (SELECT 1, \'a\') SELECT * FROM cte', 'read-only')
    ).not.toThrow();
  });

  it('should reject CTE with column list followed by DELETE in read-only mode', () => {
    expect(() =>
      sanitizeQuery('WITH cte(id) AS (SELECT 1) DELETE FROM users', 'read-only')
    ).toThrow();
  });

  it('should allow WITH RECURSIVE with column list in read-only mode', () => {
    // Note: real RECURSIVE CTEs use UNION ALL SELECT, which is blocked by
    // the pre-existing DANGEROUS_PATTERNS regex. This test uses a simplified
    // body to verify the parser handles WITH RECURSIVE + column list syntax.
    expect(() =>
      sanitizeQuery(
        'WITH RECURSIVE cte(n) AS (SELECT 1 FROM items) SELECT * FROM cte',
        'read-only'
      )
    ).not.toThrow();
  });

  it('should reject WITH RECURSIVE with column list followed by DELETE in read-only mode', () => {
    expect(() =>
      sanitizeQuery(
        'WITH RECURSIVE cte(n) AS (SELECT 1 FROM items) DELETE FROM users',
        'read-only'
      )
    ).toThrow();
  });

  // --- Dollar-quoted strings (review comment #7) ---

  it('should allow CTE with dollar-quoted string followed by SELECT in read-only mode', () => {
    expect(() =>
      sanitizeQuery(
        "WITH cte AS (SELECT $$ some ) text ( $$ AS val) SELECT * FROM cte",
        'read-only'
      )
    ).not.toThrow();
  });

  it('should allow CTE with tagged dollar-quoted string followed by SELECT in read-only mode', () => {
    expect(() =>
      sanitizeQuery(
        "WITH cte AS (SELECT $tag$ some ) text ( $tag$ AS val) SELECT * FROM cte",
        'read-only'
      )
    ).not.toThrow();
  });

  it('should reject CTE with dollar-quoted string followed by DELETE in read-only mode', () => {
    expect(() =>
      sanitizeQuery(
        "WITH cte AS (SELECT $$ ) $$ AS val) DELETE FROM users",
        'read-only'
      )
    ).toThrow();
  });

  // --- Fail-closed behavior (review comment #3) ---

  it('should reject malformed CTE that cannot be parsed in read-only mode', () => {
    expect(() =>
      sanitizeQuery('WITH AS (SELECT 1) SELECT * FROM cte', 'read-only')
    ).toThrow();
  });

  // --- MATERIALIZED / NOT MATERIALIZED ---

  it('should allow CTE with MATERIALIZED followed by SELECT in read-only mode', () => {
    expect(() =>
      sanitizeQuery(
        'WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte',
        'read-only'
      )
    ).not.toThrow();
  });

  it('should allow CTE with NOT MATERIALIZED followed by SELECT in read-only mode', () => {
    expect(() =>
      sanitizeQuery(
        'WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte',
        'read-only'
      )
    ).not.toThrow();
  });

  it('should reject CTE with MATERIALIZED followed by DELETE in read-only mode', () => {
    expect(() =>
      sanitizeQuery(
        'WITH cte AS MATERIALIZED (SELECT 1) DELETE FROM users',
        'read-only'
      )
    ).toThrow();
  });

  // --- E-string escape literals (review round 2, comment #1) ---

  it('should allow CTE with E-string containing backslash-escaped quote followed by SELECT', () => {
    expect(() =>
      sanitizeQuery(
        "WITH cte AS (SELECT E'it\\'s ) here' AS val) SELECT * FROM cte",
        'read-only'
      )
    ).not.toThrow();
  });

  it('should reject CTE with E-string containing backslash-escaped quote followed by DELETE', () => {
    expect(() =>
      sanitizeQuery(
        "WITH cte AS (SELECT E'\\') fake' AS val) DELETE FROM users",
        'read-only'
      )
    ).toThrow();
  });

  // --- Double-quoted identifiers (review round 2, comments #2/#3) ---

  it('should allow CTE with double-quoted identifier containing parens followed by SELECT', () => {
    expect(() =>
      sanitizeQuery(
        'WITH cte AS (SELECT 1 AS ") SELECT (") SELECT * FROM cte',
        'read-only'
      )
    ).not.toThrow();
  });

  it('should reject CTE with double-quoted identifier containing paren followed by DELETE', () => {
    expect(() =>
      sanitizeQuery(
        'WITH cte AS (SELECT 1 AS ") SELECT ") DELETE FROM users',
        'read-only'
      )
    ).toThrow();
  });
});
