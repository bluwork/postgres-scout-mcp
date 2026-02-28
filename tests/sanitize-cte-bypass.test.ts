import { describe, it, expect } from 'vitest';
import { sanitizeQuery } from '../src/utils/sanitize.js';

describe('sanitizeQuery: CTE with data-modifying main statement in read-only mode', () => {
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
});
