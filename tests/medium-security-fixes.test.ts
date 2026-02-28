import { describe, it, expect } from 'vitest';
import { sanitizeLogValue } from '../src/utils/sanitize.js';
import { sanitizeQuery } from '../src/utils/sanitize.js';

// === Issue #19: Rate limiting disabled by default ===
import { createServerConfig } from '../src/config/environment.js';

describe('rate limiting default (issue #19)', () => {
  it('should enable rate limiting by default when env var is not set', () => {
    const originalEnv = process.env.ENABLE_RATE_LIMIT;
    delete process.env.ENABLE_RATE_LIMIT;
    try {
      const config = createServerConfig({
        mode: 'read-only',
        connectionString: 'postgresql://localhost/test'
      });
      expect(config.enableRateLimit).toBe(true);
    } finally {
      if (originalEnv !== undefined) {
        process.env.ENABLE_RATE_LIMIT = originalEnv;
      }
    }
  });

  it('should allow disabling rate limiting explicitly', () => {
    const originalEnv = process.env.ENABLE_RATE_LIMIT;
    process.env.ENABLE_RATE_LIMIT = 'false';
    try {
      const config = createServerConfig({
        mode: 'read-only',
        connectionString: 'postgresql://localhost/test'
      });
      expect(config.enableRateLimit).toBe(false);
    } finally {
      if (originalEnv !== undefined) {
        process.env.ENABLE_RATE_LIMIT = originalEnv;
      } else {
        delete process.env.ENABLE_RATE_LIMIT;
      }
    }
  });
});

// === Issue #20: No server-side cap on maxRows ===
import { _testClampMaxRows } from '../src/tools/mutations.js';

describe('maxRows server-side cap (issue #20)', () => {
  it('should clamp maxRows to server maximum', () => {
    expect(_testClampMaxRows(999999999)).toBeLessThanOrEqual(10000);
  });

  it('should allow maxRows within limit', () => {
    expect(_testClampMaxRows(500)).toBe(500);
  });

  it('should use default when maxRows is default 1000', () => {
    expect(_testClampMaxRows(1000)).toBe(1000);
  });

  it('should respect MAX_MUTATION_ROWS env var', () => {
    const original = process.env.MAX_MUTATION_ROWS;
    process.env.MAX_MUTATION_ROWS = '5000';
    try {
      expect(_testClampMaxRows(6000)).toBe(5000);
      expect(_testClampMaxRows(3000)).toBe(3000);
    } finally {
      if (original !== undefined) {
        process.env.MAX_MUTATION_ROWS = original;
      } else {
        delete process.env.MAX_MUTATION_ROWS;
      }
    }
  });
});

// === Issue #21: Log injection via unsanitized tool arguments ===
describe('sanitizeLogValue: log injection prevention (issue #21)', () => {
  it('should strip newlines from string values', () => {
    const sanitized = sanitizeLogValue('line1\nline2\nline3');
    expect(sanitized).not.toContain('\n');
  });

  it('should strip carriage returns', () => {
    const sanitized = sanitizeLogValue('line1\r\nline2');
    expect(sanitized).not.toContain('\r');
  });

  it('should strip tab characters', () => {
    const sanitized = sanitizeLogValue('col1\tcol2\tcol3');
    expect(sanitized).not.toContain('\t');
  });

  it('should strip null bytes', () => {
    const sanitized = sanitizeLogValue('text\x00hidden');
    expect(sanitized).not.toContain('\x00');
  });

  it('should handle object values by sanitizing serialized form', () => {
    const sanitized = sanitizeLogValue({ key: 'value\ninjected' });
    expect(sanitized).not.toContain('\n');
  });

  it('should preserve normal text', () => {
    const sanitized = sanitizeLogValue('normal log message');
    expect(sanitized).toBe('normal log message');
  });

  it('should handle empty string', () => {
    const sanitized = sanitizeLogValue('');
    expect(sanitized).toBe('');
  });
});

// === Issue #22: DDL operations unrestricted in read-write mode ===
describe('DDL restrictions in read-write mode (issue #22)', () => {
  it('should reject DROP TABLE in read-write mode', () => {
    expect(() =>
      sanitizeQuery('DROP TABLE users', 'read-write')
    ).toThrow();
  });

  it('should reject TRUNCATE in read-write mode', () => {
    expect(() =>
      sanitizeQuery('TRUNCATE users', 'read-write')
    ).toThrow();
  });

  it('should reject ALTER TABLE in read-write mode', () => {
    expect(() =>
      sanitizeQuery('ALTER TABLE users ADD COLUMN age int', 'read-write')
    ).toThrow();
  });

  it('should reject CREATE TABLE in read-write mode', () => {
    expect(() =>
      sanitizeQuery('CREATE TABLE evil (id int)', 'read-write')
    ).toThrow();
  });

  it('should still allow INSERT in read-write mode', () => {
    expect(() =>
      sanitizeQuery("INSERT INTO users (name) VALUES ('test')", 'read-write')
    ).not.toThrow();
  });

  it('should still allow UPDATE in read-write mode', () => {
    expect(() =>
      sanitizeQuery("UPDATE users SET name = 'test' WHERE id = 1", 'read-write')
    ).not.toThrow();
  });

  it('should still allow DELETE in read-write mode', () => {
    expect(() =>
      sanitizeQuery('DELETE FROM users WHERE id = 1', 'read-write')
    ).not.toThrow();
  });

  it('should still allow SELECT in read-write mode', () => {
    expect(() =>
      sanitizeQuery('SELECT * FROM users', 'read-write')
    ).not.toThrow();
  });
});
