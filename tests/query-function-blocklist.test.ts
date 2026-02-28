import { describe, it, expect } from 'vitest';
import { sanitizeQuery } from '../src/utils/sanitize.js';

describe('sanitizeQuery: dangerous function blocking in queries', () => {
  // --- VULN-001: pg_read_file ---

  it('should reject pg_read_file in SELECT query (read-only)', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_read_file('/etc/passwd', 0, 1000)", 'read-only')
    ).toThrow();
  });

  it('should reject pg_read_file in SELECT query (read-write)', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_read_file('/etc/passwd', 0, 1000)", 'read-write')
    ).toThrow();
  });

  it('should reject pg_read_binary_file', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_read_binary_file('/etc/passwd')", 'read-only')
    ).toThrow();
  });

  // --- VULN-002: pg_ls_dir ---

  it('should reject pg_ls_dir in SELECT query', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_ls_dir('/etc')", 'read-only')
    ).toThrow();
  });

  it('should reject pg_ls_logdir', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_ls_logdir()", 'read-only')
    ).toThrow();
  });

  it('should reject pg_ls_waldir', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_ls_waldir()", 'read-only')
    ).toThrow();
  });

  it('should reject pg_ls_tmpdir', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_ls_tmpdir()", 'read-only')
    ).toThrow();
  });

  it('should reject pg_ls_archive_statusdir', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_ls_archive_statusdir()", 'read-only')
    ).toThrow();
  });

  // --- VULN-004: pg_stat_file ---

  it('should reject pg_stat_file in SELECT query', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_stat_file('/etc/shadow')", 'read-only')
    ).toThrow();
  });

  // --- VULN-006: current_setting / set_config ---

  it('should reject current_setting in SELECT query', () => {
    expect(() =>
      sanitizeQuery("SELECT current_setting('data_directory')", 'read-only')
    ).toThrow();
  });

  it('should reject set_config in SELECT query', () => {
    expect(() =>
      sanitizeQuery("SELECT set_config('log_statement', 'none', false)", 'read-only')
    ).toThrow();
  });

  // --- Other dangerous functions ---

  it('should reject pg_sleep in SELECT query', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_sleep(10)", 'read-only')
    ).toThrow();
  });

  it('should reject lo_import in SELECT query', () => {
    expect(() =>
      sanitizeQuery("SELECT lo_import('/etc/passwd')", 'read-only')
    ).toThrow();
  });

  it('should reject lo_export in SELECT query', () => {
    expect(() =>
      sanitizeQuery("SELECT lo_export(1234, '/tmp/out')", 'read-only')
    ).toThrow();
  });

  it('should reject dblink in SELECT query', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM dblink('host=evil', 'SELECT 1')", 'read-only')
    ).toThrow();
  });

  // --- Case insensitivity ---

  it('should reject case-varied pg_read_file', () => {
    expect(() =>
      sanitizeQuery("SELECT PG_READ_FILE('/etc/passwd')", 'read-only')
    ).toThrow();
  });

  it('should reject mixed-case Pg_Ls_Dir', () => {
    expect(() =>
      sanitizeQuery("SELECT Pg_Ls_Dir('/tmp')", 'read-only')
    ).toThrow();
  });

  // --- Embedded in expressions ---

  it('should reject dangerous function in column expression', () => {
    expect(() =>
      sanitizeQuery("SELECT 1, pg_read_file('/etc/passwd') AS content", 'read-only')
    ).toThrow();
  });

  it('should reject dangerous function in CASE expression', () => {
    expect(() =>
      sanitizeQuery("SELECT CASE WHEN true THEN pg_read_file('/etc/passwd') END", 'read-only')
    ).toThrow();
  });

  // --- Legitimate queries must still pass ---

  it('should allow normal SELECT', () => {
    expect(() =>
      sanitizeQuery("SELECT id, name FROM users", 'read-only')
    ).not.toThrow();
  });

  it('should allow SELECT with WHERE', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM orders WHERE status = 'active'", 'read-only')
    ).not.toThrow();
  });

  it('should allow SELECT with aggregation', () => {
    expect(() =>
      sanitizeQuery("SELECT COUNT(*), AVG(price) FROM products", 'read-only')
    ).not.toThrow();
  });

  it('should allow SELECT with JOIN', () => {
    expect(() =>
      sanitizeQuery("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id", 'read-only')
    ).not.toThrow();
  });

  it('should allow EXPLAIN', () => {
    expect(() =>
      sanitizeQuery("EXPLAIN SELECT * FROM users", 'read-only')
    ).not.toThrow();
  });

  it('should allow CTE with SELECT', () => {
    expect(() =>
      sanitizeQuery("WITH cte AS (SELECT 1) SELECT * FROM cte", 'read-only')
    ).not.toThrow();
  });

  it('should allow safe built-in functions like NOW(), COUNT(), COALESCE()', () => {
    expect(() =>
      sanitizeQuery("SELECT NOW(), COUNT(*), COALESCE(name, 'unknown') FROM users", 'read-only')
    ).not.toThrow();
  });

  it('should allow columns that contain blocked substrings (e.g. "current_settings_count")', () => {
    expect(() =>
      sanitizeQuery("SELECT current_settings_count FROM dashboard", 'read-only')
    ).not.toThrow();
  });
});

describe('sanitizeQuery: sensitive system catalog blocking', () => {
  // --- VULN-003: pg_shadow ---

  it('should reject SELECT from pg_shadow', () => {
    expect(() =>
      sanitizeQuery("SELECT usename, passwd FROM pg_shadow", 'read-only')
    ).toThrow();
  });

  // --- VULN-005: pg_authid ---

  it('should reject SELECT from pg_authid', () => {
    expect(() =>
      sanitizeQuery("SELECT rolname, rolpassword FROM pg_authid", 'read-only')
    ).toThrow();
  });

  it('should reject SELECT from pg_auth_members', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_auth_members", 'read-only')
    ).toThrow();
  });

  // --- pg_hba_file_rules / pg_file_settings ---

  it('should reject SELECT from pg_hba_file_rules', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_hba_file_rules", 'read-only')
    ).toThrow();
  });

  it('should reject SELECT from pg_file_settings', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_file_settings", 'read-only')
    ).toThrow();
  });

  // --- Case insensitivity ---

  it('should reject case-varied PG_SHADOW', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM PG_SHADOW", 'read-only')
    ).toThrow();
  });

  // --- Schema-qualified access ---

  it('should reject pg_catalog.pg_shadow', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_catalog.pg_shadow", 'read-only')
    ).toThrow();
  });

  it('should reject pg_catalog.pg_authid', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_catalog.pg_authid", 'read-only')
    ).toThrow();
  });

  // --- In CTE context ---

  it('should reject pg_shadow in CTE', () => {
    expect(() =>
      sanitizeQuery("WITH creds AS (SELECT * FROM pg_shadow) SELECT * FROM creds", 'read-only')
    ).toThrow();
  });

  // --- Legitimate catalog access must still pass ---

  it('should allow pg_tables', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_tables WHERE schemaname = 'public'", 'read-only')
    ).not.toThrow();
  });

  it('should allow pg_stat_activity', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_stat_activity", 'read-only')
    ).not.toThrow();
  });

  it('should allow information_schema', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM information_schema.columns", 'read-only')
    ).not.toThrow();
  });

  it('should allow pg_settings (non-sensitive)', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_settings", 'read-only')
    ).not.toThrow();
  });

  it('should allow tables with "shadow" in the name that are not pg_shadow', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM user_shadow_copies", 'read-only')
    ).not.toThrow();
  });
});
