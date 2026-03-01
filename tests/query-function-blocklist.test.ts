import { describe, it, expect } from 'vitest';
import { sanitizeQuery, assertNoSensitiveCatalogAccess } from '../src/utils/sanitize.js';

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

describe('sanitizeQuery: XML function bypass blocking (VULN-007 through VULN-009)', () => {
  // --- VULN-007: query_to_xml ---

  it('should reject query_to_xml', () => {
    expect(() =>
      sanitizeQuery("SELECT query_to_xml('SELECT 1', true, true, '')", 'read-only')
    ).toThrow();
  });

  it('should reject query_to_xml with concat bypass', () => {
    expect(() =>
      sanitizeQuery("SELECT query_to_xml(concat('SEL','ECT 1'), true, true, '')", 'read-only')
    ).toThrow();
  });

  it('should reject query_to_xml_and_xmlschema', () => {
    expect(() =>
      sanitizeQuery("SELECT query_to_xml_and_xmlschema('SELECT 1', true, true, '')", 'read-only')
    ).toThrow();
  });

  // --- VULN-008: table_to_xml ---

  it('should reject table_to_xml', () => {
    expect(() =>
      sanitizeQuery("SELECT table_to_xml('users', true, true, '')", 'read-only')
    ).toThrow();
  });

  it('should reject table_to_xml_and_xmlschema', () => {
    expect(() =>
      sanitizeQuery("SELECT table_to_xml_and_xmlschema('users', true, true, '')", 'read-only')
    ).toThrow();
  });

  // --- VULN-009: schema/database_to_xml ---

  it('should reject schema_to_xml', () => {
    expect(() =>
      sanitizeQuery("SELECT schema_to_xml('public', true, true, '')", 'read-only')
    ).toThrow();
  });

  it('should reject schema_to_xml_and_xmlschema', () => {
    expect(() =>
      sanitizeQuery("SELECT schema_to_xml_and_xmlschema('public', true, true, '')", 'read-only')
    ).toThrow();
  });

  it('should reject database_to_xml', () => {
    expect(() =>
      sanitizeQuery("SELECT database_to_xml(true, true, '')", 'read-only')
    ).toThrow();
  });

  it('should reject database_to_xml_and_xmlschema', () => {
    expect(() =>
      sanitizeQuery("SELECT database_to_xml_and_xmlschema(true, true, '')", 'read-only')
    ).toThrow();
  });

  it('should reject cursor_to_xml', () => {
    expect(() =>
      sanitizeQuery("SELECT cursor_to_xml('my_cursor', 10, false, true, '')", 'read-only')
    ).toThrow();
  });

  // --- Case insensitivity ---

  it('should reject case-varied QUERY_TO_XML', () => {
    expect(() =>
      sanitizeQuery("SELECT QUERY_TO_XML('SELECT 1', true, true, '')", 'read-only')
    ).toThrow();
  });

  // --- Also blocked in read-write mode ---

  it('should reject query_to_xml in read-write mode', () => {
    expect(() =>
      sanitizeQuery("SELECT query_to_xml('SELECT 1', true, true, '')", 'read-write')
    ).toThrow();
  });

  // --- Legitimate XML functions must still pass ---

  it('should allow xmlelement', () => {
    expect(() =>
      sanitizeQuery("SELECT xmlelement(name foo, 'bar')", 'read-only')
    ).not.toThrow();
  });

  it('should allow xmlforest', () => {
    expect(() =>
      sanitizeQuery("SELECT xmlforest(name, age) FROM users", 'read-only')
    ).not.toThrow();
  });

  it('should allow xpath', () => {
    expect(() =>
      sanitizeQuery("SELECT xpath('/root/text()', '<root>hello</root>'::xml)", 'read-only')
    ).not.toThrow();
  });
});

describe('sanitizeQuery: process control and resource abuse blocking (VULN-010 through VULN-012)', () => {
  // --- VULN-010: pg_terminate_backend / pg_cancel_backend ---

  it('should reject pg_terminate_backend', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_terminate_backend(12345)", 'read-only')
    ).toThrow();
  });

  it('should reject pg_cancel_backend', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_cancel_backend(12345)", 'read-only')
    ).toThrow();
  });

  it('should reject pg_reload_conf', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_reload_conf()", 'read-only')
    ).toThrow();
  });

  it('should reject pg_rotate_logfile', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_rotate_logfile()", 'read-only')
    ).toThrow();
  });

  // --- VULN-011: advisory locks ---

  it('should reject pg_advisory_lock', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_advisory_lock(12345)", 'read-only')
    ).toThrow();
  });

  it('should reject pg_advisory_lock_shared', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_advisory_lock_shared(12345)", 'read-only')
    ).toThrow();
  });

  it('should reject pg_try_advisory_lock', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_try_advisory_lock(12345)", 'read-only')
    ).toThrow();
  });

  it('should reject pg_try_advisory_lock_shared', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_try_advisory_lock_shared(12345)", 'read-only')
    ).toThrow();
  });

  it('should reject pg_advisory_xact_lock', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_advisory_xact_lock(12345)", 'read-only')
    ).toThrow();
  });

  it('should reject pg_advisory_xact_lock_shared', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_advisory_xact_lock_shared(12345)", 'read-only')
    ).toThrow();
  });

  // --- VULN-012: pg_notify ---

  it('should reject pg_notify', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_notify('channel', 'payload')", 'read-only')
    ).toThrow();
  });

  // --- Process control blocked in read-write mode too ---

  it('should reject pg_terminate_backend in read-write mode', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_terminate_backend(12345)", 'read-write')
    ).toThrow();
  });

  // --- Legitimate functions must still pass ---

  it('should reject pg_backend_pid (R3-015)', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_backend_pid()", 'read-only')
    ).toThrow();
  });

  it('should reject pg_stat_get_activity with pg_backend_pid', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_stat_get_activity(pg_backend_pid())", 'read-only')
    ).toThrow();
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

  // --- Additional sensitive catalogs (VULN-015, VULN-018) ---

  it('should reject pg_roles', () => {
    expect(() =>
      sanitizeQuery("SELECT rolname, rolsuper FROM pg_roles", 'read-only')
    ).toThrow();
  });

  it('should reject pg_stat_ssl', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_stat_ssl", 'read-only')
    ).toThrow();
  });

  it('should reject pg_largeobject', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_largeobject", 'read-only')
    ).toThrow();
  });

  it('should reject pg_largeobject_metadata', () => {
    expect(() =>
      sanitizeQuery("SELECT * FROM pg_largeobject_metadata", 'read-only')
    ).toThrow();
  });
});

describe('sanitizeQuery: round 3 — large object, network, metadata, DoS function blocking', () => {
  // Large object API (R3-004)
  const loFunctions = [
    'lo_creat(-1)',
    'lo_create(0)',
    'lo_open(12345, 262144)',
    'lo_close(0)',
    'lo_get(32771)',
    'lo_put(32771, 0, E\'\\\\x48\')',
    "lo_from_bytea(0, E'\\\\x48656c6c6f')",
    'lo_truncate(0, 100)',
    'lo_unlink(32771)',
    'loread(0, 100)',
    'lowrite(0, E\'\\\\x48\')',
  ];

  for (const fn of loFunctions) {
    it(`should reject ${fn.split('(')[0]}`, () => {
      expect(() =>
        sanitizeQuery(`SELECT ${fn}`, 'read-only')
      ).toThrow();
    });
  }

  // Network topology (R3-010)
  const networkFunctions = [
    'inet_server_addr()',
    'inet_server_port()',
    'inet_client_addr()',
    'inet_client_port()',
  ];

  for (const fn of networkFunctions) {
    it(`should reject ${fn.split('(')[0]}`, () => {
      expect(() =>
        sanitizeQuery(`SELECT ${fn}`, 'read-only')
      ).toThrow();
    });
  }

  // Server metadata (R3-012, R3-015)
  const metadataFunctions = [
    'pg_export_snapshot()',
    'pg_current_logfile()',
    'pg_postmaster_start_time()',
    'pg_conf_load_time()',
    'pg_backend_pid()',
    'pg_tablespace_location(1663)',
  ];

  for (const fn of metadataFunctions) {
    it(`should reject ${fn.split('(')[0]}`, () => {
      expect(() =>
        sanitizeQuery(`SELECT ${fn}`, 'read-only')
      ).toThrow();
    });
  }

  // DoS vectors (R3-016, R3-017)
  it('should reject generate_series', () => {
    expect(() =>
      sanitizeQuery('SELECT * FROM generate_series(1, 1000000)', 'read-only')
    ).toThrow();
  });

  it('should reject repeat', () => {
    expect(() =>
      sanitizeQuery("SELECT repeat('A', 100000000)", 'read-only')
    ).toThrow();
  });

  // Ensure existing allowed functions still work
  it('should still allow NOW()', () => {
    expect(() =>
      sanitizeQuery('SELECT NOW()', 'read-only')
    ).not.toThrow();
  });

  it('should still allow COUNT(*)', () => {
    expect(() =>
      sanitizeQuery('SELECT COUNT(*) FROM users', 'read-only')
    ).not.toThrow();
  });
});

describe('assertNoSensitiveCatalogAccess: user query catalog blocking (R3-001/002/007/008/009/011/013/014)', () => {
  const blockedCatalogs = [
    'pg_settings',
    'pg_stat_activity',
    'pg_stat_replication',
    'pg_stat_gssapi',
    'pg_ident_file_mappings',
    'pg_proc',
    'pg_database',
    'pg_tablespace',
    'pg_prepared_statements',
  ];

  for (const catalog of blockedCatalogs) {
    it(`should reject SELECT from ${catalog}`, () => {
      expect(() =>
        assertNoSensitiveCatalogAccess(`SELECT * FROM ${catalog}`)
      ).toThrow();
    });

    it(`should reject ${catalog} with schema prefix`, () => {
      expect(() =>
        assertNoSensitiveCatalogAccess(`SELECT * FROM pg_catalog.${catalog}`)
      ).toThrow();
    });

    it(`should reject ${catalog} case-insensitive`, () => {
      expect(() =>
        assertNoSensitiveCatalogAccess(`SELECT * FROM ${catalog.toUpperCase()}`)
      ).toThrow();
    });
  }

  // information_schema privilege views
  const blockedInfoSchemaViews = [
    'information_schema.enabled_roles',
    'information_schema.role_table_grants',
    'information_schema.applicable_roles',
    'information_schema.role_routine_grants',
  ];

  for (const view of blockedInfoSchemaViews) {
    it(`should reject SELECT from ${view}`, () => {
      expect(() =>
        assertNoSensitiveCatalogAccess(`SELECT * FROM ${view}`)
      ).toThrow();
    });
  }

  // Must NOT block safe catalogs/views used by internal tools
  it('should allow pg_tables', () => {
    expect(() =>
      assertNoSensitiveCatalogAccess("SELECT * FROM pg_tables WHERE schemaname = 'public'")
    ).not.toThrow();
  });

  it('should allow information_schema.columns', () => {
    expect(() =>
      assertNoSensitiveCatalogAccess('SELECT * FROM information_schema.columns')
    ).not.toThrow();
  });

  it('should allow pg_indexes', () => {
    expect(() =>
      assertNoSensitiveCatalogAccess('SELECT * FROM pg_indexes')
    ).not.toThrow();
  });

  it('should allow regular user tables', () => {
    expect(() =>
      assertNoSensitiveCatalogAccess('SELECT * FROM users')
    ).not.toThrow();
  });

  it('should allow tables with catalog-like substrings', () => {
    expect(() =>
      assertNoSensitiveCatalogAccess('SELECT * FROM user_settings')
    ).not.toThrow();
  });
});

describe('sanitizeQuery: round 4 — stats reset, sequence manipulation, WAL ops', () => {
  // R4-001: pg_stat_reset family
  const statResetFunctions = [
    'pg_stat_reset()',
    "pg_stat_reset_shared('bgwriter')",
    'pg_stat_reset_single_table_counters(12345)',
    "pg_stat_reset_slru('CommitTs')",
    "pg_stat_reset_replication_slot('my_slot')",
  ];

  for (const fn of statResetFunctions) {
    it(`should reject ${fn.split('(')[0]}`, () => {
      expect(() =>
        sanitizeQuery(`SELECT ${fn}`, 'read-only')
      ).toThrow();
    });
  }

  // R4-002: sequence manipulation
  it('should reject setval', () => {
    expect(() =>
      sanitizeQuery("SELECT setval('my_seq', 1000)", 'read-only')
    ).toThrow();
  });

  it('should reject nextval', () => {
    expect(() =>
      sanitizeQuery("SELECT nextval('my_seq')", 'read-only')
    ).toThrow();
  });

  // R4-003: WAL / restore-point / logical message
  it('should reject pg_switch_wal', () => {
    expect(() =>
      sanitizeQuery('SELECT pg_switch_wal()', 'read-only')
    ).toThrow();
  });

  it('should reject pg_create_restore_point', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_create_restore_point('before_deploy')", 'read-only')
    ).toThrow();
  });

  it('should reject pg_logical_emit_message', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_logical_emit_message(true, 'prefix', 'msg')", 'read-only')
    ).toThrow();
  });

  // Case insensitivity
  it('should reject PG_STAT_RESET (upper case)', () => {
    expect(() =>
      sanitizeQuery('SELECT PG_STAT_RESET()', 'read-only')
    ).toThrow();
  });

  it('should reject SETVAL (upper case)', () => {
    expect(() =>
      sanitizeQuery("SELECT SETVAL('my_seq', 1)", 'read-only')
    ).toThrow();
  });

  // Blocked in read-write mode too
  it('should reject setval in read-write mode', () => {
    expect(() =>
      sanitizeQuery("SELECT setval('my_seq', 1)", 'read-write')
    ).toThrow();
  });

  it('should reject pg_switch_wal in read-write mode', () => {
    expect(() =>
      sanitizeQuery('SELECT pg_switch_wal()', 'read-write')
    ).toThrow();
  });

  // False-positive safety: legitimate functions must still pass
  it('should allow pg_stat_user_tables (not pg_stat_reset)', () => {
    expect(() =>
      sanitizeQuery('SELECT * FROM pg_stat_user_tables', 'read-only')
    ).not.toThrow();
  });

  it('should allow currval (not setval/nextval)', () => {
    expect(() =>
      sanitizeQuery("SELECT currval('my_seq')", 'read-only')
    ).not.toThrow();
  });

  it('should allow columns containing "setval" substring', () => {
    expect(() =>
      sanitizeQuery('SELECT resetval_count FROM metrics', 'read-only')
    ).not.toThrow();
  });
});

describe('sanitizeQuery: round 4 — info disclosure and DoS (R4-009 through R4-018)', () => {
  // R4-010: version() — server metadata disclosure
  it('should reject version()', () => {
    expect(() =>
      sanitizeQuery('SELECT version()', 'read-only')
    ).toThrow();
  });

  it('should reject VERSION() (upper case)', () => {
    expect(() =>
      sanitizeQuery('SELECT VERSION()', 'read-only')
    ).toThrow();
  });

  it('should reject version() in read-write mode', () => {
    expect(() =>
      sanitizeQuery('SELECT version()', 'read-write')
    ).toThrow();
  });

  // R4-011: current_user / session_user — role identity disclosure
  it('should reject current_user', () => {
    expect(() =>
      sanitizeQuery('SELECT current_user', 'read-only')
    ).toThrow();
  });

  it('should reject session_user', () => {
    expect(() =>
      sanitizeQuery('SELECT session_user', 'read-only')
    ).toThrow();
  });

  it('should reject CURRENT_USER (upper case)', () => {
    expect(() =>
      sanitizeQuery('SELECT CURRENT_USER', 'read-only')
    ).toThrow();
  });

  it('should reject current_user in read-write mode', () => {
    expect(() =>
      sanitizeQuery('SELECT current_user', 'read-write')
    ).toThrow();
  });

  // R4-012: pg_relation_filepath — physical path disclosure
  it('should reject pg_relation_filepath()', () => {
    expect(() =>
      sanitizeQuery("SELECT pg_relation_filepath('pg_class')", 'read-only')
    ).toThrow();
  });

  // R4-013: WAL/recovery state disclosure
  it('should reject pg_is_in_recovery()', () => {
    expect(() =>
      sanitizeQuery('SELECT pg_is_in_recovery()', 'read-only')
    ).toThrow();
  });

  it('should reject pg_last_wal_replay_lsn()', () => {
    expect(() =>
      sanitizeQuery('SELECT pg_last_wal_replay_lsn()', 'read-only')
    ).toThrow();
  });

  it('should reject pg_current_wal_lsn()', () => {
    expect(() =>
      sanitizeQuery('SELECT pg_current_wal_lsn()', 'read-only')
    ).toThrow();
  });

  // R4-016: privilege enumeration
  it('should reject has_table_privilege()', () => {
    expect(() =>
      sanitizeQuery("SELECT has_table_privilege('users', 'SELECT')", 'read-only')
    ).toThrow();
  });

  it('should reject has_schema_privilege()', () => {
    expect(() =>
      sanitizeQuery("SELECT has_schema_privilege('public', 'USAGE')", 'read-only')
    ).toThrow();
  });

  it('should reject has_database_privilege()', () => {
    expect(() =>
      sanitizeQuery("SELECT has_database_privilege('mydb', 'CONNECT')", 'read-only')
    ).toThrow();
  });

  it('should reject HAS_TABLE_PRIVILEGE (upper case)', () => {
    expect(() =>
      sanitizeQuery("SELECT HAS_TABLE_PRIVILEGE('users', 'SELECT')", 'read-only')
    ).toThrow();
  });

  // R4-017: transaction ID disclosure
  it('should reject txid_current()', () => {
    expect(() =>
      sanitizeQuery('SELECT txid_current()', 'read-only')
    ).toThrow();
  });

  it('should reject txid_current_snapshot()', () => {
    expect(() =>
      sanitizeQuery('SELECT txid_current_snapshot()', 'read-only')
    ).toThrow();
  });

  // R4-018: DoS via string padding (repeat() alternative)
  it('should reject rpad()', () => {
    expect(() =>
      sanitizeQuery("SELECT rpad('A', 100000000)", 'read-only')
    ).toThrow();
  });

  it('should reject lpad()', () => {
    expect(() =>
      sanitizeQuery("SELECT lpad('A', 100000000)", 'read-only')
    ).toThrow();
  });

  it('should reject RPAD (upper case)', () => {
    expect(() =>
      sanitizeQuery("SELECT RPAD('A', 100000000)", 'read-only')
    ).toThrow();
  });

  it('should reject lpad in read-write mode', () => {
    expect(() =>
      sanitizeQuery("SELECT lpad('x', 999999999)", 'read-write')
    ).toThrow();
  });

  // False-positive safety: legitimate queries must still pass
  it('should allow SELECT my_version FROM t (no parens)', () => {
    expect(() =>
      sanitizeQuery('SELECT my_version FROM t', 'read-only')
    ).not.toThrow();
  });

  it('should allow SELECT "current_user_id" FROM t (different word boundary)', () => {
    expect(() =>
      sanitizeQuery('SELECT current_user_id FROM t', 'read-only')
    ).not.toThrow();
  });

  it('should allow SELECT padding FROM t (not rpad)', () => {
    expect(() =>
      sanitizeQuery('SELECT padding FROM t', 'read-only')
    ).not.toThrow();
  });

  it('should allow SELECT session_user_name FROM t (different word boundary)', () => {
    expect(() =>
      sanitizeQuery('SELECT session_user_name FROM t', 'read-only')
    ).not.toThrow();
  });
});

describe('sanitizeQuery / assertNoSensitiveCatalogAccess: round 4 catalogs (R4-009, R4-014, R4-015)', () => {
  // R4-015: pg_available_extensions — blocked via SENSITIVE_CATALOGS (all queries)
  it('should reject pg_available_extensions via sanitizeQuery', () => {
    expect(() =>
      sanitizeQuery('SELECT * FROM pg_available_extensions', 'read-only')
    ).toThrow();
  });

  it('should reject pg_available_extensions in read-write mode', () => {
    expect(() =>
      sanitizeQuery('SELECT * FROM pg_available_extensions', 'read-write')
    ).toThrow();
  });

  // R4-009: pg_stat_database — blocked via USER_QUERY_SENSITIVE_CATALOGS (user queries only)
  it('should reject pg_stat_database via assertNoSensitiveCatalogAccess', () => {
    expect(() =>
      assertNoSensitiveCatalogAccess('SELECT * FROM pg_stat_database')
    ).toThrow();
  });

  it('should reject pg_stat_database case-insensitive', () => {
    expect(() =>
      assertNoSensitiveCatalogAccess('SELECT * FROM PG_STAT_DATABASE')
    ).toThrow();
  });

  it('should reject pg_stat_database with schema prefix', () => {
    expect(() =>
      assertNoSensitiveCatalogAccess('SELECT * FROM pg_catalog.pg_stat_database')
    ).toThrow();
  });

  // R4-014: pg_stat_user_tables — blocked via USER_QUERY_SENSITIVE_CATALOGS (user queries only)
  it('should reject pg_stat_user_tables via assertNoSensitiveCatalogAccess', () => {
    expect(() =>
      assertNoSensitiveCatalogAccess('SELECT * FROM pg_stat_user_tables')
    ).toThrow();
  });

  it('should reject pg_stat_user_tables case-insensitive', () => {
    expect(() =>
      assertNoSensitiveCatalogAccess('SELECT * FROM PG_STAT_USER_TABLES')
    ).toThrow();
  });

  // pg_stat_database and pg_stat_user_tables must NOT be blocked by sanitizeQuery
  // (internal tools use them)
  it('should allow pg_stat_database via sanitizeQuery (internal tools need it)', () => {
    expect(() =>
      sanitizeQuery('SELECT * FROM pg_stat_database', 'read-only')
    ).not.toThrow();
  });

  it('should allow pg_stat_user_tables via sanitizeQuery (internal tools need it)', () => {
    expect(() =>
      sanitizeQuery('SELECT * FROM pg_stat_user_tables', 'read-only')
    ).not.toThrow();
  });
});
