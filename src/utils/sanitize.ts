import { DatabaseMode } from '../types.js';

const ALLOWED_READ_ONLY_OPERATIONS = ['SELECT', 'EXPLAIN', 'WITH'];
const ALLOWED_READ_WRITE_OPERATIONS = [
  'SELECT', 'INSERT', 'UPDATE', 'DELETE',
  'EXPLAIN', 'WITH'
];

const DANGEROUS_PATTERNS = [
  /;\s*DROP\b/i,
  /;\s*DELETE\s+FROM\b/i,
  /;\s*TRUNCATE\b/i,
  /;\s*ALTER\b/i,
  /;\s*INSERT\b/i,
  /;\s*UPDATE\b/i,
  /;\s*CREATE\b/i,
  /;\s*GRANT\b/i,
  /;\s*REVOKE\b/i,
  /--/,
  /\/\*/,
  /\*\//,
  /;\s*EXEC\b/i,
  /;\s*EXECUTE\b/i,
  /xp_/i,
  /UNION\s+(ALL\s+)?SELECT/i
];

const QUERY_DANGEROUS_FUNCTIONS = [
  // Filesystem access
  /\bpg_read_file\s*\(/i,
  /\bpg_read_binary_file\s*\(/i,
  /\bpg_ls_dir\s*\(/i,
  /\bpg_ls_logdir\s*\(/i,
  /\bpg_ls_waldir\s*\(/i,
  /\bpg_ls_tmpdir\s*\(/i,
  /\bpg_ls_archive_statusdir\s*\(/i,
  /\bpg_stat_file\s*\(/i,
  // Timing / sleep
  /\bpg_sleep\s*\(/i,
  // Large object API — complete set (R3-004)
  /\blo_import\s*\(/i,
  /\blo_export\s*\(/i,
  /\blo_creat\s*\(/i,
  /\blo_create\s*\(/i,
  /\blo_open\s*\(/i,
  /\blo_close\s*\(/i,
  /\blo_get\s*\(/i,
  /\blo_put\s*\(/i,
  /\blo_from_bytea\s*\(/i,
  /\blo_truncate\s*\(/i,
  /\blo_unlink\s*\(/i,
  /\bloread\s*\(/i,
  /\blowrite\s*\(/i,
  // Remote execution
  /\bdblink\s*\(/i,
  // Configuration
  /\bcurrent_setting\s*\(/i,
  /\bset_config\s*\(/i,
  // XML export (execute arbitrary SQL via string arguments)
  /\bquery_to_xml\s*\(/i,
  /\bquery_to_xml_and_xmlschema\s*\(/i,
  /\btable_to_xml\s*\(/i,
  /\btable_to_xml_and_xmlschema\s*\(/i,
  /\bschema_to_xml\s*\(/i,
  /\bschema_to_xml_and_xmlschema\s*\(/i,
  /\bdatabase_to_xml\s*\(/i,
  /\bdatabase_to_xml_and_xmlschema\s*\(/i,
  /\bcursor_to_xml\s*\(/i,
  // Process control (DoS)
  /\bpg_terminate_backend\s*\(/i,
  /\bpg_cancel_backend\s*\(/i,
  /\bpg_reload_conf\s*\(/i,
  /\bpg_rotate_logfile\s*\(/i,
  // Resource abuse (advisory locks, notifications)
  /\bpg_advisory_lock\s*\(/i,
  /\bpg_advisory_lock_shared\s*\(/i,
  /\bpg_try_advisory_lock\s*\(/i,
  /\bpg_try_advisory_lock_shared\s*\(/i,
  /\bpg_advisory_xact_lock\s*\(/i,
  /\bpg_advisory_xact_lock_shared\s*\(/i,
  /\bpg_notify\s*\(/i,
  // Network topology disclosure (R3-010)
  /\binet_server_addr\s*\(/i,
  /\binet_server_port\s*\(/i,
  /\binet_client_addr\s*\(/i,
  /\binet_client_port\s*\(/i,
  // Server metadata disclosure (R3-012, R3-015)
  /\bpg_export_snapshot\s*\(/i,
  /\bpg_current_logfile\s*\(/i,
  /\bpg_postmaster_start_time\s*\(/i,
  /\bpg_conf_load_time\s*\(/i,
  /\bpg_backend_pid\s*\(/i,
  /\bpg_tablespace_location\s*\(/i,
  // DoS / resource exhaustion (R3-016, R3-017)
  /\bgenerate_series\s*\(/i,
  /\brepeat\s*\(/i,
  // Stats reset (R4-001) — can zero out monitoring data
  /\bpg_stat_reset\s*\(/i,
  /\bpg_stat_reset_shared\s*\(/i,
  /\bpg_stat_reset_single_table_counters\s*\(/i,
  /\bpg_stat_reset_slru\s*\(/i,
  /\bpg_stat_reset_replication_slot\s*\(/i,
  // Sequence manipulation (R4-002) — can alter auto-increment state
  /\bsetval\s*\(/i,
  /\bnextval\s*\(/i,
  // WAL / restore-point / logical replication (R4-003)
  /\bpg_switch_wal\s*\(/i,
  /\bpg_create_restore_point\s*\(/i,
  /\bpg_logical_emit_message\s*\(/i,
];

const SENSITIVE_CATALOGS = [
  /\bpg_shadow\b/i,
  /\bpg_authid\b/i,
  /\bpg_auth_members\b/i,
  /\bpg_hba_file_rules\b/i,
  /\bpg_file_settings\b/i,
  /\bpg_roles\b/i,
  /\bpg_stat_ssl\b/i,
  /\bpg_largeobject\b/i,
  /\bpg_largeobject_metadata\b/i,
];

const USER_QUERY_SENSITIVE_CATALOGS = [
  /\bpg_settings\b/i,
  /\bpg_stat_activity\b/i,
  /\bpg_stat_replication\b/i,
  /\bpg_stat_gssapi\b/i,
  /\bpg_ident_file_mappings\b/i,
  /\bpg_proc\b/i,
  /\bpg_database\b/i,
  /\bpg_tablespace\b/i,
  /\bpg_prepared_statements\b/i,
  /\binformation_schema\.enabled_roles\b/i,
  /\binformation_schema\.role_table_grants\b/i,
  /\binformation_schema\.applicable_roles\b/i,
  /\binformation_schema\.role_routine_grants\b/i,
];

const CTE_DATA_MODIFYING_PATTERN = /\bAS\s+(NOT\s+)?MATERIALIZED\s*\(\s*(INSERT|UPDATE|DELETE|TRUNCATE)\b|\bAS\s*\(\s*(INSERT|UPDATE|DELETE|TRUNCATE)\b/i;

const WHERE_DANGEROUS_PATTERNS = [
  /;\s*\w/i,
  /--/,
  /\/\*/,
  /\*\//,
  /UNION\s+(ALL\s+)?SELECT/i,
  /INTO\s+(OUT|DUMP)FILE/i,
  /LOAD_FILE\s*\(/i,
  /\bSELECT\b/i,
  /\bEXECUTE\b/i,
  /\bCOPY\b/i,
  ...QUERY_DANGEROUS_FUNCTIONS,
];

const ALLOWED_CTE_MAIN_OPERATIONS = ['SELECT', 'EXPLAIN'];

function assertNoMatch(patterns: RegExp[], input: string, message: string): void {
  for (const pattern of patterns) {
    if (pattern.test(input)) {
      throw new Error(message);
    }
  }
}

export function assertNoSensitiveCatalogAccess(query: string): void {
  assertNoMatch(
    USER_QUERY_SENSITIVE_CATALOGS,
    query,
    'Access to sensitive system catalog is not allowed in user queries'
  );
}

function isWordChar(c: string): boolean {
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c === '_';
}

function skipWhitespace(query: string, pos: number): number {
  while (pos < query.length && /\s/.test(query[pos])) pos++;
  return pos;
}

function skipDollarQuotedString(query: string, pos: number): number | null {
  if (query[pos] !== '$') return null;

  // Find the tag: $$ or $tag$
  let tagEnd = pos + 1;
  while (tagEnd < query.length && query[tagEnd] !== '$' && /[a-zA-Z0-9_]/.test(query[tagEnd])) {
    tagEnd++;
  }
  if (tagEnd >= query.length || query[tagEnd] !== '$') return null;

  const tag = query.substring(pos, tagEnd + 1); // e.g. "$$" or "$tag$"
  const searchFrom = tagEnd + 1;
  const closeIdx = query.indexOf(tag, searchFrom);
  if (closeIdx === -1) return null; // unterminated

  return closeIdx + tag.length;
}

function skipSingleQuotedString(query: string, pos: number): number | null {
  if (query[pos] !== "'") return null;

  // Detect PostgreSQL E-string literals (E'...' with backslash escapes)
  const isEString = pos > 0 && (query[pos - 1] === 'E' || query[pos - 1] === 'e') &&
    (pos < 2 || !isWordChar(query[pos - 2]));

  let i = pos + 1;
  while (i < query.length) {
    if (isEString && query[i] === '\\') {
      i += 2; // skip backslash-escaped character
      continue;
    }
    if (query[i] === "'" && query[i + 1] === "'") {
      i += 2; // doubled quote escape
      continue;
    }
    if (query[i] === "'") {
      return i + 1;
    }
    i++;
  }
  return null; // unterminated
}

function skipDoubleQuotedIdentifier(query: string, pos: number): number | null {
  if (query[pos] !== '"') return null;
  let i = pos + 1;
  while (i < query.length) {
    if (query[i] === '"' && query[i + 1] === '"') {
      i += 2; // escaped double quote
      continue;
    }
    if (query[i] === '"') {
      return i + 1;
    }
    i++;
  }
  return null; // unterminated
}

function findKeywordAt(query: string, pos: number, keyword: string): boolean {
  const upper = query.toUpperCase();
  if (upper.substring(pos, pos + keyword.length) !== keyword) return false;
  if (pos > 0 && isWordChar(query[pos - 1])) return false;
  if (pos + keyword.length < query.length && isWordChar(query[pos + keyword.length])) return false;
  return true;
}

function extractMainStatementAfterCTEs(query: string): string | null {
  const len = query.length;
  let i = 0;

  // Skip past leading "WITH" (and optional "RECURSIVE")
  const withMatch = query.match(/^\s*WITH\s+/i);
  if (!withMatch) return null;
  i = withMatch[0].length;

  i = skipWhitespace(query, i);
  if (findKeywordAt(query, i, 'RECURSIVE')) {
    i += 'RECURSIVE'.length;
    i = skipWhitespace(query, i);
  }

  // Process each CTE definition
  while (i < len) {
    // Skip CTE name (identifier)
    i = skipWhitespace(query, i);
    while (i < len && !(/\s/.test(query[i])) && query[i] !== '(' && query[i] !== ',') i++;
    i = skipWhitespace(query, i);

    // Skip optional column list: cte(col1, col2)
    if (i < len && query[i] === '(') {
      let depth = 1;
      i++; // skip opening (
      while (i < len && depth > 0) {
        const skipSQ = skipSingleQuotedString(query, i);
        if (skipSQ !== null) { i = skipSQ; continue; }
        const skipDQI = skipDoubleQuotedIdentifier(query, i);
        if (skipDQI !== null) { i = skipDQI; continue; }
        if (query[i] === '(') depth++;
        else if (query[i] === ')') depth--;
        if (depth > 0) i++;
      }
      if (depth !== 0) return null; // unterminated column list
      i++; // skip closing )
      i = skipWhitespace(query, i);
    }

    // Expect AS keyword
    if (!findKeywordAt(query, i, 'AS')) return null;
    i += 2; // skip "AS"
    i = skipWhitespace(query, i);

    // Skip optional NOT MATERIALIZED / MATERIALIZED
    if (findKeywordAt(query, i, 'NOT')) {
      i += 3;
      i = skipWhitespace(query, i);
      if (findKeywordAt(query, i, 'MATERIALIZED')) {
        i += 12;
        i = skipWhitespace(query, i);
      }
    } else if (findKeywordAt(query, i, 'MATERIALIZED')) {
      i += 12;
      i = skipWhitespace(query, i);
    }

    // Expect opening ( of CTE body
    if (i >= len || query[i] !== '(') return null;

    // Walk through CTE body tracking depth, handling strings and identifiers
    let depth = 1;
    i++; // skip opening (
    while (i < len && depth > 0) {
      const skipSQ = skipSingleQuotedString(query, i);
      if (skipSQ !== null) { i = skipSQ; continue; }
      const skipDollar = skipDollarQuotedString(query, i);
      if (skipDollar !== null) { i = skipDollar; continue; }
      const skipDQI = skipDoubleQuotedIdentifier(query, i);
      if (skipDQI !== null) { i = skipDQI; continue; }
      if (query[i] === '(') depth++;
      else if (query[i] === ')') depth--;
      if (depth > 0) i++;
    }

    if (depth !== 0) return null; // unterminated CTE body
    i++; // skip closing )

    i = skipWhitespace(query, i);

    // Check for comma (another CTE follows)
    if (i < len && query[i] === ',') {
      i++; // skip comma
      continue;
    }

    // No comma — what follows is the main statement
    const rest = query.substring(i).trimStart();
    return rest.length > 0 ? rest : null;
  }

  return null;
}

export function sanitizeQuery(query: string, mode: DatabaseMode): void {
  const trimmedQuery = query.trim();

  if (!trimmedQuery) {
    throw new Error('Query cannot be empty');
  }

  const operation = trimmedQuery.split(/\s+/)[0].toUpperCase();
  const allowedOps = mode === 'read-only'
    ? ALLOWED_READ_ONLY_OPERATIONS
    : ALLOWED_READ_WRITE_OPERATIONS;

  if (!allowedOps.includes(operation)) {
    throw new Error(
      `Operation ${operation} not allowed in ${mode} mode. Allowed operations: ${allowedOps.join(', ')}`
    );
  }

  assertNoMatch(DANGEROUS_PATTERNS, trimmedQuery, 'Potentially dangerous query pattern detected');
  assertNoMatch(QUERY_DANGEROUS_FUNCTIONS, trimmedQuery, 'Potentially dangerous function call detected');
  assertNoMatch(SENSITIVE_CATALOGS, trimmedQuery, 'Access to sensitive system catalog is not allowed');

  if (mode === 'read-only' && CTE_DATA_MODIFYING_PATTERN.test(trimmedQuery)) {
    throw new Error(
      'Data-modifying statements (INSERT, UPDATE, DELETE, TRUNCATE) are not allowed within CTEs in read-only mode'
    );
  }

  if (mode === 'read-only' && operation === 'WITH') {
    const mainStatement = extractMainStatementAfterCTEs(trimmedQuery);
    if (!mainStatement) {
      throw new Error(
        'Unable to determine main statement after CTEs; query not allowed in read-only mode.'
      );
    }
    const mainOp = mainStatement.split(/\s+/)[0].toUpperCase();
    if (!ALLOWED_CTE_MAIN_OPERATIONS.includes(mainOp)) {
      throw new Error(
        `Operation ${mainOp} not allowed in read-only mode. CTE queries must use a read-only main statement (${ALLOWED_CTE_MAIN_OPERATIONS.join(', ')}).`
      );
    }
  }

  if (trimmedQuery.includes(';') && trimmedQuery.indexOf(';') !== trimmedQuery.length - 1) {
    throw new Error('Multiple statements not allowed. Use single queries only.');
  }
}

export function sanitizeIdentifier(identifier: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(identifier)) {
    throw new Error(`Invalid identifier: ${identifier}. Must contain only letters, numbers, and underscores, and start with a letter or underscore.`);
  }
  return identifier;
}

export function sanitizeSchemaTable(schema: string, table: string): { schema: string; table: string } {
  return {
    schema: sanitizeIdentifier(schema),
    table: sanitizeIdentifier(table)
  };
}

export function escapeIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

export function validateUserWhereClause(where: string): void {
  if (!where || !where.trim()) {
    throw new Error('WHERE clause cannot be empty');
  }

  const trimmed = where.trim();

  assertNoMatch(WHERE_DANGEROUS_PATTERNS, trimmed, 'Potentially dangerous pattern detected in WHERE clause');

  const openParens = (trimmed.match(/\(/g) || []).length;
  const closeParens = (trimmed.match(/\)/g) || []).length;
  if (openParens !== closeParens) {
    throw new Error('Unbalanced parentheses in WHERE clause');
  }

  const singleQuotes = (trimmed.match(/'/g) || []).length;
  if (singleQuotes % 2 !== 0) {
    throw new Error('Unbalanced quotes in WHERE clause');
  }
}

export function validateCondition(condition: string): void {
  if (!condition || !condition.trim()) {
    throw new Error('Condition cannot be empty');
  }

  const trimmed = condition.trim();

  assertNoMatch(WHERE_DANGEROUS_PATTERNS, trimmed, 'Potentially dangerous pattern detected in condition');

  const openParens = (trimmed.match(/\(/g) || []).length;
  const closeParens = (trimmed.match(/\)/g) || []).length;
  if (openParens !== closeParens) {
    throw new Error('Unbalanced parentheses in condition');
  }
}

export function validateInterval(interval: string): void {
  if (!interval || !interval.trim()) {
    throw new Error('Interval cannot be empty');
  }

  const validIntervalPattern = /^\d+\s+(second|minute|hour|day|week|month|year)s?$/i;
  if (!validIntervalPattern.test(interval.trim())) {
    throw new Error(
      `Invalid interval format: "${interval}". Use format like "7 days", "2 hours", "30 minutes"`
    );
  }
}

export function validateOrderBy(orderBy: string): void {
  if (!orderBy || !orderBy.trim()) {
    return;
  }

  const trimmed = orderBy.trim();

  assertNoMatch(WHERE_DANGEROUS_PATTERNS, trimmed, 'Potentially dangerous pattern detected in ORDER BY clause');

  const validOrderPattern = /^[\w"]+(\s+(ASC|DESC))?(,\s*[\w"]+(\s+(ASC|DESC))?)*$/i;
  if (!validOrderPattern.test(trimmed)) {
    throw new Error(
      'Invalid ORDER BY format. Use column names with optional ASC/DESC'
    );
  }
}

export function parseIntSafe(value: string, defaultValue: number): number {
  const parsed = parseInt(value, 10);
  if (Number.isNaN(parsed)) {
    return defaultValue;
  }
  return parsed;
}

const PG_ERROR_CATEGORIES: Array<{ pattern: RegExp; message: (match: RegExpMatchArray) => string }> = [
  { pattern: /syntax error/i, message: () => 'Query syntax error' },
  { pattern: /statement timeout/i, message: () => 'Query timed out' },
  { pattern: /permission denied/i, message: () => 'Permission denied' },
  { pattern: /does not exist/i, message: () => 'Referenced object does not exist' },
  { pattern: /already exists/i, message: () => 'Object already exists' },
  { pattern: /duplicate key/i, message: () => 'Duplicate key violation' },
  { pattern: /not-null/i, message: () => 'NOT NULL constraint violation' },
  { pattern: /foreign key/i, message: () => 'Foreign key constraint violation' },
  { pattern: /check constraint/i, message: () => 'Check constraint violation' },
  { pattern: /deadlock detected/i, message: () => 'Deadlock detected' },
  { pattern: /connection refused/i, message: () => 'Database connection refused' },
  { pattern: /too many connections/i, message: () => 'Too many database connections' },
  { pattern: /division by zero/i, message: () => 'Division by zero' },
  { pattern: /invalid input/i, message: () => 'Invalid input value' },
  { pattern: /out of range/i, message: () => 'Value out of range' },
  { pattern: /cannot be cast/i, message: () => 'Type cast error' },
];

export function sanitizeErrorMessage(error: string): string {
  for (const category of PG_ERROR_CATEGORIES) {
    if (category.pattern.test(error)) {
      return category.message(error.match(category.pattern)!);
    }
  }
  return 'Database operation failed';
}

export function sanitizeLogValue(value: any): string {
  const str = typeof value === 'string' ? value : (JSON.stringify(value) ?? '');
  return str.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\r\n\t]/g, ' ');
}
