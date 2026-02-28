import { DatabaseMode } from '../types.js';

const ALLOWED_READ_ONLY_OPERATIONS = ['SELECT', 'EXPLAIN', 'WITH'];
const ALLOWED_READ_WRITE_OPERATIONS = [
  'SELECT', 'INSERT', 'UPDATE', 'DELETE',
  'CREATE', 'ALTER', 'DROP', 'TRUNCATE',
  'VACUUM', 'ANALYZE', 'REINDEX',
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

const CTE_DATA_MODIFYING_PATTERN = /\bAS\s+(NOT\s+)?MATERIALIZED\s*\(\s*(INSERT|UPDATE|DELETE|TRUNCATE)\b|\bAS\s*\(\s*(INSERT|UPDATE|DELETE|TRUNCATE)\b/i;

const WHERE_DANGEROUS_PATTERNS = [
  /;\s*\w/i,
  /--/,
  /\/\*/,
  /\*\//,
  /UNION\s+(ALL\s+)?SELECT/i,
  /INTO\s+(OUT|DUMP)FILE/i,
  /LOAD_FILE\s*\(/i
];

function extractMainStatementAfterCTEs(query: string): string | null {
  let depth = 0;
  let inSingleQuote = false;
  let i = 0;
  const upper = query.toUpperCase();

  // Skip past "WITH"
  const withMatch = upper.match(/^\s*WITH\s+/i);
  if (!withMatch) return null;
  i = withMatch[0].length;

  // Walk through CTE definitions, tracking parenthesis depth
  // CTEs end when we reach depth 0 after a closing paren, followed by a non-comma keyword
  while (i < query.length) {
    const char = query[i];

    if (inSingleQuote) {
      if (char === "'" && query[i + 1] === "'") {
        i += 2; // escaped quote
        continue;
      }
      if (char === "'") {
        inSingleQuote = false;
      }
      i++;
      continue;
    }

    if (char === "'") {
      inSingleQuote = true;
      i++;
      continue;
    }

    if (char === '(') {
      depth++;
      i++;
      continue;
    }

    if (char === ')') {
      depth--;
      if (depth === 0) {
        // After closing a CTE body at depth 0, look ahead for comma (another CTE) or main statement
        const rest = query.substring(i + 1).trimStart();
        if (rest.startsWith(',')) {
          // Another CTE follows, skip the comma and continue
          i = query.length - rest.length + 1;
          continue;
        }
        // This is the main statement
        return rest;
      }
      i++;
      continue;
    }

    i++;
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

  for (const pattern of DANGEROUS_PATTERNS) {
    if (pattern.test(trimmedQuery)) {
      throw new Error('Potentially dangerous query pattern detected');
    }
  }

  if (mode === 'read-only' && CTE_DATA_MODIFYING_PATTERN.test(trimmedQuery)) {
    throw new Error(
      'Data-modifying statements (INSERT, UPDATE, DELETE, TRUNCATE) are not allowed within CTEs in read-only mode'
    );
  }

  if (mode === 'read-only' && operation === 'WITH') {
    const mainStatement = extractMainStatementAfterCTEs(trimmedQuery);
    if (mainStatement) {
      const mainOp = mainStatement.split(/\s+/)[0].toUpperCase();
      if (!ALLOWED_READ_ONLY_OPERATIONS.includes(mainOp)) {
        throw new Error(
          `Operation ${mainOp} not allowed in read-only mode. CTE queries must use a read-only main statement (SELECT, EXPLAIN).`
        );
      }
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

export function buildWhereClause(conditions: Record<string, any>): { clause: string; params: any[] } {
  const params: any[] = [];
  const clauses: string[] = [];

  Object.entries(conditions).forEach(([key, value]) => {
    const identifier = sanitizeIdentifier(key);
    params.push(value);
    clauses.push(`${escapeIdentifier(identifier)} = $${params.length}`);
  });

  return {
    clause: clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '',
    params
  };
}

export function validateUserWhereClause(where: string): void {
  if (!where || !where.trim()) {
    throw new Error('WHERE clause cannot be empty');
  }

  const trimmed = where.trim();

  for (const pattern of WHERE_DANGEROUS_PATTERNS) {
    if (pattern.test(trimmed)) {
      throw new Error('Potentially dangerous pattern detected in WHERE clause');
    }
  }

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

  for (const pattern of WHERE_DANGEROUS_PATTERNS) {
    if (pattern.test(trimmed)) {
      throw new Error('Potentially dangerous pattern detected in condition');
    }
  }

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

  for (const pattern of WHERE_DANGEROUS_PATTERNS) {
    if (pattern.test(trimmed)) {
      throw new Error('Potentially dangerous pattern detected in ORDER BY clause');
    }
  }

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
