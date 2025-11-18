import { escapeIdentifier, sanitizeIdentifier } from './sanitize.js';

export interface SelectOptions {
  schema?: string;
  columns?: string[];
  where?: string;
  orderBy?: string;
  limit?: number;
  offset?: number;
}

export function buildSelectQuery(table: string, options: SelectOptions = {}): string {
  const {
    schema = 'public',
    columns = ['*'],
    where,
    orderBy,
    limit,
    offset
  } = options;

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);
  const sanitizedColumns = columns.map(col =>
    col === '*' ? '*' : escapeIdentifier(sanitizeIdentifier(col))
  );

  const parts = [
    'SELECT',
    sanitizedColumns.join(', '),
    'FROM',
    `${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}`
  ];

  if (where) {
    parts.push('WHERE', where);
  }

  if (orderBy) {
    parts.push('ORDER BY', orderBy);
  }

  if (limit !== undefined) {
    parts.push(`LIMIT ${parseInt(String(limit), 10)}`);
  }

  if (offset !== undefined) {
    parts.push(`OFFSET ${parseInt(String(offset), 10)}`);
  }

  return parts.join(' ');
}

export function buildCountQuery(table: string, schema: string = 'public', where?: string): string {
  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const parts = [
    'SELECT COUNT(*) as count',
    'FROM',
    `${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}`
  ];

  if (where) {
    parts.push('WHERE', where);
  }

  return parts.join(' ');
}

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 Bytes';

  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

export function formatNumber(num: number): string {
  return num.toLocaleString();
}
