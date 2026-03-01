import { sanitizeIdentifier, escapeIdentifier } from './sanitize.js';

// --- Types ---

export type ComparisonOp = '=' | '!=' | '>' | '<' | '>=' | '<=' | 'LIKE' | 'ILIKE';

export type WhereCondition =
  | { field: string; op: ComparisonOp; value: string | number | boolean }
  | { field: string; op: 'IN'; value: (string | number)[] }
  | { field: string; op: 'NOT IN'; value: (string | number)[] }
  | { field: string; op: 'IS NULL' }
  | { field: string; op: 'IS NOT NULL' }
  | { field: string; op: 'BETWEEN'; value: [string | number, string | number] }
  | { and: WhereCondition[] }
  | { or: WhereCondition[] };

export interface WhereClauseResult {
  clause: string;
  params: any[];
}

// --- Builder (stub — returns wrong output for TDD red phase) ---

export function buildWhereClause(
  conditions: WhereCondition[],
  startParam: number = 1
): WhereClauseResult {
  return { clause: '', params: [] };
}

// --- Formatting utilities (unchanged) ---

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
