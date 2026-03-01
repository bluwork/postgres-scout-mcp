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

// --- Builder ---

function buildCondition(
  condition: WhereCondition,
  paramCounter: { value: number },
  params: any[]
): string {
  // AND group
  if ('and' in condition) {
    const parts = condition.and.map(c => buildCondition(c, paramCounter, params));
    return parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`;
  }

  // OR group
  if ('or' in condition) {
    const parts = condition.or.map(c => buildCondition(c, paramCounter, params));
    return parts.length === 1 ? parts[0] : `(${parts.join(' OR ')})`;
  }

  // Leaf condition — sanitize and escape the field name
  const escapedField = escapeIdentifier(sanitizeIdentifier(condition.field));

  if (condition.op === 'IS NULL') {
    return `${escapedField} IS NULL`;
  }

  if (condition.op === 'IS NOT NULL') {
    return `${escapedField} IS NOT NULL`;
  }

  if (condition.op === 'IN' || condition.op === 'NOT IN') {
    const placeholders = condition.value.map(v => {
      params.push(v);
      return `$${paramCounter.value++}`;
    });
    return `${escapedField} ${condition.op} (${placeholders.join(', ')})`;
  }

  if (condition.op === 'BETWEEN') {
    const p1 = `$${paramCounter.value++}`;
    params.push(condition.value[0]);
    const p2 = `$${paramCounter.value++}`;
    params.push(condition.value[1]);
    return `${escapedField} BETWEEN ${p1} AND ${p2}`;
  }

  // Comparison operators: =, !=, >, <, >=, <=, LIKE, ILIKE
  const placeholder = `$${paramCounter.value++}`;
  params.push(condition.value);
  return `${escapedField} ${condition.op} ${placeholder}`;
}

export function buildWhereClause(
  conditions: WhereCondition[],
  startParam: number = 1
): WhereClauseResult {
  if (conditions.length === 0) {
    return { clause: '', params: [] };
  }

  const params: any[] = [];
  const paramCounter = { value: startParam };
  const parts = conditions.map(c => buildCondition(c, paramCounter, params));

  return {
    clause: parts.join(' AND '),
    params
  };
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
