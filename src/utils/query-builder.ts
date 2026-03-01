import { sanitizeIdentifier, escapeIdentifier } from './sanitize.js';
import { z } from 'zod';

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

const ComparisonOpSchema = z.enum(['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'ILIKE']);

const ComparisonConditionSchema = z.object({
  field: z.string(),
  op: ComparisonOpSchema,
  value: z.union([z.string(), z.number(), z.boolean()])
}).strict();

const InConditionSchema = z.object({
  field: z.string(),
  op: z.enum(['IN', 'NOT IN']),
  value: z.array(z.union([z.string(), z.number()])).min(1)
}).strict();

const NullConditionSchema = z.object({
  field: z.string(),
  op: z.enum(['IS NULL', 'IS NOT NULL'])
}).strict();

const BetweenConditionSchema = z.object({
  field: z.string(),
  op: z.literal('BETWEEN'),
  value: z.tuple([z.union([z.string(), z.number()]), z.union([z.string(), z.number()])])
}).strict();

const LeafConditionSchema = z.union([
  ComparisonConditionSchema,
  InConditionSchema,
  NullConditionSchema,
  BetweenConditionSchema
]);

export const WhereConditionSchema: z.ZodType<WhereCondition> = z.lazy(() =>
  z.union([
    LeafConditionSchema,
    z.object({ and: z.array(WhereConditionSchema).min(1) }).strict(),
    z.object({ or: z.array(WhereConditionSchema).min(1) }).strict()
  ])
);

export interface WhereClauseResult {
  clause: string;
  params: any[];
}

// --- Trivially-true condition detection ---

const MAX_SAFE_BETWEEN_RANGE = 1_000_000_000; // 1 billion — any range wider than this is suspicious

type ComplementaryPair = { op1: ComparisonOp | 'IS NULL' | 'IS NOT NULL'; op2: ComparisonOp | 'IS NULL' | 'IS NOT NULL' };
const COMPLEMENTARY_OPS: ComplementaryPair[] = [
  { op1: '>', op2: '<=' },
  { op1: '>=', op2: '<' },
  { op1: '=', op2: '!=' },
  { op1: 'IS NULL', op2: 'IS NOT NULL' },
];

function checkOrGroupTriviallyTrue(conditions: WhereCondition[]): void {
  const leaves = conditions.filter((c): c is Exclude<WhereCondition, { and: any } | { or: any }> => 'op' in c);

  for (let i = 0; i < leaves.length; i++) {
    for (let j = i + 1; j < leaves.length; j++) {
      const a = leaves[i];
      const b = leaves[j];
      if (a.field !== b.field) continue;

      for (const pair of COMPLEMENTARY_OPS) {
        const match =
          (a.op === pair.op1 && b.op === pair.op2) ||
          (a.op === pair.op2 && b.op === pair.op1);
        if (!match) continue;

        if (pair.op1 === 'IS NULL') {
          throw new Error(
            `Trivially true condition detected: OR(${a.field} IS NULL, ${a.field} IS NOT NULL) matches all rows.`
          );
        }

        if ('value' in a && 'value' in b && a.value === b.value) {
          throw new Error(
            `Trivially true condition detected: OR(${a.field} ${a.op} ${a.value}, ${b.field} ${b.op} ${b.value}) matches all rows.`
          );
        }
      }
    }
  }
}

function checkLeafTriviallyTrue(condition: WhereCondition): void {
  if ('and' in condition) {
    for (const c of condition.and) checkLeafTriviallyTrue(c);
    return;
  }
  if ('or' in condition) {
    checkOrGroupTriviallyTrue(condition.or);
    for (const c of condition.or) checkLeafTriviallyTrue(c);
    return;
  }

  // LIKE/ILIKE wildcard-only patterns (%, %%, _%, %_%, __, etc.)
  if ((condition.op === 'LIKE' || condition.op === 'ILIKE') && 'value' in condition) {
    const val = String(condition.value);
    if (val.replace(/[%_]/g, '') === '') {
      throw new Error(
        `Trivially true condition detected: ${condition.op} '${val}' matches all rows. Use a more specific pattern.`
      );
    }
  }

  // BETWEEN with extreme numeric range
  if (condition.op === 'BETWEEN' && 'value' in condition) {
    const [low, high] = condition.value;
    if (typeof low === 'number' && typeof high === 'number') {
      const range = high - low;
      if (range >= MAX_SAFE_BETWEEN_RANGE) {
        throw new Error(
          `Trivially true condition detected: BETWEEN ${low} AND ${high} spans ${range.toLocaleString()} values. Use a narrower range.`
        );
      }
    }
  }
}

function assertNotTriviallyTrue(conditions: WhereCondition[]): void {
  // Flatten: if there's exactly one top-level condition and it's an AND group, unwrap it
  let effective = conditions;
  if (effective.length === 1 && 'and' in effective[0]) {
    effective = effective[0].and;
  }

  // Check: sole IS NOT NULL
  if (effective.length === 1) {
    const c = effective[0];
    if ('op' in c && c.op === 'IS NOT NULL') {
      throw new Error(
        'Trivially true condition detected: IS NOT NULL as the sole WHERE condition matches all non-null rows. Add additional conditions to narrow the scope.'
      );
    }
  }

  // Recursively check all leaf conditions for trivially-true patterns
  for (const condition of effective) {
    checkLeafTriviallyTrue(condition);
  }
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
    if (condition.value.length === 0) {
      throw new Error(`${condition.op} requires at least one value`);
    }
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

  assertNotTriviallyTrue(conditions);

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
