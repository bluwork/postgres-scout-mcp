import { describe, it, expect } from 'vitest';
import { buildWhereClause, WhereCondition } from '../src/utils/query-builder.js';

describe('buildWhereClause: comparison operators', () => {
  it('should build simple equality condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'status', op: '=', value: 'active' }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"status" = $1');
    expect(result.params).toEqual(['active']);
  });

  it('should build != condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'role', op: '!=', value: 'guest' }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"role" != $1');
    expect(result.params).toEqual(['guest']);
  });

  it('should build > condition with number', () => {
    const conditions: WhereCondition[] = [
      { field: 'age', op: '>', value: 18 }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"age" > $1');
    expect(result.params).toEqual([18]);
  });

  it('should build < condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'price', op: '<', value: 100 }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"price" < $1');
    expect(result.params).toEqual([100]);
  });

  it('should build >= condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'score', op: '>=', value: 90 }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"score" >= $1');
    expect(result.params).toEqual([90]);
  });

  it('should build <= condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'quantity', op: '<=', value: 0 }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"quantity" <= $1');
    expect(result.params).toEqual([0]);
  });

  it('should build LIKE condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'name', op: 'LIKE', value: '%john%' }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"name" LIKE $1');
    expect(result.params).toEqual(['%john%']);
  });

  it('should build ILIKE condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'email', op: 'ILIKE', value: '%@example.com' }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"email" ILIKE $1');
    expect(result.params).toEqual(['%@example.com']);
  });

  it('should build boolean value condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'active', op: '=', value: true }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"active" = $1');
    expect(result.params).toEqual([true]);
  });
});

describe('buildWhereClause: IN / NOT IN', () => {
  it('should build IN condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'status', op: 'IN', value: ['active', 'pending'] }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"status" IN ($1, $2)');
    expect(result.params).toEqual(['active', 'pending']);
  });

  it('should build NOT IN condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'id', op: 'NOT IN', value: [1, 2, 3] }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"id" NOT IN ($1, $2, $3)');
    expect(result.params).toEqual([1, 2, 3]);
  });

  it('should build IN with single value', () => {
    const conditions: WhereCondition[] = [
      { field: 'type', op: 'IN', value: ['admin'] }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"type" IN ($1)');
    expect(result.params).toEqual(['admin']);
  });
});

describe('buildWhereClause: IS NULL / IS NOT NULL', () => {
  it('should build IS NULL condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'deleted_at', op: 'IS NULL' }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"deleted_at" IS NULL');
    expect(result.params).toEqual([]);
  });

  it('should build IS NOT NULL condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'name', op: 'IS NOT NULL' }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"name" IS NOT NULL');
    expect(result.params).toEqual([]);
  });
});

describe('buildWhereClause: BETWEEN', () => {
  it('should build BETWEEN condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'price', op: 'BETWEEN', value: [10, 50] }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"price" BETWEEN $1 AND $2');
    expect(result.params).toEqual([10, 50]);
  });

  it('should build BETWEEN with string values', () => {
    const conditions: WhereCondition[] = [
      { field: 'created_at', op: 'BETWEEN', value: ['2024-01-01', '2024-12-31'] }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"created_at" BETWEEN $1 AND $2');
    expect(result.params).toEqual(['2024-01-01', '2024-12-31']);
  });
});

describe('buildWhereClause: multiple conditions (implicit AND)', () => {
  it('should AND multiple top-level conditions', () => {
    const conditions: WhereCondition[] = [
      { field: 'status', op: '=', value: 'active' },
      { field: 'age', op: '>', value: 18 }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"status" = $1 AND "age" > $2');
    expect(result.params).toEqual(['active', 18]);
  });

  it('should handle three conditions', () => {
    const conditions: WhereCondition[] = [
      { field: 'a', op: '=', value: 1 },
      { field: 'b', op: '=', value: 2 },
      { field: 'c', op: '=', value: 3 }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"a" = $1 AND "b" = $2 AND "c" = $3');
    expect(result.params).toEqual([1, 2, 3]);
  });
});

describe('buildWhereClause: explicit AND/OR groups', () => {
  it('should build explicit OR group', () => {
    const conditions: WhereCondition[] = [
      { or: [
        { field: 'role', op: '=', value: 'admin' },
        { field: 'role', op: '=', value: 'moderator' }
      ]}
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('("role" = $1 OR "role" = $2)');
    expect(result.params).toEqual(['admin', 'moderator']);
  });

  it('should build explicit AND group', () => {
    const conditions: WhereCondition[] = [
      { and: [
        { field: 'age', op: '>=', value: 18 },
        { field: 'age', op: '<=', value: 65 }
      ]}
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('("age" >= $1 AND "age" <= $2)');
    expect(result.params).toEqual([18, 65]);
  });

  it('should build nested AND inside OR', () => {
    const conditions: WhereCondition[] = [
      { or: [
        { and: [
          { field: 'status', op: '=', value: 'active' },
          { field: 'age', op: '>', value: 18 }
        ]},
        { field: 'role', op: '=', value: 'admin' }
      ]}
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('(("status" = $1 AND "age" > $2) OR "role" = $3)');
    expect(result.params).toEqual(['active', 18, 'admin']);
  });

  it('should combine top-level condition with OR group', () => {
    const conditions: WhereCondition[] = [
      { field: 'active', op: '=', value: true },
      { or: [
        { field: 'role', op: '=', value: 'admin' },
        { field: 'role', op: '=', value: 'editor' }
      ]}
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"active" = $1 AND ("role" = $2 OR "role" = $3)');
    expect(result.params).toEqual([true, 'admin', 'editor']);
  });
});

describe('buildWhereClause: startParam offset', () => {
  it('should offset param numbering', () => {
    const conditions: WhereCondition[] = [
      { field: 'id', op: '=', value: 5 }
    ];
    const result = buildWhereClause(conditions, 3);
    expect(result.clause).toBe('"id" = $3');
    expect(result.params).toEqual([5]);
  });

  it('should offset with multiple conditions', () => {
    const conditions: WhereCondition[] = [
      { field: 'a', op: '=', value: 1 },
      { field: 'b', op: 'IN', value: [2, 3] }
    ];
    const result = buildWhereClause(conditions, 4);
    expect(result.clause).toBe('"a" = $4 AND "b" IN ($5, $6)');
    expect(result.params).toEqual([1, 2, 3]);
  });
});

describe('buildWhereClause: edge cases', () => {
  it('should return empty clause for empty conditions array', () => {
    const result = buildWhereClause([]);
    expect(result.clause).toBe('');
    expect(result.params).toEqual([]);
  });

  it('should sanitize field names (reject invalid identifiers)', () => {
    const conditions: WhereCondition[] = [
      { field: 'Robert; DROP TABLE users--', op: '=', value: 1 }
    ];
    expect(() => buildWhereClause(conditions)).toThrow();
  });

  it('should handle single condition in AND group', () => {
    const conditions: WhereCondition[] = [
      { and: [{ field: 'x', op: '=', value: 1 }] }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"x" = $1');
    expect(result.params).toEqual([1]);
  });

  it('should handle single condition in OR group', () => {
    const conditions: WhereCondition[] = [
      { or: [{ field: 'x', op: '=', value: 1 }] }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"x" = $1');
    expect(result.params).toEqual([1]);
  });
});
