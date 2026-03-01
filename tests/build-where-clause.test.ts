import { describe, it, expect } from 'vitest';
import { buildWhereClause, WhereCondition, WhereConditionSchema } from '../src/utils/query-builder.js';

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

  it('should build IS NOT NULL condition (with additional condition)', () => {
    const conditions: WhereCondition[] = [
      { field: 'name', op: 'IS NOT NULL' },
      { field: 'status', op: '=', value: 'active' }
    ];
    const result = buildWhereClause(conditions);
    expect(result.clause).toBe('"name" IS NOT NULL AND "status" = $1');
    expect(result.params).toEqual(['active']);
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

describe('WhereConditionSchema: Zod validation', () => {
  it('should accept a valid comparison condition', () => {
    const result = WhereConditionSchema.safeParse({ field: 'id', op: '=', value: 1 });
    expect(result.success).toBe(true);
  });

  it('should accept a valid IN condition', () => {
    const result = WhereConditionSchema.safeParse({ field: 'status', op: 'IN', value: ['a', 'b'] });
    expect(result.success).toBe(true);
  });

  it('should accept IS NULL condition', () => {
    const result = WhereConditionSchema.safeParse({ field: 'deleted_at', op: 'IS NULL' });
    expect(result.success).toBe(true);
  });

  it('should accept BETWEEN condition', () => {
    const result = WhereConditionSchema.safeParse({ field: 'price', op: 'BETWEEN', value: [10, 50] });
    expect(result.success).toBe(true);
  });

  it('should accept nested AND/OR', () => {
    const result = WhereConditionSchema.safeParse({
      or: [
        { field: 'a', op: '=', value: 1 },
        { and: [
          { field: 'b', op: '>', value: 2 },
          { field: 'c', op: 'IS NULL' }
        ]}
      ]
    });
    expect(result.success).toBe(true);
  });

  it('should reject invalid operator', () => {
    const result = WhereConditionSchema.safeParse({ field: 'id', op: 'DROP', value: 1 });
    expect(result.success).toBe(false);
  });

  it('should reject missing field', () => {
    const result = WhereConditionSchema.safeParse({ op: '=', value: 1 });
    expect(result.success).toBe(false);
  });

  it('should reject IN with non-array value', () => {
    const result = WhereConditionSchema.safeParse({ field: 'id', op: 'IN', value: 'not-array' });
    expect(result.success).toBe(false);
  });

  it('should reject BETWEEN with wrong-length array', () => {
    const result = WhereConditionSchema.safeParse({ field: 'id', op: 'BETWEEN', value: [1] });
    expect(result.success).toBe(false);
  });

  it('should reject empty object', () => {
    const result = WhereConditionSchema.safeParse({});
    expect(result.success).toBe(false);
  });

  it('should reject mixed leaf and group keys', () => {
    const result = WhereConditionSchema.safeParse({
      field: 'id', op: '=', value: 1, and: [{ field: 'x', op: '=', value: 2 }]
    });
    expect(result.success).toBe(false);
  });

  it('should reject group with extra keys', () => {
    const result = WhereConditionSchema.safeParse({
      or: [{ field: 'id', op: '=', value: 1 }], field: 'extra'
    });
    expect(result.success).toBe(false);
  });
});

describe('buildWhereClause: trivially-true condition detection (R3-019/020/021)', () => {
  // R3-019: LIKE '%' matches all
  it('should reject LIKE with % wildcard only', () => {
    const conditions: WhereCondition[] = [
      { field: 'name', op: 'LIKE', value: '%' }
    ];
    expect(() => buildWhereClause(conditions)).toThrow(/trivially true/i);
  });

  it('should reject LIKE with %% wildcard only', () => {
    const conditions: WhereCondition[] = [
      { field: 'name', op: 'LIKE', value: '%%' }
    ];
    expect(() => buildWhereClause(conditions)).toThrow(/trivially true/i);
  });

  it('should reject ILIKE with % wildcard only', () => {
    const conditions: WhereCondition[] = [
      { field: 'name', op: 'ILIKE', value: '%' }
    ];
    expect(() => buildWhereClause(conditions)).toThrow(/trivially true/i);
  });

  it('should allow LIKE with actual pattern', () => {
    const conditions: WhereCondition[] = [
      { field: 'name', op: 'LIKE', value: '%john%' }
    ];
    expect(() => buildWhereClause(conditions)).not.toThrow();
  });

  it('should allow LIKE with prefix pattern', () => {
    const conditions: WhereCondition[] = [
      { field: 'name', op: 'LIKE', value: 'john%' }
    ];
    expect(() => buildWhereClause(conditions)).not.toThrow();
  });

  // R3-020: IS NOT NULL as sole condition
  it('should reject IS NOT NULL as sole condition', () => {
    const conditions: WhereCondition[] = [
      { field: 'id', op: 'IS NOT NULL' }
    ];
    expect(() => buildWhereClause(conditions)).toThrow(/trivially true/i);
  });

  it('should allow IS NOT NULL combined with other conditions', () => {
    const conditions: WhereCondition[] = [
      { field: 'id', op: 'IS NOT NULL' },
      { field: 'status', op: '=', value: 'active' }
    ];
    expect(() => buildWhereClause(conditions)).not.toThrow();
  });

  it('should allow IS NULL as sole condition (not trivially true)', () => {
    const conditions: WhereCondition[] = [
      { field: 'deleted_at', op: 'IS NULL' }
    ];
    expect(() => buildWhereClause(conditions)).not.toThrow();
  });

  // R3-021: BETWEEN with extreme ranges
  it('should reject BETWEEN with int4 full range', () => {
    const conditions: WhereCondition[] = [
      { field: 'id', op: 'BETWEEN', value: [-2147483648, 2147483647] }
    ];
    expect(() => buildWhereClause(conditions)).toThrow(/trivially true/i);
  });

  it('should reject BETWEEN with very large numeric range', () => {
    const conditions: WhereCondition[] = [
      { field: 'id', op: 'BETWEEN', value: [-9999999999, 9999999999] }
    ];
    expect(() => buildWhereClause(conditions)).toThrow(/trivially true/i);
  });

  it('should allow BETWEEN with reasonable numeric range', () => {
    const conditions: WhereCondition[] = [
      { field: 'price', op: 'BETWEEN', value: [10, 50] }
    ];
    expect(() => buildWhereClause(conditions)).not.toThrow();
  });

  it('should allow BETWEEN with date strings (not numeric)', () => {
    const conditions: WhereCondition[] = [
      { field: 'created_at', op: 'BETWEEN', value: ['2024-01-01', '2024-12-31'] }
    ];
    expect(() => buildWhereClause(conditions)).not.toThrow();
  });

  // Sole IS NOT NULL nested in AND group should also be caught
  it('should reject sole IS NOT NULL inside AND group', () => {
    const conditions: WhereCondition[] = [
      { and: [{ field: 'id', op: 'IS NOT NULL' }] }
    ];
    expect(() => buildWhereClause(conditions)).toThrow(/trivially true/i);
  });

  // Recursive detection: trivially-true inside nested OR/AND groups
  it('should reject LIKE % nested inside OR group', () => {
    const conditions: WhereCondition[] = [
      { or: [
        { field: 'name', op: 'LIKE', value: '%' },
        { field: 'status', op: '=', value: 'active' }
      ]}
    ];
    expect(() => buildWhereClause(conditions)).toThrow(/trivially true/i);
  });

  it('should reject extreme BETWEEN nested inside AND group', () => {
    const conditions: WhereCondition[] = [
      { and: [
        { field: 'id', op: 'BETWEEN', value: [-2147483648, 2147483647] },
        { field: 'status', op: '=', value: 'active' }
      ]}
    ];
    expect(() => buildWhereClause(conditions)).toThrow(/trivially true/i);
  });

  it('should reject ILIKE % deeply nested in OR inside AND', () => {
    const conditions: WhereCondition[] = [
      { and: [
        { or: [
          { field: 'name', op: 'ILIKE', value: '%%' },
          { field: 'role', op: '=', value: 'admin' }
        ]},
        { field: 'active', op: '=', value: true }
      ]}
    ];
    expect(() => buildWhereClause(conditions)).toThrow(/trivially true/i);
  });
});
