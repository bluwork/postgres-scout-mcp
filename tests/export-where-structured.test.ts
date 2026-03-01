import { describe, it, expect } from 'vitest';
import { buildWhereClause, WhereConditionSchema } from '../src/utils/query-builder.js';

describe('Export tools: structured WHERE replaces raw strings (R3-005/006/022)', () => {
  it('should reject blind boolean extraction via structured WHERE (R3-005)', () => {
    // Old attack: where: "ascii(substring(host(inet_server_addr()), 1, 1)) = 49 AND 1=1"
    // With structured WHERE, the field name must pass sanitizeIdentifier
    const result = WhereConditionSchema.safeParse({
      field: 'ascii(substring(host(inet_server_addr()), 1, 1))',
      op: '=',
      value: 49
    });
    expect(result.success).toBe(true); // Zod accepts it...
    // but buildWhereClause will reject it due to sanitizeIdentifier
    expect(() => buildWhereClause([result.data!])).toThrow();
  });

  it('should reject parenthesis injection via structured WHERE (R3-006)', () => {
    // Old attack: where: "1=1) OR (1=1"
    const result = WhereConditionSchema.safeParse({
      field: '1=1) OR (1=1',
      op: '=',
      value: 1
    });
    expect(result.success).toBe(true); // Zod accepts it...
    // but sanitizeIdentifier will reject the field name
    expect(() => buildWhereClause([result.data!])).toThrow();
  });

  it('should reject function call via structured WHERE (R3-022)', () => {
    // Old attack: where: "name = version()"
    // With structured conditions, version() would be a parameterized value, not SQL
    const result = WhereConditionSchema.safeParse({
      field: 'name',
      op: '=',
      value: 'version()'
    });
    expect(result.success).toBe(true);
    // value is parameterized -- treated as literal string "version()", not a function call
    const built = buildWhereClause([result.data!]);
    expect(built.clause).toBe('"name" = $1');
    expect(built.params).toEqual(['version()']);
  });
});
