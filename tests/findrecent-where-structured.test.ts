import { describe, it, expect } from 'vitest';
import { temporalTools } from '../src/tools/temporal.js';
import { buildWhereClause, WhereConditionSchema } from '../src/utils/query-builder.js';

const FindRecentSchema = temporalTools.findRecent.schema;

describe('findRecent: structured WHERE replaces raw strings (R3-005/006/022)', () => {
  describe('schema validation', () => {
    it('should accept structured WhereCondition array for where parameter', () => {
      const input = {
        table: 'events',
        timestampColumn: 'created_at',
        timeWindow: '7 days',
        where: [{ field: 'status', op: '=', value: 'active' }]
      };
      const result = FindRecentSchema.safeParse(input);
      expect(result.success).toBe(true);
    });

    it('should reject raw string for where parameter', () => {
      const input = {
        table: 'events',
        timestampColumn: 'created_at',
        timeWindow: '7 days',
        where: "status = 'active'"
      };
      const result = FindRecentSchema.safeParse(input);
      expect(result.success).toBe(false);
    });

    it('should accept omitted where parameter', () => {
      const input = {
        table: 'events',
        timestampColumn: 'created_at',
        timeWindow: '7 days'
      };
      const result = FindRecentSchema.safeParse(input);
      expect(result.success).toBe(true);
    });

    it('should accept complex nested conditions', () => {
      const input = {
        table: 'events',
        timestampColumn: 'created_at',
        timeWindow: '7 days',
        where: [
          { or: [
            { field: 'status', op: '=', value: 'active' },
            { field: 'priority', op: '>=', value: 5 }
          ]}
        ]
      };
      const result = FindRecentSchema.safeParse(input);
      expect(result.success).toBe(true);
    });
  });

  describe('SQL injection prevention via structured WHERE', () => {
    it('should reject blind boolean extraction via structured WHERE (R3-005)', () => {
      // Old attack: where: "ascii(substring(host(inet_server_addr()), 1, 1)) = 49 AND 1=1"
      const result = WhereConditionSchema.safeParse({
        field: 'ascii(substring(host(inet_server_addr()), 1, 1))',
        op: '=',
        value: 49
      });
      expect(result.success).toBe(true);
      // sanitizeIdentifier rejects the malicious field name
      expect(() => buildWhereClause([result.data!])).toThrow();
    });

    it('should reject parenthesis injection via structured WHERE (R3-006)', () => {
      // Old attack: where: "1=1) OR (1=1"
      const result = WhereConditionSchema.safeParse({
        field: '1=1) OR (1=1',
        op: '=',
        value: 1
      });
      expect(result.success).toBe(true);
      // sanitizeIdentifier rejects the malicious field name
      expect(() => buildWhereClause([result.data!])).toThrow();
    });

    it('should parameterize function calls in values (R3-022)', () => {
      // Old attack: where: "name = version()"
      // Now version() is a parameterized literal string, not SQL
      const result = WhereConditionSchema.safeParse({
        field: 'name',
        op: '=',
        value: 'version()'
      });
      expect(result.success).toBe(true);
      const built = buildWhereClause([result.data!]);
      expect(built.clause).toBe('"name" = $1');
      expect(built.params).toEqual(['version()']);
    });

    it('should produce correct parameterization with startParam offset', () => {
      // findRecent uses $1 for LIMIT, so WHERE params start at $2
      const conditions = [
        { field: 'status', op: '=' as const, value: 'active' },
        { field: 'priority', op: '>=' as const, value: 3 }
      ];
      const built = buildWhereClause(conditions, 2);
      expect(built.clause).toBe('"status" = $2 AND "priority" >= $3');
      expect(built.params).toEqual(['active', 3]);
    });
  });
});
