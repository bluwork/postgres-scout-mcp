import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import { zodToJsonSchema } from '../src/utils/zod-to-json-schema.js';
import { WhereConditionSchema } from '../src/utils/query-builder.js';

describe('zodToJsonSchema', () => {
  describe('primitive types', () => {
    it('converts z.string()', () => {
      const schema = z.object({ name: z.string() });
      const result = zodToJsonSchema(schema);
      expect(result.properties.name).toEqual({ type: 'string' });
      expect(result.required).toContain('name');
    });

    it('converts z.number()', () => {
      const schema = z.object({ count: z.number() });
      const result = zodToJsonSchema(schema);
      expect(result.properties.count).toEqual({ type: 'number' });
    });

    it('converts z.boolean()', () => {
      const schema = z.object({ flag: z.boolean() });
      const result = zodToJsonSchema(schema);
      expect(result.properties.flag).toEqual({ type: 'boolean' });
    });
  });

  describe('optional and default', () => {
    it('marks optional fields as not required', () => {
      const schema = z.object({
        name: z.string(),
        nick: z.string().optional()
      });
      const result = zodToJsonSchema(schema);
      expect(result.required).toContain('name');
      expect(result.required).not.toContain('nick');
    });

    it('marks default fields as not required', () => {
      const schema = z.object({
        name: z.string(),
        limit: z.number().optional().default(10)
      });
      const result = zodToJsonSchema(schema);
      expect(result.required).toContain('name');
      expect(result.required).not.toContain('limit');
    });

    it('resolves default values (not functions)', () => {
      const schema = z.object({
        limit: z.number().optional().default(10)
      });
      const result = zodToJsonSchema(schema);
      expect(result.properties.limit.default).toBe(10);
    });
  });

  describe('enums', () => {
    it('converts z.enum()', () => {
      const schema = z.object({
        mode: z.enum(['fast', 'slow'])
      });
      const result = zodToJsonSchema(schema);
      expect(result.properties.mode).toEqual({
        type: 'string',
        enum: ['fast', 'slow']
      });
    });
  });

  describe('arrays', () => {
    it('converts z.array(z.string())', () => {
      const schema = z.object({
        tags: z.array(z.string())
      });
      const result = zodToJsonSchema(schema);
      expect(result.properties.tags).toEqual({
        type: 'array',
        items: { type: 'string' }
      });
    });
  });

  describe('z.lazy (recursive types)', () => {
    it('converts z.lazy wrapping a union', () => {
      const schema = z.object({
        where: z.array(WhereConditionSchema)
      });
      const result = zodToJsonSchema(schema);
      // where should be an array with structured items, NOT { type: 'string' }
      expect(result.properties.where.type).toBe('array');
      expect(result.properties.where.items).toBeDefined();
      expect(result.properties.where.items.type).not.toBe('string');
    });

    it('produces anyOf for WhereConditionSchema union', () => {
      const schema = z.object({
        where: z.array(WhereConditionSchema)
      });
      const result = zodToJsonSchema(schema);
      // The items should have anyOf with the leaf conditions + and/or groups
      expect(result.properties.where.items.anyOf).toBeDefined();
      expect(result.properties.where.items.anyOf.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe('z.union', () => {
    it('converts z.union to anyOf', () => {
      const schema = z.object({
        value: z.union([z.string(), z.number()])
      });
      const result = zodToJsonSchema(schema);
      expect(result.properties.value.anyOf).toEqual([
        { type: 'string' },
        { type: 'number' }
      ]);
    });
  });

  describe('z.object (nested)', () => {
    it('converts nested z.object', () => {
      const schema = z.object({
        filter: z.object({
          field: z.string(),
          op: z.string()
        })
      });
      const result = zodToJsonSchema(schema);
      expect(result.properties.filter.type).toBe('object');
      expect(result.properties.filter.properties.field).toEqual({ type: 'string' });
      expect(result.properties.filter.properties.op).toEqual({ type: 'string' });
    });
  });

  describe('z.record', () => {
    it('converts z.record to object type', () => {
      const schema = z.object({
        set: z.record(z.any())
      });
      const result = zodToJsonSchema(schema);
      expect(result.properties.set.type).toBe('object');
    });
  });

  describe('z.literal', () => {
    it('converts z.literal to const', () => {
      const schema = z.object({
        op: z.literal('BETWEEN')
      });
      const result = zodToJsonSchema(schema);
      expect(result.properties.op.const).toBe('BETWEEN');
    });
  });

  describe('z.tuple', () => {
    it('converts z.tuple to array with prefixItems', () => {
      const schema = z.object({
        range: z.tuple([z.number(), z.number()])
      });
      const result = zodToJsonSchema(schema);
      expect(result.properties.range.type).toBe('array');
      expect(result.properties.range.prefixItems).toEqual([
        { type: 'number' },
        { type: 'number' }
      ]);
    });
  });

  describe('z.refine (ZodEffects)', () => {
    it('unwraps .refine() to the inner type', () => {
      const schema = z.object({
        set: z.record(z.any()).refine(obj => Object.keys(obj).length > 0)
      });
      const result = zodToJsonSchema(schema);
      expect(result.properties.set.type).toBe('object');
    });
  });

  describe('real-world: mutation tool schemas', () => {
    it('PreviewUpdateSchema produces valid where schema', () => {
      const PreviewUpdateSchema = z.object({
        table: z.string(),
        schema: z.string().optional().default('public'),
        where: z.array(WhereConditionSchema),
        limit: z.number().optional().default(5)
      });
      const result = zodToJsonSchema(PreviewUpdateSchema);

      // table is required, schema/limit are not
      expect(result.required).toContain('table');
      expect(result.required).toContain('where');
      expect(result.required).not.toContain('schema');
      expect(result.required).not.toContain('limit');

      // where is array of structured conditions, not strings
      expect(result.properties.where.type).toBe('array');
      expect(result.properties.where.items.anyOf).toBeDefined();
    });

    it('SafeUpdateSchema produces valid set and where schemas', () => {
      const SafeUpdateSchema = z.object({
        table: z.string(),
        schema: z.string().optional().default('public'),
        set: z.record(z.any()).refine(obj => Object.keys(obj).length > 0),
        where: z.array(WhereConditionSchema),
        dryRun: z.boolean().optional().default(false),
        maxRows: z.number().optional().default(1000),
        allowEmptyWhere: z.boolean().optional().default(false)
      });
      const result = zodToJsonSchema(SafeUpdateSchema);

      // set should be object, not string
      expect(result.properties.set.type).toBe('object');
      // where should be array of conditions, not strings
      expect(result.properties.where.items.anyOf).toBeDefined();
    });
  });

  describe('real-world: DetectSeasonalitySchema defaults', () => {
    it('exposes default values as concrete values not functions', () => {
      const DetectSeasonalitySchema = z.object({
        table: z.string(),
        timestampColumn: z.string(),
        valueColumn: z.string(),
        schema: z.string().optional().default('public'),
        groupBy: z.enum(['day_of_week', 'day_of_month', 'month', 'quarter']).optional().default('day_of_week'),
        minPeriods: z.number().optional().default(4)
      });
      const result = zodToJsonSchema(DetectSeasonalitySchema);

      expect(result.properties.schema.default).toBe('public');
      expect(result.properties.groupBy.default).toBe('day_of_week');
      expect(result.properties.minPeriods.default).toBe(4);
      // defaults should be values, not functions
      expect(typeof result.properties.schema.default).not.toBe('function');
      expect(typeof result.properties.groupBy.default).not.toBe('function');
      expect(typeof result.properties.minPeriods.default).not.toBe('function');
    });
  });
});
