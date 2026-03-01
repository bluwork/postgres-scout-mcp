import { describe, it, expect } from 'vitest';

import { PreviewUpdateSchema, PreviewDeleteSchema, SafeUpdateSchema, SafeDeleteSchema } from '../../src/tools/mutations.js';

describe('Mutation schemas accept structured WhereCondition', () => {
  it('PreviewUpdateSchema should accept structured where', () => {
    const result = PreviewUpdateSchema.safeParse({
      table: 'users',
      where: [{ field: 'id', op: '=', value: 1 }]
    });
    expect(result.success).toBe(true);
  });

  it('PreviewUpdateSchema should reject raw WHERE string', () => {
    const result = PreviewUpdateSchema.safeParse({
      table: 'users',
      where: "id = 1"
    });
    expect(result.success).toBe(false);
  });

  it('PreviewDeleteSchema should accept structured where', () => {
    const result = PreviewDeleteSchema.safeParse({
      table: 'users',
      where: [{ field: 'status', op: '=', value: 'inactive' }]
    });
    expect(result.success).toBe(true);
  });

  it('SafeUpdateSchema should accept structured where and object set', () => {
    const result = SafeUpdateSchema.safeParse({
      table: 'users',
      set: { name: 'Alice' },
      where: [{ field: 'id', op: '=', value: 1 }]
    });
    expect(result.success).toBe(true);
  });

  it('SafeUpdateSchema should reject string set', () => {
    const result = SafeUpdateSchema.safeParse({
      table: 'users',
      set: "name = 'injected'",
      where: [{ field: 'id', op: '=', value: 1 }]
    });
    expect(result.success).toBe(false);
  });

  it('SafeDeleteSchema should accept structured where', () => {
    const result = SafeDeleteSchema.safeParse({
      table: 'users',
      where: [{ field: 'id', op: '=', value: 1 }]
    });
    expect(result.success).toBe(true);
  });

  it('SafeDeleteSchema should accept empty where array with allowEmptyWhere', () => {
    const result = SafeDeleteSchema.safeParse({
      table: 'users',
      where: [],
      allowEmptyWhere: true
    });
    expect(result.success).toBe(true);
  });

  it('SafeUpdateSchema should reject empty set object', () => {
    const result = SafeUpdateSchema.safeParse({
      table: 'users',
      set: {},
      where: [{ field: 'id', op: '=', value: 1 }]
    });
    expect(result.success).toBe(false);
  });
});
