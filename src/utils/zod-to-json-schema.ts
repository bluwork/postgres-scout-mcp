/**
 * Converts a Zod schema to a JSON Schema object suitable for MCP tool inputSchema.
 */
export function zodToJsonSchema(schema: any): any {
  return convertZodObject(schema, new Set());
}

function convertZodObject(schema: any, seen: Set<any>): any {
  const shape = schema._def?.shape?.() || {};
  const properties: Record<string, any> = {};
  const required: string[] = [];

  for (const [key, value] of Object.entries(shape)) {
    const field = value as any;
    properties[key] = convertZodType(field, seen);

    if (isRequired(field)) {
      required.push(key);
    }
  }

  return {
    type: 'object',
    properties,
    required: required.length > 0 ? required : undefined
  };
}

function isRequired(field: any): boolean {
  const typeName = field._def?.typeName;
  if (typeName === 'ZodOptional' || typeName === 'ZodDefault') return false;
  return true;
}

function convertZodType(zodType: any, seen: Set<any>): any {
  const typeName = zodType._def?.typeName;

  switch (typeName) {
    case 'ZodString':
      return { type: 'string' };
    case 'ZodNumber':
      return { type: 'number' };
    case 'ZodBoolean':
      return { type: 'boolean' };
    case 'ZodArray':
      return {
        type: 'array',
        items: convertZodType(zodType._def?.type, seen)
      };
    case 'ZodEnum':
      return { type: 'string', enum: zodType._def?.values || [] };
    case 'ZodOptional':
      return convertZodType(zodType._def?.innerType, seen);
    case 'ZodDefault': {
      const inner = convertZodType(zodType._def?.innerType, seen);
      const defaultValue = zodType._def?.defaultValue;
      inner.default = typeof defaultValue === 'function' ? defaultValue() : defaultValue;
      return inner;
    }
    case 'ZodLazy': {
      if (seen.has(zodType)) return {};
      seen.add(zodType);
      return convertZodType(zodType._def.getter(), seen);
    }
    case 'ZodUnion':
      return {
        anyOf: zodType._def.options.map((o: any) => convertZodType(o, seen))
      };
    case 'ZodObject':
      return convertZodObject(zodType, seen);
    case 'ZodRecord':
      return { type: 'object' };
    case 'ZodLiteral': {
      const val = zodType._def?.value;
      return { type: typeof val, const: val };
    }
    case 'ZodTuple':
      return {
        type: 'array',
        prefixItems: zodType._def?.items?.map((i: any) => convertZodType(i, seen))
      };
    case 'ZodEffects':
      return convertZodType(zodType._def?.schema, seen);
    case 'ZodAny':
      return {};
    default:
      return { type: 'string' };
  }
}
