import { QueryResult, QueryResultRow } from 'pg';

export interface FormattedQueryResult {
  rows: any[];
  rowCount: number;
  fields: Array<{ name: string; dataType: string }>;
  executionTimeMs?: number;
}

export function formatQueryResult(
  result: QueryResult<any>,
  executionTimeMs?: number
): FormattedQueryResult {
  return {
    rows: result.rows,
    rowCount: result.rowCount || 0,
    fields: result.fields.map(field => ({
      name: field.name,
      dataType: getPostgresTypeName(field.dataTypeID)
    })),
    executionTimeMs
  };
}

export function formatError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function getPostgresTypeName(oid: number): string {
  const typeMap: Record<number, string> = {
    16: 'bool',
    20: 'int8',
    21: 'int2',
    23: 'int4',
    25: 'text',
    114: 'json',
    1043: 'varchar',
    1082: 'date',
    1083: 'time',
    1114: 'timestamp',
    1184: 'timestamptz',
    1700: 'numeric',
    2950: 'uuid',
    3802: 'jsonb'
  };

  return typeMap[oid] || `oid_${oid}`;
}

export function truncateText(text: string, maxLength: number = 100): string {
  if (text.length <= maxLength) return text;
  return text.substring(0, maxLength - 3) + '...';
}
