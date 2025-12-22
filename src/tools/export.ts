import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { escapeIdentifier, sanitizeIdentifier, validateUserWhereClause } from '../utils/sanitize.js';

const ExportTableSchema = z.object({
  table: z.string(),
  format: z.preprocess(
    (val) => typeof val === 'string' ? val.toLowerCase() : val,
    z.enum(['csv', 'json', 'jsonl', 'sql'])
  ),
  schema: z.string().optional().default('public'),
  where: z.string().optional(),
  columns: z.array(z.string()).optional(),
  limit: z.number().optional().default(10000),
  includeHeaders: z.boolean().optional().default(true)
});

const GenerateInsertStatementsSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  where: z.string().optional(),
  batchSize: z.number().optional().default(100),
  includeSchema: z.boolean().optional().default(true),
  limit: z.number().optional().default(1000)
});

export async function exportTable(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof ExportTableSchema>
): Promise<any> {
  const { table, format, schema, where, columns, limit, includeHeaders } = args;

  logger.info('exportTable', 'Exporting table data', { table, format });

  if (where) {
    validateUserWhereClause(where);
  }

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const columnList = columns && columns.length > 0
    ? columns.map(sanitizeIdentifier).map(escapeIdentifier).join(', ')
    : '*';

  const whereClause = where ? `WHERE ${where}` : '';

  const query = `
    SELECT ${columnList}
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    ${whereClause}
    LIMIT $1
  `;

  const startTime = Date.now();
  const result = await executeQuery(connection, logger, {
    query,
    params: [limit]
  });
  const executionTimeMs = Date.now() - startTime;

  let output = '';
  let preview = '';

  switch (format) {
    case 'csv':
      output = formatAsCSV(result.rows, includeHeaders);
      preview = output.split('\n').slice(0, 5).join('\n');
      break;

    case 'json':
      output = JSON.stringify(result.rows, null, 2);
      preview = output.substring(0, 500);
      break;

    case 'jsonl':
      output = result.rows.map(row => JSON.stringify(row)).join('\n');
      preview = output.split('\n').slice(0, 5).join('\n');
      break;

    case 'sql':
      output = formatAsSQL(sanitizedSchema, sanitizedTable, result.rows);
      preview = output.split('\n').slice(0, 10).join('\n');
      break;

    default:
      throw new Error(`Unsupported format: ${format}`);
  }

  const sizeBytes = Buffer.byteLength(output, 'utf8');
  const sizeMB = (sizeBytes / 1024 / 1024).toFixed(2);

  return {
    table,
    schema,
    format,
    rowsExported: result.rows.length,
    sizeBytes,
    sizeMB,
    executionTimeMs,
    preview: preview + (output.length > preview.length ? '\n...' : ''),
    data: output
  };
}

export async function generateInsertStatements(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof GenerateInsertStatementsSchema>
): Promise<any> {
  const { table, schema, where, batchSize, includeSchema, limit } = args;

  logger.info('generateInsertStatements', 'Generating INSERT statements', { table });

  if (where) {
    validateUserWhereClause(where);
  }

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const columnsQuery = `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = $1
      AND table_name = $2
    ORDER BY ordinal_position
  `;

  const columnsResult = await executeQuery(connection, logger, {
    query: columnsQuery,
    params: [sanitizedSchema, sanitizedTable]
  });

  const columns = columnsResult.rows.map(row => row.column_name);

  const whereClause = where ? `WHERE ${where}` : '';

  const dataQuery = `
    SELECT *
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    ${whereClause}
    LIMIT $1
  `;

  const dataResult = await executeQuery(connection, logger, {
    query: dataQuery,
    params: [limit]
  });

  const statements: string[] = [];
  const batches = Math.ceil(dataResult.rows.length / batchSize);

  const tableName = includeSchema
    ? `${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}`
    : escapeIdentifier(sanitizedTable);

  for (let i = 0; i < batches; i++) {
    const batchStart = i * batchSize;
    const batchEnd = Math.min((i + 1) * batchSize, dataResult.rows.length);
    const batchRows = dataResult.rows.slice(batchStart, batchEnd);

    statements.push(`-- Batch ${i + 1} (${batchRows.length} rows)`);

    const columnNames = columns.map(escapeIdentifier).join(', ');
    statements.push(`INSERT INTO ${tableName} (${columnNames}) VALUES`);

    const valueRows = batchRows.map((row, idx) => {
      const values = columns.map(col => formatValue(row[col]));
      const isLast = idx === batchRows.length - 1;
      return `  (${values.join(', ')})${isLast ? ';' : ','}`;
    });

    statements.push(...valueRows);
    statements.push('');
  }

  return {
    table,
    schema,
    rowCount: dataResult.rows.length,
    batchCount: batches,
    batchSize,
    statements: statements.join('\n')
  };
}

function formatAsCSV(rows: any[], includeHeaders: boolean): string {
  if (rows.length === 0) return '';

  const columns = Object.keys(rows[0]);
  const lines: string[] = [];

  function escapeCsvValue(value: string): string {
    const trimmed = value.trimStart();
    const needsFormulaEscape = /^[=+\-@]/.test(trimmed);
    const safeValue = needsFormulaEscape ? `'${value}` : value;
    if (safeValue.includes(',') || safeValue.includes('"') || safeValue.includes('\n')) {
      return `"${safeValue.replace(/"/g, '""')}"`;
    }
    return safeValue;
  }

  if (includeHeaders) {
    lines.push(columns.join(','));
  }

  for (const row of rows) {
    const values = columns.map(col => {
      const value = row[col];
      if (value === null || value === undefined) return '';
      const str = String(value);
      return escapeCsvValue(str);
    });
    lines.push(values.join(','));
  }

  return lines.join('\n');
}

function formatAsSQL(schema: string, table: string, rows: any[]): string {
  if (rows.length === 0) return '';

  const columns = Object.keys(rows[0]);
  const tableName = `${escapeIdentifier(schema)}.${escapeIdentifier(table)}`;
  const columnNames = columns.map(escapeIdentifier).join(', ');

  const statements: string[] = [];
  statements.push(`-- INSERT statements for ${tableName}`);
  statements.push(`INSERT INTO ${tableName} (${columnNames}) VALUES`);

  const valueRows = rows.map((row, idx) => {
    const values = columns.map(col => formatValue(row[col]));
    const isLast = idx === rows.length - 1;
    return `  (${values.join(', ')})${isLast ? ';' : ','}`;
  });

  statements.push(...valueRows);

  return statements.join('\n');
}

function formatValue(value: any): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }

  if (typeof value === 'number') {
    return String(value);
  }

  if (typeof value === 'boolean') {
    return value ? 'TRUE' : 'FALSE';
  }

  if (value instanceof Date) {
    return `'${value.toISOString()}'`;
  }

  if (typeof value === 'object') {
    return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
  }

  const str = String(value);
  return `'${str.replace(/'/g, "''")}'`;
}

export const exportTools = {
  exportTable: {
    schema: ExportTableSchema,
    handler: exportTable
  },
  generateInsertStatements: {
    schema: GenerateInsertStatementsSchema,
    handler: generateInsertStatements
  }
};
