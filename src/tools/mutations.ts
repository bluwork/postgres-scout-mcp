import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { sanitizeIdentifier, parseIntSafe } from '../utils/sanitize.js';
import { buildWhereClause, WhereCondition, WhereConditionSchema } from '../utils/query-builder.js';

function clampMaxRows(clientMaxRows: number): number {
  const serverMax = parseIntSafe(process.env.MAX_MUTATION_ROWS || '10000', 10000);
  return Math.min(clientMaxRows, serverMax);
}

export const PreviewUpdateSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  where: z.array(WhereConditionSchema),
  limit: z.number().optional().default(5)
});

export const PreviewDeleteSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  where: z.array(WhereConditionSchema),
  limit: z.number().optional().default(5)
});

export const SafeUpdateSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  set: z.record(z.any()),
  where: z.array(WhereConditionSchema),
  dryRun: z.boolean().optional().default(false),
  maxRows: z.number().optional().default(1000),
  allowEmptyWhere: z.boolean().optional().default(false)
});

const SafeInsertSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  columns: z.array(z.string()),
  rows: z.array(z.string()),
  dryRun: z.boolean().optional().default(false),
  maxRows: z.number().optional().default(1000),
  onConflict: z.enum(['error', 'skip']).optional().default('error'),
});

export const SafeDeleteSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  where: z.array(WhereConditionSchema),
  dryRun: z.boolean().optional().default(false),
  maxRows: z.number().optional().default(1000),
  allowEmptyWhere: z.boolean().optional().default(false)
});

function validateWhereClause(where: WhereCondition[], allowEmpty: boolean): { valid: boolean; warning?: string } {
  if (where.length === 0) {
    if (!allowEmpty) {
      return {
        valid: false,
        warning: 'No WHERE conditions provided. This would affect ALL rows. Set allowEmptyWhere=true to proceed.'
      };
    }
    return {
      valid: true,
      warning: 'WARNING: This will affect ALL rows in the table.'
    };
  }
  return { valid: true };
}

function getOperationWarning(count: number, operation: string): string | undefined {
  if (count > 10000) {
    return `CRITICAL: This ${operation} will affect ${count.toLocaleString()} rows. Consider using smaller batches.`;
  }
  if (count > 1000) {
    return `WARNING: This ${operation} will affect ${count.toLocaleString()} rows.`;
  }
  if (count > 100) {
    return `Note: This ${operation} will affect ${count.toLocaleString()} rows.`;
  }
  return undefined;
}

export async function previewUpdate(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof PreviewUpdateSchema>
): Promise<any> {
  const { table, schema, where, limit } = args;

  logger.info('previewUpdate', 'Previewing UPDATE operation', { schema, table });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const validation = validateWhereClause(where, false);
  if (!validation.valid) {
    return {
      blocked: true,
      reason: validation.warning
    };
  }

  const whereResult = buildWhereClause(where);

  const countQuery = `
    SELECT COUNT(*) as count
    FROM ${sanitizedSchema}.${sanitizedTable}
    ${whereResult.clause ? `WHERE ${whereResult.clause}` : ''}
  `;

  const sampleQuery = `
    SELECT *
    FROM ${sanitizedSchema}.${sanitizedTable}
    ${whereResult.clause ? `WHERE ${whereResult.clause}` : ''}
    LIMIT ${limit}
  `;

  const [countResult, sampleResult] = await Promise.all([
    executeQuery(connection, logger, { query: countQuery, params: whereResult.params }),
    executeQuery(connection, logger, { query: sampleQuery, params: whereResult.params })
  ]);

  const affectedCount = parseInt(countResult.rows[0]?.count || '0', 10);
  const warning = getOperationWarning(affectedCount, 'UPDATE');

  return {
    willAffect: affectedCount,
    sampleDocuments: sampleResult.rows,
    samplesShown: sampleResult.rows.length,
    message: warning || (affectedCount <= 10
      ? `Will update ${affectedCount} row${affectedCount !== 1 ? 's' : ''}`
      : undefined),
    filterWarning: validation.warning
  };
}

export async function previewDelete(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof PreviewDeleteSchema>
): Promise<any> {
  const { table, schema, where, limit } = args;

  logger.info('previewDelete', 'Previewing DELETE operation', { schema, table });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const validation = validateWhereClause(where, false);
  if (!validation.valid) {
    return {
      blocked: true,
      reason: validation.warning
    };
  }

  const whereResult = buildWhereClause(where);

  const countQuery = `
    SELECT COUNT(*) as count
    FROM ${sanitizedSchema}.${sanitizedTable}
    ${whereResult.clause ? `WHERE ${whereResult.clause}` : ''}
  `;

  const sampleQuery = `
    SELECT *
    FROM ${sanitizedSchema}.${sanitizedTable}
    ${whereResult.clause ? `WHERE ${whereResult.clause}` : ''}
    LIMIT ${limit}
  `;

  const [countResult, sampleResult] = await Promise.all([
    executeQuery(connection, logger, { query: countQuery, params: whereResult.params }),
    executeQuery(connection, logger, { query: sampleQuery, params: whereResult.params })
  ]);

  const deleteCount = parseInt(countResult.rows[0]?.count || '0', 10);
  const warning = getOperationWarning(deleteCount, 'DELETE');

  return {
    willDelete: deleteCount,
    sampleDocuments: sampleResult.rows,
    samplesShown: sampleResult.rows.length,
    message: warning || (deleteCount <= 10
      ? `Will delete ${deleteCount} row${deleteCount !== 1 ? 's' : ''}`
      : undefined),
    filterWarning: validation.warning
  };
}

export async function safeUpdate(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof SafeUpdateSchema>
): Promise<any> {
  const { table, schema, set, where, dryRun, maxRows: clientMaxRows, allowEmptyWhere } = args;
  const maxRows = clampMaxRows(clientMaxRows);

  logger.info('safeUpdate', 'Executing safe UPDATE', { schema, table, dryRun });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const validation = validateWhereClause(where, allowEmptyWhere);
  if (!validation.valid) {
    return {
      blocked: true,
      reason: validation.warning
    };
  }

  // For count and sample queries, build WHERE with startParam=1 (no SET params)
  const countWhereResult = buildWhereClause(where);

  const countQuery = `
    SELECT COUNT(*) as count
    FROM ${sanitizedSchema}.${sanitizedTable}
    ${countWhereResult.clause ? `WHERE ${countWhereResult.clause}` : ''}
  `;

  const countResult = await executeQuery(connection, logger, { query: countQuery, params: countWhereResult.params });
  const affectedCount = parseInt(countResult.rows[0]?.count || '0', 10);

  if (affectedCount > maxRows) {
    return {
      blocked: true,
      reason: `Operation blocked: Would affect ${affectedCount.toLocaleString()} rows, exceeds maxRows limit of ${maxRows.toLocaleString()}.`,
      suggestion: 'Use previewUpdate() to see affected rows, or increase maxRows limit.'
    };
  }

  if (dryRun) {
    const sampleWhereResult = buildWhereClause(where);
    const sampleQuery = `
      SELECT *
      FROM ${sanitizedSchema}.${sanitizedTable}
      ${sampleWhereResult.clause ? `WHERE ${sampleWhereResult.clause}` : ''}
      LIMIT 5
    `;
    const sampleResult = await executeQuery(connection, logger, { query: sampleQuery, params: sampleWhereResult.params });

    return {
      dryRun: true,
      operation: 'UPDATE',
      table: `${schema}.${table}`,
      wouldAffect: affectedCount,
      sampleRows: sampleResult.rows,
      setClause: set,
      message: getOperationWarning(affectedCount, 'UPDATE'),
      whereWarning: validation.warning
    };
  }

  // Build SET clause with parameterized values
  const setClauses: string[] = [];
  const setParams: any[] = [];
  let paramIndex = 1;

  for (const [column, value] of Object.entries(set)) {
    setClauses.push(`${sanitizeIdentifier(column)} = $${paramIndex}`);
    setParams.push(value);
    paramIndex++;
  }
  const setClause = setClauses.join(', ');

  // Build WHERE with offset after SET params
  const whereResult = buildWhereClause(where, paramIndex);
  const allParams = [...setParams, ...whereResult.params];

  const updateQuery = `
    UPDATE ${sanitizedSchema}.${sanitizedTable}
    SET ${setClause}
    ${whereResult.clause ? `WHERE ${whereResult.clause}` : ''}
  `;

  const result = await executeQuery(connection, logger, { query: updateQuery, params: allParams });

  return {
    success: true,
    operation: 'UPDATE',
    table: `${schema}.${table}`,
    rowsAffected: result.rowCount,
    message: getOperationWarning(result.rowCount || 0, 'UPDATE'),
    whereWarning: validation.warning
  };
}

export async function safeDelete(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof SafeDeleteSchema>
): Promise<any> {
  const { table, schema, where, dryRun, maxRows: clientMaxRows, allowEmptyWhere } = args;
  const maxRows = clampMaxRows(clientMaxRows);

  logger.info('safeDelete', 'Executing safe DELETE', { schema, table, dryRun });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const validation = validateWhereClause(where, allowEmptyWhere);
  if (!validation.valid) {
    return {
      blocked: true,
      reason: validation.warning
    };
  }

  const whereResult = buildWhereClause(where);

  const countQuery = `
    SELECT COUNT(*) as count
    FROM ${sanitizedSchema}.${sanitizedTable}
    ${whereResult.clause ? `WHERE ${whereResult.clause}` : ''}
  `;

  const countResult = await executeQuery(connection, logger, { query: countQuery, params: whereResult.params });
  const deleteCount = parseInt(countResult.rows[0]?.count || '0', 10);

  if (deleteCount > maxRows) {
    return {
      blocked: true,
      reason: `Operation blocked: Would delete ${deleteCount.toLocaleString()} rows, exceeds maxRows limit of ${maxRows.toLocaleString()}.`,
      suggestion: 'Use previewDelete() to see affected rows, or increase maxRows limit.'
    };
  }

  if (dryRun) {
    const sampleWhereResult = buildWhereClause(where);
    const sampleQuery = `
      SELECT *
      FROM ${sanitizedSchema}.${sanitizedTable}
      ${sampleWhereResult.clause ? `WHERE ${sampleWhereResult.clause}` : ''}
      LIMIT 5
    `;
    const sampleResult = await executeQuery(connection, logger, { query: sampleQuery, params: sampleWhereResult.params });

    return {
      dryRun: true,
      operation: 'DELETE',
      table: `${schema}.${table}`,
      wouldDelete: deleteCount,
      sampleRows: sampleResult.rows,
      message: getOperationWarning(deleteCount, 'DELETE'),
      whereWarning: validation.warning
    };
  }

  const deleteWhereResult = buildWhereClause(where);

  const deleteQuery = `
    DELETE FROM ${sanitizedSchema}.${sanitizedTable}
    ${deleteWhereResult.clause ? `WHERE ${deleteWhereResult.clause}` : ''}
  `;

  const result = await executeQuery(connection, logger, { query: deleteQuery, params: deleteWhereResult.params });

  return {
    success: true,
    operation: 'DELETE',
    table: `${schema}.${table}`,
    rowsDeleted: result.rowCount,
    message: getOperationWarning(result.rowCount || 0, 'DELETE'),
    whereWarning: validation.warning
  };
}

const INSERT_BATCH_SIZE = 500;

export async function safeInsert(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof SafeInsertSchema>
): Promise<any> {
  const { table, schema, columns, rows, dryRun, maxRows: clientMaxRows, onConflict } = args;
  const maxRows = clampMaxRows(clientMaxRows);

  logger.info('safeInsert', 'Executing safe INSERT', { schema, table, dryRun });

  // Validation guards
  if (!columns.length) {
    return { blocked: true, reason: 'No columns specified.' };
  }

  if (!rows.length) {
    return { blocked: true, reason: 'No rows provided.' };
  }

  if (rows.length > maxRows) {
    return {
      blocked: true,
      reason: `Row count (${rows.length}) exceeds maxRows limit (${maxRows}).`,
    };
  }

  // Parse and validate each row
  const parsedRows: any[][] = [];
  for (let i = 0; i < rows.length; i++) {
    let parsed: any[];
    try {
      parsed = JSON.parse(rows[i]);
    } catch {
      return { blocked: true, reason: `Invalid row JSON at index ${i}: ${rows[i]}` };
    }
    if (!Array.isArray(parsed)) {
      return { blocked: true, reason: `Invalid row JSON at index ${i}: expected array` };
    }
    if (parsed.length !== columns.length) {
      return {
        blocked: true,
        reason: `Row ${i} has ${parsed.length} values but ${columns.length} columns expected.`,
      };
    }
    parsedRows.push(parsed);
  }

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);
  const sanitizedColumns = columns.map(c => sanitizeIdentifier(c));

  if (dryRun) {
    return {
      dryRun: true,
      operation: 'INSERT',
      table: `${schema}.${table}`,
      wouldInsert: parsedRows.length,
      columns,
      sampleRows: parsedRows.slice(0, 5),
    };
  }

  // Execute in batches
  let totalInserted = 0;
  const allReturnedRows: any[] = [];

  for (let batchStart = 0; batchStart < parsedRows.length; batchStart += INSERT_BATCH_SIZE) {
    const batch = parsedRows.slice(batchStart, batchStart + INSERT_BATCH_SIZE);

    const valuePlaceholders: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    for (const row of batch) {
      const rowPlaceholders: string[] = [];
      for (const value of row) {
        rowPlaceholders.push(`$${paramIndex}`);
        params.push(value);
        paramIndex++;
      }
      valuePlaceholders.push(`(${rowPlaceholders.join(', ')})`);
    }

    let query = `INSERT INTO ${sanitizedSchema}.${sanitizedTable} (${sanitizedColumns.join(', ')}) VALUES ${valuePlaceholders.join(', ')}`;

    if (onConflict === 'skip') {
      query += ' ON CONFLICT DO NOTHING';
    }

    query += ' RETURNING *';

    const result = await executeQuery(connection, logger, { query, params });
    totalInserted += result.rowCount || 0;
    allReturnedRows.push(...result.rows);
  }

  return {
    success: true,
    operation: 'INSERT',
    table: `${schema}.${table}`,
    rowsInserted: totalInserted,
    rows: allReturnedRows,
  };
}

/** @internal Exposed for testing only */
export function _testNormalizeWhereForSafety(where: WhereCondition[]): boolean {
  const validation = validateWhereClause(where, false);
  return !validation.valid;
}

/** @internal Exposed for testing only */
export function _testClampMaxRows(maxRows: number): number {
  return clampMaxRows(maxRows);
}

export const mutationTools = {
  previewUpdate: {
    schema: PreviewUpdateSchema,
    handler: previewUpdate
  },
  previewDelete: {
    schema: PreviewDeleteSchema,
    handler: previewDelete
  },
  safeUpdate: {
    schema: SafeUpdateSchema,
    handler: safeUpdate
  },
  safeDelete: {
    schema: SafeDeleteSchema,
    handler: safeDelete
  },
  safeInsert: {
    schema: SafeInsertSchema,
    handler: safeInsert
  }
};