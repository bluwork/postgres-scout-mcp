import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { sanitizeIdentifier, validateUserWhereClause } from '../utils/sanitize.js';

const PreviewUpdateSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  where: z.string(),
  limit: z.number().optional().default(5)
});

const PreviewDeleteSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  where: z.string(),
  limit: z.number().optional().default(5)
});

const SafeUpdateSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  set: z.union([z.string(), z.record(z.any())]),
  where: z.string(),
  dryRun: z.boolean().optional().default(false),
  maxRows: z.number().optional().default(1000),
  allowEmptyWhere: z.boolean().optional().default(false)
});

const SafeDeleteSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  where: z.string(),
  dryRun: z.boolean().optional().default(false),
  maxRows: z.number().optional().default(1000),
  allowEmptyWhere: z.boolean().optional().default(false)
});

function validateWhereClause(where: string, allowEmpty: boolean): { valid: boolean; warning?: string } {
  const trimmed = where.trim().toLowerCase();

  if (!trimmed || trimmed === '1=1' || trimmed === 'true') {
    if (!allowEmpty) {
      return {
        valid: false,
        warning: `Dangerous WHERE clause detected: "${where}". This would affect ALL rows. Set allowEmptyWhere=true to proceed.`
      };
    }
    return {
      valid: true,
      warning: `WARNING: This will affect ALL rows in the table.`
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

  validateUserWhereClause(where);

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const validation = validateWhereClause(where, false);
  if (!validation.valid) {
    return {
      blocked: true,
      reason: validation.warning
    };
  }

  const countQuery = `
    SELECT COUNT(*) as count
    FROM ${sanitizedSchema}.${sanitizedTable}
    WHERE ${where}
  `;

  const sampleQuery = `
    SELECT *
    FROM ${sanitizedSchema}.${sanitizedTable}
    WHERE ${where}
    LIMIT ${limit}
  `;

  const [countResult, sampleResult] = await Promise.all([
    executeQuery(connection, logger, { query: countQuery, params: [] }),
    executeQuery(connection, logger, { query: sampleQuery, params: [] })
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

  validateUserWhereClause(where);

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const validation = validateWhereClause(where, false);
  if (!validation.valid) {
    return {
      blocked: true,
      reason: validation.warning
    };
  }

  const countQuery = `
    SELECT COUNT(*) as count
    FROM ${sanitizedSchema}.${sanitizedTable}
    WHERE ${where}
  `;

  const sampleQuery = `
    SELECT *
    FROM ${sanitizedSchema}.${sanitizedTable}
    WHERE ${where}
    LIMIT ${limit}
  `;

  const [countResult, sampleResult] = await Promise.all([
    executeQuery(connection, logger, { query: countQuery, params: [] }),
    executeQuery(connection, logger, { query: sampleQuery, params: [] })
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
  const { table, schema, set, where, dryRun, maxRows, allowEmptyWhere } = args;

  logger.info('safeUpdate', 'Executing safe UPDATE', { schema, table, dryRun });

  validateUserWhereClause(where);

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const validation = validateWhereClause(where, allowEmptyWhere);
  if (!validation.valid) {
    return {
      blocked: true,
      reason: validation.warning
    };
  }

  const countQuery = `
    SELECT COUNT(*) as count
    FROM ${sanitizedSchema}.${sanitizedTable}
    WHERE ${where}
  `;

  const countResult = await executeQuery(connection, logger, { query: countQuery, params: [] });
  const affectedCount = parseInt(countResult.rows[0]?.count || '0', 10);

  if (affectedCount > maxRows) {
    return {
      blocked: true,
      reason: `Operation blocked: Would affect ${affectedCount.toLocaleString()} rows, exceeds maxRows limit of ${maxRows.toLocaleString()}.`,
      suggestion: 'Use previewUpdate() to see affected rows, or increase maxRows limit.'
    };
  }

  if (dryRun) {
    const sampleQuery = `
      SELECT *
      FROM ${sanitizedSchema}.${sanitizedTable}
      WHERE ${where}
      LIMIT 5
    `;
    const sampleResult = await executeQuery(connection, logger, { query: sampleQuery, params: [] });

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

  let setClause: string;
  let params: any[] = [];

  if (typeof set === 'string') {
    setClause = set;
  } else {
    const setClauses: string[] = [];
    let paramIndex = 1;

    for (const [column, value] of Object.entries(set)) {
      setClauses.push(`${sanitizeIdentifier(column)} = $${paramIndex}`);
      params.push(value);
      paramIndex++;
    }
    setClause = setClauses.join(', ');
  }

  const updateQuery = `
    UPDATE ${sanitizedSchema}.${sanitizedTable}
    SET ${setClause}
    WHERE ${where}
  `;

  const result = await executeQuery(connection, logger, { query: updateQuery, params });

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
  const { table, schema, where, dryRun, maxRows, allowEmptyWhere } = args;

  logger.info('safeDelete', 'Executing safe DELETE', { schema, table, dryRun });

  validateUserWhereClause(where);

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const validation = validateWhereClause(where, allowEmptyWhere);
  if (!validation.valid) {
    return {
      blocked: true,
      reason: validation.warning
    };
  }

  const countQuery = `
    SELECT COUNT(*) as count
    FROM ${sanitizedSchema}.${sanitizedTable}
    WHERE ${where}
  `;

  const countResult = await executeQuery(connection, logger, { query: countQuery, params: [] });
  const deleteCount = parseInt(countResult.rows[0]?.count || '0', 10);

  if (deleteCount > maxRows) {
    return {
      blocked: true,
      reason: `Operation blocked: Would delete ${deleteCount.toLocaleString()} rows, exceeds maxRows limit of ${maxRows.toLocaleString()}.`,
      suggestion: 'Use previewDelete() to see affected rows, or increase maxRows limit.'
    };
  }

  if (dryRun) {
    const sampleQuery = `
      SELECT *
      FROM ${sanitizedSchema}.${sanitizedTable}
      WHERE ${where}
      LIMIT 5
    `;
    const sampleResult = await executeQuery(connection, logger, { query: sampleQuery, params: [] });

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

  const deleteQuery = `
    DELETE FROM ${sanitizedSchema}.${sanitizedTable}
    WHERE ${where}
  `;

  const result = await executeQuery(connection, logger, { query: deleteQuery, params: [] });

  return {
    success: true,
    operation: 'DELETE',
    table: `${schema}.${table}`,
    rowsDeleted: result.rowCount,
    message: getOperationWarning(result.rowCount || 0, 'DELETE'),
    whereWarning: validation.warning
  };
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
  }
};