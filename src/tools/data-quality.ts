import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { escapeIdentifier, sanitizeIdentifier, validateCondition } from '../utils/sanitize.js';

const FindDuplicatesSchema = z.object({
  table: z.string(),
  columns: z.array(z.string()),
  schema: z.string().optional().default('public'),
  limit: z.number().optional().default(100),
  minCount: z.number().optional().default(2),
  includeRows: z.boolean().optional().default(true)
});

const FindMissingValuesSchema = z.object({
  table: z.string(),
  columns: z.array(z.string()),
  schema: z.string().optional().default('public'),
  includeRows: z.boolean().optional().default(true),
  limit: z.number().optional().default(100)
});

const FindOrphansSchema = z.object({
  table: z.string(),
  foreignKey: z.string(),
  referenceTable: z.string(),
  referenceColumn: z.string(),
  schema: z.string().optional().default('public'),
  referenceSchema: z.string().optional().default('public'),
  limit: z.number().optional().default(100)
});

const CheckConstraintViolationsSchema = z.object({
  table: z.string(),
  condition: z.string().describe('SQL boolean expression to check, e.g., "email IS NOT NULL"'),
  constraintName: z.string().optional().describe('Name for the constraint'),
  schema: z.string().optional().default('public')
});

const AnalyzeTypeConsistencySchema = z.object({
  table: z.string(),
  column: z.string(),
  schema: z.string().optional().default('public'),
  suggestConversion: z.boolean().optional().default(true),
  sampleSize: z.number().optional().default(10000)
});

export async function findDuplicates(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof FindDuplicatesSchema>
): Promise<any> {
  const { table, columns, schema, limit, minCount, includeRows } = args;

  logger.info('findDuplicates', 'Finding duplicate rows', { table, columns });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);
  const sanitizedColumns = columns.map(sanitizeIdentifier);

  const columnList = sanitizedColumns.map(escapeIdentifier).join(', ');
  const groupByList = sanitizedColumns.map((col, idx) => `${idx + 1}`).join(', ');

  const countQuery = `
    SELECT COUNT(*) as total_rows
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
  `;

  const duplicatesQuery = `
    SELECT
      ${columnList},
      COUNT(*) as count
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    GROUP BY ${columnList}
    HAVING COUNT(*) >= $1
    ORDER BY COUNT(*) DESC
    LIMIT $2
  `;

  const [totalResult, duplicatesResult] = await Promise.all([
    executeQuery(connection, logger, { query: countQuery }),
    executeQuery(connection, logger, {
      query: duplicatesQuery,
      params: [minCount, limit]
    })
  ]);

  const totalRows = parseInt(totalResult.rows[0]?.total_rows || '0', 10);
  const duplicateGroups = duplicatesResult.rows;

  let duplicateGroupsWithRows = duplicateGroups;

  if (includeRows && duplicateGroups.length > 0) {
    duplicateGroupsWithRows = await Promise.all(
      duplicateGroups.map(async (group) => {
        const whereConditions = sanitizedColumns.map((col, idx) => {
          return `${escapeIdentifier(col)} = $${idx + 1}`;
        }).join(' AND ');

        const rowsQuery = `
          SELECT *
          FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
          WHERE ${whereConditions}
          LIMIT 10
        `;

        const params = sanitizedColumns.map(col => group[col]);
        const rowsResult = await executeQuery(connection, logger, {
          query: rowsQuery,
          params
        });

        return {
          ...group,
          rows: rowsResult.rows
        };
      })
    );
  }

  const totalDuplicateRows = duplicateGroups.reduce(
    (sum, group) => sum + parseInt(group.count, 10),
    0
  );

  const recommendations: string[] = [];

  if (duplicateGroups.length > 0) {
    recommendations.push(
      `Found ${totalDuplicateRows} duplicate rows across ${duplicateGroups.length} groups`
    );
    recommendations.push(
      `Consider adding UNIQUE constraint: ALTER TABLE ${schema}.${table} ADD CONSTRAINT ${table}_${columns.join('_')}_unique UNIQUE (${columns.join(', ')})`
    );
    recommendations.push(
      'Review and delete duplicates, keeping the most recent or earliest record'
    );
  } else {
    recommendations.push('✓ No duplicates found');
  }

  return {
    table,
    schema,
    columns,
    totalDuplicateGroups: duplicateGroups.length,
    affectedRows: totalDuplicateRows,
    statistics: {
      totalRows,
      uniqueRows: totalRows - totalDuplicateRows,
      duplicateRows: totalDuplicateRows,
      duplicatePercentage: totalRows > 0 ? ((totalDuplicateRows / totalRows) * 100).toFixed(2) : '0'
    },
    duplicateGroups: duplicateGroupsWithRows.slice(0, limit),
    recommendations
  };
}

export async function findMissingValues(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof FindMissingValuesSchema>
): Promise<any> {
  const { table, columns, schema, includeRows, limit } = args;

  logger.info('findMissingValues', 'Finding NULL values', { table, columns });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);
  const sanitizedColumns = columns.map(sanitizeIdentifier);

  const countQuery = `
    SELECT COUNT(*) as total_rows
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
  `;

  const totalResult = await executeQuery(connection, logger, { query: countQuery });
  const totalRows = parseInt(totalResult.rows[0]?.total_rows || '0', 10);

  const analysis: Record<string, any> = {};

  for (const column of sanitizedColumns) {
    const nullCountQuery = `
      SELECT
        COUNT(*) FILTER (WHERE ${escapeIdentifier(column)} IS NULL) as null_count
      FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    `;

    const nullResult = await executeQuery(connection, logger, { query: nullCountQuery });
    const nullCount = parseInt(nullResult.rows[0]?.null_count || '0', 10);
    const nullPercentage = totalRows > 0 ? ((nullCount / totalRows) * 100).toFixed(2) : '0';

    let recommendation = '';
    let sampleRows = [];

    if (nullCount === 0) {
      recommendation = '✓ No NULL values';
    } else {
      const percentage = parseFloat(nullPercentage);
      if (percentage < 1) {
        recommendation = `${nullCount} rows with NULL ${column} - minor issue`;
      } else if (percentage < 5) {
        recommendation = `⚠ ${percentage}% of rows missing ${column} - investigate`;
      } else {
        recommendation = `⚠ ${percentage}% of rows missing ${column} - set default or make required`;
      }

      if (includeRows && nullCount > 0) {
        const sampleQuery = `
          SELECT *
          FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
          WHERE ${escapeIdentifier(column)} IS NULL
          LIMIT $1
        `;
        const sampleResult = await executeQuery(connection, logger, {
          query: sampleQuery,
          params: [limit]
        });
        sampleRows = sampleResult.rows;
      }
    }

    analysis[column] = {
      nullCount,
      nullPercentage: parseFloat(nullPercentage),
      recommendation,
      ...(sampleRows.length > 0 && { sampleRows })
    };
  }

  const recommendations: string[] = [];
  for (const [column, data] of Object.entries(analysis)) {
    if (data.nullCount === 0) {
      recommendations.push(`Consider adding NOT NULL constraint to ${column}`);
    } else if (data.nullPercentage > 5) {
      recommendations.push(`High NULL rate in ${column} (${data.nullPercentage}%) - investigate data quality`);
    }
  }

  return {
    table,
    schema,
    totalRows,
    analysis,
    recommendations
  };
}

export async function findOrphans(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof FindOrphansSchema>
): Promise<any> {
  const { table, foreignKey, referenceTable, referenceColumn, schema, referenceSchema, limit } = args;

  logger.info('findOrphans', 'Finding orphaned records', { table, foreignKey, referenceTable });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);
  const sanitizedForeignKey = sanitizeIdentifier(foreignKey);
  const sanitizedRefSchema = sanitizeIdentifier(referenceSchema);
  const sanitizedRefTable = sanitizeIdentifier(referenceTable);
  const sanitizedRefColumn = sanitizeIdentifier(referenceColumn);

  const orphansQuery = `
    SELECT t.*
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)} t
    LEFT JOIN ${escapeIdentifier(sanitizedRefSchema)}.${escapeIdentifier(sanitizedRefTable)} r
      ON t.${escapeIdentifier(sanitizedForeignKey)} = r.${escapeIdentifier(sanitizedRefColumn)}
    WHERE t.${escapeIdentifier(sanitizedForeignKey)} IS NOT NULL
      AND r.${escapeIdentifier(sanitizedRefColumn)} IS NULL
    LIMIT $1
  `;

  const countQuery = `
    SELECT COUNT(*) as orphan_count
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)} t
    LEFT JOIN ${escapeIdentifier(sanitizedRefSchema)}.${escapeIdentifier(sanitizedRefTable)} r
      ON t.${escapeIdentifier(sanitizedForeignKey)} = r.${escapeIdentifier(sanitizedRefColumn)}
    WHERE t.${escapeIdentifier(sanitizedForeignKey)} IS NOT NULL
      AND r.${escapeIdentifier(sanitizedRefColumn)} IS NULL
  `;

  const totalQuery = `
    SELECT COUNT(*) as total_count
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
  `;

  const [orphansResult, countResult, totalResult] = await Promise.all([
    executeQuery(connection, logger, { query: orphansQuery, params: [limit] }),
    executeQuery(connection, logger, { query: countQuery }),
    executeQuery(connection, logger, { query: totalQuery })
  ]);

  const orphanCount = parseInt(countResult.rows[0]?.orphan_count || '0', 10);
  const totalCount = parseInt(totalResult.rows[0]?.total_count || '0', 10);
  const orphanPercentage = totalCount > 0 ? ((orphanCount / totalCount) * 100).toFixed(2) : '0';

  const recommendations: string[] = [];

  if (orphanCount > 0) {
    recommendations.push(`Found ${orphanCount} orphaned records (${orphanPercentage}% of total)`);
    recommendations.push(
      `Delete orphaned records: DELETE FROM ${schema}.${table} WHERE ${foreignKey} NOT IN (SELECT ${referenceColumn} FROM ${referenceSchema}.${referenceTable})`
    );
    recommendations.push(
      `Or set to NULL: UPDATE ${schema}.${table} SET ${foreignKey} = NULL WHERE ${foreignKey} NOT IN (SELECT ${referenceColumn} FROM ${referenceSchema}.${referenceTable})`
    );
    recommendations.push(
      `After cleanup, add FK constraint: ALTER TABLE ${schema}.${table} ADD CONSTRAINT ${table}_${foreignKey}_fkey FOREIGN KEY (${foreignKey}) REFERENCES ${referenceSchema}.${referenceTable}(${referenceColumn})`
    );
  } else {
    recommendations.push('✓ No orphaned records found');
    recommendations.push(
      `Safe to add FK constraint: ALTER TABLE ${schema}.${table} ADD CONSTRAINT ${table}_${foreignKey}_fkey FOREIGN KEY (${foreignKey}) REFERENCES ${referenceSchema}.${referenceTable}(${referenceColumn})`
    );
  }

  return {
    table,
    schema,
    foreignKey,
    referenceTable,
    referenceSchema,
    referenceColumn,
    orphanCount,
    totalCount,
    orphanPercentage: parseFloat(orphanPercentage),
    orphanedRows: orphansResult.rows,
    recommendations
  };
}

export async function checkConstraintViolations(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof CheckConstraintViolationsSchema>
): Promise<any> {
  const { table, condition, constraintName, schema } = args;

  logger.info('checkConstraintViolations', 'Checking constraint violations', { table, condition });

  validateCondition(condition);

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);
  const name = constraintName || `${table}_check`;

  const violationsQuery = `
    SELECT *
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    WHERE NOT (${condition})
    LIMIT 100
  `;

  const countQuery = `
    SELECT COUNT(*) as violation_count
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    WHERE NOT (${condition})
  `;

  const [violationsResult, countResult] = await Promise.all([
    executeQuery(connection, logger, { query: violationsQuery }),
    executeQuery(connection, logger, { query: countQuery })
  ]);

  const violationCount = parseInt(countResult.rows[0]?.violation_count || '0', 10);

  const recommendations: string[] = [];

  if (violationCount > 0) {
    recommendations.push(`⚠ ${violationCount} rows would violate CHECK constraint`);
    recommendations.push('Fix violations before adding constraint');
    recommendations.push(`Example: UPDATE ${schema}.${table} SET ... WHERE NOT (${condition})`);
  } else {
    recommendations.push('✓ No violations found - safe to add constraint');
    recommendations.push(
      `ALTER TABLE ${schema}.${table} ADD CONSTRAINT ${name} CHECK (${condition})`
    );
  }

  return {
    table,
    schema,
    constraint: name,
    condition,
    violationCount,
    violations: violationsResult.rows.slice(0, 20),
    recommendations
  };
}

export async function analyzeTypeConsistency(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof AnalyzeTypeConsistencySchema>
): Promise<any> {
  const { table, column, schema, suggestConversion, sampleSize } = args;

  logger.info('analyzeTypeConsistency', 'Analyzing type consistency', { table, column });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);
  const sanitizedColumn = sanitizeIdentifier(column);

  const typeQuery = `
    SELECT
      data_type as current_type
    FROM information_schema.columns
    WHERE table_schema = $1
      AND table_name = $2
      AND column_name = $3
  `;

  const typeResult = await executeQuery(connection, logger, {
    query: typeQuery,
    params: [sanitizedSchema, sanitizedTable, sanitizedColumn]
  });

  const currentType = typeResult.rows[0]?.current_type || 'unknown';

  const analysisQuery = `
    SELECT
      COUNT(*) as total_rows,
      COUNT(*) FILTER (WHERE ${escapeIdentifier(sanitizedColumn)} IS NULL) as null_count,
      COUNT(*) FILTER (WHERE ${escapeIdentifier(sanitizedColumn)} ~ '^[0-9]+$') as integer_count,
      COUNT(*) FILTER (WHERE ${escapeIdentifier(sanitizedColumn)} ~ '^[0-9]+\\.[0-9]+$') as decimal_count,
      COUNT(*) FILTER (WHERE ${escapeIdentifier(sanitizedColumn)} ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}') as date_count,
      COUNT(*) FILTER (WHERE ${escapeIdentifier(sanitizedColumn)} ~ '^(true|false|t|f|yes|no|y|n|1|0)$') as boolean_count
    FROM (
      SELECT ${escapeIdentifier(sanitizedColumn)}
      FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
      LIMIT $1
    ) sample
  `;

  const analysisResult = await executeQuery(connection, logger, {
    query: analysisQuery,
    params: [sampleSize]
  });

  const stats = analysisResult.rows[0];
  const totalRows = parseInt(stats.total_rows, 10);
  const nullCount = parseInt(stats.null_count, 10);
  const numericCount = parseInt(stats.integer_count, 10) + parseInt(stats.decimal_count, 10);
  const dateCount = parseInt(stats.date_count, 10);
  const booleanCount = parseInt(stats.boolean_count, 10);

  const invalidCount = totalRows - nullCount - numericCount - dateCount - booleanCount;

  const patterns: Record<string, any> = {
    numeric: {
      count: numericCount,
      percentage: ((numericCount / totalRows) * 100).toFixed(1)
    },
    date: {
      count: dateCount,
      percentage: ((dateCount / totalRows) * 100).toFixed(1)
    },
    boolean: {
      count: booleanCount,
      percentage: ((booleanCount / totalRows) * 100).toFixed(1)
    },
    null: {
      count: nullCount,
      percentage: ((nullCount / totalRows) * 100).toFixed(1)
    },
    invalid: {
      count: invalidCount,
      percentage: ((invalidCount / totalRows) * 100).toFixed(1)
    }
  };

  const recommendations: string[] = [];
  let suggestedMigration = null;

  if (numericCount / totalRows > 0.95) {
    recommendations.push(`✓ ${patterns.numeric.percentage}% of values are numeric`);
    if (invalidCount > 0) {
      recommendations.push(`${invalidCount} rows contain non-numeric values - clean up first`);
    }

    if (suggestConversion) {
      const targetType = parseInt(stats.decimal_count, 10) > 0 ? 'numeric(10,2)' : 'integer';
      recommendations.push(
        `Consider converting to ${targetType}: ALTER TABLE ${schema}.${table} ALTER COLUMN ${column} TYPE ${targetType} USING ${column}::${targetType}`
      );

      suggestedMigration = {
        targetType,
        needsCleanup: invalidCount > 0,
        cleanupQuery: `UPDATE ${schema}.${table} SET ${column} = NULL WHERE ${column} !~ '^[0-9.]+$'`,
        conversionQuery: `ALTER TABLE ${schema}.${table} ALTER COLUMN ${column} TYPE ${targetType} USING ${column}::${targetType}`
      };
    }
  } else if (dateCount / totalRows > 0.95) {
    recommendations.push(`✓ ${patterns.date.percentage}% of values are date-like`);
    recommendations.push(`Consider converting to DATE or TIMESTAMP`);
  } else if (booleanCount / totalRows > 0.95) {
    recommendations.push(`✓ ${patterns.boolean.percentage}% of values are boolean-like`);
    recommendations.push(`Consider converting to BOOLEAN`);
  } else {
    recommendations.push('⚠ Data has mixed types - not suitable for type conversion');
  }

  return {
    table,
    schema,
    column,
    currentType,
    analysis: {
      totalRows,
      sampleSize,
      patterns
    },
    recommendations,
    ...(suggestedMigration && { suggestedMigration })
  };
}

export const dataQualityTools = {
  findDuplicates: {
    schema: FindDuplicatesSchema,
    handler: findDuplicates
  },
  findMissingValues: {
    schema: FindMissingValuesSchema,
    handler: findMissingValues
  },
  findOrphans: {
    schema: FindOrphansSchema,
    handler: findOrphans
  },
  checkConstraintViolations: {
    schema: CheckConstraintViolationsSchema,
    handler: checkConstraintViolations
  },
  analyzeTypeConsistency: {
    schema: AnalyzeTypeConsistencySchema,
    handler: analyzeTypeConsistency
  }
};
