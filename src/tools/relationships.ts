import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { escapeIdentifier, sanitizeIdentifier } from '../utils/sanitize.js';

const ExploreRelationshipsSchema = z.object({
  table: z.string(),
  recordId: z.union([z.string(), z.number()]),
  schema: z.string().optional().default('public'),
  depth: z.number().optional().default(1),
  includeReverse: z.boolean().optional().default(true)
});

const AnalyzeForeignKeysSchema = z.object({
  schema: z.string().optional().default('public'),
  checkOrphans: z.boolean().optional().default(false),
  checkIndexes: z.boolean().optional().default(true)
});

export async function exploreRelationships(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof ExploreRelationshipsSchema>
): Promise<any> {
  const { table, recordId, schema, depth, includeReverse } = args;

  logger.info('exploreRelationships', 'Exploring relationships', { table, recordId, depth });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const pkQuery = `
    SELECT a.attname as column_name
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = $1::regclass
      AND i.indisprimary
    LIMIT 1
  `;

  const pkResult = await executeQuery(connection, logger, {
    query: pkQuery,
    params: [`${sanitizedSchema}.${sanitizedTable}`]
  });

  if (pkResult.rows.length === 0) {
    throw new Error(`No primary key found for table ${schema}.${table}`);
  }

  const pkColumn = pkResult.rows[0].column_name;

  const recordQuery = `
    SELECT *
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    WHERE ${escapeIdentifier(sanitizeIdentifier(pkColumn))} = $1
  `;

  const recordResult = await executeQuery(connection, logger, {
    query: recordQuery,
    params: [recordId]
  });

  if (recordResult.rows.length === 0) {
    throw new Error(`Record not found: ${schema}.${table} where ${pkColumn} = ${recordId}`);
  }

  const record = recordResult.rows[0];

  const fkQuery = `
    SELECT
      c.conname as constraint_name,
      a.attname as column_name,
      ref_ns.nspname as ref_schema,
      ref_tbl.relname as ref_table,
      ref_attr.attname as ref_column
    FROM pg_constraint c
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    JOIN pg_class ref_tbl ON ref_tbl.oid = c.confrelid
    JOIN pg_namespace ref_ns ON ref_ns.oid = ref_tbl.relnamespace
    JOIN pg_attribute ref_attr ON ref_attr.attrelid = c.confrelid AND ref_attr.attnum = ANY(c.confkey)
    WHERE c.conrelid = $1::regclass
      AND c.contype = 'f'
  `;

  const fkResult = await executeQuery(connection, logger, {
    query: fkQuery,
    params: [`${sanitizedSchema}.${sanitizedTable}`]
  });

  const related: Record<string, any> = {};

  for (const fk of fkResult.rows) {
    const fkValue = record[fk.column_name];

    if (fkValue) {
      const relatedQuery = `
        SELECT *
        FROM ${escapeIdentifier(fk.ref_schema)}.${escapeIdentifier(fk.ref_table)}
        WHERE ${escapeIdentifier(fk.ref_column)} = $1
      `;

      const relatedResult = await executeQuery(connection, logger, {
        query: relatedQuery,
        params: [fkValue]
      });

      if (relatedResult.rows.length > 0) {
        related[fk.ref_table] = {
          via: `${fk.column_name} -> ${fk.ref_table}.${fk.ref_column}`,
          record: relatedResult.rows[0]
        };
      }
    }
  }

  const reverseReferences: Record<string, any[]> = {};

  if (includeReverse) {
    const reverseFkQuery = `
      SELECT
        ns.nspname as schema,
        tbl.relname as table,
        a.attname as column_name,
        c.conname as constraint_name
      FROM pg_constraint c
      JOIN pg_class tbl ON tbl.oid = c.conrelid
      JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
      WHERE c.confrelid = $1::regclass
        AND c.contype = 'f'
    `;

    const reverseFkResult = await executeQuery(connection, logger, {
      query: reverseFkQuery,
      params: [`${sanitizedSchema}.${sanitizedTable}`]
    });

    for (const revFk of reverseFkResult.rows) {
      const reverseQuery = `
        SELECT *
        FROM ${escapeIdentifier(revFk.schema)}.${escapeIdentifier(revFk.table)}
        WHERE ${escapeIdentifier(revFk.column_name)} = $1
        LIMIT 10
      `;

      const reverseResult = await executeQuery(connection, logger, {
        query: reverseQuery,
        params: [recordId]
      });

      if (reverseResult.rows.length > 0) {
        reverseReferences[revFk.table] = reverseResult.rows;
      }
    }
  }

  return {
    table,
    schema,
    primaryKey: pkColumn,
    recordId,
    record,
    related,
    ...(includeReverse && { reverseReferences })
  };
}

export async function analyzeForeignKeys(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof AnalyzeForeignKeysSchema>
): Promise<any> {
  const { schema, checkOrphans, checkIndexes } = args;

  logger.info('analyzeForeignKeys', 'Analyzing foreign keys', { schema });

  const sanitizedSchema = sanitizeIdentifier(schema);

  const fkQuery = `
    SELECT
      c.conname as constraint_name,
      ns.nspname as schema,
      tbl.relname as table,
      a.attname as column,
      ref_ns.nspname as ref_schema,
      ref_tbl.relname as ref_table,
      ref_attr.attname as ref_column,
      c.confupdtype as on_update,
      c.confdeltype as on_delete
    FROM pg_constraint c
    JOIN pg_class tbl ON tbl.oid = c.conrelid
    JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    JOIN pg_class ref_tbl ON ref_tbl.oid = c.confrelid
    JOIN pg_namespace ref_ns ON ref_ns.oid = ref_tbl.relnamespace
    JOIN pg_attribute ref_attr ON ref_attr.attrelid = c.confrelid AND ref_attr.attnum = ANY(c.confkey)
    WHERE ns.nspname = $1
      AND c.contype = 'f'
    ORDER BY tbl.relname, c.conname
  `;

  const fkResult = await executeQuery(connection, logger, {
    query: fkQuery,
    params: [sanitizedSchema]
  });

  const issues: any[] = [];

  for (const fk of fkResult.rows) {
    const fkIssues: any[] = [];

    if (checkIndexes) {
      const indexQuery = `
        SELECT COUNT(*) as index_count
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = $1::regclass
          AND a.attname = $2
      `;

      const indexResult = await executeQuery(connection, logger, {
        query: indexQuery,
        params: [`${fk.schema}.${fk.table}`, fk.column]
      });

      const indexCount = parseInt(indexResult.rows[0]?.index_count || '0', 10);

      if (indexCount === 0) {
        fkIssues.push({
          type: 'missing_index',
          severity: 'warning',
          message: 'Foreign key column not indexed',
          impact: `Slow DELETE/UPDATE on ${fk.ref_table} table`,
          recommendation: `CREATE INDEX CONCURRENTLY idx_${fk.table}_${fk.column} ON ${fk.schema}.${fk.table}(${fk.column});`,
          estimatedImpact: 'Will speed up CASCADE operations and JOIN queries'
        });
      }
    }

    if (checkOrphans) {
      const orphanQuery = `
        SELECT COUNT(*) as orphan_count
        FROM ${escapeIdentifier(fk.schema)}.${escapeIdentifier(fk.table)} t
        LEFT JOIN ${escapeIdentifier(fk.ref_schema)}.${escapeIdentifier(fk.ref_table)} r
          ON t.${escapeIdentifier(fk.column)} = r.${escapeIdentifier(fk.ref_column)}
        WHERE t.${escapeIdentifier(fk.column)} IS NOT NULL
          AND r.${escapeIdentifier(fk.ref_column)} IS NULL
      `;

      const orphanResult = await executeQuery(connection, logger, {
        query: orphanQuery
      });

      const orphanCount = parseInt(orphanResult.rows[0]?.orphan_count || '0', 10);

      if (orphanCount > 0) {
        fkIssues.push({
          type: 'orphans',
          severity: 'error',
          message: `${orphanCount} orphaned records found`,
          orphanCount,
          recommendation: 'Clean up orphans before enforcing constraint',
          cleanupQuery: `DELETE FROM ${fk.schema}.${fk.table} WHERE ${fk.column} NOT IN (SELECT ${fk.ref_column} FROM ${fk.ref_schema}.${fk.ref_table});`
        });
      }
    }

    if (fkIssues.length > 0) {
      issues.push({
        constraint: fk.constraint_name,
        table: fk.table,
        schema: fk.schema,
        column: fk.column,
        referencesTable: fk.ref_table,
        referencesSchema: fk.ref_schema,
        referencesColumn: fk.ref_column,
        issues: fkIssues
      });
    }
  }

  const recommendations: string[] = [];

  const missingIndexes = issues.filter(i =>
    i.issues.some((issue: any) => issue.type === 'missing_index')
  ).length;

  const orphanedRecords = issues.filter(i =>
    i.issues.some((issue: any) => issue.type === 'orphans')
  ).length;

  if (missingIndexes > 0) {
    recommendations.push(
      `⚠ ${missingIndexes} foreign key columns missing indexes - significant performance impact`
    );
    recommendations.push('Add missing indexes during low-traffic period (use CONCURRENTLY)');
  }

  if (orphanedRecords > 0) {
    recommendations.push(`⚠ ${orphanedRecords} foreign keys have orphaned records - data integrity issue`);
    recommendations.push('Clean up orphaned records before enforcing constraints');
  }

  if (issues.length === 0) {
    recommendations.push('✓ All foreign keys are properly indexed and have no orphaned records');
  }

  return {
    schema,
    totalForeignKeys: fkResult.rows.length,
    issuesFound: issues.length,
    issues,
    recommendations
  };
}

export const relationshipTools = {
  exploreRelationships: {
    schema: ExploreRelationshipsSchema,
    handler: exploreRelationships
  },
  analyzeForeignKeys: {
    schema: AnalyzeForeignKeysSchema,
    handler: analyzeForeignKeys
  }
};
