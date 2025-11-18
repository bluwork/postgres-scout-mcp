import { z } from 'zod';
import { DatabaseConnection, TableInfo, ColumnInfo, ConstraintInfo, IndexInfo } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { formatBytes } from '../utils/query-builder.js';

const ListTablesSchema = z.object({
  schema: z.string().optional().default('public'),
  includeSystemTables: z.boolean().optional().default(false)
});

const DescribeTableSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public')
});

const ListSchemasSchema = z.object({});

export async function listSchemas(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof ListSchemasSchema>
): Promise<any> {
  logger.info('listSchemas', 'Listing database schemas');

  const query = `
    SELECT
      n.nspname as name,
      pg_catalog.pg_get_userbyid(n.nspowner) as owner,
      (SELECT COUNT(*) FROM pg_catalog.pg_class c WHERE c.relnamespace = n.oid AND c.relkind = 'r') as table_count
    FROM pg_catalog.pg_namespace n
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
    ORDER BY n.nspname;
  `;

  const result = await executeQuery(connection, logger, { query });

  return {
    schemas: result.rows.map(row => ({
      name: row.name,
      owner: row.owner,
      tableCount: parseInt(row.table_count, 10)
    }))
  };
}

export async function listTables(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof ListTablesSchema>
): Promise<any> {
  const { schema, includeSystemTables } = args;

  logger.info('listTables', 'Listing tables', { schema, includeSystemTables });

  const systemTableFilter = includeSystemTables ? '' : "AND c.relname NOT LIKE 'pg_%'";

  const query = `
    SELECT
      c.relname as name,
      n.nspname as schema,
      CASE c.relkind
        WHEN 'r' THEN 'BASE TABLE'
        WHEN 'v' THEN 'VIEW'
        WHEN 'm' THEN 'MATERIALIZED VIEW'
        WHEN 'p' THEN 'PARTITIONED TABLE'
      END as type,
      c.reltuples::bigint as row_estimate,
      pg_total_relation_size(c.oid) as total_size,
      pg_table_size(c.oid) as table_size,
      pg_indexes_size(c.oid) as index_size,
      pg_stat_get_last_vacuum_time(c.oid) as last_vacuum,
      pg_stat_get_last_analyze_time(c.oid) as last_analyze,
      c.relkind = 'p' as is_partitioned
    FROM pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'v', 'm', 'p')
      AND n.nspname = $1
      ${systemTableFilter}
    ORDER BY c.relname;
  `;

  const result = await executeQuery(connection, logger, {
    query,
    params: [schema]
  });

  const tables: TableInfo[] = result.rows.map(row => ({
    name: row.name,
    schema: row.schema,
    type: row.type,
    rowEstimate: parseInt(row.row_estimate, 10),
    sizeBytes: parseInt(row.table_size, 10),
    indexSize: parseInt(row.index_size, 10),
    totalSize: parseInt(row.total_size, 10),
    lastVacuum: row.last_vacuum,
    lastAnalyze: row.last_analyze,
    isPartitioned: row.is_partitioned
  }));

  return {
    schema,
    tableCount: tables.length,
    tables: tables.map(t => ({
      ...t,
      sizeMB: (t.sizeBytes / 1024 / 1024).toFixed(2),
      indexSizeMB: (t.indexSize / 1024 / 1024).toFixed(2),
      totalSizeMB: (t.totalSize / 1024 / 1024).toFixed(2)
    }))
  };
}

export async function describeTable(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof DescribeTableSchema>
): Promise<any> {
  const { table, schema } = args;

  logger.info('describeTable', 'Describing table', { table, schema });

  const [columns, constraints, indexes, stats] = await Promise.all([
    getColumns(connection, logger, schema, table),
    getConstraints(connection, logger, schema, table),
    getIndexes(connection, logger, schema, table),
    getTableStats(connection, logger, schema, table)
  ]);

  return {
    table,
    schema,
    columns,
    constraints,
    indexes,
    ...stats
  };
}

async function getColumns(
  connection: DatabaseConnection,
  logger: Logger,
  schema: string,
  table: string
): Promise<ColumnInfo[]> {
  const query = `
    SELECT
      a.attname as name,
      pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
      NOT a.attnotnull as nullable,
      pg_catalog.pg_get_expr(d.adbin, d.adrelid) as default_value,
      EXISTS(
        SELECT 1 FROM pg_catalog.pg_constraint c
        WHERE c.conrelid = a.attrelid
          AND a.attnum = ANY(c.conkey)
          AND c.contype = 'p'
      ) as is_primary_key,
      EXISTS(
        SELECT 1 FROM pg_catalog.pg_constraint c
        WHERE c.conrelid = a.attrelid
          AND a.attnum = ANY(c.conkey)
          AND c.contype = 'f'
      ) as is_foreign_key
    FROM pg_catalog.pg_attribute a
    LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
    WHERE a.attrelid = $1::regclass
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum;
  `;

  const result = await executeQuery(connection, logger, {
    query,
    params: [`${schema}.${table}`]
  });

  return result.rows.map(row => ({
    name: row.name,
    type: row.type,
    nullable: row.nullable,
    default: row.default_value,
    isPrimaryKey: row.is_primary_key,
    isForeignKey: row.is_foreign_key
  }));
}

async function getConstraints(
  connection: DatabaseConnection,
  logger: Logger,
  schema: string,
  table: string
): Promise<ConstraintInfo[]> {
  const query = `
    SELECT
      c.conname as name,
      c.contype as type,
      pg_catalog.pg_get_constraintdef(c.oid, true) as definition,
      ARRAY(
        SELECT a.attname
        FROM unnest(c.conkey) k(n)
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n
      ) as columns
    FROM pg_catalog.pg_constraint c
    WHERE c.conrelid = $1::regclass
    ORDER BY c.conname;
  `;

  const result = await executeQuery(connection, logger, {
    query,
    params: [`${schema}.${table}`]
  });

  return result.rows.map(row => {
    const typeMap: Record<string, ConstraintInfo['type']> = {
      'p': 'PRIMARY KEY',
      'f': 'FOREIGN KEY',
      'u': 'UNIQUE',
      'c': 'CHECK'
    };

    return {
      name: row.name,
      type: typeMap[row.type] || 'CHECK',
      columns: row.columns,
      definition: row.definition
    };
  });
}

async function getIndexes(
  connection: DatabaseConnection,
  logger: Logger,
  schema: string,
  table: string
): Promise<IndexInfo[]> {
  const query = `
    SELECT
      i.relname as name,
      ARRAY(
        SELECT a.attname
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = i.oid
          AND a.attnum > 0
        ORDER BY a.attnum
      ) as columns,
      am.amname as type,
      ix.indisunique as unique,
      ix.indisprimary as primary,
      pg_size_pretty(pg_relation_size(i.oid)) as size
    FROM pg_catalog.pg_index ix
    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
    JOIN pg_catalog.pg_am am ON am.oid = i.relam
    WHERE ix.indrelid = $1::regclass
    ORDER BY i.relname;
  `;

  const result = await executeQuery(connection, logger, {
    query,
    params: [`${schema}.${table}`]
  });

  return result.rows.map(row => ({
    name: row.name,
    columns: row.columns,
    type: row.type,
    unique: row.unique,
    primary: row.primary,
    size: row.size
  }));
}

async function getTableStats(
  connection: DatabaseConnection,
  logger: Logger,
  schema: string,
  table: string
): Promise<any> {
  const query = `
    SELECT
      c.reltuples::bigint as estimated_row_count,
      pg_size_pretty(pg_table_size(c.oid)) as disk_size,
      pg_size_pretty(pg_indexes_size(c.oid)) as index_size,
      pg_size_pretty(pg_total_relation_size(c.oid)) as total_size
    FROM pg_catalog.pg_class c
    WHERE c.oid = $1::regclass;
  `;

  const result = await executeQuery(connection, logger, {
    query,
    params: [`${schema}.${table}`]
  });

  return result.rows[0] || {};
}

export const schemaTools = {
  listSchemas: {
    schema: ListSchemasSchema,
    handler: listSchemas
  },
  listTables: {
    schema: ListTablesSchema,
    handler: listTables
  },
  describeTable: {
    schema: DescribeTableSchema,
    handler: describeTable
  }
};
