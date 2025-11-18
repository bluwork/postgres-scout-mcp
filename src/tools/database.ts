import { z } from 'zod';
import { DatabaseConnection, DatabaseStats } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';

const ListDatabasesSchema = z.object({});

const GetDatabaseStatsSchema = z.object({
  database: z.string().optional()
});

export async function listDatabases(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof ListDatabasesSchema>
): Promise<any> {
  logger.info('listDatabases', 'Listing all databases');

  const query = `
    SELECT
      d.datname as name,
      pg_catalog.pg_get_userbyid(d.datdba) as owner,
      pg_catalog.pg_encoding_to_char(d.encoding) as encoding,
      pg_catalog.pg_database_size(d.datname) as size_bytes,
      (SELECT COUNT(*) FROM pg_catalog.pg_stat_activity WHERE datname = d.datname) as connections
    FROM pg_catalog.pg_database d
    WHERE d.datistemplate = false
    ORDER BY d.datname;
  `;

  const result = await executeQuery(connection, logger, { query });

  return {
    databases: result.rows.map(row => ({
      name: row.name,
      owner: row.owner,
      encoding: row.encoding,
      sizeBytes: parseInt(row.size_bytes, 10),
      sizeMB: (parseInt(row.size_bytes, 10) / 1024 / 1024).toFixed(2),
      connections: parseInt(row.connections, 10)
    }))
  };
}

export async function getDatabaseStats(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof GetDatabaseStatsSchema>
): Promise<DatabaseStats> {
  const database = args.database || connection.pool.options.database || 'current';

  logger.info('getDatabaseStats', 'Getting database statistics', { database });

  const queries = await Promise.all([
    getSizeStats(connection, logger, database),
    getObjectCounts(connection, logger),
    getConnectionStats(connection, logger),
    getCacheStats(connection, logger),
    getTupleStats(connection, logger)
  ]);

  const [sizeStats, objectCounts, connectionStats, cacheStats, tupleStats] = queries;

  return {
    database,
    size: sizeStats.size,
    tables: objectCounts.tables,
    indexes: objectCounts.indexes,
    sequences: objectCounts.sequences,
    views: objectCounts.views,
    functions: objectCounts.functions,
    activeConnections: connectionStats.active,
    maxConnections: connectionStats.max,
    cacheHitRatio: cacheStats.ratio,
    transactionRate: cacheStats.transactionRate,
    tupleStats: tupleStats
  };
}

async function getSizeStats(
  connection: DatabaseConnection,
  logger: Logger,
  database: string
): Promise<any> {
  const query = `SELECT pg_size_pretty(pg_database_size(current_database())) as size`;
  const result = await executeQuery(connection, logger, { query });
  return { size: result.rows[0]?.size || '0' };
}

async function getObjectCounts(
  connection: DatabaseConnection,
  logger: Logger
): Promise<any> {
  const query = `
    SELECT
      (SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relkind = 'r') as tables,
      (SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relkind = 'i') as indexes,
      (SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relkind = 'S') as sequences,
      (SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relkind = 'v') as views,
      (SELECT COUNT(*) FROM pg_catalog.pg_proc) as functions
  `;

  const result = await executeQuery(connection, logger, { query });
  return {
    tables: parseInt(result.rows[0]?.tables || '0', 10),
    indexes: parseInt(result.rows[0]?.indexes || '0', 10),
    sequences: parseInt(result.rows[0]?.sequences || '0', 10),
    views: parseInt(result.rows[0]?.views || '0', 10),
    functions: parseInt(result.rows[0]?.functions || '0', 10)
  };
}

async function getConnectionStats(
  connection: DatabaseConnection,
  logger: Logger
): Promise<any> {
  const query = `
    SELECT
      (SELECT COUNT(*) FROM pg_catalog.pg_stat_activity) as active,
      (SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'max_connections') as max
  `;

  const result = await executeQuery(connection, logger, { query });
  return {
    active: parseInt(result.rows[0]?.active || '0', 10),
    max: parseInt(result.rows[0]?.max || '100', 10)
  };
}

async function getCacheStats(
  connection: DatabaseConnection,
  logger: Logger
): Promise<any> {
  const query = `
    SELECT
      CASE
        WHEN (blks_hit + blks_read) = 0 THEN 0
        ELSE ROUND(blks_hit::numeric / (blks_hit + blks_read), 4)
      END as cache_hit_ratio,
      xact_commit + xact_rollback as total_transactions
    FROM pg_catalog.pg_stat_database
    WHERE datname = current_database()
  `;

  const result = await executeQuery(connection, logger, { query });
  return {
    ratio: parseFloat(result.rows[0]?.cache_hit_ratio || '0'),
    transactionRate: parseInt(result.rows[0]?.total_transactions || '0', 10)
  };
}

async function getTupleStats(
  connection: DatabaseConnection,
  logger: Logger
): Promise<any> {
  const query = `
    SELECT
      tup_returned,
      tup_fetched,
      tup_inserted,
      tup_updated,
      tup_deleted
    FROM pg_catalog.pg_stat_database
    WHERE datname = current_database()
  `;

  const result = await executeQuery(connection, logger, { query });
  const row = result.rows[0] || {};

  return {
    returned: parseInt(row.tup_returned || '0', 10),
    fetched: parseInt(row.tup_fetched || '0', 10),
    inserted: parseInt(row.tup_inserted || '0', 10),
    updated: parseInt(row.tup_updated || '0', 10),
    deleted: parseInt(row.tup_deleted || '0', 10)
  };
}

export const databaseTools = {
  listDatabases: {
    schema: ListDatabasesSchema,
    handler: listDatabases
  },
  getDatabaseStats: {
    schema: GetDatabaseStatsSchema,
    handler: getDatabaseStats
  }
};
