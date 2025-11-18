import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';

const GetCurrentActivitySchema = z.object({
  includeIdle: z.boolean().optional().default(false),
  minDurationMs: z.number().optional().default(0)
});

const AnalyzeLocksSchema = z.object({
  includeWaiting: z.boolean().optional().default(true)
});

const GetIndexUsageSchema = z.object({
  schema: z.string().optional().default('public'),
  minSizeMB: z.number().optional().default(0),
  includeUnused: z.boolean().optional().default(true)
});

export async function getCurrentActivity(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof GetCurrentActivitySchema>
): Promise<any> {
  const { includeIdle, minDurationMs } = args;

  logger.info('getCurrentActivity', 'Getting current database activity');

  if (!Number.isFinite(minDurationMs)) {
    throw new Error('minDurationMs must be a finite number');
  }

  const stateFilter = includeIdle ? '' : "AND state != 'idle'";
  const durationFilter = minDurationMs > 0
    ? `AND EXTRACT(EPOCH FROM (NOW() - query_start)) * 1000 >= ${Number(minDurationMs)}`
    : '';

  const query = `
    SELECT
      pid,
      usename as user,
      datname as database,
      state,
      query,
      EXTRACT(EPOCH FROM (NOW() - query_start)) * 1000 as duration_ms,
      wait_event_type,
      wait_event,
      backend_type
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()
      ${stateFilter}
      ${durationFilter}
    ORDER BY query_start DESC
    LIMIT 100
  `;

  const statsQuery = `
    SELECT
      COUNT(*) as total_connections,
      COUNT(*) FILTER (WHERE state = 'active') as active_queries,
      COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
      COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()
  `;

  const [result, statsResult] = await Promise.all([
    executeQuery(connection, logger, { query }),
    executeQuery(connection, logger, { query: statsQuery })
  ]);

  const stats = statsResult.rows[0];

  const queries = result.rows.map(row => {
    const durationMs = parseFloat(row.duration_ms || '0');
    const warnings: string[] = [];

    if (row.state === 'idle in transaction' && durationMs > 30000) {
      warnings.push('⚠ Long idle transaction - potential lock holder');
    }

    if (row.state === 'active' && durationMs > 30000) {
      warnings.push('⚠ Long running query - monitor for timeout');
    }

    return {
      pid: parseInt(row.pid, 10),
      user: row.user,
      database: row.database,
      state: row.state,
      query: row.query?.substring(0, 500),
      durationMs: Math.round(durationMs),
      waitEventType: row.wait_event_type,
      waitEvent: row.wait_event,
      backendType: row.backend_type,
      ...(warnings.length > 0 && { warnings })
    };
  });

  const recommendations: string[] = [];

  const idleInTransaction = parseInt(stats.idle_in_transaction || '0', 10);
  if (idleInTransaction > 0) {
    recommendations.push(`${idleInTransaction} idle transactions - check application connection handling`);
  }

  const longRunning = queries.filter(q => q.durationMs > 5000).length;
  if (longRunning > 0) {
    recommendations.push(`${longRunning} queries running for >5s - monitor for timeouts`);
  }

  return {
    totalConnections: parseInt(stats.total_connections || '0', 10),
    activeQueries: parseInt(stats.active_queries || '0', 10),
    idleConnections: parseInt(stats.idle_connections || '0', 10),
    idleInTransaction,
    queries,
    recommendations
  };
}

export async function analyzeLocks(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof AnalyzeLocksSchema>
): Promise<any> {
  const { includeWaiting } = args;

  logger.info('analyzeLocks', 'Analyzing database locks');

  const query = `
    SELECT
      l.locktype,
      l.database,
      l.relation::regclass::text as relation,
      l.mode,
      l.granted,
      l.pid,
      a.query,
      EXTRACT(EPOCH FROM (NOW() - a.query_start)) * 1000 as duration_ms,
      (SELECT pid FROM pg_locks WHERE NOT granted AND relation = l.relation AND pid != l.pid LIMIT 1) as blocked_pid
    FROM pg_locks l
    LEFT JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE l.relation IS NOT NULL
      ${includeWaiting ? '' : 'AND l.granted = true'}
    ORDER BY l.granted, duration_ms DESC
    LIMIT 100
  `;

  const blockingQuery = `
    SELECT
      blocking.pid as blocking_pid,
      blocked.pid as blocked_pid,
      blocking_activity.query as blocking_query,
      blocked_activity.query as blocked_query,
      EXTRACT(EPOCH FROM (NOW() - blocked_activity.query_start)) * 1000 as wait_time_ms
    FROM pg_locks blocked
    JOIN pg_stat_activity blocked_activity ON blocked_activity.pid = blocked.pid
    JOIN pg_locks blocking ON blocking.relation = blocked.relation
      AND blocking.granted = true
      AND blocking.pid != blocked.pid
    JOIN pg_stat_activity blocking_activity ON blocking_activity.pid = blocking.pid
    WHERE NOT blocked.granted
      AND blocked_activity.state = 'active'
    LIMIT 50
  `;

  const [result, blockingResult] = await Promise.all([
    executeQuery(connection, logger, { query }),
    executeQuery(connection, logger, { query: blockingQuery })
  ]);

  const locks = result.rows.map(row => ({
    lockType: row.locktype,
    database: row.database,
    relation: row.relation,
    mode: row.mode,
    granted: row.granted,
    pid: parseInt(row.pid, 10),
    query: row.query?.substring(0, 200),
    durationMs: Math.round(parseFloat(row.duration_ms || '0')),
    ...(row.blocked_pid && { blockedPid: parseInt(row.blocked_pid, 10) })
  }));

  const blockingInfo = blockingResult.rows.map(row => ({
    blockingPid: parseInt(row.blocking_pid, 10),
    blockedPid: parseInt(row.blocked_pid, 10),
    blockingQuery: row.blocking_query?.substring(0, 200),
    blockedQuery: row.blocked_query?.substring(0, 200),
    waitTimeMs: Math.round(parseFloat(row.wait_time_ms || '0'))
  }));

  const recommendations: string[] = [];

  if (blockingInfo.length > 0) {
    recommendations.push(`⚠ ${blockingInfo.length} blocking queries detected`);
    blockingInfo.forEach(b => {
      recommendations.push(
        `PID ${b.blockingPid} is blocking PID ${b.blockedPid} (waiting ${Math.round(b.waitTimeMs / 1000)}s)`
      );
    });
    recommendations.push('Consider shorter transactions to reduce lock contention');
  } else {
    recommendations.push('✓ No blocking queries detected');
  }

  return {
    activeLocks: locks.length,
    blockingQueries: blockingInfo.length,
    locks,
    blockingInfo,
    recommendations
  };
}

export async function getIndexUsage(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof GetIndexUsageSchema>
): Promise<any> {
  const { schema, minSizeMB, includeUnused } = args;

  logger.info('getIndexUsage', 'Analyzing index usage', { schema });

  if (!Number.isFinite(minSizeMB)) {
    throw new Error('minSizeMB must be a finite number');
  }

  const sizeFilter = minSizeMB > 0 ? `AND pg_relation_size(i.indexrelid) >= ${Number(minSizeMB) * 1024 * 1024}` : '';
  const usageFilter = includeUnused ? '' : 'AND (s.idx_scan IS NULL OR s.idx_scan > 0)';

  const query = `
    SELECT
      n.nspname as schema,
      t.relname as table,
      i.relname as index,
      pg_size_pretty(pg_relation_size(i.oid)) as size,
      pg_relation_size(i.oid) as size_bytes,
      COALESCE(s.idx_scan, 0) as scans,
      COALESCE(s.idx_tup_read, 0) as tuples_read,
      COALESCE(s.idx_tup_fetch, 0) as tuples_fetched,
      ix.indisunique as unique,
      ix.indisprimary as primary
    FROM pg_class i
    JOIN pg_index ix ON ix.indexrelid = i.oid
    JOIN pg_class t ON t.oid = ix.indrelid
    JOIN pg_namespace n ON n.oid = i.relnamespace
    LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
    WHERE n.nspname = $1
      AND i.relkind = 'i'
      ${sizeFilter}
      ${usageFilter}
    ORDER BY pg_relation_size(i.oid) DESC
    LIMIT 100
  `;

  const result = await executeQuery(connection, logger, {
    query,
    params: [schema]
  });

  const indexes = result.rows.map(row => {
    const scans = parseInt(row.scans || '0', 10);
    const sizeBytes = parseInt(row.size_bytes, 10);
    const sizeMB = (sizeBytes / 1024 / 1024).toFixed(2);

    let usage = 'unknown';
    let recommendation = '';

    if (scans === 0) {
      usage = 'unused';
      recommendation = `⚠ Never used - consider dropping to save ${sizeMB} MB`;
    } else if (scans < 100) {
      usage = 'rarely used';
      recommendation = 'Rarely used - evaluate if necessary';
    } else if (scans < 1000) {
      usage = 'occasionally used';
      recommendation = 'Occasionally used';
    } else {
      usage = 'frequently used';
      recommendation = '✓ Frequently used - keep';
    }

    return {
      schema: row.schema,
      table: row.table,
      index: row.index,
      size: row.size,
      sizeMB: parseFloat(sizeMB),
      scans,
      tuplesRead: parseInt(row.tuples_read || '0', 10),
      tuplesFetched: parseInt(row.tuples_fetched || '0', 10),
      unique: row.unique,
      primary: row.primary,
      usage,
      recommendation,
      ...(scans === 0 && !row.primary && { dropCommand: `DROP INDEX CONCURRENTLY ${row.schema}.${row.index};` })
    };
  });

  const unusedIndexes = indexes.filter(idx => idx.usage === 'unused' && !idx.primary);
  const unusedSizeMB = unusedIndexes.reduce((sum, idx) => sum + idx.sizeMB, 0);

  const recommendations: string[] = [];

  if (unusedIndexes.length > 0) {
    recommendations.push(`${unusedIndexes.length} unused indexes consuming ${unusedSizeMB.toFixed(2)} MB`);
    recommendations.push('Drop unused indexes to reduce write overhead and save space');
    unusedIndexes.slice(0, 5).forEach(idx => {
      recommendations.push(`Consider: DROP INDEX CONCURRENTLY ${idx.schema}.${idx.index};`);
    });
  } else {
    recommendations.push('✓ All indexes are being used');
  }

  return {
    schema,
    totalIndexes: indexes.length,
    totalSizeMB: indexes.reduce((sum, idx) => sum + idx.sizeMB, 0).toFixed(2),
    unusedCount: unusedIndexes.length,
    indexes,
    recommendations
  };
}

export const monitoringTools = {
  getCurrentActivity: {
    schema: GetCurrentActivitySchema,
    handler: getCurrentActivity
  },
  analyzeLocks: {
    schema: AnalyzeLocksSchema,
    handler: analyzeLocks
  },
  getIndexUsage: {
    schema: GetIndexUsageSchema,
    handler: getIndexUsage
  }
};
