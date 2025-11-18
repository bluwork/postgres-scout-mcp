import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { sanitizeIdentifier } from '../utils/sanitize.js';

const GetLiveMetricsSchema = z.object({
  duration: z.number().optional().default(10000),
  interval: z.number().optional().default(1000),
  metrics: z.array(z.enum(['queries', 'connections', 'locks', 'transactions', 'cache'])).optional()
});

const GetHottestTablesSchema = z.object({
  schema: z.string().optional().default('public'),
  limit: z.number().optional().default(10),
  sampleDuration: z.number().optional().default(5000),
  orderBy: z.enum(['seq_scan', 'idx_scan', 'writes', 'size']).optional().default('seq_scan')
});

const GetTableMetricsSchema = z.object({
  table: z.string(),
  schema: z.string().optional().default('public'),
  includeTrends: z.boolean().optional().default(true)
});

interface MetricSnapshot {
  timestamp: string;
  queries: {
    active: number;
    idle: number;
    idleInTransaction: number;
    waiting: number;
  };
  connections: {
    current: number;
    max: number;
    usagePercent: number;
  };
  transactions: {
    committed: number;
    rolledBack: number;
  };
  cache: {
    hitRatio: number;
    blocksHit: number;
    blocksRead: number;
  };
  locks: {
    total: number;
    waiting: number;
  };
}

async function collectMetricSnapshot(
  connection: DatabaseConnection,
  logger: Logger
): Promise<MetricSnapshot> {
  const activityQuery = `
    SELECT
      COUNT(*) FILTER (WHERE state = 'active') as active,
      COUNT(*) FILTER (WHERE state = 'idle') as idle,
      COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
      COUNT(*) FILTER (WHERE wait_event IS NOT NULL AND state = 'active') as waiting
    FROM pg_stat_activity
    WHERE backend_type = 'client backend'
  `;

  const connectionsQuery = `
    SELECT
      (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend') as current,
      setting::int as max
    FROM pg_settings
    WHERE name = 'max_connections'
  `;

  const transactionsQuery = `
    SELECT
      xact_commit as committed,
      xact_rollback as rolled_back
    FROM pg_stat_database
    WHERE datname = current_database()
  `;

  const cacheQuery = `
    SELECT
      COALESCE(
        ROUND(
          SUM(blks_hit) * 100.0 / NULLIF(SUM(blks_hit) + SUM(blks_read), 0),
          2
        ),
        0
      ) as hit_ratio,
      SUM(blks_hit) as blocks_hit,
      SUM(blks_read) as blocks_read
    FROM pg_stat_database
    WHERE datname = current_database()
  `;

  const locksQuery = `
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE NOT granted) as waiting
    FROM pg_locks
  `;

  const [activityResult, connectionsResult, transactionsResult, cacheResult, locksResult] = await Promise.all([
    executeQuery(connection, logger, { query: activityQuery, params: [] }),
    executeQuery(connection, logger, { query: connectionsQuery, params: [] }),
    executeQuery(connection, logger, { query: transactionsQuery, params: [] }),
    executeQuery(connection, logger, { query: cacheQuery, params: [] }),
    executeQuery(connection, logger, { query: locksQuery, params: [] })
  ]);

  const activity = activityResult.rows[0];
  const connections = connectionsResult.rows[0];
  const transactions = transactionsResult.rows[0];
  const cache = cacheResult.rows[0];
  const locks = locksResult.rows[0];

  const current = parseInt(connections.current || '0', 10);
  const max = parseInt(connections.max || '100', 10);

  return {
    timestamp: new Date().toISOString(),
    queries: {
      active: parseInt(activity.active || '0', 10),
      idle: parseInt(activity.idle || '0', 10),
      idleInTransaction: parseInt(activity.idle_in_transaction || '0', 10),
      waiting: parseInt(activity.waiting || '0', 10)
    },
    connections: {
      current,
      max,
      usagePercent: Math.round((current / max) * 100)
    },
    transactions: {
      committed: parseInt(transactions.committed || '0', 10),
      rolledBack: parseInt(transactions.rolled_back || '0', 10)
    },
    cache: {
      hitRatio: parseFloat(cache.hit_ratio || '0'),
      blocksHit: parseInt(cache.blocks_hit || '0', 10),
      blocksRead: parseInt(cache.blocks_read || '0', 10)
    },
    locks: {
      total: parseInt(locks.total || '0', 10),
      waiting: parseInt(locks.waiting || '0', 10)
    }
  };
}

export async function getLiveMetrics(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof GetLiveMetricsSchema>
): Promise<any> {
  const { duration, interval } = args;

  logger.info('getLiveMetrics', 'Collecting live metrics', { duration, interval });

  const snapshots: MetricSnapshot[] = [];
  const iterations = Math.floor(duration / interval);

  for (let i = 0; i < iterations; i++) {
    const snapshot = await collectMetricSnapshot(connection, logger);
    snapshots.push(snapshot);

    if (i < iterations - 1) {
      await new Promise(resolve => setTimeout(resolve, interval));
    }
  }

  const first = snapshots[0];
  const last = snapshots[snapshots.length - 1];

  const summary = {
    duration: `${duration}ms`,
    samples: snapshots.length,
    averages: {
      activeQueries: Math.round(snapshots.reduce((sum, s) => sum + s.queries.active, 0) / snapshots.length),
      connectionUsage: Math.round(snapshots.reduce((sum, s) => sum + s.connections.usagePercent, 0) / snapshots.length),
      cacheHitRatio: Math.round(snapshots.reduce((sum, s) => sum + s.cache.hitRatio, 0) / snapshots.length * 10) / 10,
      waitingLocks: Math.round(snapshots.reduce((sum, s) => sum + s.locks.waiting, 0) / snapshots.length * 10) / 10
    },
    peaks: {
      maxActiveQueries: Math.max(...snapshots.map(s => s.queries.active)),
      maxConnections: Math.max(...snapshots.map(s => s.connections.current)),
      maxWaitingLocks: Math.max(...snapshots.map(s => s.locks.waiting))
    },
    deltas: {
      transactionsCommitted: last.transactions.committed - first.transactions.committed,
      transactionsRolledBack: last.transactions.rolledBack - first.transactions.rolledBack,
      blocksRead: last.cache.blocksRead - first.cache.blocksRead
    }
  };

  const issues: string[] = [];
  if (summary.averages.connectionUsage > 80) {
    issues.push(`High connection usage: ${summary.averages.connectionUsage}%`);
  }
  if (summary.averages.cacheHitRatio < 90) {
    issues.push(`Low cache hit ratio: ${summary.averages.cacheHitRatio}%`);
  }
  if (summary.peaks.maxWaitingLocks > 5) {
    issues.push(`Lock contention detected: up to ${summary.peaks.maxWaitingLocks} waiting locks`);
  }

  return {
    summary,
    issues: issues.length > 0 ? issues : undefined,
    snapshots
  };
}

export async function getHottestTables(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof GetHottestTablesSchema>
): Promise<any> {
  const { schema, limit, sampleDuration, orderBy } = args;

  logger.info('getHottestTables', 'Identifying hottest tables', { schema, orderBy });

  const sanitizedSchema = sanitizeIdentifier(schema);

  const beforeQuery = `
    SELECT
      relname as table_name,
      seq_scan,
      seq_tup_read,
      idx_scan,
      idx_tup_fetch,
      n_tup_ins,
      n_tup_upd,
      n_tup_del
    FROM pg_stat_user_tables
    WHERE schemaname = '${sanitizedSchema}'
  `;

  const beforeResult = await executeQuery(connection, logger, { query: beforeQuery, params: [] });
  const beforeStats = new Map(beforeResult.rows.map(r => [r.table_name, r]));

  await new Promise(resolve => setTimeout(resolve, sampleDuration));

  const afterResult = await executeQuery(connection, logger, { query: beforeQuery, params: [] });

  const activity = afterResult.rows.map(after => {
    const before = beforeStats.get(after.table_name) || after;

    const seqScanDelta = parseInt(after.seq_scan || '0', 10) - parseInt(before.seq_scan || '0', 10);
    const idxScanDelta = parseInt(after.idx_scan || '0', 10) - parseInt(before.idx_scan || '0', 10);
    const insertsDelta = parseInt(after.n_tup_ins || '0', 10) - parseInt(before.n_tup_ins || '0', 10);
    const updatesDelta = parseInt(after.n_tup_upd || '0', 10) - parseInt(before.n_tup_upd || '0', 10);
    const deletesDelta = parseInt(after.n_tup_del || '0', 10) - parseInt(before.n_tup_del || '0', 10);

    return {
      table: after.table_name,
      activity: {
        seqScans: seqScanDelta,
        idxScans: idxScanDelta,
        totalScans: seqScanDelta + idxScanDelta,
        writes: insertsDelta + updatesDelta + deletesDelta,
        inserts: insertsDelta,
        updates: updatesDelta,
        deletes: deletesDelta
      },
      totals: {
        seqScans: parseInt(after.seq_scan || '0', 10),
        idxScans: parseInt(after.idx_scan || '0', 10)
      }
    };
  });

  let sortKey: (a: any) => number;
  switch (orderBy) {
    case 'seq_scan':
      sortKey = a => a.activity.seqScans;
      break;
    case 'idx_scan':
      sortKey = a => a.activity.idxScans;
      break;
    case 'writes':
      sortKey = a => a.activity.writes;
      break;
    default:
      sortKey = a => a.activity.totalScans;
  }

  const sorted = activity
    .filter(a => a.activity.totalScans > 0 || a.activity.writes > 0)
    .sort((a, b) => sortKey(b) - sortKey(a))
    .slice(0, limit);

  return {
    schema,
    sampleDuration: `${sampleDuration}ms`,
    orderBy,
    tables: sorted,
    totalTablesWithActivity: sorted.length
  };
}

export async function getTableMetrics(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof GetTableMetricsSchema>
): Promise<any> {
  const { table, schema, includeTrends } = args;

  logger.info('getTableMetrics', 'Getting table metrics', { schema, table });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  const statsQuery = `
    SELECT
      relname as table_name,
      seq_scan,
      seq_tup_read,
      idx_scan,
      idx_tup_fetch,
      n_tup_ins,
      n_tup_upd,
      n_tup_del,
      n_tup_hot_upd,
      n_live_tup,
      n_dead_tup,
      n_mod_since_analyze,
      last_vacuum,
      last_autovacuum,
      last_analyze,
      last_autoanalyze,
      vacuum_count,
      autovacuum_count,
      analyze_count,
      autoanalyze_count
    FROM pg_stat_user_tables
    WHERE schemaname = '${sanitizedSchema}' AND relname = '${sanitizedTable}'
  `;

  const sizeQuery = `
    SELECT
      pg_total_relation_size('${sanitizedSchema}.${sanitizedTable}') as total_bytes,
      pg_relation_size('${sanitizedSchema}.${sanitizedTable}') as table_bytes,
      pg_indexes_size('${sanitizedSchema}.${sanitizedTable}'::regclass) as index_bytes
  `;

  const ioQuery = `
    SELECT
      heap_blks_read,
      heap_blks_hit,
      idx_blks_read,
      idx_blks_hit,
      toast_blks_read,
      toast_blks_hit
    FROM pg_statio_user_tables
    WHERE schemaname = '${sanitizedSchema}' AND relname = '${sanitizedTable}'
  `;

  const [statsResult, sizeResult, ioResult] = await Promise.all([
    executeQuery(connection, logger, { query: statsQuery, params: [] }),
    executeQuery(connection, logger, { query: sizeQuery, params: [] }),
    executeQuery(connection, logger, { query: ioQuery, params: [] })
  ]);

  if (statsResult.rows.length === 0) {
    return { error: `Table ${schema}.${table} not found` };
  }

  const stats = statsResult.rows[0];
  const size = sizeResult.rows[0];
  const io = ioResult.rows[0];

  const liveTuples = parseInt(stats.n_live_tup || '0', 10);
  const deadTuples = parseInt(stats.n_dead_tup || '0', 10);
  const heapHit = parseInt(io?.heap_blks_hit || '0', 10);
  const heapRead = parseInt(io?.heap_blks_read || '0', 10);
  const idxHit = parseInt(io?.idx_blks_hit || '0', 10);
  const idxRead = parseInt(io?.idx_blks_read || '0', 10);

  const result: any = {
    table: `${schema}.${table}`,
    size: {
      total: formatBytes(parseInt(size.total_bytes || '0', 10)),
      table: formatBytes(parseInt(size.table_bytes || '0', 10)),
      indexes: formatBytes(parseInt(size.index_bytes || '0', 10))
    },
    rows: {
      live: liveTuples,
      dead: deadTuples,
      deadPercent: liveTuples > 0 ? Math.round((deadTuples / (liveTuples + deadTuples)) * 100) : 0
    },
    scans: {
      sequential: parseInt(stats.seq_scan || '0', 10),
      index: parseInt(stats.idx_scan || '0', 10),
      seqTuplesRead: parseInt(stats.seq_tup_read || '0', 10),
      idxTuplesFetched: parseInt(stats.idx_tup_fetch || '0', 10)
    },
    modifications: {
      inserts: parseInt(stats.n_tup_ins || '0', 10),
      updates: parseInt(stats.n_tup_upd || '0', 10),
      deletes: parseInt(stats.n_tup_del || '0', 10),
      hotUpdates: parseInt(stats.n_tup_hot_upd || '0', 10),
      modsSinceAnalyze: parseInt(stats.n_mod_since_analyze || '0', 10)
    },
    io: {
      heapHitRatio: heapHit + heapRead > 0 ? Math.round((heapHit / (heapHit + heapRead)) * 100) : 100,
      indexHitRatio: idxHit + idxRead > 0 ? Math.round((idxHit / (idxHit + idxRead)) * 100) : 100
    },
    maintenance: {
      lastVacuum: stats.last_vacuum,
      lastAutovacuum: stats.last_autovacuum,
      lastAnalyze: stats.last_analyze,
      lastAutoanalyze: stats.last_autoanalyze,
      vacuumCount: parseInt(stats.vacuum_count || '0', 10),
      analyzeCount: parseInt(stats.analyze_count || '0', 10)
    }
  };

  const recommendations: string[] = [];

  if (result.rows.deadPercent > 20) {
    recommendations.push(`High dead tuple ratio (${result.rows.deadPercent}%) - consider VACUUM`);
  }

  if (result.scans.sequential > result.scans.index * 10 && result.scans.sequential > 100) {
    recommendations.push('High sequential scan ratio - review indexes');
  }

  if (result.io.heapHitRatio < 90) {
    recommendations.push(`Low heap cache hit ratio (${result.io.heapHitRatio}%) - consider increasing shared_buffers`);
  }

  if (result.modifications.modsSinceAnalyze > liveTuples * 0.1) {
    recommendations.push('Many modifications since last ANALYZE - statistics may be stale');
  }

  if (recommendations.length > 0) {
    result.recommendations = recommendations;
  }

  return result;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

export const liveMonitoringTools = {
  getLiveMetrics: {
    schema: GetLiveMetricsSchema,
    handler: getLiveMetrics
  },
  getHottestTables: {
    schema: GetHottestTablesSchema,
    handler: getHottestTables
  },
  getTableMetrics: {
    schema: GetTableMetricsSchema,
    handler: getTableMetrics
  }
};
