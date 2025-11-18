import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { escapeIdentifier, sanitizeIdentifier } from '../utils/sanitize.js';

const AnalyzeTableBloatSchema = z.object({
  schema: z.string().optional().default('public'),
  table: z.string().optional(),
  thresholdPercent: z.number().optional().default(20)
});

const SuggestVacuumSchema = z.object({
  schema: z.string().optional().default('public'),
  minDeadTuples: z.number().optional().default(1000),
  minBloatPercent: z.number().optional().default(10)
});

const GetHealthScoreSchema = z.object({
  database: z.string().optional()
});

const GetSlowQueriesSchema = z.object({
  minDurationMs: z.number().optional().default(100),
  limit: z.number().optional().default(10),
  orderBy: z.enum(['total_time', 'mean_time', 'calls']).optional().default('total_time')
});

export async function analyzeTableBloat(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof AnalyzeTableBloatSchema>
): Promise<any> {
  const { schema, table, thresholdPercent } = args;

  logger.info('analyzeTableBloat', 'Analyzing table bloat', { schema, table });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const tableFilter = table ? `AND c.relname = $2` : '';
  const params = table ? [sanitizedSchema, sanitizeIdentifier(table)] : [sanitizedSchema];

  // Table bloat analysis
  const tableBloatQuery = `
    SELECT
      s.schemaname as schema,
      s.relname as table,
      'table' as type,
      pg_stat_get_live_tuples(c.oid) as live_tuples,
      pg_stat_get_dead_tuples(c.oid) as dead_tuples,
      pg_total_relation_size(c.oid) as total_size,
      CASE
        WHEN pg_stat_get_live_tuples(c.oid) + pg_stat_get_dead_tuples(c.oid) = 0 THEN 0
        ELSE ROUND((pg_stat_get_dead_tuples(c.oid)::numeric /
          (pg_stat_get_live_tuples(c.oid) + pg_stat_get_dead_tuples(c.oid))) * 100, 2)
      END as bloat_percent,
      pg_size_pretty(pg_total_relation_size(c.oid)) as total_size_pretty,
      pg_stat_get_last_vacuum_time(c.oid) as last_vacuum,
      pg_stat_get_last_autovacuum_time(c.oid) as last_autovacuum
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.relname = s.relname AND c.relnamespace = (
      SELECT oid FROM pg_namespace WHERE nspname = s.schemaname
    )
    WHERE s.schemaname = $1
      ${tableFilter}
    ORDER BY (pg_stat_get_dead_tuples(c.oid)::numeric /
      NULLIF(pg_stat_get_live_tuples(c.oid) + pg_stat_get_dead_tuples(c.oid), 0)) DESC NULLS LAST
  `;

  // Index bloat estimation
  const indexBloatQuery = `
    SELECT
      sui.schemaname as schema,
      sui.relname as table,
      sui.indexrelname as index_name,
      'index' as type,
      pg_relation_size(sui.indexrelid) as size_bytes,
      pg_size_pretty(pg_relation_size(sui.indexrelid)) as size,
      sui.idx_scan as scans
    FROM pg_stat_user_indexes sui
    WHERE sui.schemaname = $1
      ${tableFilter ? tableFilter.replace('c.relname', 'sui.relname') : ''}
      AND pg_relation_size(sui.indexrelid) > 0
    ORDER BY pg_relation_size(sui.indexrelid) DESC
    LIMIT 50
  `;

  const [tableBloatResult, indexBloatResult] = await Promise.all([
    executeQuery(connection, logger, { query: tableBloatQuery, params }),
    executeQuery(connection, logger, { query: indexBloatQuery, params })
  ]);

  const analysis: any[] = [];

  // Process table bloat
  for (const row of tableBloatResult.rows) {
    const bloatPercent = parseFloat(row.bloat_percent || '0');
    const deadTuples = parseInt(row.dead_tuples || '0', 10);
    const liveTuples = parseInt(row.live_tuples || '0', 10);

    if (bloatPercent >= thresholdPercent || deadTuples >= 1000) {
      const bloatBytes = Math.round((deadTuples / (liveTuples + deadTuples)) * parseInt(row.total_size, 10));

      analysis.push({
        schema: row.schema,
        table: row.table,
        type: 'table',
        liveTuples,
        deadTuples,
        totalSize: row.total_size_pretty,
        bloatPercent,
        bloatBytes,
        wastedSpace: (bloatBytes / 1024 / 1024).toFixed(2) + ' MB',
        lastVacuum: row.last_vacuum,
        lastAutoVacuum: row.last_autovacuum
      });
    }
  }

  // Process index bloat (simplified heuristic)
  for (const row of indexBloatResult.rows) {
    const sizeBytes = parseInt(row.size_bytes, 10);
    const scans = parseInt(row.scans || '0', 10);

    // If index is large but rarely used, consider it potentially bloated
    if (sizeBytes > 10 * 1024 * 1024 && scans < 100) {
      analysis.push({
        schema: row.schema,
        table: row.table,
        type: 'index',
        indexName: row.index_name,
        size: row.size,
        sizeBytes,
        scans,
        estimatedBloatPercent: 'unknown',
        recommendation: scans === 0 ? 'Consider dropping unused index' : 'Consider REINDEX if performance degrades'
      });
    }
  }

  const recommendations: string[] = [];

  const highBloatTables = analysis.filter(a => a.type === 'table' && a.bloatPercent > 30);
  const mediumBloatTables = analysis.filter(a => a.type === 'table' && a.bloatPercent >= 20 && a.bloatPercent <= 30);

  if (highBloatTables.length > 0) {
    recommendations.push(`⚠ ${highBloatTables.length} tables with >30% bloat - consider VACUUM FULL during maintenance window`);
    highBloatTables.slice(0, 3).forEach(t => {
      recommendations.push(`  VACUUM FULL ANALYZE ${t.schema}.${t.table};`);
    });
  }

  if (mediumBloatTables.length > 0) {
    recommendations.push(`${mediumBloatTables.length} tables with 20-30% bloat - schedule regular VACUUM`);
    mediumBloatTables.slice(0, 3).forEach(t => {
      recommendations.push(`  VACUUM ANALYZE ${t.schema}.${t.table};`);
    });
  }

  const unusedIndexes = analysis.filter(a => a.type === 'index' && a.scans === 0);
  if (unusedIndexes.length > 0) {
    recommendations.push(`${unusedIndexes.length} unused indexes - consider dropping to reduce bloat`);
  }

  if (analysis.length === 0) {
    recommendations.push('✓ No significant bloat detected');
  }

  return {
    schema,
    ...(table && { table }),
    thresholdPercent,
    analysis,
    recommendations
  };
}

export async function suggestVacuum(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof SuggestVacuumSchema>
): Promise<any> {
  const { schema, minDeadTuples, minBloatPercent } = args;

  logger.info('suggestVacuum', 'Suggesting VACUUM operations', { schema });

  const sanitizedSchema = sanitizeIdentifier(schema);

  const query = `
    SELECT
      s.schemaname as schema,
      s.relname as table,
      pg_stat_get_live_tuples(c.oid) as live_tuples,
      pg_stat_get_dead_tuples(c.oid) as dead_tuples,
      CASE
        WHEN pg_stat_get_live_tuples(c.oid) + pg_stat_get_dead_tuples(c.oid) = 0 THEN 0
        ELSE ROUND((pg_stat_get_dead_tuples(c.oid)::numeric /
          (pg_stat_get_live_tuples(c.oid) + pg_stat_get_dead_tuples(c.oid))) * 100, 2)
      END as dead_tuples_percent,
      pg_stat_get_last_vacuum_time(c.oid) as last_vacuum,
      pg_stat_get_last_autovacuum_time(c.oid) as last_autovacuum,
      pg_total_relation_size(c.oid) as total_size,
      pg_size_pretty(pg_total_relation_size(c.oid)) as size_pretty
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.relname = s.relname AND c.relnamespace = (
      SELECT oid FROM pg_namespace WHERE nspname = s.schemaname
    )
    WHERE s.schemaname = $1
      AND pg_stat_get_dead_tuples(c.oid) >= $2
    ORDER BY pg_stat_get_dead_tuples(c.oid) DESC
  `;

  const result = await executeQuery(connection, logger, {
    query,
    params: [sanitizedSchema, minDeadTuples]
  });

  const recommendations: any[] = [];

  for (const row of result.rows) {
    const deadTuples = parseInt(row.dead_tuples || '0', 10);
    const liveTuples = parseInt(row.live_tuples || '0', 10);
    const deadPercent = parseFloat(row.dead_tuples_percent || '0');
    const totalSize = parseInt(row.total_size, 10);

    if (deadPercent < minBloatPercent) continue;

    const lastVacuum = row.last_vacuum || row.last_autovacuum;
    const daysSinceVacuum = lastVacuum
      ? Math.floor((Date.now() - new Date(lastVacuum).getTime()) / (1000 * 60 * 60 * 24))
      : null;

    let priority = 'low';
    let command = `VACUUM ANALYZE ${schema}.${row.table};`;
    let warning = null;

    if (deadPercent > 50) {
      priority = 'critical';
      command = `VACUUM FULL ANALYZE ${schema}.${row.table};`;
      warning = `⚠ ${deadPercent.toFixed(1)}% bloat - VACUUM FULL recommended (requires exclusive lock)`;
    } else if (deadPercent > 30) {
      priority = 'high';
      command = `VACUUM ANALYZE ${schema}.${row.table};`;
    } else if (deadPercent > 20) {
      priority = 'medium';
    }

    // Estimate duration based on table size
    let estimatedDuration = '< 1 minute';
    if (totalSize > 1024 * 1024 * 1024) { // > 1GB
      estimatedDuration = command.includes('FULL') ? '10-30 minutes' : '1-5 minutes';
    } else if (totalSize > 100 * 1024 * 1024) { // > 100MB
      estimatedDuration = command.includes('FULL') ? '1-10 minutes' : '< 1 minute';
    }

    recommendations.push({
      table: row.table,
      schema: row.schema,
      deadTuples,
      liveTuples,
      deadTuplesPercent: deadPercent,
      bloatPercent: deadPercent,
      lastVacuum: row.last_vacuum,
      lastAutoVacuum: row.last_autovacuum,
      daysSinceVacuum,
      priority,
      command,
      estimatedDuration,
      recommendConcurrent: false, // VACUUM doesn't support CONCURRENTLY
      ...(warning && { warning })
    });
  }

  // Sort by priority
  const priorityOrder = { critical: 0, high: 1, medium: 2, low: 3 };
  recommendations.sort((a, b) => priorityOrder[a.priority as keyof typeof priorityOrder] - priorityOrder[b.priority as keyof typeof priorityOrder]);

  return {
    schema,
    tablesNeedingVacuum: recommendations.length,
    recommendations
  };
}

export async function getHealthScore(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof GetHealthScoreSchema>
): Promise<any> {
  const database = args.database || connection.pool.options.database || 'current';

  logger.info('getHealthScore', 'Calculating health score', { database });

  // Gather multiple health metrics in parallel
  const [
    cacheStats,
    connectionStats,
    bloatStats,
    indexStats,
    activityStats,
    replicationStats
  ] = await Promise.all([
    getCacheHitRatio(connection, logger),
    getConnectionHealth(connection, logger),
    getBloatHealth(connection, logger),
    getIndexHealth(connection, logger),
    getActivityHealth(connection, logger),
    getReplicationHealth(connection, logger)
  ]);

  // Calculate component scores (0-100)
  const scoreBreakdown = {
    cacheHitRatio: calculateCacheScore(cacheStats.ratio),
    indexUsage: calculateIndexScore(indexStats.usage),
    bloat: calculateBloatScore(bloatStats.avgBloat),
    connectionUsage: calculateConnectionScore(connectionStats.usage),
    deadTuples: calculateDeadTuplesScore(bloatStats.avgDeadPercent),
    longRunningQueries: calculateLongQueryScore(activityStats.longRunning),
    replicationLag: calculateReplicationScore(replicationStats.lag)
  };

  // Calculate overall score (weighted average)
  const weights = {
    cacheHitRatio: 0.20,
    indexUsage: 0.15,
    bloat: 0.15,
    connectionUsage: 0.10,
    deadTuples: 0.15,
    longRunningQueries: 0.15,
    replicationLag: 0.10
  };

  let overallScore = 0;
  for (const [key, weight] of Object.entries(weights)) {
    overallScore += scoreBreakdown[key as keyof typeof scoreBreakdown].score * weight;
  }
  overallScore = Math.round(overallScore);

  // Generate issues and recommendations
  const issues: any[] = [];
  const recommendations: string[] = [];

  if (scoreBreakdown.cacheHitRatio.score < 90) {
    issues.push({
      severity: 'warning',
      category: 'cache',
      message: `Cache hit ratio is ${(cacheStats.ratio * 100).toFixed(1)}%`,
      recommendation: 'Consider increasing shared_buffers or investigating query patterns'
    });
  } else {
    recommendations.push(`✓ Cache hit ratio is excellent (${(cacheStats.ratio * 100).toFixed(1)}%)`);
  }

  if (scoreBreakdown.bloat.score < 70) {
    issues.push({
      severity: 'warning',
      category: 'bloat',
      message: `Average table bloat is ${bloatStats.avgBloat.toFixed(1)}%`,
      recommendation: 'Schedule VACUUM for bloated tables during maintenance window'
    });
  }

  if (scoreBreakdown.indexUsage.score < 80) {
    issues.push({
      severity: 'info',
      category: 'indexes',
      message: `${indexStats.unusedCount} indexes are unused`,
      recommendation: `Consider dropping unused indexes to save ${indexStats.unusedSizeMB.toFixed(0)} MB`
    });
  }

  if (scoreBreakdown.connectionUsage.score > 90) {
    recommendations.push(`✓ Connection pool usage is healthy (${(connectionStats.usage * 100).toFixed(0)}% of max)`);
  } else if (scoreBreakdown.connectionUsage.score < 50) {
    issues.push({
      severity: 'critical',
      category: 'connections',
      message: `Using ${(connectionStats.usage * 100).toFixed(0)}% of max connections`,
      recommendation: 'Increase max_connections or investigate connection leaks'
    });
  }

  return {
    database,
    overallScore,
    scoreBreakdown,
    issues,
    recommendations
  };
}

export async function getSlowQueries(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof GetSlowQueriesSchema>
): Promise<any> {
  const { minDurationMs, limit, orderBy } = args;

  logger.info('getSlowQueries', 'Analyzing slow queries');

  // Check if pg_stat_statements extension is available
  const extensionCheck = `
    SELECT COUNT(*) as count
    FROM pg_extension
    WHERE extname = 'pg_stat_statements'
  `;

  const extResult = await executeQuery(connection, logger, { query: extensionCheck });
  const hasExtension = parseInt(extResult.rows[0]?.count || '0', 10) > 0;

  if (!hasExtension) {
    return {
      error: 'pg_stat_statements extension not installed',
      recommendation: 'Install with: CREATE EXTENSION pg_stat_statements;',
      slowQueries: []
    };
  }

  const orderByMap = {
    total_time: 'total_exec_time DESC',
    mean_time: 'mean_exec_time DESC',
    calls: 'calls DESC'
  };

  const query = `
    SELECT
      query,
      calls,
      total_exec_time as total_time_ms,
      mean_exec_time as mean_time_ms,
      min_exec_time as min_time_ms,
      max_exec_time as max_time_ms,
      stddev_exec_time as stddev_time_ms,
      rows,
      CASE WHEN calls > 0 THEN rows::numeric / calls ELSE 0 END as rows_per_call,
      100.0 * shared_blks_hit / NULLIF(shared_blks_hit + shared_blks_read, 0) as cache_hit_percent
    FROM pg_stat_statements
    WHERE mean_exec_time >= $1
      AND query NOT LIKE '%pg_stat_statements%'
    ORDER BY ${orderByMap[orderBy]}
    LIMIT $2
  `;

  const result = await executeQuery(connection, logger, {
    query,
    params: [minDurationMs, limit]
  });

  const slowQueries = result.rows.map(row => {
    const meanTime = parseFloat(row.mean_time_ms || '0');
    const cacheHit = parseFloat(row.cache_hit_percent || '0');

    const recommendations: string[] = [];

    if (meanTime > 1000) {
      recommendations.push('⚠ Very slow query (>1s average) - investigate and optimize');
    }

    if (cacheHit < 90) {
      recommendations.push(`⚠ Low cache hit ratio (${cacheHit.toFixed(1)}%) - data mostly from disk`);
    }

    if (row.query.toLowerCase().includes('select *')) {
      recommendations.push('Consider selecting only needed columns instead of SELECT *');
    }

    const rowsPerCall = parseFloat(row.rows_per_call || '0');
    if (rowsPerCall > 1000) {
      recommendations.push(`High rows per call (${Math.round(rowsPerCall)}) - consider pagination or filtering`);
    }

    return {
      query: row.query.substring(0, 500),
      calls: parseInt(row.calls, 10),
      totalTimeMs: parseFloat(row.total_time_ms).toFixed(2),
      meanTimeMs: meanTime.toFixed(2),
      minTimeMs: parseFloat(row.min_time_ms).toFixed(2),
      maxTimeMs: parseFloat(row.max_time_ms).toFixed(2),
      stddevTimeMs: parseFloat(row.stddev_time_ms || '0').toFixed(2),
      rows: parseInt(row.rows || '0', 10),
      rowsPerCall: Math.round(rowsPerCall),
      cacheHitPercent: cacheHit.toFixed(1),
      ...(recommendations.length > 0 && { recommendations })
    };
  });

  return {
    minDurationMs,
    queriesFound: slowQueries.length,
    slowQueries
  };
}

// Helper functions for health scoring

async function getCacheHitRatio(connection: DatabaseConnection, logger: Logger): Promise<any> {
  const query = `
    SELECT
      CASE
        WHEN (blks_hit + blks_read) = 0 THEN 1.0
        ELSE blks_hit::numeric / (blks_hit + blks_read)
      END as ratio
    FROM pg_stat_database
    WHERE datname = current_database()
  `;
  const result = await executeQuery(connection, logger, { query });
  return { ratio: parseFloat(result.rows[0]?.ratio || '1') };
}

async function getConnectionHealth(connection: DatabaseConnection, logger: Logger): Promise<any> {
  const query = `
    SELECT
      (SELECT COUNT(*) FROM pg_stat_activity)::numeric /
      NULLIF((SELECT setting::int FROM pg_settings WHERE name = 'max_connections'), 0) as usage
  `;
  const result = await executeQuery(connection, logger, { query });
  return { usage: parseFloat(result.rows[0]?.usage || '0') };
}

async function getBloatHealth(connection: DatabaseConnection, logger: Logger): Promise<any> {
  const query = `
    SELECT
      AVG(
        CASE
          WHEN pg_stat_get_live_tuples(c.oid) + pg_stat_get_dead_tuples(c.oid) = 0 THEN 0
          ELSE (pg_stat_get_dead_tuples(c.oid)::numeric /
            (pg_stat_get_live_tuples(c.oid) + pg_stat_get_dead_tuples(c.oid))) * 100
        END
      ) as avg_bloat,
      AVG(
        CASE
          WHEN pg_stat_get_live_tuples(c.oid) + pg_stat_get_dead_tuples(c.oid) = 0 THEN 0
          ELSE (pg_stat_get_dead_tuples(c.oid)::numeric /
            (pg_stat_get_live_tuples(c.oid) + pg_stat_get_dead_tuples(c.oid))) * 100
        END
      ) as avg_dead_percent
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.relname = s.relname
  `;
  const result = await executeQuery(connection, logger, { query });
  return {
    avgBloat: parseFloat(result.rows[0]?.avg_bloat || '0'),
    avgDeadPercent: parseFloat(result.rows[0]?.avg_dead_percent || '0')
  };
}

async function getIndexHealth(connection: DatabaseConnection, logger: Logger): Promise<any> {
  const query = `
    SELECT
      COUNT(*) FILTER (WHERE idx_scan = 0) as unused_count,
      SUM(pg_relation_size(indexrelid)) FILTER (WHERE idx_scan = 0) as unused_size,
      COUNT(*) FILTER (WHERE idx_scan > 0)::numeric / NULLIF(COUNT(*), 0) as usage_ratio
    FROM pg_stat_user_indexes
  `;
  const result = await executeQuery(connection, logger, { query });
  return {
    unusedCount: parseInt(result.rows[0]?.unused_count || '0', 10),
    unusedSizeMB: (parseInt(result.rows[0]?.unused_size || '0', 10) / 1024 / 1024),
    usage: parseFloat(result.rows[0]?.usage_ratio || '1')
  };
}

async function getActivityHealth(connection: DatabaseConnection, logger: Logger): Promise<any> {
  const query = `
    SELECT
      COUNT(*) FILTER (
        WHERE state = 'active'
        AND EXTRACT(EPOCH FROM (NOW() - query_start)) > 30
      ) as long_running
    FROM pg_stat_activity
  `;
  const result = await executeQuery(connection, logger, { query });
  return {
    longRunning: parseInt(result.rows[0]?.long_running || '0', 10)
  };
}

async function getReplicationHealth(connection: DatabaseConnection, logger: Logger): Promise<any> {
  try {
    const query = `
      SELECT COALESCE(MAX(EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp()))), 0) as lag
      FROM pg_stat_replication
    `;
    const result = await executeQuery(connection, logger, { query });
    return { lag: parseFloat(result.rows[0]?.lag || '0') };
  } catch {
    return { lag: 0 };
  }
}

function calculateCacheScore(ratio: number): any {
  const percent = ratio * 100;
  let score = 100;
  let status = 'excellent';

  if (percent < 80) {
    score = 50;
    status = 'poor';
  } else if (percent < 90) {
    score = 70;
    status = 'fair';
  } else if (percent < 95) {
    score = 85;
    status = 'good';
  }

  return { score, value: ratio, status };
}

function calculateIndexScore(usage: number): any {
  const percent = usage * 100;
  let score = Math.round(usage * 100);
  let status = percent > 90 ? 'excellent' : percent > 80 ? 'good' : percent > 70 ? 'fair' : 'poor';
  return { score, value: usage, status };
}

function calculateBloatScore(bloatPercent: number): any {
  let score = 100;
  let status = 'excellent';

  if (bloatPercent > 30) {
    score = 50;
    status = 'poor';
  } else if (bloatPercent > 20) {
    score = 70;
    status = 'fair';
  } else if (bloatPercent > 10) {
    score = 85;
    status = 'good';
  }

  return { score, value: bloatPercent, status };
}

function calculateConnectionScore(usage: number): any {
  let score = 100;
  let status = 'excellent';

  if (usage > 0.9) {
    score = 40;
    status = 'critical';
  } else if (usage > 0.7) {
    score = 60;
    status = 'fair';
  } else if (usage > 0.5) {
    score = 80;
    status = 'good';
  }

  return { score, value: usage, status };
}

function calculateDeadTuplesScore(deadPercent: number): any {
  return calculateBloatScore(deadPercent);
}

function calculateLongQueryScore(count: number): any {
  let score = 100;
  let status = 'excellent';

  if (count > 10) {
    score = 50;
    status = 'poor';
  } else if (count > 5) {
    score = 70;
    status = 'fair';
  } else if (count > 2) {
    score = 85;
    status = 'good';
  }

  return { score, value: count, status };
}

function calculateReplicationScore(lag: number): any {
  if (lag === 0) {
    return { score: 100, value: 0, status: 'n/a' };
  }

  let score = 100;
  let status = 'excellent';

  if (lag > 60) {
    score = 50;
    status = 'critical';
  } else if (lag > 10) {
    score = 70;
    status = 'fair';
  } else if (lag > 5) {
    score = 85;
    status = 'good';
  }

  return { score, value: lag, status };
}

export const maintenanceTools = {
  analyzeTableBloat: {
    schema: AnalyzeTableBloatSchema,
    handler: analyzeTableBloat
  },
  suggestVacuum: {
    schema: SuggestVacuumSchema,
    handler: suggestVacuum
  },
  getHealthScore: {
    schema: GetHealthScoreSchema,
    handler: getHealthScore
  },
  getSlowQueries: {
    schema: GetSlowQueriesSchema,
    handler: getSlowQueries
  }
};
