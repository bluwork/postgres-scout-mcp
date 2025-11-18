import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { sanitizeIdentifier } from '../utils/sanitize.js';

const SuggestIndexesSchema = z.object({
  schema: z.string().optional().default('public'),
  table: z.string().optional(),
  minSeqScans: z.number().optional().default(100),
  minRowsPerScan: z.number().optional().default(1000),
  includePartialIndexes: z.boolean().optional().default(false),
  includeCoveringIndexes: z.boolean().optional().default(false),
  analyzeQueries: z.boolean().optional().default(true)
});

const SuggestPartitioningSchema = z.object({
  schema: z.string().optional().default('public'),
  table: z.string(),
  minRowsThreshold: z.number().optional().default(1000000),
  analyzeQueryPatterns: z.boolean().optional().default(true),
  targetPartitionSize: z.string().optional().default('1GB')
});

const DetectAnomaliesSchema = z.object({
  type: z.enum(['query_performance', 'data_volume', 'connections', 'errors', 'all']).optional().default('all'),
  schema: z.string().optional().default('public'),
  table: z.string().optional(),
  timeWindow: z.string().optional().default('24h'),
  sensitivityLevel: z.enum(['low', 'medium', 'high']).optional().default('medium'),
  zScoreThreshold: z.number().optional().default(2)
});

const OptimizeQuerySchema = z.object({
  query: z.string(),
  includeRewrite: z.boolean().optional().default(true),
  includeIndexes: z.boolean().optional().default(true),
  targetTimeMs: z.number().optional()
});

export async function suggestIndexes(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof SuggestIndexesSchema>
): Promise<any> {
  const { schema, table, minSeqScans, minRowsPerScan, analyzeQueries } = args;

  logger.info('suggestIndexes', 'Analyzing index opportunities', { schema, table });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const tableFilter = table ? `AND relname = $2` : '';
  const params = table ? [sanitizedSchema, sanitizeIdentifier(table)] : [sanitizedSchema];

  // Find tables with high sequential scan activity
  const seqScanQuery = `
    SELECT
      schemaname,
      relname as table_name,
      seq_scan,
      seq_tup_read,
      idx_scan,
      idx_tup_fetch,
      n_live_tup as row_count,
      CASE
        WHEN seq_scan = 0 THEN 0
        ELSE ROUND(seq_tup_read::numeric / seq_scan, 0)
      END as avg_rows_per_scan,
      pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) as table_size
    FROM pg_stat_user_tables
    WHERE schemaname = $1
      ${tableFilter}
      AND seq_scan >= ${minSeqScans}
      AND seq_tup_read / NULLIF(seq_scan, 0) >= ${minRowsPerScan}
    ORDER BY seq_tup_read DESC
    LIMIT 50
  `;

  // Find foreign keys without indexes
  const fkWithoutIndexQuery = `
    SELECT
      tc.table_schema,
      tc.table_name,
      kcu.column_name,
      ccu.table_name as referenced_table,
      ccu.column_name as referenced_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name
      AND tc.table_schema = ccu.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = $1
      ${tableFilter ? tableFilter.replace('relname', 'tc.table_name') : ''}
      AND NOT EXISTS (
        SELECT 1
        FROM pg_indexes pi
        WHERE pi.schemaname = tc.table_schema
          AND pi.tablename = tc.table_name
          AND pi.indexdef LIKE '%(' || kcu.column_name || ')%'
      )
  `;

  // Find columns frequently used in WHERE clauses (from pg_stat_statements if available)
  const queryPatternQuery = analyzeQueries ? `
    SELECT
      queryid,
      query,
      calls,
      mean_exec_time,
      total_exec_time
    FROM pg_stat_statements
    WHERE query ILIKE '%WHERE%'
      AND query ILIKE '%${sanitizedSchema}%'
      ${table ? `AND query ILIKE '%${sanitizeIdentifier(table)}%'` : ''}
      AND calls > 10
    ORDER BY total_exec_time DESC
    LIMIT 20
  ` : null;

  // Get existing indexes for analysis
  const existingIndexesQuery = `
    SELECT
      schemaname,
      tablename,
      indexname,
      indexdef,
      pg_relation_size(schemaname || '.' || indexname) as size_bytes
    FROM pg_indexes
    WHERE schemaname = $1
      ${tableFilter ? tableFilter.replace('relname', 'tablename') : ''}
    ORDER BY pg_relation_size(schemaname || '.' || indexname) DESC
  `;

  // Get index usage stats
  const indexUsageQuery = `
    SELECT
      schemaname,
      relname as table_name,
      indexrelname as index_name,
      idx_scan,
      idx_tup_read,
      idx_tup_fetch,
      pg_size_pretty(pg_relation_size(indexrelid)) as index_size
    FROM pg_stat_user_indexes
    WHERE schemaname = $1
      ${tableFilter ? tableFilter.replace('relname', 'relname') : ''}
    ORDER BY idx_scan ASC
  `;

  const [seqScanResult, fkResult, existingResult, usageResult] = await Promise.all([
    executeQuery(connection, logger, { query: seqScanQuery, params }),
    executeQuery(connection, logger, { query: fkWithoutIndexQuery, params }),
    executeQuery(connection, logger, { query: existingIndexesQuery, params }),
    executeQuery(connection, logger, { query: indexUsageQuery, params })
  ]);

  let queryPatternResult: { rows: any[] } = { rows: [] };
  if (queryPatternQuery) {
    try {
      queryPatternResult = await executeQuery(connection, logger, { query: queryPatternQuery, params: [] });
    } catch {
      // pg_stat_statements might not be available
    }
  }

  const suggestions: any[] = [];

  // Analyze sequential scans and suggest indexes
  for (const row of seqScanResult.rows) {
    const avgRowsPerScan = parseInt(row.avg_rows_per_scan || '0', 10);
    const seqScans = parseInt(row.seq_scan || '0', 10);
    const idxScans = parseInt(row.idx_scan || '0', 10);

    // Only suggest if sequential scans significantly outnumber index scans
    if (seqScans > idxScans * 2 && avgRowsPerScan > minRowsPerScan) {
      const impact = avgRowsPerScan > 10000 ? 'critical' : avgRowsPerScan > 5000 ? 'high' : 'medium';

      suggestions.push({
        table: row.table_name,
        type: 'sequential_scan',
        impact,
        reason: `High sequential scan activity: ${seqScans.toLocaleString()} scans reading ${avgRowsPerScan.toLocaleString()} avg rows`,
        metrics: {
          seqScans,
          avgRowsPerScan,
          idxScans,
          tableSize: row.table_size,
          rowCount: parseInt(row.row_count || '0', 10)
        },
        recommendation: 'Analyze common WHERE clause columns for this table and add appropriate indexes',
        notes: [
          'Run EXPLAIN ANALYZE on slow queries to identify filter columns',
          'Consider composite indexes for multi-column filters'
        ]
      });
    }
  }

  // Add FK without index suggestions
  for (const row of fkResult.rows) {
    suggestions.push({
      table: row.table_name,
      columns: [row.column_name],
      type: 'foreign_key',
      impact: 'critical',
      reason: `Foreign key to ${row.referenced_table}(${row.referenced_column}) without index`,
      estimatedSpeedup: '10-100x for JOINs and cascading operations',
      createStatement: `CREATE INDEX CONCURRENTLY idx_${row.table_name}_${row.column_name} ON ${schema}.${row.table_name} (${row.column_name});`,
      notes: [
        'Missing FK indexes cause slow JOINs',
        'Cascading DELETEs/UPDATEs will be very slow',
        'Use CONCURRENTLY to avoid blocking writes'
      ]
    });
  }

  // Analyze existing indexes for issues
  const existingAnalysis = {
    totalIndexes: existingResult.rows.length,
    unusedIndexes: 0,
    duplicateIndexes: 0,
    recommendations: [] as string[]
  };

  const indexDefs = new Map<string, string[]>();

  for (const row of existingResult.rows) {
    // Track for duplicate detection
    const key = `${row.tablename}:${row.indexdef.replace(/CREATE.*ON/, '').trim()}`;
    if (!indexDefs.has(key)) {
      indexDefs.set(key, []);
    }
    indexDefs.get(key)!.push(row.indexname);
  }

  // Find duplicates
  for (const [, indexes] of indexDefs) {
    if (indexes.length > 1) {
      existingAnalysis.duplicateIndexes++;
      existingAnalysis.recommendations.push(
        `Duplicate indexes detected: ${indexes.join(', ')} - consider dropping all but one`
      );
    }
  }

  // Find unused indexes
  for (const row of usageResult.rows) {
    const scans = parseInt(row.idx_scan || '0', 10);
    if (scans === 0 && !row.index_name.includes('pkey') && !row.index_name.includes('_unique')) {
      existingAnalysis.unusedIndexes++;
      existingAnalysis.recommendations.push(
        `DROP INDEX CONCURRENTLY ${schema}.${row.index_name}; -- never used, ${row.index_size}`
      );
    }
  }

  // Parse query patterns if available
  const queryInsights: any[] = [];
  if (queryPatternResult.rows.length > 0) {
    for (const row of queryPatternResult.rows) {
      const query = row.query;
      // Extract potential column names from WHERE clauses
      const whereMatch = query.match(/WHERE\s+([^;]+)/i);
      if (whereMatch) {
        queryInsights.push({
          queryFragment: whereMatch[0].substring(0, 200),
          calls: parseInt(row.calls || '0', 10),
          meanTime: parseFloat(row.mean_exec_time || '0').toFixed(2) + 'ms',
          hint: 'Analyze columns in this WHERE clause for indexing'
        });
      }
    }
  }

  return {
    schema,
    table: table || 'all tables',
    suggestions,
    existingIndexAnalysis: existingAnalysis,
    queryInsights: queryInsights.length > 0 ? queryInsights : undefined,
    summary: {
      suggestionsCount: suggestions.length,
      criticalCount: suggestions.filter(s => s.impact === 'critical').length,
      highCount: suggestions.filter(s => s.impact === 'high').length,
      mediumCount: suggestions.filter(s => s.impact === 'medium').length
    }
  };
}

export async function suggestPartitioning(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof SuggestPartitioningSchema>
): Promise<any> {
  const { schema, table, minRowsThreshold, targetPartitionSize } = args;

  logger.info('suggestPartitioning', 'Analyzing partitioning opportunities', { schema, table });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);

  // Get table stats
  const tableStatsQuery = `
    SELECT
      n_live_tup as row_count,
      pg_total_relation_size($1 || '.' || $2) as total_bytes,
      pg_size_pretty(pg_total_relation_size($1 || '.' || $2)) as total_size,
      pg_size_pretty(pg_relation_size($1 || '.' || $2)) as table_size,
      pg_size_pretty(pg_indexes_size(($1 || '.' || $2)::regclass)) as index_size
    FROM pg_stat_user_tables
    WHERE schemaname = $1 AND relname = $2
  `;

  // Get columns with data types suitable for partitioning
  const columnsQuery = `
    SELECT
      column_name,
      data_type,
      is_nullable
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    ORDER BY ordinal_position
  `;

  // Check if table is already partitioned
  const partitionCheckQuery = `
    SELECT
      relkind,
      CASE relkind
        WHEN 'p' THEN 'partitioned'
        WHEN 'r' THEN 'regular'
        ELSE 'other'
      END as table_type
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname = $2
  `;

  const [statsResult, columnsResult, partitionResult] = await Promise.all([
    executeQuery(connection, logger, { query: tableStatsQuery, params: [sanitizedSchema, sanitizedTable] }),
    executeQuery(connection, logger, { query: columnsQuery, params: [sanitizedSchema, sanitizedTable] }),
    executeQuery(connection, logger, { query: partitionCheckQuery, params: [sanitizedSchema, sanitizedTable] })
  ]);

  if (statsResult.rows.length === 0) {
    return {
      error: `Table ${schema}.${table} not found`
    };
  }

  const stats = statsResult.rows[0];
  const rowCount = parseInt(stats.row_count || '0', 10);
  const totalBytes = parseInt(stats.total_bytes || '0', 10);

  // Check if already partitioned
  if (partitionResult.rows[0]?.table_type === 'partitioned') {
    return {
      table: `${schema}.${table}`,
      status: 'already_partitioned',
      message: 'This table is already partitioned',
      currentSize: stats.total_size,
      rowCount
    };
  }

  // Check if table meets threshold
  if (rowCount < minRowsThreshold) {
    return {
      table: `${schema}.${table}`,
      status: 'not_recommended',
      message: `Table has ${rowCount.toLocaleString()} rows, below threshold of ${minRowsThreshold.toLocaleString()}`,
      currentSize: stats.total_size,
      rowCount,
      recommendation: 'Partitioning adds complexity without significant benefit for smaller tables'
    };
  }

  // Analyze columns for partitioning candidates
  const columns = columnsResult.rows;
  const candidates: any[] = [];

  // Find timestamp/date columns (best for range partitioning)
  const temporalColumns = columns.filter((c: any) =>
    ['timestamp', 'timestamptz', 'date'].includes(c.data_type)
  );

  for (const col of temporalColumns) {
    // Get value distribution
    const distQuery = `
      SELECT
        MIN(${col.column_name}) as min_val,
        MAX(${col.column_name}) as max_val,
        COUNT(DISTINCT DATE_TRUNC('month', ${col.column_name})) as distinct_months
      FROM ${sanitizedSchema}.${sanitizedTable}
      WHERE ${col.column_name} IS NOT NULL
      LIMIT 1
    `;

    try {
      const distResult = await executeQuery(connection, logger, { query: distQuery, params: [] });
      if (distResult.rows.length > 0) {
        const dist = distResult.rows[0];
        candidates.push({
          column: col.column_name,
          dataType: col.data_type,
          strategy: 'range',
          minValue: dist.min_val,
          maxValue: dist.max_val,
          distinctMonths: parseInt(dist.distinct_months || '0', 10),
          score: 90 // Temporal columns are usually best
        });
      }
    } catch {
      // Skip if query fails
    }
  }

  // Find integer columns that could be used for hash partitioning
  const intColumns = columns.filter((c: any) =>
    ['integer', 'bigint', 'smallint'].includes(c.data_type)
  );

  for (const col of intColumns) {
    if (col.column_name.includes('id')) {
      candidates.push({
        column: col.column_name,
        dataType: col.data_type,
        strategy: 'hash',
        score: 60
      });
    }
  }

  // Find low-cardinality columns for list partitioning
  for (const col of columns) {
    if (['character varying', 'text', 'varchar'].includes(col.data_type)) {
      const cardinalityQuery = `
        SELECT COUNT(DISTINCT ${col.column_name}) as cardinality
        FROM ${sanitizedSchema}.${sanitizedTable}
      `;

      try {
        const cardResult = await executeQuery(connection, logger, { query: cardinalityQuery, params: [] });
        const cardinality = parseInt(cardResult.rows[0]?.cardinality || '0', 10);

        if (cardinality > 0 && cardinality <= 20) {
          candidates.push({
            column: col.column_name,
            dataType: col.data_type,
            strategy: 'list',
            cardinality,
            score: 70
          });
        }
      } catch {
        // Skip if query fails
      }
    }
  }

  // Sort by score
  candidates.sort((a, b) => b.score - a.score);

  // Generate recommendation
  const recommendation = candidates.length > 0 ? generatePartitionRecommendation(
    schema, table, candidates[0], rowCount, totalBytes, targetPartitionSize
  ) : null;

  return {
    table: `${schema}.${table}`,
    currentSize: stats.total_size,
    tableSize: stats.table_size,
    indexSize: stats.index_size,
    rowCount,
    status: 'recommended',
    candidates,
    recommendation,
    alternativeStrategies: candidates.slice(1, 3).map(c => ({
      strategy: c.strategy,
      partitionKey: c.column,
      rationale: getStrategyRationale(c)
    })),
    warnings: [
      'Partitioning requires table recreation - plan for downtime',
      'Ensure application queries include partition key in WHERE clauses',
      'Consider impact on existing indexes and constraints'
    ]
  };
}

function generatePartitionRecommendation(
  schema: string,
  table: string,
  candidate: any,
  rowCount: number,
  totalBytes: number,
  targetPartitionSize: string
): any {
  const targetBytes = parseTargetSize(targetPartitionSize);
  const partitionCount = Math.ceil(totalBytes / targetBytes);

  if (candidate.strategy === 'range' && candidate.distinctMonths) {
    const interval = candidate.distinctMonths > 24 ? 'monthly' : 'quarterly';

    return {
      strategy: 'range',
      partitionKey: candidate.column,
      interval,
      rationale: [
        `Time-series data spanning ${candidate.distinctMonths} months`,
        'Enables partition pruning for date-filtered queries',
        'Easy archival of old data by dropping partitions'
      ],
      benefits: [
        'Query performance: 10-100x for time-filtered queries',
        'Maintenance: VACUUM/ANALYZE per partition',
        'Archival: DROP old partitions instantly'
      ],
      migrationPlan: {
        steps: [
          '1. Create partitioned table with identical schema',
          '2. Create partitions covering data range',
          '3. Copy data in batches during maintenance window',
          '4. Recreate indexes on partitioned table',
          '5. Rename tables to swap',
          '6. Drop old table after verification'
        ],
        estimatedDuration: rowCount > 10000000 ? '2-8 hours' : '30 min - 2 hours',
        downtime: '5-15 minutes for final swap'
      },
      ddl: generateRangePartitionDDL(schema, table, candidate.column, interval)
    };
  } else if (candidate.strategy === 'hash') {
    return {
      strategy: 'hash',
      partitionKey: candidate.column,
      partitions: Math.min(partitionCount, 16),
      rationale: [
        'Even data distribution across partitions',
        'Good for parallel query execution',
        'No need to manage partition boundaries'
      ],
      ddl: generateHashPartitionDDL(schema, table, candidate.column, Math.min(partitionCount, 16))
    };
  } else if (candidate.strategy === 'list') {
    return {
      strategy: 'list',
      partitionKey: candidate.column,
      rationale: [
        `Low cardinality column (${candidate.cardinality} distinct values)`,
        'Natural data segregation',
        'Easy to add new partitions for new values'
      ],
      ddl: generateListPartitionDDL(schema, table, candidate.column)
    };
  }

  return null;
}

function parseTargetSize(size: string): number {
  const match = size.match(/^(\d+)(GB|MB|KB)?$/i);
  if (!match) return 1073741824; // Default 1GB

  const num = parseInt(match[1], 10);
  const unit = (match[2] || 'GB').toUpperCase();

  switch (unit) {
    case 'KB': return num * 1024;
    case 'MB': return num * 1024 * 1024;
    case 'GB': return num * 1024 * 1024 * 1024;
    default: return num * 1024 * 1024 * 1024;
  }
}

function getStrategyRationale(candidate: any): string {
  switch (candidate.strategy) {
    case 'range': return 'Good for time-series queries with date filtering';
    case 'hash': return 'Good for parallel queries across all data';
    case 'list': return `Natural grouping by ${candidate.cardinality} distinct values`;
    default: return '';
  }
}

function generateRangePartitionDDL(schema: string, table: string, column: string, interval: string): string[] {
  return [
    `-- Create partitioned table`,
    `CREATE TABLE ${schema}.${table}_partitioned (`,
    `  -- Copy columns from original table`,
    `  LIKE ${schema}.${table} INCLUDING ALL`,
    `) PARTITION BY RANGE (${column});`,
    ``,
    `-- Create partitions (example for ${interval} intervals)`,
    `CREATE TABLE ${schema}.${table}_2024_01 PARTITION OF ${schema}.${table}_partitioned`,
    `  FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');`,
    ``,
    `-- Add more partitions as needed`,
    `-- CREATE TABLE ${schema}.${table}_2024_02 PARTITION OF ${schema}.${table}_partitioned`,
    `--   FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');`
  ];
}

function generateHashPartitionDDL(schema: string, table: string, column: string, partitions: number): string[] {
  const ddl = [
    `-- Create partitioned table`,
    `CREATE TABLE ${schema}.${table}_partitioned (`,
    `  LIKE ${schema}.${table} INCLUDING ALL`,
    `) PARTITION BY HASH (${column});`,
    ``
  ];

  for (let i = 0; i < partitions; i++) {
    ddl.push(`CREATE TABLE ${schema}.${table}_p${i} PARTITION OF ${schema}.${table}_partitioned`);
    ddl.push(`  FOR VALUES WITH (MODULUS ${partitions}, REMAINDER ${i});`);
  }

  return ddl;
}

function generateListPartitionDDL(schema: string, table: string, column: string): string[] {
  return [
    `-- Create partitioned table`,
    `CREATE TABLE ${schema}.${table}_partitioned (`,
    `  LIKE ${schema}.${table} INCLUDING ALL`,
    `) PARTITION BY LIST (${column});`,
    ``,
    `-- Create partitions for each distinct value`,
    `-- First, identify distinct values:`,
    `-- SELECT DISTINCT ${column} FROM ${schema}.${table};`,
    ``,
    `-- Then create partitions:`,
    `-- CREATE TABLE ${schema}.${table}_value1 PARTITION OF ${schema}.${table}_partitioned`,
    `--   FOR VALUES IN ('value1');`
  ];
}

export async function detectAnomalies(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof DetectAnomaliesSchema>
): Promise<any> {
  const { type, schema, table, timeWindow, sensitivityLevel, zScoreThreshold } = args;

  logger.info('detectAnomalies', 'Detecting anomalies', { type, schema, timeWindow });

  const anomalies: any[] = [];
  const sensitivityMultiplier = sensitivityLevel === 'high' ? 0.5 : sensitivityLevel === 'low' ? 2 : 1;
  const threshold = zScoreThreshold * sensitivityMultiplier;

  // Parse time window
  const windowMatch = timeWindow.match(/^(\d+)(h|d|w|m)$/);
  const windowHours = windowMatch
    ? parseInt(windowMatch[1], 10) * (
        windowMatch[2] === 'h' ? 1 :
        windowMatch[2] === 'd' ? 24 :
        windowMatch[2] === 'w' ? 168 :
        windowMatch[2] === 'm' ? 720 : 24
      )
    : 24;

  // Query performance anomalies
  if (type === 'all' || type === 'query_performance') {
    const queryAnomaliesQuery = `
      WITH query_stats AS (
        SELECT
          queryid,
          query,
          calls,
          mean_exec_time,
          stddev_exec_time,
          total_exec_time,
          rows
        FROM pg_stat_statements
        WHERE calls > 10
      ),
      stats_analysis AS (
        SELECT
          *,
          AVG(mean_exec_time) OVER() as global_avg,
          STDDEV(mean_exec_time) OVER() as global_stddev
        FROM query_stats
      )
      SELECT
        queryid,
        LEFT(query, 200) as query_fragment,
        calls,
        ROUND(mean_exec_time::numeric, 2) as mean_time_ms,
        ROUND(total_exec_time::numeric, 2) as total_time_ms,
        ROUND(global_avg::numeric, 2) as avg_mean_time,
        ROUND(global_stddev::numeric, 2) as stddev_time,
        CASE
          WHEN global_stddev = 0 THEN 0
          ELSE ROUND(((mean_exec_time - global_avg) / global_stddev)::numeric, 2)
        END as z_score
      FROM stats_analysis
      WHERE global_stddev > 0
        AND ABS((mean_exec_time - global_avg) / global_stddev) > ${threshold}
      ORDER BY ABS((mean_exec_time - global_avg) / global_stddev) DESC
      LIMIT 20
    `;

    try {
      const result = await executeQuery(connection, logger, { query: queryAnomaliesQuery, params: [] });

      for (const row of result.rows) {
        const zScore = parseFloat(row.z_score || '0');
        const severity = Math.abs(zScore) > 4 ? 'critical' : Math.abs(zScore) > 3 ? 'high' : 'medium';

        anomalies.push({
          type: 'query_performance',
          severity,
          description: zScore > 0 ? 'Unusually slow query' : 'Unusually fast query',
          details: {
            query: row.query_fragment,
            meanTimeMs: row.mean_time_ms,
            avgMeanTimeMs: row.avg_mean_time,
            zScore: row.z_score,
            calls: parseInt(row.calls || '0', 10),
            totalTimeMs: row.total_time_ms
          },
          recommendations: zScore > 0 ? [
            'Check for missing indexes',
            'Analyze query plan with EXPLAIN ANALYZE',
            'Review recent schema or data changes'
          ] : [
            'Query is performing better than average',
            'Consider using similar patterns elsewhere'
          ]
        });
      }
    } catch {
      // pg_stat_statements might not be available
    }
  }

  // Connection anomalies
  if (type === 'all' || type === 'connections') {
    const connectionQuery = `
      SELECT
        state,
        COUNT(*) as count,
        MAX(EXTRACT(EPOCH FROM (NOW() - backend_start))) as max_duration_sec
      FROM pg_stat_activity
      WHERE backend_type = 'client backend'
      GROUP BY state
    `;

    const maxConnectionsQuery = `
      SELECT setting::int as max_connections
      FROM pg_settings
      WHERE name = 'max_connections'
    `;

    const [connResult, maxResult] = await Promise.all([
      executeQuery(connection, logger, { query: connectionQuery, params: [] }),
      executeQuery(connection, logger, { query: maxConnectionsQuery, params: [] })
    ]);

    const maxConnections = parseInt(maxResult.rows[0]?.max_connections || '100', 10);
    let totalConnections = 0;
    let idleInTransaction = 0;

    for (const row of connResult.rows) {
      const count = parseInt(row.count || '0', 10);
      totalConnections += count;

      if (row.state === 'idle in transaction') {
        idleInTransaction = count;
      }
    }

    const connectionRatio = totalConnections / maxConnections;

    if (connectionRatio > 0.8) {
      anomalies.push({
        type: 'connections',
        severity: connectionRatio > 0.95 ? 'critical' : 'high',
        description: 'High connection usage',
        details: {
          currentConnections: totalConnections,
          maxConnections,
          usagePercent: (connectionRatio * 100).toFixed(1) + '%'
        },
        recommendations: [
          'Review connection pooling configuration',
          'Check for connection leaks',
          'Consider increasing max_connections'
        ]
      });
    }

    if (idleInTransaction > 5) {
      anomalies.push({
        type: 'connections',
        severity: idleInTransaction > 20 ? 'high' : 'medium',
        description: 'Idle in transaction connections',
        details: {
          idleInTransaction,
          issue: 'Connections holding transactions without activity'
        },
        recommendations: [
          'Review application transaction handling',
          'Set idle_in_transaction_session_timeout',
          'Check for uncommitted transactions'
        ]
      });
    }
  }

  // Table bloat anomalies
  if (type === 'all' || type === 'data_volume') {
    const sanitizedSchema = sanitizeIdentifier(schema);
    const tableFilter = table ? `AND relname = '${sanitizeIdentifier(table)}'` : '';

    const bloatQuery = `
      SELECT
        schemaname,
        relname as table_name,
        n_live_tup as live_tuples,
        n_dead_tup as dead_tuples,
        CASE
          WHEN n_live_tup + n_dead_tup = 0 THEN 0
          ELSE ROUND((n_dead_tup::numeric / (n_live_tup + n_dead_tup)) * 100, 2)
        END as dead_percent,
        last_vacuum,
        last_autovacuum
      FROM pg_stat_user_tables
      WHERE schemaname = '${sanitizedSchema}'
        ${tableFilter}
        AND n_dead_tup > 10000
      ORDER BY n_dead_tup DESC
      LIMIT 20
    `;

    const bloatResult = await executeQuery(connection, logger, { query: bloatQuery, params: [] });

    for (const row of bloatResult.rows) {
      const deadPercent = parseFloat(row.dead_percent || '0');

      if (deadPercent > 30) {
        anomalies.push({
          type: 'data_volume',
          severity: deadPercent > 50 ? 'critical' : 'high',
          description: 'High dead tuple ratio',
          details: {
            table: `${row.schemaname}.${row.table_name}`,
            liveTuples: parseInt(row.live_tuples || '0', 10),
            deadTuples: parseInt(row.dead_tuples || '0', 10),
            deadPercent: deadPercent + '%',
            lastVacuum: row.last_vacuum || 'never',
            lastAutovacuum: row.last_autovacuum || 'never'
          },
          recommendations: [
            `VACUUM ANALYZE ${row.schemaname}.${row.table_name};`,
            'Review autovacuum settings',
            'Consider VACUUM FULL for severe bloat (requires exclusive lock)'
          ]
        });
      }
    }
  }

  // Error rate anomalies (from pg_stat_database)
  if (type === 'all' || type === 'errors') {
    const errorQuery = `
      SELECT
        datname,
        xact_commit,
        xact_rollback,
        CASE
          WHEN xact_commit + xact_rollback = 0 THEN 0
          ELSE ROUND((xact_rollback::numeric / (xact_commit + xact_rollback)) * 100, 2)
        END as rollback_percent,
        conflicts,
        deadlocks
      FROM pg_stat_database
      WHERE datname = current_database()
    `;

    const errorResult = await executeQuery(connection, logger, { query: errorQuery, params: [] });

    if (errorResult.rows.length > 0) {
      const row = errorResult.rows[0];
      const rollbackPercent = parseFloat(row.rollback_percent || '0');
      const deadlocks = parseInt(row.deadlocks || '0', 10);

      if (rollbackPercent > 5) {
        anomalies.push({
          type: 'errors',
          severity: rollbackPercent > 20 ? 'critical' : rollbackPercent > 10 ? 'high' : 'medium',
          description: 'High transaction rollback rate',
          details: {
            commits: parseInt(row.xact_commit || '0', 10),
            rollbacks: parseInt(row.xact_rollback || '0', 10),
            rollbackPercent: rollbackPercent + '%'
          },
          recommendations: [
            'Review application error handling',
            'Check for constraint violations',
            'Analyze transaction patterns'
          ]
        });
      }

      if (deadlocks > 0) {
        anomalies.push({
          type: 'errors',
          severity: deadlocks > 10 ? 'high' : 'medium',
          description: 'Deadlocks detected',
          details: {
            deadlockCount: deadlocks
          },
          recommendations: [
            'Review transaction lock ordering',
            'Check for long-running transactions',
            'Consider using advisory locks'
          ]
        });
      }
    }
  }

  // Sort by severity
  const severityOrder = { critical: 0, high: 1, medium: 2, low: 3 };
  anomalies.sort((a, b) => severityOrder[a.severity as keyof typeof severityOrder] - severityOrder[b.severity as keyof typeof severityOrder]);

  return {
    timeWindow,
    sensitivityLevel,
    anomaliesFound: anomalies.length,
    anomalies,
    summary: {
      bySeverity: {
        critical: anomalies.filter(a => a.severity === 'critical').length,
        high: anomalies.filter(a => a.severity === 'high').length,
        medium: anomalies.filter(a => a.severity === 'medium').length,
        low: anomalies.filter(a => a.severity === 'low').length
      },
      byType: {
        query_performance: anomalies.filter(a => a.type === 'query_performance').length,
        connections: anomalies.filter(a => a.type === 'connections').length,
        data_volume: anomalies.filter(a => a.type === 'data_volume').length,
        errors: anomalies.filter(a => a.type === 'errors').length
      },
      healthStatus: anomalies.some(a => a.severity === 'critical') ? 'critical' :
                    anomalies.some(a => a.severity === 'high') ? 'attention_needed' :
                    anomalies.length > 0 ? 'minor_issues' : 'healthy'
    }
  };
}

export async function optimizeQuery(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof OptimizeQuerySchema>
): Promise<any> {
  const { query, includeRewrite, includeIndexes, targetTimeMs } = args;

  logger.info('optimizeQuery', 'Analyzing query for optimization');

  // Validate query is a SELECT (we only optimize read queries)
  const trimmedQuery = query.trim().toLowerCase();
  if (!trimmedQuery.startsWith('select')) {
    return {
      error: 'Only SELECT queries can be optimized',
      query: query.substring(0, 100)
    };
  }

  // Get execution plan
  const explainQuery = `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${query}`;

  let planResult;
  try {
    planResult = await executeQuery(connection, logger, { query: explainQuery, params: [] });
  } catch (error) {
    return {
      error: `Query execution failed: ${error instanceof Error ? error.message : String(error)}`,
      query: query.substring(0, 200)
    };
  }

  const plan = planResult.rows[0]['QUERY PLAN'][0];
  const planningTime = plan['Planning Time'];
  const executionTime = plan['Execution Time'];
  const totalTime = planningTime + executionTime;

  const issues: any[] = [];
  const optimizations: any[] = [];

  // Analyze the plan recursively
  analyzeNode(plan.Plan, issues, 0);

  // Generate optimizations based on issues
  let priority = 1;

  for (const issue of issues) {
    if (issue.type === 'sequential_scan' && includeIndexes) {
      optimizations.push({
        type: 'add_index',
        priority: priority++,
        description: `Add index on ${issue.table} for filter columns`,
        table: issue.table,
        impact: issue.impact,
        details: issue.description,
        notes: [
          'Analyze the filter condition to determine which columns need indexing',
          'Use EXPLAIN to identify the filter columns',
          'Consider composite index if multiple columns are filtered'
        ]
      });
    }

    if (issue.type === 'sort_memory') {
      optimizations.push({
        type: 'increase_work_mem',
        priority: priority++,
        description: 'Increase work_mem to avoid disk sorts',
        impact: issue.impact,
        sql: `SET work_mem = '256MB'; -- or adjust in postgresql.conf`,
        notes: [
          'Be careful with high work_mem on systems with many connections',
          'Consider setting per-session for specific queries'
        ]
      });
    }

    if (issue.type === 'nested_loop' && issue.rows > 1000) {
      optimizations.push({
        type: 'query_rewrite',
        priority: priority++,
        description: 'Consider rewriting nested loop join',
        impact: issue.impact,
        details: `Nested loop processing ${issue.rows} rows`,
        notes: [
          'Check if hash join or merge join would be better',
          'Ensure join columns have indexes',
          'Consider query restructuring'
        ]
      });
    }
  }

  // Query rewrite suggestions
  if (includeRewrite) {
    // Check for SELECT *
    if (query.toLowerCase().includes('select *')) {
      optimizations.push({
        type: 'query_rewrite',
        priority: priority++,
        description: 'Replace SELECT * with specific columns',
        rationale: 'Reduces I/O and memory usage, enables index-only scans',
        before: 'SELECT * FROM ...',
        after: 'SELECT column1, column2, ... FROM ...',
        impact: 'medium'
      });
    }

    // Check for missing LIMIT
    if (!query.toLowerCase().includes('limit') && plan.Plan['Actual Rows'] > 1000) {
      optimizations.push({
        type: 'query_rewrite',
        priority: priority++,
        description: 'Add LIMIT clause',
        rationale: `Query returned ${plan.Plan['Actual Rows']} rows`,
        notes: [
          'If you only need a subset of results, add LIMIT',
          'Consider pagination for large result sets'
        ],
        impact: 'low'
      });
    }

    // Check for inefficient OR conditions
    if (query.toLowerCase().includes(' or ')) {
      optimizations.push({
        type: 'query_rewrite',
        priority: priority++,
        description: 'Consider replacing OR with UNION',
        rationale: 'OR conditions can prevent index usage',
        before: 'SELECT ... WHERE a = 1 OR b = 2',
        after: 'SELECT ... WHERE a = 1 UNION ALL SELECT ... WHERE b = 2',
        notes: ['Only beneficial if each condition can use an index'],
        impact: 'medium'
      });
    }
  }

  // Sort optimizations by priority
  optimizations.sort((a, b) => a.priority - b.priority);

  // Estimate optimized time
  let estimatedOptimizedTime = totalTime;
  for (const opt of optimizations) {
    if (opt.impact === 'critical') estimatedOptimizedTime *= 0.1;
    else if (opt.impact === 'high') estimatedOptimizedTime *= 0.3;
    else if (opt.impact === 'medium') estimatedOptimizedTime *= 0.7;
  }

  const meetsTarget = targetTimeMs ? totalTime <= targetTimeMs : null;

  return {
    query: query.length > 500 ? query.substring(0, 500) + '...' : query,
    executionPlan: {
      planningTime: planningTime.toFixed(2) + 'ms',
      executionTime: executionTime.toFixed(2) + 'ms',
      totalTime: totalTime.toFixed(2) + 'ms'
    },
    targetTimeMs: targetTimeMs || null,
    meetsTarget,
    issues,
    optimizations,
    estimatedOptimizedTime: estimatedOptimizedTime.toFixed(2) + 'ms',
    summary: {
      issuesFound: issues.length,
      optimizationsAvailable: optimizations.length,
      criticalIssues: issues.filter(i => i.impact === 'critical').length,
      highImpactIssues: issues.filter(i => i.impact === 'high').length
    }
  };
}

function analyzeNode(node: any, issues: any[], depth: number): void {
  if (!node) return;

  const nodeType = node['Node Type'];
  const actualRows = node['Actual Rows'] || 0;
  const planRows = node['Plan Rows'] || 0;
  const actualTime = node['Actual Total Time'] || 0;

  // Check for sequential scans on large tables
  if (nodeType === 'Seq Scan' && actualRows > 1000) {
    const impact = actualRows > 100000 ? 'critical' : actualRows > 10000 ? 'high' : 'medium';
    issues.push({
      type: 'sequential_scan',
      table: node['Relation Name'],
      rows: actualRows,
      impact,
      description: `Sequential scan on ${node['Relation Name']} reading ${actualRows.toLocaleString()} rows`
    });
  }

  // Check for sorts spilling to disk
  if (nodeType === 'Sort' && node['Sort Method']?.includes('external')) {
    issues.push({
      type: 'sort_memory',
      impact: 'high',
      description: `Sort operation spilling to disk (${node['Sort Space Used']} kB)`
    });
  }

  // Check for nested loops with high row counts
  if (nodeType === 'Nested Loop' && actualRows > 1000) {
    issues.push({
      type: 'nested_loop',
      rows: actualRows,
      impact: actualRows > 10000 ? 'high' : 'medium',
      description: `Nested loop join processing ${actualRows.toLocaleString()} rows`
    });
  }

  // Check for bad row estimates (can indicate stale statistics)
  if (planRows > 0 && actualRows > 0) {
    const ratio = actualRows / planRows;
    if (ratio > 10 || ratio < 0.1) {
      issues.push({
        type: 'estimate_error',
        impact: 'medium',
        description: `Row estimate off by ${ratio.toFixed(1)}x (planned: ${planRows}, actual: ${actualRows})`,
        recommendation: `ANALYZE the affected table to update statistics`
      });
    }
  }

  // Check for hash operations using too much memory
  if (nodeType === 'Hash' && node['Peak Memory Usage']) {
    const memoryKB = node['Peak Memory Usage'];
    if (memoryKB > 100000) { // > 100MB
      issues.push({
        type: 'hash_memory',
        impact: 'medium',
        description: `Hash operation using ${(memoryKB / 1024).toFixed(0)} MB`
      });
    }
  }

  // Recursively analyze child nodes
  if (node.Plans) {
    for (const childNode of node.Plans) {
      analyzeNode(childNode, issues, depth + 1);
    }
  }
}

export const optimizationTools = {
  suggestIndexes: {
    schema: SuggestIndexesSchema,
    handler: suggestIndexes
  },
  suggestPartitioning: {
    schema: SuggestPartitioningSchema,
    handler: suggestPartitioning
  },
  detectAnomalies: {
    schema: DetectAnomaliesSchema,
    handler: detectAnomalies
  },
  optimizeQuery: {
    schema: OptimizeQuerySchema,
    handler: optimizeQuery
  }
};
