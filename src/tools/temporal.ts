import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { escapeIdentifier, sanitizeIdentifier, validateUserWhereClause, validateInterval, validateOrderBy } from '../utils/sanitize.js';

const FindRecentSchema = z.object({
  table: z.string(),
  timestampColumn: z.string(),
  timeWindow: z.string().describe('PostgreSQL interval format, e.g., "7 days", "2 hours", "30 minutes"'),
  schema: z.string().optional().default('public'),
  where: z.string().optional(),
  limit: z.number().optional().default(100),
  orderBy: z.string().optional()
});

const AnalyzeTimeSeriesSchema = z.object({
  table: z.string(),
  timestampColumn: z.string(),
  valueColumn: z.string(),
  schema: z.string().optional().default('public'),
  groupBy: z.enum(['hour', 'day', 'week', 'month']).optional().default('day'),
  aggregation: z.enum(['sum', 'avg', 'count', 'min', 'max']).optional().default('sum'),
  startDate: z.string().optional(),
  endDate: z.string().optional(),
  includeMovingAverage: z.boolean().optional().default(true),
  movingAverageWindow: z.number().optional().default(7)
});

const DetectSeasonalitySchema = z.object({
  table: z.string(),
  timestampColumn: z.string(),
  valueColumn: z.string(),
  schema: z.string().optional().default('public'),
  groupBy: z.enum(['day_of_week', 'day_of_month', 'month', 'quarter']).optional().default('day_of_week'),
  minPeriods: z.number().optional().default(4)
});

export async function findRecent(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof FindRecentSchema>
): Promise<any> {
  const { table, timestampColumn, timeWindow, schema, where, limit, orderBy } = args;

  logger.info('findRecent', 'Finding recent records', { table, timeWindow });

  validateInterval(timeWindow);
  if (where) {
    validateUserWhereClause(where);
  }
  if (orderBy) {
    validateOrderBy(orderBy);
  }

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);
  const sanitizedTimestamp = sanitizeIdentifier(timestampColumn);

  const whereClause = where ? `AND (${where})` : '';
  const orderClause = orderBy || `${escapeIdentifier(sanitizedTimestamp)} DESC`;

  const query = `
    SELECT *
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    WHERE ${escapeIdentifier(sanitizedTimestamp)} >= NOW() - INTERVAL '${timeWindow}'
      ${whereClause}
    ORDER BY ${orderClause}
    LIMIT $1
  `;

  const countQuery = `
    SELECT
      COUNT(*) as rows_found,
      NOW() - INTERVAL '${timeWindow}' as threshold
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    WHERE ${escapeIdentifier(sanitizedTimestamp)} >= NOW() - INTERVAL '${timeWindow}'
      ${whereClause}
  `;

  const [result, countResult] = await Promise.all([
    executeQuery(connection, logger, { query, params: [limit] }),
    executeQuery(connection, logger, { query: countQuery })
  ]);

  return {
    table,
    schema,
    timestampColumn,
    timeWindow: `Last ${timeWindow}`,
    threshold: countResult.rows[0]?.threshold,
    rowsFound: parseInt(countResult.rows[0]?.rows_found || '0', 10),
    rows: result.rows
  };
}

export async function analyzeTimeSeries(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof AnalyzeTimeSeriesSchema>
): Promise<any> {
  const {
    table,
    timestampColumn,
    valueColumn,
    schema,
    groupBy,
    aggregation,
    startDate,
    endDate,
    includeMovingAverage,
    movingAverageWindow
  } = args;

  logger.info('analyzeTimeSeries', 'Analyzing time series', { table, groupBy, aggregation });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);
  const sanitizedTimestamp = sanitizeIdentifier(timestampColumn);
  const sanitizedValue = sanitizeIdentifier(valueColumn);

  const dateGroupMap: Record<string, string> = {
    hour: `DATE_TRUNC('hour', ${escapeIdentifier(sanitizedTimestamp)})`,
    day: `DATE_TRUNC('day', ${escapeIdentifier(sanitizedTimestamp)})`,
    week: `DATE_TRUNC('week', ${escapeIdentifier(sanitizedTimestamp)})`,
    month: `DATE_TRUNC('month', ${escapeIdentifier(sanitizedTimestamp)})`
  };

  const aggMap: Record<string, string> = {
    sum: `SUM(${escapeIdentifier(sanitizedValue)})`,
    avg: `AVG(${escapeIdentifier(sanitizedValue)})`,
    count: 'COUNT(*)',
    min: `MIN(${escapeIdentifier(sanitizedValue)})`,
    max: `MAX(${escapeIdentifier(sanitizedValue)})`
  };

  const dateFilter = [];
  const params: any[] = [];

  if (startDate) {
    params.push(startDate);
    dateFilter.push(`${escapeIdentifier(sanitizedTimestamp)} >= $${params.length}`);
  }

  if (endDate) {
    params.push(endDate);
    dateFilter.push(`${escapeIdentifier(sanitizedTimestamp)} <= $${params.length}`);
  }

  const whereClause = dateFilter.length > 0 ? `WHERE ${dateFilter.join(' AND ')}` : '';

  const baseQuery = `
    WITH time_series AS (
      SELECT
        ${dateGroupMap[groupBy]} as period,
        ${aggMap[aggregation]} as value,
        COUNT(*) as count
      FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
      ${whereClause}
      GROUP BY ${dateGroupMap[groupBy]}
      ORDER BY period
    )
    SELECT
      period,
      value,
      count,
      ${includeMovingAverage ? `
        AVG(value) OVER (
          ORDER BY period
          ROWS BETWEEN ${movingAverageWindow - 1} PRECEDING AND CURRENT ROW
        ) as moving_average,
      ` : ''}
      LAG(value) OVER (ORDER BY period) as previous_value,
      CASE
        WHEN LAG(value) OVER (ORDER BY period) IS NOT NULL AND LAG(value) OVER (ORDER BY period) != 0
        THEN ((value - LAG(value) OVER (ORDER BY period)) / LAG(value) OVER (ORDER BY period) * 100)
        ELSE NULL
      END as percent_change
    FROM time_series
    ORDER BY period
  `;

  const statsQuery = `
    SELECT
      ${aggMap[aggregation]} as total,
      AVG(${escapeIdentifier(sanitizedValue)}) as average,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${escapeIdentifier(sanitizedValue)}) as median,
      STDDEV(${escapeIdentifier(sanitizedValue)}) as std_dev,
      MIN(${escapeIdentifier(sanitizedValue)}) as min,
      MAX(${escapeIdentifier(sanitizedValue)}) as max
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    ${whereClause}
  `;

  const [timeSeriesResult, statsResult] = await Promise.all([
    executeQuery(connection, logger, { query: baseQuery, params }),
    executeQuery(connection, logger, { query: statsQuery, params })
  ]);

  const timeSeries = timeSeriesResult.rows.map(row => {
    const ma = includeMovingAverage ? parseFloat(row.moving_average) : undefined;
    const value = parseFloat(row.value);
    let isAnomaly = false;
    let anomalyReason = undefined;

    if (includeMovingAverage && ma && Math.abs(value - ma) > ma * 1.5) {
      isAnomaly = true;
      anomalyReason = `Value is ${(value / ma).toFixed(1)}x the moving average`;
    }

    return {
      period: row.period,
      value: parseFloat(row.value),
      count: parseInt(row.count, 10),
      ...(includeMovingAverage && { movingAverage: ma }),
      percentChange: row.percent_change ? parseFloat(row.percent_change).toFixed(2) : null,
      isAnomaly,
      ...(anomalyReason && { anomalyReason })
    };
  });

  const anomalyCount = timeSeries.filter(t => t.isAnomaly).length;

  const stats = statsResult.rows[0];
  const statistics = {
    total: parseFloat(stats.total || '0'),
    average: parseFloat(stats.average || '0'),
    median: parseFloat(stats.median || '0'),
    stdDev: parseFloat(stats.std_dev || '0'),
    min: parseFloat(stats.min || '0'),
    max: parseFloat(stats.max || '0'),
    anomalyCount
  };

  const recommendations: string[] = [];

  if (anomalyCount > 0) {
    recommendations.push(`⚠ ${anomalyCount} anomalies detected - investigate unusual spikes or drops`);
  } else {
    recommendations.push('✓ No significant anomalies detected');
  }

  const avgChange = timeSeries
    .filter(t => t.percentChange !== null)
    .reduce((sum, t) => sum + parseFloat(t.percentChange || '0'), 0) / timeSeries.length;

  if (avgChange > 5) {
    recommendations.push('✓ Positive growth trend observed');
  } else if (avgChange < -5) {
    recommendations.push('⚠ Declining trend observed');
  } else {
    recommendations.push('✓ Stable trend');
  }

  return {
    table,
    schema,
    period: startDate && endDate ? `${startDate} to ${endDate}` : 'All time',
    groupBy,
    aggregation,
    timeSeries,
    statistics,
    recommendations
  };
}

export async function detectSeasonality(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof DetectSeasonalitySchema>
): Promise<any> {
  const { table, timestampColumn, valueColumn, schema, groupBy, minPeriods } = args;

  logger.info('detectSeasonality', 'Detecting seasonal patterns', { table, groupBy });

  const sanitizedSchema = sanitizeIdentifier(schema);
  const sanitizedTable = sanitizeIdentifier(table);
  const sanitizedTimestamp = sanitizeIdentifier(timestampColumn);
  const sanitizedValue = sanitizeIdentifier(valueColumn);

  const patternMap: Record<string, string> = {
    day_of_week: `TO_CHAR(${escapeIdentifier(sanitizedTimestamp)}, 'Day')`,
    day_of_month: `EXTRACT(DAY FROM ${escapeIdentifier(sanitizedTimestamp)})`,
    month: `TO_CHAR(${escapeIdentifier(sanitizedTimestamp)}, 'Month')`,
    quarter: `EXTRACT(QUARTER FROM ${escapeIdentifier(sanitizedTimestamp)})`
  };

  const query = `
    SELECT
      ${patternMap[groupBy]} as period,
      AVG(${escapeIdentifier(sanitizedValue)}) as avg_value,
      STDDEV(${escapeIdentifier(sanitizedValue)}) as std_dev,
      COUNT(DISTINCT DATE_TRUNC('${groupBy === 'day_of_week' ? 'week' : 'month'}', ${escapeIdentifier(sanitizedTimestamp)})) as period_count,
      MIN(${escapeIdentifier(sanitizedValue)}) as min_value,
      MAX(${escapeIdentifier(sanitizedValue)}) as max_value
    FROM ${escapeIdentifier(sanitizedSchema)}.${escapeIdentifier(sanitizedTable)}
    GROUP BY ${patternMap[groupBy]}
    HAVING COUNT(DISTINCT DATE_TRUNC('${groupBy === 'day_of_week' ? 'week' : 'month'}', ${escapeIdentifier(sanitizedTimestamp)})) >= $1
    ORDER BY
      CASE ${patternMap[groupBy]}
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
        ELSE ${patternMap[groupBy]}::int
      END
  `;

  const result = await executeQuery(connection, logger, {
    query,
    params: [minPeriods]
  });

  const patterns = result.rows.map(row => ({
    period: typeof row.period === 'string' ? row.period.trim() : row.period,
    avgValue: parseFloat(row.avg_value),
    stdDev: parseFloat(row.std_dev || '0'),
    coefficient: parseFloat(row.std_dev || '0') / parseFloat(row.avg_value || '1'),
    minValue: parseFloat(row.min_value),
    maxValue: parseFloat(row.max_value),
    periodsAnalyzed: parseInt(row.period_count, 10)
  }));

  const insights: string[] = [];

  if (patterns.length > 0) {
    insights.push('✓ Seasonal pattern detected');

    const maxPattern = patterns.reduce((max, p) => p.avgValue > max.avgValue ? p : max);
    const minPattern = patterns.reduce((min, p) => p.avgValue < min.avgValue ? p : min);

    insights.push(`${maxPattern.period} has highest average value (${maxPattern.avgValue.toFixed(2)})`);
    insights.push(`${minPattern.period} has lowest average value (${minPattern.avgValue.toFixed(2)})`);

    const avgCoefficient = patterns.reduce((sum, p) => sum + p.coefficient, 0) / patterns.length;
    if (avgCoefficient < 0.2) {
      insights.push('✓ Strong consistent pattern (low variance)');
    } else if (avgCoefficient > 0.5) {
      insights.push('⚠ High variance in pattern - less predictable');
    }
  } else {
    insights.push('⚠ Insufficient data to detect seasonal patterns');
  }

  return {
    table,
    schema,
    pattern: groupBy,
    periodsAnalyzed: patterns.length > 0 ? patterns[0].periodsAnalyzed : 0,
    patterns,
    insights
  };
}

export const temporalTools = {
  findRecent: {
    schema: FindRecentSchema,
    handler: findRecent
  },
  analyzeTimeSeries: {
    schema: AnalyzeTimeSeriesSchema,
    handler: analyzeTimeSeries
  },
  detectSeasonality: {
    schema: DetectSeasonalitySchema,
    handler: detectSeasonality
  }
};
