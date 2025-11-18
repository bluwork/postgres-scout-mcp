import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { executeQuery } from '../utils/database.js';
import { formatQueryResult } from '../utils/result-formatter.js';

const ExecuteQuerySchema = z.object({
  query: z.string(),
  params: z.array(z.any()).optional().default([]),
  timeout: z.number().optional(),
  maxRows: z.number().optional()
});

const ExplainQuerySchema = z.object({
  query: z.string(),
  params: z.array(z.any()).optional().default([]),
  analyze: z.boolean().optional().default(true),
  verbose: z.boolean().optional().default(false),
  buffers: z.boolean().optional().default(true)
});

export async function executeQueryTool(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof ExecuteQuerySchema>
): Promise<any> {
  const { query, params, timeout, maxRows } = args;

  logger.info('executeQuery', 'Executing user query', {
    queryLength: query.length,
    paramCount: params.length
  });

  const startTime = Date.now();

  const result = await executeQuery(connection, logger, {
    query,
    params,
    options: { timeout, maxRows }
  });

  const executionTimeMs = Date.now() - startTime;

  return formatQueryResult(result, executionTimeMs);
}

export async function explainQueryTool(
  connection: DatabaseConnection,
  logger: Logger,
  args: z.infer<typeof ExplainQuerySchema>
): Promise<any> {
  const { query, params, analyze, verbose, buffers } = args;

  logger.info('explainQuery', 'Analyzing query performance', {
    queryLength: query.length,
    analyze,
    verbose,
    buffers
  });

  const explainOptions: string[] = ['FORMAT JSON'];
  if (analyze) explainOptions.push('ANALYZE');
  if (verbose) explainOptions.push('VERBOSE');
  if (buffers) explainOptions.push('BUFFERS');

  const explainQuery = `EXPLAIN (${explainOptions.join(', ')}) ${query}`;

  const startTime = Date.now();
  const result = await executeQuery(connection, logger, {
    query: explainQuery,
    params
  });
  const executionTimeMs = Date.now() - startTime;

  const planData = result.rows[0]?.['QUERY PLAN'] || result.rows[0];

  let plan;
  let planningTime = 0;
  let executionTime = 0;

  if (Array.isArray(planData)) {
    plan = planData[0]?.Plan;
    planningTime = planData[0]?.['Planning Time'] || 0;
    executionTime = planData[0]?.['Execution Time'] || 0;
  } else {
    plan = planData;
  }

  const recommendations = generateRecommendations(plan);

  return {
    query,
    plan,
    executionTimeMs: executionTime || executionTimeMs,
    planningTimeMs: planningTime,
    recommendations
  };
}

function generateRecommendations(plan: any): string[] {
  const recommendations: string[] = [];

  if (!plan) return recommendations;

  const nodeType = plan['Node Type'];
  const relationName = plan['Relation Name'];

  if (nodeType === 'Seq Scan') {
    recommendations.push(
      `⚠ Sequential scan on table "${relationName}" - consider adding an index`
    );
  }

  if (nodeType === 'Index Scan' || nodeType === 'Index Only Scan') {
    recommendations.push(`✓ Using index: ${plan['Index Name']}`);
  }

  if (plan['Actual Rows'] && plan['Plan Rows']) {
    const ratio = plan['Actual Rows'] / plan['Plan Rows'];
    if (ratio > 10 || ratio < 0.1) {
      recommendations.push(
        `⚠ Poor row estimate (planned: ${plan['Plan Rows']}, actual: ${plan['Actual Rows']}) - run ANALYZE`
      );
    }
  }

  if (plan['Buffers']) {
    const shared = plan['Buffers']['Shared'];
    if (shared) {
      const hit = shared['Hit'] || 0;
      const read = shared['Read'] || 0;
      const total = hit + read;
      if (total > 0) {
        const hitRatio = hit / total;
        if (hitRatio < 0.9) {
          recommendations.push(
            `⚠ Low cache hit ratio (${(hitRatio * 100).toFixed(1)}%) - data mostly from disk`
          );
        } else {
          recommendations.push(
            `✓ Excellent cache hit ratio (${(hitRatio * 100).toFixed(1)}%)`
          );
        }
      }
    }
  }

  if (plan.Plans && Array.isArray(plan.Plans)) {
    for (const subPlan of plan.Plans) {
      recommendations.push(...generateRecommendations(subPlan));
    }
  }

  return recommendations;
}

export const queryTools = {
  executeQuery: {
    schema: ExecuteQuerySchema,
    handler: executeQueryTool
  },
  explainQuery: {
    schema: ExplainQuerySchema,
    handler: explainQueryTool
  }
};
