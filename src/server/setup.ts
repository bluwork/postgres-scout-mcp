import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool
} from '@modelcontextprotocol/sdk/types.js';
import { DatabaseConnection, ServerConfig } from '../types.js';
import { Logger } from '../utils/logger.js';
import { RateLimiter } from '../utils/rate-limiter.js';
import { tools, executeTool } from '../tools/index.js';

export function createMCPServer(
  connection: DatabaseConnection,
  logger: Logger,
  config: ServerConfig
): Server {
  const server = new Server(
    {
      name: 'postgres-scout-mcp',
      version: '0.1.0',
    },
    {
      capabilities: {
        tools: {},
      },
    }
  );

  const rateLimiter = new RateLimiter(
    config.rateLimitMaxRequests,
    config.rateLimitWindowMs,
    config.enableRateLimit
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => {
    logger.debug('mcp', 'Listing available tools');

    const toolList: Tool[] = Object.entries(tools).map(([name, tool]) => ({
      name,
      description: getToolDescription(name, config.mode),
      inputSchema: zodToJsonSchema(tool.schema)
    }));

    return { tools: toolList };
  });

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    logger.info('mcp', `Tool called: ${name}`, { args });

    try {
      rateLimiter.checkLimit();

      const result = await executeTool(name, connection, logger, args || {});

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('mcp', `Error executing tool ${name}`, { error: errorMessage });

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: errorMessage,
              tool: name
            }, null, 2)
          }
        ],
        isError: true
      };
    }
  });

  return server;
}

export async function startServer(server: Server, logger: Logger): Promise<void> {
  const transport = new StdioServerTransport();

  logger.info('server', 'Starting MCP server');

  await server.connect(transport);

  logger.info('server', 'MCP server running on stdio');
}

function getToolDescription(name: string, mode: string): string {
  const descriptions: Record<string, string> = {
    // Database operations
    listDatabases: 'List all databases the user has access to',
    getDatabaseStats: 'Get comprehensive database statistics including size, cache hit ratio, and connection info',

    // Schema operations
    listSchemas: 'List all schemas in the current database',
    listTables: 'List all tables in a schema with size and statistics',
    describeTable: 'Get comprehensive table information including columns, constraints, and indexes',

    // Query operations
    executeQuery: `Execute SELECT queries${mode === 'read-write' ? ' or write operations' : ' (read-only)'}`,
    explainQuery: `Analyze query performance using EXPLAIN${mode === 'read-only' ? ' (ANALYZE disabled)' : ' ANALYZE'}`,

    // Data quality tools
    findDuplicates: 'Find duplicate rows based on column combinations',
    findMissingValues: 'Find NULL values or missing data in columns',
    findOrphans: 'Find orphaned records with invalid foreign key references',
    checkConstraintViolations: 'Check for rows that would violate a constraint before adding it',
    analyzeTypeConsistency: 'Analyze if text columns contain consistent data types',

    // Temporal tools
    findRecent: 'Find rows within a time window',
    analyzeTimeSeries: 'Advanced time-series analysis with window functions and anomaly detection',
    detectSeasonality: 'Detect seasonal patterns in time-series data',

    // Monitoring tools
    getCurrentActivity: 'Get current active queries and connections',
    analyzeLocks: 'Analyze current locks and blocking queries',
    getIndexUsage: 'Analyze index usage and identify unused indexes',

    // Relationship tools
    exploreRelationships: 'Follow foreign key relationships to explore related records',
    analyzeForeignKeys: 'Analyze foreign key health and performance',

    // Export tools
    exportTable: 'Export table data to various formats (CSV, JSON, SQL)',
    generateInsertStatements: 'Generate INSERT statements for data migration',

    // Maintenance & health tools
    analyzeTableBloat: 'Detect table and index bloat for VACUUM planning',
    suggestVacuum: 'Analyze and recommend VACUUM operations based on dead tuples and bloat',
    getHealthScore: 'Calculate overall database health score with component breakdown',
    getSlowQueries: 'Analyze slow queries from pg_stat_statements extension',

    // Optimization tools
    suggestIndexes: 'Analyze query patterns and table scans to recommend missing indexes',
    suggestPartitioning: 'Analyze large tables and recommend partitioning strategies',
    detectAnomalies: 'Detect anomalies in query performance, connections, and data patterns',
    optimizeQuery: 'Analyze a specific query and provide optimization recommendations',

    // Mutation tools (safe write operations)
    previewUpdate: 'Preview which rows would be affected by an UPDATE without modifying data',
    previewDelete: 'Preview which rows would be deleted without actually deleting them',
    safeUpdate: 'Execute UPDATE with safety guards: dry-run mode, maxRows limit, empty WHERE protection. Raw SET strings require allowRawSet=true',
    safeDelete: 'Execute DELETE with safety guards: dry-run mode, maxRows limit, empty WHERE protection',

    // Live monitoring tools
    getLiveMetrics: 'Collect real-time database metrics over a time period with configurable intervals',
    getHottestTables: 'Identify tables with highest activity during a sample period',
    getTableMetrics: 'Get comprehensive metrics for a specific table including I/O, scans, and maintenance stats'
  };

  return descriptions[name] || `Execute ${name} operation`;
}

function zodToJsonSchema(schema: any): any {
  const shape = schema._def?.shape?.() || {};
  const properties: Record<string, any> = {};
  const required: string[] = [];

  for (const [key, value] of Object.entries(shape)) {
    const field = value as any;
    properties[key] = convertZodType(field);

    if (!field._def?.defaultValue) {
      required.push(key);
    }
  }

  return {
    type: 'object',
    properties,
    required: required.length > 0 ? required : undefined
  };
}

function convertZodType(zodType: any): any {
  const typeName = zodType._def?.typeName;

  switch (typeName) {
    case 'ZodString':
      return { type: 'string' };
    case 'ZodNumber':
      return { type: 'number' };
    case 'ZodBoolean':
      return { type: 'boolean' };
    case 'ZodArray':
      return {
        type: 'array',
        items: convertZodType(zodType._def?.type)
      };
    case 'ZodOptional':
      return convertZodType(zodType._def?.innerType);
    case 'ZodDefault':
      const inner = convertZodType(zodType._def?.innerType);
      inner.default = zodType._def?.defaultValue;
      return inner;
    default:
      return { type: 'string' };
  }
}
