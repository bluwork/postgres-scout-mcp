import { z } from 'zod';
import { DatabaseConnection } from '../types.js';
import { Logger } from '../utils/logger.js';
import { databaseTools } from './database.js';
import { schemaTools } from './schema.js';
import { queryTools } from './query.js';
import { dataQualityTools } from './data-quality.js';
import { temporalTools } from './temporal.js';
import { monitoringTools } from './monitoring.js';
import { relationshipTools } from './relationships.js';
import { exportTools } from './export.js';
import { maintenanceTools } from './maintenance.js';
import { optimizationTools } from './optimization.js';
import { mutationTools } from './mutations.js';
import { liveMonitoringTools } from './live-monitoring.js';

export interface ToolDefinition {
  schema: z.ZodType<any>;
  handler: (connection: DatabaseConnection, logger: Logger, args: any) => Promise<any>;
}

export const tools: Record<string, ToolDefinition> = {
  ...databaseTools,
  ...schemaTools,
  ...queryTools,
  ...dataQualityTools,
  ...temporalTools,
  ...monitoringTools,
  ...relationshipTools,
  ...exportTools,
  ...maintenanceTools,
  ...optimizationTools,
  ...mutationTools,
  ...liveMonitoringTools
};

export function getToolNames(): string[] {
  return Object.keys(tools);
}

export function getTool(name: string): ToolDefinition | undefined {
  return tools[name];
}

export async function executeTool(
  name: string,
  connection: DatabaseConnection,
  logger: Logger,
  args: any
): Promise<any> {
  const tool = getTool(name);

  if (!tool) {
    throw new Error(`Tool "${name}" not found`);
  }

  const validatedArgs = tool.schema.parse(args);

  logger.info('tool', `Executing tool: ${name}`, { args: validatedArgs });

  try {
    const result = await tool.handler(connection, logger, validatedArgs);
    logger.info('tool', `Tool ${name} completed successfully`);
    return result;
  } catch (error) {
    logger.error('tool', `Tool ${name} failed`, {
      error: error instanceof Error ? error.message : String(error)
    });
    throw error;
  }
}
