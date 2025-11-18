import { ServerConfig, DatabaseMode } from '../types.js';
import { parseIntSafe } from '../utils/sanitize.js';

export interface ParsedArgs {
  mode: DatabaseMode;
  connectionString: string;
}

export function parseCommandLineArgs(args: string[]): ParsedArgs {
  let mode: DatabaseMode = 'read-only';
  let connectionString = '';

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    if (arg === '--read-write' || arg === '-w') {
      mode = 'read-write';
    } else if (arg === '--read-only' || arg === '-r') {
      mode = 'read-only';
    } else if (!arg.startsWith('-')) {
      connectionString = arg;
    }
  }

  if (!connectionString) {
    connectionString = process.env.DATABASE_URL || '';
  }

  if (!connectionString) {
    throw new Error('Database connection string required. Provide as argument or set DATABASE_URL environment variable.');
  }

  return { mode, connectionString };
}

export function createServerConfig(args: ParsedArgs): ServerConfig {
  return {
    mode: args.mode,
    connectionString: args.connectionString,
    queryTimeout: parseIntSafe(process.env.QUERY_TIMEOUT || '30000', 30000),
    maxResultRows: parseIntSafe(process.env.MAX_RESULT_ROWS || '10000', 10000),
    enableRateLimit: process.env.ENABLE_RATE_LIMIT === 'true',
    rateLimitMaxRequests: parseIntSafe(process.env.RATE_LIMIT_MAX_REQUESTS || '100', 100),
    rateLimitWindowMs: parseIntSafe(process.env.RATE_LIMIT_WINDOW_MS || '60000', 60000),
    logDir: process.env.LOG_DIR || './logs',
    logLevel: (process.env.LOG_LEVEL as 'debug' | 'info' | 'warn' | 'error') || 'info'
  };
}

export function validateConfig(config: ServerConfig): void {
  if (!config.connectionString) {
    throw new Error('Connection string is required');
  }

  if (config.queryTimeout < 1000 || config.queryTimeout > 600000) {
    throw new Error('Query timeout must be between 1000ms and 600000ms');
  }

  if (config.maxResultRows < 1 || config.maxResultRows > 100000) {
    throw new Error('Max result rows must be between 1 and 100000');
  }

  if (!['read-only', 'read-write'].includes(config.mode)) {
    throw new Error('Mode must be either "read-only" or "read-write"');
  }
}
