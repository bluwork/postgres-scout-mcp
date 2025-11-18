#!/usr/bin/env node

import { parseCommandLineArgs, createServerConfig, validateConfig } from './config/environment.js';
import { createLogger, Logger } from './utils/logger.js';
import { createDatabaseConnection, closeDatabaseConnection } from './utils/database.js';
import { createMCPServer, startServer } from './server/setup.js';
import { DatabaseConnection } from './types.js';

async function main() {
  let logger: Logger | undefined;
  let connection: DatabaseConnection | undefined;

  try {
    const args = parseCommandLineArgs(process.argv.slice(2));
    const config = createServerConfig(args);
    validateConfig(config);

    logger = createLogger(config.logDir, config.logLevel);

    logger.info('main', 'Starting Postgres Scout MCP', {
      mode: config.mode,
      logLevel: config.logLevel
    });

    connection = await createDatabaseConnection(config, logger);

    logger.info('main', 'Database connection established', {
      database: connection.pool.options.database
    });

    const server = createMCPServer(connection, logger, config);

    await startServer(server, logger);

    const shutdown = async () => {
      if (logger) {
        logger.info('main', 'Shutting down...');
      }
      if (connection && logger) {
        await closeDatabaseConnection(connection, logger);
      }
      process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    if (logger) {
      logger.error('main', 'Fatal error', { error: errorMessage });
    } else {
      console.error('Fatal error:', errorMessage);
    }

    if (connection) {
      try {
        await closeDatabaseConnection(connection, logger!);
      } catch (closeError) {
        console.error('Error closing connection:', closeError);
      }
    }

    process.exit(1);
  }
}

main().catch((error) => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
