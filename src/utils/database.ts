import { Pool, PoolClient, QueryResult } from 'pg';
import { ServerConfig, DatabaseConnection, QueryParams } from '../types.js';
import { Logger } from './logger.js';
import { sanitizeQuery, parseIntSafe } from './sanitize.js';
import { formatQueryResult, formatError } from './result-formatter.js';

export async function createDatabaseConnection(config: ServerConfig, logger: Logger): Promise<DatabaseConnection> {
  const pool = new Pool({
    connectionString: config.connectionString,
    max: parseIntSafe(process.env.PGMAXPOOLSIZE || '10', 10),
    min: parseIntSafe(process.env.PGMINPOOLSIZE || '2', 2),
    idleTimeoutMillis: parseIntSafe(process.env.PGIDLETIMEOUT || '10000', 10000),
    connectionTimeoutMillis: 5000,
  });

  pool.on('error', (err) => {
    logger.error('database', 'Unexpected database error', { error: err.message });
  });

  try {
    const client = await pool.connect();
    logger.info('database', 'Database connection established');
    client.release();
  } catch (error) {
    logger.error('database', 'Failed to connect to database', { error: formatError(error) });
    throw new Error(`Database connection failed: ${formatError(error)}`);
  }

  return { pool, config };
}

export async function executeQuery(
  connection: DatabaseConnection,
  logger: Logger,
  params: QueryParams
): Promise<QueryResult<any>> {
  const { query, params: queryParams = [], options = {} } = params;
  const { config, pool } = connection;

  sanitizeQuery(query, config.mode);

  const timeout = options.timeout || config.queryTimeout;
  const maxRows = options.maxRows || config.maxResultRows;

  logger.debug('query', 'Executing query', {
    query: query.substring(0, 200),
    params: queryParams,
    timeout,
    maxRows
  });

  let client: PoolClient | null = null;
  const startTime = Date.now();

  try {
    client = await pool.connect();

    if (!Number.isFinite(timeout) || timeout < 0) {
      throw new Error('Invalid timeout value');
    }
    await client.query(`SET statement_timeout = ${Math.floor(timeout)}`);

    const result = await client.query({
      text: query,
      values: queryParams
    });

    const executionTime = Date.now() - startTime;

    if (result.rowCount && result.rowCount > maxRows) {
      logger.warn('query', 'Result set exceeds max rows', {
        rowCount: result.rowCount,
        maxRows
      });
      result.rows = result.rows.slice(0, maxRows);
      result.rowCount = maxRows;
    }

    logger.info('query', 'Query executed successfully', {
      rowCount: result.rowCount,
      executionTimeMs: executionTime
    });

    return result;
  } catch (error) {
    const executionTime = Date.now() - startTime;
    logger.error('query', 'Query execution failed', {
      error: formatError(error),
      executionTimeMs: executionTime,
      query: query.substring(0, 200)
    });
    throw error;
  } finally {
    if (client) {
      client.release();
    }
  }
}

export async function closeDatabaseConnection(connection: DatabaseConnection, logger: Logger): Promise<void> {
  try {
    await connection.pool.end();
    logger.info('database', 'Database connection closed');
  } catch (error) {
    logger.error('database', 'Error closing database connection', { error: formatError(error) });
    throw error;
  }
}

export async function testConnection(pool: Pool): Promise<boolean> {
  try {
    const client = await pool.connect();
    await client.query('SELECT 1');
    client.release();
    return true;
  } catch (error) {
    return false;
  }
}
