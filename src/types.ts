import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';

export type DatabaseMode = 'read-only' | 'read-write';

export interface ServerConfig {
  mode: DatabaseMode;
  connectionString: string;
  queryTimeout: number;
  maxResultRows: number;
  enableRateLimit: boolean;
  rateLimitMaxRequests: number;
  rateLimitWindowMs: number;
  logDir: string;
  logLevel: 'debug' | 'info' | 'warn' | 'error';
}

export interface DatabaseConnection {
  pool: Pool;
  config: ServerConfig;
}

export interface QueryOptions {
  timeout?: number;
  maxRows?: number;
}

export interface QueryParams {
  query: string;
  params?: any[];
  options?: QueryOptions;
}

export interface TableInfo {
  name: string;
  schema: string;
  type: string;
  rowEstimate: number;
  needsAnalyze?: boolean;
  sizeBytes: number;
  indexSize: number;
  totalSize: number;
  lastVacuum: string | null;
  lastAnalyze: string | null;
  isPartitioned: boolean;
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  default: string | null;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
  hasIndex?: boolean;
  uniqueConstraint?: boolean;
  references?: {
    table: string;
    column: string;
    onDelete: string;
    onUpdate: string;
  };
}

export interface ConstraintInfo {
  name: string;
  type: 'PRIMARY KEY' | 'FOREIGN KEY' | 'UNIQUE' | 'CHECK';
  columns: string[];
  definition?: string;
  references?: {
    table: string;
    columns: string[];
  };
}

export interface IndexInfo {
  name: string;
  columns: string[];
  type: string;
  unique: boolean;
  primary: boolean;
  size: string;
}

export interface DatabaseStats {
  database: string;
  size: string;
  tables: number;
  indexes: number;
  sequences: number;
  views: number;
  functions: number;
  activeConnections: number;
  maxConnections: number;
  cacheHitRatio: number;
  transactionRate: number;
  tupleStats: {
    returned: number;
    fetched: number;
    inserted: number;
    updated: number;
    deleted: number;
  };
}

export interface ExplainPlan {
  nodeType: string;
  relationName?: string;
  alias?: string;
  indexName?: string;
  planRows: number;
  planWidth: number;
  actualRows?: number;
  actualLoops?: number;
  startupCost: number;
  totalCost: number;
  actualTotalTime?: number;
  indexCond?: string;
  rowsRemoved?: number;
  buffersShared?: {
    hit: number;
    read: number;
  };
}

export interface LogEntry {
  timestamp: Date;
  level: string;
  tool: string;
  message: string;
  data?: any;
}
