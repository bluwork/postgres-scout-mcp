# Postgres Scout MCP

Scout your PostgreSQL databases with AI - A production-ready Model Context Protocol server with built-in safety features, monitoring, and data quality tools.

[![npm](https://img.shields.io/npm/v/postgres-scout-mcp)](https://www.npmjs.com/package/postgres-scout-mcp) [![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## Setup

### Claude Desktop / Claude Code

Add to your MCP config (`~/.config/claude-desktop/config.json` or `~/.claude.json`):

```json
{
  "mcpServers": {
    "postgres-scout": {
      "command": "npx",
      "args": [
        "-y",
        "postgres-scout-mcp",
        "postgresql://localhost:5432/mydb"
      ],
      "type": "stdio"
    }
  }
}
```

### Recommended: Separate Read-Only and Read-Write Instances

The server runs in **read-only mode by default** for safety. For write operations, use a separate instance:

```json
{
  "mcpServers": {
    "postgres-scout-readonly": {
      "command": "npx",
      "args": ["-y", "postgres-scout-mcp", "--read-only", "postgresql://localhost:5432/production"],
      "type": "stdio"
    },
    "postgres-scout-readwrite": {
      "command": "npx",
      "args": ["-y", "postgres-scout-mcp", "--read-write", "postgresql://localhost:5432/development"],
      "type": "stdio"
    }
  }
}
```

This gives you:
- **postgres-scout-readonly**: Safe exploration without risk of data modification
- **postgres-scout-readwrite**: Write operations available when explicitly needed
- Clear separation of capabilities
- Option to point read-write to a development database for extra safety

### Global Install

```bash
npm install -g postgres-scout-mcp
postgres-scout-mcp postgresql://localhost:5432/mydb
```

### Standalone Usage

```bash
# Default read-only mode (safest)
postgres-scout-mcp

# Explicitly enable read-write mode (use with caution)
postgres-scout-mcp --read-write

# With custom URI in read-only mode
postgres-scout-mcp postgresql://localhost:5432/mydb

# Read-write mode with custom connection
postgres-scout-mcp --read-write postgresql://localhost:5432/mydb
```

### Command Line Options

```
--read-only          Run server in read-only mode (default)
--read-write         Run server in read-write mode (enables all write operations)
--mode <mode>        Set mode: 'read-only' or 'read-write'
```

### Environment Variables

```bash
# Security
QUERY_TIMEOUT=30000         # milliseconds (default: 30s)
MAX_RESULT_ROWS=10000       # prevent memory exhaustion
ENABLE_RATE_LIMIT=true
RATE_LIMIT_MAX_REQUESTS=100
RATE_LIMIT_WINDOW_MS=60000  # 1 minute

# Logging
LOG_DIR=./logs
LOG_LEVEL=info              # debug, info, warn, error

# Connection Pool
PGMAXPOOLSIZE=10
PGMINPOOLSIZE=2
PGIDLETIMEOUT=10000
```

## Security

- **Read-only by default** — write operations must be explicitly enabled
- All queries use parameterized values
- SQL injection prevention with input validation and pattern detection
- Identifier sanitization for table/column names
- Rate limiting on all operations
- Query timeouts to prevent long-running queries
- Response size limits to prevent memory exhaustion

## Available Tools

### Read Operations (both modes)

- **Database**: `listDatabases`, `getDatabaseStats`
- **Schema**: `listSchemas`, `listTables`, `describeTable`
- **Query**: `executeQuery`, `explainQuery`

### Data Quality

- `findDuplicates` — find duplicate rows based on column combinations
- `findMissingValues` — NULL analysis across columns
- `findOrphans` — find orphaned records via foreign key references
- `checkConstraintViolations` — detect constraint issues
- `analyzeTypeConsistency` — find type inconsistencies across rows

### Export

- `exportTable` — export to CSV, JSON, JSONL, or SQL
- `generateInsertStatements` — generate INSERT statements with batching

### Relationships

- `exploreRelationships` — follow multi-hop relationships and discover dependencies
- `analyzeForeignKeys` — analyze foreign key structure

### Temporal Queries

- `findRecent` — find rows within a time window
- `analyzeTimeSeries` — time series analysis with anomaly detection
- `detectSeasonality` — detect seasonal patterns

### Monitoring

- `getCurrentActivity` — active queries and connections
- `analyzeLocks` — lock analysis
- `getIndexUsage` — index usage statistics

### Live Monitoring

- `getLiveMetrics` — real-time performance metrics
- `getHottestTables` — identify most active tables
- `getTableMetrics` — detailed metrics per table

### Maintenance & Optimization

- `getHealthScore` — overall database health score
- `getSlowQueries` — slow query analysis (requires `pg_stat_statements`)
- `analyzeTableBloat` — table bloat analysis
- `suggestVacuum` — VACUUM recommendations
- `suggestIndexes` — index recommendations
- `suggestPartitioning` — partitioning suggestions
- `detectAnomalies` — anomaly detection
- `optimizeQuery` — query optimization suggestions

### Write Operations (read-write mode only)

- `previewUpdate`, `previewDelete` — preview changes before applying
- `safeUpdate` — UPDATE with row limits and preview
- `safeDelete` — DELETE with row limits and preview
- `safeInsert` — INSERT with validation

## Logging

File logging is **disabled by default**. Enable it with the `ENABLE_LOGGING=true` environment variable:

```json
{
  "mcpServers": {
    "postgres-scout": {
      "command": "npx",
      "args": ["-y", "postgres-scout-mcp", "postgresql://localhost:5432/mydb"],
      "env": { "ENABLE_LOGGING": "true", "LOG_DIR": "./logs" },
      "type": "stdio"
    }
  }
}
```

When enabled, two log files are created in `LOG_DIR` (defaults to `./logs`):

- **tool-usage.log**: Every tool call with timestamp, tool name, and arguments
- **error.log**: Errors with stack traces and the arguments that caused them

## Examples

### Basic Operations
```
executeQuery({ query: "SELECT id, email FROM users WHERE status = $1 LIMIT 10", params: ["active"] })
explainQuery({ query: "SELECT * FROM orders WHERE customer_id = $1", params: [123], analyze: true })
listTables({ schema: "public" })
```

### Data Quality
```
findDuplicates({ table: "users", columns: ["email"] })
findOrphans({ table: "orders", column: "customer_id", referencedTable: "customers", referencedColumn: "id" })
findMissingValues({ table: "users", columns: ["email", "phone"] })
```

### Monitoring
```
getLiveMetrics({ metrics: ["queries", "connections", "cache"], duration: 30000, interval: 1000 })
getHottestTables({ limit: 5, orderBy: "seq_scan" })
getSlowQueries({ minDurationMs: 100, limit: 10 })
```

## Development

```bash
git clone https://github.com/bluwork/postgres-scout-mcp.git
cd postgres-scout-mcp
pnpm install
pnpm build
pnpm test
```

## License

Apache-2.0
