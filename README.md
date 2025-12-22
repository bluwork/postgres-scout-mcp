# Postgres Scout MCP

Model Context Protocol server for safe PostgreSQL database interaction. Enables AI assistants to explore, analyze, and maintain PostgreSQL databases with built-in safety features.

## Features

- **Safety First**: Read-only mode by default, explicit opt-in for write operations
- **SQL Injection Prevention**: All queries use parameterization
- **Rate Limiting**: Prevent accidental DoS attacks
- **Comprehensive Logging**: Audit trail of all operations
- **Query Timeouts**: Configurable timeout protection
- **Connection Pooling**: Efficient database resource management

## Installation

```bash
pnpm install
pnpm build
```

## Quick Start

### Read-Only Mode (Default)

Safe for production database exploration:

```bash
node dist/index.js postgresql://localhost:5432/mydb
```

### Read-Write Mode

Requires explicit flag:

```bash
node dist/index.js --read-write postgresql://localhost:5432/mydb
```

## Configuration

### Environment Variables

```bash
# Database Connection
DATABASE_URL=postgresql://user:password@localhost:5432/dbname

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

### Claude Desktop Configuration

Add to your Claude Desktop config file:

```json
{
  "mcpServers": {
    "postgres-scout-readonly": {
      "command": "node",
      "args": [
        "/absolute/path/to/postgres-scout-mcp/dist/index.js",
        "postgresql://localhost:5432/production"
      ],
      "type": "stdio"
    },
    "postgres-scout-dev": {
      "command": "node",
      "args": [
        "/absolute/path/to/postgres-scout-mcp/dist/index.js",
        "--read-write",
        "postgresql://localhost:5432/development"
      ],
      "type": "stdio"
    }
  }
}
```

## Available Tools

### Database Operations

#### `listDatabases`
List all databases the user has access to.

```json
{}
```

#### `getDatabaseStats`
Get comprehensive database statistics.

```json
{
  "database": "production"
}
```

Note: the `database` parameter must match the current connection; reconnect to target a different database.

### Schema Operations

#### `listSchemas`
List all schemas in the database.

```json
{}
```

#### `listTables`
List tables with detailed information.

```json
{
  "schema": "public",
  "includeSystemTables": false
}
```

Notes:
- `rowEstimate` is based on PostgreSQL statistics; when `needsAnalyze` is `true`, run `ANALYZE` for a reliable estimate.

#### `describeTable`
Get comprehensive table information including columns, constraints, and indexes.

```json
{
  "table": "users",
  "schema": "public"
}
```

### Query Operations

#### `executeQuery`
Execute SELECT queries with safety checks.

```json
{
  "query": "SELECT id, email FROM users WHERE status = $1 LIMIT 10",
  "params": ["active"],
  "timeout": 5000,
  "maxRows": 1000
}
```

#### `explainQuery`
Analyze query performance with EXPLAIN ANALYZE. In read-only mode, `analyze` is forced to `false` to avoid executing statements.

```json
{
  "query": "SELECT * FROM users WHERE email = $1",
  "params": ["user@example.com"],
  "analyze": true,
  "verbose": true,
  "buffers": true
}
```

### Maintenance & Diagnostics

#### `getHealthScore`
Calculate overall database health score.

```json
{
  "database": "production"
}
```

Note: the `database` parameter must match the current connection; reconnect to target a different database.

#### `getSlowQueries`
Analyze slow queries using `pg_stat_statements` (extension required).

```json
{
  "minDurationMs": 100,
  "limit": 10,
  "orderBy": "total_time"
}
```

## Enum Parameter Reference

- `exportTable.format`: `csv`, `json`, `jsonl`, `sql`
- `getSlowQueries.orderBy`: `total_time`, `mean_time`, `calls`
- `getLiveMetrics.metrics`: `queries`, `connections`, `locks`, `transactions`, `cache`
- `getHottestTables.orderBy`: `seq_scan`, `idx_scan`, `writes`, `size`
- `detectAnomalies.type`: `query_performance`, `data_volume`, `connections`, `errors`, `all`
- `detectAnomalies.sensitivityLevel`: `low`, `medium`, `high`
- `analyzeTimeSeries.groupBy`: `hour`, `day`, `week`, `month`
- `analyzeTimeSeries.aggregation`: `sum`, `avg`, `count`, `min`, `max`
- `detectSeasonality.groupBy`: `day_of_week`, `day_of_month`, `month`, `quarter`

## Usage Examples

### Exploring Database Schema

```
User: Show me all tables in the public schema

AI: [Uses listTables tool]

AI: Found 45 tables in the public schema:
- users (150,000 rows, 25 MB)
- orders (2M rows, 450 MB)
- products (5,000 rows, 2 MB)
...
```

### Analyzing Query Performance

```
User: Why is this query slow: SELECT * FROM orders WHERE customer_id = 123

AI: [Uses explainQuery tool]

AI: The query is doing a sequential scan on 2M rows.
Recommendation: Create an index:
CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders(customer_id);

This should reduce query time from ~2500ms to ~5ms.
```

### Database Health Check

```
User: How healthy is my database?

AI: [Uses getDatabaseStats tool]

AI: Database Statistics:
- Size: 1.2 GB
- Tables: 45
- Cache Hit Ratio: 98% (Excellent)
- Active Connections: 12/100
- Transaction Rate: 150/sec
```

## Security

### Read-Only vs Read-Write Mode

**Read-Only Mode** (default):
- SELECT queries
- EXPLAIN ANALYZE
- Schema inspection
- Performance monitoring

**Read-Write Mode** (requires --read-write flag):
- All read-only operations
- INSERT, UPDATE, DELETE
- CREATE, ALTER, DROP
- VACUUM, ANALYZE

### SQL Injection Prevention

Multiple layers of protection:

1. **Parameterized Queries**: All user data passed as parameters
2. **Input Validation**: WHERE clauses, conditions, and ORDER BY validated for dangerous patterns
3. **Identifier Sanitization**: Table/column names validated against whitelist pattern
4. **Pattern Detection**: Blocks SQL comments, UNION SELECT, and other injection patterns

```typescript
// Safe - parameterized
executeQuery({
  query: "SELECT * FROM users WHERE email = $1",
  params: ["user@example.com"]
})

// Safe - validated WHERE clause
previewUpdate({
  table: "users",
  where: "status = 'active' AND created_at > '2024-01-01'"
})

// SafeUpdate: raw SET strings are opt-in
safeUpdate({
  table: "users",
  set: "status = 'inactive'",
  where: "last_login < NOW() - INTERVAL '1 year'",
  allowRawSet: true
})

// Blocked - dangerous patterns
previewUpdate({
  table: "users",
  where: "1=1; DROP TABLE users --"  // Error: dangerous pattern detected
})
```

### Rate Limiting

Prevents accidental DoS:
- Default: 100 requests per minute
- Configurable via environment variables
- Can be disabled for trusted environments

### Query Timeouts

All queries have configurable timeouts:
- Default: 30 seconds
- Prevents long-running queries
- Protects database resources

## Development

### Build

```bash
pnpm build
```

### Watch Mode

```bash
pnpm watch
```

### Project Structure

```
postgres-scout-mcp/
├── src/
│   ├── index.ts              # Entry point
│   ├── types.ts              # TypeScript types
│   ├── server/
│   │   └── setup.ts          # MCP server configuration
│   ├── tools/
│   │   ├── index.ts          # Tool registration
│   │   ├── database.ts       # Database operations
│   │   ├── schema.ts         # Schema inspection
│   │   └── query.ts          # Query execution
│   ├── utils/
│   │   ├── logger.ts         # Logging
│   │   ├── sanitize.ts       # SQL injection prevention
│   │   ├── query-builder.ts  # Query construction
│   │   ├── rate-limiter.ts   # Rate limiting
│   │   ├── database.ts       # Connection management
│   │   └── result-formatter.ts
│   └── config/
│       └── environment.ts    # Configuration
├── dist/                     # Compiled output
├── logs/                     # Log files
└── bin/
    └── cli.js               # CLI wrapper
```

## Troubleshooting

### Connection Issues

```
Error: Database connection failed
```

**Solutions:**
- Verify connection string format: `postgresql://user:password@host:port/database`
- Check database server is running
- Verify network connectivity
- Check firewall rules
- Verify credentials

### Permission Errors

```
Error: permission denied for table users
```

**Solutions:**
- Verify database user has necessary permissions
- In read-only mode, SELECT permission is required
- In read-write mode, additional permissions needed
- Contact database administrator

### Rate Limit Exceeded

```
Error: Rate limit exceeded. Try again in 30 seconds.
```

**Solutions:**
- Wait for the rate limit window to expire
- Increase `RATE_LIMIT_MAX_REQUESTS` if needed
- Disable rate limiting for trusted environments: `ENABLE_RATE_LIMIT=false`

## Logging

All operations are logged to:
- `logs/tool-usage.log` - All tool executions
- `logs/error.log` - Errors only
- Console (stderr) - Real-time output

Log format:
```
2025-01-17T10:30:00Z [INFO] Tool: executeQuery, Message: Query executed successfully, Data: {"rowCount": 10, "executionTimeMs": 12}
```

## Implemented Features

### Core Features ✅
- Database operations (list databases, stats, health scoring)
- Schema inspection (tables, columns, constraints, indexes)
- Query execution with safety checks
- Query performance analysis (EXPLAIN ANALYZE)

### Data Quality Tools ✅
- Find duplicates
- Find missing values (NULL analysis)
- Find orphaned records
- Check constraint violations
- Analyze type consistency

### Export Tools ✅
- Export to CSV, JSON, JSONL, SQL
- Generate INSERT statements with batching

### Temporal Tools ✅
- Find recent records
- Time series analysis with anomaly detection
- Seasonality detection

### Monitoring Tools ✅
- Current activity monitoring
- Lock analysis
- Index usage analysis

### Mutation Tools ✅ (read-write mode)
- Preview UPDATE/DELETE operations
- Safe UPDATE with row limits
- Safe DELETE with row limits

## Roadmap

### Future Enhancements
- AI-powered index recommendations
- Partitioning suggestions
- Bloat analysis and VACUUM recommendations
- Query optimization suggestions

## License

ISC

## Contributing

Contributions welcome! Areas of focus:
- Additional tools and features
- Performance optimizations
- Better error messages
- Documentation improvements
- Test coverage

## Support

- Issues: [GitHub Issues](https://github.com/bluwork/postgres-scout-mcp/issues)
- Repository: [GitHub](https://github.com/bluwork/postgres-scout-mcp)
