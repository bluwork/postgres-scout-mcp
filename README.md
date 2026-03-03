# Postgres Scout MCP

Scout your PostgreSQL databases with AI - A production-ready Model Context Protocol server with built-in safety features, monitoring, and data quality tools.

[![npm](https://img.shields.io/npm/v/postgres-scout-mcp)](https://www.npmjs.com/package/postgres-scout-mcp) [![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## What You Get

You ask:

> *"How healthy is my production database? Any urgent issues?"*

Postgres Scout returns:

---

### Overall Health Score: 78/100

**Component Breakdown**
| Component | Score | Status |
|-----------|-------|--------|
| Cache Performance | 94/100 | Healthy |
| Index Efficiency | 82/100 | Good |
| Table Bloat | 61/100 | Needs Attention |
| Connection Usage | 75/100 | Fair |

**Issues Found**
- **HIGH** — Table `orders` has 34% bloat (2.1 GB wasted). VACUUM FULL recommended.
- **MEDIUM** — 3 unused indexes on `sessions` consuming 890 MB.
- **LOW** — Cache hit ratio for `analytics_events` is 71% (target: >90%).

**Recommendations**
- Run `VACUUM FULL orders` during maintenance window
- Drop unused indexes: `idx_sessions_legacy`, `idx_sessions_old_token`, `idx_sessions_temp`
- Consider adding `analytics_events` to shared_buffers or partitioning by date

---

That's `getHealthScore` — one of 38 tools covering exploration, diagnostics, optimization, monitoring, data quality, and safe writes.

## Quick Start

### Claude Code

```bash
claude mcp add postgres-scout -- npx -y postgres-scout-mcp postgresql://localhost:5432/mydb
```

Then ask: *"Show me the largest tables and whether they have any bloat issues."*

<details>
<summary>Claude Desktop</summary>

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "postgres-scout": {
      "command": "npx",
      "args": ["-y", "postgres-scout-mcp", "postgresql://localhost:5432/mydb"],
      "type": "stdio"
    }
  }
}
```

</details>

<details>
<summary>Cursor / VS Code</summary>

Add to your MCP settings:

```json
{
  "postgres-scout": {
    "command": "npx",
    "args": ["-y", "postgres-scout-mcp", "postgresql://localhost:5432/mydb"]
  }
}
```

</details>

<details>
<summary>Read-Only vs Read-Write</summary>

The server runs in **read-only mode by default**. For write operations, run a separate instance:

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

- **postgres-scout-readonly**: Safe exploration, no risk of data modification
- **postgres-scout-readwrite**: Write operations when explicitly needed

</details>

## Tools

### Explore — understand your database

- `listDatabases` — databases the user has access to
- `getDatabaseStats` — size, cache hit ratio, connection info
- `listSchemas` — all schemas in the current database
- `listTables` — tables with size and row statistics
- `describeTable` — columns, constraints, indexes, and more

### Query — run and analyze

- `executeQuery` — run SELECT queries (or writes in read-write mode)
- `explainQuery` — EXPLAIN plans for performance analysis
- `optimizeQuery` — optimization recommendations for a specific query

### Diagnose — find problems before they find you

- `getHealthScore` — overall health score with component breakdown
- `detectAnomalies` — anomalies in performance, connections, and data
- `analyzeTableBloat` — bloat analysis for VACUUM planning
- `getSlowQueries` — slow query analysis (requires pg_stat_statements)
- `suggestVacuum` — VACUUM recommendations based on dead tuples and bloat

### Optimize — make it faster

- `suggestIndexes` — missing index recommendations from query patterns
- `suggestPartitioning` — partitioning strategies for large tables
- `getIndexUsage` — identify unused or underused indexes

### Monitor — watch it live

- `getCurrentActivity` — active queries and connections
- `analyzeLocks` — lock contention and blocking queries
- `getLiveMetrics` — real-time metrics over a time window
- `getHottestTables` — tables with highest activity
- `getTableMetrics` — comprehensive per-table I/O and scan stats

### Data Quality — trust your data

- `findDuplicates` — duplicate rows by column combination
- `findMissingValues` — NULL analysis across columns
- `findOrphans` — orphaned records with invalid foreign keys
- `checkConstraintViolations` — test constraints before adding them
- `analyzeTypeConsistency` — type inconsistencies in text columns

### Relationships — follow the connections

- `exploreRelationships` — multi-hop foreign key traversal
- `analyzeForeignKeys` — foreign key health and performance

### Time Series — temporal analysis

- `findRecent` — rows within a time window
- `analyzeTimeSeries` — window functions and anomaly detection
- `detectSeasonality` — seasonal pattern detection

### Export — get data out

- `exportTable` — CSV, JSON, JSONL, or SQL
- `generateInsertStatements` — INSERT statements for migration

### Write (read-write only) — safe modifications

- `previewUpdate` / `previewDelete` — see what would change before committing
- `safeUpdate` — UPDATE with dry-run, row limits, empty WHERE protection
- `safeDelete` — DELETE with dry-run, row limits, empty WHERE protection
- `safeInsert` — INSERT with validation, batching, ON CONFLICT support

## Security

- **Read-only by default** — write operations must be explicitly enabled
- All queries use parameterized values
- SQL injection prevention with input validation and pattern detection
- Identifier sanitization for table/column names
- Rate limiting on all operations
- Query timeouts to prevent long-running queries
- Response size limits to prevent memory exhaustion

## Examples

> *"What are the largest tables and do they have bloat?"*

```
listTables({ schema: "public" })
analyzeTableBloat({ schema: "public", minSizeMb: 100 })
```

> *"Find duplicate emails in the users table."*

```
findDuplicates({ table: "users", columns: ["email"] })
```

> *"Which queries are slowest and how can I speed them up?"*

```
getSlowQueries({ minDurationMs: 100, limit: 10 })
suggestIndexes({ schema: "public" })
```

> *"Show me what's happening on the database right now."*

```
getCurrentActivity()
getLiveMetrics({ metrics: ["queries", "connections", "cache"], duration: 30000, interval: 1000 })
getHottestTables({ limit: 5, orderBy: "seq_scan" })
```

> *"Find orphaned orders that reference deleted customers."*

```
findOrphans({ table: "orders", foreignKey: "customer_id", referenceTable: "customers", referenceColumn: "id" })
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `QUERY_TIMEOUT` | `30000` | Query timeout in milliseconds |
| `MAX_RESULT_ROWS` | `10000` | Maximum rows returned per query |
| `ENABLE_RATE_LIMIT` | `true` | Enable rate limiting |
| `RATE_LIMIT_MAX_REQUESTS` | `100` | Requests per window |
| `RATE_LIMIT_WINDOW_MS` | `60000` | Rate limit window (ms) |
| `PGMAXPOOLSIZE` | `10` | Connection pool max size |
| `PGMINPOOLSIZE` | `2` | Connection pool min size |
| `PGIDLETIMEOUT` | `10000` | Idle connection timeout (ms) |
| `ENABLE_LOGGING` | `false` | Enable file logging |
| `LOG_DIR` | `./logs` | Log file directory |
| `LOG_LEVEL` | `info` | Log verbosity: debug, info, warn, error |

CLI flags: `--read-only` (default), `--read-write`, `--mode <mode>`

## Logging

File logging is disabled by default. Set `ENABLE_LOGGING=true` to enable. Two log files are created in `LOG_DIR`:

- **tool-usage.log** — every tool call with timestamp, name, and arguments
- **error.log** — errors with stack traces

Connection strings are automatically redacted in all output.

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
