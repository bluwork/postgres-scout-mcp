#!/usr/bin/env node

import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { spawnSync } from 'child_process';

// Get the directory of the current module
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Parse command line arguments
const args = process.argv.slice(2);
const helpText = `
Postgres Scout MCP - PostgreSQL Model Context Protocol Server

Usage:
  postgres-scout-mcp [options] [postgresql-uri]

Options:
  --help, -h         Show this help message
  --version, -v      Show version number
  --read-only        Run server in read-only mode (default)
  --read-write       Run server in read-write mode (enables all write operations)
  --mode <mode>      Set mode: 'read-only' or 'read-write'

Arguments:
  postgresql-uri     PostgreSQL connection URI (default: postgresql://localhost:5432/postgres)

Examples:
  postgres-scout-mcp
  postgres-scout-mcp --read-write postgresql://localhost:5432/mydb
  postgres-scout-mcp --mode read-only postgresql://localhost:5432/mydb
`;

// Handle command-line options
if (args.includes('--help') || args.includes('-h')) {
  console.log(helpText);
  process.exit(0);
}

if (args.includes('--version') || args.includes('-v')) {
  // Read version from package.json
  try {
    const packageJsonPath = join(__dirname, '..', 'package.json');
    const packageJson = await import(packageJsonPath, { with: { type: 'json' } });
    console.log(`Postgres Scout MCP v${packageJson.default.version}`);
    process.exit(0);
  } catch (error) {
    console.error(
      'Could not determine version:',
      error instanceof Error ? error.message : String(error)
    );
    process.exit(1);
  }
}

const filteredArgs = args.filter((arg) => !['--help', '-h', '--version', '-v'].includes(arg));

// Launch the actual MCP server
const serverPath = join(__dirname, '..', 'dist', 'index.js');
const nodeProcess = spawnSync('node', [serverPath, ...filteredArgs], {
  stdio: 'inherit',
  shell: process.platform === 'win32',
});

// Forward the exit code from the child process
process.exit(nodeProcess.status || 0);
