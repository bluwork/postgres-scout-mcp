import { appendFileSync, existsSync, mkdirSync } from 'fs';
import { join } from 'path';
import { LogEntry } from '../types.js';
import { sanitizeLogValue } from './sanitize.js';

export class Logger {
  private logDir: string;
  private logLevel: 'debug' | 'info' | 'warn' | 'error';
  private toolLogPath: string;
  private errorLogPath: string;
  private fileLoggingEnabled: boolean;

  constructor(logDir: string, logLevel: 'debug' | 'info' | 'warn' | 'error' = 'info') {
    this.logDir = logDir;
    this.logLevel = logLevel;
    this.toolLogPath = join(logDir, 'tool-usage.log');
    this.errorLogPath = join(logDir, 'error.log');
    this.fileLoggingEnabled = process.env.ENABLE_LOGGING === 'true';

    if (this.fileLoggingEnabled && !existsSync(logDir)) {
      mkdirSync(logDir, { recursive: true });
    }
  }

  private getLevelPriority(level: string): number {
    const priorities = { debug: 0, info: 1, warn: 2, error: 3 };
    return priorities[level as keyof typeof priorities] ?? 1;
  }

  private shouldLog(level: string): boolean {
    return this.getLevelPriority(level) >= this.getLevelPriority(this.logLevel);
  }

  private formatLogEntry(entry: LogEntry): string {
    const timestamp = entry.timestamp.toISOString();
    const safeMessage = sanitizeLogValue(entry.message);
    const safeTool = sanitizeLogValue(entry.tool);
    const dataStr = entry.data ? `, Data: ${sanitizeLogValue(entry.data)}` : '';
    return `${timestamp} [${entry.level.toUpperCase()}] Tool: ${safeTool}, Message: ${safeMessage}${dataStr}\n`;
  }

  private writeLog(filePath: string, content: string): void {
    try {
      appendFileSync(filePath, content, 'utf8');
    } catch (error) {
      console.error(`Failed to write to log file ${filePath}:`, error);
    }
  }

  log(level: 'debug' | 'info' | 'warn' | 'error', tool: string, message: string, data?: any): void {
    if (!this.shouldLog(level)) return;

    const entry: LogEntry = {
      timestamp: new Date(),
      level,
      tool,
      message,
      data
    };

    const formatted = this.formatLogEntry(entry);

    console.error(formatted.trim());

    if (this.fileLoggingEnabled) {
      this.writeLog(this.toolLogPath, formatted);

      if (level === 'error') {
        this.writeLog(this.errorLogPath, formatted);
      }
    }
  }

  debug(tool: string, message: string, data?: any): void {
    this.log('debug', tool, message, data);
  }

  info(tool: string, message: string, data?: any): void {
    this.log('info', tool, message, data);
  }

  warn(tool: string, message: string, data?: any): void {
    this.log('warn', tool, message, data);
  }

  error(tool: string, message: string, data?: any): void {
    this.log('error', tool, message, data);
  }
}

export const createLogger = (logDir: string, logLevel: 'debug' | 'info' | 'warn' | 'error' = 'info'): Logger => {
  return new Logger(logDir, logLevel);
};
