import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { existsSync, mkdirSync, appendFileSync } from 'fs';

vi.mock('fs', () => ({
  existsSync: vi.fn().mockReturnValue(false),
  mkdirSync: vi.fn(),
  appendFileSync: vi.fn(),
  writeFileSync: vi.fn(),
}));

describe('Logger opt-in file logging', () => {
  const originalEnv = process.env.ENABLE_LOGGING;

  beforeEach(() => {
    vi.resetModules();
    vi.mocked(existsSync).mockReturnValue(false);
    vi.mocked(mkdirSync).mockClear();
    vi.mocked(appendFileSync).mockClear();
  });

  afterEach(() => {
    if (originalEnv === undefined) {
      delete process.env.ENABLE_LOGGING;
    } else {
      process.env.ENABLE_LOGGING = originalEnv;
    }
  });

  it('should NOT create log directory when ENABLE_LOGGING is not set', async () => {
    delete process.env.ENABLE_LOGGING;
    const { Logger } = await import('../src/utils/logger.js');
    new Logger('./logs', 'info');
    expect(mkdirSync).not.toHaveBeenCalled();
  });

  it('should NOT write to log files when ENABLE_LOGGING is not set', async () => {
    delete process.env.ENABLE_LOGGING;
    const { Logger } = await import('../src/utils/logger.js');
    const logger = new Logger('./logs', 'info');
    logger.info('test-tool', 'some message');
    expect(appendFileSync).not.toHaveBeenCalled();
  });

  it('should create log directory when ENABLE_LOGGING=true', async () => {
    process.env.ENABLE_LOGGING = 'true';
    const { Logger } = await import('../src/utils/logger.js');
    new Logger('./logs', 'info');
    expect(mkdirSync).toHaveBeenCalledWith('./logs', { recursive: true });
  });

  it('should write to log files when ENABLE_LOGGING=true', async () => {
    process.env.ENABLE_LOGGING = 'true';
    const { Logger } = await import('../src/utils/logger.js');
    const logger = new Logger('./logs', 'info');
    logger.info('test-tool', 'some message');
    expect(appendFileSync).toHaveBeenCalled();
  });

  it('should NOT write to log files when ENABLE_LOGGING=false', async () => {
    process.env.ENABLE_LOGGING = 'false';
    const { Logger } = await import('../src/utils/logger.js');
    const logger = new Logger('./logs', 'info');
    logger.info('test-tool', 'some message');
    expect(appendFileSync).not.toHaveBeenCalled();
  });
});
