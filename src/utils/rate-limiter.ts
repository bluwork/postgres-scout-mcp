export class RateLimiter {
  private requests: number[] = [];
  private maxRequests: number;
  private windowMs: number;
  private enabled: boolean;

  constructor(maxRequests: number, windowMs: number, enabled: boolean = true) {
    this.maxRequests = maxRequests;
    this.windowMs = windowMs;
    this.enabled = enabled;
  }

  checkLimit(): void {
    if (!this.enabled) return;

    const now = Date.now();
    const windowStart = now - this.windowMs;

    this.requests = this.requests.filter(timestamp => timestamp > windowStart);

    if (this.requests.length >= this.maxRequests) {
      const oldestRequest = this.requests[0];
      const resetIn = Math.ceil((oldestRequest + this.windowMs - now) / 1000);
      throw new Error(
        `Rate limit exceeded. Maximum ${this.maxRequests} requests per ${this.windowMs / 1000} seconds. ` +
        `Try again in ${resetIn} seconds.`
      );
    }

    this.requests.push(now);
  }

  reset(): void {
    this.requests = [];
  }

  getStats(): { current: number; max: number; windowMs: number } {
    const now = Date.now();
    const windowStart = now - this.windowMs;
    const currentRequests = this.requests.filter(timestamp => timestamp > windowStart).length;

    return {
      current: currentRequests,
      max: this.maxRequests,
      windowMs: this.windowMs
    };
  }
}
