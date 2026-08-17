/**
 * Log + Analytics adapter interfaces (Phase 3C.1).
 * Contract/seam only — no live connections.
 */

export const LOG_ADAPTER_VERSION = "ai_visibility_log_adapter_v1";
export const ANALYTICS_ADAPTER_VERSION = "ai_visibility_analytics_adapter_v1";

export const LOG_ENTRY_CONTRACT = Object.freeze({
  timestamp: "ISO8601",
  userAgent: "string",
  providerCrawler: "string|null",
  requestUrl: "string",
  statusCode: "number",
  method: "string",
  country: "string|null",
  edgeOrOrigin: "edge|origin|null",
  bytes: "number|null",
  botClassificationSource: "string",
});

export const LOG_ADAPTER_PROVIDERS_FUTURE = Object.freeze([
  "cloudflare",
  "aws_cloudfront",
  "aws_alb",
  "akamai",
  "fastly",
  "vercel",
  "netlify",
  "origin_server",
  "manual_upload",
]);

export const LOG_BASED_CRAWLER_SIGNALS = Object.freeze([
  "ai_bot_requests",
  "unique_urls_crawled",
  "priority_pages_crawled",
  "last_crawl",
  "crawl_frequency",
  "provider_crawler",
  "http_status",
  "blocked_error_responses",
]);

export const ANALYTICS_ENTRY_CONTRACT = Object.freeze({
  sessions: "number",
  source: "string|null",
  medium: "string|null",
  referrer: "string|null",
  landingPage: "string|null",
  engagement: "number|null",
  event: "string|null",
  timestamp: "ISO8601",
  sessionKey: "string|null",
  userKey: "string|null",
});

/**
 * Log adapter interface — implement for future CDN/server providers.
 */
export class LogAdapterInterface {
  constructor(config = {}) {
    this.config = config;
    this.provider = config.provider || "unknown";
  }

  /** @returns {Promise<{ ok: boolean, connectionStatus: string }>} */
  async checkConnection() {
    return { ok: false, connectionStatus: "CONNECTION_REQUIRED" };
  }

  /** @returns {Promise<typeof LOG_ENTRY_CONTRACT[]>} */
  async fetchBotRequests(_opts = {}) {
    throw new Error("LOG_ADAPTER_NOT_CONNECTED");
  }
}

/**
 * Analytics adapter interface — implement for GA4, Adobe, Matomo, etc.
 */
export class AnalyticsAdapterInterface {
  constructor(config = {}) {
    this.config = config;
    this.provider = config.provider || "unknown";
  }

  /** @returns {Promise<{ ok: boolean, connectionStatus: string }>} */
  async checkConnection() {
    return { ok: false, connectionStatus: "CONNECTION_REQUIRED" };
  }

  /** @returns {Promise<typeof ANALYTICS_ENTRY_CONTRACT[]>} */
  async fetchReferralSessions(_opts = {}) {
    throw new Error("ANALYTICS_ADAPTER_NOT_CONNECTED");
  }

  /** @returns {Promise<typeof ANALYTICS_ENTRY_CONTRACT[]>} */
  async fetchEvents(_opts = {}) {
    throw new Error("ANALYTICS_ADAPTER_NOT_CONNECTED");
  }
}

export const LOG_ADAPTER_INTERFACE_READY = true;
export const ANALYTICS_ADAPTER_INTERFACE_READY = true;
export const LIVE_LOG_CONNECTION = false;
export const LIVE_ANALYTICS_CONNECTION = false;
