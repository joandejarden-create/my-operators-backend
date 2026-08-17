/**
 * AI crawler identity registry (Phase 3C.1).
 * Governed, publicly documented where known. Unknown purposes stay UNKNOWN.
 */

export const AI_CRAWLER_REGISTRY_VERSION = "ai_visibility_ai_crawler_registry_v1";

export const CRAWLER_PURPOSE = Object.freeze({
  SEARCH_CRAWLER: "search_crawler",
  TRAINING_CRAWLER: "training_crawler",
  USER_TRIGGERED_FETCHER: "user_triggered_fetcher",
  UNKNOWN: "UNKNOWN",
});

export const CRAWLER_IDENTITY_CONFIDENCE = Object.freeze({
  DOCUMENTED: "DOCUMENTED",
  PARTIAL: "PARTIAL",
  UNKNOWN: "UNKNOWN",
});

export const AI_CRAWLER_REGISTRY = Object.freeze([
  {
    provider: "openai",
    crawlerName: "OAI-SearchBot",
    userAgentPattern: "OAI-SearchBot",
    purpose: CRAWLER_PURPOSE.SEARCH_CRAWLER,
    publiclyDocumented: true,
    source: "OpenAI published crawler documentation",
    lastVerified: "2026-08-14",
    status: "active",
  },
  {
    provider: "openai",
    crawlerName: "GPTBot",
    userAgentPattern: "GPTBot",
    purpose: CRAWLER_PURPOSE.TRAINING_CRAWLER,
    publiclyDocumented: true,
    source: "OpenAI published crawler documentation",
    lastVerified: "2026-08-14",
    status: "active",
  },
  {
    provider: "openai",
    crawlerName: "ChatGPT-User",
    userAgentPattern: "ChatGPT-User",
    purpose: CRAWLER_PURPOSE.USER_TRIGGERED_FETCHER,
    publiclyDocumented: true,
    source: "OpenAI published crawler documentation",
    lastVerified: "2026-08-14",
    status: "active",
  },
  {
    provider: "google",
    crawlerName: "Googlebot",
    userAgentPattern: "Googlebot",
    purpose: CRAWLER_PURPOSE.SEARCH_CRAWLER,
    publiclyDocumented: true,
    source: "Google Search documentation",
    lastVerified: "2026-08-14",
    status: "active",
    geminiRelevance: "Gemini grounding may use Google Search index; distinct crawler identity not separately documented for Gemini fetches",
  },
  {
    provider: "google",
    crawlerName: "Google-Extended",
    userAgentPattern: "Google-Extended",
    purpose: CRAWLER_PURPOSE.TRAINING_CRAWLER,
    publiclyDocumented: true,
    source: "Google AI training opt-out documentation",
    lastVerified: "2026-08-14",
    status: "active",
  },
  {
    provider: "perplexity",
    crawlerName: "PerplexityBot",
    userAgentPattern: "PerplexityBot",
    purpose: CRAWLER_PURPOSE.SEARCH_CRAWLER,
    publiclyDocumented: true,
    source: "Perplexity published bot documentation",
    lastVerified: "2026-08-14",
    status: "active",
  },
  {
    provider: "anthropic",
    crawlerName: "ClaudeBot",
    userAgentPattern: "ClaudeBot",
    userAgentPatternAlt: "anthropic-ai",
    purpose: CRAWLER_PURPOSE.UNKNOWN,
    publiclyDocumented: true,
    source: "Anthropic published crawler documentation",
    lastVerified: "2026-08-14",
    status: "active",
    note: "Purpose category not explicitly classified in public docs — remains UNKNOWN",
  },
  {
    provider: "anthropic",
    crawlerName: "Claude-User",
    userAgentPattern: "Claude-User",
    purpose: CRAWLER_PURPOSE.USER_TRIGGERED_FETCHER,
    publiclyDocumented: true,
    source: "Anthropic published crawler documentation",
    lastVerified: "2026-08-14",
    status: "active",
  },
]);

export const OAI_SEARCHBOT_SIGNAL = Object.freeze({
  ROBOTS_ACCESS: {
    allowed: "robots_access_allowed",
    blocked: "robots_access_blocked",
    noExplicitDirective: "no_explicit_directive",
    unknown: "unknown",
  },
  ACTUAL_CRAWL: {
    observed: "actual_crawl_observed_in_logs",
    notObserved: "not_observed",
    unknown: "unknown",
  },
  DATA_SOURCE: {
    robots: "public_robots_txt",
    logs: "server_cdn_logs",
  },
  RULE: "robots permission != actual crawl — keep separate",
});

export const PROVIDER_CRAWLER_READINESS_MATRIX = Object.freeze({
  openai: {
    PUBLIC_ROBOTS_CHECK: true,
    LOG_IDENTIFIABLE: true,
    CRAWLER_IDENTITY_CONFIDENCE: CRAWLER_IDENTITY_CONFIDENCE.DOCUMENTED,
    primaryCrawler: "OAI-SearchBot",
  },
  gemini: {
    PUBLIC_ROBOTS_CHECK: true,
    LOG_IDENTIFIABLE: "PARTIAL",
    CRAWLER_IDENTITY_CONFIDENCE: CRAWLER_IDENTITY_CONFIDENCE.PARTIAL,
    note: "Gemini may use Google Search index; separate Gemini-specific crawler not deterministically identifiable",
    primaryCrawler: "Googlebot",
  },
  perplexity: {
    PUBLIC_ROBOTS_CHECK: true,
    LOG_IDENTIFIABLE: true,
    CRAWLER_IDENTITY_CONFIDENCE: CRAWLER_IDENTITY_CONFIDENCE.DOCUMENTED,
    primaryCrawler: "PerplexityBot",
  },
  claude: {
    PUBLIC_ROBOTS_CHECK: true,
    LOG_IDENTIFIABLE: true,
    CRAWLER_IDENTITY_CONFIDENCE: CRAWLER_IDENTITY_CONFIDENCE.PARTIAL,
    primaryCrawler: "ClaudeBot",
    note: "ClaudeBot purpose not explicitly documented",
  },
});

/**
 * Find crawler entries for a provider.
 */
export function getCrawlersForProvider(provider) {
  const id = String(provider || "").toLowerCase();
  const map = {
    openai: "openai",
    gemini: "google",
    google: "google",
    perplexity: "perplexity",
    claude: "anthropic",
    anthropic: "anthropic",
  };
  const key = map[id] || id;
  return AI_CRAWLER_REGISTRY.filter((c) => c.provider === key);
}

/**
 * Match user-agent string to registry entry.
 */
export function matchCrawlerUserAgent(userAgent) {
  const ua = String(userAgent || "");
  for (const entry of AI_CRAWLER_REGISTRY) {
    if (ua.includes(entry.userAgentPattern)) return entry;
    if (entry.userAgentPatternAlt && ua.includes(entry.userAgentPatternAlt)) return entry;
  }
  return null;
}
