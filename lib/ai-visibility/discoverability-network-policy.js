/**
 * Bounded public network access policy for Discoverability checks (Phase 3C.1).
 */

export const DISCOVERABILITY_NETWORK_POLICY_VERSION =
  "ai_visibility_discoverability_network_policy_v1";

export const NETWORK_ACCESS_POLICY = Object.freeze({
  USER_AGENT: "DealalityDiscoverabilityBot/1.0 (+https://dealality.com; brand-crawl-readiness)",
  TIMEOUT_MS: 15000,
  MAX_REDIRECTS: 5,
  MAX_RESPONSE_BYTES: 2 * 1024 * 1024,
  MAX_REQUESTS_PER_RUN: 20,
  MAX_REQUESTS_PER_DOMAIN: 5,
  RESPECT_ROBOTS: true,
  RATE_LIMIT_MS: 1000,
  AGGRESSIVE_CRAWLING: false,
  RULE: "Bounded requests only — no full-site scraping",
});

export function assertWithinRequestBudget(stats = {}) {
  const total = stats.totalRequests || 0;
  const domainCounts = stats.byDomain || {};
  if (total >= NETWORK_ACCESS_POLICY.MAX_REQUESTS_PER_RUN) {
    return { ok: false, reason: "max_requests_per_run_exceeded" };
  }
  for (const [domain, count] of Object.entries(domainCounts)) {
    if (count >= NETWORK_ACCESS_POLICY.MAX_REQUESTS_PER_DOMAIN) {
      return { ok: false, reason: `max_requests_per_domain_exceeded:${domain}` };
    }
  }
  return { ok: true };
}
