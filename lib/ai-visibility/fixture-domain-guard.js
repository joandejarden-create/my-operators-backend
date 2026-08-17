/**
 * Fixture / synthetic domain guard for Brand AI Visibility product reads.
 * Fixtures stay in fixtures/; runtime UI must not surface known test domains
 * unless they appear as genuine cited hosts (example.com is never genuine).
 */

export const FIXTURE_DOMAIN_GUARD_VERSION = "ai_visibility_fixture_domain_guard_v1";

/** Domains that must never appear in product-facing Evidence / Sources. */
export const BLOCKED_FIXTURE_DOMAINS = Object.freeze([
  "example.com",
  "example.org",
  "example.net",
  "test.com",
  "localhost",
  "127.0.0.1",
]);

/**
 * @param {string|null|undefined} domainOrUrl
 * @returns {string|null} lowercase hostname without www.
 */
export function normalizeSourceHost(domainOrUrl) {
  if (domainOrUrl == null) return null;
  let s = String(domainOrUrl).trim().toLowerCase();
  if (!s) return null;
  try {
    if (s.includes("://")) s = new URL(s).hostname;
  } catch {
    // plain domain
  }
  s = s.replace(/^www\./, "").split("/")[0].split("?")[0];
  return s || null;
}

/**
 * @param {string|null|undefined} domainOrUrl
 */
export function isBlockedFixtureDomain(domainOrUrl) {
  const host = normalizeSourceHost(domainOrUrl);
  if (!host) return false;
  return BLOCKED_FIXTURE_DOMAINS.some(
    (blocked) => host === blocked || host.endsWith(`.${blocked}`)
  );
}

/**
 * Filter citation/source rows that carry a domain or url field.
 * @template T
 * @param {T[]} rows
 * @param {{ domainKey?: string, urlKey?: string }} [opts]
 * @returns {T[]}
 */
export function filterFixtureContaminatedSources(rows, opts = {}) {
  const domainKey = opts.domainKey || "domain";
  const urlKey = opts.urlKey || "url";
  if (!Array.isArray(rows)) return [];
  return rows.filter((row) => {
    if (!row || typeof row !== "object") return true;
    const domain = row[domainKey] ?? row.host ?? row.hostname;
    const url = row[urlKey] ?? row.href;
    if (isBlockedFixtureDomain(domain) || isBlockedFixtureDomain(url)) return false;
    return true;
  });
}
