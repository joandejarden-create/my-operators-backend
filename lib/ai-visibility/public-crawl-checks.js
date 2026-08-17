/**
 * Public crawl readiness checks — HTTP, canonical, noindex, HTML, sitemap (Phase 3C.1).
 */

import { parseRobotsTxt, evaluateOaiSearchBotAccess, extractSitemapsFromRobots, isPathAllowed } from "./robots-parser.js";
import { CONTENT_IN_INITIAL_HTML, INDEXABILITY_STATUS } from "./discoverability-dimensions.js";
import { DEVELOPMENT_CONTENT_FIELDS_V1 } from "./discoverability-dimensions.js";

export const PUBLIC_CRAWL_CHECKS_VERSION = "ai_visibility_public_crawl_checks_v1";

export const PUBLIC_CHECKS_V1 = Object.freeze([
  "robots_txt_reachable",
  "robots_directives",
  "sitemap_reachable",
  "http_status",
  "canonical_tag",
  "meta_robots_noindex",
  "crawlable_html",
  "page_title",
  "redirect_chain",
  "server_response",
]);

/**
 * Extract canonical URL from HTML.
 */
export function extractCanonicalFromHtml(html) {
  const match = String(html || "").match(
    /<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i
  );
  return match ? match[1].trim() : null;
}

/**
 * Extract meta robots noindex from HTML.
 */
export function extractMetaRobots(html) {
  const match = String(html || "").match(
    /<meta[^>]+name=["']robots["'][^>]+content=["']([^"']+)["']/i
  );
  if (!match) return { present: false, noindex: false, content: null };
  const content = match[1].toLowerCase();
  return {
    present: true,
    noindex: content.includes("noindex"),
    nofollow: content.includes("nofollow"),
    content: match[1],
  };
}

/**
 * Extract page title from HTML.
 */
export function extractPageTitle(html) {
  const match = String(html || "").match(/<title[^>]*>([^<]*)<\/title>/i);
  return match ? match[1].trim() : null;
}

/**
 * Assess content in initial HTML (no JS execution).
 */
export function assessContentInInitialHtml(html, opts = {}) {
  const text = stripHtmlToText(html);
  const minChars = opts.minChars ?? 200;
  if (!text || text.length < 50) return CONTENT_IN_INITIAL_HTML.NO;
  if (text.length >= minChars) return CONTENT_IN_INITIAL_HTML.YES;
  return CONTENT_IN_INITIAL_HTML.PARTIAL;
}

function stripHtmlToText(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Deterministic development content presence (factual, not quality scoring).
 */
export function assessDevelopmentContentPresence(html, opts = {}) {
  const text = stripHtmlToText(html).toLowerCase();
  const brandName = String(opts.brandName || "").toLowerCase();
  return {
    brandNamePresent: brandName ? text.includes(brandName) : null,
    developmentFranchisePositioningPresent: /develop|franchise|ownership|owner|invest/.test(text),
    conversionLanguagePresent: /contact|inquiry|apply|get started|learn more/.test(text),
    ownerDeveloperContactPresent: /contact|inquiry|email|phone|form/.test(text),
    residencesPresent: /residen/.test(text),
    geographicDevelopmentInfoPresent: /market|region|country|location|territor/.test(text),
    brandDifferentiationContentPresent: /unique|different|position|value|promise/.test(text),
  };
}

/**
 * Evaluate indexability from public signals.
 */
export function evaluateIndexability(checks = {}) {
  if (checks.httpStatus == null) return INDEXABILITY_STATUS.UNKNOWN;
  if (checks.httpStatus < 200 || checks.httpStatus >= 300) {
    return INDEXABILITY_STATUS.NOT_TECHNICALLY_INDEXABLE;
  }
  if (checks.metaRobots?.noindex) return INDEXABILITY_STATUS.NOT_TECHNICALLY_INDEXABLE;
  if (checks.robotsAllowed === false) return INDEXABILITY_STATUS.NOT_TECHNICALLY_INDEXABLE;
  if (assessContentInInitialHtml(checks.html) === CONTENT_IN_INITIAL_HTML.NO) {
    return INDEXABILITY_STATUS.NOT_TECHNICALLY_INDEXABLE;
  }
  return INDEXABILITY_STATUS.TECHNICALLY_INDEXABLE;
}

/**
 * Parse robots.txt check result from fetched content.
 */
export function analyzeRobotsTxt(content, opts = {}) {
  const parsed = parseRobotsTxt(content);
  const path = opts.path || "/";
  const crawlers = opts.crawlers || ["OAI-SearchBot", "GPTBot", "Googlebot", "PerplexityBot", "ClaudeBot"];
  const access = {};
  for (const ua of crawlers) {
    if (ua === "OAI-SearchBot") {
      access[ua] = evaluateOaiSearchBotAccess(parsed, path);
    } else {
      access[ua] = {
        userAgent: ua,
        allowed: parsed.groups.length ? isPathAllowed(parsed, ua, path).allowed : true,
      };
    }
  }
  return {
    parsed,
    sitemaps: extractSitemapsFromRobots(parsed),
    crawlerAccess: access,
    oaiSearchBot: evaluateOaiSearchBotAccess(parsed, path),
    malformed: parsed.malformed,
  };
}

/**
 * Parse simple sitemap index or urlset for URL count (bounded).
 */
export function parseSitemapUrls(xml, limit = 100) {
  const urls = [];
  const re = /<loc>([^<]+)<\/loc>/gi;
  let m;
  while ((m = re.exec(String(xml || ""))) && urls.length < limit) {
    urls.push(m[1].trim());
  }
  return urls;
}

/**
 * Check if priority page URL appears in sitemap URLs.
 */
export function isPageInSitemap(pageUrl, sitemapUrls = []) {
  if (!pageUrl) return null;
  const normalized = String(pageUrl).replace(/\/$/, "");
  return sitemapUrls.some((u) => String(u).replace(/\/$/, "") === normalized);
}
