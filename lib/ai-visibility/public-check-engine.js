/**
 * Public check engine — bounded deterministic technical checks (Phase 3C.1).
 * Fixtures/tests first. Optional bounded live fetch when explicitly enabled.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { NETWORK_ACCESS_POLICY, assertWithinRequestBudget } from "./discoverability-network-policy.js";
import {
  analyzeRobotsTxt,
  extractCanonicalFromHtml,
  extractMetaRobots,
  extractPageTitle,
  assessContentInInitialHtml,
  assessDevelopmentContentPresence,
  evaluateIndexability,
  parseSitemapUrls,
  isPageInSitemap,
  PUBLIC_CHECKS_V1,
} from "./public-crawl-checks.js";
import { resolveGovernedBrandUrl } from "./brand-url-governance.js";
import { buildDiscoverabilitySnapshot } from "./discoverability-snapshot.js";
import { DATA_STATE } from "./discoverability-data-states.js";

export const PUBLIC_CHECK_ENGINE_VERSION = "ai_visibility_public_check_engine_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Bounded HTTP fetch with redirect limit.
 */
export async function boundedFetch(url, opts = {}) {
  const timeout = opts.timeoutMs || NETWORK_ACCESS_POLICY.TIMEOUT_MS;
  const maxRedirects = opts.maxRedirects ?? NETWORK_ACCESS_POLICY.MAX_REDIRECTS;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  const redirectChain = [];

  try {
    let currentUrl = url;
    for (let i = 0; i <= maxRedirects; i++) {
      const res = await fetch(currentUrl, {
        signal: controller.signal,
        headers: {
          "User-Agent": opts.userAgent || NETWORK_ACCESS_POLICY.USER_AGENT,
          Accept: "text/html,application/xhtml+xml,text/plain,*/*",
        },
        redirect: "manual",
      });

      if (res.status >= 300 && res.status < 400) {
        const loc = res.headers.get("location");
        if (!loc || i >= maxRedirects) {
          return {
            ok: false,
            url: currentUrl,
            status: res.status,
            redirectChain,
            error: "redirect_limit_or_missing_location",
          };
        }
        redirectChain.push({ from: currentUrl, status: res.status, to: loc });
        currentUrl = new URL(loc, currentUrl).href;
        continue;
      }

      const buf = await res.arrayBuffer();
      const truncated = buf.byteLength > NETWORK_ACCESS_POLICY.MAX_RESPONSE_BYTES;
      const body = new TextDecoder("utf-8", { fatal: false }).decode(
        truncated ? buf.slice(0, NETWORK_ACCESS_POLICY.MAX_RESPONSE_BYTES) : buf
      );

      return {
        ok: res.ok,
        url: currentUrl,
        status: res.status,
        headers: Object.fromEntries(res.headers.entries()),
        body,
        truncated,
        redirectChain,
      };
    }
    return { ok: false, url, error: "redirect_exhausted", redirectChain };
  } catch (err) {
    return { ok: false, url, error: err.message || "fetch_failed", redirectChain };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Run public checks from pre-fetched/fixture data (no network).
 */
export function runPublicChecksFromFixtures(input = {}) {
  const pageUrl = input.pageUrl || input.url;
  const html = input.html || "";
  const robotsContent = input.robotsContent || "";
  const sitemapContent = input.sitemapContent || "";
  const httpStatus = input.httpStatus ?? 200;

  const robots = robotsContent ? analyzeRobotsTxt(robotsContent, { path: new URL(pageUrl).pathname }) : null;
  const metaRobots = extractMetaRobots(html);
  const canonical = extractCanonicalFromHtml(html);
  const title = extractPageTitle(html);
  const contentInInitialHtml = assessContentInInitialHtml(html);
  const developmentContent = assessDevelopmentContentPresence(html, {
    brandName: input.brandName,
  });
  const sitemapUrls = sitemapContent ? parseSitemapUrls(sitemapContent) : [];
  const inSitemap = isPageInSitemap(pageUrl, sitemapUrls);

  const indexability = evaluateIndexability({
    httpStatus,
    metaRobots,
    robotsAllowed: robots?.oaiSearchBot?.allowed,
    html,
  });

  return {
    version: PUBLIC_CHECK_ENGINE_VERSION,
    pageUrl,
    checks: PUBLIC_CHECKS_V1,
    httpStatus,
    robots: robots
      ? {
          reachable: true,
          oaiSearchBot: robots.oaiSearchBot,
          sitemaps: robots.sitemaps,
          malformed: robots.malformed,
        }
      : { reachable: false },
    sitemap: {
      declaredInRobots: (robots?.sitemaps || []).length > 0,
      reachable: Boolean(sitemapContent),
      pagePresent: inSitemap,
      urlCount: sitemapUrls.length,
    },
    canonical,
    metaRobots,
    title,
    contentInInitialHtml,
    developmentContent,
    indexability,
    redirectChain: input.redirectChain || [],
    dataState: DATA_STATE.MEASURED,
  };
}

/**
 * Run bounded live public check for one brand URL.
 */
export async function runBoundedLivePublicCheck(brandInput = {}, stats = {}) {
  const governed = resolveGovernedBrandUrl(brandInput);
  if (!governed.ok || !governed.url) {
    return {
      ok: false,
      brandId: brandInput.brandId,
      error: governed.gap || "no_url",
      dataState: DATA_STATE.CONNECTION_REQUIRED,
    };
  }

  const domain = governed.domain;
  stats.byDomain = stats.byDomain || {};
  stats.byDomain[domain] = (stats.byDomain[domain] || 0) + 1;
  stats.totalRequests = (stats.totalRequests || 0) + 1;
  const budget = assertWithinRequestBudget(stats);
  if (!budget.ok) {
    return { ok: false, error: budget.reason, dataState: DATA_STATE.UNAVAILABLE };
  }

  let origin;
  try {
    origin = new URL(governed.url).origin;
  } catch {
    return { ok: false, error: "invalid_url", dataState: DATA_STATE.UNAVAILABLE };
  }

  const robotsUrl = `${origin}/robots.txt`;
  stats.totalRequests += 1;
  stats.byDomain[domain] += 1;
  const robotsFetch = await boundedFetch(robotsUrl);

  stats.totalRequests += 1;
  stats.byDomain[domain] += 1;
  const pageFetch = await boundedFetch(governed.url);

  let sitemapContent = null;
  const robotsAnalysis = robotsFetch.body
    ? analyzeRobotsTxt(robotsFetch.body, { path: new URL(governed.url).pathname })
    : null;
  const sitemapUrl = robotsAnalysis?.sitemaps?.[0];
  if (sitemapUrl) {
    stats.totalRequests += 1;
    stats.byDomain[domain] += 1;
    const sm = await boundedFetch(sitemapUrl);
    sitemapContent = sm.body || null;
  }

  const result = runPublicChecksFromFixtures({
    pageUrl: pageFetch.url || governed.url,
    html: pageFetch.body || "",
    robotsContent: robotsFetch.body || "",
    sitemapContent,
    httpStatus: pageFetch.status,
    brandName: brandInput.brandName,
    redirectChain: pageFetch.redirectChain,
  });

  const snapshot = buildDiscoverabilitySnapshot({
    brandId: brandInput.brandId,
    domain: governed.domain,
    pageUrl: governed.url,
    robots: result.robots,
    canonical: result.canonical,
    indexability: result.indexability,
    crawlerAccess: result.robots?.oaiSearchBot,
    contentInInitialHtml: result.contentInInitialHtml,
    developmentContent: result.developmentContent,
    evidence: [{ type: "public_check", at: new Date().toISOString() }],
  });

  return {
    ok: true,
    brandId: brandInput.brandId,
    brandName: brandInput.brandName,
    governed,
    result,
    snapshot,
    stats,
    LIVE_FETCH: true,
  };
}

/**
 * Load pilot brand fixture.
 */
export function loadDiscoverabilityPilotBrands(fixturePath) {
  const p =
    fixturePath ||
    path.join(__dirname, "..", "..", "fixtures", "ai-visibility", "discoverability-pilot-brands-v1.json");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export const PUBLIC_CHECK_ENGINE_READY = true;
