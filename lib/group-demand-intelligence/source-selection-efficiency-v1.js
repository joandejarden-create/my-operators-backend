/**
 * GDI 2.0 Phase 2F — source selection efficiency.
 *
 * Prefer verified seeds, discovered official nav links, leadership directories,
 * housing/planning providers, and announcements. Normalize URLs. Treat 404 /
 * irrelevant / empty commercial pages as strategy-change signals.
 */

export const SOURCE_SELECTION_EFFICIENCY_V1 = "gdi-source-selection-efficiency-v1";

const SOCIAL_RE = /linkedin\.com|facebook\.com|twitter\.com|x\.com|instagram\.com|youtube\.com|tiktok\.com/i;
const LOW_VALUE_PATH_RE = /\/(privacy|terms|cookie|legal|login|signin|cart|checkout)(\/|$|\?)/i;
const CAREERS_ONLY_RE = /\/(careers|jobs|job-openings|recruiting)(\/|$|\?)/i;
const HIGH_VALUE_PATH_RE =
  /\/(about|about-us|team|leadership|our-team|company|contact|contact-us|housing|hotel|hotels|accommodation|events|meetings|conference|organizer|venue|press|news|newsroom|announcement)/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Normalize equivalent URLs for dedupe / cache keys.
 */
export function normalizeSourceUrl(raw) {
  const s = text(raw);
  if (!/^https?:\/\//i.test(s)) return "";
  try {
    const u = new URL(s);
    u.hash = "";
    // Drop common tracking params
    for (const key of [...u.searchParams.keys()]) {
      if (/^(utm_|fbclid|gclid|mc_|_ga|ref$)/i.test(key)) u.searchParams.delete(key);
    }
    let path = u.pathname.replace(/\/+$/, "") || "/";
    // Collapse index
    path = path.replace(/\/(index|default)\.(html?|aspx|php)$/i, "");
    u.pathname = path || "/";
    u.hostname = u.hostname.replace(/^www\./i, "").toLowerCase();
    // Prefer https form for cache key
    const proto = "https:";
    const q = u.searchParams.toString();
    return `${proto}//${u.hostname}${u.pathname}${q ? `?${q}` : ""}`;
  } catch {
    return s.toLowerCase();
  }
}

export function scoreSourceCandidate(url, meta = {}) {
  const n = normalizeSourceUrl(url);
  if (!n) return { url, normalized: "", score: -100, reason: "invalid_url" };
  if (SOCIAL_RE.test(n)) return { url, normalized: n, score: -50, reason: "social_excluded" };
  if (LOW_VALUE_PATH_RE.test(n)) return { url, normalized: n, score: -20, reason: "low_value_path" };

  let score = 0;
  const reasons = [];
  if (meta.verifiedExisting) {
    score += 40;
    reasons.push("verified_existing_source");
  }
  if (meta.discoveredNavLink) {
    score += 25;
    reasons.push("official_nav_discovered");
  }
  if (meta.fromCrossOpportunityCache) {
    score += 20;
    reasons.push("cross_opportunity_cache");
  }
  if (HIGH_VALUE_PATH_RE.test(n)) {
    score += 15;
    reasons.push("high_value_path");
  }
  if (CAREERS_ONLY_RE.test(n) && !meta.allowCareers) {
    score -= 10;
    reasons.push("careers_deprioritized_for_hotel_buyer");
  }
  if (meta.prior404) {
    score -= 40;
    reasons.push("prior_404");
  }
  if (meta.priorNoCommercialEvidence) {
    score -= 25;
    reasons.push("prior_no_commercial_evidence");
  }
  if (meta.priorIrrelevant) {
    score -= 30;
    reasons.push("prior_irrelevant");
  }
  if (meta.guessedPath) {
    score -= 5;
    reasons.push("guessed_path");
  }
  return { url, normalized: n, score, reasons };
}

/**
 * Extract same-origin navigation-looking links from HTML/text.
 */
export function discoverOfficialNavLinks(pageText = "", baseUrl = "") {
  const origin = (() => {
    try {
      return new URL(baseUrl).origin;
    } catch {
      return "";
    }
  })();
  if (!origin) return [];
  const found = new Set();
  const re = /href=["']([^"']+)["']/gi;
  let m;
  while ((m = re.exec(String(pageText || "")))) {
    const href = text(m[1]);
    if (!href || href.startsWith("#") || href.startsWith("mailto:") || href.startsWith("tel:")) continue;
    let abs = "";
    try {
      abs = new URL(href, origin).href;
    } catch {
      continue;
    }
    if (!abs.startsWith(origin)) continue;
    const scored = scoreSourceCandidate(abs, { discoveredNavLink: true });
    if (scored.score < 0) continue;
    if (HIGH_VALUE_PATH_RE.test(scored.normalized) || /contact|about|team|leadership|housing|hotel|event/i.test(scored.normalized)) {
      found.add(scored.normalized);
    }
  }
  return [...found];
}

/**
 * Rank fetch candidates: seeds first, then discovered nav, then careful guesses.
 */
export function selectResearchUrls({
  seedUrls = [],
  discoveredNav = [],
  strategyId = "",
  attemptedNormalized = new Set(),
  signalMemory = new Map(),
  maxUrls = 2,
  crossOpportunityCache = [],
} = {}) {
  const candidates = [];

  for (const u of seedUrls) {
    candidates.push(
      scoreSourceCandidate(u, {
        verifiedExisting: true,
        ...(signalMemory.get(normalizeSourceUrl(u)) || {}),
      }),
    );
  }
  for (const u of crossOpportunityCache) {
    candidates.push(
      scoreSourceCandidate(u, {
        fromCrossOpportunityCache: true,
        ...(signalMemory.get(normalizeSourceUrl(u)) || {}),
      }),
    );
  }
  for (const u of discoveredNav) {
    candidates.push(
      scoreSourceCandidate(u, {
        discoveredNavLink: true,
        ...(signalMemory.get(normalizeSourceUrl(u)) || {}),
      }),
    );
  }

  // Limited path guesses from seed origins only
  const origins = [
    ...new Set(
      seedUrls
        .map((u) => {
          try {
            return new URL(u).origin;
          } catch {
            return "";
          }
        })
        .filter(Boolean),
    ),
  ].slice(0, 2);

  const pathHints = [];
  if (/LEADERSHIP|OFFICIAL|BUYER|WHO/i.test(strategyId)) {
    pathHints.push("/about", "/about-us", "/team", "/leadership", "/our-team");
  }
  if (/MEETINGS|EVENT|HOUSING|DEMAND|CONTACT|ORG_ROUTE/i.test(strategyId)) {
    pathHints.push("/contact", "/contact-us", "/housing", "/hotels", "/events", "/meetings");
  }
  if (/WORKPLACE|FACILITIES|PEOPLE|HR/i.test(strategyId)) {
    pathHints.push("/about", "/team", "/people");
    // careers allowed but deprioritized
    pathHints.push("/careers");
  }
  if (!pathHints.length) {
    pathHints.push("/about", "/contact", "/team");
  }

  for (const origin of origins) {
    for (const path of pathHints) {
      candidates.push(
        scoreSourceCandidate(`${origin}${path}`, {
          guessedPath: true,
          allowCareers: path === "/careers",
          ...(signalMemory.get(normalizeSourceUrl(`${origin}${path}`)) || {}),
        }),
      );
    }
  }

  const seen = new Set();
  const ranked = [];
  for (const c of candidates.sort((a, b) => b.score - a.score)) {
    if (!c.normalized || c.score < 0) continue;
    if (attemptedNormalized.has(c.normalized)) continue;
    if (seen.has(c.normalized)) continue;
    seen.add(c.normalized);
    ranked.push(c);
  }

  return {
    version: SOURCE_SELECTION_EFFICIENCY_V1,
    selected: ranked.slice(0, maxUrls),
    deferred: ranked.slice(maxUrls),
    skippedLowValue: candidates.filter((c) => c.score < 0).length,
  };
}

/**
 * Update per-URL signal memory after a fetch.
 */
export function updateSourceSignalMemory(memory, url, result = {}) {
  const n = normalizeSourceUrl(url);
  if (!n) return memory;
  const prev = memory.get(n) || {};
  const next = { ...prev };
  if (result.status === 404 || /404|not_found/i.test(String(result.error || ""))) {
    next.prior404 = true;
  }
  if (result.ok && result.noCommercialEvidence) {
    next.priorNoCommercialEvidence = true;
  }
  if (result.irrelevant) {
    next.priorIrrelevant = true;
  }
  if (result.useful) {
    next.verifiedExisting = true;
    next.priorNoCommercialEvidence = false;
  }
  memory.set(n, next);
  return memory;
}

/**
 * Recommend whether to change strategy based on recent fetch signals.
 */
export function recommendStrategyChange({ recentFetches = [], currentStrategyId = "" } = {}) {
  if (!recentFetches.length) {
    return { change: false, reason: "no_fetches_yet" };
  }
  const fails = recentFetches.filter((f) => f.failed || f.status === 404).length;
  const empty = recentFetches.filter((f) => f.noCommercialEvidence).length;
  const irrelevant = recentFetches.filter((f) => f.irrelevant).length;
  if (fails >= 2) {
    return {
      change: true,
      reason: "repeated_404_or_failed_fetches",
      currentStrategyId,
    };
  }
  if (empty + irrelevant >= 2) {
    return {
      change: true,
      reason: "repeated_low_information_or_irrelevant_pages",
      currentStrategyId,
    };
  }
  return { change: false, reason: "continue_current_strategy" };
}

export function createSourceEfficiencyDiagnostics() {
  return {
    uniqueUsefulSources: new Set(),
    failedRequests: 0,
    duplicateRequests: 0,
    novelEvidenceFound: 0,
    buyerGapClosures: 0,
    demandThesisGapClosures: 0,
    requests: 0,
    guessedPathFetches: 0,
    navDiscoveredFetches: 0,
    seedFetches: 0,
  };
}

export function finalizeSourceEfficiencyDiagnostics(diag) {
  const requests = diag.requests || 1;
  const useful = diag.uniqueUsefulSources.size;
  return {
    uniqueUsefulSources: useful,
    failedRequests: diag.failedRequests,
    duplicateRequests: diag.duplicateRequests,
    novelEvidenceFound: diag.novelEvidenceFound,
    buyerGapClosures: diag.buyerGapClosures,
    demandThesisGapClosures: diag.demandThesisGapClosures,
    usefulSourcesPerRequest: Number((useful / requests).toFixed(3)),
    costPerEvidenceImprovementUsd: 0,
    guessedPathFetches: diag.guessedPathFetches,
    navDiscoveredFetches: diag.navDiscoveredFetches,
    seedFetches: diag.seedFetches,
  };
}
