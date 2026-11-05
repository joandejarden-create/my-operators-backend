/**
 * GDI 2.0 Phase 2G — source discovery before retrieval.
 *
 * Soft-404 / irrelevant detection, source-type classification, freshness hints,
 * country/language-aware path preference. Builds on source-selection-efficiency-v1.
 * No unauthorized external search providers.
 */

import {
  normalizeSourceUrl,
  discoverOfficialNavLinks,
  scoreSourceCandidate,
  selectResearchUrls,
} from "./source-selection-efficiency-v1.js";
import { getBuyerDiscoveryStrategy } from "./buyer-discovery-strategies-v1.js";

export const SOURCE_DISCOVERY_V1 = "gdi-source-discovery-v1";

export const SOURCE_TYPE = Object.freeze({
  OFFICIAL_ABOUT_TEAM: "OFFICIAL_ABOUT_TEAM",
  CONTACT: "CONTACT",
  HOUSING_ACCOMMODATION: "HOUSING_ACCOMMODATION",
  EVENTS_MEETINGS: "EVENTS_MEETINGS",
  PRESS_ANNOUNCEMENT: "PRESS_ANNOUNCEMENT",
  CAREERS: "CAREERS",
  DIRECTORY_SPAM: "DIRECTORY_SPAM",
  UNKNOWN: "UNKNOWN",
});

const SOFT_404_RE =
  /\b(página no encontrada|page not found|404|no encontrada|doesn'?t exist|content not available|errore 404)\b/i;
const IRRELEVANT_RE =
  /\b(privacy policy|cookie policy|terms of (use|service)|login|sign in|cart|checkout)\b/i;
const DIRECTORY_SPAM_RE =
  /\b(expo guide|fair directory|international fairs directory|construct data|eventseye)\b/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function classifySourceType(url = "", pageText = "") {
  const u = String(url || "").toLowerCase();
  const blob = `${u} ${String(pageText || "").slice(0, 500)}`;
  if (DIRECTORY_SPAM_RE.test(blob)) return SOURCE_TYPE.DIRECTORY_SPAM;
  if (/\/(careers|jobs)\b/.test(u)) return SOURCE_TYPE.CAREERS;
  if (/\/(housing|hotel|hotels|accommodation|alojamiento|hospedaje|viajes)\b/.test(u)) {
    return SOURCE_TYPE.HOUSING_ACCOMMODATION;
  }
  if (/\/(event|events|meeting|meetings|congress|congreso|conference)\b/.test(u)) {
    return SOURCE_TYPE.EVENTS_MEETINGS;
  }
  if (/\/(contact|contacto|contact-us)\b/.test(u)) return SOURCE_TYPE.CONTACT;
  if (/\/(about|about-us|team|leadership|our-team|company|equipo)\b/.test(u)) {
    return SOURCE_TYPE.OFFICIAL_ABOUT_TEAM;
  }
  if (/\/(news|newsroom|press|blog|announcement)\b/.test(u)) return SOURCE_TYPE.PRESS_ANNOUNCEMENT;
  return SOURCE_TYPE.UNKNOWN;
}

/**
 * Detect soft-404 / empty / irrelevant pages even when HTTP 200.
 */
export function assessRetrievedPageQuality(page = {}, url = "") {
  const body = text(page.text || page.body || "");
  const finalUrl = page.finalUrl || url;
  const okHttp = page.ok === true;
  const soft404 = SOFT_404_RE.test(body) || /\/404\b/i.test(finalUrl);
  const tooShort = body.length < 80;
  const irrelevant = IRRELEVANT_RE.test(body.slice(0, 400)) && body.length < 1200;
  const directorySpam = DIRECTORY_SPAM_RE.test(body.slice(0, 800));
  const sourceType = classifySourceType(finalUrl, body);
  const commercialSignal =
    /\b(hotel|housing|accommodation|meeting|meetings|event|events|travel|procurement|facilities|workplace|congress|hospedaje|alojamiento)\b/i.test(
      body,
    );

  return {
    version: SOURCE_DISCOVERY_V1,
    okHttp,
    soft404,
    tooShort,
    irrelevant,
    directorySpam,
    commercialSignal,
    sourceType,
    usable: okHttp && !soft404 && !tooShort && !directorySpam && !irrelevant,
    changeStrategy: soft404 || directorySpam || (irrelevant && !commercialSignal),
    normalizedUrl: normalizeSourceUrl(finalUrl),
  };
}

/**
 * Country / language-aware path hints.
 */
export function languageAwarePathHints(opportunity = {}, strategyId = "") {
  const blob = `${opportunity.organizationName || ""} ${opportunity.eventName || ""} ${opportunity.market || ""} ${opportunity.country || ""}`;
  const es = /[áéíóúñü]|spain|españa|méxico|mexico|argentina|colombia|chile|coruña|madrid|barcelona/i.test(blob);
  const paths = [];
  if (/LEADERSHIP|OFFICIAL|TEAM|WHO|FACILITIES|BUYER|TRAVEL|MEETINGS|ASSOCIATION/i.test(strategyId)) {
    paths.push("/about", "/about-us", "/team", "/leadership", "/our-team");
    if (es) paths.push("/equipo", "/nosotros", "/quienes-somos", "/la-empresa");
  }
  if (/CONTACT|ROUTE|HOUSING|EVENT|MEETINGS|DEMAND|PCO|ASSOCIATION/i.test(strategyId)) {
    paths.push("/contact", "/contact-us", "/housing", "/hotels", "/events", "/meetings");
    if (es) {
      paths.push("/contacto", "/alojamiento", "/hospedaje", "/congreso", "/eventos", "/viajes");
    }
  }
  return [...new Set(paths)];
}

/**
 * Build ranked fetch list using discovery strategy preferred source types + seeds/nav.
 */
export function buildDiscoveredFetchPlan({
  opportunity = {},
  discoveryStrategyId = "",
  strategyId = "",
  attemptedNormalized = new Set(),
  signalMemory = new Map(),
  discoveredNav = [],
  crossOpportunityCache = [],
  maxUrls = 2,
} = {}) {
  const discovery = discoveryStrategyId ? getBuyerDiscoveryStrategy(discoveryStrategyId) : null;
  const seeds = [];
  const push = (u) => {
    const s = text(u);
    if (/^https?:\/\//i.test(s)) seeds.push(s);
  };
  for (const s of opportunity.sources || []) push(s?.url || s);
  for (const e of opportunity.evidence || []) push(e?.sourceUrl || e?.url);
  push(opportunity.officialSource);
  push(opportunity.sourceUrl);
  push(opportunity.primarySourceUrl);
  push(opportunity.contactUrl);
  push(opportunity.organizationContactUrl);

  // Prefer paths matching discovery preferred source types
  const preferCareers = discovery?.preferredSourceTypes?.some((t) => /careers/i.test(t));
  const selection = selectResearchUrls({
    seedUrls: [...new Set(seeds)],
    discoveredNav,
    strategyId: strategyId || discoveryStrategyId || "OFFICIAL_LEADERSHIP_PAGES",
    attemptedNormalized,
    signalMemory,
    maxUrls: Math.max(maxUrls, 4),
    crossOpportunityCache,
  });

  // Re-rank: boost housing/events/contact; demote careers unless strategy allows
  const ranked = selection.selected
    .map((c) => {
      const type = classifySourceType(c.url);
      let score = c.score;
      if (type === SOURCE_TYPE.HOUSING_ACCOMMODATION || type === SOURCE_TYPE.EVENTS_MEETINGS) score += 12;
      if (type === SOURCE_TYPE.CONTACT || type === SOURCE_TYPE.OFFICIAL_ABOUT_TEAM) score += 8;
      if (type === SOURCE_TYPE.CAREERS && !preferCareers) score -= 15;
      if (type === SOURCE_TYPE.DIRECTORY_SPAM) score -= 50;
      return { ...c, sourceType: type, score };
    })
    .sort((a, b) => b.score - a.score)
    .filter((c) => c.score >= 0)
    .slice(0, maxUrls);

  // Language-aware guesses only if still short
  if (ranked.length < maxUrls) {
    const origins = [
      ...new Set(
        seeds
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
    for (const origin of origins) {
      for (const path of languageAwarePathHints(opportunity, strategyId || discoveryStrategyId)) {
        const scored = scoreSourceCandidate(`${origin}${path}`, { guessedPath: true });
        if (scored.score < 0) continue;
        if (attemptedNormalized.has(scored.normalized)) continue;
        if (ranked.some((r) => r.normalized === scored.normalized)) continue;
        ranked.push({
          ...scored,
          sourceType: classifySourceType(scored.url),
        });
        if (ranked.length >= maxUrls) break;
      }
      if (ranked.length >= maxUrls) break;
    }
  }

  return {
    version: SOURCE_DISCOVERY_V1,
    discoveryStrategyId: discoveryStrategyId || null,
    selected: ranked.slice(0, maxUrls),
    preferredSourceTypes: discovery?.preferredSourceTypes || [],
    note: "No external search providers — seeds, nav graph, cache, and language-aware paths only.",
  };
}

export function extractAndCacheNav(pageText, baseUrl, cacheMap) {
  const links = discoverOfficialNavLinks(pageText, baseUrl);
  try {
    const origin = new URL(baseUrl).origin;
    const prev = cacheMap.get(origin) || [];
    cacheMap.set(origin, [...new Set([...prev, ...links])]);
  } catch {
    /* ignore */
  }
  return links;
}

/**
 * Freshness / contradiction hints (page-level, non-authoritative).
 */
export function assessEvidenceFreshnessAndContradiction(pageText = "", opportunity = {}) {
  const blob = String(pageText || "");
  const yearMatches = [...blob.matchAll(/\b(20[2-3]\d)\b/g)].map((m) => Number(m[1]));
  const latestYear = yearMatches.length ? Math.max(...yearMatches) : null;
  const stale =
    latestYear != null && latestYear < new Date().getUTCFullYear() - 3;
  const contradiction = /\b(cancelled|canceled|postponed|virtual only|no lodging)\b/i.test(blob);
  return {
    latestYearMentioned: latestYear,
    possiblyStale: stale,
    contradictionLanguage: contradiction,
    opportunityId: opportunity.id || null,
  };
}
