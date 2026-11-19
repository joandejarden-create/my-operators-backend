/**
 * Market Alerts → Hotel Explorer Sources & Evidence citations.
 * Reuses MarketAlerts rows by ID/URL — does not copy articles into HI Evidence.
 */

import {
  HOTEL_ARTICLE_MATCH,
  HE_NEWS_VISIBLE_MATCHES,
  HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
} from "./hotel-news-from-market-alerts-v1.js";

export const MARKET_ALERTS_EVIDENCE_CITATIONS_V1 = "market-alerts-evidence-citations-v1";

/**
 * Map a selected hotel-news card into a Sources & Evidence citation shape.
 * @param {object} article — from selectHotelNewsArticles / API
 * @param {{ hotelName?: string, hotelFact?: string }} [opts]
 */
export function toMarketAlertsEvidenceCitation(article = {}, opts = {}) {
  const hotelName = opts.hotelName || article.hotelName || "this hotel";
  const hotelFact =
    opts.hotelFact ||
    article.supportedFact ||
    article.relevanceSummary ||
    `Property-relevant Market Alerts coverage for ${hotelName}.`;
  const matchClass = article.matchClass || null;
  const verificationStatus =
    article.verificationStatus ||
    (Array.isArray(article.matchReasons) &&
    article.matchReasons.includes("canonical_hpc_association")
      ? "VERIFIED_HPC_ASSOCIATION"
      : matchClass === HOTEL_ARTICLE_MATCH.HOTEL_SPECIFIC ||
          matchClass === HOTEL_ARTICLE_MATCH.MULTI_HOTEL_SPECIFIC
        ? "PROPERTY_MATCH_READ"
        : "UNVERIFIED");

  return {
    title: article.headline || article.title || "Market Alerts article",
    url: article.url || null,
    provider: article.publisher || "Market Alerts",
    publisher: article.publisher || null,
    observed_date: article.publishedAt || article.published_at || null,
    excerpt: hotelFact,
    claim: hotelFact,
    proves:
      "Supports the hotel-specific Market Alerts fact summarized for this property (news / renovation / transaction context).",
    does_not:
      "Does not by itself prove completed ownership transfer, CapEx execution, franchise contracting, or economic control without independent verification.",
    supports: "market_alerts_hotel_fact",
    source_type: "Market Alerts",
    verification_status: verificationStatus,
    market_alerts_id: article.id || article.marketAlertsId || null,
    dedupe_id: article.dedupeKey || article.dedupeId || null,
    canonicalSource: "MarketAlerts",
    matchClass: matchClass || null,
  };
}

/**
 * Convert news cards → citations, filtering to visible match classes when present.
 */
export function marketAlertsArticlesToCitations(articles = [], opts = {}) {
  const out = [];
  const seen = new Set();
  for (const a of articles) {
    if (a.matchClass && !HE_NEWS_VISIBLE_MATCHES.has(a.matchClass)) continue;
    const cite = toMarketAlertsEvidenceCitation(a, opts);
    const key =
      cite.market_alerts_id ||
      cite.dedupe_id ||
      (cite.url ? String(cite.url).toLowerCase() : null) ||
      cite.title;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(cite);
  }
  return out;
}

/**
 * Merge Market Alerts citations into an existing Sources list without duplicating
 * the same MarketAlerts article (by id / dedupe / URL).
 */
export function mergeMarketAlertsIntoCitations(baseCitations = [], maCitations = []) {
  const out = Array.isArray(baseCitations) ? baseCitations.slice() : [];
  const seen = new Set();
  for (const c of out) {
    if (c && c.market_alerts_id) seen.add(`id:${c.market_alerts_id}`);
    if (c && c.dedupe_id) seen.add(`dedupe:${c.dedupe_id}`);
    if (c && c.url) seen.add(`url:${String(c.url).toLowerCase()}`);
    if (c && c.canonicalSource === "MarketAlerts" && c.title) {
      seen.add(`title:${String(c.title).toLowerCase()}`);
    }
  }
  for (const c of maCitations) {
    const keys = [];
    if (c.market_alerts_id) keys.push(`id:${c.market_alerts_id}`);
    if (c.dedupe_id) keys.push(`dedupe:${c.dedupe_id}`);
    if (c.url) keys.push(`url:${String(c.url).toLowerCase()}`);
    if (c.title) keys.push(`title:${String(c.title).toLowerCase()}`);
    if (keys.some((k) => seen.has(k))) continue;
    keys.forEach((k) => seen.add(k));
    out.push(c);
  }
  return out;
}

/**
 * Proposed (not applied) schema for durable multi-hotel association text field.
 */
export function proposedHpcHotelRecordIdsFieldSchema() {
  return {
    applyAuthorized: false,
    table: "MarketAlerts",
    fieldName: "HPC Hotel Record IDs",
    type: "multilineText",
    purpose:
      "Durable multi-hotel association complementing Tags hpc:<recId>; one article → many HPC ids; no duplicate article rows.",
    writeGate: "MARKET_ALERTS_HOTEL_LINK_APPLY=1 + Market Alerts owner + founder authorization",
    note: "Field does not exist yet (live probe 2026-11-19). Do not create without explicit authorization.",
    newsVersion: HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
  };
}
