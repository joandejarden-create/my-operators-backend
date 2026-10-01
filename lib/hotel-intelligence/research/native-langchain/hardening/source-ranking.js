/**
 * Packet 2.8C-2 — Jurisdiction + domain source ranking before fetch expansion.
 */

export const SOURCE_RANKING_VERSION = "source-ranking-engine-v1";

const AUTHORITY_BONUS = Object.freeze({
  issuer_filing: 40,
  bmv: 38,
  annual_report: 36,
  securities_filing: 35,
  government: 34,
  brand_first_party: 32,
  owner_first_party: 31,
  operator_first_party: 30,
  official_acquisition: 30,
  hotel_first_party: 28,
  tourism_authority: 27,
  trade_press: 18,
  local_press: 14,
  ota: 4,
  aggregator: 2,
  social: 1,
  unknown: 8,
});

const DOMAIN_PREFERENCE = Object.freeze({
  OWNERSHIP: ["issuer_filing", "bmv", "annual_report", "securities_filing", "owner_first_party", "government"],
  PROPCO: ["issuer_filing", "annual_report", "government", "owner_first_party"],
  BRAND_HISTORY: ["brand_first_party", "hotel_first_party", "owner_first_party", "operator_first_party", "trade_press"],
  TRANSACTIONS: ["official_acquisition", "issuer_filing", "securities_filing", "trade_press", "local_press"],
  DEVELOPMENT: ["brand_first_party", "owner_first_party", "hotel_first_party", "trade_press"],
  OPERATOR: ["operator_first_party", "brand_first_party", "owner_first_party", "trade_press"],
  PEOPLE: ["owner_first_party", "operator_first_party", "securities_filing", "trade_press"],
  MARKET: ["hotel_first_party", "brand_first_party", "tourism_authority", "trade_press"],
  PORTFOLIO: ["issuer_filing", "annual_report", "owner_first_party"],
});

export function classifySourceFamily(url = "", title = "", snippet = "") {
  const u = String(url || "").toLowerCase();
  const t = `${title} ${snippet}`.toLowerCase();
  if (/bmv\.com\.mx|cnbv|reporte.?anual|hechos.?relevantes|\.pdf/i.test(u) && /reporte|anual|bmv|cnbv|filing/i.test(t + u)) {
    if (/bmv\.com\.mx/.test(u)) return "bmv";
    if (/reporte.?anual|annual.?report/.test(t + u)) return "annual_report";
    return "issuer_filing";
  }
  if (/\.pdf($|\?)/i.test(u) && /(annual|reporte|10-k|filing|investor)/i.test(t + u)) return "annual_report";
  if (/sec\.gov|edgar|investor/.test(u)) return "securities_filing";
  if (/gob\.mx|gov\.bm|dof\.gob|tourism/.test(u)) return "government";
  if (/marriott\.com|ihg\.com|hilton\.com|hyatt\.com|voco|sheraton/.test(u)) return "brand_first_party";
  if (/aimbridge|gsf|santafe|dovetail|capitali|alliance/.test(u)) return "operator_first_party";
  if (/booking\.com|expedia|hotels\.com|tripadvisor|orbitz/.test(u)) return "ota";
  if (/linkedin\.com\/(company|in)/.test(u)) return "social";
  if (/tophotel|hotelinvestment|reportur|hospitalitynet|travelweekly|riotimes|bernews|royalgazette/.test(u)) {
    return "trade_press";
  }
  if (/cambridgebeaches\.com|krystal|hotsson|hnf/.test(u)) return "hotel_first_party";
  return "unknown";
}

export function scoreSourceCandidate(candidate = {}, { domain = "OWNERSHIP", hotel_name = "", aliases = [] } = {}) {
  const family = classifySourceFamily(candidate.url, candidate.title, candidate.snippet);
  let score = AUTHORITY_BONUS[family] || AUTHORITY_BONUS.unknown;
  const preferred = DOMAIN_PREFERENCE[String(domain).toUpperCase()] || DOMAIN_PREFERENCE.OWNERSHIP;
  const prefIdx = preferred.indexOf(family);
  if (prefIdx >= 0) score += 20 - prefIdx * 2;

  const blob = `${candidate.title || ""} ${candidate.snippet || ""} ${candidate.url || ""}`.toLowerCase();
  const names = [hotel_name, ...aliases].filter(Boolean).map((x) => String(x).toLowerCase());
  if (names.some((n) => n && blob.includes(n))) score += 15;
  if (/\.pdf($|\?)/i.test(candidate.url || "")) score += 8;
  if (/ota|booking|expedia|tripadvisor/.test(family)) score -= 10;
  if (/linkedin\.com\/company/.test(String(candidate.url || "").toLowerCase()) && /PEOPLE/i.test(domain)) {
    score -= 12; // company page ≠ person profile
  }

  // Recency hint
  if (/202[4-6]/.test(blob)) score += 4;
  if (/201[0-8]/.test(blob) && !/202[0-6]/.test(blob)) score -= 3;

  return {
    ...candidate,
    source_family: family,
    rank_score: score,
    document_value: classifyDocumentValue(family, candidate.url, candidate.title),
  };
}

export function classifyDocumentValue(family, url = "", title = "") {
  const t = `${title} ${url}`.toLowerCase();
  if (
    ["issuer_filing", "bmv", "annual_report", "securities_filing", "government", "official_acquisition"].includes(
      family
    ) ||
    /reporte anual|annual report|hecho relevante|acquisition|portfolio/.test(t)
  ) {
    return "HIGH_VALUE_DOCUMENT";
  }
  if (["brand_first_party", "owner_first_party", "operator_first_party", "hotel_first_party", "trade_press"].includes(family)) {
    return "SUPPORTING_DOCUMENT";
  }
  if (["ota", "aggregator", "social"].includes(family)) return "LOW_VALUE";
  return "DISCOVERY_ONLY";
}

export function rankSourceCandidates(candidates = [], opts = {}) {
  return [...candidates]
    .map((c) => scoreSourceCandidate(c, opts))
    .sort((a, b) => b.rank_score - a.rank_score);
}

export function buildSourcePlanBeforeSearch({ profile, domain, hotel_name, company, aliases = [] }) {
  const d = String(domain || "OWNERSHIP").toUpperCase();
  const preferred = DOMAIN_PREFERENCE[d] || DOMAIN_PREFERENCE.OWNERSHIP;
  return {
    version: SOURCE_RANKING_VERSION,
    jurisdiction: profile?.country_code || "XX",
    domain: d,
    hotel_name,
    company: company || null,
    aliases,
    decisive_source_families: preferred.slice(0, 4),
    query_language: profile?.primary_query_language || "en",
    legal_entity_forms: profile?.legal_entity_forms || [],
    corporate_paths: profile?.corporate_sources || [],
    government_paths: profile?.government_sources || [],
    fallbacks: profile?.media_sources || [],
    disclosure_limitations: profile?.disclosure_limitations || [],
    do_not_start_with: ["broad_ota_search", "generic_web_only"],
  };
}
