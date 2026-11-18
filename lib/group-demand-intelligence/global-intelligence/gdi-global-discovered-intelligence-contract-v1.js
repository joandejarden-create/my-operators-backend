/**
 * Global Discovered Intelligence contract V1.
 *
 * Separates independently sourced public demand facts from hotel-specific
 * opportunity matches and confidential hotel sales activity.
 *
 * Does not spend. Does not publish. Does not write Airtable.
 */

import { createHash } from "node:crypto";
import { normalizeOrgIdentityKey } from "../workflow/reuse-prior-intelligence-v1.js";
import { editionIdentityKey } from "../research/gdi-paid-deep-two-stage-dedupe-v1.js";

export const GDI_GLOBAL_DISCOVERED_INTELLIGENCE_V1 =
  "gdi-global-discovered-intelligence-v1";

/** Lifecycle for global (hotel-independent) demand intelligence. */
export const GLOBAL_INTELLIGENCE_STATE = Object.freeze({
  RAW_DISCOVERED: "RAW_DISCOVERED",
  SCREENED: "SCREENED",
  VERIFIED_GLOBAL_SIGNAL: "VERIFIED_GLOBAL_SIGNAL",
  MARKET_RELEVANT: "MARKET_RELEVANT",
  HOTEL_MATCH_CANDIDATE: "HOTEL_MATCH_CANDIDATE",
  HOTEL_QUALIFIED: "HOTEL_QUALIFIED",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  REJECTED_UNSUPPORTED: "REJECTED_UNSUPPORTED",
  SUPERSEDED_OR_DUPLICATE: "SUPERSEDED_OR_DUPLICATE",
});

/**
 * Hotel-match / screening reason codes — not global rejection by default.
 */
export const HOTEL_MATCH_REASON = Object.freeze({
  OUT_OF_MARKET_FOR_CURRENT_HOTEL: "OUT_OF_MARKET_FOR_CURRENT_HOTEL",
  NO_CREDIBLE_DEMAND_EVIDENCE: "NO_CREDIBLE_DEMAND_EVIDENCE",
  DUPLICATE_GLOBAL_DEMAND: "DUPLICATE_GLOBAL_DEMAND",
  HOTEL_SPECIFIC_REJECTION: "HOTEL_SPECIFIC_REJECTION",
  HOTEL_FIT_PLAUSIBLE: "HOTEL_FIT_PLAUSIBLE",
  ENRICHMENT_CANDIDATE: "ENRICHMENT_CANDIDATE",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function marketLocationKey(row = {}) {
  const parts = [
    row.city || row.marketCity || row.eventCity,
    row.region || row.state || row.marketRegion,
    row.country || row.marketCountry || "US",
  ]
    .map((p) =>
      text(p)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim(),
    )
    .filter(Boolean);
  return parts.join("|") || "unknown_market";
}

export function buildGlobalDemandIdentityKey(row = {}) {
  const org = normalizeOrgIdentityKey(
    row.organizationName || row.organizingEntity || row.venueName || "",
  );
  const edition = editionIdentityKey(row);
  const market = marketLocationKey(row);
  const demandType = text(row.demandType || row.opportunityType || row.segmentId)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .slice(0, 48);
  return `${org}|${edition}|${market}|${demandType || "demand"}`;
}

export function stableGlobalDemandId(row = {}) {
  const key = buildGlobalDemandIdentityKey(row);
  const hash = createHash("sha256").update(key).digest("hex").slice(0, 16);
  return `gdi_gbl_${hash}`;
}

function extractPublicEvidence(row = {}) {
  const urls = [
    ...(row.seedUrls || []),
    ...(row.evidence || []).map((e) => e?.url || e?.sourceUrl),
    ...(row.acceptedClaims || []).map((e) => e?.url),
    row.sourceUrl,
    row.url,
  ]
    .map((u) => text(u))
    .filter((u) => /^https?:\/\//i.test(u));
  return [...new Set(urls)].slice(0, 20).map((url) => ({
    url,
    retrievedAt: row.evidenceRetrievedAt || row.discoveredAt || null,
  }));
}

function hasCredibleEvidence(row = {}) {
  const evidence = extractPublicEvidence(row);
  if (evidence.length) return true;
  return text(row.hotelOpportunityThesis || row.whatIsHappening || row.title).length > 60;
}

function looksOutOfMarketForHotel(row = {}, hotelProfile = {}) {
  const fit = String(row.geoFit?.geoFitClass || row.hotelFit || "");
  if (/MISMATCH|ELSEWHERE_ONLY|OUT_OF_MARKET|NO_FIT|WEAK_OR_OUT_OF_MARKET/i.test(fit)) {
    return true;
  }
  const blob = [
    row.hotelOpportunityThesis,
    row.whyThisHotel,
    row.discoveryRejectReason,
    row.internalDisposition,
  ]
    .map(text)
    .join(" ");
  if (/out of market|wrong geography|no .* fit|cannot compete/i.test(blob)) {
    return true;
  }
  const eventCity = text(row.city || row.eventCity || row.marketCity).toLowerCase();
  const hotelCity = text(hotelProfile.city || hotelProfile.marketCity).toLowerCase();
  if (
    eventCity &&
    hotelCity &&
    eventCity !== hotelCity &&
    !/bethesda|rockville|montgomery|dmv|washington|arlington|alexandria/i.test(
      `${eventCity} ${text(row.region)}`,
    ) &&
    /chicago|dallas|atlanta|boston|seattle|denver|miami|los angeles|san francisco|london|paris|tokyo/i.test(
      eventCity,
    )
  ) {
    return true;
  }
  return false;
}

/**
 * Classify a discovery row into global state + hotel-match reason without
 * treating geographic irrelevance as global rejection.
 */
export function classifyDiscoveryForGlobalRetention(row = {}, context = {}) {
  const evidenceOk = hasCredibleEvidence(row);
  const unsupported =
    row.unsupportedNameOnly === true ||
    row.discoveryRejectReason === "NO_CREDIBLE_DEMAND_EVIDENCE" ||
    (!evidenceOk && text(row.organizationName).length < 2);

  if (unsupported) {
    return {
      globalState: GLOBAL_INTELLIGENCE_STATE.REJECTED_UNSUPPORTED,
      hotelMatchReason: HOTEL_MATCH_REASON.NO_CREDIBLE_DEMAND_EVIDENCE,
      retainAsVerifiedGlobal: false,
      retainForAudit: true,
      outOfMarketForCurrentHotel: false,
    };
  }

  const outOfMarket = looksOutOfMarketForHotel(row, context.hotelProfile || {});
  if (outOfMarket) {
    return {
      globalState: evidenceOk
        ? GLOBAL_INTELLIGENCE_STATE.VERIFIED_GLOBAL_SIGNAL
        : GLOBAL_INTELLIGENCE_STATE.SCREENED,
      hotelMatchReason: HOTEL_MATCH_REASON.OUT_OF_MARKET_FOR_CURRENT_HOTEL,
      retainAsVerifiedGlobal: evidenceOk,
      retainForAudit: true,
      outOfMarketForCurrentHotel: true,
    };
  }

  return {
    globalState: evidenceOk
      ? GLOBAL_INTELLIGENCE_STATE.HOTEL_MATCH_CANDIDATE
      : GLOBAL_INTELLIGENCE_STATE.RAW_DISCOVERED,
    hotelMatchReason: HOTEL_MATCH_REASON.HOTEL_FIT_PLAUSIBLE,
    retainAsVerifiedGlobal: evidenceOk,
    retainForAudit: true,
    outOfMarketForCurrentHotel: false,
  };
}

/**
 * Build a public global intelligence record (no hotel-private fields).
 */
export function buildGlobalDiscoveredIntelligenceRecord(row = {}, context = {}) {
  const classification = classifyDiscoveryForGlobalRetention(row, context);
  const globalDemandId = stableGlobalDemandId(row);
  const now = context.nowIso || new Date().toISOString();
  return {
    contract: GDI_GLOBAL_DISCOVERED_INTELLIGENCE_V1,
    globalDemandId,
    identityKey: buildGlobalDemandIdentityKey(row),
    state: classification.globalState,
    organizationName: text(row.organizationName || row.organizingEntity) || null,
    organizationKey: normalizeOrgIdentityKey(
      row.organizationName || row.organizingEntity || "",
    ),
    eventOrProjectTitle: text(row.title || row.programmeName || row.activityTitle) || null,
    demandType: text(row.demandType || row.opportunityType) || null,
    segmentId: text(row.researchSegment || row.segmentId) || null,
    marketLocation: {
      city: text(row.city || row.eventCity || row.marketCity) || null,
      region: text(row.region || row.state || row.marketRegion) || null,
      country: text(row.country || row.marketCountry) || null,
      key: marketLocationKey(row),
    },
    dates: {
      start: row.eventStartDate || row.programmeStartDate || null,
      end: row.eventEndDate || row.programmeEndDate || null,
      needDate: row.needDate || null,
    },
    evidence: extractPublicEvidence(row),
    discoveryRunId: context.discoveryRunId || context.runId || null,
    researchRunId: context.researchRunId || null,
    providerId: row.providerId || context.providerId || null,
    providerRunId: row.providerRunId || row.exaAgentRunId || null,
    sourceHotelIdDiscovering: context.hotelId || row.hotelId || null,
    classification,
    firstDiscoveredAt: now,
    lastUpdatedAt: now,
    // Explicitly absent private surfaces
    hotelSalesActivity: null,
    privateHotelFeedback: null,
  };
}

/**
 * Hotel-specific match row — references global demand; no sales CRM payload.
 */
export function buildHotelOpportunityMatchRecord(row = {}, context = {}, globalRecord = null) {
  const classification = classifyDiscoveryForGlobalRetention(row, context);
  const globalDemandId =
    globalRecord?.globalDemandId || stableGlobalDemandId(row);
  const now = context.nowIso || new Date().toISOString();
  return {
    contract: GDI_GLOBAL_DISCOVERED_INTELLIGENCE_V1,
    kind: "HOTEL_OPPORTUNITY_MATCH",
    hotelId: context.hotelId || row.hotelId || null,
    globalDemandId,
    candidateId: row.id || row.opportunityId || row.candidateId || null,
    segmentId: text(row.researchSegment || row.segmentId) || null,
    geographicSuitability: classification.outOfMarketForCurrentHotel
      ? "OUT_OF_MARKET"
      : "PLAUSIBLE_OR_UNKNOWN",
    matchReason: classification.hotelMatchReason,
    hotelSpecificQualification: null,
    researchStatus: classification.globalState,
    publicationStatus: "NOT_PUBLISHED",
    recommendedAction: classification.outOfMarketForCurrentHotel
      ? "RETAIN_GLOBAL_ONLY"
      : "CONTINUE_HOTEL_RESEARCH",
    createdAt: now,
    updatedAt: now,
  };
}

export default {
  GDI_GLOBAL_DISCOVERED_INTELLIGENCE_V1,
  GLOBAL_INTELLIGENCE_STATE,
  HOTEL_MATCH_REASON,
  stableGlobalDemandId,
  buildGlobalDiscoveredIntelligenceRecord,
  buildHotelOpportunityMatchRecord,
  classifyDiscoveryForGlobalRetention,
};
