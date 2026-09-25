/**
 * Canonical provider-agnostic research normalization for GDI.
 * REUSABLE_RESEARCH_LOGIC — adapters parse provider shape; product logic is shared.
 *
 * Providers: NATIVE | PARALLEL | WEBHOUND
 * Downstream GDI never branches on provider-specific business rules.
 */

import { CLAIM_KIND, OPPORTUNITY_TYPE, VENUE_SOURCING_STATUS } from "./claim-types.js";
import { detectFutureCycleEvidence } from "./future-cycle-evidence.js";

export const RESEARCH_PROVIDER = Object.freeze({
  NATIVE: "NATIVE",
  PARALLEL: "PARALLEL",
  WEBHOUND: "WEBHOUND",
  UNKNOWN: "UNKNOWN",
});

export const CANONICAL_VENUE_SOURCING = Object.freeze({
  OPEN_UNRESOLVED: VENUE_SOURCING_STATUS.OPEN_UNRESOLVED,
  RFP_ACTIVE_SOURCING: VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING,
  HOTEL_VENUE_TBD: VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD,
  DESTINATION_TBD: VENUE_SOURCING_STATUS.DESTINATION_TBD,
  HOUSING_PENDING: VENUE_SOURCING_STATUS.HOUSING_PENDING,
  REGISTRATION_OPEN_HOTEL_UNANNOUNCED:
    VENUE_SOURCING_STATUS.REGISTRATION_OPEN_HOTEL_UNANNOUNCED,
  PARTIALLY_PLACED: VENUE_SOURCING_STATUS.PARTIALLY_PLACED,
  PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE:
    VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
  PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE:
    VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
  FULLY_PLACED: VENUE_SOURCING_STATUS.FULLY_PLACED,
  CURRENT_CYCLE_CLOSED: VENUE_SOURCING_STATUS.CURRENT_CYCLE_CLOSED,
  UNKNOWN: VENUE_SOURCING_STATUS.UNKNOWN,
});

const VENUE_ALIASES = Object.freeze({
  OPEN: CANONICAL_VENUE_SOURCING.OPEN_UNRESOLVED,
  UNRESOLVED: CANONICAL_VENUE_SOURCING.OPEN_UNRESOLVED,
  "OPEN / UNRESOLVED": CANONICAL_VENUE_SOURCING.OPEN_UNRESOLVED,
  OPEN_UNRESOLVED: CANONICAL_VENUE_SOURCING.OPEN_UNRESOLVED,
  RFP: CANONICAL_VENUE_SOURCING.RFP_ACTIVE_SOURCING,
  "ACTIVE SOURCING": CANONICAL_VENUE_SOURCING.RFP_ACTIVE_SOURCING,
  RFP_ACTIVE: CANONICAL_VENUE_SOURCING.RFP_ACTIVE_SOURCING,
  RFP_ACTIVE_SOURCING: CANONICAL_VENUE_SOURCING.RFP_ACTIVE_SOURCING,
  TBD: CANONICAL_VENUE_SOURCING.HOTEL_VENUE_TBD,
  "VENUE TBD": CANONICAL_VENUE_SOURCING.HOTEL_VENUE_TBD,
  "HOTEL TBD": CANONICAL_VENUE_SOURCING.HOTEL_VENUE_TBD,
  VENUE_TBD: CANONICAL_VENUE_SOURCING.HOTEL_VENUE_TBD,
  HOTEL_TBD: CANONICAL_VENUE_SOURCING.HOTEL_VENUE_TBD,
  HOTEL_VENUE_TBD: CANONICAL_VENUE_SOURCING.HOTEL_VENUE_TBD,
  "DESTINATION TBD": CANONICAL_VENUE_SOURCING.DESTINATION_TBD,
  DESTINATION_TBD: CANONICAL_VENUE_SOURCING.DESTINATION_TBD,
  "LOCATION TBD": CANONICAL_VENUE_SOURCING.DESTINATION_TBD,
  LOCATION_TBD: CANONICAL_VENUE_SOURCING.DESTINATION_TBD,
  "HOUSING PENDING": CANONICAL_VENUE_SOURCING.HOUSING_PENDING,
  HOUSING_PENDING: CANONICAL_VENUE_SOURCING.HOUSING_PENDING,
  "ACCOMMODATIONS PENDING": CANONICAL_VENUE_SOURCING.HOUSING_PENDING,
  "HOTEL COMING SOON": CANONICAL_VENUE_SOURCING.REGISTRATION_OPEN_HOTEL_UNANNOUNCED,
  "REGISTRATION OPEN": CANONICAL_VENUE_SOURCING.REGISTRATION_OPEN_HOTEL_UNANNOUNCED,
  REGISTRATION_OPEN_HOTEL_UNANNOUNCED:
    CANONICAL_VENUE_SOURCING.REGISTRATION_OPEN_HOTEL_UNANNOUNCED,
  "PARTIALLY PLACED": CANONICAL_VENUE_SOURCING.PARTIALLY_PLACED,
  PARTIALLY_PLACED: CANONICAL_VENUE_SOURCING.PARTIALLY_PLACED,
  OVERFLOW: CANONICAL_VENUE_SOURCING.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
  "OVERFLOW POSSIBLE": CANONICAL_VENUE_SOURCING.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
  "NO OVERFLOW": CANONICAL_VENUE_SOURCING.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
  "PRIMARY SELECTED": CANONICAL_VENUE_SOURCING.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
  "FULLY PLACED": CANONICAL_VENUE_SOURCING.FULLY_PLACED,
  FULLY_PLACED: CANONICAL_VENUE_SOURCING.FULLY_PLACED,
  CLOSED: CANONICAL_VENUE_SOURCING.CURRENT_CYCLE_CLOSED,
  "CURRENT CYCLE CLOSED": CANONICAL_VENUE_SOURCING.CURRENT_CYCLE_CLOSED,
});

function asClaim(value, kind = CLAIM_KIND.UNKNOWN, meta = {}) {
  return {
    value: value == null || value === "" ? null : value,
    kind,
    source: meta.source || null,
    sourceAuthority: meta.sourceAuthority || null,
    sourceDate: meta.sourceDate || null,
    evidence: meta.evidence || null,
    corroborationCount: meta.corroborationCount ?? 0,
    contradiction: meta.contradiction || null,
  };
}

/**
 * Normalize free-text / alias venue-sourcing labels into canonical GDI codes.
 * Used by Native discovery mapping and provider import — not a scoring-threshold change.
 */
export function normalizeVenueStatus(raw) {
  if (!raw) return CANONICAL_VENUE_SOURCING.UNKNOWN;
  const s = String(raw).trim();
  if (Object.values(CANONICAL_VENUE_SOURCING).includes(s)) return s;
  const upper = s.toUpperCase().replace(/[\s/-]+/g, "_");
  if (Object.values(CANONICAL_VENUE_SOURCING).includes(upper)) return upper;
  if (VENUE_ALIASES[upper]) return VENUE_ALIASES[upper];
  // Loose phrase match on original (spaces preserved for multi-word aliases)
  const upperSpaced = s.toUpperCase().trim();
  if (VENUE_ALIASES[upperSpaced]) return VENUE_ALIASES[upperSpaced];
  for (const [k, v] of Object.entries(VENUE_ALIASES)) {
    if (upperSpaced.includes(k) || upper.includes(k.replace(/\s+/g, "_"))) return v;
  }
  if (/tbd|unresolved|not yet (selected|announced)|hotel.?tbd|venue.?tbd/i.test(s)) {
    return CANONICAL_VENUE_SOURCING.HOTEL_VENUE_TBD;
  }
  if (/rfp|request for proposal|accepting (bids|proposals)|sourcing/i.test(s)) {
    return CANONICAL_VENUE_SOURCING.RFP_ACTIVE_SOURCING;
  }
  if (/overflow|housing (open|available)|official hotel list/i.test(s)) {
    return CANONICAL_VENUE_SOURCING.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE;
  }
  if (/fully.?placed|already.?selected|sold out (block|housing)/i.test(s)) {
    return CANONICAL_VENUE_SOURCING.FULLY_PLACED;
  }
  // Ambiguous "confirmed" alone = destination/date, not venue placed
  if (/^confirmed$/i.test(s)) return CANONICAL_VENUE_SOURCING.UNKNOWN;
  if (/open/i.test(s)) return CANONICAL_VENUE_SOURCING.OPEN_UNRESOLVED;
  return CANONICAL_VENUE_SOURCING.UNKNOWN;
}

function normalizeProviderId(provider) {
  const p = String(provider || "").trim().toUpperCase();
  if (p === "DEALALITY_NATIVE" || p === "NATIVE") return RESEARCH_PROVIDER.NATIVE;
  if (p === "PARALLEL") return RESEARCH_PROVIDER.PARALLEL;
  if (p === "WEBHOUND" || p === "WH") return RESEARCH_PROVIDER.WEBHOUND;
  return RESEARCH_PROVIDER.UNKNOWN;
}

/** Provider-specific light parsers — shape only, no product decisions. */
export function parseNativeResearchResult(raw = {}) {
  return {
    eventName: raw.eventName || raw.title || raw.name || null,
    organization: raw.organization || raw.organizationName || raw.org || null,
    dates: raw.dates || raw.eventDates || { start: raw.eventStartDate || null, end: raw.eventEndDate || null },
    geography: raw.geography || raw.destinationStatus || raw.location || null,
    venueStatusRaw: raw.venueStatus || raw.venueSourcingStatus || raw.sourcingStatus || null,
    sourcingStatusRaw: raw.sourcingStatus || raw.venueSourcingStatus || null,
    opportunityTypeCandidate: raw.opportunityType || raw.typeCandidate || null,
    attendance: raw.attendance || raw.estimatedAttendance || null,
    roomDemand: raw.roomDemand || raw.estimatedPeakRooms || raw.peakRooms || null,
    housingEvidence: raw.housingEvidence || raw.housing || null,
    contactEvidence: raw.contacts || raw.contactEvidence || raw.primaryContact || null,
    sourceEvidence: raw.sources || raw.sourceEvidence || raw.evidence || [],
    whyNow: raw.whyNow || null,
    winThesis: raw.winThesis || raw.hotelOpportunityThesis || null,
    contradictions: raw.contradictions || [],
    unresolvedQuestions: raw.unresolvedQuestions || [],
    confidence: raw.confidence ?? raw.evidenceConfidence ?? null,
    researchPlan: raw.researchPlan || raw.queries || null,
    raw,
  };
}

export function parseParallelResearchResult(raw = {}) {
  const output = raw.output || raw.result || raw;
  return parseNativeResearchResult({
    ...output,
    sources: output.citations || output.sources || raw.citations || [],
    researchPlan: raw.task_spec || raw.researchPlan || null,
    confidence: output.confidence ?? raw.confidence,
  });
}

export function parseWebhoundResearchResult(raw = {}) {
  return parseNativeResearchResult({
    eventName: raw.event || raw.eventName || raw.title,
    organization: raw.organization || raw.org || raw.organizationName,
    dates: raw.dates || { start: raw.startDate || raw.eventStartDate, end: raw.endDate },
    geography: raw.location || raw.geography || raw.destinationStatus,
    venueStatusRaw: raw.venue || raw.venueStatus || raw.venueSourcingStatus,
    sourcingStatusRaw: raw.sourcing || raw.sourcingStatus,
    opportunityTypeCandidate: raw.opportunityType || raw.category,
    attendance: raw.attendance,
    roomDemand: raw.rooms || raw.peakRooms || raw.estimatedPeakRooms,
    housingEvidence: raw.housing || raw.housingEvidence,
    contactEvidence: raw.who || raw.contact || raw.primaryContact,
    sourceEvidence: raw.sources || raw.citations || raw.evidence || [],
    whyNow: raw.whyNow || raw.why_now,
    winThesis: raw.winThesis || raw.hotelFit || raw.hotelOpportunityThesis,
    contradictions: raw.contradictions || [],
    unresolvedQuestions: raw.openQuestions || raw.unresolvedQuestions || [],
    confidence: raw.confidence ?? raw.evidenceConfidence,
    researchPlan: raw.plan || raw.researchPlan,
    ...raw,
  });
}

function pickParser(provider) {
  if (provider === RESEARCH_PROVIDER.PARALLEL) return parseParallelResearchResult;
  if (provider === RESEARCH_PROVIDER.WEBHOUND) return parseWebhoundResearchResult;
  return parseNativeResearchResult;
}

/**
 * Normalize any provider result into the canonical GDI research evidence schema.
 */
export function normalizeResearchProviderResult({
  provider,
  rawOutput = {},
  taskContract = null,
  hotelContext = null,
  opportunityContext = null,
} = {}) {
  const providerId = normalizeProviderId(provider);
  const parsed = pickParser(providerId)(rawOutput || {});
  const venueStatus = normalizeVenueStatus(parsed.venueStatusRaw || parsed.sourcingStatusRaw);
  const futureCycle = detectFutureCycleEvidence({
    title: parsed.eventName,
    organizationName: parsed.organization,
    whyNow: parsed.whyNow,
    hotelOpportunityThesis: parsed.winThesis,
    opportunityType: parsed.opportunityTypeCandidate,
    venueSourcingStatus: venueStatus,
    destinationStatus: parsed.geography,
  });

  let opportunityTypeCandidate = parsed.opportunityTypeCandidate || null;
  if (
    futureCycle.preserveAsWatch &&
    (!opportunityTypeCandidate ||
      /closed|placed|disqual/i.test(String(opportunityTypeCandidate)))
  ) {
    opportunityTypeCandidate = OPPORTUNITY_TYPE.FUTURE_CYCLE;
  }

  const sources = Array.isArray(parsed.sourceEvidence)
    ? parsed.sourceEvidence.map((s) =>
        typeof s === "string"
          ? { url: s, claimKind: CLAIM_KIND.FACT }
          : {
              url: s.url || s.sourceUrl || null,
              title: s.title || null,
              claimKind: s.claimKind || s.kind || CLAIM_KIND.UNKNOWN,
              authority: s.authority || s.sourceAuthority || null,
              date: s.date || s.sourceDate || null,
            }
      )
    : [];

  return {
    schemaVersion: "gdi_research_evidence_v1",
    provider: providerId,
    providerMetadata: {
      taskContract: taskContract || null,
      hotelId: hotelContext?.hotelId || hotelContext?.id || null,
      opportunityId: opportunityContext?.id || opportunityContext?.opportunityId || null,
      normalizedAt: new Date().toISOString(),
    },
    eventName: asClaim(parsed.eventName, parsed.eventName ? CLAIM_KIND.FACT : CLAIM_KIND.UNKNOWN),
    organization: asClaim(
      parsed.organization,
      parsed.organization ? CLAIM_KIND.FACT : CLAIM_KIND.UNKNOWN
    ),
    dates: {
      start: asClaim(parsed.dates?.start || null, parsed.dates?.start ? CLAIM_KIND.FACT : CLAIM_KIND.UNKNOWN),
      end: asClaim(parsed.dates?.end || null, parsed.dates?.end ? CLAIM_KIND.ESTIMATED : CLAIM_KIND.UNKNOWN),
    },
    geography: asClaim(parsed.geography, parsed.geography ? CLAIM_KIND.FACT : CLAIM_KIND.UNKNOWN),
    venueStatus,
    sourcingStatus: venueStatus,
    opportunityTypeCandidate,
    attendance: asClaim(
      parsed.attendance,
      parsed.attendance != null ? CLAIM_KIND.ESTIMATED : CLAIM_KIND.UNKNOWN
    ),
    roomDemand: asClaim(
      parsed.roomDemand,
      parsed.roomDemand != null ? CLAIM_KIND.ESTIMATED : CLAIM_KIND.UNKNOWN
    ),
    housingEvidence: asClaim(
      parsed.housingEvidence,
      parsed.housingEvidence ? CLAIM_KIND.FACT : CLAIM_KIND.UNKNOWN
    ),
    contactEvidence: parsed.contactEvidence || null,
    sourceEvidence: sources,
    factEstimateInference: {
      eventName: parsed.eventName ? CLAIM_KIND.FACT : CLAIM_KIND.UNKNOWN,
      organization: parsed.organization ? CLAIM_KIND.FACT : CLAIM_KIND.UNKNOWN,
      roomDemand: parsed.roomDemand != null ? CLAIM_KIND.ESTIMATED : CLAIM_KIND.UNKNOWN,
      winThesis: parsed.winThesis ? CLAIM_KIND.INFERENCE : CLAIM_KIND.UNKNOWN,
      whyNow: parsed.whyNow ? CLAIM_KIND.INFERENCE : CLAIM_KIND.UNKNOWN,
    },
    contradictions: parsed.contradictions || [],
    unresolvedQuestions: parsed.unresolvedQuestions || [],
    confidence: parsed.confidence,
    researchCompleteness: {
      hasEvent: Boolean(parsed.eventName),
      hasOrg: Boolean(parsed.organization),
      hasVenue: venueStatus !== CANONICAL_VENUE_SOURCING.UNKNOWN,
      hasSources: sources.length > 0,
      hasWhyNow: Boolean(parsed.whyNow),
      hasWinThesis: Boolean(parsed.winThesis),
    },
    futureCycleEvidenceState: futureCycle.state,
    whyMonitor: futureCycle.whyMonitor,
    whyNow: parsed.whyNow || null,
    winThesis: parsed.winThesis || null,
    researchPlan: parsed.researchPlan || null,
    rawOutput: parsed.raw || rawOutput,
  };
}
