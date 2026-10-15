/**
 * Shared GDI demand model V1 — research segment ≠ revenue/use case.
 *
 * One account → one standing opportunity (merge growth signals).
 * Distinct dated occasions stay distinct.
 * Hotel-specific opportunities are independently assessed.
 * Multiple use cases share a standing card unless separate pursuits are evidenced.
 */

export const SHARED_DEMAND_MODEL_VERSION = "gdi-shared-demand-model-v1";

/** Customer-facing commercial use cases (orthogonal to research segment). */
export const DEMAND_USE_CASE = Object.freeze({
  GROUP_LODGING: "GROUP_LODGING",
  MEETINGS_AND_CATERING: "MEETINGS_AND_CATERING",
  ROOMS_PLUS_MEETINGS: "ROOMS_PLUS_MEETINGS",
  VENUE_REFERRAL_PARTNERSHIP: "VENUE_REFERRAL_PARTNERSHIP",
});

export const DEMAND_USE_CASE_LABEL = Object.freeze({
  [DEMAND_USE_CASE.GROUP_LODGING]: "Group lodging",
  [DEMAND_USE_CASE.MEETINGS_AND_CATERING]: "Meetings & catering",
  [DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS]: "Rooms + meetings",
  [DEMAND_USE_CASE.VENUE_REFERRAL_PARTNERSHIP]: "Venue referral partnership",
});

/** Relationship grain (not a use case). */
export const RELATIONSHIP_GRAIN = Object.freeze({
  STANDING_ACCOUNT: "STANDING_ACCOUNT",
  DATED_OCCASION: "DATED_OCCASION",
  VENUE_REFERRAL: "VENUE_REFERRAL",
});

export const GUESTROOM_STATUS = Object.freeze({
  NOT_REQUIRED: "NOT_REQUIRED",
  POTENTIAL_ADDITIONAL_USE: "POTENTIAL_ADDITIONAL_USE",
  NOT_ESTABLISHED: "NOT_ESTABLISHED",
  REQUIRED_EVIDENCED: "REQUIRED_EVIDENCED",
});

export const GUESTROOM_STATUS_LABEL = Object.freeze({
  [GUESTROOM_STATUS.NOT_REQUIRED]: "Not required for this opportunity",
  [GUESTROOM_STATUS.POTENTIAL_ADDITIONAL_USE]: "Potential additional use",
  [GUESTROOM_STATUS.NOT_ESTABLISHED]: "Not established",
  [GUESTROOM_STATUS.REQUIRED_EVIDENCED]: "Guestrooms evidenced",
});

/** Coverage status — not triggered by prospect count alone. */
export const COVERAGE_STATUS = Object.freeze({
  NOT_STARTED: "not_started",
  PARTIAL: "partial",
  COMPLETED_FOR_DECLARED_SCOPE: "completed_for_declared_scope",
  BLOCKED: "blocked",
  NOT_APPLICABLE: "not_applicable",
});

/** How research was executed for a segment/hotel. */
export const EXECUTION_MODE = Object.freeze({
  REUSE: "reuse",
  FRESH_RESEARCH: "fresh_research",
  REPLAY: "replay",
  PLAN_ONLY: "plan_only",
});

/**
 * Normalize use-case list on an opportunity (dedupe, validate).
 */
export function normalizeDemandUseCases(raw = []) {
  const list = []
    .concat(raw || [])
    .map((u) => String(u || "").trim().toUpperCase())
    .filter((u) => Object.values(DEMAND_USE_CASE).includes(u));
  return [...new Set(list)];
}

/**
 * Dedupe rule: same standing account at one hotel → one opportunity.
 * Dated occasions with distinct evidence remain separate.
 */
export function shouldMergeStandingAccountSignals({
  existing = {},
  candidate = {},
} = {}) {
  const sameHotel =
    String(existing.hotelId || "") === String(candidate.hotelId || "");
  if (!sameHotel) return false;
  const grainA = existing.relationshipGrain || RELATIONSHIP_GRAIN.STANDING_ACCOUNT;
  const grainB = candidate.relationshipGrain || RELATIONSHIP_GRAIN.STANDING_ACCOUNT;
  if (
    grainA === RELATIONSHIP_GRAIN.DATED_OCCASION ||
    grainB === RELATIONSHIP_GRAIN.DATED_OCCASION
  ) {
    // Distinct dated occasions do not merge into standing cards automatically
    const dateA = String(existing.eventStartDate || existing.occasionKey || "");
    const dateB = String(candidate.eventStartDate || candidate.occasionKey || "");
    if (dateA && dateB && dateA !== dateB) return false;
  }
  const orgA = String(existing.organizationName || existing.company || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  const orgB = String(candidate.organizationName || candidate.company || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  if (!orgA || !orgB) return false;
  return orgA === orgB || orgA.includes(orgB) || orgB.includes(orgA);
}

/**
 * Merge use cases + growth signals onto an existing standing card.
 */
export function mergeStandingAccountFields(existing = {}, incoming = {}) {
  const useCases = normalizeDemandUseCases([
    ...(existing.demandUseCases || existing.useCases || []),
    ...(incoming.demandUseCases || incoming.useCases || []),
  ]);
  const growthSignals = [
    ...new Set(
      []
        .concat(existing.growthSignals || [])
        .concat(incoming.growthSignals || [])
        .map((s) => String(s || "").trim())
        .filter(Boolean)
    ),
  ];
  return {
    ...existing,
    ...incoming,
    id: existing.id || existing.opportunityId || incoming.id,
    opportunityId: existing.opportunityId || existing.id || incoming.opportunityId,
    demandUseCases: useCases,
    growthSignals,
    relationshipGrain:
      existing.relationshipGrain ||
      incoming.relationshipGrain ||
      RELATIONSHIP_GRAIN.STANDING_ACCOUNT,
  };
}

export function guestroomStatusForUseCases(useCases = [], explicit = null) {
  if (explicit && Object.values(GUESTROOM_STATUS).includes(explicit)) {
    return explicit;
  }
  const uc = normalizeDemandUseCases(useCases);
  if (
    uc.includes(DEMAND_USE_CASE.MEETINGS_AND_CATERING) &&
    !uc.includes(DEMAND_USE_CASE.GROUP_LODGING) &&
    !uc.includes(DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS)
  ) {
    return GUESTROOM_STATUS.NOT_REQUIRED;
  }
  if (
    uc.includes(DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS) ||
    (uc.includes(DEMAND_USE_CASE.MEETINGS_AND_CATERING) &&
      uc.includes(DEMAND_USE_CASE.GROUP_LODGING))
  ) {
    return GUESTROOM_STATUS.POTENTIAL_ADDITIONAL_USE;
  }
  if (uc.includes(DEMAND_USE_CASE.GROUP_LODGING)) {
    return GUESTROOM_STATUS.NOT_ESTABLISHED;
  }
  return GUESTROOM_STATUS.NOT_ESTABLISHED;
}
