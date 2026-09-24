/**
 * GDI WHO gap taxonomy — root causes + researched outcome states.
 * Precision-first: do not invent named WHO; distinguish researched vs silent gaps.
 */

export const WHO_GAP_ROOT_CAUSE = Object.freeze({
  NO_PERSON_FOUND: "NO_PERSON_FOUND",
  PERSON_FOUND_ROLE_UNCLEAR: "PERSON_FOUND_ROLE_UNCLEAR",
  PERSON_FOUND_NOT_CURRENT: "PERSON_FOUND_NOT_CURRENT",
  MULTIPLE_PLAUSIBLE_PEOPLE: "MULTIPLE_PLAUSIBLE_PEOPLE",
  ORGANIZATION_ONLY_SOURCE: "ORGANIZATION_ONLY_SOURCE",
  EVENT_SITE_NO_STAFF: "EVENT_SITE_NO_STAFF",
  HOUSING_VENDOR_ONLY: "HOUSING_VENDOR_ONLY",
  COMMITTEE_BASED_DECISION: "COMMITTEE_BASED_DECISION",
  ROTATING_EVENT_OWNER: "ROTATING_EVENT_OWNER",
  OUTSOURCED_MEETING_PLANNER: "OUTSOURCED_MEETING_PLANNER",
  REGISTRATION_ONLY_CONTACT: "REGISTRATION_ONLY_CONTACT",
  SPONSORSHIP_ONLY_CONTACT: "SPONSORSHIP_ONLY_CONTACT",
  GENERIC_EVENTS_INBOX: "GENERIC_EVENTS_INBOX",
  SOURCE_TOO_OLD: "SOURCE_TOO_OLD",
  IDENTITY_CONFLICT: "IDENTITY_CONFLICT",
  OTHER: "OTHER",
});

/** Production WHO research outcome — no silent unresolved after a research pass. */
export const WHO_RESEARCH_STATE = Object.freeze({
  NAMED_PERSON_CONFIRMED: "NAMED_PERSON_CONFIRMED",
  NAMED_PERSON_NEEDS_VALIDATION: "NAMED_PERSON_NEEDS_VALIDATION",
  FUNCTIONAL_CONTACT_ONLY_RESEARCHED: "FUNCTIONAL_CONTACT_ONLY_RESEARCHED",
  NO_WHO_RESEARCHED: "NO_WHO_RESEARCHED",
  NO_WHO_UNRESEARCHED: "NO_WHO_UNRESEARCHED",
  FUNCTIONAL_CONTACT_ONLY_UNRESEARCHED: "FUNCTIONAL_CONTACT_ONLY_UNRESEARCHED",
});

export const WHO_CURRENTNESS = Object.freeze({
  CURRENT: "CURRENT",
  LIKELY_CURRENT: "LIKELY_CURRENT",
  HISTORICAL: "HISTORICAL",
  UNKNOWN: "UNKNOWN",
});

export const WHO_EVENT_RELATION = Object.freeze({
  DIRECT_EVENT_OWNER: "DIRECT_EVENT_OWNER",
  DIRECT_EVENT_OPERATOR: "DIRECT_EVENT_OPERATOR",
  ORGANIZATION_EVENTS_ROLE: "ORGANIZATION_EVENTS_ROLE",
  HOUSING_LOGISTICS_ROLE: "HOUSING_LOGISTICS_ROLE",
  EXECUTIVE_SPONSOR: "EXECUTIVE_SPONSOR",
  GENERIC_ORGANIZATION_STAFF: "GENERIC_ORGANIZATION_STAFF",
  UNRELATED: "UNRELATED",
});

export const WHO_QUALITY_AUDIT = Object.freeze({
  SUPPORTED: "SUPPORTED",
  PLAUSIBLE_NEEDS_VALIDATION: "PLAUSIBLE_NEEDS_VALIDATION",
  WEAK: "WEAK",
  REJECTED: "REJECTED",
});

export const SOURCE_AUTHORITY_TIER = Object.freeze({
  TIER_1_OFFICIAL_EVENT_ORG: 1,
  TIER_2_OFFICIAL_PARTNER: 2,
  TIER_3_PRESS_PROFILE: 3,
  TIER_4_AGGREGATOR: 4,
});

/**
 * Heuristic root-cause codes from current opportunity contact shape (pre-research).
 */
export function classifyWhoGapRootCauses(opportunity = {}, whoStatus = null) {
  const pc = opportunity.primaryContact || {};
  const cq = String(opportunity.contactQuality || pc.contactQuality || "");
  const blob = [
    pc.name,
    pc.role,
    pc.relationshipToEvent,
    opportunity.title,
    cq,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");

  const codes = new Set();

  if (whoStatus === "NO_WHO" || cq === "NO_CONTACT" || !pc.name) {
    codes.add(WHO_GAP_ROOT_CAUSE.NO_PERSON_FOUND);
  }
  if (cq === "GENERIC_INBOX" || /info@|events@|contact@/.test(String(pc.email || ""))) {
    codes.add(WHO_GAP_ROOT_CAUSE.GENERIC_EVENTS_INBOX);
  }
  if (/housing|onpeak|passkey|team travel|connections housing|hbc/i.test(blob)) {
    codes.add(WHO_GAP_ROOT_CAUSE.HOUSING_VENDOR_ONLY);
  }
  if (/procurement|corporate travel|preferred rate/i.test(blob)) {
    codes.add(WHO_GAP_ROOT_CAUSE.ORGANIZATION_ONLY_SOURCE);
  }
  if (/committee|board of directors|chairs/i.test(blob)) {
    codes.add(WHO_GAP_ROOT_CAUSE.COMMITTEE_BASED_DECISION);
  }
  if (/sponsor/i.test(blob) && !/events manager|meetings|conference/i.test(blob)) {
    codes.add(WHO_GAP_ROOT_CAUSE.SPONSORSHIP_ONLY_CONTACT);
  }
  if (/registration|registrar/i.test(blob) && !/meetings|events|housing/i.test(blob)) {
    codes.add(WHO_GAP_ROOT_CAUSE.REGISTRATION_ONLY_CONTACT);
  }
  if (/pbconventioncenter|venue listing/i.test(String(pc.sourceUrl || pc.source || ""))) {
    codes.add(WHO_GAP_ROOT_CAUSE.EVENT_SITE_NO_STAFF);
  }
  if (!codes.size) codes.add(WHO_GAP_ROOT_CAUSE.OTHER);

  return [...codes];
}

/**
 * Map employment + event relationship → currentness / event relation enums.
 */
export function mapDiscoveryToCurrentness(candidate = {}) {
  const emp = String(candidate.employmentStatus || "");
  if (emp === "CURRENT_CONFIRMED") return WHO_CURRENTNESS.CURRENT;
  if (emp === "CURRENT_PROBABLE") return WHO_CURRENTNESS.LIKELY_CURRENT;
  if (emp === "HISTORICAL_ONLY" || emp === "FORMER_EMPLOYEE") {
    return WHO_CURRENTNESS.HISTORICAL;
  }
  const rel = String(candidate.eventRelationship || "");
  if (rel === "CURRENT_EVENT_CONTACT" || rel === "CURRENT_ROLE_LIKELY_OWNER") {
    return WHO_CURRENTNESS.LIKELY_CURRENT;
  }
  return WHO_CURRENTNESS.UNKNOWN;
}

export function mapDiscoveryToEventRelation(candidate = {}) {
  const rel = String(candidate.eventRelationship || "");
  if (rel === "CURRENT_EVENT_CONTACT") return WHO_EVENT_RELATION.DIRECT_EVENT_OPERATOR;
  if (rel === "CURRENT_ROLE_LIKELY_OWNER") return WHO_EVENT_RELATION.ORGANIZATION_EVENTS_ROLE;
  if (rel === "HISTORICAL_EVENT_CONTACT") return WHO_EVENT_RELATION.GENERIC_ORGANIZATION_STAFF;
  if (/housing/i.test(String(candidate.gdiContactRole || candidate.role || ""))) {
    return WHO_EVENT_RELATION.HOUSING_LOGISTICS_ROLE;
  }
  if (/executive/i.test(String(candidate.role || ""))) {
    return WHO_EVENT_RELATION.EXECUTIVE_SPONSOR;
  }
  if (rel === "ORGANIZATION_ROLE_ONLY") return WHO_EVENT_RELATION.GENERIC_ORGANIZATION_STAFF;
  if (rel === "WEAK_CONTEXT") return WHO_EVENT_RELATION.UNRELATED;
  return WHO_EVENT_RELATION.ORGANIZATION_EVENTS_ROLE;
}

/**
 * Acceptance gate for NAMED_PERSON_CONFIRMED vs NEEDS_VALIDATION.
 */
export function acceptNamedWhoGate(candidate = {}) {
  const currentness = mapDiscoveryToCurrentness(candidate);
  const eventRel = mapDiscoveryToEventRelation(candidate);
  const tier = Number(candidate.sourceAuthorityTier || candidate.sourceTier || 3);
  const score = Number(candidate.candidateScore || 0);
  const quality = String(candidate.qualityAudit || "");

  if (currentness === WHO_CURRENTNESS.HISTORICAL) {
    return {
      ok: false,
      researchState: WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION,
      reason: "historical_not_promoted",
    };
  }
  if (eventRel === WHO_EVENT_RELATION.UNRELATED) {
    return {
      ok: false,
      researchState: WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION,
      reason: "unrelated_event_relation",
    };
  }
  if (tier > 2 && score < 70 && quality !== "SUPPORTED") {
    return {
      ok: false,
      researchState: WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION,
      reason: "weak_source_authority",
    };
  }

  const roleOk =
    eventRel === WHO_EVENT_RELATION.DIRECT_EVENT_OWNER ||
    eventRel === WHO_EVENT_RELATION.DIRECT_EVENT_OPERATOR ||
    eventRel === WHO_EVENT_RELATION.ORGANIZATION_EVENTS_ROLE ||
    eventRel === WHO_EVENT_RELATION.HOUSING_LOGISTICS_ROLE;

  if (
    (currentness === WHO_CURRENTNESS.CURRENT ||
      currentness === WHO_CURRENTNESS.LIKELY_CURRENT) &&
    roleOk
  ) {
    return {
      ok: true,
      researchState: WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED,
      reason: "identity_role_currentness_ok",
    };
  }

  // Tier-1/2 official seed with SUPPORTED audit may confirm on role alone
  if (tier <= 2 && roleOk && (quality === "SUPPORTED" || score >= 65)) {
    return {
      ok: true,
      researchState: WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED,
      reason: "tier12_official_role_ok",
    };
  }

  return {
    ok: false,
    researchState: WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION,
    reason: "needs_validation",
  };
}
