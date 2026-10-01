/**
 * Stage-level ownership research tracing and earliest-failure classification.
 * Staging diagnostics only — never stores secrets or raw personal contact values.
 */
export const OWNERSHIP_RESEARCH_FAILURE_STAGE = Object.freeze({
  NO_SEARCH_RESULT: "NO_SEARCH_RESULT",
  RESULT_FILTERED: "RESULT_FILTERED",
  FETCH_FAILED: "FETCH_FAILED",
  NO_OWNERSHIP_PASSAGE: "NO_OWNERSHIP_PASSAGE",
  RELATIONSHIP_AMBIGUOUS: "RELATIONSHIP_AMBIGUOUS",
  CURRENTNESS_UNRESOLVED: "CURRENTNESS_UNRESOLVED",
  OWNER_DOMAIN_UNRESOLVED: "OWNER_DOMAIN_UNRESOLVED",
  PERSON_UNRESOLVED: "PERSON_UNRESOLVED",
  CONTACT_UNAVAILABLE: "CONTACT_UNAVAILABLE",
  NONE: "NONE",
});

export const OWNERSHIP_PARTY_KIND = Object.freeze({
  ECONOMIC_OWNER: "ECONOMIC_OWNER",
  PROPERTY_OWNING_COMPANY: "PROPERTY_OWNING_COMPANY",
  OPERATOR: "OPERATOR",
  MANAGER: "MANAGER",
  BRAND: "BRAND",
  FRANCHISEE: "FRANCHISEE",
  DEVELOPER: "DEVELOPER",
  SPONSOR: "SPONSOR",
  PORTFOLIO_MEMBER: "PORTFOLIO_MEMBER",
  CORPORATE_PARENT: "CORPORATE_PARENT",
  HISTORICAL_ACQUIRER: "HISTORICAL_ACQUIRER",
  MEDIA_IR_CONTACT: "MEDIA_IR_CONTACT",
  UNRESOLVED: "UNRESOLVED",
});

export function createOwnershipStageTrace(hotel = {}) {
  return {
    version: "ownership-research-stage-trace-v1",
    hotel_id: hotel.hotel_id || null,
    hotel_name: hotel.hotel_name || null,
    language: hotel.language || null,
    stages: [],
    searches: [],
    fetches: [],
    claims: [],
    follow_ups: [],
    budget_snapshots: [],
    earliest_failure_stage: null,
    final_stop_reasons: [],
  };
}

export function pushStageEvent(trace, event = {}) {
  if (!trace || typeof trace !== "object") return;
  const row = {
    at: new Date().toISOString(),
    stage: event.stage || "UNKNOWN",
    ...event,
  };
  delete row.api_key;
  delete row.email;
  delete row.phone;
  delete row.raw_contact;
  trace.stages.push(row);
  if (event.kind === "search") trace.searches.push(row);
  if (event.kind === "fetch") trace.fetches.push(row);
  if (event.kind === "claim") trace.claims.push(row);
  if (event.kind === "follow_up") trace.follow_ups.push(row);
  if (event.kind === "budget") trace.budget_snapshots.push(row);
}

/**
 * Normalize claim contract fields required by ownership semantics.
 * Does not invent parties or promote current ownership.
 */
export function normalizeOwnershipClaimContract(claim = {}, hotel = {}) {
  const rel = String(claim.relationship || "").toUpperCase() || null;
  const currentness = String(claim.currentness || "UNRESOLVED").toUpperCase();
  const partyKind = claim.party_kind || mapRelationshipToPartyKind(claim) || OWNERSHIP_PARTY_KIND.UNRESOLVED;
  return {
    subject: claim.subject ?? claim.name ?? "",
    relationship: rel,
    object: claim.object ?? hotel.hotel_name ?? "",
    hotel_asset_scope: claim.hotel_asset_scope || claim.scope || "HOTEL_ASSET",
    source_url: claim.source_url || claim.url || null,
    source_date: claim.source_date ?? null,
    event_date: claim.event_date ?? null,
    currentness,
    evidence_grounded: Boolean(claim.evidence_grounded),
    target_hotel_link_validated: Boolean(
      claim.target_hotel_link_validated ??
        claim.target_hotel_link?.establishes_relationship_to_target_validated
    ),
    party_kind: partyKind,
    party_role: claim.party_role || null,
    evidence_span: claim.evidence_span || claim.excerpt || null,
    promotes_to_current_ownership: false,
    enrichment_authorized: false,
    publication_authorized: false,
    next_research_question: claim.next_research_question || null,
  };
}

export function mapRelationshipToPartyKind(claim = {}) {
  const rel = String(claim.relationship || "").toUpperCase();
  const cur = String(claim.currentness || "").toUpperCase();
  const role = String(claim.party_role || "").toUpperCase();
  if (rel === "OPERATES" || rel === "MANAGES" || role === "OPERATOR") return OWNERSHIP_PARTY_KIND.OPERATOR;
  if (rel === "BRANDS" || rel === "BRAND" || role === "BRAND") return OWNERSHIP_PARTY_KIND.BRAND;
  if (rel === "PARENT_ACQUIRED" || claim.scope === "COMPANY") return OWNERSHIP_PARTY_KIND.CORPORATE_PARENT;
  if (claim.scope === "GENERAL_PORTFOLIO") return OWNERSHIP_PARTY_KIND.PORTFOLIO_MEMBER;
  if (
    role === "HISTORICAL_OWNER" ||
    cur === "HISTORICAL" ||
    (rel === "ACQUIRED" && cur !== "CURRENT_AS_OF_STATED_DATE")
  ) {
    return OWNERSHIP_PARTY_KIND.HISTORICAL_ACQUIRER;
  }
  if (rel === "OWNS" || rel === "OWNED_BY") {
    return cur === "CURRENT_AS_OF_STATED_DATE"
      ? OWNERSHIP_PARTY_KIND.ECONOMIC_OWNER
      : OWNERSHIP_PARTY_KIND.HISTORICAL_ACQUIRER;
  }
  if (rel === "SPONSORS" || role === "ECONOMIC_SPONSOR") return OWNERSHIP_PARTY_KIND.SPONSOR;
  if (/media|investor.?relations|press.?contact/i.test(`${claim.subject || ""} ${claim.title || ""}`)) {
    return OWNERSHIP_PARTY_KIND.MEDIA_IR_CONTACT;
  }
  return OWNERSHIP_PARTY_KIND.UNRESOLVED;
}

export function historicalClaimFollowUpQuestions(claim = {}, hotel = {}) {
  const hotelName = hotel.hotel_name || claim.object || "this hotel";
  const party = claim.subject || claim.corrected_subject || "the named party";
  return [
    `Who owns or controls ${hotelName} now?`,
    `Was ${hotelName} sold after the cited transaction involving ${party}?`,
    `Did the transaction concern the hotel asset or only an operating company?`,
    `Is ${party} an owner, operator, brand, or sponsor of ${hotelName}?`,
  ];
}

/**
 * Infer earliest failure stage from loop state / handoff outcome.
 */
export function inferEarliestOwnershipFailureStage({
  claims = [],
  searches = [],
  fetches = [],
  rankedEmptyBecauseFiltered = false,
  confirmedDomain = null,
  evidencedPeople = [],
  hasAttributableContact = false,
  stopReasons = [],
  objectiveSupported = false,
} = {}) {
  if (objectiveSupported && confirmedDomain && evidencedPeople.length && hasAttributableContact) {
    return OWNERSHIP_RESEARCH_FAILURE_STAGE.NONE;
  }

  const searchCount = searches.length;
  const rawHits = searches.reduce((n, s) => n + Number(s.raw_result_count || s.result_count || 0), 0);
  const fetchAttempts = fetches.length;
  const fetchOk = fetches.filter((f) => f.ok && (f.characters || 0) > 0).length;
  const fetchFailed = fetches.some((f) => f.ok === false || ((f.characters || 0) === 0 && f.attempted));

  const grounded = claims.filter((c) => c.evidence_grounded);
  const ownershipLike = grounded.filter((c) =>
    ["OWNS", "OWNED_BY", "ACQUIRED", "SPONSORS"].includes(String(c.relationship || "").toUpperCase())
  );
  const current = ownershipLike.filter(
    (c) => String(c.currentness || "").toUpperCase() === "CURRENT_AS_OF_STATED_DATE"
  );
  const historicalOnly = ownershipLike.length > 0 && current.length === 0;
  const ambiguousOnly =
    grounded.length > 0 &&
    ownershipLike.length === 0 &&
    grounded.every((c) =>
      ["OTHER", "FAMILY_OWNS_UNNAMED", "OPERATES", "BRANDS"].includes(String(c.relationship || "").toUpperCase())
    );

  if (searchCount > 0 && rawHits === 0) return OWNERSHIP_RESEARCH_FAILURE_STAGE.NO_SEARCH_RESULT;
  if (rawHits > 0 && rankedEmptyBecauseFiltered && fetchAttempts === 0) {
    return OWNERSHIP_RESEARCH_FAILURE_STAGE.RESULT_FILTERED;
  }
  if (fetchAttempts > 0 && fetchOk === 0 && fetchFailed) return OWNERSHIP_RESEARCH_FAILURE_STAGE.FETCH_FAILED;
  if (fetchOk > 0 && grounded.length === 0) return OWNERSHIP_RESEARCH_FAILURE_STAGE.NO_OWNERSHIP_PASSAGE;
  if (ambiguousOnly) return OWNERSHIP_RESEARCH_FAILURE_STAGE.RELATIONSHIP_AMBIGUOUS;
  if (historicalOnly || (ownershipLike.length > 0 && current.length === 0)) {
    return OWNERSHIP_RESEARCH_FAILURE_STAGE.CURRENTNESS_UNRESOLVED;
  }
  if (current.length > 0 && !confirmedDomain) return OWNERSHIP_RESEARCH_FAILURE_STAGE.OWNER_DOMAIN_UNRESOLVED;
  if (confirmedDomain && !(evidencedPeople || []).length) return OWNERSHIP_RESEARCH_FAILURE_STAGE.PERSON_UNRESOLVED;
  if ((evidencedPeople || []).length && !hasAttributableContact) {
    return OWNERSHIP_RESEARCH_FAILURE_STAGE.CONTACT_UNAVAILABLE;
  }
  if (stopReasons.includes("BUDGET_EXHAUSTED") && ownershipLike.length === 0 && fetchOk === 0) {
    return rawHits > 0
      ? OWNERSHIP_RESEARCH_FAILURE_STAGE.RESULT_FILTERED
      : OWNERSHIP_RESEARCH_FAILURE_STAGE.NO_SEARCH_RESULT;
  }
  if (grounded.length === 0 && fetchOk === 0 && rawHits > 0) return OWNERSHIP_RESEARCH_FAILURE_STAGE.RESULT_FILTERED;
  if (grounded.length === 0) return OWNERSHIP_RESEARCH_FAILURE_STAGE.NO_OWNERSHIP_PASSAGE;
  return OWNERSHIP_RESEARCH_FAILURE_STAGE.CURRENTNESS_UNRESOLVED;
}

export function sanitizeOwnershipStageTraceForFixture(trace = {}) {
  const clone = JSON.parse(JSON.stringify(trace || {}));
  for (const s of clone.stages || []) {
    if (s.document_excerpt) s.document_excerpt = String(s.document_excerpt).slice(0, 240);
    if (s.evidence_span) s.evidence_span = String(s.evidence_span).slice(0, 240);
    delete s.markdown;
    delete s.email;
    delete s.phone;
  }
  return clone;
}
