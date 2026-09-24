/**
 * Role evidence tiers + rejection gates for Native WHO V3/V4.
 * Precision before coverage.
 *
 * V4: separate evidence dimensions — never let source authority alone create Tier A.
 */

export const ROLE_EVIDENCE_TIER = Object.freeze({
  A_DIRECT_EVENT: "A",
  B_ORG_FUNCTION: "B",
  C_STRONG_INFERENCE: "C",
  D_WEAK: "D",
});

export const REJECTION_GATE = Object.freeze({
  SPEAKER_NOT_ORGANIZER: "SPEAKER_NOT_ORGANIZER",
  BOARD_NOT_EVENT_OWNER: "BOARD_NOT_EVENT_OWNER",
  WRONG_ORGANIZATION: "WRONG_ORGANIZATION",
  ROLE_TOO_GENERIC: "ROLE_TOO_GENERIC",
  OUTDATED_PERSON: "OUTDATED_PERSON",
  UNSUPPORTED_EXECUTIVE: "UNSUPPORTED_EXECUTIVE",
  PRESIDENT_ONLY: "PRESIDENT_ONLY",
  EXECUTIVE_ONLY: "EXECUTIVE_ONLY",
  PRESS_MENTION_ONLY: "PRESS_MENTION_ONLY",
  PARTNER_NOT_OWNER: "PARTNER_NOT_OWNER",
  SOCIAL_ONLY_SOURCE: "SOCIAL_ONLY_SOURCE",
  VENUE_STAFF: "VENUE_STAFF",
  AGGREGATOR_SOURCE: "AGGREGATOR_SOURCE",
  WEAK_RELEVANCE: "WEAK_RELEVANCE",
  MISSING_IDENTITY: "MISSING_IDENTITY",
});

const MEETINGS_TITLE_RE =
  /\b(meetings?|events?|conference|convention|housing|registration|exhibitor|sponsorship|tournament|program|member services|audience engagement|partnerships?|business development|bd|summit|forum|expo)\b.*\b(director|manager|coordinator|lead|owner|chair|vp|vice president)\b|\b(director|manager|coordinator|vp|vice president|senior manager)\b.*\b(meetings?|events?|conference|convention|housing|registration|partnerships?|member services|audience engagement|programs?|business development|sponsorship|summit|forum|expo)\b|\b(tournament director|executive director|cups? director|program managers?)\b|\bdirector of\b.+\b(framework|program|initiative|education|cyber|nice)\b/i;

// Do NOT match "Vice President" as bare president (lookbehind).
const EXEC_ONLY_RE =
  /\b(ceo|(?<!vice\s)president|chairman|chair of the board|board member|trustee|founder|evangelism)\b/i;
const PRESIDENT_ONLY_RE = /^(president|ceo|chairman)\b/i;
const SPEAKER_RE = /\b(speaker|keynote|panelist|presenter|faculty)\b/i;
const BOARD_RE = /\b(board of directors|board member|trustee)\b/i;
const PRESS_URL_RE =
  /\/press\/|press-release|newsroom|newsdesk|\/news\/|articles\/|viewpoint/i;

/**
 * Build independent evidence dimensions (V4).
 * One strong dimension must not substitute for another.
 */
export function buildEvidenceMatrix(candidate = {}, ctx = {}) {
  const role = String(candidate.role || "");
  const quote = String(candidate.evidenceQuote || "");
  const blob = `${role} ${quote}`.toLowerCase();
  const sourceUrl = String(candidate.sourceUrl || "").toLowerCase();
  const onOfficial = Boolean(candidate.onOfficialDomain);
  const fromPdf = Boolean(candidate.fromPdf);
  const fromHtml = Boolean(candidate.fromHtml);
  const pdfCtx = String(candidate.pdfRoleContext || "");

  const isPress = PRESS_URL_RE.test(sourceUrl);
  const hasMeetingsRole = MEETINGS_TITLE_RE.test(role) || MEETINGS_TITLE_RE.test(quote);
  const contactBlock =
    fromPdf ||
    fromHtml ||
    /contact|mailto|please (?:do not hesitate to )?contact|send email|tournament contacts/i.test(
      quote
    );
  const eventStaffBlock =
    /executive staff|tournament (?:director|staff)|conference (?:director|manager)|housing|registration/i.test(
      quote
    ) ||
    ["ORGANIZER", "EVENT_STAFF", "HOUSING", "REGISTRATION", "SPONSORSHIP"].includes(pdfCtx);

  const identityEvidence =
    candidate.name && (onOfficial || fromPdf || fromHtml || Boolean(candidate.email))
      ? contactBlock || hasMeetingsRole
        ? "STRONG"
        : isPress
          ? "MEDIUM"
          : "MEDIUM"
      : "WEAK";

  const currentnessEvidence = isPress
    ? "STRONG"
    : candidate.currentness === "CURRENT"
      ? "STRONG"
      : candidate.currentness === "LIKELY_CURRENT"
        ? "MEDIUM"
        : candidate.currentness === "HISTORICAL"
          ? "WEAK"
          : onOfficial
            ? "MEDIUM"
            : "WEAK";

  const roleEvidence = hasMeetingsRole
    ? "STRONG"
    : EXEC_ONLY_RE.test(role) && !hasMeetingsRole
      ? "WEAK"
      : /director|manager|coordinator/i.test(role)
        ? "MEDIUM"
        : "WEAK";

  const eventRelationEvidence =
    eventStaffBlock || (fromPdf && contactBlock) || (candidate.eventSpecificEvidence && hasMeetingsRole)
      ? "STRONG"
      : hasMeetingsRole && onOfficial && !isPress
        ? "MEDIUM"
        : isPress && EXEC_ONLY_RE.test(role)
          ? "WEAK"
          : candidate.eventSpecificEvidence
            ? "MEDIUM"
            : "WEAK";

  const sourceAuthority = onOfficial
    ? isPress
      ? "MEDIUM"
      : "STRONG"
    : fromPdf
      ? "STRONG"
      : "WEAK";

  return {
    identityEvidence,
    currentnessEvidence,
    roleEvidence,
    eventRelationEvidence,
    sourceAuthority,
  };
}

function strengthRank(s) {
  return s === "STRONG" ? 3 : s === "MEDIUM" ? 2 : 1;
}

/**
 * Infer evidence tier from matrix — Tier A requires strong ROLE + EVENT RELATION,
 * not merely strong source authority / currentness.
 */
export function inferRoleEvidenceTier(candidate = {}, ctx = {}) {
  const matrix = candidate.evidenceMatrix || buildEvidenceMatrix(candidate, ctx);
  const role = String(candidate.role || "");
  const quote = String(candidate.evidenceQuote || "");
  const blob = `${role} ${quote}`.toLowerCase();
  const sourceUrl = String(candidate.sourceUrl || "").toLowerCase();
  const pdfCtx = String(candidate.pdfRoleContext || "");

  if (SPEAKER_RE.test(blob) || /\/speaker\//i.test(sourceUrl) || pdfCtx === "SPEAKER") {
    return ROLE_EVIDENCE_TIER.D_WEAK;
  }
  if (BOARD_RE.test(blob) && !MEETINGS_TITLE_RE.test(role)) {
    return ROLE_EVIDENCE_TIER.D_WEAK;
  }

  // President/CEO/press-only cannot be Tier A
  if (
    (PRESIDENT_ONLY_RE.test(role.trim()) || EXEC_ONLY_RE.test(role)) &&
    !MEETINGS_TITLE_RE.test(role) &&
    strengthRank(matrix.eventRelationEvidence) < 3
  ) {
    return ROLE_EVIDENCE_TIER.D_WEAK;
  }
  if (
    PRESS_URL_RE.test(sourceUrl) &&
    strengthRank(matrix.eventRelationEvidence) < 3 &&
    strengthRank(matrix.roleEvidence) < 3
  ) {
    return ROLE_EVIDENCE_TIER.D_WEAK;
  }

  if (
    strengthRank(matrix.roleEvidence) >= 3 &&
    strengthRank(matrix.eventRelationEvidence) >= 3 &&
    strengthRank(matrix.identityEvidence) >= 2
  ) {
    return ROLE_EVIDENCE_TIER.A_DIRECT_EVENT;
  }

  if (
    candidate.onOfficialDomain &&
    strengthRank(matrix.roleEvidence) >= 3 &&
    strengthRank(matrix.eventRelationEvidence) >= 2 &&
    !PRESS_URL_RE.test(sourceUrl)
  ) {
    return ROLE_EVIDENCE_TIER.B_ORG_FUNCTION;
  }

  // Association ED / Member Services / Partnerships on official non-press page
  if (
    candidate.onOfficialDomain &&
    !PRESS_URL_RE.test(sourceUrl) &&
    /\b(executive director|member services|partnerships? and events|audience engagement|director of programs)\b/i.test(
      role
    ) &&
    /ASSOCIATION|CONFERENCE|UNKNOWN|SPORTS/i.test(String(ctx.eventFamily || ""))
  ) {
    return ROLE_EVIDENCE_TIER.B_ORG_FUNCTION;
  }

  if (
    MEETINGS_TITLE_RE.test(role) ||
    /PRIMARY_DECISION_MAKER|STRONG_INFLUENCER|OPERATIONAL_CONTACT/.test(
      String(candidate.roleRelevance || "")
    )
  ) {
    return ROLE_EVIDENCE_TIER.C_STRONG_INFERENCE;
  }

  return ROLE_EVIDENCE_TIER.D_WEAK;
}

/**
 * Hard rejection gates before confirmation.
 */
export function applyRejectionGates(candidate = {}, ctx = {}) {
  const name = String(candidate.name || "").trim();
  if (!name || name.split(/\s+/).length < 2) {
    return { reject: true, gate: REJECTION_GATE.MISSING_IDENTITY };
  }

  const role = String(candidate.role || "");
  const quote = String(candidate.evidenceQuote || "");
  const blob = `${role} ${quote}`.toLowerCase();
  const sourceUrl = String(candidate.sourceUrl || "").toLowerCase();
  const pdfCtx = String(candidate.pdfRoleContext || "");
  const matrix = candidate.evidenceMatrix || buildEvidenceMatrix(candidate, ctx);

  if (
    SPEAKER_RE.test(blob) ||
    /\/speaker\//i.test(sourceUrl) ||
    pdfCtx === "SPEAKER"
  ) {
    if (!MEETINGS_TITLE_RE.test(role) && !/organizer|staff contact|conference director|please .*contact/i.test(blob)) {
      return { reject: true, gate: REJECTION_GATE.SPEAKER_NOT_ORGANIZER };
    }
  }

  if (
    (BOARD_RE.test(blob) || pdfCtx === "BOARD_MEMBER") &&
    !MEETINGS_TITLE_RE.test(role)
  ) {
    return { reject: true, gate: REJECTION_GATE.BOARD_NOT_EVENT_OWNER };
  }

  // President / CEO without meetings function + weak event relation
  if (
    (PRESIDENT_ONLY_RE.test(role.trim()) || /\bceo\b/i.test(role)) &&
    !MEETINGS_TITLE_RE.test(role) &&
    strengthRank(matrix.eventRelationEvidence) < 3
  ) {
    return { reject: true, gate: REJECTION_GATE.PRESIDENT_ONLY };
  }

  // Press-only mention without contact-block / meetings role
  if (
    PRESS_URL_RE.test(sourceUrl) &&
    strengthRank(matrix.eventRelationEvidence) < 3 &&
    !/please .*contact|mailto|tournament contacts/i.test(quote)
  ) {
    return { reject: true, gate: REJECTION_GATE.PRESS_MENTION_ONLY };
  }

  if (
    EXEC_ONLY_RE.test(role) &&
    !MEETINGS_TITLE_RE.test(role) &&
    strengthRank(matrix.eventRelationEvidence) < 3
  ) {
    return { reject: true, gate: REJECTION_GATE.EXECUTIVE_ONLY };
  }

  if (
    /linkedin\.com|facebook\.com|twitter\.com|instagram\.com/i.test(sourceUrl) &&
    !candidate.onOfficialDomain
  ) {
    return { reject: true, gate: REJECTION_GATE.SOCIAL_ONLY_SOURCE };
  }

  if (/leadiq|zoominfo|rocketreach|crunchbase/i.test(sourceUrl)) {
    return { reject: true, gate: REJECTION_GATE.AGGREGATOR_SOURCE };
  }

  if (
    candidate.domainClass === "VENUE_OFFICIAL" ||
    /marriott|hilton|hyatt|hotel\.com/i.test(sourceUrl)
  ) {
    return { reject: true, gate: REJECTION_GATE.VENUE_STAFF };
  }

  const org = String(candidate.organization || "").toLowerCase();
  const expected = String(ctx.organization || ctx.opportunityName || "").toLowerCase();
  if (
    org &&
    expected &&
    candidate.onOfficialDomain === false &&
    !looseOrgMatch(org, expected) &&
    candidate.domainClass === "UNKNOWN"
  ) {
    return { reject: true, gate: REJECTION_GATE.WRONG_ORGANIZATION };
  }

  // Speaker/sponsor company staff on buyer-side conference pages
  if (
    /mastercard|state street|sponsor|panelist/i.test(`${org} ${role} ${quote}`) &&
    !looseOrgMatch(org, expected) &&
    !candidate.onOfficialDomain
  ) {
    return { reject: true, gate: REJECTION_GATE.PARTNER_NOT_OWNER };
  }

  if (
    candidate.roleRelevance === "WEAK_RELEVANCE" &&
    candidate.evidenceTier === ROLE_EVIDENCE_TIER.D_WEAK
  ) {
    return { reject: true, gate: REJECTION_GATE.WEAK_RELEVANCE };
  }

  if (candidate.currentness === "HISTORICAL") {
    return { reject: true, gate: REJECTION_GATE.OUTDATED_PERSON };
  }

  return { reject: false, gate: null };
}

function looseOrgMatch(a, b) {
  const tokens = (s) =>
    s
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((t) => t.length >= 4);
  const ta = tokens(a);
  const tb = tokens(b);
  return ta.some((t) => tb.includes(t)) || tb.some((t) => ta.includes(t));
}

/**
 * Confirmation: A/B may confirm; C needs validation; D reject.
 * Requires role + event-relation dimensions, not source-only.
 */
export function confirmWhoFromEvidence(candidate = {}) {
  const tier = candidate.evidenceTier || ROLE_EVIDENCE_TIER.D_WEAK;
  const matrix = candidate.evidenceMatrix || {};

  if (tier === ROLE_EVIDENCE_TIER.D_WEAK) {
    return { researchState: "REJECTED", reason: "tier_d_weak" };
  }

  if (tier === ROLE_EVIDENCE_TIER.C_STRONG_INFERENCE) {
    return {
      researchState: "NAMED_PERSON_NEEDS_VALIDATION",
      reason: "tier_c_needs_corroboration",
    };
  }

  const roleOk = strengthRank(matrix.roleEvidence || "WEAK") >= 2;
  const eventOk = strengthRank(matrix.eventRelationEvidence || "WEAK") >= 2;
  const idOk = strengthRank(matrix.identityEvidence || "WEAK") >= 2;

  if (
    (tier === ROLE_EVIDENCE_TIER.A_DIRECT_EVENT ||
      tier === ROLE_EVIDENCE_TIER.B_ORG_FUNCTION) &&
    roleOk &&
    eventOk &&
    idOk
  ) {
    return {
      researchState: "NAMED_PERSON_CONFIRMED",
      reason:
        tier === ROLE_EVIDENCE_TIER.A_DIRECT_EVENT
          ? "tier_a_role_and_event_relation"
          : "tier_b_org_function",
    };
  }

  if (tier === ROLE_EVIDENCE_TIER.A_DIRECT_EVENT || tier === ROLE_EVIDENCE_TIER.B_ORG_FUNCTION) {
    return {
      researchState: "NAMED_PERSON_NEEDS_VALIDATION",
      reason: "tier_ab_incomplete_evidence_matrix",
    };
  }

  return {
    researchState: "NAMED_PERSON_NEEDS_VALIDATION",
    reason: "default_validation",
  };
}

/**
 * Rank: direct event responsibility > meetings/partnerships > housing >
 * registration > ops > executive sponsor > generic.
 * Seniority alone does not win.
 */
export function rankWhoCandidates(a, b) {
  const score = (c) => {
    const tier =
      c.evidenceTier === "A" ? 100 : c.evidenceTier === "B" ? 70 : c.evidenceTier === "C" ? 35 : 0;
    const role = String(c.gdiContactRole || "");
    const title = String(c.role || "");
    let rolePts = 0;
    if (/PARTNERSHIPS_OWNER|MEETINGS_OWNER|CONFERENCE_DIRECTOR|EVENT_OWNER/.test(role)) {
      rolePts = 28;
    } else if (/HOUSING_OWNER/.test(role)) rolePts = 22;
    else if (/REGISTRATION_OWNER|PROGRAM_OWNER|SPONSORSHIP_OWNER|SALES_OWNER/.test(role)) {
      rolePts = 16;
    } else if (/EXECUTIVE_SPONSOR/.test(role)) rolePts = 6;

    // Title boost for partnerships/events / tournament director / member services
    if (/partnerships?|audience engagement|member services|tournament director|director of programs/i.test(title)) {
      rolePts += 8;
    }
    // Penalize president/ceo
    if (/^(president|ceo)\b/i.test(title.trim())) rolePts -= 40;

    const matrix = c.evidenceMatrix || {};
    const matrixPts =
      strengthRank(matrix.eventRelationEvidence) * 4 +
      strengthRank(matrix.roleEvidence) * 4 -
      (String(c.sourceUrl || "").match(PRESS_URL_RE) ? 15 : 0);

    const reach = (c.email ? 6 : 0) + (c.phone ? 4 : 0);
    const official = c.onOfficialDomain ? 8 : 0;
    return tier + rolePts + matrixPts + reach + official;
  };
  return score(b) - score(a);
}

/**
 * Structured confidence from evidence matrix.
 */
export function buildConfidenceComponents(candidate = {}) {
  const matrix = candidate.evidenceMatrix || buildEvidenceMatrix(candidate);
  const map = (s) => (s === "STRONG" ? 0.9 : s === "MEDIUM" ? 0.65 : 0.3);
  return {
    identityConfidence: map(matrix.identityEvidence),
    roleConfidence: map(matrix.roleEvidence),
    eventRelationConfidence: map(matrix.eventRelationEvidence),
    currentnessConfidence: map(matrix.currentnessEvidence),
    sourceAuthority: map(matrix.sourceAuthority),
    evidenceMatrix: matrix,
  };
}

/**
 * Role-family coverage for stop rule (V4).
 */
export function roleCoverageSatisfied(people = [], eventFamily = "") {
  const roles = new Set(people.map((p) => p.gdiContactRole).filter(Boolean));
  const titles = people.map((p) => String(p.role || "").toLowerCase()).join(" ");

  if (/SPORTS/i.test(eventFamily)) {
    const hasTournament =
      roles.has("EVENT_OWNER") ||
      roles.has("HOUSING_OWNER") ||
      /tournament director|executive director|doubles coordinator/i.test(titles);
    // Prefer ≥2 leadership names when staff block implies multi-person
    const multi = people.filter((p) => p.gateOk || p.researchState?.includes("NAMED")).length >= 2;
    return hasTournament && (multi || people.length >= 2);
  }

  if (/ASSOCIATION|CONFERENCE|UNKNOWN/i.test(eventFamily)) {
    return (
      roles.has("MEETINGS_OWNER") ||
      roles.has("CONFERENCE_DIRECTOR") ||
      roles.has("PARTNERSHIPS_OWNER") ||
      roles.has("EVENT_OWNER") ||
      /member services|partnerships|audience engagement|director of programs|executive director/i.test(
        titles
      )
    );
  }

  return people.some((p) => p.evidenceTier === "A" || p.evidenceTier === "B");
}
