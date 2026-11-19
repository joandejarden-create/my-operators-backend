/**
 * Property → person decision-maker ranking (Hotel Explorer).
 * Free-source research first; Surfe is user-initiated fallback only.
 *
 * Version: property-decision-maker-engine-v1
 */

export const PROPERTY_DECISION_MAKER_ENGINE_VERSION = "property-decision-maker-engine-v1";

/** Technical contact classes (UI maps to professional labels). */
export const CONTACT_CLASS = Object.freeze({
  PRIMARY_PROPERTY_DECISION_MAKER: "PRIMARY_PROPERTY_DECISION_MAKER",
  OWNER_DECISION_AUTHORITY: "OWNER_DECISION_AUTHORITY",
  RELEVANT_COMMERCIAL_ENTRY_POINT: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  CORPORATE_FALLBACK: "CORPORATE_FALLBACK",
  ROLE_NOT_VERIFIED: "ROLE_NOT_VERIFIED",
});

export const PROPERTY_RESPONSIBILITY = Object.freeze({
  CONFIRMED: "CONFIRMED",
  INFERRED: "INFERRED",
  UNKNOWN: "UNKNOWN",
});

export const CONTACT_CLASS_LABEL = Object.freeze({
  PRIMARY_PROPERTY_DECISION_MAKER: "Primary property decision-maker",
  OWNER_DECISION_AUTHORITY: "Owner decision authority",
  RELEVANT_COMMERCIAL_ENTRY_POINT: "Relevant commercial entry point",
  CORPORATE_FALLBACK: "Corporate fallback",
  ROLE_NOT_VERIFIED: "Role not verified",
});

export const PROPERTY_RESPONSIBILITY_LABEL = Object.freeze({
  CONFIRMED: "Confirmed for this hotel",
  INFERRED: "Inferred from portfolio / role",
  UNKNOWN: "Property responsibility not confirmed",
});

const CEO_TITLE_RE = /\b(ceo|chief executive|president\s*(and|&)?\s*ceo|chairman)\b/i;
const ASSET_MGMT_RE =
  /\b(asset\s*management|asset\s*manager|portfolio\s*manager|investments?\s*(vp|vice)|vp[, ].*invest)/i;
const OPERATOR_RE = /\b(general\s*manager|hotel\s*manager|director\s*of\s*sales|front\s*office)\b/i;

/** Reject invented first.last@domain style guesses without evidence. */
export function looksLikeInventedEmail(email, personName) {
  const e = String(email || "")
    .trim()
    .toLowerCase();
  if (!e || !e.includes("@")) return false;
  const local = e.split("@")[0];
  const parts = String(personName || "")
    .toLowerCase()
    .replace(/[^a-z\s]/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (parts.length < 2) return false;
  const first = parts[0];
  const last = parts[parts.length - 1];
  const patterns = [
    `${first}.${last}`,
    `${first}${last}`,
    `${first[0]}${last}`,
    `${first}_${last}`,
  ];
  return patterns.includes(local);
}

export function isCorporateChannelMislabeledAsDirect(channel = {}) {
  const classification = String(channel.classification || channel.type || "").toLowerCase();
  const value = String(channel.value || "").toLowerCase();
  const subject = String(channel.subject_type || channel.subjectType || "").toLowerCase();
  const orgish =
    /official_org|org_general|organization|switchboard|info@|contact@|hello@|reception@/.test(
      classification + " " + value + " " + subject
    );
  const claimedDirect = /direct|personal|person_specific|individual/.test(classification);
  return orgish && claimedDirect;
}

/**
 * Score factors (documented, not opaque 0–100 confidence).
 * Higher = more appropriate as outreach candidate for this hotel.
 */
export function scoreContactForHotel(person = {}, hotelContext = {}) {
  const factors = [];
  let score = 0;
  const title = String(person.title || person.role || "");
  const responsibility =
    person.propertyResponsibility ||
    person.property_responsibility ||
    PROPERTY_RESPONSIBILITY.UNKNOWN;
  const contactClass = person.contactClass || person.contact_class || null;

  if (responsibility === PROPERTY_RESPONSIBILITY.CONFIRMED) {
    score += 40;
    factors.push({ factor: "verified_hotel_responsibility", weight: "very_high", points: 40 });
  } else if (responsibility === PROPERTY_RESPONSIBILITY.INFERRED) {
    score += 22;
    factors.push({ factor: "inferred_portfolio_responsibility", weight: "high", points: 22 });
  } else {
    factors.push({ factor: "property_responsibility_unknown", weight: "n/a", points: 0 });
  }

  if (ASSET_MGMT_RE.test(title) || /asset management/i.test(String(person.role_group || ""))) {
    score += 28;
    factors.push({ factor: "asset_management_authority", weight: "very_high", points: 28 });
  }
  if (CEO_TITLE_RE.test(title) || /ownership|decision/i.test(String(person.role_group || ""))) {
    score += 18;
    factors.push({ factor: "ownership_decision_authority", weight: "very_high", points: 18 });
  }
  if (OPERATOR_RE.test(title)) {
    score += 8;
    factors.push({ factor: "operator_side_role", weight: "medium", points: 8 });
  }

  if (person.hotelEvidenceCount > 0 || (person.hotelEvidenceIds || []).length > 0) {
    score += 20;
    factors.push({ factor: "hotel_specific_evidence", weight: "very_high", points: 20 });
  }
  if (person.portfolioEvidenceCount > 0 || (person.portfolioEvidenceIds || []).length > 0) {
    score += 10;
    factors.push({ factor: "portfolio_evidence", weight: "high", points: 10 });
  }

  const hasLinkedIn = Boolean(
    person.linkedinUrl ||
      (person.profileUrls || []).some((p) => /linkedin/i.test(String(p.type || p.url || "")))
  );
  if (hasLinkedIn) {
    score += 6;
    factors.push({ factor: "verified_linkedin", weight: "high", points: 6 });
  }
  const hasDirectEmail = (person.emailContacts || person.emails || []).some(
    (c) => c && c.value && !isCorporateChannelMislabeledAsDirect(c)
  );
  const hasDirectPhone = (person.phoneContacts || person.phones || []).some(
    (c) => c && c.value && !isCorporateChannelMislabeledAsDirect(c)
  );
  if (hasDirectEmail) {
    score += 8;
    factors.push({ factor: "direct_email", weight: "high", points: 8 });
  }
  if (hasDirectPhone) {
    score += 6;
    factors.push({ factor: "direct_phone", weight: "high", points: 6 });
  }

  // Seniority alone without hotel/portfolio relevance is capped.
  const seniorityOnly =
    CEO_TITLE_RE.test(title) &&
    responsibility === PROPERTY_RESPONSIBILITY.UNKNOWN &&
    !(person.hotelEvidenceCount > 0) &&
    !(person.portfolioEvidenceCount > 0) &&
    !ASSET_MGMT_RE.test(title);
  if (seniorityOnly) {
    score = Math.min(score, 15);
    factors.push({
      factor: "seniority_only_cap",
      weight: "low",
      points: 0,
      note: "CEO/chair without hotel or portfolio evidence must not outrank asset managers",
    });
  }

  if (contactClass === CONTACT_CLASS.ROLE_NOT_VERIFIED) {
    score = Math.min(score, 8);
  }

  return {
    score,
    factors,
    hotelId: hotelContext.hotelId || hotelContext.hpcId || null,
  };
}

export function inferContactClass(person = {}) {
  if (person.contactClass || person.contact_class) {
    return person.contactClass || person.contact_class;
  }
  const responsibility =
    person.propertyResponsibility || person.property_responsibility || PROPERTY_RESPONSIBILITY.UNKNOWN;
  const title = String(person.title || person.role || "");
  if (responsibility === PROPERTY_RESPONSIBILITY.CONFIRMED && ASSET_MGMT_RE.test(title)) {
    return CONTACT_CLASS.PRIMARY_PROPERTY_DECISION_MAKER;
  }
  if (CEO_TITLE_RE.test(title) || /ownership|decision/i.test(String(person.role_group || ""))) {
    return CONTACT_CLASS.OWNER_DECISION_AUTHORITY;
  }
  if (ASSET_MGMT_RE.test(title) || /asset management/i.test(String(person.role_group || ""))) {
    return CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT;
  }
  if (/corporate|fallback|switchboard/i.test(String(person.group || ""))) {
    return CONTACT_CLASS.CORPORATE_FALLBACK;
  }
  return CONTACT_CLASS.ROLE_NOT_VERIFIED;
}

/**
 * Rank people for a hotel. Does not invent property responsibility.
 */
export function rankContactsForHotel(people = [], hotelContext = {}) {
  const ranked = (Array.isArray(people) ? people : []).map((p) => {
    const contactClass = inferContactClass(p);
    const propertyResponsibility =
      p.propertyResponsibility || p.property_responsibility || PROPERTY_RESPONSIBILITY.UNKNOWN;
    const scored = scoreContactForHotel(
      { ...p, contactClass, propertyResponsibility },
      hotelContext
    );
    return {
      ...p,
      contactClass,
      contactClassLabel: CONTACT_CLASS_LABEL[contactClass] || contactClass,
      propertyResponsibility,
      propertyResponsibilityLabel:
        PROPERTY_RESPONSIBILITY_LABEL[propertyResponsibility] || propertyResponsibility,
      ranking: scored,
    };
  });
  ranked.sort((a, b) => (b.ranking?.score || 0) - (a.ranking?.score || 0));
  return ranked;
}

/**
 * Pick recommended first contact with honesty about confirmation.
 * Never auto-selects CEO solely for seniority.
 */
export function selectRecommendedFirstContact(rankedPeople = [], opts = {}) {
  const list = Array.isArray(rankedPeople) ? rankedPeople : [];
  if (!list.length) {
    return {
      recommendationType: "NONE",
      person: null,
      label: null,
      rationale: "No researched owner-side or operator-side contacts available yet.",
      alternatives: [],
    };
  }

  const confirmed = list.find(
    (p) =>
      p.propertyResponsibility === PROPERTY_RESPONSIBILITY.CONFIRMED &&
      (p.contactClass === CONTACT_CLASS.PRIMARY_PROPERTY_DECISION_MAKER ||
        p.contactClass === CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT ||
        ASSET_MGMT_RE.test(String(p.title || "")))
  );
  if (confirmed) {
    return {
      recommendationType: "RECOMMENDED_FIRST_CONTACT",
      person: confirmed,
      label: "Recommended First Contact",
      rationale:
        confirmed.recommendationRationale ||
        confirmed.whyRelevant ||
        "Property-specific decision responsibility is confirmed by evidence.",
      alternatives: list.filter((p) => p !== confirmed).slice(0, 2),
    };
  }

  // Prefer asset-management entry points over CEO when responsibility is not confirmed.
  const preferred =
    list.find(
      (p) =>
        p.propertyResponsibility === PROPERTY_RESPONSIBILITY.INFERRED &&
        (ASSET_MGMT_RE.test(String(p.title || "")) ||
          p.contactClass === CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT ||
          p.contactClass === CONTACT_CLASS.PRIMARY_PROPERTY_DECISION_MAKER)
    ) ||
    list.find((p) => ASSET_MGMT_RE.test(String(p.title || ""))) ||
    list.find((p) => p.contactClass === CONTACT_CLASS.OWNER_DECISION_AUTHORITY) ||
    list[0];

  const forceHonesty = opts.requireConfirmed !== true;
  if (forceHonesty && preferred.propertyResponsibility !== PROPERTY_RESPONSIBILITY.CONFIRMED) {
    return {
      recommendationType: "SUGGESTED_CONTACT_PROPERTY_RESPONSIBILITY_NOT_CONFIRMED",
      person: preferred,
      label: "Suggested Contact — Property Responsibility Not Confirmed",
      rationale:
        preferred.recommendationRationale ||
        preferred.whyRelevant ||
        "Best commercial entry point from role and portfolio evidence; direct assignment to this hotel is not confirmed.",
      alternatives: list.filter((p) => p !== preferred).slice(0, 2),
    };
  }

  return {
    recommendationType: "RECOMMENDED_FIRST_CONTACT",
    person: preferred,
    label: "Recommended First Contact",
    rationale: preferred.recommendationRationale || preferred.whyRelevant || "",
    alternatives: list.filter((p) => p !== preferred).slice(0, 2),
  };
}

export function buildDecisionOrientedContactSections(people = [], hotelContext = {}) {
  const ranked = rankContactsForHotel(people, hotelContext);
  const recommendation = selectRecommendedFirstContact(ranked, hotelContext);
  const recommendedIds = new Set(
    [recommendation.person, ...(recommendation.alternatives || [])]
      .filter(Boolean)
      .map((p) => p.personId || p.name)
      .slice(0, 3)
  );
  const recommended = ranked.filter((p) => recommendedIds.has(p.personId || p.name)).slice(0, 3);
  const others = ranked.filter((p) => !recommendedIds.has(p.personId || p.name));
  return { ranked, recommendation, recommendedContacts: recommended, otherContacts: others };
}

/** Quality gates helpers */
export function assertNoUnsupportedPrimaryClaim(person = {}) {
  if (
    person.contactClass === CONTACT_CLASS.PRIMARY_PROPERTY_DECISION_MAKER &&
    person.propertyResponsibility !== PROPERTY_RESPONSIBILITY.CONFIRMED
  ) {
    return {
      ok: false,
      error: "unsupported_primary_property_claim",
      message: "PRIMARY_PROPERTY_DECISION_MAKER requires CONFIRMED property responsibility",
    };
  }
  return { ok: true };
}

export function detectDuplicateNamedContacts(people = []) {
  const seen = new Map();
  const dupes = [];
  for (const p of people) {
    const key = String(p.name || p.display_name || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, " ");
    if (!key) continue;
    if (seen.has(key)) dupes.push(key);
    else seen.set(key, p);
  }
  return dupes;
}
