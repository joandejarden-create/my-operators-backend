/**
 * GDI Contact Candidate Discovery — role ontology + event-family role priority.
 * Discovery (WHO) is separate from reachability (HOW TO REACH).
 */

/** Canonical role categories for GDI hotel/venue/housing decision influence. */
export const GDI_CONTACT_ROLE = Object.freeze({
  EVENT_OWNER: "EVENT_OWNER",
  MEETINGS_OWNER: "MEETINGS_OWNER",
  CONFERENCE_DIRECTOR: "CONFERENCE_DIRECTOR",
  HOUSING_OWNER: "HOUSING_OWNER",
  REGISTRATION_OWNER: "REGISTRATION_OWNER",
  PROGRAM_OWNER: "PROGRAM_OWNER",
  SPONSORSHIP_OWNER: "SPONSORSHIP_OWNER",
  PARTNERSHIPS_OWNER: "PARTNERSHIPS_OWNER",
  SALES_OWNER: "SALES_OWNER",
  EXECUTIVE_SPONSOR: "EXECUTIVE_SPONSOR",
  GENERAL_ORGANIZATION_CONTACT: "GENERAL_ORGANIZATION_CONTACT",
  UNKNOWN: "UNKNOWN",
});

export const GDI_CONTACT_ROLE_LABEL = Object.freeze({
  EVENT_OWNER: "Event owner",
  MEETINGS_OWNER: "Meetings owner",
  CONFERENCE_DIRECTOR: "Conference director",
  HOUSING_OWNER: "Housing owner",
  REGISTRATION_OWNER: "Registration owner",
  PROGRAM_OWNER: "Program owner",
  SPONSORSHIP_OWNER: "Sponsorship owner",
  PARTNERSHIPS_OWNER: "Partnerships owner",
  SALES_OWNER: "Sales owner",
  EXECUTIVE_SPONSOR: "Executive sponsor",
  GENERAL_ORGANIZATION_CONTACT: "General organization contact",
  UNKNOWN: "Unknown",
});

/** How relevant the role is to hotel / housing / venue decisions. */
export const ROLE_RELEVANCE = Object.freeze({
  PRIMARY_DECISION_MAKER: "PRIMARY_DECISION_MAKER",
  STRONG_INFLUENCER: "STRONG_INFLUENCER",
  OPERATIONAL_CONTACT: "OPERATIONAL_CONTACT",
  BACKUP_CONTACT: "BACKUP_CONTACT",
  WEAK_RELEVANCE: "WEAK_RELEVANCE",
});

export const ROLE_RELEVANCE_LABEL = Object.freeze({
  PRIMARY_DECISION_MAKER: "Primary decision maker",
  STRONG_INFLUENCER: "Strong influencer",
  OPERATIONAL_CONTACT: "Operational contact",
  BACKUP_CONTACT: "Backup contact",
  WEAK_RELEVANCE: "Weak relevance",
});

/** Event-family buckets that drive role priority ladders. */
export const EVENT_FAMILY = Object.freeze({
  ASSOCIATION_CONFERENCE: "ASSOCIATION_CONFERENCE",
  MEDICAL_SCIENTIFIC: "MEDICAL_SCIENTIFIC",
  SPORTS_TOURNAMENT: "SPORTS_TOURNAMENT",
  GOVERNMENT_DEFENSE: "GOVERNMENT_DEFENSE",
  HOUSING_OVERFLOW: "HOUSING_OVERFLOW",
  CORPORATE: "CORPORATE",
  UNIVERSITY: "UNIVERSITY",
  UNKNOWN: "UNKNOWN",
});

/**
 * Ordered role preference per event family (index 0 = most preferred).
 * Best contact = most likely to influence hotel sourcing / room block / housing / venue —
 * not the most senior executive.
 */
export const ROLE_PRIORITY_BY_FAMILY = Object.freeze({
  [EVENT_FAMILY.ASSOCIATION_CONFERENCE]: [
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.REGISTRATION_OWNER,
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
    GDI_CONTACT_ROLE.HOUSING_OWNER,
    GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR,
    GDI_CONTACT_ROLE.PARTNERSHIPS_OWNER,
    GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT,
  ],
  [EVENT_FAMILY.MEDICAL_SCIENTIFIC]: [
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR,
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.REGISTRATION_OWNER,
    GDI_CONTACT_ROLE.HOUSING_OWNER,
    GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR,
    GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT,
  ],
  [EVENT_FAMILY.SPORTS_TOURNAMENT]: [
    GDI_CONTACT_ROLE.HOUSING_OWNER,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR,
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.REGISTRATION_OWNER,
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
    GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR,
    GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT,
  ],
  [EVENT_FAMILY.GOVERNMENT_DEFENSE]: [
    GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.REGISTRATION_OWNER,
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
    GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR,
    GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT,
  ],
  [EVENT_FAMILY.HOUSING_OVERFLOW]: [
    GDI_CONTACT_ROLE.HOUSING_OWNER,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR,
    GDI_CONTACT_ROLE.REGISTRATION_OWNER,
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
    GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR,
    GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT,
  ],
  [EVENT_FAMILY.CORPORATE]: [
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
    GDI_CONTACT_ROLE.PARTNERSHIPS_OWNER,
    GDI_CONTACT_ROLE.SALES_OWNER,
    GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR,
    GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT,
  ],
  [EVENT_FAMILY.UNIVERSITY]: [
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
    GDI_CONTACT_ROLE.REGISTRATION_OWNER,
    GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR,
    GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT,
  ],
  [EVENT_FAMILY.UNKNOWN]: [
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.HOUSING_OWNER,
    GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR,
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
    GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR,
    GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT,
  ],
});

export const CANDIDATE_STATUS = Object.freeze({
  VERIFIED_EVENT_OWNER: "VERIFIED_EVENT_OWNER",
  VERIFIED_ROLE_MATCH: "VERIFIED_ROLE_MATCH",
  STRONG_PROBABLE: "STRONG_PROBABLE",
  PROBABLE: "PROBABLE",
  WEAK: "WEAK",
  STALE: "STALE",
  REJECTED: "REJECTED",
  UNRESOLVED: "UNRESOLVED",
});

export const CANDIDATE_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
  UNRESOLVED: "UNRESOLVED",
});

export const IDENTITY_STATUS = Object.freeze({
  CONFIRMED: "CONFIRMED",
  IDENTITY_UNCONFIRMED: "IDENTITY_UNCONFIRMED",
  STALE: "STALE",
  REJECTED: "REJECTED",
});

/** Map legacy TARGET_ROLE_MATCH → GDI_CONTACT_ROLE. */
export function mapTargetRoleMatchToGdiRole(targetRoleMatch) {
  switch (String(targetRoleMatch || "")) {
    case "DIRECT_DECISION_MAKER":
      return GDI_CONTACT_ROLE.EVENT_OWNER;
    case "EVENT_MEETINGS_OWNER":
      return GDI_CONTACT_ROLE.MEETINGS_OWNER;
    case "HOUSING_SOURCING_CONTACT":
      return GDI_CONTACT_ROLE.HOUSING_OWNER;
    case "EVENT_OPERATIONS_CONTACT":
      return GDI_CONTACT_ROLE.REGISTRATION_OWNER;
    case "ASSOCIATION_MANAGEMENT_CONTACT":
      return GDI_CONTACT_ROLE.MEETINGS_OWNER;
    case "EXECUTIVE_SPONSOR":
      return GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR;
    case "GENERAL_ORGANIZATION_CONTACT":
      return GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT;
    default:
      return GDI_CONTACT_ROLE.UNKNOWN;
  }
}

/**
 * Classify event family from opportunity type + segment.
 * OVERFLOW_HOUSING always wins as housing/overflow family.
 */
export function classifyEventFamily(opportunity = {}) {
  const type = String(opportunity.opportunityType || "");
  if (type === "OVERFLOW_HOUSING") return EVENT_FAMILY.HOUSING_OVERFLOW;

  const signal = String(opportunity.demandSignalType || "").toUpperCase();
  if (signal === "OVERFLOW_HOUSING") return EVENT_FAMILY.HOUSING_OVERFLOW;
  if (
    signal === "CORPORATE_MEETING" ||
    signal === "CORPORATE_RELOCATION" ||
    signal === "INCENTIVE_RETREAT" ||
    signal === "CONSULTING_ADVISORY_TEAM" ||
    signal === "TRAINING_PROGRAM" ||
    signal === "BOARD_COMMITTEE_MEETING"
  ) {
    return EVENT_FAMILY.CORPORATE;
  }
  if (
    signal === "UNIVERSITY_ACADEMIC_PROGRAM" ||
    signal === "SPORTS_ACADEMIC_COMPETITION"
  ) {
    return signal === "SPORTS_ACADEMIC_COMPETITION"
      ? EVENT_FAMILY.SPORTS_TOURNAMENT
      : EVENT_FAMILY.UNIVERSITY;
  }
  if (
    signal === "MEDICAL_HEALTHCARE_PROGRAM"
  ) {
    return EVENT_FAMILY.MEDICAL_SCIENTIFIC;
  }
  if (
    signal === "GOVERNMENT_CONTRACTOR_PROGRAM" ||
    signal === "PROJECT_TEAM" ||
    signal === "PROFESSIONAL_PROJECT_CREW"
  ) {
    return EVENT_FAMILY.GOVERNMENT_DEFENSE;
  }

  const blob = [
    opportunity.segment,
    opportunity.demandType,
    opportunity.demandSignalType,
    opportunity.title,
    opportunity.organizationName,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");

  if (/corporate|workplace|offsite|marriott hq|training program|relocation|incentive|board retreat/i.test(blob)) {
    return EVENT_FAMILY.CORPORATE;
  }
  if (/university|alumni|homecoming|georgetown|college showcase|executive education|faculty/i.test(blob)) {
    return EVENT_FAMILY.UNIVERSITY;
  }
  if (/sport|soccer|tournament|cup|weekend group|usys|msysa|robotics|debate competition/i.test(blob)) {
    return EVENT_FAMILY.SPORTS_TOURNAMENT;
  }
  if (/government|defense|contractor|nist|nih|afcea|federal/i.test(blob)) {
    return EVENT_FAMILY.GOVERNMENT_DEFENSE;
  }
  if (/medical|scientific|healthcare|clinical|biomaterial|dermatolog|translational|bioassay/i.test(blob)) {
    return EVENT_FAMILY.MEDICAL_SCIENTIFIC;
  }
  if (/association|advocacy|realtor|nado|asae|amwa|ahima|cmss|acc\b|ndss/i.test(blob)) {
    return EVENT_FAMILY.ASSOCIATION_CONFERENCE;
  }
  return EVENT_FAMILY.UNKNOWN;
}

/**
 * Infer GDI_CONTACT_ROLE from title/role text (+ optional known match).
 */
export function classifyGdiContactRole(contact = {}, opportunity = {}) {
  if (contact.gdiContactRole && GDI_CONTACT_ROLE[contact.gdiContactRole]) {
    return contact.gdiContactRole;
  }
  if (contact.targetRoleMatch) {
    const mapped = mapTargetRoleMatchToGdiRole(contact.targetRoleMatch);
    // Prefer text inference when it is more specific than EXECUTIVE/GENERAL
    if (
      mapped !== GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR &&
      mapped !== GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT &&
      mapped !== GDI_CONTACT_ROLE.UNKNOWN
    ) {
      return mapped;
    }
  }

  const blob = [
    contact.role,
    contact.title,
    contact.relationshipToEvent,
    contact.relationshipToOpportunity,
    contact.name,
    contact.organization,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");

  if (
    /hbc|housing (partner|provider|manager|coordinator|bureau|program)|stay-to-play|room.?block|lodging|onpeak|passtkey/i.test(
      blob
    )
  ) {
    return GDI_CONTACT_ROLE.HOUSING_OWNER;
  }
  if (/tournament director|event director|event owner|vp.{0,30}summit|co-?vp.{0,30}summit/i.test(blob)) {
    return GDI_CONTACT_ROLE.EVENT_OWNER;
  }
  if (
    /director of meetings|vp.{0,24}meetings|director of events|meetings manager|events manager|meeting planner|meetings & events/i.test(
      blob
    )
  ) {
    return GDI_CONTACT_ROLE.MEETINGS_OWNER;
  }
  if (/conference director|scientific meeting|annual meeting staff|conference manager/i.test(blob)) {
    return GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR;
  }
  if (/registration|registrar|cvent contact|event contact/i.test(blob)) {
    return GDI_CONTACT_ROLE.REGISTRATION_OWNER;
  }
  if (/program (director|manager|owner)|director of nice|education.{0,20}events/i.test(blob)) {
    return GDI_CONTACT_ROLE.PROGRAM_OWNER;
  }
  if (/sponsorship/i.test(blob)) return GDI_CONTACT_ROLE.SPONSORSHIP_OWNER;
  if (/partnership|advancement events/i.test(blob)) return GDI_CONTACT_ROLE.PARTNERSHIPS_OWNER;
  if (/corporate events|employee experience|workplace experience|travel\/meetings/i.test(blob)) {
    return GDI_CONTACT_ROLE.MEETINGS_OWNER;
  }
  if (/executive director|ceo|president\b|executive sponsor/i.test(blob)) {
    return GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR;
  }
  if (/sales|business development/i.test(blob)) return GDI_CONTACT_ROLE.SALES_OWNER;
  if (blob.trim()) return GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT;
  return GDI_CONTACT_ROLE.UNKNOWN;
}

/**
 * Role relevance for this opportunity family — derived from priority ladder position.
 */
export function classifyRoleRelevance(gdiRole, eventFamily) {
  const ladder = ROLE_PRIORITY_BY_FAMILY[eventFamily] || ROLE_PRIORITY_BY_FAMILY[EVENT_FAMILY.UNKNOWN];
  const idx = ladder.indexOf(gdiRole);
  if (idx === 0) return ROLE_RELEVANCE.PRIMARY_DECISION_MAKER;
  if (idx === 1 || idx === 2) return ROLE_RELEVANCE.STRONG_INFLUENCER;
  if (idx >= 3 && idx <= 5) return ROLE_RELEVANCE.OPERATIONAL_CONTACT;
  if (idx > 5) return ROLE_RELEVANCE.BACKUP_CONTACT;
  if (gdiRole === GDI_CONTACT_ROLE.UNKNOWN) return ROLE_RELEVANCE.WEAK_RELEVANCE;
  // Role not on ladder → weak
  return ROLE_RELEVANCE.WEAK_RELEVANCE;
}

/** 0–100 role-priority points from ladder position (for scoring). */
export function rolePriorityScore(gdiRole, eventFamily) {
  const ladder = ROLE_PRIORITY_BY_FAMILY[eventFamily] || ROLE_PRIORITY_BY_FAMILY[EVENT_FAMILY.UNKNOWN];
  const idx = ladder.indexOf(gdiRole);
  if (idx < 0) {
    if (gdiRole === GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT) return 25;
    if (gdiRole === GDI_CONTACT_ROLE.UNKNOWN) return 5;
    return 20;
  }
  const max = ladder.length;
  return Math.round(100 * (1 - idx / Math.max(1, max)));
}

/**
 * Build role-targeted search queries (for audit / future research passes).
 * Structured Pass A–D decomposition. Surfe/PDL must never consume these for WHO.
 */
export function buildCandidateSearchQueries(opportunity = {}) {
  const family = classifyEventFamily(opportunity);
  const eventName = String(opportunity.title || "").replace(/\s+/g, " ").trim();
  const org = String(
    opportunity.organizationName || opportunity.organization?.name || ""
  ).trim();
  const yearMatch = eventName.match(/\b(20\d{2})\b/);
  const year = yearMatch ? yearMatch[1] : "";
  const shortEvent = eventName
    .replace(/\s*[—–-].*$/, "")
    .replace(/\s*\([^)]*\)\s*/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);

  const domainHint = (() => {
    try {
      const url =
        opportunity.primaryContact?.sourceUrl ||
        opportunity.primaryContact?.source ||
        opportunity.sourceUrl;
      if (!url) return null;
      return new URL(url).hostname.replace(/^www\./, "");
    } catch {
      return null;
    }
  })();

  const passA = []; // event-specific
  const passB = []; // organization-specific
  const passC = []; // documents
  const passD = []; // professional identity
  const push = (arr, q) => {
    if (q && !arr.includes(q)) arr.push(q);
  };

  // PASS A — event-specific
  push(passA, `"${shortEvent}" "conference director"`);
  push(passA, `"${shortEvent}" "meetings director"`);
  push(passA, `"${shortEvent}" "events manager"`);
  push(passA, `"${shortEvent}" housing coordinator`);
  push(passA, `"${shortEvent}" registration contact`);
  push(passA, `"${shortEvent}" planning committee`);
  if (year) push(passA, `"${shortEvent}" ${year} staff`);
  if (domainHint) {
    push(passA, `site:${domainHint} conference staff`);
    push(passA, `site:${domainHint} meetings`);
    push(passA, `site:${domainHint} "event director"`);
  }

  // PASS B — organization-specific
  if (org) {
    push(passB, `"${org}" meetings director`);
    push(passB, `"${org}" "director of events"`);
    push(passB, `"${org}" "events manager"`);
    push(passB, `"${org}" staff directory conference`);
    push(passB, `"${org}" convention services`);
    push(passB, `"${org}" membership events staff`);
  }

  // PASS C — documents
  push(passC, `"${shortEvent}" prospectus contact`);
  push(passC, `"${shortEvent}" exhibitor kit housing`);
  push(passC, `"${shortEvent}" sponsor deck "contact"`);
  push(passC, `"${org || shortEvent}" housing PDF`);
  push(passC, `"${shortEvent}" committee roster`);

  // PASS D — professional identity (corroboration only — not sole WHO evidence)
  if (org) {
    push(passD, `"${org}" "director of events" OR "meetings manager"`);
    push(passD, `"${org}" conference leadership announcement`);
  }

  // Family ladders
  if (family === EVENT_FAMILY.HOUSING_OVERFLOW || family === EVENT_FAMILY.SPORTS_TOURNAMENT) {
    push(passA, `"${shortEvent}" tournament director`);
    push(passA, `"${shortEvent}" housing partner`);
    push(passB, `${org || shortEvent} stay-to-play hotels`);
  }
  if (family === EVENT_FAMILY.MEDICAL_SCIENTIFIC || family === EVENT_FAMILY.ASSOCIATION_CONFERENCE) {
    push(passB, `"${org || shortEvent}" director of meetings`);
    push(passC, `"${shortEvent}" exhibitor prospectus contact`);
  }
  if (family === EVENT_FAMILY.GOVERNMENT_DEFENSE) {
    push(passA, `"${shortEvent}" program office contact`);
    push(passB, `"${org || shortEvent}" conference director`);
  }
  if (family === EVENT_FAMILY.CORPORATE) {
    push(passB, `"${org}" meetings and events`);
    push(passB, `"${org}" corporate events manager`);
    push(passB, `"${org}" sourcing procurement travel`);
  }

  const queries = [...passA, ...passB, ...passC, ...passD].slice(0, 20);

  return {
    eventFamily: family,
    queries,
    passes: {
      A_EVENT_SPECIFIC: passA,
      B_ORGANIZATION_SPECIFIC: passB,
      C_DOCUMENTS: passC,
      D_PROFESSIONAL_IDENTITY: passD,
    },
    preferredRoles: ROLE_PRIORITY_BY_FAMILY[family] || ROLE_PRIORITY_BY_FAMILY[EVENT_FAMILY.UNKNOWN],
    methodId: "GDI-CONTACT-CANDIDATE-01",
    whoLaw:
      "Dealality determines WHO from opportunity/org evidence. Providers may not establish WHO.",
  };
}
