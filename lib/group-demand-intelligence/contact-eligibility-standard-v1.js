/**
 * GDI 2.0 contact eligibility standard — Scaling Companies FullE2E philosophy.
 *
 * Primary objective: identify a verified named person who can help the hotel
 * sales team pursue the opportunity.
 *
 * Hierarchy:
 *   1. DIRECT_BUYER — hotel / travel / meetings / lodging / purchasing remit.
 *   2. RELEVANT_COMMERCIAL_ENTRY_POINT — named + affiliated + commercially
 *      relevant connection to the demand (explicit assignment OR reliable
 *      contextual role↔activity-type alignment on a specific org activity).
 *      Direct booking authority is NOT required. Role/title alone is NEVER enough.
 *   3. RESEARCH_CANDIDATE — plausible role, insufficient commercial evidence;
 *      preserve for further research (not sales-ready).
 *   Not eligible — unrelated / speculative contacts.
 *
 * Scaling parity note: original Scaling accepted ops/project/training/local
 * leadership via role relevance without requiring the person be named in the
 * announcement. GDI 2.0 preserves that via DEMAND_TYPE_ALIGNED contextual
 * connection when the opportunity has a specific demand activity. It does NOT
 * re-open bare CEO / empty-role soft-pass from Scaling publication gates.
 *
 * Explicitly NOT required: direct hotel-booking authority, RFP, room block,
 * second named contact, person named in the press release.
 *
 * Does not mutate hotelFacingEligible, customer visibility, or canonical records.
 */

import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import {
  classifyHotelCommercialRemit,
  cleanBuyerRoleTitle,
  REMIT_CLASS,
} from "./hotel-commercial-responsibility-v1.js";

export const CONTACT_ELIGIBILITY_STANDARD_V1 = "gdi-contact-eligibility-standard-v1";

export const CONTACT_ELIGIBILITY_CLASS = Object.freeze({
  DIRECT_BUYER: "DIRECT_BUYER",
  RELEVANT_COMMERCIAL_ENTRY_POINT: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  RESEARCH_CANDIDATE: "RESEARCH_CANDIDATE",
  INELIGIBLE_IRRELEVANT: "INELIGIBLE_IRRELEVANT",
  UNRELATED_CONTACT: "UNRELATED_CONTACT",
  INELIGIBLE_LEADERSHIP_WITHOUT_DEMAND_LINK: "INELIGIBLE_LEADERSHIP_WITHOUT_DEMAND_LINK",
  /** @deprecated Prefer RESEARCH_CANDIDATE — kept for backward-compatible reason codes */
  INELIGIBLE_ENTRY_POINT_WITHOUT_ACTIVITY_LINK: "INELIGIBLE_ENTRY_POINT_WITHOUT_ACTIVITY_LINK",
  INELIGIBLE_AFFILIATION_UNSUPPORTED: "INELIGIBLE_AFFILIATION_UNSUPPORTED",
  INELIGIBLE_UNSPECIFIED: "INELIGIBLE_UNSPECIFIED",
  INELIGIBLE_NOT_A_PERSON: "INELIGIBLE_NOT_A_PERSON",
});

/** How ENTRY_POINT commercial relevance was established. */
export const CONTACT_CONNECTION_BASIS = Object.freeze({
  EXPLICIT_ACTIVITY_ASSIGNMENT: "EXPLICIT_ACTIVITY_ASSIGNMENT",
  DEMAND_TYPE_ALIGNED_CONTEXTUAL: "DEMAND_TYPE_ALIGNED_CONTEXTUAL",
  NONE: "NONE",
});

export const CONTACT_ELIGIBILITY_LABEL = Object.freeze({
  [CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER]:
    "Direct buyer — hotel / travel / meetings / lodging / purchasing responsibility",
  [CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT]:
    "Relevant commercial entry point — credible approach or referral path to the buyer",
  [CONTACT_ELIGIBILITY_CLASS.RESEARCH_CANDIDATE]:
    "Research candidate — potentially useful role; commercial relevance not yet established",
  [CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_IRRELEVANT]:
    "Not commercially eligible — unrelated role for hotel sales outreach",
  [CONTACT_ELIGIBILITY_CLASS.UNRELATED_CONTACT]:
    "Unrelated contact — no credible commercial relationship to this opportunity",
  [CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_LEADERSHIP_WITHOUT_DEMAND_LINK]:
    "Leadership title without evidenced link to this demand activity",
  [CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_ENTRY_POINT_WITHOUT_ACTIVITY_LINK]:
    "Plausible entry-point role without evidenced link to this specific demand activity",
  [CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_AFFILIATION_UNSUPPORTED]:
    "Named person without supported organizational affiliation for this opportunity",
  [CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_UNSPECIFIED]:
    "Named person identified — commercial role not yet verified",
  [CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_NOT_A_PERSON]:
    "Not a verified named person",
});

/** Client-facing short labels for opportunity briefs. */
export const CONTACT_BRIEF_LABEL = Object.freeze({
  [CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER]: "Direct buyer",
  [CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT]:
    "Relevant commercial entry point (intermediary / referral path)",
});

export const CONTACT_PRIORITY = Object.freeze({
  [CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER]: 1,
  [CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT]: 2,
  [CONTACT_ELIGIBILITY_CLASS.RESEARCH_CANDIDATE]: 3,
});

/** Explicit non-requirements for contact eligibility. */
export const CONTACT_ELIGIBILITY_NOT_REQUIRED = Object.freeze([
  "direct_hotel_booking_authority",
  "formal_rfp",
  "confirmed_room_block",
  "confirmed_accommodation",
  "second_named_contact",
  "person_named_in_public_announcement",
  "person_direct_email_when_org_route_reaches_named_person",
]);

const BANNED_IRRELEVANT_RE =
  /\b(content strategy|content marketing|product\b|engineering|software|sales engineer|account executive\b|customer success|recruiter|recruiting|talent acquisition|people partner|hr business|media|press|pr\b|investor relations|speaker|keynote|panelist|communications|marketing manager|brand manager|journalist|reporter|cmo|chief marketing|cto|chief technology|chief product)\b/i;

/** Roles that may be entry points when tied to a specific demand activity. */
const ENTRY_POINT_ROLE_RE =
  /\b(project (?:operations |ops )?manager|program manager|deployment|mobilization|implementation|site lead|branch manager|office lead|office manager|chief of staff|people ops|human resources|hr\b|training|learning|development|l&d|onboarding|construction manager|engineering manager|field (?:service|ops)|commissioning|facilities project|temporary relocation|workforce|local (?:lead|manager|director)|general manager|operations (?:manager|director|lead)|ops manager|ops director)\b/i;

const JOB_TITLE_SHAPE_RE =
  /\b(director|manager|coordinator|planner|officer|head|lead|vp|vice president|gerente|directora|responsable|president|ceo|coo|chief)\b/i;

const BARE_EXEC_RE =
  /\b(ceo|chief executive|founder|co-?founder|chairman|president|cfo|chro|cto|cmo|cso)\b/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normOrg(s) {
  return text(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

/**
 * Supported organizational affiliation for the named person vs the opportunity org.
 * Soft-accept when opportunity org is present and person org is unset (common public pages),
 * unless opts.requirePersonOrganization is true. Reject explicit employer mismatches.
 */
export function assessOrganizationalAffiliation(person = {}, opportunity = {}, opts = {}) {
  const opportunityOrg = text(
    opportunity.organizationName ||
      opportunity.company ||
      opportunity.organizer ||
      "",
  );
  const personOrg = text(
    person.organization ||
      person.currentOrganization ||
      person.employer ||
      opportunity.primaryContact?.organization ||
      "",
  );

  if (!opportunityOrg && !personOrg) {
    return {
      ok: false,
      reason: "organization_affiliation_missing",
      opportunityOrg: null,
      personOrg: null,
    };
  }

  if (personOrg && opportunityOrg) {
    const a = normOrg(personOrg);
    const b = normOrg(opportunityOrg);
    const overlap =
      a.includes(b) ||
      b.includes(a) ||
      opportunityOrg
        .toLowerCase()
        .split(/[^a-z0-9]+/)
        .filter((t) => t.length >= 4)
        .some((t) => a.includes(t));
    if (!overlap) {
      return {
        ok: false,
        reason: "employer_affiliation_mismatch",
        opportunityOrg,
        personOrg,
      };
    }
  }

  if (opts.requirePersonOrganization === true && !personOrg) {
    return {
      ok: false,
      reason: "person_organization_required_but_missing",
      opportunityOrg: opportunityOrg || null,
      personOrg: null,
    };
  }

  if (
    opts.affiliationVerified === false ||
    /employer_affiliation_mismatch/i.test(String(opts.affiliationReason || ""))
  ) {
    return {
      ok: false,
      reason: "affiliation_explicitly_unverified",
      opportunityOrg: opportunityOrg || null,
      personOrg: personOrg || null,
    };
  }

  return {
    ok: Boolean(opportunityOrg || personOrg),
    reason: personOrg
      ? "person_and_opportunity_organization_aligned"
      : "opportunity_organization_supported_person_org_unspecified",
    opportunityOrg: opportunityOrg || null,
    personOrg: personOrg || null,
  };
}

/**
 * Evidence that the named person is connected to THIS demand-generating activity
 * (project, meeting program, office opening, event) — not merely employed by the org
 * and not merely holding a plausible job title.
 *
 * Role-only matching is NEVER sufficient (`roleOnly` is recorded but does not set
 * `activityLinked`).
 */
export function extractDemandActivityConnection(person = {}, opportunity = {}) {
  const blobs = [
    person.relationshipToOpportunity,
    person.demandActivityConnection,
    person.demandActivityRole,
    person.activityRole,
    person.projectRole,
    person.connectionEvidence,
    opportunity.contactDemandConnectionEvidence,
    opportunity.primaryContact?.relationshipToOpportunity,
    opportunity.primaryContact?.demandActivityConnection,
    opportunity.salesBrief?.contactRelationshipToOpportunity,
  ]
    .map(text)
    .filter(Boolean);

  // relevantResponsibility alone is often just a restated title — only count when
  // it clearly references the activity beyond a job title phrase.
  const relevantResponsibility = text(person.relevantResponsibility || "");
  if (
    relevantResponsibility &&
    /\b(for|on|leading|owns|managing|overseeing|responsible for)\b/i.test(
      relevantResponsibility,
    )
  ) {
    blobs.push(relevantResponsibility);
  }

  const role = cleanBuyerRoleTitle(
    person.role || person.title || opportunity.contactRole || "",
  );

  const specificActivityBlob = text(
    [
      opportunity.activityHeadline,
      opportunity.whatIsHappening,
      opportunity.eventName,
      opportunity.projectName,
      opportunity.potentialHotelBusiness,
      opportunity.hotelDemandThesis,
      opportunity.proposedHotelUse,
    ].join(" "),
  );

  const explicit = blobs.length > 0;
  const roleSignalsDemand =
    Boolean(role) &&
    (ENTRY_POINT_ROLE_RE.test(role) ||
      /\b(meeting|event|travel|hotel|lodging|housing|procurement|workplace|facilities)\b/i.test(
        role,
      ));

  const activityIsProject =
    /\b(project|deployment|mobilization|construction|implementation|commissioning|field|cutover|erp|install)\b/i.test(
      specificActivityBlob,
    );
  const activityIsMeetingTraining =
    /\b(meeting|meetings|training|event|congress|conference|offsite|workshop|kickoff)\b/i.test(
      specificActivityBlob,
    );
  const activityIsOfficeOpening =
    /\b(office|opening|expansion|workplace|new (?:site|facility|location)|branch)\b/i.test(
      specificActivityBlob,
    );

  const hasSpecificActivity =
    activityIsProject || activityIsMeetingTraining || activityIsOfficeOpening;

  const roleMatchesActivity =
    (activityIsProject &&
      /\b(project|deployment|mobilization|implementation|program|site|construction|field|ops|operations)\b/i.test(
        role,
      )) ||
    (activityIsMeetingTraining &&
      /\b(training|learning|l&d|onboarding|meeting|event|program|hr|people)\b/i.test(role)) ||
    (activityIsOfficeOpening &&
      /\b(site|branch|office|workplace|facilities|operations|ops|local|general manager)\b/i.test(
        role,
      ));

  // Role + specific activity type alignment (Scaling-parity contextual path).
  // Does NOT require the person to be named in the public announcement — only that
  // the opportunity has a specific demand activity and the role is functionally
  // aligned to that activity type (project / meeting-training / office opening).
  const demandTypeAligned =
    roleSignalsDemand && hasSpecificActivity && roleMatchesActivity;

  const roleOnly =
    roleSignalsDemand && JOB_TITLE_SHAPE_RE.test(role) && !explicit && !demandTypeAligned;

  const activityLinked = explicit || demandTypeAligned;
  const connectionBasis = explicit
    ? CONTACT_CONNECTION_BASIS.EXPLICIT_ACTIVITY_ASSIGNMENT
    : demandTypeAligned
      ? CONTACT_CONNECTION_BASIS.DEMAND_TYPE_ALIGNED_CONTEXTUAL
      : CONTACT_CONNECTION_BASIS.NONE;

  return {
    explicit,
    roleSignalsDemand,
    demandTypeAligned,
    hasSpecificActivity,
    roleMatchesActivity,
    roleOnly,
    /** Binding: entry points require this — never role classification alone. */
    activityLinked,
    connectionBasis,
    evidenceSnippets: blobs.slice(0, 3),
    role: role || null,
  };
}

function entryPointCapableRole(role, remit) {
  return (
    remit.class === REMIT_CLASS.DEMAND_FUNCTION_RELEVANT ||
    ENTRY_POINT_ROLE_RE.test(role) ||
    (remit.class === REMIT_CLASS.HOTEL_COMMERCIAL_VERIFIED &&
      /\b(project|deployment|mobilization|implementation|commissioning|program manager|site lead|field)\b/i.test(
        role,
      ))
  );
}

function buildEntryPointResult({
  name,
  role,
  remit,
  demandConnection,
  affiliation,
  reason,
}) {
  const basis = demandConnection?.connectionBasis || CONTACT_CONNECTION_BASIS.NONE;
  return {
    version: CONTACT_ELIGIBILITY_STANDARD_V1,
    class: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
    eligible: true,
    salesReadyContactOk: true,
    priority: CONTACT_PRIORITY[CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT],
    label: CONTACT_ELIGIBILITY_LABEL[CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT],
    briefLabel: CONTACT_BRIEF_LABEL[CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT],
    reason,
    connectionBasis: basis,
    remit,
    demandConnection,
    affiliation,
    name,
    role,
    continueResearchForDirectBuyer: true,
    preserveForResearch: true,
    note:
      basis === CONTACT_CONNECTION_BASIS.DEMAND_TYPE_ALIGNED_CONTEXTUAL
        ? "Entry point via Scaling-parity contextual relevance (role aligned to specific org demand activity type) — person need not be named in the announcement. Route verified separately. Continue researching a direct buyer when justified."
        : "Entry point via explicit demand-activity connection. Outreach route verified separately by the buyer evidence gate. Continue researching a direct buyer when justified.",
  };
}

function buildResearchCandidateResult({
  name,
  role,
  remit,
  demandConnection,
  affiliation,
  reason,
}) {
  return {
    version: CONTACT_ELIGIBILITY_STANDARD_V1,
    class: CONTACT_ELIGIBILITY_CLASS.RESEARCH_CANDIDATE,
    eligible: false,
    salesReadyContactOk: false,
    priority: CONTACT_PRIORITY[CONTACT_ELIGIBILITY_CLASS.RESEARCH_CANDIDATE],
    label: CONTACT_ELIGIBILITY_LABEL[CONTACT_ELIGIBILITY_CLASS.RESEARCH_CANDIDATE],
    briefLabel: null,
    reason,
    connectionBasis: demandConnection?.connectionBasis || CONTACT_CONNECTION_BASIS.NONE,
    remit,
    demandConnection,
    affiliation,
    name,
    role,
    preserveForResearch: true,
    continueResearchForDirectBuyer: true,
    note:
      "Plausible commercial role preserved for further research — not yet sales-ready without a stronger demand connection or remit verification.",
  };
}

/**
 * Classify a verified named contact for sales-ready eligibility.
 */
export function classifyContactEligibility(person = {}, opportunity = {}, opts = {}) {
  const name = text(person.name || person.person || "");
  if (!name || !looksLikePersonName(name)) {
    return {
      version: CONTACT_ELIGIBILITY_STANDARD_V1,
      class: CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_NOT_A_PERSON,
      eligible: false,
      salesReadyContactOk: false,
      priority: null,
      label: CONTACT_ELIGIBILITY_LABEL[CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_NOT_A_PERSON],
      briefLabel: null,
      reason: "not_a_person_name",
      remit: null,
      demandConnection: null,
      affiliation: null,
    };
  }

  const role = cleanBuyerRoleTitle(
    person.role ||
      person.title ||
      person.relevantResponsibility ||
      opportunity.contactRole ||
      opportunity.buyerRole ||
      "",
  );
  if (!role) {
    return {
      version: CONTACT_ELIGIBILITY_STANDARD_V1,
      class: CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_UNSPECIFIED,
      eligible: false,
      salesReadyContactOk: false,
      priority: null,
      label: CONTACT_ELIGIBILITY_LABEL[CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_UNSPECIFIED],
      briefLabel: null,
      reason: "role_unspecified",
      remit: null,
      demandConnection: null,
      affiliation: null,
      name,
      role: null,
    };
  }

  const affiliation = assessOrganizationalAffiliation(person, opportunity, opts);
  const demandConnection = extractDemandActivityConnection({ ...person, role }, opportunity);
  const remit = classifyHotelCommercialRemit(role, opts);

  if (BANNED_IRRELEVANT_RE.test(role) && !/\b(travel|meeting|event|hotel|lodging|procurement|workplace|facilities)\b/i.test(role)) {
    return {
      version: CONTACT_ELIGIBILITY_STANDARD_V1,
      class: CONTACT_ELIGIBILITY_CLASS.UNRELATED_CONTACT,
      eligible: false,
      salesReadyContactOk: false,
      priority: null,
      label: CONTACT_ELIGIBILITY_LABEL[CONTACT_ELIGIBILITY_CLASS.UNRELATED_CONTACT],
      briefLabel: null,
      reason: "unrelated_role_speaker_marketing_recruiting_or_non_commercial",
      legacyClass: CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_IRRELEVANT,
      remit,
      demandConnection,
      affiliation,
      name,
      role,
      preserveForResearch: false,
    };
  }

  if (!affiliation.ok) {
    return {
      version: CONTACT_ELIGIBILITY_STANDARD_V1,
      class: CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_AFFILIATION_UNSUPPORTED,
      eligible: false,
      salesReadyContactOk: false,
      priority: null,
      label: CONTACT_ELIGIBILITY_LABEL[CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_AFFILIATION_UNSUPPORTED],
      briefLabel: null,
      reason: affiliation.reason,
      remit,
      demandConnection,
      affiliation,
      name,
      role,
    };
  }

  const directBuyerSignal =
    /\b(travel|meeting|meetings|event|events|hotel|lodging|housing|procurement|purchasing|sourcing|workplace|facilities|planner|planning|catering|hospedaje|alojamiento|congreso|convention|congress|venue|real estate)\b/i.test(
      role,
    );
  const projectishEntrySignal =
    /\b(project|deployment|mobilization|implementation|commissioning|program manager|site lead|field (?:service|ops)|temporary relocation)\b/i.test(
      role,
    );

  // DIRECT_BUYER: hotel / travel / meetings / lodging / purchasing remit + affiliation.
  // Project/deployment titles that only matched via "operations manager" are entry-point
  // candidates and still require activity linkage (never role-alone).
  if (remit.class === REMIT_CLASS.HOTEL_COMMERCIAL_VERIFIED && !(projectishEntrySignal && !directBuyerSignal)) {
    return {
      version: CONTACT_ELIGIBILITY_STANDARD_V1,
      class: CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER,
      eligible: true,
      salesReadyContactOk: true,
      priority: CONTACT_PRIORITY[CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER],
      label: CONTACT_ELIGIBILITY_LABEL[CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER],
      briefLabel: CONTACT_BRIEF_LABEL[CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER],
      reason: "direct_buyer_hotel_commercial_remit",
      remit,
      demandConnection,
      affiliation,
      name,
      role,
      continueResearchForDirectBuyer: false,
    };
  }

  // ENTRY_POINT candidates — need commercial connection beyond title alone.
  // Connection may be EXPLICIT or DEMAND_TYPE_ALIGNED_CONTEXTUAL (Scaling parity:
  // person need not appear in the announcement when role aligns to activity type).
  if (entryPointCapableRole(role, remit) || (projectishEntrySignal && JOB_TITLE_SHAPE_RE.test(role))) {
    if (!demandConnection.activityLinked) {
      return buildResearchCandidateResult({
        name,
        role,
        remit,
        demandConnection,
        affiliation,
        reason: demandConnection.roleOnly
          ? "entry_point_role_alone_insufficient_without_activity_connection"
          : "entry_point_missing_evidenced_demand_activity_connection",
      });
    }

    return buildEntryPointResult({
      name,
      role,
      remit,
      demandConnection,
      affiliation,
      reason: demandConnection.explicit
        ? "explicit_demand_activity_assignment"
        : "demand_type_aligned_contextual_relevance",
    });
  }

  // Explicit relationship evidence + non-banned title (not bare CEO)
  if (
    demandConnection.explicit &&
    JOB_TITLE_SHAPE_RE.test(role) &&
    !BARE_EXEC_RE.test(role) &&
    remit.class !== REMIT_CLASS.NON_HOTEL_COMMERCIAL &&
    remit.class !== REMIT_CLASS.ROLE_SPAM
  ) {
    return buildEntryPointResult({
      name,
      role,
      remit,
      demandConnection,
      affiliation,
      reason: "evidenced_demand_activity_connection",
    });
  }

  if (remit.class === REMIT_CLASS.LEADERSHIP_ONLY || BARE_EXEC_RE.test(role)) {
    return {
      version: CONTACT_ELIGIBILITY_STANDARD_V1,
      class: CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_LEADERSHIP_WITHOUT_DEMAND_LINK,
      eligible: false,
      salesReadyContactOk: false,
      priority: null,
      label:
        CONTACT_ELIGIBILITY_LABEL[
          CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_LEADERSHIP_WITHOUT_DEMAND_LINK
        ],
      briefLabel: null,
      reason: "leadership_without_evidenced_demand_activity_connection",
      remit,
      demandConnection,
      affiliation,
      name,
      role,
      note: "Arbitrary executives are not sales-ready contacts without an evidenced link to this opportunity.",
    };
  }

  if (remit.class === REMIT_CLASS.NON_HOTEL_COMMERCIAL || remit.class === REMIT_CLASS.ROLE_SPAM) {
    return {
      version: CONTACT_ELIGIBILITY_STANDARD_V1,
      class: CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_IRRELEVANT,
      eligible: false,
      salesReadyContactOk: false,
      priority: null,
      label: CONTACT_ELIGIBILITY_LABEL[CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_IRRELEVANT],
      briefLabel: null,
      reason: remit.reason || "role_not_commercially_relevant",
      remit,
      demandConnection,
      affiliation,
      name,
      role,
    };
  }

  return {
    version: CONTACT_ELIGIBILITY_STANDARD_V1,
    class: CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_UNSPECIFIED,
    eligible: false,
    salesReadyContactOk: false,
    priority: null,
    label: CONTACT_ELIGIBILITY_LABEL[CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_UNSPECIFIED],
    briefLabel: null,
    reason: "commercial_connection_not_yet_supported",
    remit,
    demandConnection,
    affiliation,
    name,
    role,
  };
}

/**
 * Prefer DIRECT_BUYER over ENTRY_POINT when ranking multiple candidates.
 */
export function rankContactsByEligibility(contacts = [], opportunity = {}) {
  return [...contacts]
    .map((c) => ({
      contact: c,
      eligibility: classifyContactEligibility(c, opportunity),
    }))
    .filter((r) => r.eligibility.salesReadyContactOk)
    .sort(
      (a, b) =>
        (a.eligibility.priority || 99) - (b.eligibility.priority || 99) ||
        text(a.contact.name).localeCompare(text(b.contact.name)),
    );
}

/**
 * Brief fields for client-facing opportunity presentation.
 * Distinguishes direct buyers from relevant intermediaries.
 */
export function buildContactEligibilityBriefFields(eligibility = {}) {
  if (!eligibility?.salesReadyContactOk) {
    return {
      contactEligibilityClass: eligibility?.class || null,
      contactEligibilityLabel: eligibility?.label || null,
      contactTypeForBrief: null,
      isDirectBuyer: false,
      isRelevantCommercialEntryPoint: false,
      continueResearchForDirectBuyer: false,
    };
  }
  return {
    contactEligibilityClass: eligibility.class,
    contactEligibilityLabel: eligibility.label,
    contactTypeForBrief: eligibility.briefLabel,
    isDirectBuyer: eligibility.class === CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER,
    isRelevantCommercialEntryPoint:
      eligibility.class === CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
    continueResearchForDirectBuyer: eligibility.continueResearchForDirectBuyer === true,
  };
}
