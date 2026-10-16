/**
 * Buyer-focused contact enrichment contract V1.
 *
 * Separates:
 * A. Publication contact eligibility (usable route minimum)
 * B. Named-contact research completeness
 * C. Contact responsibility / relevance
 * D. Channel verification and freshness
 *
 * Surfe attribution/credits/costs = INTERNAL ONLY.
 * Named person missing ≠ demote a qualified opportunity with a usable functional route.
 */

export const BUYER_CONTACT_ENRICHMENT_CONTRACT_VERSION =
  "gdi-buyer-contact-enrichment-contract-v1";

export const PUBLICATION_CONTACT_STATUS = Object.freeze({
  USABLE_ROUTE: "USABLE_ROUTE",
  HOMEPAGE_ONLY: "HOMEPAGE_ONLY",
  NO_ROUTE: "NO_ROUTE",
});

export const NAMED_RESEARCH_STATUS = Object.freeze({
  COMPLETE: "NAMED_RESEARCH_COMPLETE",
  /** Named person established but person-specific channel still missing */
  PERSON_ESTABLISHED_CHANNEL_PENDING: "NAMED_PERSON_CHANNEL_PENDING",
  PENDING: "NAMED_BUYER_NOT_YET_ESTABLISHED",
  /** Bounded public research ran; no reliable relevant person — org fallback retained */
  EXHAUSTED_ORG_FALLBACK: "NAMED_RESEARCH_EXHAUSTED_ORG_FALLBACK",
  /** Explicit: research has not been attempted yet (≠ exhausted / no person found) */
  NOT_ATTEMPTED: "NAMED_RESEARCH_NOT_ATTEMPTED",
  WEAK_RELEVANCE: "NAMED_CONTACT_WEAK_RELEVANCE",
  STALE: "NAMED_AFFILIATION_STALE_OR_CONFLICTING",
  BLOCKED: "NAMED_RESEARCH_BLOCKED",
  NOT_APPLICABLE: "NAMED_RESEARCH_NOT_APPLICABLE",
});

/**
 * Durable contact-depth outcome for coordinator / weekly refresh closeout.
 * Distinct from publication eligibility (A) and from provider verification.
 */
export const CONTACT_DEPTH_OUTCOME = Object.freeze({
  NAMED_WITH_PERSON_CHANNEL: "relevant_named_contact_with_usable_business_channel",
  NAMED_CHANNEL_PENDING: "relevant_named_contact_with_channels_pending",
  REFERRAL_LEAD_ONLY: "named_referral_lead_only",
  ORG_FALLBACK_AFTER_RESEARCH: "organization_fallback_after_bounded_person_research",
  IDENTITY_CONFLICT: "identity_conflict",
  RESEARCH_BLOCKED: "research_blocked",
  NOT_YET_ATTEMPTED: "research_not_yet_attempted",
});

/**
 * Channel ownership — never present an org inbox as a person's direct email
 * unless evidence establishes that association.
 */
export const CHANNEL_OWNERSHIP = Object.freeze({
  PERSON_SPECIFIC: "person_specific",
  ORGANIZATION_DEPARTMENT: "organization_or_department",
  INTERMEDIARY: "intermediary",
  FALLBACK: "fallback",
  UNKNOWN: "unknown",
});

export const RESPONSIBILITY_CLASS = Object.freeze({
  CONFIRMED_RELEVANT: "CONFIRMED_RELEVANT_RESPONSIBILITY",
  POTENTIALLY_RELEVANT: "POTENTIALLY_RELEVANT_CONTACT",
  REFERRAL_ENTRY: "REFERRAL_OR_ORG_ENTRY_POINT",
  UNCONFIRMED: "RESPONSIBILITY_UNCONFIRMED",
  BOOKING_DECISION_MAKER: "BOOKING_DECISION_MAKER", // only when evidence supports
});

export const CHANNEL_OUTCOME = Object.freeze({
  NO_CHANNEL_FOUND: "NO_CHANNEL_FOUND",
  PROVIDER_ERROR: "PROVIDER_ERROR",
  UNVERIFIED_PROVIDER_RESULT: "UNVERIFIED_PROVIDER_RESULT",
  IDENTITY_CONFLICT: "IDENTITY_CONFLICT",
  ACCEPTED_SOURCE_SUPPORTED: "ACCEPTED_SOURCE_SUPPORTED",
  CORROBORATED_EXISTING: "CORROBORATED_EXISTING",
  REUSED_CACHE: "REUSED_CACHE",
});

export const PHONE_TYPE = Object.freeze({
  DIRECT_BUSINESS: "DIRECT_BUSINESS",
  DEPARTMENT: "DEPARTMENT",
  SWITCHBOARD: "SWITCHBOARD",
  OTHER: "OTHER",
  UNKNOWN: "UNKNOWN",
});

/** Target roles by demand use case — founder/president is NOT automatic buyer. */
export const TARGET_ROLES_BY_USE_CASE = Object.freeze({
  ASSOCIATION_MEETINGS: [
    "meetings_events_staff",
    "housing_lead",
    "contracted_planner",
    "association_director_meetings",
  ],
  CORPORATE_LODGING: [
    "travel",
    "workplace",
    "operations",
    "executive_assistant",
  ],
  TRAINING_ONBOARDING: [
    "people_ops",
    "learning_development",
    "workplace",
    "organizer",
  ],
  LOCAL_CATERING: [
    "event_organizer",
    "administrator",
    "office_events_manager",
    "development_fundraising",
    "program_operations",
  ],
  PROFESSIONAL_PLANNER: [
    "owner_principal",
    "event_director",
    "corporate_events_lead",
    "destination_services_lead",
    "venue_sourcing_lead",
  ],
  VENUE_PARTNERSHIP: [
    "private_events_manager",
    "lodging_referral_owner",
  ],
  SPORTS: [
    "tournament_housing_organizer",
    "team_travel",
    "agency",
  ],
  GENERAL: ["events", "meetings", "workplace", "operations"],
});

export const RESEARCH_ORDER = Object.freeze([
  "canonical_intelligence",
  "official_staff_event_contact_pages",
  "brochures_prospectuses_agency",
  "public_professional_profiles",
  "other_credible_public",
  "surfe_when_justified",
]);

export const FIELD_KINDS = Object.freeze({
  NAME: "name",
  EMPLOYER_TITLE: "employer_title",
  PROFILE_URL: "profile_url",
  EMAIL: "email",
  PHONE: "phone",
});

/**
 * Infer use-case bucket for role targeting from an opportunity.
 */
export function inferContactUseCaseBucket(opp = {}) {
  const blob = [
    opp.researchSegment,
    opp.opportunityType,
    opp.demandUseCase,
    ...(opp.demandUseCases || []),
    opp.detailLayout,
    opp.title,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");

  if (/sport|tournament|team.?travel/.test(blob)) return "SPORTS";
  if (/venue.?partner|private.?event|wedding/.test(blob)) return "VENUE_PARTNERSHIP";
  if (
    /professional.?planner|planner.?intermediar|dmc\b|destination.?manag|event.?producer|events? by |lifestyle/.test(
      blob
    )
  ) {
    return "PROFESSIONAL_PLANNER";
  }
  if (/catering|local.?meeting|banquet|gala|chapter|luncheon|nonprofit/.test(blob)) {
    return "LOCAL_CATERING";
  }
  if (/train|onboard|people.?ops|learning/.test(blob)) return "TRAINING_ONBOARDING";
  if (/scaling|corporate.?account|employer|office.?expand/.test(blob)) {
    return "CORPORATE_LODGING";
  }
  if (/association|amwa|annual.?meeting|medical.?societ|government/.test(blob)) {
    return "ASSOCIATION_MEETINGS";
  }
  return "GENERAL";
}

export function targetRolesForOpportunity(opp = {}) {
  const bucket = inferContactUseCaseBucket(opp);
  return {
    bucket,
    roles: TARGET_ROLES_BY_USE_CASE[bucket] || TARGET_ROLES_BY_USE_CASE.GENERAL,
    note: "Founder/president/senior executive is not automatically the booking buyer",
  };
}

/**
 * A. Publication eligibility vs B. Named research completeness.
 */
export function assessContactResearchAxes(opp = {}, opts = {}) {
  const email =
    opp.primaryContact?.email ||
    opp.functionalContactEmail ||
    opp.contactEmail ||
    null;
  const phone = opp.primaryContact?.phone || opp.contactPhone || null;
  const url =
    opp.publicContactPath ||
    opp.organizationContactUrl ||
    opp.contactUrl ||
    opp.functionalContactUrl ||
    null;
  const homepageOnly =
    url &&
    /^https?:\/\/[^/]+\/?(?:index\.(?:html?|php))?$/i.test(String(url).trim()) &&
    !email &&
    !phone;

  let publication = PUBLICATION_CONTACT_STATUS.NO_ROUTE;
  if (email || phone || (url && !homepageOnly)) {
    publication = PUBLICATION_CONTACT_STATUS.USABLE_ROUTE;
  } else if (homepageOnly) {
    publication = PUBLICATION_CONTACT_STATUS.HOMEPAGE_ONLY;
  }

  const named = String(opp.primaryContact?.name || opp.contactName || "").trim();
  const hasNamed = named.split(/\s+/).length >= 2;
  const resp =
    opts.responsibilityClass ||
    opp.contactResponsibilityClass ||
    opp.bookingResponsibilityClass ||
    null;

  const personEmail = String(opp.primaryContact?.email || "").trim();
  const orgEmail = String(
    opp.functionalContactEmail || opp.organizationContactEmail || ""
  ).trim();
  const personEmailIsOrgInbox =
    personEmail &&
    orgEmail &&
    personEmail.toLowerCase() === orgEmail.toLowerCase();
  const hasPersonSpecificChannel = Boolean(
    (personEmail && !personEmailIsOrgInbox && opp.primaryContact?.emailOwnership !== CHANNEL_OWNERSHIP.ORGANIZATION_DEPARTMENT) ||
      (opp.primaryContact?.phone &&
        opp.primaryContact?.phoneOwnership === CHANNEL_OWNERSHIP.PERSON_SPECIFIC) ||
      opp.primaryContact?.emailOwnership === CHANNEL_OWNERSHIP.PERSON_SPECIFIC
  );

  const researchAttempted =
    opts.namedResearchAttempted === true ||
    opp.namedContactResearchAttempted === true ||
    Boolean(opp.namedContactResearchCompletedAt) ||
    Boolean(opp.contactDepthResearch?.attemptedAt);

  const priorStatus = String(
    opts.namedResearchStatus ||
      opp.namedContactResearchStatus ||
      opp.contactDepthResearch?.namedResearchStatus ||
      ""
  ).trim();

  let namedStatus = researchAttempted
    ? NAMED_RESEARCH_STATUS.PENDING
    : NAMED_RESEARCH_STATUS.NOT_ATTEMPTED;

  if (
    hasNamed &&
    resp === RESPONSIBILITY_CLASS.CONFIRMED_RELEVANT &&
    hasPersonSpecificChannel
  ) {
    namedStatus = NAMED_RESEARCH_STATUS.COMPLETE;
  } else if (
    hasNamed &&
    (resp === RESPONSIBILITY_CLASS.CONFIRMED_RELEVANT ||
      resp === RESPONSIBILITY_CLASS.POTENTIALLY_RELEVANT)
  ) {
    // Generic inbox alone must NOT mark named-buyer enrichment complete
    namedStatus = NAMED_RESEARCH_STATUS.PERSON_ESTABLISHED_CHANNEL_PENDING;
  } else if (hasNamed && resp === RESPONSIBILITY_CLASS.REFERRAL_ENTRY) {
    namedStatus = NAMED_RESEARCH_STATUS.WEAK_RELEVANCE;
  } else if (hasNamed) {
    namedStatus = NAMED_RESEARCH_STATUS.WEAK_RELEVANCE;
  } else if (
    researchAttempted &&
    (priorStatus === NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK ||
      priorStatus === "NAMED_RESEARCH_EXHAUSTED_ORG_FALLBACK" ||
      opp.contactDepthResearch?.outcome ===
        CONTACT_DEPTH_OUTCOME.ORG_FALLBACK_AFTER_RESEARCH)
  ) {
    namedStatus = NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK;
  } else if (
    researchAttempted &&
    priorStatus === NAMED_RESEARCH_STATUS.BLOCKED
  ) {
    namedStatus = NAMED_RESEARCH_STATUS.BLOCKED;
  }

  const queueReasons = [];
  // Org/functional route must NEVER suppress named-person research queueing
  if (
    publication === PUBLICATION_CONTACT_STATUS.USABLE_ROUTE &&
    !hasNamed &&
    namedStatus !== NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK &&
    namedStatus !== NAMED_RESEARCH_STATUS.COMPLETE
  ) {
    queueReasons.push("functional_route_only_named_enrichment_pending");
  }
  if (namedStatus === NAMED_RESEARCH_STATUS.NOT_ATTEMPTED) {
    queueReasons.push("named_person_research_not_yet_attempted");
  }
  if (namedStatus === NAMED_RESEARCH_STATUS.PERSON_ESTABLISHED_CHANNEL_PENDING) {
    queueReasons.push(
      "named_person_established_person_specific_channel_still_pending"
    );
  }
  if (namedStatus === NAMED_RESEARCH_STATUS.WEAK_RELEVANCE) {
    queueReasons.push("named_contact_weak_or_unclear_relevance");
  }
  if (opts.hotelReportedWrong) queueReasons.push("hotel_reported_wrong_contact");
  if (opts.staleAffiliation) queueReasons.push("stale_or_conflicting_affiliation");
  if (hasNamed && !email && !phone) {
    queueReasons.push("named_person_missing_channels");
  }
  // Person established + org inbox only → still Surfe/public channel eligible
  if (
    hasNamed &&
    !hasPersonSpecificChannel &&
    (email || phone) &&
    personEmailIsOrgInbox
  ) {
    queueReasons.push(
      "org_inbox_present_person_specific_channel_still_required"
    );
  }

  let contactDepthOutcome = CONTACT_DEPTH_OUTCOME.NOT_YET_ATTEMPTED;
  if (namedStatus === NAMED_RESEARCH_STATUS.COMPLETE) {
    contactDepthOutcome = CONTACT_DEPTH_OUTCOME.NAMED_WITH_PERSON_CHANNEL;
  } else if (
    namedStatus === NAMED_RESEARCH_STATUS.PERSON_ESTABLISHED_CHANNEL_PENDING
  ) {
    contactDepthOutcome = CONTACT_DEPTH_OUTCOME.NAMED_CHANNEL_PENDING;
  } else if (
    hasNamed &&
    resp === RESPONSIBILITY_CLASS.REFERRAL_ENTRY
  ) {
    contactDepthOutcome = CONTACT_DEPTH_OUTCOME.REFERRAL_LEAD_ONLY;
  } else if (namedStatus === NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK) {
    contactDepthOutcome = CONTACT_DEPTH_OUTCOME.ORG_FALLBACK_AFTER_RESEARCH;
  } else if (namedStatus === NAMED_RESEARCH_STATUS.BLOCKED) {
    contactDepthOutcome = CONTACT_DEPTH_OUTCOME.RESEARCH_BLOCKED;
  } else if (namedStatus === NAMED_RESEARCH_STATUS.STALE) {
    contactDepthOutcome = CONTACT_DEPTH_OUTCOME.IDENTITY_CONFLICT;
  }

  return {
    version: BUYER_CONTACT_ENRICHMENT_CONTRACT_VERSION,
    publicationContactStatus: publication,
    namedResearchStatus: namedStatus,
    contactDepthOutcome,
    namedResearchAttempted: researchAttempted,
    responsibilityClass: resp || RESPONSIBILITY_CLASS.UNCONFIRMED,
    enrichmentQueue: queueReasons.length > 0,
    queueReasons,
    customerMessageWhenNamedPending:
      namedStatus === NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK
        ? "A relevant named contact was not established after research; organization contact route remains available."
        : "A named buyer has not yet been established for this opportunity.",
    doNotClaim:
      "Do not claim that no named contact exists when research is unfinished; do not treat an organization route as named-person research complete",
  };
}

/**
 * Map axes → durable contact-depth outcome label for persistence.
 */
export function resolveContactDepthOutcome(axes = {}, opts = {}) {
  if (axes.contactDepthOutcome) return axes.contactDepthOutcome;
  if (opts.identityConflict) return CONTACT_DEPTH_OUTCOME.IDENTITY_CONFLICT;
  if (opts.blocked) return CONTACT_DEPTH_OUTCOME.RESEARCH_BLOCKED;
  if (
    axes.namedResearchStatus === NAMED_RESEARCH_STATUS.COMPLETE
  ) {
    return CONTACT_DEPTH_OUTCOME.NAMED_WITH_PERSON_CHANNEL;
  }
  if (
    axes.namedResearchStatus ===
    NAMED_RESEARCH_STATUS.PERSON_ESTABLISHED_CHANNEL_PENDING
  ) {
    return CONTACT_DEPTH_OUTCOME.NAMED_CHANNEL_PENDING;
  }
  if (
    axes.responsibilityClass === RESPONSIBILITY_CLASS.REFERRAL_ENTRY
  ) {
    return CONTACT_DEPTH_OUTCOME.REFERRAL_LEAD_ONLY;
  }
  if (
    axes.namedResearchStatus === NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK ||
    opts.exhausted === true
  ) {
    return CONTACT_DEPTH_OUTCOME.ORG_FALLBACK_AFTER_RESEARCH;
  }
  if (axes.namedResearchAttempted !== true) {
    return CONTACT_DEPTH_OUTCOME.NOT_YET_ATTEMPTED;
  }
  // Attempted but still pending a named person — do not invent exhaustion.
  if (
    axes.namedResearchStatus === NAMED_RESEARCH_STATUS.PENDING ||
    axes.namedResearchStatus === NAMED_RESEARCH_STATUS.NOT_ATTEMPTED
  ) {
    return CONTACT_DEPTH_OUTCOME.NOT_YET_ATTEMPTED;
  }
  return CONTACT_DEPTH_OUTCOME.ORG_FALLBACK_AFTER_RESEARCH;
}

export function customerSafeContactView(contact = {}, fallbackRoute = {}) {
  return {
    recommendedPerson: contact.name || null,
    title: contact.title || contact.role || null,
    organization: contact.organization || contact.employer || null,
    whyRelevant: contact.relevanceEvidence || contact.whyThisContact || null,
    email: contact.email || fallbackRoute.email || null,
    phone: contact.phone || fallbackRoute.phone || null,
    phoneType:
      contact.phoneType === PHONE_TYPE.UNKNOWN || !contact.phoneType
        ? contact.phone
          ? PHONE_TYPE.UNKNOWN
          : null
        : contact.phoneType,
    phoneTypeLabel:
      contact.phoneType === PHONE_TYPE.DIRECT_BUSINESS
        ? "Direct"
        : contact.phoneType === PHONE_TYPE.DEPARTMENT
          ? "Department"
          : contact.phoneType === PHONE_TYPE.SWITCHBOARD
            ? "Switchboard"
            : contact.phone
              ? "Phone"
              : null,
    responsibilityUncertainty: contact.responsibilityUncertainty || null,
    publicProfileUrl: contact.publicProfileUrl || contact.profileUrl || null,
    fallbackRoute: {
      email: fallbackRoute.email || null,
      phone: fallbackRoute.phone || null,
      url: fallbackRoute.url || null,
      label: fallbackRoute.label || "Organization contact route",
    },
    lastChecked: contact.verificationDate || contact.lastChecked || null,
    // Explicitly omit: surfe*, provider*, credits, costs, request/job IDs
  };
}
