/**
 * Scaling Companies contact intelligence V1.
 *
 * Two-contact research standard (preferred): primary + secondary distinct named
 * commercially relevant people. Minimum: one CONTACT_RELEVANT named person.
 *
 * CONTACT_RELEVANT ≠ CONTRACTING_AUTHORITY_VERIFIED.
 * Does not invent names, emails, phones, or LinkedIn URLs.
 */
export const SCALING_CONTACT_INTELLIGENCE_V1 =
  "gdi-scaling-contact-intelligence-v1";

export const CONTACT_FUNCTIONAL_CATEGORY = Object.freeze({
  TRAVEL_PROCUREMENT: "travel_procurement",
  MEETINGS_PROCUREMENT: "meetings_procurement",
  OFFICE_WORKPLACE_FACILITIES: "office_workplace_facilities",
  EXECUTIVE_ASSISTANT_COORDINATOR: "executive_assistant_coordinator",
  OPERATIONS_PROJECT: "operations_project",
  HR_PEOPLE: "hr_people",
  LOCAL_LEADERSHIP: "local_leadership",
  OTHER_OR_UNKNOWN: "other_or_unknown",
});

export const CONTACT_COMMERCIAL_CLASS = Object.freeze({
  HOTEL_SOURCING_DECISION_MAKER: "hotel_sourcing_decision_maker",
  OPERATIONAL_BUYER_OR_COORDINATOR: "operational_buyer_or_coordinator",
  COMMERCIAL_INFLUENCER_OR_REFERRAL: "commercial_influencer_or_referral",
  ORGANIZATIONAL_ROUTE_ONLY: "organizational_route_only",
  NOT_COMMERCIALLY_RELEVANT: "not_commercially_relevant",
  UNKNOWN: "unknown",
});

export const CONTRACTING_AUTHORITY_STATUS = Object.freeze({
  VERIFIED: "CONTRACTING_AUTHORITY_VERIFIED",
  NOT_VERIFIED: "CONTRACTING_AUTHORITY_NOT_VERIFIED",
  UNKNOWN: "CONTRACTING_AUTHORITY_UNKNOWN",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normalizePersonKey(name = "") {
  return text(name)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function looksLikePersonName(name = "") {
  const n = text(name);
  if (!n || n.length < 3) return false;
  if (/^(relevant contact|contact being researched|unknown|n\/a|—|-)$/i.test(n)) {
    return false;
  }
  if (/mynewsdesk|pr newswire|business wire|mexico city|riviera maya/i.test(n)) {
    return false;
  }
  // Prefer at least two tokens for a person name
  return n.split(/\s+/).length >= 2 || /^[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+\.?$/u.test(n);
}

/**
 * Map Exa relevanceClass / role text → functional category + commercial class.
 */
export function classifyContactRole({
  role = "",
  relevanceClass = "",
  whyRelevant = "",
} = {}) {
  const blob = `${role} ${relevanceClass} ${whyRelevant}`.toLowerCase();

  let functionalCategory = CONTACT_FUNCTIONAL_CATEGORY.OTHER_OR_UNKNOWN;
  if (/travel.?manager|travel.?procurement|hotel category|strategic sourcing.*travel|corporate travel/i.test(blob)) {
    functionalCategory = CONTACT_FUNCTIONAL_CATEGORY.TRAVEL_PROCUREMENT;
  } else if (/meetings.?procurement|meetings and events|venue sourcing|event manager/i.test(blob)) {
    functionalCategory = CONTACT_FUNCTIONAL_CATEGORY.MEETINGS_PROCUREMENT;
  } else if (/office manager|workplace|facilities|administrative services|office.?services/i.test(blob)) {
    functionalCategory = CONTACT_FUNCTIONAL_CATEGORY.OFFICE_WORKPLACE_FACILITIES;
  } else if (/executive assistant|partner assistant|ea\b|assistant to/i.test(blob)) {
    functionalCategory = CONTACT_FUNCTIONAL_CATEGORY.EXECUTIVE_ASSISTANT_COORDINATOR;
  } else if (/operations|project ops|business operations/i.test(blob)) {
    functionalCategory = CONTACT_FUNCTIONAL_CATEGORY.OPERATIONS_PROJECT;
  } else if (/human resources|\bhr\b|people operations|talent|onboarding/i.test(blob)) {
    functionalCategory = CONTACT_FUNCTIONAL_CATEGORY.HR_PEOPLE;
  } else if (/managing partner|partner|director|country manager|general manager/i.test(blob)) {
    functionalCategory = CONTACT_FUNCTIONAL_CATEGORY.LOCAL_LEADERSHIP;
  }

  let commercialClass = CONTACT_COMMERCIAL_CLASS.UNKNOWN;
  // Tier 1 / Tier 2 title signals must come from the role/title string — not whyRelevant
  // prose (which often contains negations like "not … travel procurement").
  const tier1Role =
    /travel manager|corporate travel|travel.?procurement|hotel category|meetings.?procurement|strategic sourcing|hotel.?sourcing/i.test(
      role,
    );
  const tier2Role =
    /office manager|partner assistant|executive assistant|workplace manager|facilities manager|administrative services|meetings and events manager|\bea\b/i.test(
      role,
    );
  if (
    relevanceClass === "direct_buyer" ||
    (tier1Role && relevanceClass !== "press_or_unrelated")
  ) {
    commercialClass = CONTACT_COMMERCIAL_CLASS.HOTEL_SOURCING_DECISION_MAKER;
  } else if (tier2Role) {
    commercialClass = CONTACT_COMMERCIAL_CLASS.OPERATIONAL_BUYER_OR_COORDINATOR;
  } else if (relevanceClass === "press_or_unrelated") {
    commercialClass = CONTACT_COMMERCIAL_CLASS.NOT_COMMERCIALLY_RELEVANT;
  } else if (
    relevanceClass === "relevant_influencer" ||
    relevanceClass === "organizational_referral" ||
    /hr|people|onboarding|recruiting|customer success|account development|operations consultant/i.test(
      blob,
    )
  ) {
    commercialClass = CONTACT_COMMERCIAL_CLASS.COMMERCIAL_INFLUENCER_OR_REFERRAL;
  }

  // HR titles without hotel/travel connection stay influencer/referral
  if (
    functionalCategory === CONTACT_FUNCTIONAL_CATEGORY.HR_PEOPLE &&
    commercialClass !== CONTACT_COMMERCIAL_CLASS.HOTEL_SOURCING_DECISION_MAKER &&
    commercialClass !== CONTACT_COMMERCIAL_CLASS.OPERATIONAL_BUYER_OR_COORDINATOR
  ) {
    commercialClass = CONTACT_COMMERCIAL_CLASS.COMMERCIAL_INFLUENCER_OR_REFERRAL;
  }

  const contractingAuthorityStatus =
    commercialClass === CONTACT_COMMERCIAL_CLASS.HOTEL_SOURCING_DECISION_MAKER &&
    /verified|authority|signing|negotiat|procurement owner|rate.?approv/i.test(blob)
      ? CONTRACTING_AUTHORITY_STATUS.VERIFIED
      : commercialClass === CONTACT_COMMERCIAL_CLASS.HOTEL_SOURCING_DECISION_MAKER
        ? CONTRACTING_AUTHORITY_STATUS.UNKNOWN
        : CONTRACTING_AUTHORITY_STATUS.NOT_VERIFIED;

  const contactRelevant =
    commercialClass === CONTACT_COMMERCIAL_CLASS.HOTEL_SOURCING_DECISION_MAKER ||
    commercialClass === CONTACT_COMMERCIAL_CLASS.OPERATIONAL_BUYER_OR_COORDINATOR ||
    commercialClass === CONTACT_COMMERCIAL_CLASS.COMMERCIAL_INFLUENCER_OR_REFERRAL;

  return {
    functionalCategory,
    commercialClass,
    contractingAuthorityStatus,
    contactRelevant,
    contactRelevanceLabel: contactRelevant ? "CONTACT_RELEVANT" : "NOT_CONTACT_RELEVANT",
  };
}

function scoreContact(c) {
  const cls = c.commercialClass;
  let score = 0;
  if (cls === CONTACT_COMMERCIAL_CLASS.HOTEL_SOURCING_DECISION_MAKER) score += 100;
  else if (cls === CONTACT_COMMERCIAL_CLASS.OPERATIONAL_BUYER_OR_COORDINATOR) score += 70;
  else if (cls === CONTACT_COMMERCIAL_CLASS.COMMERCIAL_INFLUENCER_OR_REFERRAL) score += 40;
  else score -= 50;
  if (c.employerVerified === true) score += 10;
  if (c.contractingAuthorityStatus === CONTRACTING_AUTHORITY_STATUS.VERIFIED) score += 25;
  if (c.localMarketRelevant === true) score += 8;
  if (text(c.sourceUrl)) score += 5;
  if (text(c.whyRelevantToHotelDemand || c.commercialRelevance)) score += 5;
  // Seniority alone must not dominate — slight penalty for leadership without ops/travel cues
  if (
    c.functionalCategory === CONTACT_FUNCTIONAL_CATEGORY.LOCAL_LEADERSHIP &&
    cls === CONTACT_COMMERCIAL_CLASS.COMMERCIAL_INFLUENCER_OR_REFERRAL
  ) {
    score -= 5;
  }
  return score;
}

/**
 * Normalize a raw buyer candidate (Exa or opportunity) into Stage D contact record.
 */
export function normalizeNamedContactCandidate(raw = {}, opts = {}) {
  const fullName = text(raw.fullName || raw.name);
  if (!looksLikePersonName(fullName)) {
    return {
      ok: false,
      reason: "not_person_name",
      fullName: fullName || null,
    };
  }
  const role = text(raw.role || raw.title);
  const classified = classifyContactRole({
    role,
    relevanceClass: raw.relevanceClass || raw.buyerRole || "",
    whyRelevant: raw.whyRelevantToHotelDemand || raw.commercialRelevance || raw.whyThisContact || "",
  });
  if (
    /mynewsdesk|pr newswire|business wire/i.test(text(raw.employer || raw.company || opts.companyName))
  ) {
    classified.commercialClass = CONTACT_COMMERCIAL_CLASS.NOT_COMMERCIALLY_RELEVANT;
    classified.contactRelevant = false;
    classified.contactRelevanceLabel = "NOT_CONTACT_RELEVANT";
  }

  const record = {
    name: fullName,
    company: text(raw.employer || raw.company || opts.companyName) || null,
    currentTitle: role || null,
    functionalCategory: classified.functionalCategory,
    relevantGeography: text(raw.relevantGeography || raw.location || opts.geography) || null,
    commercialClass: classified.commercialClass,
    contactRelevanceLabel: classified.contactRelevanceLabel,
    contractingAuthorityStatus: classified.contractingAuthorityStatus,
    originalRelevanceClass: text(raw.relevanceClass) || null,
    commercialRelevance: text(
      raw.whyRelevantToHotelDemand || raw.commercialRelevance || raw.whyThisContact,
    ) || null,
    supportingClaimIds: Array.isArray(raw.supportingClaimIds) ? raw.supportingClaimIds : [],
    supportingSourceUrls: [raw.sourceUrl, ...(raw.supportingSourceUrls || [])]
      .map((u) => text(u))
      .filter(Boolean),
    researchConfidence: text(raw.researchConfidence) || (raw.employerVerified === true ? "medium" : "low"),
    suggestedOutreachObjective: text(raw.suggestedOutreachObjective) || null,
    verificationDate: text(raw.verificationDate || opts.verificationDate) || null,
    employerVerified: raw.employerVerified === true,
    localMarketRelevant: raw.localMarketRelevant !== false,
    email: null,
    phone: null,
    sourceUrl: text(raw.sourceUrl) || null,
    personKey: normalizePersonKey(fullName),
    score: 0,
  };
  record.score = scoreContact(record);
  // Prefer evidenced influencers/coordinators over weak organizational referrals
  if (record.originalRelevanceClass === "organizational_referral") {
    record.score -= 15;
  }
  if (record.originalRelevanceClass === "relevant_influencer") {
    record.score += 5;
  }
  if (record.originalRelevanceClass === "direct_buyer") {
    record.score += 20;
  }
  record.ok = classified.contactRelevant;
  record.reason = classified.contactRelevant ? null : "not_commercially_relevant";
  return record;
}

/**
 * Select primary + secondary distinct CONTACT_RELEVANT people from candidates.
 */
export function selectPrimaryAndSecondaryContacts(candidates = [], opts = {}) {
  const normalized = (Array.isArray(candidates) ? candidates : [])
    .map((c) => normalizeNamedContactCandidate(c, opts))
    .filter((c) => c.ok && c.contactRelevanceLabel === "CONTACT_RELEVANT");

  // Dedupe same person
  const seen = new Set();
  const unique = [];
  for (const c of normalized.sort((a, b) => b.score - a.score)) {
    if (seen.has(c.personKey)) continue;
    seen.add(c.personKey);
    unique.push(c);
  }

  const primary = unique[0] || null;
  const secondary = unique[1] || null;
  const additional = unique.slice(2, 6);

  return {
    version: SCALING_CONTACT_INTELLIGENCE_V1,
    primaryContact: primary,
    secondaryContact: secondary,
    additionalCandidates: additional,
    namedContactCount: unique.length,
    meetsMinimumOneRelevantNamed: unique.length >= 1,
    meetsPreferredTwoDistinct: unique.length >= 2,
    contactResearchGaps: buildContactResearchGaps(unique, opts),
  };
}

function buildContactResearchGaps(unique = [], opts = {}) {
  const gaps = [];
  if (unique.length === 0) {
    gaps.push({
      priority: "P0",
      question:
        "Identify at least one verified named commercially relevant person (buyer, coordinator, influencer, or referral) for hotel demand.",
    });
  }
  if (unique.length === 1) {
    gaps.push({
      priority: "P1",
      question:
        "Preferred: identify a second distinct named contact (decision-maker, operational coordinator, or credible referral).",
    });
  }
  if (
    unique.length > 0 &&
    !unique.some(
      (c) =>
        c.contractingAuthorityStatus === CONTRACTING_AUTHORITY_STATUS.VERIFIED ||
        c.commercialClass === CONTACT_COMMERCIAL_CLASS.HOTEL_SOURCING_DECISION_MAKER,
    )
  ) {
    gaps.push({
      priority: "P1",
      question:
        "Verify whether any identified contact (or a referred owner) has hotel/travel contracting or approval authority.",
    });
  }
  if (opts.demandMechanism && !/unknown/i.test(text(opts.demandMechanism))) {
    gaps.push({
      priority: "P2",
      question: `Confirm which function owns sourcing for demand mechanism: ${text(opts.demandMechanism).slice(0, 160)}`,
    });
  }
  return gaps;
}

/**
 * Decision-process scaffold from deep dossier fields.
 */
export function extractDecisionProcess(dossier = {}) {
  const sourcing = dossier.sourcingAccessibility || {};
  const demand = dossier.hotelDemandHypotheses || {};
  return {
    demandMechanism: text(demand.primaryMechanism) || null,
    demandEvidenceStatus: demand.evidenceStatus || null,
    travelCentrallyManaged: sourcing.preferredHotelPatternKnown === true ? null : null,
    preferredHotelPatternKnown: sourcing.preferredHotelPatternKnown ?? null,
    corporateTravelOwnerKnown: sourcing.corporateTravelOwnerKnown ?? null,
    summary: text(sourcing.summary) || null,
    notes: text(sourcing.notes) || null,
    tmcOrExternalMentioned: /tmc|amex|bcd|fcm|travel management|travel agency/i.test(
      `${sourcing.summary || ""} ${sourcing.notes || ""} ${(dossier.verifiedFacts || []).join(" ")}`,
    ),
    unknowns: [
      ...(dossier.materialUncertainties || []).filter((u) =>
        /travel|hotel|buyer|procurement|tmc|preferred/i.test(String(u)),
      ),
    ].slice(0, 8),
  };
}

/**
 * Build outreach objects for primary/secondary without inventing channels.
 */
export function buildContactOutreachPlan(contact = {}, opts = {}) {
  if (!contact?.name) return null;
  const hotelName = text(opts.hotelName) || "the hotel";
  const company = text(contact.company || opts.companyName) || "the company";
  return {
    name: contact.name,
    whyThisPersonMatters: contact.commercialRelevance || contact.currentTitle || null,
    relevantHotelBusiness: text(opts.demandMechanism) || null,
    suggestedFirstDiscussion: `Introduce ${hotelName} for ${company}'s ${
      text(opts.demandMechanism) || "visiting-staff / meetings"
    } needs and ask who owns lodging or meetings sourcing.`,
    specificQualificationQuestion:
      contact.commercialClass === CONTACT_COMMERCIAL_CLASS.HOTEL_SOURCING_DECISION_MAKER
        ? "Is there an active preferred-hotel or travel-management pattern we should respect before proposing rates?"
        : "Who owns hotel booking approval or preferred-hotel decisions for visiting staff and local meetings?",
    appropriateNextStep:
      contact.commercialClass === CONTACT_COMMERCIAL_CLASS.OPERATIONAL_BUYER_OR_COORDINATOR ||
      contact.commercialClass === CONTACT_COMMERCIAL_CLASS.COMMERCIAL_INFLUENCER_OR_REFERRAL
        ? "Request a short referral to the travel/meetings owner; do not pitch a negotiated contract yet."
        : "Qualify demand dates, volume band, and procurement path before a rate proposal.",
    knownLimitations: [
      contact.contractingAuthorityStatus !== CONTRACTING_AUTHORITY_STATUS.VERIFIED
        ? "Contracting authority not verified"
        : null,
      "Direct email/phone not researched in this pass (on-demand enrichment only)",
    ].filter(Boolean),
  };
}

/**
 * Evaluate Stage C / Exa dossier contacts into Stage D contact intelligence pack.
 */
export function evaluateContactIntelligenceFromDossier(dossier = {}, opts = {}) {
  const buyers = Array.isArray(dossier.buyerCandidates) ? dossier.buyerCandidates : [];
  const selected = selectPrimaryAndSecondaryContacts(buyers, {
    companyName: dossier.companyIdentity?.legalOrTradingName || opts.companyName,
    geography: dossier.activityAndChronology?.geography,
    demandMechanism: dossier.hotelDemandHypotheses?.primaryMechanism,
    verificationDate: opts.verificationDate || "2026-10-28",
  });
  const decisionProcess = extractDecisionProcess(dossier);
  const organizationalRoutes = (dossier.organizationalContactRoutes || [])
    .filter((r) => r?.url && r.isTargetCompanyRoute !== false)
    .map((r) => ({
      url: r.url,
      label: r.label || null,
      isTargetCompanyRoute: true,
      class: CONTACT_COMMERCIAL_CLASS.ORGANIZATIONAL_ROUTE_ONLY,
    }));

  return {
    version: SCALING_CONTACT_INTELLIGENCE_V1,
    decisionProcess,
    primaryContact: selected.primaryContact,
    secondaryContact: selected.secondaryContact,
    additionalCandidates: selected.additionalCandidates,
    organizationalRoutes,
    contactResearchGaps: selected.contactResearchGaps,
    namedContactCount: selected.namedContactCount,
    meetsMinimumOneRelevantNamed: selected.meetsMinimumOneRelevantNamed,
    meetsPreferredTwoDistinct: selected.meetsPreferredTwoDistinct,
    primaryOutreach: buildContactOutreachPlan(selected.primaryContact, {
      hotelName: opts.hotelName,
      companyName: dossier.companyIdentity?.legalOrTradingName,
      demandMechanism: decisionProcess.demandMechanism,
    }),
    secondaryOutreach: buildContactOutreachPlan(selected.secondaryContact, {
      hotelName: opts.hotelName,
      companyName: dossier.companyIdentity?.legalOrTradingName,
      demandMechanism: decisionProcess.demandMechanism,
    }),
  };
}

/**
 * Apply contact intelligence onto opportunity fields (shadow-safe).
 */
export function applyContactIntelligenceToOpportunity(opportunity = {}, pack = {}) {
  const next = { ...opportunity };
  next.scalingContactIntelligence = pack;
  next.decisionProcess = pack.decisionProcess || null;
  next.contactResearchGaps = pack.contactResearchGaps || [];

  if (pack.primaryContact?.name) {
    next.primaryContact = {
      name: pack.primaryContact.name,
      title: pack.primaryContact.currentTitle,
      role: pack.primaryContact.currentTitle,
      organization: pack.primaryContact.company,
      email: null,
      phone: null,
      whyThisContact: pack.primaryContact.commercialRelevance,
      commercialClass: pack.primaryContact.commercialClass,
      contactRelevanceLabel: pack.primaryContact.contactRelevanceLabel,
      contractingAuthorityStatus: pack.primaryContact.contractingAuthorityStatus,
      functionalCategory: pack.primaryContact.functionalCategory,
      sourceUrl: pack.primaryContact.sourceUrl,
    };
    next.contactName = pack.primaryContact.name;
    next.contactRole = pack.primaryContact.currentTitle;
    next.contactRelevance = pack.primaryContact.contactRelevanceLabel;
  }
  if (pack.secondaryContact?.name) {
    next.secondaryContact = {
      name: pack.secondaryContact.name,
      title: pack.secondaryContact.currentTitle,
      role: pack.secondaryContact.currentTitle,
      organization: pack.secondaryContact.company,
      email: null,
      phone: null,
      whyThisContact: pack.secondaryContact.commercialRelevance,
      commercialClass: pack.secondaryContact.commercialClass,
      contactRelevanceLabel: pack.secondaryContact.contactRelevanceLabel,
      contractingAuthorityStatus: pack.secondaryContact.contractingAuthorityStatus,
      functionalCategory: pack.secondaryContact.functionalCategory,
      sourceUrl: pack.secondaryContact.sourceUrl,
    };
  } else {
    next.secondaryContact = null;
  }
  // Never auto-flip verified contracting buyer from influencer-class contacts
  if (
    pack.primaryContact?.contractingAuthorityStatus !==
    CONTRACTING_AUTHORITY_STATUS.VERIFIED
  ) {
    // leave hasVerifiedNamedRelevantContact untouched unless already true
  } else if (pack.meetsMinimumOneRelevantNamed) {
    next.hasVerifiedNamedRelevantContact = true;
  }
  return next;
}
