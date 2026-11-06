/**
 * Canonical customer-facing published opportunity snapshot contract.
 *
 * Published inventory must carry approved commercial research so customer
 * surfaces work without local-shadow / research-store overlays.
 * Internal-only research metadata is excluded.
 */
export const GDI_CUSTOMER_FACING_PUBLISHED_SNAPSHOT_V1 =
  "gdi-customer-facing-published-snapshot-v1";

const INTERNAL_ONLY_SNAPSHOT_KEYS = Object.freeze([
  "founderReviewDecision",
  "founderReviewComment",
  "founderCustomerVisibilityDisable",
  "founderVisibilityDisableReason",
  "researchContinuationFocus",
  "exaAgentDeepResearch",
  "internalResearchEligible",
  "monitoringEligible",
  "doesNotAffectProduction",
  "localShadowOnly",
  "stageEShadowPreview",
  "liveCampaignV1Shadow",
  "certificationFixtureOnly",
  "gdiMaturityReason",
  "crossHotelContamination",
  "scoreAuditInternal",
  "generatorOnly",
  "isTestData",
  "airtableRecordId",
  "rawResearchPayload",
  "webhound",
  "providerRaw",
]);

function text(v) {
  if (v == null) return "";
  return String(v).replace(/\s+/g, " ").trim();
}

function list(v, max = 16) {
  if (!Array.isArray(v)) return [];
  return v
    .map((x) => (typeof x === "string" ? text(x) : x))
    .filter((x) => (typeof x === "string" ? x.length > 0 : x != null))
    .slice(0, max);
}

function contactShape(c) {
  if (!c || typeof c !== "object") return null;
  const name = text(c.name);
  if (!name) return null;
  return {
    name,
    title: text(c.title || c.role) || null,
    role: text(c.role || c.title) || null,
    organization: text(c.organization || c.company) || null,
    email: text(c.email) || null,
    phone: text(c.phone || c.companyPhone) || null,
    companyPhone: text(c.companyPhone || c.phone) || null,
    organizationEmail: text(c.organizationEmail) || null,
    organizationContactRoute:
      text(c.organizationContactRoute || c.sourceUrl || c.profileUrl) || null,
    profileUrl: text(c.profileUrl || c.linkedinUrl || c.sourceUrl) || null,
    linkedinUrl: text(c.linkedinUrl) || null,
    sourceUrl: text(c.sourceUrl) || null,
    whyThisContact:
      text(
        c.whyThisContact ||
          c.whyRelevant ||
          c.whyRelevantToHotelDemand ||
          c.relevanceEvidence,
      ) || null,
    whyRelevant:
      text(
        c.whyRelevant ||
          c.whyThisContact ||
          c.whyRelevantToHotelDemand ||
          c.relevanceEvidence,
      ) || null,
    whyRelevantToHotelDemand:
      text(c.whyRelevantToHotelDemand || c.whyRelevant || c.whyThisContact) ||
      null,
  };
}

function sourceShape(s) {
  if (!s) return null;
  if (typeof s === "string") {
    const url = text(s);
    return url ? { url, title: null, claim: null, establishes: null } : null;
  }
  const url = text(s.url || s.href);
  if (!url) return null;
  const claim = text(s.claim || s.establishes || s.title || s.name) || null;
  return {
    url,
    title: text(s.title || s.name || claim) || null,
    name: text(s.name || s.title) || null,
    claim,
    establishes: text(s.establishes || claim) || null,
    publisher: text(s.publisher) || null,
    publicationDate: text(s.publicationDate || s.date) || null,
  };
}

function resolveBrief(opportunity = {}) {
  return (
    opportunity.commercialIntelligenceBrief ||
    opportunity.salesBrief ||
    opportunity.researchToDrawerV2?.salesBrief ||
    null
  );
}

function resolveFacts(opportunity = {}, brief = null) {
  const fromTop = list(opportunity.establishedFacts);
  if (fromTop.length) return fromTop;
  const fromBrief = list(
    brief?.establishedFacts ||
      brief?.establishedFactsAndUncertainties?.facts ||
      brief?.evidenceAndUncertainty?.establishedFacts ||
      brief?.evidenceAndUncertainty?.facts,
  );
  return fromBrief;
}

function resolveUncertainties(opportunity = {}, brief = null) {
  const fromTop = list(opportunity.uncertainties);
  if (fromTop.length) return fromTop;
  return list(
    brief?.establishedFactsAndUncertainties?.uncertainties ||
      brief?.evidenceAndUncertainty?.uncertainties ||
      opportunity.constraints,
  );
}

function resolveQualificationQuestions(opportunity = {}, brief = null) {
  const top = list(
    opportunity.qualificationQuestions ||
      opportunity.suggestedQualificationQuestions,
    12,
  );
  if (top.length) return top;
  const fromBrief = list(brief?.qualificationQuestions, 12);
  if (fromBrief.length) return fromBrief;
  return list(brief?.recommendedSalesApproach?.qualificationQuestions, 12);
}

function resolveWhat(opportunity = {}, brief = null) {
  return (
    text(
      opportunity.whatIsHappening ||
        opportunity.summaryWhat ||
        brief?.whatIsHappening ||
        brief?.companyAndLocalActivity ||
        brief?.opportunityOverview,
    ) || null
  );
}

function resolvePotential(opportunity = {}, brief = null) {
  return (
    text(
      opportunity.potentialHotelBusiness ||
        opportunity.potentialBusiness ||
        opportunity.proposedHotelUse ||
        opportunity.whyHotelDemand ||
        brief?.potentialBusinessForThisHotel ||
        brief?.potentialBusinessForHotel ||
        brief?.potentialBusiness,
    ) || null
  );
}

function resolveWhyHotel(opportunity = {}, brief = null, potential = null) {
  const raw =
    text(
      opportunity.whyThisHotel ||
        opportunity.summaryWhyHotel ||
        opportunity.hotelFitRationale ||
        brief?.whyThisHotel,
    ) || null;
  // Prefer distinct property-fit copy. When source reused the same paragraph for
  // potential + why-hotel, keep potential as the commercial body and rebuild why
  // from advantages/limitations only — never invent new commercial claims.
  const pot = text(potential);
  if (raw && pot && raw.toLowerCase() === pot.toLowerCase()) {
    const adv = list(
      opportunity.whyThisHotelAdvantages || brief?.whyThisHotelAdvantages,
      8,
    );
    const lim = list(
      opportunity.whyThisHotelLimitations || brief?.whyThisHotelLimitations,
      8,
    );
    const rebuilt = [...adv, ...lim].filter(Boolean).join(" ");
    return text(rebuilt) || raw;
  }
  return raw;
}

function resolveWhyActNow(opportunity = {}, brief = null) {
  return (
    text(
      opportunity.whyActNow ||
        opportunity.whyNow ||
        opportunity.whyEstablishContactNow ||
        opportunity.summaryWhyNow ||
        brief?.whyActNow ||
        brief?.recommendedSalesApproach?.timingRationale,
    ) || null
  );
}

function resolveNextStep(opportunity = {}, brief = null) {
  return (
    text(
      opportunity.recommendedNextStep ||
        opportunity.recommendedAction ||
        opportunity.recommendedNextAction ||
        brief?.recommendedNextStep ||
        (typeof brief?.recommendedSalesApproach === "string"
          ? brief.recommendedSalesApproach
          : brief?.recommendedSalesApproach?.nextAction),
    ) || null
  );
}

function resolvePrimaryContact(opportunity = {}, brief = null) {
  return (
    contactShape(opportunity.primaryContact) ||
    contactShape(brief?.whoToContact?.primary) ||
    contactShape({
      name: opportunity.contactName,
      role: opportunity.contactRole,
      title: opportunity.contactRole,
      organization: opportunity.organizationName,
      email: opportunity.contactEmail,
      phone: opportunity.contactPhone,
      whyRelevant: opportunity.contactRelevance || opportunity.whyThisContact,
    })
  );
}

function resolveSecondaryContact(opportunity = {}, brief = null) {
  return (
    contactShape(opportunity.secondaryContact) ||
    contactShape(brief?.whoToContact?.secondary) ||
    null
  );
}

function customerSafeBrief(brief, opportunity = {}) {
  if (!brief || typeof brief !== "object") return null;
  const what = resolveWhat(opportunity, brief);
  const potential = resolvePotential(opportunity, brief);
  const whyHotel = resolveWhyHotel(opportunity, brief, potential);
  const whyActNow = resolveWhyActNow(opportunity, brief);
  const facts = resolveFacts(opportunity, brief);
  const uncertainties = resolveUncertainties(opportunity, brief);
  const questions = resolveQualificationQuestions(opportunity, brief);
  const next = resolveNextStep(opportunity, brief);
  const primary = resolvePrimaryContact(opportunity, brief);
  const secondary = resolveSecondaryContact(opportunity, brief);
  return {
    version: brief.version || GDI_CUSTOMER_FACING_PUBLISHED_SNAPSHOT_V1,
    sourceMapping: brief.sourceMapping || null,
    semanticContract: brief.semanticContract || null,
    demandSegmentId: brief.demandSegmentId || opportunity.demandSegmentId || null,
    segmentDisplayLabel:
      brief.segmentDisplayLabel || opportunity.segmentDisplayLabel || null,
    whatIsHappening: what,
    companyAndLocalActivity: what,
    opportunityOverview: text(brief.opportunityOverview) || what,
    potentialBusinessForThisHotel: potential,
    potentialBusinessForHotel: potential,
    potentialBusiness: potential,
    whyThisHotel: whyHotel,
    whyThisHotelAdvantages: list(
      brief.whyThisHotelAdvantages || opportunity.whyThisHotelAdvantages,
      8,
    ),
    whyThisHotelLimitations: list(
      brief.whyThisHotelLimitations || opportunity.whyThisHotelLimitations,
      8,
    ),
    whyActNow,
    establishedFacts: facts,
    establishedFactsAndUncertainties: {
      facts,
      uncertainties,
    },
    evidenceAndUncertainty: {
      originalActivityDate:
        text(brief.evidenceAndUncertainty?.originalActivityDate) || null,
      sourcePublicationDate:
        text(brief.evidenceAndUncertainty?.sourcePublicationDate) || null,
      researchVerificationDate:
        text(brief.evidenceAndUncertainty?.researchVerificationDate) || null,
      activityStatus:
        text(
          brief.evidenceAndUncertainty?.activityStatus ||
            opportunity.activityStatus,
        ) || null,
      establishedFacts: facts,
      facts,
      uncertainties,
      sources: list(
        (brief.evidenceAndUncertainty?.sources || opportunity.sources || []).map(
          sourceShape,
        ).filter(Boolean),
        12,
      ),
    },
    whoToContact: {
      primary,
      secondary,
      narrative: text(brief.whoToContact?.narrative) || null,
    },
    qualificationQuestions: questions,
    recommendedNextStep: next,
    recommendedSalesApproach:
      brief.recommendedSalesApproach &&
      typeof brief.recommendedSalesApproach === "object"
        ? {
            suggestedInitialTarget:
              text(brief.recommendedSalesApproach.suggestedInitialTarget) ||
              null,
            purposeOfOutreach:
              text(brief.recommendedSalesApproach.purposeOfOutreach) || null,
            qualificationQuestions: questions,
            commercialAngle:
              text(brief.recommendedSalesApproach.commercialAngle) || null,
            nextAction:
              text(brief.recommendedSalesApproach.nextAction) || next,
            timingRationale:
              text(brief.recommendedSalesApproach.timingRationale) ||
              whyActNow,
          }
        : next
          ? { nextAction: next, qualificationQuestions: questions }
          : null,
  };
}

/**
 * Build the durable customer-facing snapshot for publication.
 * Never invents commercial prose — only maps approved source fields.
 */
export function buildCustomerFacingPublishedSnapshot(opportunity = {}, opts = {}) {
  const briefRaw = resolveBrief(opportunity);
  const brief = customerSafeBrief(briefRaw, opportunity);
  const what = resolveWhat(opportunity, briefRaw);
  const potential = resolvePotential(opportunity, briefRaw);
  const whyHotel = resolveWhyHotel(opportunity, briefRaw, potential);
  const whyActNow = resolveWhyActNow(opportunity, briefRaw);
  const facts = resolveFacts(opportunity, briefRaw);
  const uncertainties = resolveUncertainties(opportunity, briefRaw);
  const questions = resolveQualificationQuestions(opportunity, briefRaw);
  const next = resolveNextStep(opportunity, briefRaw);
  const primary = resolvePrimaryContact(opportunity, briefRaw);
  const secondary = resolveSecondaryContact(opportunity, briefRaw);
  const sources = list(
    (opportunity.sources || []).map(sourceShape).filter(Boolean),
    16,
  );
  const evidenceUrls = list(
    opportunity.evidenceUrls ||
      sources.map((s) => s.url).filter(Boolean) ||
      [],
    16,
  );

  const canonicalTitle =
    text(opts.canonicalTitle) ||
    text(
      opportunity.canonicalTitle ||
        opportunity.organizationName ||
        opportunity.displayCompanyName ||
        opportunity.eventDisplayName ||
        opportunity.displayTitle ||
        opportunity.title,
    ) ||
    null;

  const snapshot = {
    contractVersion: GDI_CUSTOMER_FACING_PUBLISHED_SNAPSHOT_V1,
    id: text(opportunity.id) || null,
    hotelId: text(opportunity.hotelId || opts.hotelId) || null,
    title: canonicalTitle,
    displayTitle: canonicalTitle,
    organizationName:
      text(
        opportunity.organizationName ||
          opportunity.displayCompanyName ||
          opportunity.companyName,
      ) || null,
    segment: text(opportunity.segment) || null,
    researchSegment: text(opportunity.researchSegment) || null,
    demandSegmentId: text(opportunity.demandSegmentId) || null,
    eventStartDate: opportunity.eventStartDate || null,
    eventEndDate: opportunity.eventEndDate || null,
    eventDateDisplay: text(opportunity.eventDateDisplay) || null,
    whatIsHappening: what,
    summaryWhat: what,
    potentialHotelBusiness: potential,
    potentialBusiness: potential,
    proposedHotelUse: potential,
    whyThisHotel: whyHotel,
    summaryWhyHotel: whyHotel,
    hotelFitRationale: whyHotel,
    whyActNow,
    whyNow: whyActNow,
    whyEstablishContactNow: whyActNow,
    establishedFacts: facts,
    uncertainties,
    qualificationQuestions: questions,
    recommendedNextStep: next,
    recommendedAction: next,
    recommendedNextAction: next,
    primaryContact: primary,
    secondaryContact: secondary,
    scalingTwoContactPresentation:
      opportunity.scalingTwoContactPresentation || null,
    scalingTwoContactVersion:
      opportunity.scalingTwoContactVersion || null,
    contactName: text(primary?.name) || text(opportunity.contactName) || null,
    contactRole:
      text(primary?.role || primary?.title) ||
      text(opportunity.contactRole) ||
      null,
    contactRelevance:
      text(
        opportunity.contactRelevance ||
          primary?.whyRelevant ||
          primary?.whyThisContact,
      ) || null,
    organizationContactUrl:
      text(
        opportunity.organizationContactUrl ||
          primary?.organizationContactRoute ||
          primary?.sourceUrl,
      ) || null,
    sources,
    evidenceUrls,
    whyThisHotelAdvantages: list(
      opportunity.whyThisHotelAdvantages || briefRaw?.whyThisHotelAdvantages,
      8,
    ),
    whyThisHotelLimitations: list(
      opportunity.whyThisHotelLimitations || briefRaw?.whyThisHotelLimitations,
      8,
    ),
    commercialIntelligenceBrief: brief,
    salesBrief: brief,
    commercialInventoryClass:
      text(
        opts.classification ||
          opportunity.commercialInventoryClass ||
          opportunity.commercialClassification ||
          opportunity.inventoryLane,
      ) || null,
    commercialClassification:
      text(
        opts.classification ||
          opportunity.commercialClassification ||
          opportunity.commercialInventoryClass ||
          opportunity.inventoryLane,
      ) || null,
    // Frozen presentation fields — external surfaces must not recompute these.
    publishedPresentationLocked: true,
    salesActionKey: text(opportunity.salesActionKey) || null,
    salesActionPillLabel: text(opportunity.salesActionPillLabel) || null,
    priority: text(opportunity.priority) || null,
    priorityPillLabel: text(opportunity.priorityPillLabel) || null,
    priorityFilterKey: text(opportunity.priorityFilterKey) || null,
    salesActivationCategory: text(opportunity.salesActivationCategory) || null,
    salesActivationLabel: text(opportunity.salesActivationLabel) || null,
    hasVerifiedNamedRelevantContact:
      opportunity.hasVerifiedNamedRelevantContact === true,
    inventorySalesReady: opportunity.inventorySalesReady === true,
    accountDevelopmentPublished:
      opportunity.accountDevelopmentPublished === true,
    tileContactName:
      text(opportunity.tileContactName) || text(primary?.name) || null,
    tileContactRoleLabel:
      text(opportunity.tileContactRoleLabel) ||
      text(primary?.role || primary?.title) ||
      null,
    salesOpportunityQuickFilterKeys: Array.isArray(
      opportunity.salesOpportunityQuickFilterKeys,
    )
      ? opportunity.salesOpportunityQuickFilterKeys
      : null,
    commercialStageKey: text(opportunity.commercialStageKey) || null,
    opportunityTypeFilterCategory:
      text(opportunity.opportunityTypeFilterCategory) || null,
    opportunityTypeFilterLabel:
      text(opportunity.opportunityTypeFilterLabel) || null,
    customerFacing: true,
  };

  for (const k of INTERNAL_ONLY_SNAPSHOT_KEYS) {
    if (k in snapshot) delete snapshot[k];
  }
  return snapshot;
}

/** Required commercial content for publication (hotel-agnostic). */
export const REQUIRED_PUBLISHED_COMMERCIAL_FIELDS = Object.freeze([
  "title",
  "whatIsHappening",
  "potentialHotelBusiness",
  "whyThisHotel",
  "whyActNow",
  "establishedFacts",
  "uncertainties",
  "recommendedNextStep",
  "primaryContact",
  "sources",
]);

export function validateCustomerFacingPublishedSnapshot(snapshot = {}) {
  const fails = [];
  const warnings = [];
  const id = text(snapshot.id) || "(missing-id)";

  for (const field of REQUIRED_PUBLISHED_COMMERCIAL_FIELDS) {
    const v = snapshot[field];
    if (field === "primaryContact") {
      if (!v || !text(v.name)) fails.push(`${field}:missing_name`);
      continue;
    }
    if (field === "establishedFacts" || field === "uncertainties" || field === "sources") {
      if (!Array.isArray(v) || v.length === 0) fails.push(`${field}:empty`);
      continue;
    }
    if (!text(v)) fails.push(`${field}:empty`);
  }

  // Prevent company-name substitution for commercial narrative.
  const title = text(snapshot.title || snapshot.organizationName);
  const what = text(snapshot.whatIsHappening);
  if (title && what && what.toLowerCase() === title.toLowerCase()) {
    fails.push("whatIsHappening:equals_title");
  }
  const potential = text(snapshot.potentialHotelBusiness);
  if (title && potential && potential.toLowerCase() === title.toLowerCase()) {
    fails.push("potentialHotelBusiness:equals_title");
  }

  if (!snapshot.secondaryContact?.name) {
    warnings.push("secondaryContact:optional_missing");
  }
  if (
    !Array.isArray(snapshot.qualificationQuestions) ||
    snapshot.qualificationQuestions.length === 0
  ) {
    warnings.push("qualificationQuestions:optional_missing");
  }
  if (!snapshot.commercialIntelligenceBrief && !snapshot.salesBrief) {
    warnings.push("commercialBrief:optional_missing");
  }

  return {
    ok: fails.length === 0,
    opportunityId: id,
    fails,
    warnings,
    contractVersion: GDI_CUSTOMER_FACING_PUBLISHED_SNAPSHOT_V1,
  };
}

/**
 * Flatten brief aliases onto a live opportunity before drawer/share projection.
 * Safe for auth + share; does not invent content.
 */
export function hydrateOpportunityFromPublishedCommercialFields(opportunity = {}) {
  const brief = resolveBrief(opportunity);
  const what = resolveWhat(opportunity, brief);
  const potential = resolvePotential(opportunity, brief);
  const whyHotel = resolveWhyHotel(opportunity, brief, potential);
  const whyActNow = resolveWhyActNow(opportunity, brief);
  const facts = resolveFacts(opportunity, brief);
  const uncertainties = resolveUncertainties(opportunity, brief);
  const questions = resolveQualificationQuestions(opportunity, brief);
  const next = resolveNextStep(opportunity, brief);
  const primary = resolvePrimaryContact(opportunity, brief);
  const secondary = resolveSecondaryContact(opportunity, brief);
  const safeBrief =
    customerSafeBrief(brief, opportunity) ||
    (what || potential || whyHotel
      ? customerSafeBrief(
          {
            whatIsHappening: what,
            potentialBusinessForThisHotel: potential,
            whyThisHotel: whyHotel,
            whyActNow,
            establishedFacts: facts,
            establishedFactsAndUncertainties: { facts, uncertainties },
            qualificationQuestions: questions,
            recommendedNextStep: next,
            whoToContact: { primary, secondary },
          },
          opportunity,
        )
      : null);

  return {
    ...opportunity,
    whatIsHappening: opportunity.whatIsHappening || what,
    summaryWhat: opportunity.summaryWhat || what,
    potentialHotelBusiness: opportunity.potentialHotelBusiness || potential,
    potentialBusiness: opportunity.potentialBusiness || potential,
    proposedHotelUse: opportunity.proposedHotelUse || potential,
    whyThisHotel: opportunity.whyThisHotel || whyHotel,
    summaryWhyHotel: opportunity.summaryWhyHotel || whyHotel,
    whyActNow: opportunity.whyActNow || whyActNow,
    whyNow: opportunity.whyNow || whyActNow,
    whyEstablishContactNow: opportunity.whyEstablishContactNow || whyActNow,
    establishedFacts:
      Array.isArray(opportunity.establishedFacts) && opportunity.establishedFacts.length
        ? opportunity.establishedFacts
        : facts,
    uncertainties:
      Array.isArray(opportunity.uncertainties) && opportunity.uncertainties.length
        ? opportunity.uncertainties
        : uncertainties,
    qualificationQuestions:
      Array.isArray(opportunity.qualificationQuestions) &&
      opportunity.qualificationQuestions.length
        ? opportunity.qualificationQuestions
        : questions,
    recommendedNextStep: opportunity.recommendedNextStep || next,
    recommendedAction: opportunity.recommendedAction || next,
    primaryContact: opportunity.primaryContact?.name
      ? opportunity.primaryContact
      : primary || opportunity.primaryContact,
    secondaryContact: opportunity.secondaryContact?.name
      ? opportunity.secondaryContact
      : secondary || opportunity.secondaryContact,
    commercialIntelligenceBrief:
      opportunity.commercialIntelligenceBrief || safeBrief,
    salesBrief: opportunity.salesBrief || safeBrief,
    commercialInventoryClass:
      opportunity.commercialInventoryClass ||
      opportunity.commercialClassification ||
      opportunity.publicationClass ||
      null,
    commercialClassification:
      opportunity.commercialClassification ||
      opportunity.commercialInventoryClass ||
      opportunity.publicationClass ||
      null,
  };
}
