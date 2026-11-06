/**
 * GDI Opportunity Research Quality V1
 *
 * Offline commercial qualification lens for Director-of-Sales review.
 * Does not publish, mutate Airtable, or invent contacts.
 *
 * Distinguishes:
 *   Demand: CONFIRMED_DEMAND | RELIABLE_INFERRED_DEMAND | SPECULATIVE_DEMAND | UNKNOWN_DEMAND
 *   Lane:   SALES_READY | ACCOUNT_DEVELOPMENT | NEEDS_MORE_RESEARCH | EXCLUDED
 *   Research completion: COMMERCIALLY_SUFFICIENT | PARTIAL | INCOMPLETE | EXHAUSTED_INCOMPLETE
 */

export const GDI_OPPORTUNITY_RESEARCH_QUALITY_V1 =
  "GDI_OPPORTUNITY_RESEARCH_QUALITY_V1";

export const DEMAND_THESIS_CLASS = Object.freeze({
  CONFIRMED_DEMAND: "CONFIRMED_DEMAND",
  RELIABLE_INFERRED_DEMAND: "RELIABLE_INFERRED_DEMAND",
  SPECULATIVE_DEMAND: "SPECULATIVE_DEMAND",
  UNKNOWN_DEMAND: "UNKNOWN_DEMAND",
});

export const QUALIFICATION_LANE = Object.freeze({
  SALES_READY: "SALES_READY",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  NEEDS_MORE_RESEARCH: "NEEDS_MORE_RESEARCH",
  EXCLUDED: "EXCLUDED",
});

export const RESEARCH_COMPLETION = Object.freeze({
  COMMERCIALLY_SUFFICIENT: "COMMERCIALLY_SUFFICIENT",
  PARTIAL: "PARTIAL",
  INCOMPLETE: "INCOMPLETE",
  EXHAUSTED_INCOMPLETE: "EXHAUSTED_INCOMPLETE",
});

function text(v) {
  return String(v ?? "").trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

function hasUrl(o) {
  const sources = o.sources || o.sourceUrls || o.evidenceUrls || [];
  if (Array.isArray(sources) && sources.some((s) => text(s?.url || s))) return true;
  return Boolean(text(o.sourceUrl || o.primarySourceUrl));
}

function contactQuality(o) {
  const primary =
    o.primaryContact ||
    o.buyerContact ||
    o.contact ||
    (Array.isArray(o.contacts) ? o.contacts[0] : null) ||
    {};
  const name = text(primary.name || primary.fullName || o.contactName);
  const role = text(primary.title || primary.role || o.contactTitle);
  const email = text(primary.email || o.contactEmail);
  const orgRoute = text(
    o.organizationRoute ||
      o.orgRoute ||
      primary.organization ||
      o.organizationName ||
      o.company
  );
  const pathClass = text(o.contactPathClass || o.whoPathClass);
  const named =
    pathClass === "NAMED_CONTACT" ||
    (name && !/unknown|tbd|n\/a/i.test(name) && (email || role));
  const functional =
    pathClass === "FUNCTIONAL_CONTACT" ||
    pathClass === "ORG_ROUTE" ||
    (!named && Boolean(orgRoute));
  const verified =
    o.contactVerified === true ||
    primary.verified === true ||
    /VERIFIED|CONFIRMED/i.test(text(o.contactVerificationStatus));
  return {
    namedBuyerOrEntryPoint: Boolean(named),
    functionalEntryPoint: Boolean(functional),
    verifiedRelevantContact: Boolean(named && verified),
    meaningfulPlannerOrIntermediary:
      /planner|dmc|pco|agency|intermediary|housing/i.test(
        `${role} ${text(o.buyerType)} ${text(o.commercialCategory)}`
      ) && Boolean(name || orgRoute),
    name: name || null,
    role: role || null,
    emailPresent: Boolean(email),
    orgRoute: orgRoute || null,
    pathClass: pathClass || null,
  };
}

function demandThesis(o) {
  const thesis = text(
    o.hotelDemandThesis ||
      o.hotelOpportunityThesis ||
      o.demandThesis ||
      o.whyThisHotel ||
      o.whyThisMatters ||
      o.proposedHotelUse
  );
  const status = text(
    o.thesisStandard || o.demandThesisClass || o.activityStatus
  ).toUpperCase();
  // Do not trust stale demandStatus labels alone — require positive confirmation evidence.
  // Negations like "without an open RFP" must not count as confirmed demand.
  const deniedConfirmation =
    /without.{0,40}open rfp|no open rfp|not claiming.{0,20}rfp|unconfirmed|no room block|without claiming/i.test(
      thesis
    );
  const confirmed =
    !deniedConfirmation &&
    (o.openRfp === true ||
      o.roomBlockConfirmed === true ||
      /\b(open rfp|active rfp|room block confirmed|contracted|awarded|confirmed booking)\b/i.test(
        thesis
      ) ||
      (status.includes("CONFIRMED") &&
        /\b(rfp|room block|contract|booking)\b/i.test(thesis) &&
        !deniedConfirmation));
  const inferred =
    status.includes("INFERRED") ||
    status.includes("RELIABLE") ||
    /recurring|annual|historically|typically books|prior years|prior cycles/i.test(
      thesis
    ) ||
    (thesis.length >= 80 && hasUrl(o) && !confirmed);

  let demandClass = DEMAND_THESIS_CLASS.UNKNOWN_DEMAND;
  if (confirmed) demandClass = DEMAND_THESIS_CLASS.CONFIRMED_DEMAND;
  else if (inferred) demandClass = DEMAND_THESIS_CLASS.RELIABLE_INFERRED_DEMAND;
  else if (thesis && thesis.length >= 40)
    demandClass = DEMAND_THESIS_CLASS.SPECULATIVE_DEMAND;
  else if (thesis) demandClass = DEMAND_THESIS_CLASS.SPECULATIVE_DEMAND;

  const hotelSpecific =
    /this (hotel|property|resort)|guestroom|meeting|catering|marina|beach|bethesda|spice|waterstone|bocas?/i.test(
      thesis
    ) || Boolean(text(o.hotelFitRationale || o.localConnection));

  return {
    demandClass,
    thesisPresent: Boolean(thesis),
    thesisLength: thesis.length,
    hotelSpecific: Boolean(hotelSpecific),
    thesisPreview: thesis ? thesis.slice(0, 220) : null,
    requiresConfirmedRfp: false, // policy: inferred demand may qualify
  };
}

function unsupportedOrNoise(o, thesis, contact) {
  const reasons = [];
  const title = lower(o.title || o.organizationName || o.company);
  if (/housing tab|competitor hotel|job posting|hiring|linkedin only/i.test(title)) {
    reasons.push("noise_title");
  }
  if (
    !thesis.thesisPresent &&
    !contact.namedBuyerOrEntryPoint &&
    !contact.functionalEntryPoint
  ) {
    reasons.push("no_thesis_no_contact");
  }
  if (
    thesis.demandClass === DEMAND_THESIS_CLASS.SPECULATIVE_DEMAND &&
    !contact.namedBuyerOrEntryPoint &&
    !hasUrl(o)
  ) {
    reasons.push("speculative_without_evidence");
  }
  if (o.customerVisible === false && o.excluded === true) {
    reasons.push("already_excluded");
  }
  if (/partnership_not_lodging|place.?management|listing.?url/i.test(text(o.exclusionReason || o.disposition))) {
    reasons.push("non_lodging_sales_path");
  }
  return reasons;
}

function researchCompletion(o, thesis, contact, flags) {
  const attempted =
    o.namedContactResearchAttempted === true ||
    o.researchAttempted === true ||
    Boolean(o.researchCompleteness);
  const exhausted =
    /EXHAUSTED/i.test(text(o.namedResearchStatus || o.researchDisposition)) ||
    o.researchExhausted === true;

  if (
    thesis.demandClass !== DEMAND_THESIS_CLASS.SPECULATIVE_DEMAND &&
    thesis.demandClass !== DEMAND_THESIS_CLASS.UNKNOWN_DEMAND &&
    thesis.hotelSpecific &&
    (contact.namedBuyerOrEntryPoint || contact.functionalEntryPoint) &&
    hasUrl(o)
  ) {
    return RESEARCH_COMPLETION.COMMERCIALLY_SUFFICIENT;
  }
  if (exhausted && flags.length) return RESEARCH_COMPLETION.EXHAUSTED_INCOMPLETE;
  if (thesis.thesisPresent || contact.namedBuyerOrEntryPoint || attempted) {
    return RESEARCH_COMPLETION.PARTIAL;
  }
  return RESEARCH_COMPLETION.INCOMPLETE;
}

function qualificationLane(o, thesis, contact, flags, completion) {
  const explicit = text(
    o.qualificationLane ||
      o.salesLane ||
      o.commercialResearchCompleteness ||
      o.actionStatus
  ).toUpperCase();
  if (flags.includes("already_excluded") || flags.includes("non_lodging_sales_path")) {
    return QUALIFICATION_LANE.EXCLUDED;
  }
  if (flags.includes("noise_title") || flags.includes("no_thesis_no_contact")) {
    return QUALIFICATION_LANE.EXCLUDED;
  }
  if (/EXCLUD|DISQUAL/i.test(explicit)) return QUALIFICATION_LANE.EXCLUDED;
  if (
    (thesis.demandClass === DEMAND_THESIS_CLASS.CONFIRMED_DEMAND ||
      thesis.demandClass === DEMAND_THESIS_CLASS.RELIABLE_INFERRED_DEMAND) &&
    thesis.hotelSpecific &&
    (contact.namedBuyerOrEntryPoint || contact.verifiedRelevantContact) &&
    completion === RESEARCH_COMPLETION.COMMERCIALLY_SUFFICIENT
  ) {
    return QUALIFICATION_LANE.SALES_READY;
  }
  if (
    (thesis.demandClass === DEMAND_THESIS_CLASS.RELIABLE_INFERRED_DEMAND ||
      thesis.demandClass === DEMAND_THESIS_CLASS.CONFIRMED_DEMAND) &&
    (contact.functionalEntryPoint ||
      contact.meaningfulPlannerOrIntermediary ||
      contact.namedBuyerOrEntryPoint) &&
    thesis.hotelSpecific
  ) {
    return QUALIFICATION_LANE.ACCOUNT_DEVELOPMENT;
  }
  if (
    thesis.thesisPresent &&
    thesis.hotelSpecific &&
    !contact.namedBuyerOrEntryPoint &&
    !contact.functionalEntryPoint
  ) {
    return QUALIFICATION_LANE.NEEDS_MORE_RESEARCH;
  }
  if (/SALES_READY/i.test(explicit) && !flags.length && contact.namedBuyerOrEntryPoint) {
    return QUALIFICATION_LANE.SALES_READY;
  }
  if (
    /ACCOUNT_DEVELOPMENT/i.test(explicit) &&
    (contact.functionalEntryPoint || contact.namedBuyerOrEntryPoint)
  ) {
    return QUALIFICATION_LANE.ACCOUNT_DEVELOPMENT;
  }
  return QUALIFICATION_LANE.NEEDS_MORE_RESEARCH;
}

/**
 * @param {object} opportunity
 * @param {{ hotelId?: string, hotelName?: string }} [ctx]
 */
export function evaluateOpportunityResearchQuality(opportunity = {}, ctx = {}) {
  const o = opportunity || {};
  const contact = contactQuality(o);
  const thesis = demandThesis(o);
  const flags = unsupportedOrNoise(o, thesis, contact);
  const completion = researchCompletion(o, thesis, contact, flags);
  const lane = qualificationLane(o, thesis, contact, flags, completion);

  const dosAnswers = {
    relevantToHotel:
      thesis.hotelSpecific &&
      lane !== QUALIFICATION_LANE.EXCLUDED &&
      thesis.demandClass !== DEMAND_THESIS_CLASS.UNKNOWN_DEMAND,
    whyNeedProperty: thesis.thesisPreview,
    evidenceSupports: hasUrl(o) || thesis.demandClass === DEMAND_THESIS_CLASS.CONFIRMED_DEMAND,
    whoToApproach:
      contact.name ||
      contact.orgRoute ||
      (contact.functionalEntryPoint ? "Organization route" : null),
    whatToSayOrInvestigate: text(
      o.recommendedAction || o.suggestedAction || o.nextAction
    ) || null,
    whenToAct: text(o.eventDateDisplay || o.needDateLabel || o.timingSummary) || null,
    stillUncertain: flags.length
      ? flags
      : completion === RESEARCH_COMPLETION.COMMERCIALLY_SUFFICIENT
        ? []
        : ["research_incomplete"],
    worthPursuing:
      lane === QUALIFICATION_LANE.SALES_READY ||
      lane === QUALIFICATION_LANE.ACCOUNT_DEVELOPMENT,
  };

  return {
    version: GDI_OPPORTUNITY_RESEARCH_QUALITY_V1,
    hotelId: ctx.hotelId || o.hotelId || null,
    opportunityId: o.id || o.opportunityId || null,
    title: text(o.title || o.organizationName || o.company) || null,
    demandClass: thesis.demandClass,
    qualificationLane: lane,
    researchCompletion: completion,
    contact,
    thesis,
    flags,
    dosAnswers,
    sourcePresent: hasUrl(o),
  };
}

export function summarizeHotelResearchQuality(rows = []) {
  const counts = {
    total: rows.length,
    SALES_READY: 0,
    ACCOUNT_DEVELOPMENT: 0,
    NEEDS_MORE_RESEARCH: 0,
    EXCLUDED: 0,
    CONFIRMED_DEMAND: 0,
    RELIABLE_INFERRED_DEMAND: 0,
    SPECULATIVE_DEMAND: 0,
    UNKNOWN_DEMAND: 0,
    verifiedRelevantContacts: 0,
    credibleDemandTheses: 0,
    hotelFitQuality: 0,
    unsupportedClaims: 0,
    commerciallySufficient: 0,
  };
  for (const r of rows) {
    counts[r.qualificationLane] = (counts[r.qualificationLane] || 0) + 1;
    counts[r.demandClass] = (counts[r.demandClass] || 0) + 1;
    if (r.contact?.verifiedRelevantContact || r.contact?.namedBuyerOrEntryPoint) {
      counts.verifiedRelevantContacts += 1;
    }
    if (
      r.demandClass === DEMAND_THESIS_CLASS.CONFIRMED_DEMAND ||
      r.demandClass === DEMAND_THESIS_CLASS.RELIABLE_INFERRED_DEMAND
    ) {
      counts.credibleDemandTheses += 1;
    }
    if (r.thesis?.hotelSpecific) counts.hotelFitQuality += 1;
    if (r.flags?.length) counts.unsupportedClaims += 1;
    if (r.researchCompletion === RESEARCH_COMPLETION.COMMERCIALLY_SUFFICIENT) {
      counts.commerciallySufficient += 1;
    }
  }
  const credibleProspects =
    (counts.SALES_READY || 0) + (counts.ACCOUNT_DEVELOPMENT || 0);
  return {
    ...counts,
    credibleProspects,
    withinQualityTargetBand: credibleProspects >= 10 && credibleProspects <= 15,
    qualityOverVolumeNote:
      "Target ~10–15 credible prospects; do not inflate counts to hit the band.",
  };
}

export function evaluateHotelOpportunityInventory(opportunities = [], ctx = {}) {
  const rows = (opportunities || []).map((o) =>
    evaluateOpportunityResearchQuality(o, ctx)
  );
  const byId = new Map();
  let duplicates = 0;
  for (const r of rows) {
    const key = lower(r.title);
    if (!key) continue;
    if (byId.has(key)) duplicates += 1;
    else byId.set(key, r.opportunityId);
  }
  return {
    version: GDI_OPPORTUNITY_RESEARCH_QUALITY_V1,
    hotelId: ctx.hotelId || null,
    hotelName: ctx.hotelName || null,
    summary: {
      ...summarizeHotelResearchQuality(rows),
      duplicateOpportunities: duplicates,
    },
    rows,
  };
}
