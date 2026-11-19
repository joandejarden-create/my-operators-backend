/**
 * GDI Discovery Intake Disposition V1
 *
 * Separates discovery provenance (yield) from commercial disposition.
 * Reuses recurring-meeting intelligence + competitor-host signals.
 * Does not invent facts, write Airtable, publish, or call paid providers.
 */

import {
  organizationKeyFor,
  programKeyFor,
  classifyMeetingCycle,
  PROGRAM_TREATMENT,
  readRecurringFromOpportunity,
} from "./recurring-meeting-intelligence-v1.js";
import { namedCompetitorHost } from "./commercial-pathway-v1.js";
import {
  evaluateCertifiedDrawerCommercialContent,
  resolveCoreCommercialBodies,
} from "../presentation/gdi-certified-drawer-commercial-content-contract-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { DEMAND_THESIS_STANDARD } from "../commercial-demand-thesis-standard-v1.js";

export const INTAKE_DISPOSITION_VERSION = "gdi-discovery-intake-disposition-v1";
export const INTAKE_DISPOSITION_AS_OF = "2026-11-19";

/** Discovery provenance — never confuse with commercial disposition. */
export const DISCOVERY_YIELD_LABEL = Object.freeze({
  LIVE_NEW: "LIVE_NEW",
  HISTORICAL_REPLAY: "HISTORICAL_REPLAY",
  FIXTURE: "FIXTURE",
  INVENTORY_REOPEN: "INVENTORY_REOPEN",
});

/** What kind of novelty (if any) was established — orthogonal to yield label. */
export const DISCOVERY_NOVELTY_KIND = Object.freeze({
  NEWLY_RETRIEVED_EVIDENCE: "NEWLY_RETRIEVED_EVIDENCE",
  NEWLY_IDENTIFIED_ORGANIZATION: "NEWLY_IDENTIFIED_ORGANIZATION",
  NEWLY_DISCOVERED_DEMAND_OPPORTUNITY: "NEWLY_DISCOVERED_DEMAND_OPPORTUNITY",
  NEWLY_QUALIFIED_OPPORTUNITY: "NEWLY_QUALIFIED_OPPORTUNITY",
  NONE: "NONE",
});

/** Commercial disposition at discovery intake. */
export const COMMERCIAL_DISPOSITION = Object.freeze({
  OPEN: "OPEN",
  COMPETITOR_PLACED: "COMPETITOR_PLACED",
  TOO_EARLY_WATCH: "TOO_EARLY_WATCH",
  SPECULATIVE: "SPECULATIVE",
});

export const PHASE2B_EXCLUDED_OPPORTUNITY_IDS = Object.freeze([
  "gdi_lmc_00562bf81b1e1957",
  "gdi_opp_2027_ddaa_annual_conference_8",
]);

/** Hosts commonly evidenced in Bethesda / CALA inventories beyond pathway defaults. */
const EXTRA_NAMED_HOSTS = Object.freeze([
  [/marriott marquis/i, "Marriott Marquis"],
  [/gaylord national/i, "Gaylord National"],
  [/the bethesdan\b/i, "The Bethesdan"],
  [/club med\b/i, "Club Med"],
  [/wewcc|walter e\.?\s*washington convention/i, "WEWCC"],
  [/hyatt regency washington/i, "Hyatt Regency Washington"],
  [/crystal gateway marriott/i, "Crystal Gateway Marriott"],
  [/bethesda north marriott|\bbethesda north\b/i, "Bethesda North Marriott"],
]);

function text(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

function opportunityIdOf(row = {}) {
  return text(row.id || row.opportunityId || row.candidateId) || null;
}

function evidenceBlob(row = {}) {
  const parts = [
    row.evidenceQuote,
    row.eventActivityEvidence,
    row.localPresence,
    row.localConnection,
    row.summaryWhat,
    row.whatIsHappening,
    row.competitorBookingClaim,
    row.cmiClassificationReason,
    row.title,
    row.displayTitle,
    ...(Array.isArray(row.establishedFacts) ? row.establishedFacts : []),
  ];
  return parts.map(text).filter(Boolean).join("\n");
}

function hotelSourcingStillOpenLanguage(blob = "") {
  return /\bhotel\s+(tba|tbd|not (?:yet )?named|not specified)\b|\bhotel not yet named\b|\bno venue is published\b|\bvenue (?:is )?tba\b/i.test(
    text(blob),
  );
}

/**
 * Host/competitor detection must use event-specific evidence only.
 * Exclude STR competitorRationale / competitors[] boilerplate (often names
 * Bethesda North etc. for every Bethesda Marriott row).
 */
function hostEvidenceBlob(row = {}) {
  const parts = [
    row.evidenceQuote,
    row.eventActivityEvidence,
    row.summaryWhat,
    row.whatIsHappening,
    row.competitorBookingClaim,
    row.title,
    row.displayTitle,
    row.opportunityName,
    // localPresence/localConnection often encode the named banquet/host venue
    row.localPresence,
    row.localConnection,
  ];
  return parts.map(text).filter(Boolean).join("\n");
}

function isSameAsTargetHotel(hostName, targetHotel) {
  const host = text(hostName).toLowerCase();
  const target = text(targetHotel).toLowerCase();
  if (!host || !target) return false;
  if (host.includes("bethesda north") && target.includes("bethesda") && !target.includes("north")) {
    return false;
  }
  return target.includes(host) || host.includes(target);
}

function resolveNamedHost(row = {}, targetHotel = "") {
  const explicit = text(row.competitorHotel || row.competitorVenue || row.hostHotel);
  if (explicit && !isSameAsTargetHotel(explicit, targetHotel)) return explicit;
  const blob = hostEvidenceBlob(row);
  // Hotel-not-yet-named / TBA language → do not treat historical STR names as hosts
  if (/\bhotel\s+(tba|tbd|not (?:yet )?named|not specified)\b/i.test(blob)) {
    return null;
  }
  const fromPathway = namedCompetitorHost(blob, targetHotel);
  if (fromPathway) return fromPathway;
  for (const [pattern, name] of EXTRA_NAMED_HOSTS) {
    if (!pattern.test(blob)) continue;
    if (!isSameAsTargetHotel(name, targetHotel)) return name;
  }
  return null;
}

function isOverflowLanguage(row = {}) {
  const blob = evidenceBlob(row);
  return (
    /overflow/i.test(blob) ||
    text(row.guestroomStatus || row.roomDemandStatus).toUpperCase() === "OVERFLOW_ONLY"
  );
}

/**
 * Label discovery provenance for a candidate or provider result.
 * Historical / fixture / inventory reopen can never be LIVE_NEW.
 */
export function labelDiscoveryYield(input = {}) {
  const retrievalMode = text(input.retrievalMode || input.mode).toUpperCase();
  const providerId = text(input.providerId);
  const candidate = input.candidate || {};
  const freshRetrievals = Number(input.freshRetrievals || 0);
  const isLiveProvider = input.isLive === true || input.providerIsLive === true;
  const knownOrgKeys = input.knownOrgKeys instanceof Set ? input.knownOrgKeys : new Set(input.knownOrgKeys || []);
  const knownProgramKeys =
    input.knownProgramKeys instanceof Set
      ? input.knownProgramKeys
      : new Set(input.knownProgramKeys || []);
  const knownOpportunityIds =
    input.knownOpportunityIds instanceof Set
      ? input.knownOpportunityIds
      : new Set(input.knownOpportunityIds || []);

  const fixture =
    candidate.fixtureOnly === true ||
    candidate.fixture === true ||
    retrievalMode === "FIXTURE" ||
    /fixture/i.test(providerId) ||
    /example\.invalid/i.test(String((candidate.evidenceUrls || [])[0] || candidate.url || ""));

  const historical =
    candidate.historical === true ||
    retrievalMode === "HISTORICAL_REPLAY" ||
    /historical/i.test(providerId);

  const orgName = text(candidate.organizationName || candidate.orgName || input.organizationName);
  const orgKey = orgName ? organizationKeyFor(orgName) : null;
  const programKey = programKeyFor({
    organizationName: orgName,
    programName: candidate.title || candidate.programName || candidate.programmeName,
    title: candidate.title,
  });
  const oppId = opportunityIdOf(candidate) || opportunityIdOf(input);

  let yieldLabel = DISCOVERY_YIELD_LABEL.LIVE_NEW;
  if (fixture) yieldLabel = DISCOVERY_YIELD_LABEL.FIXTURE;
  else if (historical) yieldLabel = DISCOVERY_YIELD_LABEL.HISTORICAL_REPLAY;
  else if (
    (oppId && knownOpportunityIds.has(oppId)) ||
    (programKey && knownProgramKeys.has(programKey)) ||
    input.inventoryReopen === true ||
    retrievalMode === "INVENTORY_REOPEN"
  ) {
    yieldLabel = DISCOVERY_YIELD_LABEL.INVENTORY_REOPEN;
  } else if (!isLiveProvider || freshRetrievals <= 0) {
    // Fail closed: without live fresh retrieval, never claim LIVE_NEW.
    if (retrievalMode === "FRESH_RETRIEVAL" && isLiveProvider && freshRetrievals > 0) {
      yieldLabel = DISCOVERY_YIELD_LABEL.LIVE_NEW;
    } else if (historical) {
      yieldLabel = DISCOVERY_YIELD_LABEL.HISTORICAL_REPLAY;
    } else if (fixture) {
      yieldLabel = DISCOVERY_YIELD_LABEL.FIXTURE;
    } else if (input.inventoryReopen === true || knownProgramKeys.has(programKey)) {
      yieldLabel = DISCOVERY_YIELD_LABEL.INVENTORY_REOPEN;
    } else {
      // Non-live zero-hit / blocked path — treat as inventory reopen only when
      // rehydrating known inventory; otherwise historical-shaped unknown.
      yieldLabel =
        input.defaultNonLiveLabel || DISCOVERY_YIELD_LABEL.HISTORICAL_REPLAY;
    }
  }

  // Hard rule: fixture/historical never LIVE_NEW
  if (
    yieldLabel === DISCOVERY_YIELD_LABEL.LIVE_NEW &&
    (fixture || historical || !isLiveProvider || freshRetrievals <= 0)
  ) {
    yieldLabel = fixture
      ? DISCOVERY_YIELD_LABEL.FIXTURE
      : historical
        ? DISCOVERY_YIELD_LABEL.HISTORICAL_REPLAY
        : DISCOVERY_YIELD_LABEL.INVENTORY_REOPEN;
  }

  const novelty = [];
  if (yieldLabel === DISCOVERY_YIELD_LABEL.LIVE_NEW) {
    if (orgKey && !knownOrgKeys.has(orgKey)) {
      novelty.push(DISCOVERY_NOVELTY_KIND.NEWLY_IDENTIFIED_ORGANIZATION);
    }
    if (programKey && !knownProgramKeys.has(programKey)) {
      novelty.push(DISCOVERY_NOVELTY_KIND.NEWLY_DISCOVERED_DEMAND_OPPORTUNITY);
    }
    if (freshRetrievals > 0) {
      novelty.push(DISCOVERY_NOVELTY_KIND.NEWLY_RETRIEVED_EVIDENCE);
    }
    if (input.newlyQualified === true) {
      novelty.push(DISCOVERY_NOVELTY_KIND.NEWLY_QUALIFIED_OPPORTUNITY);
    }
  }
  if (!novelty.length) novelty.push(DISCOVERY_NOVELTY_KIND.NONE);

  return {
    version: INTAKE_DISPOSITION_VERSION,
    discoveryYieldLabel: yieldLabel,
    discoveryNoveltyKinds: novelty,
    organizationKey: orgKey,
    programKey: programKey || null,
    claimsGenuineNewDiscovery: yieldLabel === DISCOVERY_YIELD_LABEL.LIVE_NEW,
    rationale: {
      retrievalMode: retrievalMode || null,
      providerId: providerId || null,
      isLiveProvider,
      freshRetrievals,
      fixture,
      historical,
    },
  };
}

/**
 * Evidence-grounded commercial disposition. Separate from discovery yield.
 * Competitor-placed demand is parked for sales-ready paths but retained with
 * displacement notes when overflow / alternate lodging is evidenced.
 */
export function classifyCommercialDisposition(opportunity = {}, options = {}) {
  const targetHotel = text(
    options.targetHotel ||
      opportunity.targetHotelName ||
      opportunity.hotelName ||
      opportunity.targetHotel ||
      "",
  );
  const blob = evidenceBlob(opportunity);
  const hostBlob = hostEvidenceBlob(opportunity);
  const qf = text(opportunity.qualificationFailureReason).toUpperCase();
  const demandClass = text(
    opportunity.demandEvidenceClass ||
      opportunity.demandThesisStandard ||
      opportunity.thesisStandard,
  ).toUpperCase();
  const guest = text(opportunity.guestroomStatus || opportunity.roomDemandStatus).toUpperCase();
  const host = resolveNamedHost(opportunity, targetHotel);
  const overflow = isOverflowLanguage(opportunity);
  const explicitCompetitorField =
    Boolean(text(opportunity.competitorHotel || opportunity.competitorVenue || opportunity.hostHotel)) ||
    qf === "VENUE_ALREADY_SELECTED" ||
    text(opportunity.competitorVenueStatus).toUpperCase().includes("CONFIRMED") ||
    opportunity.competitorVenueConfirmed === true;
  const sourcingOpenLanguage = hotelSourcingStillOpenLanguage(`${hostBlob}\n${blob}`);
  // When the edition itself says hotel TBA / not named, do not promote historical
  // host mentions (or STR spill) into COMPETITOR_PLACED unless structural fields say so.
  const venueSelected =
    explicitCompetitorField ||
    (Boolean(host) && !sourcingOpenLanguage) ||
    (guest === "OVERFLOW_ONLY" && Boolean(host));

  const cycle = classifyMeetingCycle(
    {
      organizationName: opportunity.organizationName || opportunity.company,
      programName: opportunity.programName || opportunity.title || opportunity.displayTitle,
      title: opportunity.title || opportunity.displayTitle,
      startDate: opportunity.eventStartDate || opportunity.tileEventStartDate,
      endDate: opportunity.eventEndDate || opportunity.tileEventEndDate,
      evidenceQuote: hostBlob,
      sourceUrl:
        opportunity.officialSource ||
        opportunity.sourceUrl ||
        (Array.isArray(opportunity.sources) ? opportunity.sources[0]?.url : null),
      host: venueSelected ? host : null,
    },
    { targetHotel },
  );

  let disposition = COMMERCIAL_DISPOSITION.OPEN;
  const reasons = [];

  const cycleCommitted =
    !sourcingOpenLanguage &&
    (cycle.sourcingStatus === "HOST_SELECTED" ||
      cycle.sourcingStatus === "MULTI_YEAR_AGREEMENT");

  if (venueSelected || cycleCommitted) {
    disposition = COMMERCIAL_DISPOSITION.COMPETITOR_PLACED;
    reasons.push(
      host
        ? `Named host/competitor evidenced: ${host}`
        : `Sourcing status ${cycle.sourcingStatus || "HOST_SELECTED"}`,
    );
  } else if (
    qf === "TOO_EARLY" ||
    /lodging (?:need|requirement|block).{0,48}(?:not|un)(?: yet )?(?:published|available|announced)/i.test(
      blob,
    ) ||
    /housing (?:page|block|partner).{0,40}(?:not|un)(?: yet )?(?:published|available|announced)/i.test(
      blob,
    )
  ) {
    disposition = COMMERCIAL_DISPOSITION.TOO_EARLY_WATCH;
    reasons.push(
      qf === "TOO_EARLY"
        ? "qualificationFailureReason=TOO_EARLY"
        : "Lodging/housing not yet published",
    );
  }

  if (
    disposition === COMMERCIAL_DISPOSITION.OPEN &&
    (demandClass === DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND ||
      demandClass === "SPECULATIVE" ||
      demandClass === "SPECULATIVE_DEMAND")
  ) {
    disposition = COMMERCIAL_DISPOSITION.SPECULATIVE;
    reasons.push(`demandEvidenceClass=${demandClass || "SPECULATIVE"}`);
  }

  // Banquet / day-meeting NOT_REQUIRED with named competitor venue → competitor placed
  if (
    disposition === COMMERCIAL_DISPOSITION.OPEN &&
    guest === "NOT_REQUIRED" &&
    host
  ) {
    disposition = COMMERCIAL_DISPOSITION.COMPETITOR_PLACED;
    reasons.push(`guestroomStatus=NOT_REQUIRED with named venue ${host}`);
  }

  const displacementOpportunity =
    disposition === COMMERCIAL_DISPOSITION.COMPETITOR_PLACED &&
    (overflow ||
      cycle.treatment === PROGRAM_TREATMENT.COMPETITIVE_OVERFLOW ||
      guest === "OVERFLOW_ONLY");

  let watch = null;
  if (disposition === COMMERCIAL_DISPOSITION.TOO_EARLY_WATCH) {
    watch = {
      evidenceGap:
        text(opportunity.qualificationFailureReasonLabel) ||
        "Lodging demand, housing page, or open hotel sourcing is not yet evidenced.",
      revisitTrigger:
        "Revisit when an official housing/hotel page, RFP, or named open sourcing statement appears.",
      suggestedReviewTiming: opportunity.eventStartDate
        ? `Checkpoint 9–12 months before ${String(opportunity.eventStartDate).slice(0, 10)} (watch only — not an open RFP).`
        : "Checkpoint in 90 days or when the organizer publishes lodging details.",
    };
  } else if (disposition === COMMERCIAL_DISPOSITION.COMPETITOR_PLACED && displacementOpportunity) {
    watch = {
      evidenceGap: "Primary host is placed; overflow/alternate lodging is hypothesized or partial.",
      revisitTrigger: "Revisit if the host block sells out or overflow hotels are solicited publicly.",
      suggestedReviewTiming: "Monitor 60–120 days before event start for overflow solicitation.",
    };
  }

  return {
    version: INTAKE_DISPOSITION_VERSION,
    commercialDisposition: disposition,
    competitorHost: host || null,
    displacementOpportunity: Boolean(displacementOpportunity),
    preserveAsCompetitiveIntelligence: disposition === COMMERCIAL_DISPOSITION.COMPETITOR_PLACED,
    rejectFromSalesReadyAutomatically: disposition === COMMERCIAL_DISPOSITION.COMPETITOR_PLACED,
    autoRejectOpportunity: false, // never delete competitor-placed rows
    cycleTreatment: cycle.treatment || null,
    sourcingStatus: cycle.sourcingStatus || null,
    reasons,
    watch,
    organizationKey: organizationKeyFor(opportunity.organizationName || opportunity.company || ""),
    programKey: programKeyFor({
      organizationName: opportunity.organizationName || opportunity.company,
      programName: opportunity.programName || opportunity.title || opportunity.displayTitle,
      title: opportunity.title || opportunity.displayTitle,
    }),
  };
}

/**
 * Shared opportunity identity for cross-hotel reuse without a parallel DB.
 */
export function resolveSharedOpportunityIdentity(opportunity = {}) {
  const organizationName = text(opportunity.organizationName || opportunity.company);
  const programName = text(
    opportunity.programName || opportunity.title || opportunity.displayTitle || opportunity.opportunityName,
  );
  const organizationKey = organizationKeyFor(organizationName);
  const programKey = programKeyFor({
    organizationName,
    programName,
    title: programName,
  });
  return {
    version: INTAKE_DISPOSITION_VERSION,
    opportunityId: opportunityIdOf(opportunity),
    organizationName: organizationName || null,
    organizationKey: organizationKey || null,
    programName: programName || null,
    programKey: programKey || null,
    sharedDemandKey: programKey || organizationKey || opportunityIdOf(opportunity),
  };
}

/**
 * Attach hotel-specific evaluation state without duplicating org research.
 */
export function attachHotelSpecificEvaluation(sharedIdentity, hotelContext = {}, priorResearch = null) {
  return {
    version: INTAKE_DISPOSITION_VERSION,
    sharedDemandKey: sharedIdentity?.sharedDemandKey || null,
    organizationKey: sharedIdentity?.organizationKey || null,
    programKey: sharedIdentity?.programKey || null,
    hotelId: hotelContext.hotelId || null,
    hotelName: hotelContext.hotelName || hotelContext.displayName || null,
    reusedPriorResearch: Boolean(priorResearch),
    priorResearchFingerprint: priorResearch?.evidenceFingerprint || priorResearch?.contentHash || null,
    hotelSpecificState: {
      disposition: null,
      drawerReadiness: null,
      fitNotes: null,
    },
    duplicateResearchAvoided: Boolean(priorResearch),
  };
}

/**
 * Drawer readiness — missing fields listed explicitly, never fabricated.
 */
export function assessOpportunityDrawerReadiness(opportunity = {}, opts = {}) {
  const bodies = resolveCoreCommercialBodies(opportunity);
  const certified = evaluateCertifiedDrawerCommercialContent(opportunity, opts);
  const contact =
    opportunity.primaryContact ||
    opportunity.canonicalCustomerPresentation?.contact ||
    {};
  const namedBuyer = text(contact.name);
  const buyerRole = text(contact.title || contact.role || contact.professionalTitle);
  const demandMechanism = text(
    bodies.whatIsHappening ||
      opportunity.demandMechanism ||
      opportunity.eventActivityEvidence ||
      opportunity.summaryWhat,
  );
  const locationTiming = Boolean(
    opportunity.eventStartDate ||
      opportunity.tileEventStartDate ||
      opportunity.eventLocationSummary ||
      /20\d{2}/.test(evidenceBlob(opportunity)),
  );
  const hotelRelevance = Boolean(text(bodies.whyThisHotel));
  const provenance = Boolean(
    (Array.isArray(opportunity.sources) && opportunity.sources.length) ||
      opportunity.officialSource ||
      opportunity.sourceUrl ||
      (Array.isArray(opportunity.evidenceUrls) && opportunity.evidenceUrls.length),
  );
  const freshness = text(
    opportunity.retrievedOn ||
      opportunity.lastResearchedAt ||
      opportunity.researchFreshness ||
      opportunity.updatedAt ||
      "",
  );

  const gaps = [];
  if (!demandMechanism) gaps.push("evidence_backed_demand_mechanism");
  if (!locationTiming) gaps.push("relevant_location_and_timing");
  if (!hotelRelevance) gaps.push("hotel_specific_commercial_relevance");
  if (!namedBuyer || !buyerRole) gaps.push("credible_named_buyer_or_entry_point");
  if (!text(bodies.whyActNow)) gaps.push("whyActNow");
  if (!text(bodies.recommendedNextStep)) gaps.push("recommendedNextStep");
  if (!provenance) gaps.push("evidence_provenance");
  if (!freshness) gaps.push("research_freshness");

  // Surface certified defects as remaining research gaps (do not invent copy).
  for (const d of certified.defects || []) {
    const code = `certified:${d.code}:${d.detail || ""}`.replace(/:$/, "");
    if (!gaps.includes(code)) gaps.push(code);
  }

  return {
    version: INTAKE_DISPOSITION_VERSION,
    opportunityId: opportunityIdOf(opportunity),
    ready: gaps.length === 0 && certified.ok === true,
    fields: {
      evidenceBackedDemandMechanism: Boolean(demandMechanism),
      relevantLocationAndTiming: locationTiming,
      hotelSpecificCommercialRelevance: hotelRelevance,
      credibleNamedBuyerOrEntryPoint: Boolean(namedBuyer && buyerRole),
      namedBuyer: namedBuyer || null,
      buyerRole: buyerRole || null,
      whyActNow: Boolean(text(bodies.whyActNow)),
      recommendedNextStep: Boolean(text(bodies.recommendedNextStep)),
      evidenceProvenance: provenance,
      researchFreshness: freshness || null,
    },
    gaps,
    certifiedDrawer: {
      ok: certified.ok,
      emptyCoreCount: certified.emptyCoreCount,
      defectCount: (certified.defects || []).length,
    },
  };
}

/**
 * Compose full intake assessment for one opportunity/candidate.
 */
export function assessDiscoveryIntake(opportunity = {}, options = {}) {
  const yieldInfo = labelDiscoveryYield({
    candidate: opportunity,
    retrievalMode: options.retrievalMode || opportunity.retrievalMode || opportunity.sourceFamily,
    providerId: options.providerId,
    isLive: options.isLive,
    freshRetrievals: options.freshRetrievals,
    knownOrgKeys: options.knownOrgKeys,
    knownProgramKeys: options.knownProgramKeys,
    knownOpportunityIds: options.knownOpportunityIds,
    inventoryReopen: options.inventoryReopen,
    newlyQualified: options.newlyQualified,
    defaultNonLiveLabel: options.defaultNonLiveLabel,
  });
  const disposition = classifyCommercialDisposition(opportunity, options);
  const shared = resolveSharedOpportunityIdentity(opportunity);
  const drawer = assessOpportunityDrawerReadiness(opportunity, options);
  const hotelAttach = attachHotelSpecificEvaluation(shared, {
    hotelId: options.hotelId || opportunity.hotelId,
    hotelName: options.targetHotel || opportunity.hotelName,
  }, options.priorResearch || null);
  hotelAttach.hotelSpecificState = {
    disposition: disposition.commercialDisposition,
    drawerReadiness: drawer.ready,
    fitNotes: disposition.reasons.slice(0, 3),
  };

  let eligibilityShadow = null;
  if (options.runEligibilityShadow === true) {
    eligibilityShadow = evaluateSharedCommercialEligibility(opportunity, {
      hotel: options.hotel || null,
    });
  }

  const blocksSalesReady =
    disposition.commercialDisposition === COMMERCIAL_DISPOSITION.COMPETITOR_PLACED ||
    disposition.commercialDisposition === COMMERCIAL_DISPOSITION.SPECULATIVE ||
    disposition.commercialDisposition === COMMERCIAL_DISPOSITION.TOO_EARLY_WATCH;

  return {
    version: INTAKE_DISPOSITION_VERSION,
    asOf: INTAKE_DISPOSITION_AS_OF,
    opportunityId: opportunityIdOf(opportunity),
    discoveryYield: yieldInfo,
    commercialDisposition: disposition,
    sharedIdentity: shared,
    hotelEvaluation: hotelAttach,
    drawerReadiness: drawer,
    eligibilityShadow,
    salesReadyBlockedByIntake: blocksSalesReady,
    phase2bExcluded: PHASE2B_EXCLUDED_OPPORTUNITY_IDS.includes(opportunityIdOf(opportunity)),
  };
}

/**
 * Guard: speculative / competitor-placed / too-early must not promote to SALES_READY
 * via intake alone. Does not replace shared eligibility — overlays a hard block.
 */
export function applyIntakeSalesReadyGate(eligibilityResult = {}, dispositionResult = {}) {
  const disposition =
    dispositionResult.commercialDisposition ||
    dispositionResult.disposition ||
    null;
  const status = text(eligibilityResult.status || eligibilityResult.eligibilityStatus);
  const blocked =
    disposition === COMMERCIAL_DISPOSITION.SPECULATIVE ||
    disposition === COMMERCIAL_DISPOSITION.COMPETITOR_PLACED ||
    disposition === COMMERCIAL_DISPOSITION.TOO_EARLY_WATCH;

  if (!blocked || status !== "SALES_READY") {
    return {
      ...eligibilityResult,
      intakeGateApplied: true,
      intakeDisposition: disposition,
      intakeBlockedSalesReady: false,
    };
  }
  return {
    ...eligibilityResult,
    status: "RESEARCH_CONTINUATION",
    salesReady: false,
    intakeGateApplied: true,
    intakeDisposition: disposition,
    intakeBlockedSalesReady: true,
    gaps: [
      ...(eligibilityResult.gaps || []),
      `INTAKE_DISPOSITION_BLOCKS_SALES_READY:${disposition}`,
    ],
  };
}

export function buildKnownIndexFromOpportunities(opportunities = []) {
  const knownOrgKeys = new Set();
  const knownProgramKeys = new Set();
  const knownOpportunityIds = new Set();
  for (const opp of opportunities) {
    const id = opportunityIdOf(opp);
    if (id) knownOpportunityIds.add(id);
    const shared = resolveSharedOpportunityIdentity(opp);
    if (shared.organizationKey) knownOrgKeys.add(shared.organizationKey);
    if (shared.programKey) knownProgramKeys.add(shared.programKey);
  }
  return { knownOrgKeys, knownProgramKeys, knownOpportunityIds };
}

export {
  organizationKeyFor,
  programKeyFor,
  readRecurringFromOpportunity,
  PROGRAM_TREATMENT,
};
