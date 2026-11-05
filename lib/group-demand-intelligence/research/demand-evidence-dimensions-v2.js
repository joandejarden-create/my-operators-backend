/**
 * Demand evidence dimensions V2 — audit / proposed pre-sourcing disposition.
 *
 * Separates:
 *   VERIFIED_GROUP_ACTIVITY      — source-backed activity that could generate demand
 *   INFERRED_LODGING_POTENTIAL   — credible overnight potential (not a confirmed room block)
 *   VERIFIED_ACCOMMODATION_DEMAND — direct guestrooms / sourcing / housing / contracted inventory
 *
 * Does NOT change production publication eligibility.
 * Recommended for ACCOUNT_LED / early EVENT_LED internal pre-sourcing only.
 */

import {
  classifyAccommodationEvidenceRole,
  qualifyHotelSpecificProspectV2,
} from "./hotel-specific-qualification-v2.js";
import {
  RESEARCH_PATHWAY,
  COMMERCIAL_STAGE,
  SALES_READINESS,
  namedCompetitorHost,
} from "./commercial-pathway-v1.js";

export const DEMAND_EVIDENCE_DIMENSIONS_VERSION = "gdi-demand-evidence-dimensions-v2";

export const DEMAND_EVIDENCE_LAYER = Object.freeze({
  VERIFIED_GROUP_ACTIVITY: "VERIFIED_GROUP_ACTIVITY",
  INFERRED_LODGING_POTENTIAL: "INFERRED_LODGING_POTENTIAL",
  VERIFIED_ACCOMMODATION_DEMAND: "VERIFIED_ACCOMMODATION_DEMAND",
});

export const DOWNGRADE_CAUSE = Object.freeze({
  MISSING_GROUP_ACTIVITY: "MISSING_EVIDENCE_OF_MEETING_GROUP_INCENTIVE_OR_TRAVEL_ACTIVITY",
  MISSING_EXPLICIT_GUESTROOM_DESPITE_CREDIBLE_DEMAND: "MISSING_EXPLICIT_GUESTROOM_OR_ROOM_BLOCK_DESPITE_CREDIBLE_DEMAND_ACTIVITY",
  MISSING_HOTEL_SPECIFIC_FIT: "MISSING_HOTEL_SPECIFIC_GEOGRAPHIC_OR_COMMERCIAL_FIT",
  MISSING_CONTACT_ROUTE: "MISSING_RELEVANT_CONTACT_ROUTE",
  COMPETITOR_HOTEL_COMMITMENT: "CONFIRMED_COMPETING_HOTEL_COMMITMENT",
  VENUE_LISTING_OR_LOCAL_ONLY: "GENUINE_VENUE_LISTING_OR_LOCAL_ONLY_ACTIVITY",
  EVIDENCE_MAPPING_OR_CLASSIFIER_DEFECT: "EVIDENCE_MAPPING_OR_CLASSIFICATION_DEFECT",
  ALREADY_USEFUL_OR_RECOVERED: "ALREADY_INTERNALLY_USEFUL_OR_RECOVERED",
});

/** Multi-day / travel / residential / association programmes that often need lodging. */
const GROUP_ACTIVITY = /\b(?:incentive\s+programm?e|corporate\s+meetings?|corporate\s+groups?|association\s+(?:meeting|conference|congress|event)|annual\s+(?:meeting|conference|congress)|conference|congress|summit|symposium|training\s+(?:programme|program|event)|delegate|hill\s*day|advocacy\s*day|national\s+(?:meeting|conference)|multi[- ]?day|residential\s+meetings?|group\s+travel|incentive\s+travel|scientific\s+(?:meeting|congress)|medical\s+(?:meeting|conference)|chapter\s+(?:meeting|conference))\b/i;

/**
 * Parent-org / CMS navigation chrome that must not create guestroom inference.
 * e.g. chapter sites listing "CMAA National Conference" beside Join / Mission links.
 */
const PARENT_ORG_NAV_CHROME = /\b(?:CMAA\s+National(?:\s+(?:Conference|Mission\s*\/\s*Vision|Professional\s+Development(?:\s+Information)?|Focus|Ethic[s]?))?|Join\s+CMAA|You\s+are\s+viewing\s+this\s+design\s+in\s+preview\s+mode|design\s+MUST\s+be\s+published|BOMA\s+International|BOMA\s+California|Member\s+Portal\s+Login|Create\s+Your\s+My\s+\w+\s+Account)\b/gi;

/** Explicit exclusion: day banquet / local luncheon / venue hire without travel. */
const LOCAL_OR_DAY_ONLY = /\b(?:awards?\s+(?:banquet|dinner|luncheon)|speaker\s+luncheon|recognition\s+luncheon|gala\b|reception\b|ballroom\s+(?:hire|rental)|outdoor\s+festivals?|nightclub|local\s+(?:chapter\s+)?(?:lunch|dinner|meeting)|day\s+events?\s+only)\b/i;

/** Defensible inferred lodging — explains WHY overnight is plausible without inventing a block. */
const INFERRED_LODGING = /\b(?:incentive\s+(?:programm?e|travel)|residential\s+meetings?|multi[- ]?day|delegates?\s+(?:from|travel)|out[- ]?of[- ]?town|national\s+(?:association|meeting|conference)|hill\s*day|advocacy\s*day|overflow|satellite\s+meetings?|housing\s+(?:needed|required|pattern)|attendees?\s+(?:travel|stay)|programme\s+base\s+hotel|group\s+accommodation|hotel[\s-]*sourc|accommodation\s+services|corporate\s+meetings?\s+and\s+incentives)\b/i;

/**
 * Strip generic parent-org navigation before demand-layer assessment.
 * Does not remove chapter-specific programme prose.
 */
export function stripParentOrgNavChrome(raw = "") {
  return String(raw || "")
    .replace(PARENT_ORG_NAV_CHROME, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Patterns that look like lodging but are hotel inventory / venue amenity prose. */
const HOTEL_INVENTORY_OR_AMENITY = /\b(?:\d+\s+guestrooms?|\d+\s+bedrooms?\s+and\s+suites|supplied\s+(?:hotel|property)\s+(?:facts?|profile)|physical\s+capacity)\b/i;

const VENUE_FINDER = /\b(?:free\s+venue\s+find|venue\s+(?:listing|hire|finding|sourcing)|exact[- ]property\s+(?:listing|ballroom)|bookable\s+(?:london\s+)?event\s+venue|layout\s*&\s*spaces)\b/i;

/**
 * Assess the three demand-evidence layers from frozen claims/text.
 */
export function assessDemandEvidenceDimensions({
  acceptedClaims = [],
  pageTexts = [],
  organizationName = "",
  opportunity = {},
} = {}) {
  const blobRaw = [
    ...acceptedClaims.map((c) => `${c.statement || ""} ${c.excerpt || ""}`),
    ...pageTexts,
    opportunity.summaryWhyHotel,
    opportunity.evidenceQuote,
    opportunity.eventLocationSummary,
    organizationName,
  ].join("\n");
  const blob = stripParentOrgNavChrome(blobRaw);

  const accommodation = classifyAccommodationEvidenceRole({
    acceptedClaims,
    pageTexts,
    organizationName,
  });

  const venueFinder = VENUE_FINDER.test(blob) || accommodation.venueListingOnly;
  const localOrDayOnly = LOCAL_OR_DAY_ONLY.test(blob) && !INFERRED_LODGING.test(blob);
  // Strip inventory lines before judging activity/inference.
  const demandSide = blob
    .split("\n")
    .filter((line) => !HOTEL_INVENTORY_OR_AMENITY.test(line) || GROUP_ACTIVITY.test(line) || INFERRED_LODGING.test(line))
    .join("\n");

  const blobWithoutOrgName = stripParentOrgNavChrome([
    ...acceptedClaims.map((c) => `${c.statement || ""} ${c.excerpt || ""}`),
    ...pageTexts,
    opportunity.summaryWhyHotel,
    opportunity.evidenceQuote,
    opportunity.eventLocationSummary,
  ].join("\n"));
  // National Conference nav chrome alone is not chapter programme activity.
  const onlyNationalConferenceNav = /\bnational\s+conference\b/i.test(blobWithoutOrgName)
    && !/\b(?:awards?\s+(?:banquet|dinner)|chapter\s+(?:meeting|event|banquet)|incentive|residential|multi[- ]?day|room\s*block|housing|delegate)\b/i.test(blobWithoutOrgName)
    && !Boolean(opportunity.eventName || opportunity.meetingName || opportunity.programmeName);
  const groupActivityInSources = (!onlyNationalConferenceNav && GROUP_ACTIVITY.test(blobWithoutOrgName))
    || Boolean(opportunity.eventName || opportunity.meetingName || opportunity.programmeName);
  // Org-name tokens alone (e.g. "… Congress") are insufficient — need source-backed activity.
  const groupActivityOrgNameOnly = !groupActivityInSources && GROUP_ACTIVITY.test(organizationName || "");
  const verifiedGroupActivity = !venueFinder && groupActivityInSources;

  const verifiedAccommodationDemand = accommodation.guestroomOrHotelSourcingControl === true
    || /\b(?:room\s*block|delegate\s*housing|housing\s*block|contract(?:ed|ing)\s+(?:hotel|rooms)|peak\s*rooms?)\b/i.test(demandSide);

  // Inferred lodging: credible explanation of overnight potential — NOT venue amenity prose,
  // NOT local awards banquet alone, NOT mere "hotel has 600 guestrooms".
  let inferredLodgingPotential = false;
  let inferredLodgingReason = null;
  if (verifiedAccommodationDemand) {
    inferredLodgingPotential = true;
    inferredLodgingReason = "Verified accommodation demand implies lodging potential.";
  } else if (venueFinder) {
    inferredLodgingPotential = false;
    inferredLodgingReason = "Venue listing / finder activity does not imply guestroom purchasing control or lodging demand.";
  } else if (localOrDayOnly) {
    inferredLodgingPotential = false;
    inferredLodgingReason = "Local/day banquet, luncheon, or reception activity without travel/residential signals.";
  } else if (verifiedGroupActivity && INFERRED_LODGING.test(blobWithoutOrgName)) {
    // Reject pure hotel-fit overflow prose when it is the only lodging signal and activity is thin.
    const onlyFitOverflowProse = /^\s*(?:NIH-corridor\s+)?overflow\b/i.test(String(opportunity.summaryWhyHotel || "").trim())
      && !/incentive|residential|multi[- ]?day|delegates?|hill\s*day|advocacy|hotel[\s-]*sourc|accommodation\s+services|corporate\s+meetings?\s+and\s+incentives/i.test(blobWithoutOrgName);
    // Reject guestroom inference from generic "National Conference" nav / parent-org links alone.
    const onlyNationalConferenceLodgingSignal = /\bnational\s+(?:association|meeting|conference)\b/i.test(blobWithoutOrgName)
      && !/\b(?:incentive|residential|multi[- ]?day|delegates?|out[- ]?of[- ]?town|hill\s*day|advocacy|hotel[\s-]*sourc|accommodation\s+services|corporate\s+meetings?\s+and\s+incentives|housing|room\s*block)\b/i.test(blobWithoutOrgName);
    if (onlyFitOverflowProse) {
      inferredLodgingPotential = false;
      inferredLodgingReason = "Hotel-fit overflow prose alone is not defensible inferred lodging without programme/travel evidence.";
    } else if (onlyNationalConferenceLodgingSignal) {
      inferredLodgingPotential = false;
      inferredLodgingReason = "Generic National Conference navigation is not defensible chapter lodging demand.";
    } else {
      inferredLodgingPotential = true;
      inferredLodgingReason = "Verified group/incentive/association activity with defensible multi-day, delegate-travel, residential, or DMC programme signals.";
    }
  } else if (verifiedGroupActivity && /DMC|destination\s+management|incentive/i.test(organizationName + blob)
    && /corporate\s+meetings?|incentive|London\s+event\s+delivery|group/i.test(blobWithoutOrgName)) {
    inferredLodgingPotential = true;
    inferredLodgingReason = "DMC/incentive delivery of corporate meetings/programmes supports inferred lodging potential without a named room block.";
  } else {
    inferredLodgingPotential = false;
    inferredLodgingReason = groupActivityOrgNameOnly
      ? "Organization name suggests a congress/association but sources do not yet verify demand-generating activity."
      : verifiedGroupActivity
        ? "Group activity present but overnight potential is not yet defensible from sources (no multi-day/travel/residential/DMC programme signal)."
        : "No verified demand-generating group activity from which to infer lodging.";
  }

  const supportingExcerpts = [];
  for (const c of acceptedClaims) {
    const s = String(c.statement || c.excerpt || "");
    if (!s) continue;
    if (GROUP_ACTIVITY.test(s) || INFERRED_LODGING.test(s) || accommodation.guestroomOrHotelSourcingControl) {
      supportingExcerpts.push({
        statement: s.slice(0, 280),
        url: c.url || null,
        provenance: c.provenance || c.topic || null,
      });
    }
  }
  if (!supportingExcerpts.length && opportunity.summaryWhyHotel) {
    supportingExcerpts.push({
      statement: String(opportunity.summaryWhyHotel).slice(0, 280),
      url: opportunity.officialSource || null,
      provenance: "frozen_corpus_summary",
    });
  }

  return {
    version: DEMAND_EVIDENCE_DIMENSIONS_VERSION,
    layers: {
      [DEMAND_EVIDENCE_LAYER.VERIFIED_GROUP_ACTIVITY]: {
        present: verifiedGroupActivity,
      },
      [DEMAND_EVIDENCE_LAYER.INFERRED_LODGING_POTENTIAL]: {
        present: inferredLodgingPotential,
        reason: inferredLodgingReason,
      },
      [DEMAND_EVIDENCE_LAYER.VERIFIED_ACCOMMODATION_DEMAND]: {
        present: verifiedAccommodationDemand,
        role: accommodation.role,
      },
    },
    venueFinderOrListing: venueFinder,
    localOrDayOnly,
    groupActivityOrgNameOnly,
    supportingExcerpts: supportingExcerpts.slice(0, 4),
    accommodationRole: accommodation.role,
  };
}

/**
 * Proposed internal pre-sourcing disposition (AUDIT ONLY — not production publication).
 * ACCOUNT_LED / early EVENT_LED may qualify on:
 *   verified group activity + inferred lodging potential + hotel fit + contact
 * without requiring verified accommodation demand / RFP / room block / room count.
 */
export function proposePreSourcingDisposition({
  opportunity = {},
  acceptedClaims = [],
  pageTexts = [],
  targetHotel = "",
  usableContactRoute = null,
  productionQualification = null,
} = {}) {
  const prod = productionQualification || qualifyHotelSpecificProspectV2({
    opportunity,
    acceptedClaims,
    pageTexts,
    targetHotel,
    usableContactRoute,
  });

  const dims = assessDemandEvidenceDimensions({
    acceptedClaims,
    pageTexts,
    organizationName: opportunity.organizationName || "",
    opportunity,
  });

  const contact = usableContactRoute
    || opportunity.contactEmail
    || opportunity.primaryContactEmail
    || opportunity.contact?.businessEmail
    || null;
  const contactOk = Boolean(contact && /@/.test(String(contact)));

  const evidenceBlob = [
    ...acceptedClaims.map((c) => c.statement || ""),
    ...pageTexts,
    opportunity.summaryWhyHotel,
    opportunity.eventLocationSummary,
    opportunity.namedHostHotel,
  ].join("\n");
  const competitor = namedCompetitorHost(evidenceBlob, targetHotel)
    || opportunity.namedHostHotel
    || null;

  const geoRejected = prod.hotelSuitability === "REJECTED"
    || prod.basePathwayEvaluation?.pathway === null
      && /SoccerPlex|Boyds\s+MD|Capitol Hill|Crystal City|Germantown/i.test(evidenceBlob)
      && /Bethesda Marriott/i.test(targetHotel)
      && !/Bethesda/i.test(opportunity.eventLocationSummary || opportunity.city || "");

  // Geography: use production geography when available; soccer/Capitol Hill patterns for Bethesda
  const weakBethesdaFit = /Bethesda Marriott/i.test(targetHotel)
    && /SoccerPlex|Boyds\s+MD|Capitol Hill|Hyatt Regency Washington/i.test(evidenceBlob)
    && !/Bethesda/i.test(`${opportunity.eventLocationSummary || ""} ${opportunity.city || ""}`);

  const weakWyndhamAsOpenHost = /Hilton San Diego Bayfront|Gaylord National/i.test(evidenceBlob)
    && /Wyndham/i.test(targetHotel);

  let proposedUseful = false;
  let proposedKind = null;
  let proposedPathway = prod.pathway;
  let proposedStage = COMMERCIAL_STAGE.ACCOUNT_DEVELOPMENT;
  let proposedReadiness = SALES_READINESS.NEEDS_RESEARCH;
  let proposedAction = null;
  let downgradeCause = null;
  let changeReason = null;

  // Preserve venue exclusions
  if (dims.venueFinderOrListing) {
    downgradeCause = DOWNGRADE_CAUSE.VENUE_LISTING_OR_LOCAL_ONLY;
    proposedAction = "Exclude venue-listing / finder orgs from guestroom pre-sourcing until accommodation-control evidence appears.";
  } else if (dims.localOrDayOnly && !dims.layers.INFERRED_LODGING_POTENTIAL.present) {
    downgradeCause = DOWNGRADE_CAUSE.VENUE_LISTING_OR_LOCAL_ONLY;
    proposedAction = "Local/day banquet or luncheon without travel/residential signals — do not auto-qualify for guestrooms.";
  } else if (competitor && !prod.competitiveFutureCycleProspect && !prod.commerciallyUsefulInternal) {
    // Competitor commitment — only future-cycle if production already said so or explicit future language
    const future = /overflow|future\s*cycle|next\s+(?:year|cycle)|202[8-9]|satellite|alternate/i.test(evidenceBlob);
    if (future && contactOk) {
      proposedUseful = true;
      proposedKind = "COMPETITIVE_FUTURE_CYCLE";
      proposedPathway = RESEARCH_PATHWAY.COMPETITIVE_FUTURE_CYCLE;
      proposedStage = COMMERCIAL_STAGE.OTHER_HOST_PLACED;
      proposedReadiness = SALES_READINESS.NEEDS_RESEARCH;
      proposedAction = `${competitor} is committed/announced. Pursue future-cycle/overflow only — sourcing status remains UNKNOWN unless verified.`;
      changeReason = prod.commerciallyUsefulInternal
        ? null
        : "Competitor host with future-cycle language + contact — internal competitive prospect (not open host).";
    } else {
      downgradeCause = DOWNGRADE_CAUSE.COMPETITOR_HOTEL_COMMITMENT;
      proposedAction = `Do not present as open host. ${competitor || "Competitor"} is named.`;
    }
  } else if (weakBethesdaFit || geoRejected) {
    downgradeCause = DOWNGRADE_CAUSE.MISSING_HOTEL_SPECIFIC_FIT;
    proposedAction = "Geography/programme site does not support target-hotel catchment.";
  } else if (weakWyndhamAsOpenHost && !/overflow|alternate|future/i.test(evidenceBlob)) {
    downgradeCause = DOWNGRADE_CAUSE.COMPETITOR_HOTEL_COMMITMENT;
    proposedAction = "Competitor/primary venue commitment — not open host for Wyndham.";
  } else if (!contactOk) {
    downgradeCause = DOWNGRADE_CAUSE.MISSING_CONTACT_ROUTE;
    proposedAction = "Recover or find a usable organization contact before sales use.";
  } else if (!dims.layers.VERIFIED_GROUP_ACTIVITY.present) {
    downgradeCause = DOWNGRADE_CAUSE.MISSING_GROUP_ACTIVITY;
    proposedAction = "Need source-backed meeting, group, incentive, or travel activity before pre-sourcing.";
  } else if (
    dims.layers.VERIFIED_GROUP_ACTIVITY.present
    && dims.layers.INFERRED_LODGING_POTENTIAL.present
    && contactOk
  ) {
    // Core proposed rule: group activity + inferred lodging + contact + fit
    proposedUseful = true;
    proposedKind = dims.layers.VERIFIED_ACCOMMODATION_DEMAND.present
      ? (prod.usefulKind || "ACCOUNT_LED")
      : (prod.pathway === RESEARCH_PATHWAY.EVENT_LED ? "EVENT_LED_PRE_SOURCING" : "ACCOUNT_LED_PRE_SOURCING");
    proposedPathway = prod.pathway === RESEARCH_PATHWAY.EVENT_LED
      ? RESEARCH_PATHWAY.EVENT_LED
      : RESEARCH_PATHWAY.ACCOUNT_LED;
    proposedStage = COMMERCIAL_STAGE.ACCOUNT_DEVELOPMENT;
    proposedReadiness = SALES_READINESS.READY_FOR_SALES;
    proposedAction = dims.layers.VERIFIED_ACCOMMODATION_DEMAND.present
      ? `Introduce ${targetHotel || "target hotel"} via ${contact} for evidenced hotel/accommodation sourcing. Do not invent an open RFP.`
      : `Pre-sourcing outreach to ${contact}: verified group/incentive activity supports inferred lodging potential for ${targetHotel || "target hotel"}. Sourcing status UNKNOWN — do not claim room block, RFP, or numeric rooms.`;
    if (!prod.commerciallyUsefulInternal) {
      changeReason = "Production required verified accommodation/overnight language; audit allows verified group activity + inferred lodging potential + fit + contact.";
      downgradeCause = DOWNGRADE_CAUSE.MISSING_EXPLICIT_GUESTROOM_DESPITE_CREDIBLE_DEMAND;
    } else {
      downgradeCause = DOWNGRADE_CAUSE.ALREADY_USEFUL_OR_RECOVERED;
    }
  } else if (dims.layers.VERIFIED_GROUP_ACTIVITY.present && !dims.layers.INFERRED_LODGING_POTENTIAL.present) {
    downgradeCause = DOWNGRADE_CAUSE.MISSING_EXPLICIT_GUESTROOM_DESPITE_CREDIBLE_DEMAND;
    // Actually this is "group activity without defensible lodging inference"
    changeReason = "Group activity present but inferred lodging potential not defensible.";
    proposedAction = "Document why overnight demand is plausible (multi-day, out-of-town delegates, residential programme) before internal useful.";
    // Re-label more precisely
    downgradeCause = DOWNGRADE_CAUSE.MISSING_EXPLICIT_GUESTROOM_DESPITE_CREDIBLE_DEMAND;
  } else {
    downgradeCause = DOWNGRADE_CAUSE.MISSING_GROUP_ACTIVITY;
    proposedAction = "Insufficient commercial evidence for pre-sourcing disposition.";
  }

  // If production already useful, proposed stays useful
  if (prod.commerciallyUsefulInternal) {
    proposedUseful = true;
    proposedKind = prod.usefulKind || proposedKind;
    proposedPathway = prod.pathway;
    proposedStage = prod.commercialStage;
    proposedReadiness = prod.salesReadiness;
    proposedAction = prod.nextSalesAction || proposedAction;
    if (!changeReason) downgradeCause = DOWNGRADE_CAUSE.ALREADY_USEFUL_OR_RECOVERED;
  }

  const changed = Boolean(proposedUseful) !== Boolean(prod.commerciallyUsefulInternal)
    || (proposedUseful && !prod.commerciallyUsefulInternal);

  return {
    version: DEMAND_EVIDENCE_DIMENSIONS_VERSION,
    auditOnly: true,
    productionPublicationEligibilityChanged: false,
    customerFacingClassificationChanged: false,
    sourcingStatus: dims.layers.VERIFIED_ACCOMMODATION_DEMAND.present ? "EVIDENCED" : "UNKNOWN",
    dimensions: dims,
    production: {
      commerciallyUsefulInternal: prod.commerciallyUsefulInternal === true,
      usefulKind: prod.usefulKind || null,
      pathway: prod.pathway,
      downgradeReason: prod.downgradeReason || null,
      salesReadiness: prod.salesReadiness,
    },
    proposed: {
      commerciallyUsefulInternal: proposedUseful,
      usefulKind: proposedKind,
      pathway: proposedPathway,
      commercialStage: proposedStage,
      salesReadiness: proposedReadiness,
      nextSalesAction: proposedAction,
      requiresVerifiedAccommodationDemand: false,
      requiresActiveRfp: false,
      requiresNamedRoomBlock: false,
      requiresNumericRoomCount: false,
    },
    primaryDowngradeCause: downgradeCause,
    changeReason: changed ? changeReason : null,
    changedToUsefulUnderProposedRules: proposedUseful && !prod.commerciallyUsefulInternal,
    falsePositiveRiskFlags: buildFalsePositiveRisks({ dims, proposedUseful, opportunity, targetHotel, competitor }),
    contactRoute: contact,
    competitorHost: competitor || null,
  };
}

function buildFalsePositiveRisks({ dims, proposedUseful, opportunity, targetHotel, competitor }) {
  const risks = [];
  if (!proposedUseful) return risks;
  if (!dims.layers.VERIFIED_ACCOMMODATION_DEMAND.present) {
    risks.push({
      level: "MEDIUM",
      risk: "INFERRED_LODGING_WITHOUT_VERIFIED_ACCOMMODATION",
      mitigation: "Keep sourcingStatus=UNKNOWN; sales action must not claim room block/RFP; require human confirmation before publication.",
    });
  }
  if (/awards|banquet|gala|luncheon/i.test(opportunity.summaryWhyHotel || "") && dims.layers.INFERRED_LODGING_POTENTIAL.present) {
    risks.push({
      level: "HIGH",
      risk: "BANQUET_MISCLASSIFIED_AS_LODGING",
      mitigation: "Reject if only banquet/luncheon signals without travel/residential language.",
    });
  }
  if (competitor) {
    risks.push({
      level: "HIGH",
      risk: "COMPETITOR_HOST_PRESENTED_AS_OPEN",
      mitigation: "Force COMPETITIVE_FUTURE_CYCLE pathway; never OPEN_HOST_PROSPECT.",
    });
  }
  if (/600 guestrooms|243 bedrooms|supplied hotel/i.test(opportunity.summaryWhyHotel || "")) {
    risks.push({
      level: "MEDIUM",
      risk: "TARGET_HOTEL_INVENTORY_PROSE_AS_DEMAND",
      mitigation: "Ignore target-hotel inventory when scoring demand dimensions.",
    });
  }
  if (/SoccerPlex|local chapter/i.test(`${opportunity.summaryWhyHotel || ""} ${opportunity.eventLocationSummary || ""}`)) {
    risks.push({
      level: "HIGH",
      risk: "LOCAL_DAY_ACTIVITY_AS_HOTEL_DEMAND",
      mitigation: "Require catchment fit + travel signals.",
    });
  }
  void targetHotel;
  return risks;
}

export const RECOMMENDED_RULE_CHANGES_FOR_APPROVAL = Object.freeze({
  version: DEMAND_EVIDENCE_DIMENSIONS_VERSION,
  status: "PROPOSED_AWAITING_FOUNDER_APPROVAL",
  appliesTo: "internal_ACCOUNT_LED_and_early_EVENT_LED_pre_sourcing_only",
  doesNotChange: [
    "production_publication_gate",
    "customer_facing_classifications",
    "Airtable_writes",
    "deployments",
  ],
  rules: [
    {
      id: "SEPARATE_THREE_DEMAND_LAYERS",
      change: "Score VERIFIED_GROUP_ACTIVITY, INFERRED_LODGING_POTENTIAL, and VERIFIED_ACCOMMODATION_DEMAND independently.",
    },
    {
      id: "PRE_SOURCING_WITHOUT_ROOM_BLOCK",
      change: "Internal useful ACCOUNT_LED / early EVENT_LED may use verified group activity + defensible inferred lodging + hotel-specific fit + usable contact — without RFP, named room block, or numeric rooms.",
    },
    {
      id: "SOURCING_STATUS_UNKNOWN_DEFAULT",
      change: "Keep sourcingStatus=UNKNOWN unless VERIFIED_ACCOMMODATION_DEMAND is present.",
    },
    {
      id: "RETAIN_VENUE_AND_LOCAL_EXCLUSIONS",
      change: "Venue listings, bedroom-as-amenity, and local-only banquet/luncheon without travel signals remain non-qualifying.",
    },
    {
      id: "RETAIN_COMPETITOR_SEPARATION",
      change: "Competitor-hosted meetings stay COMPETITIVE_FUTURE_CYCLE or excluded — never open host.",
    },
    {
      id: "PUBLICATION_STILL_REQUIRES_STRONGER_BAR",
      change: "Publication eligibility continues to require the existing production gate; inferred lodging alone never publishes.",
    },
  ],
});
