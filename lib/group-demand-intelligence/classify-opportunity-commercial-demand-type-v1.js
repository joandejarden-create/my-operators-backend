/**
 * Classify Opportunity Commercial Demand Type V1
 *
 * Evidence-grounded Guest Rooms / Local Catering / Both classification for ANY
 * hotel. Reuses HOTEL_REVENUE_STREAM_CLASS — does not invent parallel enums.
 *
 * Rules (founder directive):
 * - Do not keyword-match alone; ground theses in category + hotel thesis + fit.
 * - Do not auto-BOTH every conference.
 * - Overflow / competitor-host events → rooms only (not catering at this hotel).
 * - Partnership category ≠ demand type — classify by referral mechanism.
 * - DISQUALIFIED / quarantine / destination-outside → UNDETERMINED/NOT_APPLICABLE.
 * - Never overwrite higher-quality verified human / hotel-sales feedback.
 */

import {
  HOTEL_REVENUE_STREAM_CLASS,
  classifyHotelRevenueStream,
} from "./multi-revenue-qualification-v1.js";
import {
  COMMERCIAL_DEMAND_TYPE,
  GDI_COMMERCIAL_DEMAND_TYPE_V1,
  buildCommercialDemandTypePersistFields,
  revenueStreamClassToDemandType,
  normalizeRevenueStreamClass,
} from "./commercial-demand-type-v1.js";
import { buildRevenueStreamClassifyInput } from "./opportunity-revenue-stream-icons-v1.js";

export const CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1 =
  "gdi-classify-opportunity-commercial-demand-type-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function blobOf(opp = {}) {
  return [
    opp.title,
    opp.organizationName,
    opp.hotelOpportunityThesis,
    opp.hotelDemandThesis,
    opp.thesis,
    opp.summary,
    opp.summaryWhat,
    opp.summaryWhyMatters,
    opp.summaryWhyHotel,
    opp.tileCustomerSummary,
    opp.whatIsHappening,
    opp.recommendedSalesAction,
    opp.recommendedAction,
    opp.hotelFitNote,
    opp.hotelFitHonesty,
    opp.hotelFitRationale,
    opp.category,
    opp.opportunityType,
    opp.businessType,
  ]
    .map(text)
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function categoryOf(opp = {}) {
  return upper(
    opp.category ||
      opp.opportunityType ||
      opp.commercialCategory ||
      opp.researchCategory ||
      "",
  );
}

function thesisUnsupported(t) {
  const s = text(t);
  if (!s) return true;
  return /^(unsupported|not supported|no evidence|none|n\/a|null|unverified)\b/i.test(
    s,
  );
}

function hasVerifiedHumanDemandOverride(opp = {}) {
  const evidence = opp.commercialDemandTypeEvidence;
  if (
    evidence &&
    typeof evidence === "object" &&
    /HOTEL_SALES|HUMAN_VERIFIED|FOUNDER_VERIFIED/i.test(
      text(evidence.classifier || evidence.source || evidence.verifiedBy),
    )
  ) {
    return true;
  }
  if (opp.hotelSalesVerifiedDemandType === true) return true;
  return false;
}

function isNonCommercialQualityState(opp = {}) {
  const priority = upper(opp.priority || opp.priorityFilterKey);
  const familyRole = upper(opp.commercialEventFamilyRole);
  const fit = upper(opp.hotelFitHonesty || "");
  const territory = upper(opp.demandTerritoryFit || "");
  if (priority === "DISQUALIFIED") return { notApplicable: true, reason: "DISQUALIFIED" };
  if (opp.quarantined === true || familyRole === "QUARANTINED") {
    return { notApplicable: true, reason: "QUARANTINED" };
  }
  if (/TITLE_SOURCE_INVALID/i.test(text(opp.historicalRecoveryDisposition || ""))) {
    return { notApplicable: true, reason: "TITLE_SOURCE_INVALID" };
  }
  // True destination-outside / do-not-pursue only — not RESEARCH_GATED host honesty.
  if (
    (/DESTINATION_OUTSIDE/i.test(fit) ||
      /^OUT_OF_MARKET$/i.test(fit) ||
      territory === "OUTSIDE") &&
    /no bethesda|outside.*demand territory|do not pursue|remain disqualified/i.test(
      blobOf(opp),
    )
  ) {
    return { notApplicable: true, reason: "DESTINATION_OUTSIDE" };
  }
  if (upper(opp.category) === "CLOSED_DISQUALIFIED") {
    return { notApplicable: true, reason: "CLOSED_DISQUALIFIED" };
  }
  return null;
}

/**
 * Infer room / catering support from structured commercial category + thesis.
 * Category informs plausible mechanism; thesis must still support the claim.
 */
function inferFromCategoryAndEvidence(opp = {}) {
  const cat = categoryOf(opp);
  const blob = blobOf(opp);
  const thesis = text(opp.hotelOpportunityThesis || opp.thesis || opp.whatIsHappening);
  const fit = upper(opp.hotelFitHonesty || "");

  let rooms = false;
  let catering = false;
  let guestroomThesis = null;
  let eventFnbThesis = null;
  let guestroomConfidence = null;
  let eventFnbConfidence = null;
  let notes = [];

  // Overflow / stay-to-play / sports housing → rooms only for THIS hotel
  if (
    cat === "OVERFLOW_HOUSING" ||
    /OVERFLOW_WATCH|COMPETITOR_HOST|STAY.?TO.?PLAY/i.test(fit) ||
    /overflow|stay-to-play|housing block|room block coming|residual lodging|official room block/i.test(
      blob,
    )
  ) {
    rooms = true;
    guestroomThesis =
      thesis.slice(0, 280) ||
      "Overflow / housing / stay-to-play lodging demand near an off-site or competitor-hosted event.";
    guestroomConfidence = "RELIABLE_INFERRED";
    // Explicit ban: do not attribute host catering at another venue to this hotel
    catering = false;
    eventFnbThesis = null;
    notes.push("OVERFLOW_OR_COMPETITOR_HOST_ROOMS_ONLY");
  }

  // Preferred lodging partnerships → rooms (referral of overnight guests)
  if (cat === "PREFERRED_LODGING_PARTNERSHIP" || cat === "VENUE_PARTNERSHIP") {
    rooms = true;
    guestroomThesis =
      thesis.slice(0, 280) ||
      "Preferred lodging partnership — venue/destination refers overnight guests to the hotel.";
    guestroomConfidence = "RELIABLE_INFERRED";
    catering = false;
    notes.push("LODGING_PARTNERSHIP_ROOMS");
  }

  // Professional event planners → both (book rooms AND meetings/events)
  if (cat === "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP") {
    rooms = true;
    catering = true;
    guestroomThesis =
      "Planner partnership can refer group room blocks and overnight program lodging.";
    eventFnbThesis =
      "Planner partnership can place meetings, banquets, and catering programs at the hotel.";
    guestroomConfidence = "RELIABLE_INFERRED";
    eventFnbConfidence = "RELIABLE_INFERRED";
    notes.push("PLANNER_PARTNERSHIP_BOTH");
  }

  // Corporate / account relationships — mechanism-specific
  if (cat === "CORPORATE_ACCOUNT_RELATIONSHIP") {
    if (
      /guestrooms and meeting|guestroom.*meeting space|corporate \/ group meetings|corporate meetings.*guestroom/i.test(
        blob,
      )
    ) {
      rooms = true;
      catering = true;
      guestroomThesis =
        thesis.slice(0, 280) ||
        "Corporate meeting programs create credible overnight lodging demand when nonlocal attendees travel in.";
      eventFnbThesis =
        thesis.slice(0, 280) ||
        "Corporate meeting programs create credible meeting-space / catering demand.";
      guestroomConfidence = "RELIABLE_INFERRED";
      eventFnbConfidence = "RELIABLE_INFERRED";
      notes.push("CORPORATE_MEETINGS_BOTH");
    } else if (
      /scaling corporate|corporate account|extended stay|relocation|lodging research|visitor|overnight|room nights/i.test(
        blob,
      ) &&
      !/dinner meeting|gala|reception|chapter meeting|civic meeting|awards/i.test(blob)
    ) {
      rooms = true;
      guestroomThesis =
        thesis.slice(0, 280) ||
        "Corporate / account lodging demand (travelers, training stays, relocation, or group rooms).";
      guestroomConfidence = "RELIABLE_INFERRED";
      notes.push("CORPORATE_LODGING");
    }
    if (
      /local meeting|chapter meeting|dinner meeting|reception|gala|awards|banquet|civic meeting|community.*event|off the clock|holiday program|fedtalks|workshop portfolio/i.test(
        blob,
      )
    ) {
      catering = true;
      eventFnbThesis =
        thesis.slice(0, 280) ||
        "Local meetings, receptions, banquets, or chapter programs create catering / meeting-space demand.";
      eventFnbConfidence = "RELIABLE_INFERRED";
      notes.push("LOCAL_MEETINGS_CATERING");
      // Local chapter dinners / civic meetings rarely need room blocks unless lodging language exists
      if (
        !/overnight|room block|lodging|housing|extended stay|guestroom|hotel stay/i.test(blob) &&
        !notes.includes("CORPORATE_MEETINGS_BOTH")
      ) {
        rooms = false;
      }
    }
    if (/extended stay|relocation/i.test(blob)) {
      rooms = true;
      catering = false;
      guestroomThesis =
        thesis.slice(0, 280) ||
        "Relocation / extended-stay lodging demand for nonlocal assignees or fellows.";
      guestroomConfidence = "RELIABLE_INFERRED";
      eventFnbThesis = null;
      notes.push("RELOCATION_ROOMS_ONLY");
    }
  }

  // Travel-trade / DMC intermediaries — referral can produce rooms and/or events
  if (
    /dmc|travel-trade|travel trade|intermediary|group intermediary|route.*program/i.test(
      blob,
    ) ||
    /PARTNERSHIP_REFERRAL|BOTH/i.test(upper(opp.businessType || ""))
  ) {
    rooms = true;
    catering = true;
    guestroomThesis =
      thesis.slice(0, 280) ||
      "Intermediary / partnership channel can refer overnight group lodging into the hotel corridor.";
    eventFnbThesis =
      "Intermediary / partnership channel can place meetings, banquets, or catering programs at the hotel.";
    guestroomConfidence = "RELIABLE_INFERRED";
    eventFnbConfidence = "RELIABLE_INFERRED";
    notes.push("INTERMEDIARY_PARTNERSHIP_BOTH");
  }

  // Primary pursuit / future cycle — hostable conferences often BOTH; lodging-only if framed as housing
  if (cat === "PRIMARY_PURSUIT" || cat === "FUTURE_CYCLE") {
    const destinationOutside =
      /DESTINATION_OUTSIDE/i.test(fit) ||
      /^OUT_OF_MARKET$/i.test(fit) ||
      upper(opp.demandTerritoryFit) === "OUTSIDE";
    if (
      (destinationOutside &&
        /do not pursue|remain disqualified|outside.*demand territory/i.test(blob)) ||
      upper(opp.priority) === "DISQUALIFIED"
    ) {
      return {
        rooms: false,
        catering: false,
        guestroomThesis: null,
        eventFnbThesis: null,
        notes: ["NON_ACTIONABLE_FUTURE_OR_OUTSIDE"],
        notApplicable: true,
      };
    }
    // Competitor-host overflow path for a named primary program → rooms only
    if (/OVERFLOW_WATCH|COMPETITOR_HOST/i.test(fit)) {
      rooms = true;
      catering = false;
      guestroomThesis =
        text(opp.summaryWhat || thesis).slice(0, 280) ||
        "Program lodging demand exists; competitor or alternate host holds primary venue — pursue overflow / residual rooms.";
      guestroomConfidence = "RELIABLE_INFERRED";
      notes.push("PRIMARY_OVERFLOW_ROOMS");
      return {
        rooms,
        catering,
        guestroomThesis,
        eventFnbThesis: null,
        guestroomConfidence,
        eventFnbConfidence: null,
        notes,
      };
    }
    const lodgingOnly =
      /overflow|housing|room block|stay-to-play|lodging demand|accommodations list|commercial lodging/i.test(
        blob,
      ) &&
      !/host hotel|meeting space|banquet|catering|general session|ballroom|day meeting|annual meeting|conference and expo/i.test(
        blob,
      );
    const meetingsLanguage =
      /conference|symposium|annual meeting|expo|institute|interim meeting|workshop|session|meeting space|banquet|catering|on-site|bethesda|clinical pharmacy|translational|regulatory information/i.test(
        blob,
      );
    const roomsLanguage =
      /room|lodging|overnight|housing|hotel|accommodation|block|attendee|commercial lodging/i.test(
        blob,
      ) || meetingsLanguage; // multi-day conferences imply attendee lodging when hostable locally

    if (lodgingOnly) {
      rooms = true;
      guestroomThesis =
        text(opp.summaryWhat || thesis).slice(0, 280) ||
        "Group lodging / housing demand for a dated program.";
      guestroomConfidence = "RELIABLE_INFERRED";
      notes.push("FUTURE_LODGING_ONLY");
    } else if (meetingsLanguage && roomsLanguage) {
      // Hostable local conference — rooms + meetings/F&B independently supported
      rooms = true;
      catering = true;
      guestroomThesis =
        text(opp.summaryWhat || thesis).slice(0, 280) ||
        "Multi-day conference / meeting program creates credible overnight attendee lodging demand for a local host hotel.";
      eventFnbThesis =
        "Conference program creates credible meeting-space, banquet, or catering demand when hosted locally.";
      guestroomConfidence = "RELIABLE_INFERRED";
      eventFnbConfidence = "RELIABLE_INFERRED";
      notes.push("HOSTABLE_CONFERENCE_BOTH");
    } else if (meetingsLanguage) {
      catering = true;
      eventFnbThesis =
        text(opp.summaryWhat || thesis).slice(0, 280) ||
        "Meeting / program creates catering or meeting-space demand.";
      eventFnbConfidence = "RELIABLE_INFERRED";
      notes.push("MEETINGS_ONLY_FUTURE");
    } else if (roomsLanguage || thesis || text(opp.summaryWhat)) {
      rooms = true;
      guestroomThesis =
        text(opp.summaryWhat || thesis).slice(0, 280) ||
        "Credible overnight lodging demand for a future program cycle.";
      guestroomConfidence = "RELIABLE_INFERRED";
      notes.push("ROOMS_FUTURE_CYCLE");
    }
  }

  // Explicit existing theses win when present
  const existingGuest = text(
    opp.guestroomThesis || opp.hotelDemandHypotheses?.guestroomHypothesis,
  );
  const existingEvent = text(
    opp.eventFnbThesis ||
      opp.hotelDemandHypotheses?.meetingsHypothesis ||
      opp.hotelDemandHypotheses?.cateringHypothesis,
  );
  if (existingGuest && !thesisUnsupported(existingGuest)) {
    rooms = true;
    guestroomThesis = existingGuest;
    guestroomConfidence = guestroomConfidence || "PERSISTED";
  }
  if (existingEvent && !thesisUnsupported(existingEvent)) {
    catering = true;
    eventFnbThesis = existingEvent;
    eventFnbConfidence = eventFnbConfidence || "PERSISTED";
  }

  // Negative: competitor host catering attribution ban
  if (
    /booked at|contracted at|official.*marriott|gaylord|hilton|hyatt|ritz-carlton|bethesda north marriott|the bethesdan/i.test(
      blob,
    ) &&
    /overflow|residual|cannot win host|already.*host/i.test(blob)
  ) {
    catering = false;
    eventFnbThesis = null;
    rooms = true;
    guestroomThesis =
      guestroomThesis ||
      thesis.slice(0, 280) ||
      "Competitor or alternate host holds the meeting; this hotel’s path is overflow / residual lodging only.";
    notes.push("COMPETITOR_HOST_NO_CATERING");
  }

  return {
    rooms,
    catering,
    guestroomThesis,
    eventFnbThesis,
    guestroomConfidence,
    eventFnbConfidence,
    notes,
  };
}

/**
 * Classify one opportunity into commercial demand type + persistence fields.
 */
export function classifyOpportunityCommercialDemandType(opportunity = {}, opts = {}) {
  const opp = opportunity && typeof opportunity === "object" ? opportunity : {};
  const classifiedAt = opts.classifiedAt || new Date().toISOString();

  // Preserve verified human / hotel-sales overrides
  if (hasVerifiedHumanDemandOverride(opp) && opp.revenueStreamClass) {
    const cls = normalizeRevenueStreamClass(opp.revenueStreamClass);
    return {
      version: CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1,
      id: text(opp.id),
      preserved: true,
      commercialDemandType: revenueStreamClassToDemandType(cls),
      revenueStreamClass: cls,
      guestroomThesis: text(opp.guestroomThesis) || null,
      eventFnbThesis: text(opp.eventFnbThesis) || null,
      persist: buildCommercialDemandTypePersistFields({
        revenueStreamClass: cls,
        guestroomThesis: opp.guestroomThesis,
        eventFnbThesis: opp.eventFnbThesis,
        priority: opp.priority,
        classifiedAt,
        classifier: "HOTEL_SALES_OR_HUMAN_VERIFIED",
        sources: opp.commercialDemandTypeEvidence?.sources || [],
      }),
    };
  }

  const nonCommercial = isNonCommercialQualityState(opp);
  if (nonCommercial) {
    const persist = buildCommercialDemandTypePersistFields({
      commercialDemandType: COMMERCIAL_DEMAND_TYPE.NOT_APPLICABLE,
      revenueStreamClass: HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
      guestroomThesis: null,
      eventFnbThesis: null,
      priority: "DISQUALIFIED",
      classifiedAt,
      classifier: CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1,
      qualityState: nonCommercial.reason,
      notApplicable: true,
      notes: nonCommercial.reason,
      sources: [],
    });
    return {
      version: CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1,
      id: text(opp.id),
      commercialDemandType: COMMERCIAL_DEMAND_TYPE.NOT_APPLICABLE,
      revenueStreamClass: HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
      qualityState: nonCommercial.reason,
      guestroomThesis: null,
      eventFnbThesis: null,
      persist,
    };
  }

  const inferred = inferFromCategoryAndEvidence(opp);

  if (inferred.notApplicable === true) {
    const persist = buildCommercialDemandTypePersistFields({
      commercialDemandType: COMMERCIAL_DEMAND_TYPE.NOT_APPLICABLE,
      revenueStreamClass: HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
      guestroomThesis: null,
      eventFnbThesis: null,
      priority: "DISQUALIFIED",
      classifiedAt,
      classifier: CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1,
      qualityState: "NOT_APPLICABLE",
      notApplicable: true,
      notes: (inferred.notes || []).join(","),
      sources: [],
    });
    return {
      version: CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1,
      id: text(opp.id),
      commercialDemandType: COMMERCIAL_DEMAND_TYPE.NOT_APPLICABLE,
      revenueStreamClass: HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
      qualityState: "NOT_APPLICABLE",
      guestroomThesis: null,
      eventFnbThesis: null,
      notes: inferred.notes || [],
      persist,
    };
  }

  // Fall back to existing multi-revenue signal detection when category inference thin
  const input = buildRevenueStreamClassifyInput({
    ...opp,
    guestroomThesis: inferred.guestroomThesis || opp.guestroomThesis,
    eventFnbThesis: inferred.eventFnbThesis || opp.eventFnbThesis,
    summary: text(opp.summaryWhat || opp.summary || opp.whatIsHappening),
    hotelDemandHypotheses: {
      ...(opp.hotelDemandHypotheses || {}),
      guestroomHypothesis: inferred.guestroomThesis,
      meetingsHypothesis: inferred.eventFnbThesis,
    },
  });
  const detected = classifyHotelRevenueStream(input);

  let rooms = inferred.rooms || detected.detected?.guestroomCredible === true;
  let catering = inferred.catering || detected.detected?.eventFnbCredible === true;

  // Category overflow ban wins over incidental catering keywords in summary
  if (inferred.notes?.includes("OVERFLOW_OR_COMPETITOR_HOST_ROOMS_ONLY")) {
    catering = false;
  }
  if (inferred.notes?.includes("COMPETITOR_HOST_NO_CATERING")) {
    catering = false;
  }
  if (inferred.notes?.includes("RELOCATION_ROOMS_ONLY")) {
    catering = false;
    rooms = true;
  }

  let revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
  if (rooms && catering) {
    revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB;
  } else if (rooms) {
    revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY;
  } else if (catering) {
    revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY;
  }

  const guestroomThesis = rooms
    ? text(inferred.guestroomThesis) ||
      text(detected.guestroomThesis) ||
      null
    : null;
  const eventFnbThesis = catering
    ? text(inferred.eventFnbThesis) || text(detected.eventFnbThesis) || null
    : null;

  // BOTH requires both theses present
  if (
    revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB &&
    (!guestroomThesis || !eventFnbThesis)
  ) {
    if (guestroomThesis && !eventFnbThesis) {
      revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY;
    } else if (eventFnbThesis && !guestroomThesis) {
      revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY;
    } else {
      revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
    }
  }

  const commercialDemandType = revenueStreamClassToDemandType(revenueStreamClass);
  const persist = buildCommercialDemandTypePersistFields({
    commercialDemandType,
    revenueStreamClass,
    guestroomThesis,
    eventFnbThesis,
    guestroomConfidence: inferred.guestroomConfidence || null,
    eventFnbConfidence: inferred.eventFnbConfidence || null,
    guestroomRationale: guestroomThesis,
    cateringRationale: eventFnbThesis,
    primaryMechanism: text(opp.hotelOpportunityThesis || opp.thesis).slice(0, 200) || null,
    priority: opp.priority,
    classifiedAt,
    classifier: CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1,
    qualityState:
      revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED
        ? "UNDETERMINED"
        : null,
    notes: (inferred.notes || []).join(","),
    sources: Array.isArray(opp.sources)
      ? opp.sources.slice(0, 5)
      : opp.officialSource
        ? [opp.officialSource]
        : [],
  });

  return {
    version: CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1,
    id: text(opp.id),
    commercialDemandType,
    revenueStreamClass,
    guestroomThesis,
    eventFnbThesis,
    guestroomConfidence: inferred.guestroomConfidence || null,
    eventFnbConfidence: inferred.eventFnbConfidence || null,
    notes: inferred.notes || [],
    qualityState: persist.commercialDemandTypeEvidence?.qualityState || null,
    persist,
  };
}

/**
 * Attach classification onto an opportunity (non-destructive merge of demand fields only).
 */
export function attachCommercialDemandTypeClassification(opportunity = {}, opts = {}) {
  const result = classifyOpportunityCommercialDemandType(opportunity, opts);
  const p = result.persist || {};
  const hyp = {
    ...(opportunity.hotelDemandHypotheses && typeof opportunity.hotelDemandHypotheses === "object"
      ? opportunity.hotelDemandHypotheses
      : {}),
    ...(p.hotelDemandHypotheses || {}),
  };
  return {
    ...opportunity,
    commercialDemandType: p.commercialDemandType,
    commercialDemandTypeLabel: p.commercialDemandTypeLabel,
    revenueStreamClass: p.revenueStreamClass,
    revenueStreamLabel: p.revenueStreamLabel,
    applicableHotelRevenueStreams: p.applicableHotelRevenueStreams,
    applicableHotelRevenueStreamsLabel: p.applicableHotelRevenueStreamsLabel,
    guestroomThesis: p.guestroomThesis,
    eventFnbThesis: p.eventFnbThesis,
    hotelDemandHypotheses: hyp,
    guestroomDemandConfidence: p.guestroomDemandConfidence,
    eventFnbDemandConfidence: p.eventFnbDemandConfidence,
    commercialDemandTypeEvidence: p.commercialDemandTypeEvidence,
    revenueStreamIcons: p.revenueStreamIcons,
    revenueStreamIconsAria: p.revenueStreamIconsAria,
    revenueStreamIconsVersion: p.revenueStreamIconsVersion,
    commercialDemandTypeClassifiedAt: p.commercialDemandTypeClassifiedAt,
  };
}

/**
 * Batch classify a list of opportunities.
 */
export function classifyOpportunityCommercialDemandTypeBatch(opportunities = [], opts = {}) {
  const results = [];
  const counts = {
    GUEST_ROOMS: 0,
    LOCAL_CATERING: 0,
    BOTH: 0,
    UNDETERMINED: 0,
    NOT_APPLICABLE: 0,
  };
  for (const opp of opportunities || []) {
    const r = classifyOpportunityCommercialDemandType(opp, opts);
    results.push(r);
    const key = r.commercialDemandType || COMMERCIAL_DEMAND_TYPE.UNDETERMINED;
    counts[key] = (counts[key] || 0) + 1;
  }
  return {
    version: CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1,
    total: results.length,
    counts,
    results,
  };
}

export { GDI_COMMERCIAL_DEMAND_TYPE_V1, COMMERCIAL_DEMAND_TYPE };
