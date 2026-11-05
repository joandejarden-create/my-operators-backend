/**
 * Shadow pre-sourcing evaluation. v1.
 *
 * Scores a commercial reading beside the production publication gate.
 * evaluateOpportunity attaches this reading as evaluation.shadow.
 * The attachment does not change publishable, blocker severity, or the publication gate.
 * Shadow stays internal: internalOnly, productionPublication false, customerVisible false.
 *
 * UNKNOWN accommodation sourcing is never rewritten as available or committed.
 * Room counts are never invented. A stored peak band stays an unverified label.
 */

import {
  classifyContactCompletenessGroup,
  evaluateProspectPublicationEligibility,
  gradeTravelNeedEvidence,
  hasUsableContactRoute,
} from "../prospect-publication-eligibility-v1.js";

export const SHADOW_PRESURCING_VERSION = "gdi-shadow-presourcing-evaluation-v1";

export const SHADOW_STAGE = Object.freeze({
  VERIFIED_FUTURE_ACTIVITY: "VERIFIED_FUTURE_ACTIVITY",
  EMERGING_DEMAND: "EMERGING_DEMAND",
  ACTIVE_SOURCING: "ACTIVE_SOURCING",
  HOST_SELECTED: "HOST_SELECTED",
  ALREADY_COMMITTED: "ALREADY_COMMITTED",
  NOT_AN_OPPORTUNITY: "NOT_AN_OPPORTUNITY",
});

export const SOURCING_STATUS = Object.freeze({
  UNKNOWN: "UNKNOWN",
  ACTIVE_SOURCING: "ACTIVE_SOURCING",
  HOST_SELECTED: "HOST_SELECTED",
  ALREADY_COMMITTED: "ALREADY_COMMITTED",
});

/** Supervisor integration. Item 1 is applied. Items 2–4 are not. */
export const SUPERVISOR_INTEGRATION_PLAN = Object.freeze([
  {
    file: "lib/group-demand-intelligence/research/opportunity-research-supervisor-v1.js",
    functionName: "evaluateOpportunity",
    applied: true,
    change:
      "evaluation.shadow is attached. publishable, exploratory, and blocker severity are unchanged. The shadow object is internalOnly and is not a publication gate.",
  },
  {
    file: "lib/group-demand-intelligence/research/opportunity-research-supervisor-v1.js",
    functionName: "classifyExhaustionStop",
    applied: false,
    change:
      "When the blocker is future demand and shadow activity is VERIFIED_DATED with sourcing UNKNOWN, do not return NO_FUTURE_DEMAND_EVIDENCE. Unknown sourcing is not absence of demand.",
  },
  {
    file: "lib/group-demand-intelligence/research/research-planner-v1.js",
    functionName: "planAdaptiveStep",
    applied: false,
    change:
      "When shadow sourcing is UNKNOWN and a dated activity is already verified, do not lead with a room-block search. Confirm the event page and the organization contact first.",
  },
  {
    file: "lib/group-demand-intelligence/prospect-publication-eligibility-v1.js",
    functionName: "evaluateProspectSuitability",
    applied: false,
    change:
      "Do not change travel_need_evidence in this phase. The shadow score records the hold. Customer eligibility stays on the current gate until a separate authorization.",
  },
]);

const LODGING_ONLY = new Set(["travel_need_evidence"]);
const GENERIC_FIT = /fit reflects physical capacity, demand territory/i;
const DENIAL = /not yet evidenced|must not be treated as room nights|not a current primary sell/i;
const AS_OF = "2026-10-24";

function text(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function researchBlob(opp) {
  return [
    opp.eventLocationSummary,
    opp.eventCity,
    opp.city,
    opp.hotelOpportunityThesis,
    opp.fitExplanation,
    opp.hotelFitRationale,
    opp.lodgingEvidence,
    opp.lodgingOrReferralRationale,
  ]
    .map(text)
    .filter(Boolean)
    .join(" ");
}

function timingPrecision(opp) {
  const display = text(opp.eventDateDisplay);
  if (/^\d{4}$/.test(display)) return "YEAR_ONLY";
  if (opp.eventStartDate && opp.eventEndDate) return "DATED_RANGE";
  if (opp.eventStartDate) return "START_ONLY";
  if (/\b20\d{2}\b/.test(display)) return "YEAR_ONLY";
  return "ABSENT";
}

function isPast(opp) {
  const end = text(opp.eventEndDate || opp.eventStartDate);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(end)) return false;
  if (timingPrecision(opp) === "YEAR_ONLY") return false;
  return end < AS_OF;
}

function geography(opp) {
  const blob = researchBlob(opp);
  const lower = blob.toLowerCase();
  if (/chicago|las vegas|london|san diego|geneva/.test(lower)) return "OUTSIDE_MARKET";
  if (/bethesda north|\bwasbn\b/.test(lower)) return "SIBLING_HOTEL_SELECTED";
  if (/hyatt regency|ritz-carlton|hilton capitol hill/.test(lower)) return "OTHER_HOTEL_SELECTED";
  if (/host hotel contracted|already (crystal|booked|contracted)/.test(lower)) {
    return "OTHER_HOTEL_SELECTED";
  }
  if (/hotel not yet|not yet published|venue not named|not named|location to be announced|to be announced/.test(lower)) {
    if (/bethesda|gaithersburg|montgomery|nist|rockledge/.test(lower)) return "PLAUSIBLE_CORRIDOR";
    if (/arlington|crystal|washington|northern virginia|dmv/.test(lower)) return "DMV_STRETCH";
    return "UNKNOWN";
  }
  if (/bethesda|gaithersburg|montgomery county|rockledge|\bnih\b/.test(lower)) return "SERVES_THIS_HOTEL";
  if (/arlington|crystal gateway|washington|capitol hill|northern virginia|destination dc/.test(lower)) {
    return "DMV_STRETCH";
  }
  if (/geography not confirmed|location not confirmed/.test(lower) || !blob) return "UNKNOWN";
  return "UNKNOWN";
}

function sourcing(opp, place) {
  if (opp.alreadyContracted === true || opp.venueContracted === true) {
    return { status: SOURCING_STATUS.ALREADY_COMMITTED, overflow: false, evidence: "stored_contract_flag" };
  }
  const venue = text(opp.venueSourcingStatus).toUpperCase();
  if (venue === "RFP_ACTIVE_SOURCING") {
    return {
      status: SOURCING_STATUS.ACTIVE_SOURCING,
      overflow: false,
      evidence: "stored_field_not_reverified",
    };
  }
  if (place === "OTHER_HOTEL_SELECTED" || place === "SIBLING_HOTEL_SELECTED" || venue.includes("PRIMARY_VENUE_SELECTED")) {
    return {
      status: SOURCING_STATUS.HOST_SELECTED,
      overflow: venue.includes("OVERFLOW") || /overflow/i.test(researchBlob(opp)),
      evidence: "stored_host_or_location",
    };
  }
  return { status: SOURCING_STATUS.UNKNOWN, overflow: false, evidence: "no_sourcing_evidence" };
}

function overnight(opp, travelGrade) {
  const blob = researchBlob(opp);
  const denied = DENIAL.test(blob);
  const storedPeak = opp.peakRooms ?? opp.estimatedPeakRooms ?? null;
  const peakLabel = storedPeak == null || String(storedPeak).toUpperCase() === "UNKNOWN" ? null : String(storedPeak);
  let status = "NOT_ESTABLISHED";
  if (!denied && /verified housing|official housing|housing page/i.test(blob)) status = "STATED_HOUSING_PAGE";
  else if (!denied && travelGrade.ok === true) status = "INFERRED";
  else if (!denied && /room block|overnight|lodging|housing|attendee/i.test(blob)) status = "INFERRED";
  return {
    status,
    inference: status === "INFERRED",
    deniedByRecord: denied,
    roomCount: null,
    storedPeakBand: peakLabel,
    storedPeakVerified: false,
  };
}

function contactRelevance(opp, route) {
  const email = text(route.surface?.email || opp.primaryContactEmail || opp.primaryContact?.email).toLowerCase();
  if (!route.ok) return { relevant: false, kind: "none", email: email || null };
  const domain = email.split("@")[1] || "";
  const org = text(opp.organizationName).toLowerCase();
  const tokens = org.split(/[^a-z0-9]+/).filter((token) => token.length >= 3);
  const acronyms = [...org.matchAll(/\(([^)]+)\)/g)].map((match) => match[1].toLowerCase().replace(/[^a-z0-9]/g, ""));
  const matched = tokens.some((token) => domain.includes(token.slice(0, Math.min(token.length, 8)))) ||
    acronyms.some((token) => token.length >= 3 && domain.includes(token));
  if (matched) return { relevant: true, kind: "organization", email: email || null };
  if (/washington\.org$/.test(domain)) {
    return { relevant: false, kind: "city_intermediary_unverified", email };
  }
  if (email) return { relevant: false, kind: "unmatched_domain", email };
  if (route.ok) return { relevant: true, kind: route.reason, email: null };
  return { relevant: false, kind: "none", email: null };
}

function evidenceConfidence({ source, precision, contact, place }) {
  let score = 0;
  if (source) score += 1;
  if (precision === "DATED_RANGE" || precision === "START_ONLY") score += 1;
  if (contact.relevant) score += 1;
  if (place === "UNKNOWN" || place === "OUTSIDE_MARKET") score = Math.max(0, score - 1);
  if (score >= 3) return "HIGH";
  if (score === 2) return "MEDIUM";
  if (score === 1) return "LOW";
  return "NONE";
}

function stageFor({ disqualified, past, precision, source, place, sourceStatus, denied }) {
  if (disqualified || place === "OUTSIDE_MARKET") return SHADOW_STAGE.NOT_AN_OPPORTUNITY;
  if (past) return SHADOW_STAGE.ALREADY_COMMITTED;
  if (sourceStatus === SOURCING_STATUS.ALREADY_COMMITTED) return SHADOW_STAGE.ALREADY_COMMITTED;
  if (sourceStatus === SOURCING_STATUS.HOST_SELECTED) return SHADOW_STAGE.HOST_SELECTED;
  if (sourceStatus === SOURCING_STATUS.ACTIVE_SOURCING && precision !== "ABSENT" && source) {
    return SHADOW_STAGE.ACTIVE_SOURCING;
  }
  if (denied && precision === "YEAR_ONLY") return SHADOW_STAGE.EMERGING_DEMAND;
  if ((precision === "DATED_RANGE" || precision === "START_ONLY") && source) {
    return SHADOW_STAGE.VERIFIED_FUTURE_ACTIVITY;
  }
  if (precision === "YEAR_ONLY" && source) return SHADOW_STAGE.EMERGING_DEMAND;
  return SHADOW_STAGE.NOT_AN_OPPORTUNITY;
}

function nextAction(row) {
  if (row.disposition === "correctly_rejected") return "Do not send this record to the hotel.";
  if (row.past) return "The stored date is already past. Do not prospect it as future demand.";
  if (row.stage === SHADOW_STAGE.HOST_SELECTED) {
    return "A host hotel is already named. Ask only whether overflow is actually open. Do not describe the host block as available.";
  }
  if (row.disposition === "recovered_presourcing" && row.sourcing.status === SOURCING_STATUS.UNKNOWN) {
    return `Ask ${row.contact.email} whether attendees still need rooms. The accommodation partner is unknown. Do not quote a room count.`;
  }
  if (row.disposition === "recovered_presourcing") {
    return `Ask ${row.contact.email} whether a hotel is still unselected. Stored sourcing was not re-read in this replay. Do not quote a room count.`;
  }
  if (row.contact.kind === "city_intermediary_unverified") {
    return "The stored email is not the association. Find the organizer route before outreach.";
  }
  if (!row.contact.relevant) return "Find a source-backed organization contact before outreach.";
  if (row.stage === SHADOW_STAGE.ACTIVE_SOURCING) {
    return "Stored status says sourcing is active. Re-read the official page before telling sales an RFP is open.";
  }
  return "Keep the record in research. Unknown sourcing is not an open room block.";
}

function missingResearch(row) {
  const gaps = [];
  if (row.sourcing.evidence === "stored_field_not_reverified") {
    gaps.push("Re-read the official event page before treating stored sourcing as current.");
  }
  if (row.overnight.storedPeakBand) {
    gaps.push("Stored peak band is unverified. Do not publish it as a room count.");
  }
  if (row.overnight.status === "NOT_ESTABLISHED") {
    gaps.push("Overnight need is not established. A sales question can ask; the record must not invent it.");
  }
  if (!row.contact.relevant) gaps.push("Organization contact route is missing or not the prospect.");
  if (row.place === "UNKNOWN" || row.place === "PLAUSIBLE_CORRIDOR") {
    gaps.push("Event geography is not confirmed for this hotel.");
  }
  if (row.precision === "YEAR_ONLY") gaps.push("Dates are year-only. Confirm the meeting dates.");
  if (row.place === "DMV_STRETCH") {
    gaps.push("Fit is a DMV alternative or overflow path, not a Bethesda host award.");
  }
  return gaps;
}

/**
 * @param {object} opp Stored opportunity. Not modified.
 * @returns {object} Shadow reading. Production eligibility is copied, not altered.
 */
export function evaluateShadowPresourcing(opp = {}) {
  const current = evaluateProspectPublicationEligibility(opp);
  const route = hasUsableContactRoute(opp);
  const travel = gradeTravelNeedEvidence(opp);
  const contact = contactRelevance(opp, route);
  const precision = timingPrecision(opp);
  const past = isPast(opp);
  const place = geography(opp);
  const sourceStatus = sourcing(opp, place);
  const rooms = overnight(opp, travel);
  const source = Boolean(opp.officialSource || opp.discoverySource || opp.sourceUrl || (opp.sources || []).length);
  const disqualified = current.reasons?.includes("disqualified_or_test") || text(opp.priority).toUpperCase() === "DISQUALIFIED";
  const stage = stageFor({
    disqualified,
    past,
    precision,
    source,
    place,
    sourceStatus: sourceStatus.status,
    denied: rooms.deniedByRecord,
  });
  const confidence = evidenceConfidence({ source, precision, contact, place });
  const lodgingOnly =
    !current.hotelFacing &&
    !disqualified &&
    Array.isArray(current.reasons) &&
    current.reasons.length > 0 &&
    current.reasons.every((reason) => LODGING_ONLY.has(reason)) &&
    route.ok === true;
  const hostStillOpen =
    sourceStatus.status === SOURCING_STATUS.UNKNOWN ||
    (sourceStatus.status === SOURCING_STATUS.ACTIVE_SOURCING &&
      sourceStatus.evidence === "stored_field_not_reverified");
  const presourcingEligible =
    !disqualified &&
    !past &&
    !rooms.deniedByRecord &&
    (stage === SHADOW_STAGE.VERIFIED_FUTURE_ACTIVITY || stage === SHADOW_STAGE.ACTIVE_SOURCING) &&
    (place === "SERVES_THIS_HOTEL" || place === "DMV_STRETCH" || place === "PLAUSIBLE_CORRIDOR") &&
    hostStillOpen &&
    contact.relevant === true &&
    (precision === "DATED_RANGE" || precision === "START_ONLY");
  let disposition = "incomplete";
  if (disqualified || place === "OUTSIDE_MARKET" || stage === SHADOW_STAGE.NOT_AN_OPPORTUNITY) {
    disposition = "correctly_rejected";
  } else if (past) {
    disposition = "correctly_rejected";
  } else if (current.hotelFacing) {
    disposition = "current_gate_eligible";
  } else if (lodgingOnly && presourcingEligible) {
    disposition = "recovered_presourcing";
  } else if (stage === SHADOW_STAGE.HOST_SELECTED) {
    disposition = "host_already_selected";
  } else {
    disposition = "incomplete";
  }

  const reading = {
    version: SHADOW_PRESURCING_VERSION,
    id: opp.id || null,
    organizationName: opp.organizationName || null,
    stage,
    evidenceConfidence: confidence,
    confidenceDoesNotSetStage: true,
    activity: precision === "ABSENT" ? "ABSENT" : precision === "YEAR_ONLY" ? "YEAR_ONLY" : past ? "PAST" : "VERIFIED_DATED",
    precision,
    past,
    place,
    sourcing: sourceStatus,
    unknownIsNotAvailable: sourceStatus.status === SOURCING_STATUS.UNKNOWN,
    unknownIsNotCommitted: sourceStatus.status === SOURCING_STATUS.UNKNOWN,
    overnight: rooms,
    contact,
    contactGroup: classifyContactCompletenessGroup(opp),
    current: {
      eligibility: current.eligibility,
      hotelFacing: current.hotelFacing === true,
      reasons: current.reasons || [],
      travelNeedGrade: travel.grade,
    },
    lodgingOnlyHold: lodgingOnly,
    presourcingEligible,
    disposition,
  };
  reading.missingResearch = missingResearch(reading);
  reading.nextSalesAction = nextAction(reading);
  reading.clientPreviewReady = disposition === "recovered_presourcing" || (
    disposition === "current_gate_eligible" && !past && contact.relevant
  );
  return reading;
}
