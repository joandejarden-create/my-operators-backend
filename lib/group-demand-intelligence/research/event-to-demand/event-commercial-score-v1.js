/**
 * Commercial opportunity score for E2D — diagnostic ranking only.
 * Does NOT change Ready / publication eligibility gates.
 */

import {
  EVENT_DEMAND_CLASS,
  CONTACT_ROUTE_QUALITY,
  classifyContactRouteQuality,
  classifyEventDemandClass,
} from "./event-demand-class-v1.js";
import {
  BUYER_CONFIDENCE,
  classifyBuyerConfidence,
} from "./event-demand-ownership-v1.js";

function clamp(n, lo = 0, hi = 100) {
  return Math.max(lo, Math.min(hi, Number(n) || 0));
}

function scoreEvidenceStrength(opp = {}) {
  let s = 20;
  const sources = opp.sources || [];
  const evidence = opp.evidence || [];
  s += Math.min(25, sources.length * 8);
  s += Math.min(20, evidence.length * 4);
  if (opp.futureDemand?.supported) s += 15;
  if (opp.localConnection) s += 10;
  if (opp.eventDemandClass === EVENT_DEMAND_CLASS.CONFIRMED) s += 20;
  else if (opp.eventDemandClass === EVENT_DEMAND_CLASS.INFERRED) s += 10;
  return clamp(s);
}

function scoreDemandLikelihood(opp = {}, owner = null) {
  let s = 15;
  const demandClass = opp.eventDemandClass || classifyEventDemandClass(opp);
  if (demandClass === EVENT_DEMAND_CLASS.CONFIRMED) s += 40;
  else if (demandClass === EVENT_DEMAND_CLASS.INFERRED) s += 25;
  const mech = String(opp.eventDemandMechanism || "");
  if (/VIP|EXECUTIVE|SMALL_GROUP|OFFSITE/i.test(mech)) s += 15;
  if (/EXHIBITOR/i.test(mech) && opp.hotelFit?.territory === "TERRITORY_STRETCH") {
    s -= 15;
  }
  if (owner?.role === "EXHIBITOR" || owner?.role === "SPONSOR") s += 8;
  if (owner?.role === "VIP_HOSPITALITY_PROGRAM") s += 12;
  if (owner?.fromPageExtraction) s += 5;
  return clamp(s);
}

function scorePropertyFit(opp = {}, hotel = {}) {
  let s = 20;
  const territory = opp.hotelFit?.territory || opp.eventTerritoryClass;
  if (territory === "TERRITORY_CORE") s += 40;
  else if (territory === "TERRITORY_NEARBY") s += 32;
  else if (territory === "TERRITORY_COMPETITIVE") s += 18;
  else if (territory === "TERRITORY_STRETCH") s += 8;
  if (opp.hotelFit?.supported) s += 15;
  if (opp.hotelFit?.luxuryCompatible || /luxury/i.test(hotel.capabilityProfile?.serviceLevel || "")) {
    if (/VIP|EXECUTIVE|SMALL_GROUP/i.test(String(opp.eventDemandMechanism || ""))) {
      s += 12;
    }
  }
  return clamp(s);
}

function scoreBusinessValue(opp = {}) {
  let s = 20;
  const band = opp.valueBand?.band || "";
  if (band === "MODERATE_COMPACT") s += 35;
  else if (band === "SELECTIVE") s += 22;
  else if (band === "SELECTIVE_SPILLOVER") s += 12;
  else if (band === "NOT_ESTIMATED") s += 0;
  else s += 15;
  if (opp.capabilityPeakHint || Number(opp.demandScale?.estimatedPeakRooms) > 0) {
    s += 10;
  }
  return clamp(s);
}

function scoreBuyerAccessibility(opp = {}, buyer = null) {
  let s = 5;
  const q =
    opp.contactRouteQuality ||
    classifyContactRouteQuality(opp.contact || {});
  if (q === CONTACT_ROUTE_QUALITY.DIRECT_SALES) s += 45;
  else if (q === CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES) s += 35;
  else if (q === CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY) s += 8;
  const conf =
    buyer?.confidence ||
    classifyBuyerConfidence(opp.contact || {}, opp.buyerRoleEvidence);
  if (conf === BUYER_CONFIDENCE.CONFIRMED_BUYER) s += 40;
  else if (conf === BUYER_CONFIDENCE.LIKELY_BUYER_FUNCTION) s += 22;
  else if (conf === BUYER_CONFIDENCE.GENERIC_ORG_CONTACT) s += 12;
  return clamp(s);
}

function scoreSalesTiming(opp = {}, asOf = new Date()) {
  let s = 20;
  const start = String(opp.eventStartDate || "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(start)) return clamp(s);
  const asOfDay = asOf.toISOString().slice(0, 10);
  const days =
    (Date.parse(`${start}T00:00:00Z`) - Date.parse(`${asOfDay}T00:00:00Z`)) /
    86400000;
  if (days < 0) return 5;
  if (days <= 45) s += 35; // near-term urgency
  else if (days <= 120) s += 40; // ideal hotel sales lead time
  else if (days <= 270) s += 30;
  else if (days <= 450) s += 18;
  else s += 8;
  return clamp(s);
}

/**
 * @returns {{ total, band, dimensions, weights, note }}
 */
export function scoreCommercialOpportunity({
  opportunity = {},
  hotel = {},
  demandOwner = null,
  buyer = null,
  asOf = new Date(),
} = {}) {
  const weights = {
    evidenceStrength: 0.18,
    demandLikelihood: 0.22,
    propertyFit: 0.2,
    businessValue: 0.12,
    buyerAccessibility: 0.18,
    salesTiming: 0.1,
  };
  const dimensions = {
    evidenceStrength: scoreEvidenceStrength(opportunity),
    demandLikelihood: scoreDemandLikelihood(opportunity, demandOwner),
    propertyFit: scorePropertyFit(opportunity, hotel),
    businessValue: scoreBusinessValue(opportunity),
    buyerAccessibility: scoreBuyerAccessibility(opportunity, buyer),
    salesTiming: scoreSalesTiming(opportunity, asOf),
  };
  const total = clamp(
    Object.entries(weights).reduce(
      (sum, [k, w]) => sum + dimensions[k] * w,
      0
    )
  );
  let band = "WATCHLIST";
  if (total >= 72) band = "PRIORITY";
  else if (total >= 55) band = "ACTIVE_PIPELINE";
  else if (total >= 40) band = "NURTURE";
  return {
    version: "gdi-e2d-commercial-score-v1",
    total: Math.round(total * 10) / 10,
    band,
    dimensions,
    weights,
    note: "Diagnostic commercial ranking only — does not grant Ready or customer publication.",
  };
}

export function isSalesBriefEligible({
  opportunity = {},
  commercialScore = null,
  publicationHotelFacing = false,
} = {}) {
  const q =
    opportunity.contactRouteQuality ||
    classifyContactRouteQuality(opportunity.contact || {});
  const hasSalesRoute =
    q === CONTACT_ROUTE_QUALITY.DIRECT_SALES ||
    q === CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES;
  const scoreOk = Number(commercialScore?.total || 0) >= 55;
  return (
    publicationHotelFacing === true ||
    (hasSalesRoute && scoreOk) ||
    (hasSalesRoute && commercialScore?.band === "PRIORITY")
  );
}
