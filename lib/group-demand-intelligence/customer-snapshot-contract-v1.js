/**
 * Versioned customer snapshot. Eligibility is computed once from the V4.2 contract.
 * List, drawer, PDF, and share projections read that snapshot. They do not requalify.
 */

import { qualifyCanonicalOpportunity, CANONICAL_COMMERCIAL_CONTRACT } from "./canonical-commercial-contract-v1.js";
import { assignPriorityAndActionWindowV1, deriveProspectCommercialSection } from "./priority-action-window/assignment-contract-v1.js";
import { evaluateDrawerContentCompleteness } from "./workflow/drawer-content-contract-v1.js";

export const CUSTOMER_SNAPSHOT_VERSION = "GDI_CUSTOMER_SNAPSHOT_V4_3";

const PRIORITY_RANK = { HIGH: 0, MEDIUM: 1, WATCHLIST: 2, DISQUALIFIED: 3 };
const DELIVERABLE_ROUTES = new Set([
  "verified_individual_buyer",
  "verified_organizing_department",
  "legitimate_functional_organizer_route",
]);

function redact(value) {
  return String(value || "")
    .replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, "[redacted-email]")
    .replace(/\+?\d[\d\s().-]{8,}\d/g, "[redacted-phone]")
    .replace(/\s+/g, " ")
    .trim();
}

function cardFrom(entry) {
  const { opportunity, contract, priority, section } = entry;
  const fit = redact(opportunity.hotelOpportunityThesis || opportunity.fitExplanation || opportunity.summaryWhyHotel || "");
  const whyNow = redact(opportunity.whyNow || opportunity.summaryWhyNow || "");
  let salesAction = redact(priority.bookingWindowReason || priority.after?.bookingWindowStatus || "");
  const actionOverclaims = contract.demand.openBookingClaimed !== true && /confirmed open buying|active rfp|pursue now/i.test(salesAction);
  if (actionOverclaims) {
    salesAction = "Contact the organizing route to qualify. No open room block or RFP is claimed.";
  }
  const unsupportedBooking = contract.demand.openBookingClaimed !== true
    && /confirmed room block|open rfp|venue is available|rooms are blocked/i.test(`${fit} ${whyNow}`);
  return {
    opportunityId: contract.opportunityId,
    hotelId: contract.hotelId || opportunity.hotelId || null,
    organization: opportunity.organizationName || "",
    title: redact(opportunity.title || ""),
    policyVersion: CANONICAL_COMMERCIAL_CONTRACT,
    snapshotVersion: CUSTOMER_SNAPSHOT_VERSION,
    publicationEligibility: contract.publication.eligibility,
    hotelFacing: contract.publication.hotelFacing === true,
    historicalCustomerVisible: contract.publication.historicalCustomerVisible === true,
    demandMechanism: contract.demand.mechanism,
    openBookingClaimed: contract.demand.openBookingClaimed === true,
    evidenceStatus: contract.evidence.verificationStatus,
    destination: contract.demand.destination,
    hotelFit: fit,
    whyNow,
    routeKind: contract.evidence.routeKind,
    routeUsable: contract.readiness.usableRoute === true,
    salesAction,
    actionOverclaimsBooking: actionOverclaims,
    priority: priority.after?.priority || null,
    section: section.section,
    sectionSource: section.source,
    provenance: redact(opportunity.sourceUrl || opportunity.officialSource || opportunity.discoverySource || ""),
    unsupportedBookingLanguage: unsupportedBooking,
    accountDevelopmentPublished: false,
  };
}

export function buildCustomerSnapshot({ hotelId, opportunities = [], config = null, limit = 15 } = {}) {
  const entries = opportunities.map((opportunity) => {
    const contract = qualifyCanonicalOpportunity(opportunity, { config });
    const priority = assignPriorityAndActionWindowV1(opportunity);
    const section = deriveProspectCommercialSection(opportunity, { eligibility: contract.eligibility });
    return { opportunity, contract, priority, section };
  });
  const eligible = entries.filter((entry) => entry.contract.publication.hotelFacing === true && entry.contract.readiness.disqualified !== true);
  const deliverable = eligible.filter((entry) => DELIVERABLE_ROUTES.has(entry.contract.evidence.routeKind));
  const ranked = [...deliverable].sort((left, right) => {
    const rank = (PRIORITY_RANK[left.priority.after?.priority] ?? 9) - (PRIORITY_RANK[right.priority.after?.priority] ?? 9);
    if (rank) return rank;
    const confidence = Number(right.opportunity.evidenceConfidence || 0) - Number(left.opportunity.evidenceConfidence || 0);
    if (confidence) return confidence;
    return String(left.contract.opportunityId).localeCompare(String(right.contract.opportunityId));
  });
  const selected = ranked.slice(0, limit).map(cardFrom);
  return {
    version: CUSTOMER_SNAPSHOT_VERSION,
    policyVersion: CANONICAL_COMMERCIAL_CONTRACT,
    hotelId,
    staged: true,
    productionPublication: false,
    eligibleCount: eligible.length,
    deliverableCount: deliverable.length,
    eligibleIds: ranked.map((entry) => entry.contract.opportunityId),
    selected,
  };
}

export function projectSnapshotList(snapshot) {
  return (snapshot.selected || []).map((card) => ({
    opportunityId: card.opportunityId,
    hotelId: card.hotelId,
    section: card.section,
    priority: card.priority,
    hotelFacing: card.hotelFacing,
    historicalCustomerVisible: card.historicalCustomerVisible,
  }));
}

export function projectSnapshotShare(snapshot) {
  return (snapshot.selected || []).map((card) => ({
    opportunityId: card.opportunityId,
    hotelId: card.hotelId,
    hotelFacing: card.hotelFacing,
    demandMechanism: card.demandMechanism,
    openBookingClaimed: card.openBookingClaimed,
    accountDevelopmentPublished: false,
  }));
}

export function projectSnapshotPdf(snapshot) {
  return (snapshot.selected || []).map((card) => ({
    opportunityId: card.opportunityId,
    hotelId: card.hotelId,
    hotelFacing: card.hotelFacing,
    demandMechanism: card.demandMechanism,
    openBookingClaimed: card.openBookingClaimed,
    salesAction: card.salesAction,
  }));
}

export function auditSnapshotCard(card, drawer = null) {
  const defects = [];
  if (!card.hotelFit) defects.push("missing_hotel_fit");
  if (!card.routeUsable) defects.push("route_not_usable");
  if (!card.salesAction) defects.push("missing_sales_action");
  if (!card.demandMechanism) defects.push("missing_demand_mechanism");
  if (!card.section) defects.push("missing_section");
  if (card.actionOverclaimsBooking) defects.push("action_overclaims_booking");
  if (card.openBookingClaimed && card.demandMechanism !== "ACTIVE_SOURCING") defects.push("booking_claim_without_sourcing");
  if (drawer && drawer.layers?.qualification?.hotelFacing !== card.hotelFacing) defects.push("drawer_eligibility_mismatch");
  return defects;
}

export function weeklySnapshotDecision({ prior, next, disqualified = false, newEvidence = false } = {}) {
  if (disqualified || next?.readiness?.disqualified) {
    return { stable: false, includeInCustomerSnapshot: false, release: false, reason: "disqualified_cannot_reenter" };
  }
  if (!newEvidence && prior && next && prior.hotelFacing === next.hotelFacing && prior.demandMechanism === next.demandMechanism && prior.opportunityId === next.opportunityId) {
    return { stable: true, includeInCustomerSnapshot: next.hotelFacing === true, release: false, reason: "unchanged" };
  }
  if (newEvidence) {
    return { stable: false, includeInCustomerSnapshot: next?.hotelFacing === true, release: false, reason: "new_evidence_requires_requalification" };
  }
  return { stable: false, includeInCustomerSnapshot: false, release: false, reason: "review_required" };
}

export function renderSnapshotHtml(snapshot, hotelName) {
  const rows = (snapshot.selected || []).map((card) => `<article>
    <h2>${escapeHtml(card.organization)}</h2>
    <p>${escapeHtml(card.title)}</p>
    <p>Section ${escapeHtml(card.section || "none")} · Priority ${escapeHtml(card.priority || "none")} · ${escapeHtml(card.demandMechanism)}</p>
    <p>${escapeHtml(card.hotelFit)}</p>
    <p>Next action: ${escapeHtml(card.salesAction)}</p>
    <p>Route: ${escapeHtml(card.routeKind)}. Open booking claimed: ${card.openBookingClaimed ? "yes" : "no"}.</p>
  </article>`).join("");
  return `<!doctype html><html><head><meta charset="utf-8"><title>${escapeHtml(hotelName)} staged snapshot</title>
    <style>body{font-family:Georgia,serif;margin:32px;background:#f6f4ef;color:#1c1915}article{background:#fff;border:1px solid #ddd;padding:16px 20px;margin:0 0 12px}h1{font-size:22px}h2{font-size:16px;margin:0 0 6px}p{margin:4px 0}</style>
    </head><body><h1>${escapeHtml(hotelName)} staged customer snapshot</h1><p>Staging only. Not a production publication. ${snapshot.selected.length} of ${snapshot.eligibleCount} hotel-facing opportunities.</p>${rows}</body></html>`;
}

function escapeHtml(value) {
  return String(value || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
