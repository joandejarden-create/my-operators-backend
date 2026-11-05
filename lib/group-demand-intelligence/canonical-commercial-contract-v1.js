/**
 * One read-only commercial contract for GDI.
 * Publication eligibility is evaluateProspectPublicationEligibility.
 * Historical customerVisible is not current eligibility.
 * Account development is not publication.
 */

import { evaluateProspectPublicationEligibility, hasUsableContactRoute } from "./prospect-publication-eligibility-v1.js";
import { classifyCommercialRecord } from "./workflow/gdi-demand-sourcing-v1.js";

export const CANONICAL_COMMERCIAL_CONTRACT = "GDI_CANONICAL_COMMERCIAL_CONTRACT_V1";

export function qualifyCanonicalOpportunity(record = {}, context = {}) {
  const contactBefore = hasUsableContactRoute(record).ok === true;
  const eligibility = evaluateProspectPublicationEligibility(record);
  const commercial = classifyCommercialRecord(record, context);
  const contactAfter = hasUsableContactRoute(record).ok === true;
  return {
    contract: CANONICAL_COMMERCIAL_CONTRACT,
    opportunityId: record.id || record.opportunityId || null,
    hotelId: record.hotelId || null,
    demand: {
      mechanism: commercial.mechanism,
      openBookingClaimed: commercial.openBookingClaimed === true,
      destination: commercial.destination || "unresolved",
    },
    evidence: {
      verificationStatus: commercial.blocker || "unsupported",
      suitabilityFailed: commercial.suitabilityFailed || [],
      routeKind: commercial.routeKind || "no_route",
      travelNeedGrade: eligibility.travelNeedGrade?.grade || null,
    },
    readiness: {
      suitabilityOk: eligibility.suitability?.ok === true,
      usableRoute: eligibility.route?.ok === true,
      disqualified: eligibility.eligibility === "NOT_A_PROSPECT",
    },
    publication: {
      eligibility: eligibility.eligibility,
      hotelFacing: eligibility.hotelFacing === true,
      historicalCustomerVisible: record.customerVisible === true,
      accountDevelopmentCandidate: commercial.accountDevelopmentCandidate === true,
      accountDevelopmentPublished: false,
    },
    eligibility,
    contactRetained: contactBefore === contactAfter,
  };
}

export function projectCanonicalSharePayload(record = {}, context = {}) {
  const qualified = qualifyCanonicalOpportunity(record, context);
  return {
    opportunityId: qualified.opportunityId,
    hotelFacing: qualified.publication.hotelFacing,
    historicalCustomerVisible: qualified.publication.historicalCustomerVisible,
    demandMechanism: qualified.demand.mechanism,
    accountDevelopmentPublished: false,
    contactPresent: qualified.evidence.routeKind !== "no_route",
  };
}

export function projectCanonicalPdfFacts(record = {}, context = {}) {
  const qualified = qualifyCanonicalOpportunity(record, context);
  return {
    opportunityId: qualified.opportunityId,
    hotelFacing: qualified.publication.hotelFacing,
    historicalCustomerVisible: qualified.publication.historicalCustomerVisible,
    demandMechanism: qualified.demand.mechanism,
    accountDevelopmentPublished: false,
  };
}

export function reconcileOpportunityIdentities(local = [], remote = []) {
  const seen = new Set();
  const duplicates = [];
  const rows = [];
  for (const row of [...local, ...remote]) {
    const id = String(row?.id || row?.opportunityId || "");
    if (!id) continue;
    if (seen.has(id)) duplicates.push(id);
    else {
      seen.add(id);
      rows.push(row);
    }
  }
  return { rows, duplicates, localCount: local.length, remoteCount: remote.length };
}
