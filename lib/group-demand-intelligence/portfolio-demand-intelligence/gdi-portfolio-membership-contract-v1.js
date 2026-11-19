/**
 * GDI Portfolio Demand Intelligence — operator portfolio membership V1 (PDI-1).
 *
 * Explicit operator ↔ hotel ↔ market membership. Never infer membership solely
 * from free-text notes or fuzzy company-name matching.
 *
 * Census Management Company may authorize membership only when the exact
 * steward-verified label matches. Fixture registries are steward-authorized
 * for offline $0 certification.
 *
 * No Airtable writes. No production scoring weights.
 */

export const GDI_PORTFOLIO_MEMBERSHIP_CONTRACT_V1 =
  "gdi-portfolio-membership-contract-v1";

export const MEMBERSHIP_PROVENANCE_KIND = Object.freeze({
  /** Exact census / HPC Operator·Management Company label, steward-mapped. */
  CENSUS_MGMT_COMPANY_EXACT: "CENSUS_MGMT_COMPANY_EXACT",
  /** Explicit steward-authorized fixture (offline / pilot). */
  STEWARD_AUTHORIZED_FIXTURE: "STEWARD_AUTHORIZED_FIXTURE",
  /** Rejected — notes / fuzzy text only. */
  NOTES_OR_FUZZY_TEXT_REJECTED: "NOTES_OR_FUZZY_TEXT_REJECTED",
});

export const MEMBERSHIP_VALIDITY = Object.freeze({
  ACTIVE: "ACTIVE",
  SUSPENDED: "SUSPENDED",
  REVOKED: "REVOKED",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Normalize a steward-provided membership row.
 */
export function normalizePortfolioMembership(row = {}) {
  const operatorId = text(row.operatorId);
  const hotelId = text(row.hotelId);
  const marketId = text(row.marketId);
  const provenanceKind = text(row.provenanceKind);
  const validity = text(row.validity) || MEMBERSHIP_VALIDITY.ACTIVE;

  const blockers = [];
  if (!operatorId) blockers.push("MISSING_OPERATOR_ID");
  if (!hotelId) blockers.push("MISSING_HOTEL_ID");
  if (!marketId) blockers.push("MISSING_MARKET_ID");
  if (
    provenanceKind === MEMBERSHIP_PROVENANCE_KIND.NOTES_OR_FUZZY_TEXT_REJECTED ||
    provenanceKind === "NOTES" ||
    provenanceKind === "FUZZY_TEXT"
  ) {
    blockers.push("PROVENANCE_NOTES_OR_FUZZY_REJECTED");
  }
  if (
    provenanceKind &&
    provenanceKind !== MEMBERSHIP_PROVENANCE_KIND.CENSUS_MGMT_COMPANY_EXACT &&
    provenanceKind !== MEMBERSHIP_PROVENANCE_KIND.STEWARD_AUTHORIZED_FIXTURE
  ) {
    blockers.push("PROVENANCE_KIND_NOT_AUTHORIZED");
  }
  if (!provenanceKind) blockers.push("MISSING_PROVENANCE_KIND");
  if (validity !== MEMBERSHIP_VALIDITY.ACTIVE) {
    blockers.push(`MEMBERSHIP_NOT_ACTIVE:${validity || "UNKNOWN"}`);
  }

  return {
    operatorId: operatorId || null,
    hotelId: hotelId || null,
    marketId: marketId || null,
    hotelDisplayName: text(row.hotelDisplayName) || null,
    marketLabel: text(row.marketLabel) || null,
    operatorDisplayName: text(row.operatorDisplayName) || null,
    provenanceKind: provenanceKind || null,
    provenanceDetail: text(row.provenanceDetail) || null,
    censusManagementCompanyExact:
      text(row.censusManagementCompanyExact) || null,
    validity,
    authorized: blockers.length === 0,
    blockers,
  };
}

/**
 * Build an in-memory membership index from explicit rows only.
 */
export function buildPortfolioMembershipIndex(rows = []) {
  const memberships = rows.map(normalizePortfolioMembership);
  const active = memberships.filter((m) => m.authorized);
  const byOperatorMarket = new Map();
  for (const m of active) {
    const key = `${m.operatorId}::${m.marketId}`;
    if (!byOperatorMarket.has(key)) byOperatorMarket.set(key, []);
    byOperatorMarket.get(key).push(m);
  }
  return {
    version: GDI_PORTFOLIO_MEMBERSHIP_CONTRACT_V1,
    memberships,
    activeCount: active.length,
    rejectedCount: memberships.length - active.length,
    listHotelsForOperatorMarket(operatorId, marketId) {
      const key = `${text(operatorId)}::${text(marketId)}`;
      return byOperatorMarket.get(key) || [];
    },
    isHotelAuthorized(operatorId, marketId, hotelId) {
      return this.listHotelsForOperatorMarket(operatorId, marketId).some(
        (m) => m.hotelId === text(hotelId),
      );
    },
  };
}

/**
 * Reject note-only membership attempts (fail closed).
 */
export function rejectNotesOnlyMembershipAttempt({
  operatorId,
  hotelId,
  marketId,
  noteText,
} = {}) {
  return normalizePortfolioMembership({
    operatorId,
    hotelId,
    marketId,
    provenanceKind: MEMBERSHIP_PROVENANCE_KIND.NOTES_OR_FUZZY_TEXT_REJECTED,
    provenanceDetail: text(noteText) || "notes_only_rejected",
    validity: MEMBERSHIP_VALIDITY.ACTIVE,
  });
}

export default {
  GDI_PORTFOLIO_MEMBERSHIP_CONTRACT_V1,
  MEMBERSHIP_PROVENANCE_KIND,
  MEMBERSHIP_VALIDITY,
  normalizePortfolioMembership,
  buildPortfolioMembershipIndex,
  rejectNotesOnlyMembershipAttempt,
};
