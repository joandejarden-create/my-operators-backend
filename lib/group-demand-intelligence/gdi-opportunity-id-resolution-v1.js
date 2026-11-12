/**
 * Shared GDI opportunity ID resolution — list and detail must use the same
 * identity system (hotel store + Bethesda internal research union).
 *
 * Root cause of list→detail 404s: auth list showed Bethesda commercial rows with
 * customerVisible=false, while detail treated customerVisible=false as a
 * synthetic fixture via isGdiTestOrFixtureOpportunity and returned
 * opportunity_not_found.
 */

import {
  isBethesdaInternalUnionHotel,
  mergeBethesdaInternalResearchOpportunities,
  findBethesdaInternalResearchOpportunity,
} from "./research/bethesda-internal-research-union-v1.js";

export const GDI_OPPORTUNITY_ID_RESOLUTION_V1 = "gdi-opportunity-id-resolution-v1";

const TEST_ID_RE =
  /_test_|fixture|sample|link_only|canary_promo|schema_repair|gdi_pe_test|v13_test|temp_validation/i;

const SYNTHETIC_TITLE_RE =
  /\bpe link test\b|\[test\]|\[schema repair\]|(?:^|:\s*)\S+\s+(?:GARDEN|WEDDING(?:\s+VENUE)?|COUNTRY(?:\s+CLUB)?|BANQUET(?:\s+HALL)?|EVENT(?:\s+VENUE)?|MUSEUM|RELIGIOUS|HISTORIC|PRIVATE|WINERY)\s+\d+/i;

/**
 * True synthetic / test rows only — does NOT treat customerVisible=false as
 * a fixture (that flag means not customer-published, not "fake data").
 */
export function isSyntheticGdiTestOrFixtureOpportunity(opp = {}) {
  if (opp == null || typeof opp !== "object") return false;
  if (opp.isTestData === true) return true;
  const id = String(opp.id || opp.opportunityId || "");
  if (TEST_ID_RE.test(id)) return true;
  const title = String(opp.title || opp.opportunityName || "");
  const org = String(opp.organizationName || "");
  if (SYNTHETIC_TITLE_RE.test(title) || SYNTHETIC_TITLE_RE.test(org)) return true;
  if (opp.peVenueId === "pev_test" || /^pev_test/i.test(String(opp.peVenueId || ""))) {
    return true;
  }
  const sources = Array.isArray(opp.sources)
    ? opp.sources.map((s) => s?.url || "").join(" ")
    : "";
  if (/\.example\b/i.test(sources)) return true;
  return false;
}

function normalizeOpportunityId(opportunityId) {
  return String(opportunityId || "").trim();
}

function idsMatch(a, b) {
  return normalizeOpportunityId(a) === normalizeOpportunityId(b);
}

/**
 * Resolve an opportunity from the same sources the Bethesda list uses.
 * @param {string} hotelId
 * @param {string} opportunityId
 * @param {object[]} [storeOpportunities] hotel store opportunities
 * @returns {{ opportunity: object|null, source: string, resolvedId: string|null }}
 */
export function resolveHotelOpportunityById(
  hotelId,
  opportunityId,
  storeOpportunities = [],
) {
  const want = normalizeOpportunityId(opportunityId);
  if (!want) {
    return { opportunity: null, source: "missing_id", resolvedId: null };
  }

  const storeRows = Array.isArray(storeOpportunities) ? storeOpportunities : [];
  const fromStore = storeRows.find(
    (o) => idsMatch(o?.id, want) || idsMatch(o?.opportunityId, want),
  );
  if (fromStore) {
    return {
      opportunity: fromStore,
      source: "hotel_store",
      resolvedId: String(fromStore.id || want),
    };
  }

  if (isBethesdaInternalUnionHotel(hotelId)) {
    const hist = findBethesdaInternalResearchOpportunity(hotelId, want);
    if (hist) {
      return {
        opportunity: hist,
        source: "bethesda_internal_research_union",
        resolvedId: String(hist.id || want),
      };
    }
    // Alias / family-primary fallback: search merged union once.
    const union = mergeBethesdaInternalResearchOpportunities(hotelId, storeRows);
    const fromUnion = (union.opportunities || []).find(
      (o) =>
        idsMatch(o?.id, want) ||
        idsMatch(o?.opportunityId, want) ||
        idsMatch(o?.commercialEventFamilyPrimaryId, want) ||
        (Array.isArray(o?.legacyOpportunityIds) &&
          o.legacyOpportunityIds.some((x) => idsMatch(x, want))) ||
        (Array.isArray(o?.aliasIds) && o.aliasIds.some((x) => idsMatch(x, want))),
    );
    if (fromUnion) {
      return {
        opportunity: fromUnion,
        source: "bethesda_internal_union_scan",
        resolvedId: String(fromUnion.id || want),
      };
    }
  }

  return { opportunity: null, source: "not_found", resolvedId: null };
}

/**
 * Whether founder/auth detail may open this resolved opportunity.
 * Blocks true synthetics; allows unpublished (customerVisible=false) inventory.
 */
export function mayOpenAuthOpportunityDetail(opportunity, { hotelId } = {}) {
  if (!opportunity) return { ok: false, reason: "NOT_FOUND" };
  if (isSyntheticGdiTestOrFixtureOpportunity(opportunity)) {
    return { ok: false, reason: "SYNTHETIC_FIXTURE" };
  }
  // Cross-hotel guard when stamped
  const rowHotel = String(
    opportunity.hotelId || opportunity.hotelRecordId || "",
  ).trim();
  if (rowHotel && hotelId && rowHotel !== String(hotelId).trim()) {
    return { ok: false, reason: "CROSS_HOTEL" };
  }
  return { ok: true, reason: null };
}
