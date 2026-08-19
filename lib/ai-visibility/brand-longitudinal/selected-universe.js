/**
 * Current Brand AI selected-brand universe (dropdown SoT = showcase companies).
 * Not the full Brand Explorer Active/Live set.
 */

import { listDemoBrandPortfolioOptions } from "../../dealality/demo-brand-portfolio-context.js";
import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V2, PEER_SET_ID_V3 } from "../peer-sets.js";

export const RADISSON_BRAND_ID = "recywbx1YQSTCPqW1";
export const RADISSON_PARENT = "Choice Hotels";

export const RADISSON_FAMILY_IDS = Object.freeze({
  radisson: RADISSON_BRAND_ID,
  blu: "recWPEvxBQxVVzSq3",
  red: "recmKqo7M7mLZgRqQ",
  individuals: "recRyvM8OmLlDj9G7",
});

export const SELECTED_BRANDS_EXPECTED = 19;
export const PARENT_COMPANIES_EXPECTED = 4;

export function loadSelectedBrandUniverse() {
  const parents = listDemoBrandPortfolioOptions();
  const brands = [];
  for (const p of parents) {
    for (const b of p.brands || []) {
      brands.push({
        brandId: b.brandId,
        brandName: b.brandName,
        parent: p.label,
        companyKey: p.companyKey,
      });
    }
  }
  return {
    parents: parents.map((p) => ({
      PARENT: p.label,
      companyKey: p.companyKey,
      BRANDS: (p.brands || []).map((b) => b.brandName),
      brandIds: [...(p.brandIds || [])],
      BRAND_COUNT: p.brandCount,
    })),
    brands,
    TOTAL_PARENT_COMPANIES: parents.length,
    TOTAL_SELECTED_BRANDS: brands.length,
  };
}

export function classifyPeerSetCoverage(brandId, peerEntityIds) {
  const set = new Set(peerEntityIds || []);
  return set.has(brandId);
}

export function resolveMeasurementPeerSet() {
  const cfg = loadPeerSetConfig();
  const v2 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2, commercialRegion: "CALA" }, cfg);
  const v3 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V3, commercialRegion: "CALA" }, cfg);
  return { v2, v3, measurementPeerSetId: PEER_SET_ID_V3 };
}
