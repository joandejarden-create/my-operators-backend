/**
 * Light cross-table integrity checks for Research Engine V2 experiment.
 * Census ↔ Brand Explorer parent/presence (no full integrity engine).
 */

import { readFileSync, existsSync } from "node:fs";
import { defaultParentForFamily, resolveBrandFamily } from "./brand-family.js";

/** Brand Explorer expected parents for benchmark brands (from product knowledge / fixtures). */
export const BENCHMARK_BRAND_PARENTS = Object.freeze({
  "Hotel Indigo": "IHG Hotels & Resorts",
  Kimpton: "IHG Hotels & Resorts",
  "Tribute Portfolio": "Marriott International",
  Avani: "Minor Hotel Group Limited",
  "Radisson Individuals Americas": "Choice Hotels International, Inc.",
});

/**
 * @param {object[]} hotelResults - checkHotelFreshness results
 * @param {{ brandExplorerPresence?: Record<string, boolean> }} [opts]
 */
export function runCrossTableChecks(hotelResults, opts = {}) {
  /** @type {object[]} */
  const findings = [];

  const byBrand = new Map();
  for (const result of hotelResults || []) {
    const brand = result.hotel?.currentBrand || "";
    if (!byBrand.has(brand)) byBrand.set(brand, []);
    byBrand.get(brand).push(result);
  }

  for (const [brand, rows] of byBrand.entries()) {
    const expectedParent = BENCHMARK_BRAND_PARENTS[brand];
    if (!expectedParent) continue;

    for (const result of rows) {
      const censusParent = result.hotel?.currentParent || "";
      const observedParent = result.observation?.parent || defaultParentForFamily(result.brandFamily);
      const family = result.brandFamily || resolveBrandFamily(result.hotel || {});

      if (censusParent && expectedParent && !parentsAlign(censusParent, expectedParent)) {
        findings.push({
          type: "census_brand_parent_mismatch",
          hotelId: result.hotel?.hotelId,
          hotelName: result.hotel?.name,
          brand,
          censusParent,
          brandExplorerExpectedParent: expectedParent,
          recommended_action: "Review",
        });
      }

      if (
        result.observation?.hotelFound &&
        observedParent &&
        censusParent &&
        !parentsAlign(censusParent, observedParent)
      ) {
        findings.push({
          type: "census_official_parent_mismatch",
          hotelId: result.hotel?.hotelId,
          hotelName: result.hotel?.name,
          brand,
          censusParent,
          officialParent: observedParent,
          recommended_action: "Proposed Parent Correction",
        });
      }

      // Status vs official bookable
      const statusClaim = (result.claims || []).find((c) => c.claimType === "OPERATING_STATUS");
      if (statusClaim?.contradictionFound) {
        findings.push({
          type: "census_status_vs_official_directory",
          hotelId: result.hotel?.hotelId,
          hotelName: result.hotel?.name,
          brand,
          censusStatus: statusClaim.currentDealalityValue,
          officialStatus: statusClaim.independentlyObservedValue,
          classification: statusClaim.claimStatus,
          recommended_action: "Proposed Status Change",
        });
      }

      // Brand reflag
      const brandClaim = (result.claims || []).find((c) => c.claimType === "CURRENT_BRAND");
      if (brandClaim?.contradictionFound) {
        findings.push({
          type: "census_brand_vs_official_directory",
          hotelId: result.hotel?.hotelId,
          hotelName: result.hotel?.name,
          censusBrand: brandClaim.currentDealalityValue,
          officialBrand: brandClaim.independentlyObservedValue,
          recommended_action: "Proposed Reflag",
        });
      }

      void family;
    }

    // Brand Explorer presence vs census operating count (Mexico-focused signal)
    const presence = opts.brandExplorerPresence?.[brand];
    const operatingCount = rows.filter((r) => /open/i.test(String(r.hotel?.currentStatus || ""))).length;
    if (presence === false && operatingCount > 0) {
      findings.push({
        type: "brand_explorer_missing_vs_census_operating",
        brand,
        censusOperatingCount: operatingCount,
        brandExplorerPresent: false,
        recommended_action: "Review",
        notes: "Census has operating hotels but Brand Explorer presence flagged absent",
      });
    }
  }

  return findings;
}

/**
 * Infer Brand Explorer presence from local complete-build / fixture reports (read-only).
 * @param {string[]} brands
 */
export function inferBrandExplorerPresenceFromReports(brands) {
  /** @type {Record<string, boolean>} */
  const presence = {};
  const map = {
    "Hotel Indigo": "reports/brand-explorer-active-profile-factory-hotel-indigo.json",
    Kimpton: "reports/brand-explorer-complete-build-kimpton.json",
    "Tribute Portfolio": "reports/brand-explorer-complete-build-tribute-portfolio.json",
    "Radisson Individuals Americas":
      "reports/brand-explorer-complete-build-radisson-individuals-by-choice.json",
    Avani: null,
  };

  for (const brand of brands) {
    const path = map[brand];
    if (!path) {
      presence[brand] = false;
      continue;
    }
    presence[brand] = existsSync(path);
    if (presence[brand]) {
      try {
        const doc = JSON.parse(readFileSync(path, "utf8"));
        // File exists is enough for presence; optionally note empty
        void doc;
      } catch {
        presence[brand] = false;
      }
    }
  }
  return presence;
}

function parentsAlign(a, b) {
  const na = String(a || "").toLowerCase();
  const nb = String(b || "").toLowerCase();
  if (!na || !nb) return false;
  if (na.includes("ihg") && nb.includes("ihg")) return true;
  if (na.includes("marriott") && nb.includes("marriott")) return true;
  if (na.includes("choice") && nb.includes("choice")) return true;
  if (na.includes("minor") && nb.includes("minor")) return true;
  return na === nb || na.includes(nb) || nb.includes(na);
}
