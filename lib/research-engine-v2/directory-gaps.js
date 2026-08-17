/**
 * Directory-gap engine (V1.1): Official Inventory − Census and reverse.
 * Closures are NEVER auto-inferred from census−directory alone.
 */

import { assessEntityMatch, brandLabelsAlign } from "./match-confidence.js";
import { countriesAlign } from "./geo-normalize.js";
import { canonicalizeObservedBrand, resolveBrandFamily } from "./brand-family.js";

/**
 * @param {object[]} censusHotels
 * @param {object[]} directoryRows
 * @param {{ brandFamily: string, countryFilter?: RegExp, brandFilter?: (row: object) => boolean }} opts
 */
export function computeDirectoryGaps(censusHotels, directoryRows, opts) {
  /** @type {object[]} */
  const missingCensusCandidates = [];
  /** @type {object[]} */
  const censusNotInDirectory = [];

  const filteredDir = (directoryRows || []).filter((row) => {
    const country = String(row.country || row.countryRegion || "");
    if (opts.countryFilter && !opts.countryFilter.test(country)) return false;
    if (opts.brandFilter && !opts.brandFilter(row)) return false;
    return true;
  });

  for (const row of filteredDir) {
    const cityIsState = /quintana roo|baja california|ciudad de m[eé]xico/i.test(String(row.city || ""));
    const resolvedCity =
      (cityIsState && row.citySlug) || (!row.city && row.citySlug)
        ? String(row.citySlug).replace(/-/g, " ")
        : row.city || (row.citySlug ? String(row.citySlug).replace(/-/g, " ") : "");

    const syntheticHotel = {
      name: row.name || row.inferredHotelName || row.propertyName || "",
      country: row.country || row.countryRegion || "",
      city: resolvedCity,
      currentBrand: canonicalizeObservedBrand(row.brand || row.matchedBrandSetupBrand || opts.brandFamily),
      website: row.propertyUrl || row.website || row.overviewUrl || "",
      propertyId: row.propertyId || row.mnemonic || row.marsha || row.marshaCode || "",
    };

    let best = null;
    for (const census of censusHotels || []) {
      if (opts.countryFilter && !opts.countryFilter.test(String(census.country || ""))) continue;
      const family = resolveBrandFamily(census);
      if (opts.brandFamily && family !== opts.brandFamily && opts.brandFamily !== "marriott") {
        if (!(opts.brandFamily === "marriott" && family === "marriott")) continue;
      }
      const match = assessEntityMatch(census, {
        ...row,
        name: syntheticHotel.name,
        brand: syntheticHotel.currentBrand,
        city: resolvedCity,
        country: syntheticHotel.country,
        officialUrl: syntheticHotel.website,
        propertyId: syntheticHotel.propertyId,
        marsha: row.marsha || row.marshaCode,
      });
      if (!best || matchRank(match) > matchRank(best.match)) best = { census, match };
    }

    if (!best || best.match.level === "Reject" || best.match.level === "Low") {
      missingCensusCandidates.push({
        classification: "Missing Census Candidate",
        recommended_action: "Review",
        directoryName: syntheticHotel.name,
        country: syntheticHotel.country,
        city: syntheticHotel.city || null,
        brand: syntheticHotel.currentBrand || null,
        parent: row.parent || null,
        officialUrl: syntheticHotel.website || null,
        propertyId: syntheticHotel.propertyId || null,
        bestCensusMatch: best
          ? {
              hotelId: best.census.hotelId || best.census.recordId,
              hotelName: best.census.name,
              level: best.match.level,
              score: best.match.score,
            }
          : null,
        confidenceBand: "Medium",
      });
    }
  }

  // Reverse: census minus directory — contradiction research trigger, NOT closure
  for (const census of censusHotels || []) {
    const family = resolveBrandFamily(census);
    if (opts.brandFamily && family !== opts.brandFamily) continue;
    if (opts.countryFilter && !opts.countryFilter.test(String(census.country || ""))) continue;

    let best = null;
    for (const row of filteredDir) {
      const cityIsState = /quintana roo|baja california|ciudad de m[eé]xico/i.test(String(row.city || ""));
      const resolvedCity =
        (cityIsState && row.citySlug) || (!row.city && row.citySlug)
          ? String(row.citySlug).replace(/-/g, " ")
          : row.city || (row.citySlug ? String(row.citySlug).replace(/-/g, " ") : "");
      const match = assessEntityMatch(census, {
        ...row,
        name: row.name || row.inferredHotelName || row.propertyName,
        brand: row.brand,
        country: row.country || row.countryRegion,
        city: resolvedCity,
        officialUrl: row.propertyUrl || row.website || row.overviewUrl,
        propertyId: row.propertyId || row.mnemonic || row.marsha,
      });
      if (!best || matchRank(match) > matchRank(best.match)) best = { row, match };
    }

    if (!best || best.match.level === "Reject" || best.match.level === "Low") {
      censusNotInDirectory.push({
        classification: "Census Not In Official Directory",
        recommended_action: "Review",
        note: "Do NOT auto-classify as closed — may be pipeline, reflag, stale directory, geo mismatch, or naming duplicate",
        hotelId: census.hotelId || census.recordId,
        hotelName: census.name,
        country: census.country,
        currentBrand: census.currentBrand || census.affiliation,
        currentStatus: census.currentStatus || census.status,
        bestDirectoryMatch: best
          ? {
              directoryName: best.row.name || best.row.propertyName,
              level: best.match.level,
              score: best.match.score,
              url: best.row.propertyUrl || best.row.website || null,
            }
          : null,
        possibleExplanations: [
          "stale official directory",
          "pipeline property",
          "reflag",
          "independent / removed",
          "geography mismatch",
          "duplicate naming",
          "closed hotel",
        ],
        confidenceBand: "Low",
        triggerContradictionResearch: true,
      });
    }
  }

  return { missingCensusCandidates, censusNotInDirectory };
}

function matchRank(match) {
  const rank = { Exact: 5, High: 4, Medium: 3, Low: 2, Reject: 1 };
  return (rank[match?.level] || 0) * 10 + (match?.score || 0);
}

/**
 * Choice/Radisson Individuals Americas focused gap + mapping review.
 */
export function computeChoiceIndividualsGaps(censusHotels, choiceDirectoryRows, opts = {}) {
  const countryFilter = opts.countryFilter || /Mexico|Colombia|Panama|Peru|Costa Rica|Dominican|Chile|Argentina|Brazil|Jamaica|Barbados|Cayman|Honduras|Ecuador|Uruguay|Paraguay|Guatemala|Nicaragua|El Salvador|Bahamas|Aruba|Grenada|Dominica/i;

  const individualsFilter = (row) => {
    const blob = `${row.brand || ""} ${row.matchedBrandSetupBrand || ""} ${row.inferredHotelName || ""} ${row.name || ""} ${row.propertyUrl || ""}`;
    return /radisson.?individual|individuals|faranda/i.test(blob);
  };

  const base = computeDirectoryGaps(censusHotels, choiceDirectoryRows, {
    brandFamily: "choice",
    countryFilter,
    brandFilter: individualsFilter,
  });

  /** @type {object[]} */
  const brandMappingReviews = [];
  for (const census of censusHotels || []) {
    if (!/radisson individual/i.test(`${census.currentBrand || ""} ${census.name || ""}`)) continue;
    if (!countryFilter.test(String(census.country || ""))) continue;
    // If matched to Ascend/other Choice brand at High+ → mapping review
    for (const row of choiceDirectoryRows || []) {
      const match = assessEntityMatch(census, row);
      if (!match.allowMaterialCorrection) continue;
      const observedBrand = canonicalizeObservedBrand(row.brand || row.matchedBrandSetupBrand || "");
      if (observedBrand && brandLabelsAlign(census.currentBrand, observedBrand) === "conflict") {
        brandMappingReviews.push({
          classification: "Brand Mapping Review",
          recommended_action: "Review",
          hotelId: census.hotelId || census.recordId,
          hotelName: census.name,
          currentBrand: census.currentBrand,
          observedBrand,
          officialUrl: row.propertyUrl || row.website || null,
          matchLevel: match.level,
          confidenceBand: "Medium",
        });
      }
    }
  }

  return {
    ...base,
    brandMappingReviews,
    parentRegionalRelationshipReviews: [
      {
        classification: "Parent / Regional Relationship Review",
        recommended_action: "Review",
        note: "Choice Americas franchisor vs RHG outside Americas — narrative/relationship check only; no auto write",
        confidenceBand: "Medium",
      },
    ],
  };
}

/**
 * Marriott soft-brand gap (Tribute / Autograph / Design Hotels).
 */
export function computeMarriottSoftBrandGaps(censusHotels, marriottDirectoryRows, opts = {}) {
  const countryFilter = opts.countryFilter || /Mexico|mexico|Barbados|Colombia|Costa Rica|Argentina|Peru|Chile|Panama|Dominican|Jamaica|Cayman|Brazil/i;
  const softFilter = (row) => {
    const blob = `${row.brand || ""} ${row.propertyName || ""} ${row.name || ""} ${row.propertyUrl || ""} ${row.overviewUrl || ""}`;
    return /tribute|autograph|design-hotels|design hotels/i.test(blob);
  };

  return computeDirectoryGaps(censusHotels, marriottDirectoryRows, {
    brandFamily: "marriott",
    countryFilter,
    brandFilter: softFilter,
  });
}

export { countriesAlign };
