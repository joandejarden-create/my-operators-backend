/**
 * Accor catalog → census matching helpers (gap / open passes).
 */

import { accorBrandTokenOverlap, accorCanonicalPropertyUrl, normalizeAccorDirectoryName } from "./hotel-census/accor-directory-name-normalize.js";
import { isBlankCensusValue } from "./hotel-census/brand-directory-enrichment-contract.js";
import {
  mapCensusRowForDirectoryMatch,
  scoreDirectoryAgainstCensus,
} from "./hotel-census/match-brand-directory-to-census.js";
import { mapExtractRowToDirectoryMatchRow } from "./hotel-census/plan-brand-census-directory-match.js";
import { citiesMatch, countriesMatch, nameSimilarity, normalizeKey } from "./independent-census/match-current-census.js";

/** @typedef {'ibis_styles'|'ibis_budget'|'ibis'|'grand_mercure'|'mercure'|'novotel'|'sofitel'|'mama_shelter'|'mgallery'|'other'} AccorBrandFamily */

/**
 * @param {string} name
 * @returns {AccorBrandFamily}
 */
export function accorBrandFamilyFromName(name) {
  const k = normalizeKey(name);
  if (!k) return "other";
  if (k.includes("ibis styles")) return "ibis_styles";
  if (k.includes("ibis budget")) return "ibis_budget";
  if (k.includes("grand mercure")) return "grand_mercure";
  if (k.includes("mercure")) return "mercure";
  if (k.includes("novotel")) return "novotel";
  if (k.includes("sofitel")) return "sofitel";
  if (k.includes("mama shelter")) return "mama_shelter";
  if (k.includes("mgallery")) return "mgallery";
  if (k.includes("ibis")) return "ibis";
  return "other";
}

const INCOMPATIBLE_BRAND_PAIRS = new Set([
  "ibis_styles|ibis_budget",
  "ibis_styles|ibis",
  "ibis_budget|ibis",
  "ibis_budget|ibis_styles",
  "ibis|ibis_styles",
  "ibis|ibis_budget",
  "grand_mercure|mercure",
  "mercure|grand_mercure",
]);

/**
 * Block ibis / ibis budget / ibis Styles cross-matches and Mercure vs Grand Mercure.
 * @param {string} censusName
 * @param {string} catalogName
 */
export function accorBrandFamiliesCompatible(censusName, catalogName) {
  const c = accorBrandFamilyFromName(censusName);
  const d = accorBrandFamilyFromName(catalogName);
  if (c === "other" || d === "other") return true;
  if (c === d) return true;
  return !INCOMPATIBLE_BRAND_PAIRS.has(`${c}|${d}`);
}

/**
 * @param {string} censusName
 * @returns {string}
 */
export function accorCatalogBrandCodeFromCensusName(censusName) {
  const family = accorBrandFamilyFromName(censusName);
  const map = {
    ibis_styles: "IBS",
    ibis_budget: "IBB",
    ibis: "IBH",
    mercure: "MER",
    grand_mercure: "MER",
    novotel: "NOV",
    sofitel: "SOF",
    mama_shelter: "MSH",
    mgallery: "MGA",
  };
  return map[family] || "";
}

/**
 * @param {object} cat catalog hotel row
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 * @param {{ queryEnlarged?: boolean }} [opts]
 */
export function passesAccorOpenMatchGate(cat, censusRow, opts = {}) {
  if (!countriesMatch(cat.country, censusRow.country)) return false;
  if (!accorBrandFamiliesCompatible(censusRow.name, cat.name)) return false;

  const cityOk = citiesMatch(cat.city, censusRow.city);
  if (opts.queryEnlarged && cityOk !== true) return false;

  const nameSim = nameSimilarity(cat.name, censusRow.name);
  const family = accorBrandFamilyFromName(censusRow.name);

  if (family === "ibis_styles" || family === "ibis_budget") {
    return nameSim >= 0.45 && accorBrandTokenOverlap(censusRow.name, cat.name);
  }
  if (family === "grand_mercure" || family === "mercure" || family === "novotel") {
    return nameSim >= 0.5 && accorBrandTokenOverlap(censusRow.name, cat.name);
  }
  if (nameSim >= 0.65) return true;
  if (nameSim >= 0.85 && cityOk === true) return true;
  if (nameSim >= 0.4 && accorBrandTokenOverlap(censusRow.name, cat.name) && cityOk === true) {
    return true;
  }
  return false;
}

/**
 * @param {object[]} catalogHotels
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>[]} censusRows
 * @param {number} minScore
 * @param {{ queryMeta?: Map<string, { enlarged?: boolean }>, allowLowConfidence?: boolean }} [opts]
 */
export function matchAccorCatalogToCensus(catalogHotels, censusRows, minScore, opts = {}) {
  const pairs = [];
  for (const cat of catalogHotels) {
    const meta = opts.queryMeta?.get(cat.propertyId);
    const dirMatch = mapExtractRowToDirectoryMatchRow(
      {
        inferredHotelName: cat.name,
        city: cat.city,
        country: cat.country,
        propertyId: cat.propertyId,
        propertyUrl: cat.propertyUrl,
        latitude: cat.latitude,
        longitude: cat.longitude,
        source: cat.source || "accor_catalog_api",
      },
      { scoringProfile: "accor" }
    );

    for (const censusRow of censusRows) {
      if (!passesAccorOpenMatchGate(cat, censusRow, { queryEnlarged: meta?.enlarged })) {
        continue;
      }
      const scored = scoreDirectoryAgainstCensus(dirMatch, censusRow);
      if (scored.score < minScore) continue;
      if (scored.confidence === "none") continue;
      if (!opts.allowLowConfidence && scored.confidence === "low") continue;
      pairs.push({ cat, censusRow, scored, score: scored.score });
    }
  }

  pairs.sort((a, b) => b.score - a.score);
  const usedCensus = new Set();
  const usedCat = new Set();
  const assigned = [];
  for (const p of pairs) {
    const id = p.cat.propertyId;
    if (usedCensus.has(p.censusRow.recordId) || usedCat.has(id)) continue;
    usedCensus.add(p.censusRow.recordId);
    usedCat.add(id);
    assigned.push(p);
  }
  return assigned;
}

/**
 * @param {object[]} assigned
 */
export function buildAccorCatalogApplyPlan(assigned) {
  const plan = [];
  for (const row of assigned) {
    const f = row.censusRow.fields || {};
    const cat = row.cat;
    const url = cat.propertyUrl || accorCanonicalPropertyUrl(cat.propertyId);
    const applyFields = {};

    if (isBlankCensusValue(f.Website) && url) applyFields.Website = url;
    if (isBlankCensusValue(f["Property ID"]) && cat.propertyId) {
      applyFields["Property ID"] = cat.propertyId;
    }
    if (isBlankCensusValue(f.Telephone) && cat.telephone) applyFields.Telephone = cat.telephone;
    if (isBlankCensusValue(f["Address 1"]) && cat.address1) applyFields["Address 1"] = cat.address1;
    if (isBlankCensusValue(f["Postal Code"]) && cat.postalCode) {
      applyFields["Postal Code"] = cat.postalCode;
    }
    if (isBlankCensusValue(f.Latitude) && cat.latitude != null) applyFields.Latitude = cat.latitude;
    if (isBlankCensusValue(f.Longitude) && cat.longitude != null) {
      applyFields.Longitude = cat.longitude;
    }

    const needsAmenities = isBlankCensusValue(f.Amenities);
    if (needsAmenities && cat.amenitiesText && !applyFields.Amenities) {
      applyFields.Amenities = cat.amenitiesText;
    }

    if (!Object.keys(applyFields).length && !needsAmenities) continue;

    plan.push({
      censusRecordId: row.censusRow.recordId,
      censusName: row.censusRow.name,
      propertyId: cat.propertyId,
      propertyUrl: url,
      catalogName: cat.name,
      matchScore: row.score,
      matchConfidence: row.scored.confidence,
      applyFields,
      needsAmenities: needsAmenities && !applyFields.Amenities,
    });
  }
  return plan;
}

/**
 * @param {object} dir row from property/continent extract
 */
export function accorDirectoryRowToCatalogHotel(dir) {
  const rawName = String(dir.inferredHotelName || "").trim();
  const name = normalizeAccorDirectoryName(rawName) || rawName;
  return {
    propertyId: String(dir.propertyId || "").toUpperCase(),
    name,
    brand: "",
    city: String(dir.city || "").trim(),
    country: String(dir.country || "").trim(),
    countryCode: String(dir.countryCode || "").trim().toUpperCase(),
    address1: String(dir.address1 || "").trim(),
    postalCode: String(dir.postalCode || "").trim(),
    telephone: String(dir.telephone || "").trim(),
    latitude: dir.latitude != null ? Number(dir.latitude) : null,
    longitude: dir.longitude != null ? Number(dir.longitude) : null,
    propertyUrl: dir.propertyUrl || accorCanonicalPropertyUrl(dir.propertyId),
    amenitiesText: String(dir.amenitiesText || "").trim(),
    source: dir.source || "accor_directory_extract",
  };
}

export { mapCensusRowForDirectoryMatch };
