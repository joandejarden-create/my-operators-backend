/**
 * Cross-family property identity — Verified Independent Census.
 * Prefer coords/address/IDs over fuzzy name alone.
 */

import { tokenSimilarity } from "../adapters/adapter-utils.js";

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function normName(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(hotel|the|a|an|by|and|hilton|ihg|inn|suites|resort)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {object} a - verified record
 * @param {object} b
 */
export function classifyCrossFamilyPair(a, b) {
  const reasons = [];
  const alat = Number(a.fields?.Latitude ?? a.fields?.latitude);
  const alng = Number(a.fields?.Longitude ?? a.fields?.longitude);
  const blat = Number(b.fields?.Latitude ?? b.fields?.latitude);
  const blng = Number(b.fields?.Longitude ?? b.fields?.longitude);

  let distKm = null;
  if ([alat, alng, blat, blng].every((n) => Number.isFinite(n))) {
    distKm = haversineKm(alat, alng, blat, blng);
    if (distKm <= 0.08) reasons.push(`coords_within_${distKm.toFixed(3)}km`);
    else if (distKm <= 0.25) reasons.push(`coords_near_${distKm.toFixed(3)}km`);
  }

  const nameSim = tokenSimilarity(normName(a.fields?.name || a.canonical_hotel_name), normName(b.fields?.name || b.canonical_hotel_name));
  if (nameSim >= 0.75) reasons.push(`name_sim_${nameSim.toFixed(2)}`);

  const cityA = String(a.fields?.city || a.normalized_city || "").toLowerCase();
  const cityB = String(b.fields?.city || b.normalized_city || "").toLowerCase();
  if (cityA && cityB && (cityA.includes(cityB) || cityB.includes(cityA))) reasons.push("city_align");

  const addrA = String(a.fields?.["Address 1"] || "").toLowerCase();
  const addrB = String(b.fields?.["Address 1"] || "").toLowerCase();
  if (addrA && addrB && tokenSimilarity(addrA, addrB) >= 0.7) reasons.push("address_align");

  const brandA = String(a.brand || a.fields?.Affiliation || "").toLowerCase();
  const brandB = String(b.brand || b.fields?.Affiliation || "").toLowerCase();
  const parentA = String(a.parent || "").toLowerCase();
  const parentB = String(b.parent || "").toLowerCase();
  const crossFamily = parentA && parentB && parentA !== parentB;

  // Never merge on fuzzy name alone
  const strongGeo = distKm != null && distKm <= 0.08;
  const nearGeo = distKm != null && distKm <= 0.25;
  const nameStrong = nameSim >= 0.85;
  const nameOk = nameSim >= 0.65;

  /** @type {string} */
  let classification = "Distinct Property";
  if (strongGeo && (nameOk || reasons.includes("address_align"))) {
    if (crossFamily) {
      classification =
        brandA && brandB && brandA !== brandB
          ? "Same Physical Property — Historical Affiliation"
          : "Same Physical Property — Current Affiliation";
      // Cross-family same coords is typically reflag / conversion history
      if (crossFamily) classification = "Same Physical Property — Historical Affiliation";
    } else if (brandA && brandB && brandA !== brandB) {
      classification = "Dual-Branded Property";
    } else {
      classification = "Same Physical Property — Current Affiliation";
    }
  } else if (nearGeo && nameStrong && reasons.includes("city_align")) {
    classification = "Probable Same Property — Review";
  } else if (nameStrong && reasons.includes("city_align") && distKm == null) {
    classification = "Insufficient Evidence";
  } else if (nameSim >= 0.9 && !strongGeo && !nearGeo) {
    classification = "Insufficient Evidence"; // fuzzy name alone — do not merge
  }

  return {
    classification,
    distance_km: distKm,
    name_similarity: nameSim,
    reasons,
    a: {
      id: a.independent_record_id,
      name: a.fields?.name || a.canonical_hotel_name,
      brand: a.brand || a.fields?.Affiliation,
      parent: a.parent,
    },
    b: {
      id: b.independent_record_id,
      name: b.fields?.name || b.canonical_hotel_name,
      brand: b.brand || b.fields?.Affiliation,
      parent: b.parent,
    },
  };
}

/**
 * Compare IHG Mexico verified records vs Hilton Mexico verified records.
 * @param {object[]} ihgRecords
 * @param {object[]} hiltonRecords
 */
export function findCrossFamilyIdentities(ihgRecords, hiltonRecords) {
  /** @type {object[]} */
  const pairs = [];
  for (const a of ihgRecords || []) {
    for (const b of hiltonRecords || []) {
      const result = classifyCrossFamilyPair(a, b);
      if (
        [
          "Same Physical Property — Current Affiliation",
          "Same Physical Property — Historical Affiliation",
          "Dual-Branded Property",
          "Probable Same Property — Review",
        ].includes(result.classification)
      ) {
        pairs.push(result);
      }
    }
  }

  return {
    generatedAt: new Date().toISOString(),
    ihgCount: (ihgRecords || []).length,
    hiltonCount: (hiltonRecords || []).length,
    pairs,
    summary: {
      same_current: pairs.filter((p) => p.classification.includes("Current")).length,
      same_historical: pairs.filter((p) => p.classification.includes("Historical")).length,
      dual: pairs.filter((p) => p.classification.includes("Dual")).length,
      probable_review: pairs.filter((p) => p.classification.includes("Probable")).length,
    },
    recommendation: {
      need_property_identity_separate_from_affiliation: true,
      rationale:
        "Cross-family and reflag cases require durable property_identity with temporal affiliation history; brand affiliation alone is insufficient.",
      minimal_model: {
        property_identity: "stable Dealality property id",
        affiliation: "brand string",
        valid_from: "date|null",
        valid_to: "date|null",
        current_affiliation: "boolean",
        evidence: "claim/evidence refs",
      },
    },
  };
}
