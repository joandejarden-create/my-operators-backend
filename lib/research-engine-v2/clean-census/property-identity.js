/**
 * Property Identity V1 — durable physical property separate from brand affiliation.
 * Fuzzy name alone is never sufficient to merge.
 */

import { createHash, randomUUID } from "node:crypto";
import { tokenSimilarity } from "../adapters/adapter-utils.js";

export const PROPERTY_IDENTITY_VERSION = "property-identity-v1";

export const IDENTITY_CONFIDENCE = Object.freeze({
  EXACT: "Exact",
  HIGH: "High",
  MEDIUM: "Medium",
  LOW: "Low",
  INSUFFICIENT: "Insufficient Evidence",
});

export const REFLAG_CLASSIFICATIONS = Object.freeze({
  CONFIRMED: "Confirmed Reflag",
  PROBABLE: "Probable Reflag — Review",
  HISTORICAL: "Historical Affiliation",
  CURRENT: "Current Affiliation",
  NOT_SAME: "Not Same Property",
  INSUFFICIENT: "Insufficient Evidence",
});

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

function normText(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {object} input
 */
export function createPropertyIdentity(input = {}) {
  const officialIds = [...new Set((input.official_property_identifiers || []).filter(Boolean).map(String))];
  // Reject null/undefined/"" — Number(null)===0 would falsely treat missing coords as 0,0
  const latRaw = input.latitude;
  const lngRaw = input.longitude;
  const latOk = latRaw != null && latRaw !== "" && Number.isFinite(Number(latRaw));
  const lngOk = lngRaw != null && lngRaw !== "" && Number.isFinite(Number(lngRaw));
  const coords =
    latOk && lngOk
      ? { latitude: Number(latRaw), longitude: Number(lngRaw) }
      : null;

  const seed = [
    officialIds.sort().join("|"),
    normText(input.address || ""),
    coords ? `${coords.latitude.toFixed(5)},${coords.longitude.toFixed(5)}` : "",
    normText(input.city || ""),
    normText(input.country || ""),
  ].join("::");

  const property_id =
    input.property_id ||
    (seed.replace(/::/g, "").length > 8
      ? `prop_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`
      : `prop_${randomUUID().slice(0, 12)}`);

  return {
    property_identity_version: PROPERTY_IDENTITY_VERSION,
    property_id,
    canonical_property_name: input.canonical_property_name || null,
    address: input.address || null,
    coordinates: coords,
    city: input.city || null,
    country: input.country || null,
    official_property_identifiers: officialIds,
    official_urls: [...new Set((input.official_urls || []).filter(Boolean))],
    phones: [...new Set((input.phones || []).filter(Boolean))],
    current_affiliation: input.current_affiliation || null,
    affiliation_history: Array.isArray(input.affiliation_history) ? input.affiliation_history : [],
    known_aliases: [...new Set((input.known_aliases || []).filter(Boolean))],
    identity_confidence: input.identity_confidence || IDENTITY_CONFIDENCE.MEDIUM,
    evidence: Array.isArray(input.evidence) ? input.evidence : [],
    created_at: input.created_at || new Date().toISOString(),
  };
}

/**
 * Build identity from a verified independent census record.
 * @param {object} record
 */
export function propertyIdentityFromVerifiedRecord(record) {
  const fields = record.fields || {};
  const ids = [
    ...(record.official_property_identifiers || []),
    fields["Property ID"],
    fields["Brand Property Code"],
  ].filter(Boolean);

  const lat = fields.Latitude;
  const lng = fields.Longitude;
  const hasCoords =
    lat != null &&
    lat !== "" &&
    lng != null &&
    lng !== "" &&
    Number.isFinite(Number(lat)) &&
    Number.isFinite(Number(lng));

  let confidence = IDENTITY_CONFIDENCE.MEDIUM;
  if (ids.length && (fields["Address 1"] || hasCoords) && (fields.Website || record.official_property_url)) {
    confidence = IDENTITY_CONFIDENCE.HIGH;
  }
  if (ids.length && hasCoords && fields["Address 1"] && (fields.Website || record.official_property_url)) {
    confidence = IDENTITY_CONFIDENCE.EXACT;
  }
  if (!ids.length && !hasCoords && !fields["Address 1"]) {
    confidence = IDENTITY_CONFIDENCE.LOW;
  }

  return createPropertyIdentity({
    canonical_property_name: fields.name || record.canonical_hotel_name,
    address: fields["Address 1"] || null,
    latitude: hasCoords ? Number(lat) : null,
    longitude: hasCoords ? Number(lng) : null,
    city: fields.city || record.normalized_city || null,
    country: fields.country || record.country || null,
    official_property_identifiers: ids,
    official_urls: [fields.Website || record.official_property_url].filter(Boolean),
    phones: [fields.Telephone].filter(Boolean),
    current_affiliation: {
      brand: record.brand || fields.Affiliation || null,
      parent: record.parent || fields["Parent Company"] || null,
      current: true,
    },
    known_aliases: [fields.name, record.canonical_hotel_name].filter(Boolean),
    identity_confidence: confidence,
    evidence: [
      {
        type: "verified_independent_record",
        independent_record_id: record.independent_record_id,
        discovery_source: record.discovery_source,
      },
    ],
  });
}

/**
 * Compare two property identities / records. Never merge on fuzzy name alone.
 * @param {object} a - identity or verified record
 * @param {object} b
 */
export function assessSamePhysicalProperty(a, b) {
  const ia = a.property_id ? a : propertyIdentityFromVerifiedRecord(a);
  const ib = b.property_id ? b : propertyIdentityFromVerifiedRecord(b);
  const reasons = [];

  const idsA = new Set((ia.official_property_identifiers || []).map((x) => String(x).toUpperCase()));
  const idsB = new Set((ib.official_property_identifiers || []).map((x) => String(x).toUpperCase()));
  const idOverlap = [...idsA].filter((x) => idsB.has(x));
  if (idOverlap.length) reasons.push(`official_id_overlap:${idOverlap.join(",")}`);

  let distKm = null;
  if (ia.coordinates && ib.coordinates) {
    distKm = haversineKm(
      ia.coordinates.latitude,
      ia.coordinates.longitude,
      ib.coordinates.latitude,
      ib.coordinates.longitude
    );
    if (distKm <= 0.08) reasons.push(`coords_within_${distKm.toFixed(3)}km`);
    else if (distKm <= 0.25) reasons.push(`coords_near_${distKm.toFixed(3)}km`);
  }

  const addrSim = tokenSimilarity(normText(ia.address), normText(ib.address));
  if (ia.address && ib.address && addrSim >= 0.75) reasons.push(`address_sim_${addrSim.toFixed(2)}`);

  const urlOverlap = (ia.official_urls || []).some((u) =>
    (ib.official_urls || []).some((v) => normText(u) === normText(v) && u)
  );
  if (urlOverlap) reasons.push("url_exact");

  const phoneOverlap = (ia.phones || []).some((p) =>
    (ib.phones || []).some((q) => String(p).replace(/\D/g, "").slice(-8) === String(q).replace(/\D/g, "").slice(-8))
  );
  if (phoneOverlap) reasons.push("phone_align");

  const nameSim = tokenSimilarity(normText(ia.canonical_property_name), normText(ib.canonical_property_name));
  if (nameSim >= 0.85) reasons.push(`name_sim_${nameSim.toFixed(2)}`);

  const cityAlign =
    ia.city &&
    ib.city &&
    (normText(ia.city).includes(normText(ib.city)) || normText(ib.city).includes(normText(ia.city)));
  if (cityAlign) reasons.push("city_align");

  // Gates — fuzzy name alone NEVER sufficient
  const strongGeo = distKm != null && distKm <= 0.08;
  const nearGeo = distKm != null && distKm <= 0.25;
  const strongAddr = addrSim >= 0.75;
  const hardId = idOverlap.length > 0 || urlOverlap;

  let same = false;
  let confidence = IDENTITY_CONFIDENCE.INSUFFICIENT;

  if (hardId && (strongGeo || strongAddr || cityAlign)) {
    same = true;
    confidence = IDENTITY_CONFIDENCE.EXACT;
  } else if (strongGeo && (strongAddr || nameSim >= 0.65 || phoneOverlap)) {
    same = true;
    confidence = IDENTITY_CONFIDENCE.HIGH;
  } else if (nearGeo && strongAddr && cityAlign) {
    same = true;
    confidence = IDENTITY_CONFIDENCE.MEDIUM;
  } else if (nameSim >= 0.9 && cityAlign && !strongGeo && !strongAddr && !hardId) {
    same = false;
    confidence = IDENTITY_CONFIDENCE.INSUFFICIENT;
    reasons.push("fuzzy_name_only_rejected");
  }

  return {
    same_physical_property: same,
    identity_confidence: confidence,
    distance_km: distKm,
    name_similarity: nameSim,
    address_similarity: addrSim,
    reasons,
    property_id_a: ia.property_id,
    property_id_b: ib.property_id,
  };
}

/**
 * Classify reflag / affiliation relationship given same physical property assessment.
 * @param {object} assessment - from assessSamePhysicalProperty
 * @param {{ brandA?: string, brandB?: string, parentA?: string, parentB?: string, temporalEvidence?: boolean }} ctx
 */
export function classifyReflagOrAffiliation(assessment, ctx = {}) {
  if (!assessment.same_physical_property) {
    if (assessment.identity_confidence === IDENTITY_CONFIDENCE.INSUFFICIENT) {
      return { classification: REFLAG_CLASSIFICATIONS.INSUFFICIENT, ...assessment };
    }
    return { classification: REFLAG_CLASSIFICATIONS.NOT_SAME, ...assessment };
  }

  const brandA = String(ctx.brandA || "").toLowerCase();
  const brandB = String(ctx.brandB || "").toLowerCase();
  const parentA = String(ctx.parentA || "").toLowerCase();
  const parentB = String(ctx.parentB || "").toLowerCase();
  const brandDiff = brandA && brandB && brandA !== brandB;
  const parentDiff = parentA && parentB && parentA !== parentB;

  if (!brandDiff && !parentDiff) {
    return { classification: REFLAG_CLASSIFICATIONS.CURRENT, ...assessment };
  }

  const highIdentity =
    assessment.identity_confidence === IDENTITY_CONFIDENCE.EXACT ||
    assessment.identity_confidence === IDENTITY_CONFIDENCE.HIGH;

  if (highIdentity && ctx.temporalEvidence && brandDiff) {
    return {
      classification: parentDiff ? REFLAG_CLASSIFICATIONS.CONFIRMED : REFLAG_CLASSIFICATIONS.HISTORICAL,
      ...assessment,
    };
  }
  if (highIdentity && brandDiff) {
    return { classification: REFLAG_CLASSIFICATIONS.PROBABLE, ...assessment };
  }
  if (brandDiff) {
    return { classification: REFLAG_CLASSIFICATIONS.HISTORICAL, ...assessment };
  }
  return { classification: REFLAG_CLASSIFICATIONS.CURRENT, ...assessment };
}

/**
 * Attach property identities to a cohort; collapse duplicates within cohort.
 * Does NOT auto-merge across families — returns links.
 * @param {object[]} records
 */
export function attachPropertyIdentities(records) {
  /** @type {object[]} */
  const identities = [];
  /** @type {Map<string, string>} recordId -> property_id */
  const map = new Map();
  /** @type {object[]} */
  const intra_duplicates = [];

  for (const rec of records || []) {
    let matched = null;
    const candidate = propertyIdentityFromVerifiedRecord(rec);
    for (const existing of identities) {
      const assessment = assessSamePhysicalProperty(existing, candidate);
      if (assessment.same_physical_property && assessment.identity_confidence !== IDENTITY_CONFIDENCE.INSUFFICIENT) {
        matched = existing;
        intra_duplicates.push({
          independent_record_id: rec.independent_record_id,
          matched_property_id: existing.property_id,
          assessment,
        });
        // Preserve aliases / ids
        existing.known_aliases = [
          ...new Set([
            ...(existing.known_aliases || []),
            candidate.canonical_property_name,
            ...(candidate.known_aliases || []),
          ].filter(Boolean)),
        ];
        existing.official_property_identifiers = [
          ...new Set([
            ...(existing.official_property_identifiers || []),
            ...(candidate.official_property_identifiers || []),
          ]),
        ];
        break;
      }
    }
    if (!matched) {
      identities.push(candidate);
      matched = candidate;
    }
    map.set(rec.independent_record_id, matched.property_id);
    rec.property_id = matched.property_id;
    rec.property_identity = matched;
  }

  return {
    property_identity_version: PROPERTY_IDENTITY_VERSION,
    unique_physical_properties: identities.length,
    record_count: (records || []).length,
    intra_cohort_duplicate_links: intra_duplicates,
    identities,
    record_to_property_id: Object.fromEntries(map),
  };
}
