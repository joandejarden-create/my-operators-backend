/**
 * resolve_hotel_identity — Exact / Strong / Probable / Ambiguous.
 * Never auto-merges ambiguous records.
 */

import {
  nameSimilarity,
  normalizeKey,
  citiesMatch,
  countriesMatch,
  parseCoords,
  distanceMeters,
  websiteHost,
  normalizePhone,
} from "../independent-census/match-current-census.js";
import {
  scoreExternalToCensusMatch,
  matchExternalToCensusPool,
  MATCH_CONFIDENCE,
} from "../research-engine-v2/external-hotel-match-engine.js";
import { MAP_CENSUS_FIELDS } from "./map_hotel_intelligence_fields.js";
import { createExternalIdRegistry } from "./external-ids.js";
import { normName } from "../research-engine-v2/census-autopilot-v2/identity-dedupe.js";

export const IDENTITY_RESOLVE_VERSION = "hotel-intelligence-identity-resolve-v1";

export const MATCH_STATUS = Object.freeze({
  EXACT: "exact",
  STRONG: "strong",
  PROBABLE: "probable",
  AMBIGUOUS: "ambiguous",
  NEW: "new",
  INSUFFICIENT: "insufficient",
});

const GEO_STRONG_M = 150;
const GEO_PROBABLE_M = 500;

function blank(v) {
  return v == null || !String(v).trim();
}

function addressKey(address, city, country) {
  return normalizeKey(
    [address, city, country]
      .filter((x) => !blank(x))
      .join(" ")
      .replace(/\b(street|st|avenue|ave|boulevard|blvd|road|rd|drive|dr)\b/g, "")
  );
}

/**
 * @param {object} input — name, address, city, country, latitude, longitude, brand, external_ids, website, phone
 * @param {object[]} censusRecords — { id, fields }
 * @param {object} [opts]
 */
export function resolveHotelIdentity(input = {}, censusRecords = [], opts = {}) {
  const idRegistry = opts.idRegistry || createExternalIdRegistry(opts.store);
  const reasons = [];
  const externalIds = Array.isArray(input.external_ids)
    ? input.external_ids
    : input.external_ids && typeof input.external_ids === "object"
      ? Object.entries(input.external_ids).map(([provider, external_id]) => ({
          provider,
          external_id,
        }))
      : [];

  // 1) Exact trusted external ID
  for (const ext of externalIds) {
    const provider = String(ext.provider || "").toLowerCase();
    const externalId = String(ext.external_id || "").trim();
    if (!provider || !externalId) continue;

    const mapped = idRegistry.findByExternalId(provider, externalId);
    if (mapped?.hotel_id) {
      reasons.push(`exact_external_id_map:${provider}`);
      return {
        match_status: MATCH_STATUS.EXACT,
        hotel_id: mapped.hotel_id,
        match_score: 1,
        matching_reasons: reasons,
        candidate_matches: [
          {
            hotel_id: mapped.hotel_id,
            airtable_record_id: mapped.airtable_record_id,
            match_status: MATCH_STATUS.EXACT,
            match_score: 1,
          },
        ],
        review_required: false,
        version: IDENTITY_RESOLVE_VERSION,
      };
    }

    // HBX hotel code on census fields
    if (provider === "hotelbeds" || provider === "hbx") {
      const hit = (censusRecords || []).find(
        (r) =>
          String(r.fields?.[MAP_CENSUS_FIELDS.hbxHotelCode] || "").trim() ===
          externalId
      );
      if (hit) {
        const hotelId = idRegistry.ensureHotelIdForAirtable(hit.id, {
          property_identity_key:
            hit.fields?.[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
        });
        try {
          idRegistry.linkExternalId(hotelId, "hotelbeds", externalId);
        } catch {
          /* ignore */
        }
        reasons.push("exact_hbx_hotel_code_on_census");
        return {
          match_status: MATCH_STATUS.EXACT,
          hotel_id: hotelId,
          match_score: 1,
          matching_reasons: reasons,
          candidate_matches: [
            {
              hotel_id: hotelId,
              airtable_record_id: hit.id,
              match_status: MATCH_STATUS.EXACT,
              match_score: 1,
              name:
                hit.fields?.[MAP_CENSUS_FIELDS.officialName] ||
                hit.fields?.[MAP_CENSUS_FIELDS.propertyName],
            },
          ],
          review_required: false,
          version: IDENTITY_RESOLVE_VERSION,
        };
      }
    }
  }

  if (blank(input.name)) {
    return {
      match_status: MATCH_STATUS.INSUFFICIENT,
      hotel_id: null,
      match_score: 0,
      matching_reasons: ["missing_name"],
      candidate_matches: [],
      review_required: true,
      version: IDENTITY_RESOLVE_VERSION,
    };
  }

  // 2) Exact: normalized name + same address
  const inAddr = addressKey(input.address, input.city, input.country);
  const exactAddressHits = [];
  if (inAddr.length >= 8) {
    for (const rec of censusRecords || []) {
      const f = rec.fields || {};
      const cName =
        f[MAP_CENSUS_FIELDS.officialName] || f[MAP_CENSUS_FIELDS.propertyName] || "";
      if (normName(input.name) !== normName(cName)) continue;
      const cAddr = addressKey(
        f[MAP_CENSUS_FIELDS.address],
        f[MAP_CENSUS_FIELDS.city],
        f[MAP_CENSUS_FIELDS.country]
      );
      if (cAddr && cAddr === inAddr) {
        exactAddressHits.push(rec);
      }
    }
  }
  if (exactAddressHits.length === 1) {
    const hit = exactAddressHits[0];
    const hotelId = idRegistry.ensureHotelIdForAirtable(hit.id, {
      property_identity_key:
        hit.fields?.[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
    });
    reasons.push("exact_normalized_name_and_address");
    return {
      match_status: MATCH_STATUS.EXACT,
      hotel_id: hotelId,
      match_score: 0.99,
      matching_reasons: reasons,
      candidate_matches: [
        {
          hotel_id: hotelId,
          airtable_record_id: hit.id,
          match_status: MATCH_STATUS.EXACT,
          match_score: 0.99,
        },
      ],
      review_required: false,
      version: IDENTITY_RESOLVE_VERSION,
    };
  }
  if (exactAddressHits.length > 1) {
    return buildAmbiguous(exactAddressHits, idRegistry, reasons, "exact_address_multi");
  }

  // 3) Pool scoring via existing engine + geo/phone/website boosts
  const external = {
    name: input.name,
    city: input.city,
    country: input.country,
    brand: input.brand,
    address: input.address,
    lat: input.latitude,
    lng: input.longitude,
    official_url: input.website,
    external_id: externalIds[0]?.external_id,
  };

  const pool = matchExternalToCensusPool(censusRecords, external);
  const candidates = (pool.scored || []).map((s) => {
    const hotelId = idRegistry.ensureHotelIdForAirtable(s.census_record_id, {});
    let matchStatus = MATCH_STATUS.NEW;
    let score = s.name_sim || 0;
    if (s.confidence === MATCH_CONFIDENCE.HIGH) {
      matchStatus = MATCH_STATUS.STRONG;
      score = Math.max(score, 0.9);
    } else if (s.confidence === MATCH_CONFIDENCE.MEDIUM) {
      matchStatus = MATCH_STATUS.PROBABLE;
      score = Math.max(score, 0.75);
    } else if (s.confidence === MATCH_CONFIDENCE.LOW) {
      matchStatus = MATCH_STATUS.PROBABLE;
      score = Math.max(score, 0.55);
    } else {
      matchStatus = MATCH_STATUS.NEW;
    }

    // Strong: similar name + same city + tight geo
    const rec = (censusRecords || []).find((r) => r.id === s.census_record_id);
    const f = rec?.fields || {};
    const geo = parseCoords(
      f[MAP_CENSUS_FIELDS.latitude],
      f[MAP_CENSUS_FIELDS.longitude]
    );
    const inGeo = parseCoords(input.latitude, input.longitude);
    let geoM = null;
    if (geo && inGeo) {
      geoM = distanceMeters(geo, inGeo);
    }
    if (
      (s.name_sim || 0) >= 0.85 &&
      citiesMatch(input.city, f[MAP_CENSUS_FIELDS.city]) !== false &&
      geoM != null &&
      geoM <= GEO_STRONG_M
    ) {
      matchStatus = MATCH_STATUS.STRONG;
      score = Math.max(score, 0.92);
      reasons.push(`strong_name_city_geo_${Math.round(geoM)}m`);
    }

    // Phone / website exact-ish
    if (
      !blank(input.phone) &&
      normalizePhone(input.phone) &&
      normalizePhone(input.phone) === normalizePhone(f[MAP_CENSUS_FIELDS.phone])
    ) {
      matchStatus =
        matchStatus === MATCH_STATUS.NEW ? MATCH_STATUS.STRONG : matchStatus;
      score = Math.max(score, 0.93);
      reasons.push("phone_match");
    }
    if (
      !blank(input.website) &&
      websiteHost(input.website) &&
      websiteHost(input.website) === websiteHost(f[MAP_CENSUS_FIELDS.website])
    ) {
      matchStatus =
        matchStatus === MATCH_STATUS.NEW ? MATCH_STATUS.STRONG : matchStatus;
      score = Math.max(score, 0.91);
      reasons.push("website_host_match");
    }

    // Probable: similar name + market/city + brand + nearby
    if (
      matchStatus === MATCH_STATUS.NEW &&
      (s.name_sim || 0) >= 0.7 &&
      citiesMatch(input.city, f[MAP_CENSUS_FIELDS.city]) !== false &&
      (geoM == null || geoM <= GEO_PROBABLE_M)
    ) {
      matchStatus = MATCH_STATUS.PROBABLE;
      score = Math.max(score, 0.72);
    }

    return {
      hotel_id: hotelId,
      airtable_record_id: s.census_record_id,
      name: s.census_name,
      match_status: matchStatus,
      match_score: Math.round(score * 1000) / 1000,
      confidence: s.confidence,
      geo_meters: geoM != null ? Math.round(geoM) : s.geo_meters,
      reasons: s.reasons || [],
      conflicts: s.conflicts || [],
    };
  });

  const viable = candidates.filter(
    (c) =>
      c.match_status === MATCH_STATUS.EXACT ||
      c.match_status === MATCH_STATUS.STRONG ||
      c.match_status === MATCH_STATUS.PROBABLE
  );

  // Same normalized name + same city + multiple near-equal hits → never auto-merge
  const sameNameCity = [];
  const inNorm = normName(input.name);
  for (const rec of censusRecords || []) {
    const f = rec.fields || {};
    const cName =
      f[MAP_CENSUS_FIELDS.officialName] || f[MAP_CENSUS_FIELDS.propertyName] || "";
    if (normName(cName) !== inNorm) continue;
    if (citiesMatch(input.city, f[MAP_CENSUS_FIELDS.city]) === false) continue;
    if (
      !blank(input.country) &&
      !countriesMatch(input.country, f[MAP_CENSUS_FIELDS.country])
    ) {
      continue;
    }
    sameNameCity.push(rec);
  }
  if (sameNameCity.length >= 2) {
    return buildAmbiguous(
      sameNameCity,
      idRegistry,
      reasons,
      "ambiguous_same_name_city_multiple_census"
    );
  }

  if (pool.reason === "ambiguous_multiple_census_matches" || viable.length > 1) {
    const top = viable.slice(0, 5);
    if (
      top.length >= 2 &&
      Math.abs((top[0].match_score || 0) - (top[1].match_score || 0)) < 0.08
    ) {
      reasons.push("ambiguous_multiple_candidates");
      return {
        match_status: MATCH_STATUS.AMBIGUOUS,
        hotel_id: null,
        match_score: top[0]?.match_score || 0,
        matching_reasons: reasons,
        candidate_matches: top,
        review_required: true,
        version: IDENTITY_RESOLVE_VERSION,
      };
    }
  }

  if (!viable.length) {
    const stagedId = idRegistry.createStagedHotelId();
    reasons.push(pool.reason || "no_census_match");
    return {
      match_status: MATCH_STATUS.NEW,
      hotel_id: stagedId,
      match_score: 0,
      matching_reasons: reasons,
      candidate_matches: candidates.slice(0, 3),
      review_required: false,
      version: IDENTITY_RESOLVE_VERSION,
    };
  }

  const best = viable[0];
  const reviewRequired =
    best.match_status === MATCH_STATUS.PROBABLE ||
    (best.conflicts && best.conflicts.length > 0);

  if (best.match_status === MATCH_STATUS.STRONG || best.match_status === MATCH_STATUS.EXACT) {
    reasons.push(`engine_${pool.reason || best.confidence}`);
  }

  return {
    match_status: best.match_status,
    hotel_id: best.hotel_id,
    match_score: best.match_score,
    matching_reasons: [...new Set([...reasons, ...(best.reasons || [])])],
    candidate_matches: viable.slice(0, 5),
    review_required: reviewRequired,
    version: IDENTITY_RESOLVE_VERSION,
  };
}

function buildAmbiguous(records, idRegistry, reasons, reason) {
  reasons.push(reason);
  const candidate_matches = records.map((hit) => {
    const hotelId = idRegistry.ensureHotelIdForAirtable(hit.id, {
      property_identity_key:
        hit.fields?.[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
    });
    return {
      hotel_id: hotelId,
      airtable_record_id: hit.id,
      match_status: MATCH_STATUS.AMBIGUOUS,
      match_score: 0.99,
      name:
        hit.fields?.[MAP_CENSUS_FIELDS.officialName] ||
        hit.fields?.[MAP_CENSUS_FIELDS.propertyName],
    };
  });
  return {
    match_status: MATCH_STATUS.AMBIGUOUS,
    hotel_id: null,
    match_score: 0.99,
    matching_reasons: reasons,
    candidate_matches,
    review_required: true,
    version: IDENTITY_RESOLVE_VERSION,
  };
}

export { scoreExternalToCensusMatch, MATCH_CONFIDENCE };
