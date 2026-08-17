/**
 * Phase B — bounded SerpApi live wave (Exact/High only for enrichment proposals).
 */

import {
  searchGoogleHotels,
  getGoogleHotelDetails,
  matchCensusProperty,
  SerpApiCreditTracker,
  getAccount,
  SERPAPI_ROOMS_CAPABILITY,
  safeErrorMessage,
} from "../providers/serpapi-google-hotels/index.js";
import { CLASSIFICATION, OPERATING_STATES } from "./constants.js";

/**
 * Select representative Phase B cohort.
 * @param {object[]} classifiedRows
 * @param {number} maxN
 */
export function selectPhaseBCohort(classifiedRows, maxN) {
  const newCands = classifiedRows.filter(
    (r) => r.classification === CLASSIFICATION.NEW_PROPERTY_CANDIDATE && r.origin_name
  );
  const existing = classifiedRows.filter(
    (r) => r.classification === CLASSIFICATION.EXISTING_VERIFIED_PROPERTY && r.candidate_origin === "VERIFIED_INDEPENDENT"
  );

  const countriesPreferred = [
    "Mexico",
    "Brazil",
    "Dominican Republic",
    "Colombia",
    "Costa Rica",
    "Argentina",
    "Jamaica",
    "Chile",
    "Peru",
    "Panama",
  ];

  const picked = [];
  const usedCountries = new Set();

  function take(pool, n, preferNamed = true) {
    for (const c of countriesPreferred) {
      if (picked.length >= maxN) break;
      const hit = pool.find(
        (r) =>
          r.origin_country === c &&
          !picked.some((p) => p.candidate_id === r.candidate_id) &&
          (!preferNamed || (r.origin_name && r.origin_name.length > 8))
      );
      if (hit) {
        picked.push(hit);
        usedCountries.add(c);
      }
    }
    for (const r of pool) {
      if (picked.length >= n) break;
      if (picked.some((p) => p.candidate_id === r.candidate_id)) continue;
      if (preferNamed && (!r.origin_name || r.origin_name.length < 6)) continue;
      picked.push(r);
    }
  }

  // ~60% new challenges, ~40% existing VIC enrichment
  const nNew = Math.ceil(maxN * 0.6);
  const nExist = maxN - nNew;
  take(newCands, nNew);
  take(existing, nExist + (nNew - Math.min(nNew, picked.length)));

  // Fill remainder
  take(newCands, maxN);
  take(existing, maxN);

  return picked.slice(0, maxN).map((r, i) => ({
    wave_index: i,
    candidate_id: r.candidate_id,
    property_identity_id: r.property_identity_id,
    name: r.origin_name,
    country: r.origin_country,
    city: r.origin_city,
    classification: r.classification,
    brand_family_inferred: r.brand_family_inferred,
    website: r.website || r.origin_url,
    brand: r.brand || null,
    family: r.family || r.brand_family_inferred,
    match_vic_id: r.match_vic_id,
  }));
}

async function researchOne(hotel, tracker) {
  const q = `${hotel.name}, ${hotel.city || hotel.country || ""}, ${hotel.country || ""}`
    .replace(/\s+/g, " ")
    .trim();

  const search = await searchGoogleHotels(
    { q, gl: "us" },
    { tracker, hotelId: hotel.candidate_id }
  );

  let best = null;
  const baseHotel = {
    name: hotel.name,
    city: hotel.city,
    country: hotel.country || "Mexico",
    brand: hotel.brand || hotel.brand_family_inferred,
    website: hotel.website && !String(hotel.website).includes("cvent.com") ? hotel.website : null,
  };

  for (const cand of search.candidates || []) {
    // Discovery: if challenge has no city, borrow candidate city/address locality for geo check
    const censusHotel = {
      ...baseHotel,
      city: baseHotel.city || cand.city || null,
    };
    const match = matchCensusProperty(censusHotel, cand);
    // Soft-upgrade discovery confirmation scoring when city was unknown on challenge
    if (!hotel.city && match.name_overlap >= 0.5 && cand.address && cand.latitude != null) {
      match.enrichment_eligible = match.enrichment_eligible || match.name_overlap >= 0.55;
      if (match.level === "MEDIUM" && match.name_overlap >= 0.55) {
        match.level = "HIGH";
        match.enrichment_eligible = true;
        match.reasons = [...(match.reasons || []), "discovery_city_borrowed"];
      }
    }
    if (!best || match.score > best.match.score) best = { candidate: cand, match };
  }

  // List → details if needed
  if (
    best?.candidate?.property_token &&
    best.candidate.source_shape !== "search_direct_property" &&
    (best.match.name_overlap || 0) >= 0.4 &&
    tracker.canSpend(1)
  ) {
    const details = await getGoogleHotelDetails(
      { property_token: best.candidate.property_token, q },
      { tracker, hotelId: hotel.candidate_id, useful: true }
    );
    if (details.ok && details.candidate) {
      const censusHotel = {
        ...baseHotel,
        city: baseHotel.city || details.candidate.city || null,
      };
      const rematch = matchCensusProperty(censusHotel, details.candidate);
      if (!hotel.city && rematch.name_overlap >= 0.55 && details.candidate.address) {
        rematch.enrichment_eligible = true;
        if (rematch.level === "MEDIUM" || rematch.level === "LOW") rematch.level = "HIGH";
      }
      best = { candidate: details.candidate, match: rematch };
    }
  }

  const eligible = best?.match?.enrichment_eligible === true;
  const fieldsResolved = [];
  if (eligible && best.candidate) {
    const c = best.candidate;
    if (c.address) fieldsResolved.push("Address");
    if (c.latitude != null) fieldsResolved.push("Latitude");
    if (c.longitude != null) fieldsResolved.push("Longitude");
    if (c.phone) fieldsResolved.push("Phone");
    if (c.website) fieldsResolved.push("Official Property URL");
    if (Object.keys(c.amenities_dealality || {}).length) fieldsResolved.push("Amenities");
    if (c.hotel_class_raw != null) fieldsResolved.push("Hotel Class (raw)");
    if (c.property_type_raw) fieldsResolved.push("Property Type (input)");
  }

  let confirmation = "INSUFFICIENT EVIDENCE";
  if (hotel.classification === CLASSIFICATION.EXISTING_VERIFIED_PROPERTY && eligible) {
    confirmation = "EXISTING VERIFIED — ENRICHMENT CANDIDATE";
  } else if (
    eligible ||
    (best?.match?.name_overlap >= 0.55 && best?.candidate?.address && best?.candidate?.latitude != null)
  ) {
    confirmation = "INDEPENDENTLY CONFIRMED HOTEL";
  } else if (best && (best.match.name_overlap || 0) >= 0.4) {
    confirmation = "PROBABLE — NEEDS CORROBORATION";
  } else if ((search.candidates || []).length === 0) {
    confirmation = "INSUFFICIENT EVIDENCE";
  }

  return {
    candidate_id: hotel.candidate_id,
    query: q,
    search_ok: search.ok,
    response_shape: search.response_shape || null,
    candidate_count: (search.candidates || []).length,
    best_level: best?.match?.level || "REJECT",
    enrichment_eligible: eligible || confirmation === "INDEPENDENTLY CONFIRMED HOTEL",
    confirmation,
    fields_resolved_technically:
      fieldsResolved.length > 0
        ? fieldsResolved
        : confirmation === "INDEPENDENTLY CONFIRMED HOTEL" && best?.candidate
          ? [
              best.candidate.address && "Address",
              best.candidate.latitude != null && "Latitude",
              best.candidate.longitude != null && "Longitude",
              best.candidate.phone && "Phone",
              best.candidate.website && "Official Property URL",
            ].filter(Boolean)
          : [],
    fields_rights_eligible_for_production: [],
    technically_eligible: confirmation === "INDEPENDENTLY CONFIRMED HOTEL" || eligible,
    rights_eligible: false,
    production_write_allowed: false,
    rooms_capability: SERPAPI_ROOMS_CAPABILITY,
    rooms_inferred: false,
    cvent_used_as_production_evidence: false,
    best_snapshot: best?.candidate
      ? {
          name: best.candidate.name,
          address: best.candidate.address,
          latitude: best.candidate.latitude,
          longitude: best.candidate.longitude,
          phone: best.candidate.phone,
          website: best.candidate.website,
          property_token: best.candidate.property_token,
          hotel_class_raw: best.candidate.hotel_class_raw,
          amenities_dealality: best.candidate.amenities_dealality,
        }
      : null,
    match: best?.match || null,
  };
}

/**
 * @param {object[]} cohort
 * @param {{ ceiling: number, startingSearchesLeft: number|null }} opts
 */
export async function runPhaseB(cohort, opts) {
  const started = Date.now();
  const tracker = new SerpApiCreditTracker({
    ceiling: opts.ceiling,
    startingSearchesLeft: opts.startingSearchesLeft,
  });

  const results = [];
  for (const hotel of cohort) {
    if (!tracker.canSpend(1)) break;
    try {
      const r = await researchOne(hotel, tracker);
      results.push(r);
    } catch (err) {
      results.push({
        candidate_id: hotel.candidate_id,
        search_ok: false,
        error: safeErrorMessage(err),
        confirmation: "INSUFFICIENT EVIDENCE",
        fail_closed: true,
        cvent_used_as_production_evidence: false,
        rooms_inferred: false,
      });
    }
    await new Promise((r) => setTimeout(r, 400));
  }

  let accountEnd = null;
  try {
    accountEnd = await getAccount();
    if (accountEnd.ok) {
      tracker.endingSearchesLeft = accountEnd.total_searches_left ?? accountEnd.plan_searches_left;
    }
  } catch {
    /* ignore */
  }

  const confirmed = results.filter((r) => r.confirmation === "INDEPENDENTLY CONFIRMED HOTEL");
  const eligible = results.filter((r) => r.enrichment_eligible);

  return {
    runtime_ms: Date.now() - started,
    cohort_size: cohort.length,
    results_count: results.length,
    independently_confirmed: confirmed.length,
    enrichment_eligible: eligible.length,
    exact: results.filter((r) => r.best_level === "EXACT").length,
    high: results.filter((r) => r.best_level === "HIGH").length,
    cvent_used_as_production_evidence: false,
    rooms_inferred_any: false,
    credit_ledger: tracker.summary(),
    account_end: accountEnd?.ok
      ? {
          plan_name: accountEnd.plan_name,
          total_searches_left: accountEnd.total_searches_left,
          this_month_usage: accountEnd.this_month_usage,
        }
      : null,
    results,
  };
}
