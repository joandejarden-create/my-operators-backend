/**
 * Controlled scale wave runner — SerpApi with Dealality cache + stratified analysis.
 */

import {
  searchGoogleHotels,
  getGoogleHotelDetails,
  matchCensusProperty,
  SerpApiCreditTracker,
  getAccount,
  SERPAPI_ROOMS_CAPABILITY,
  safeErrorMessage,
  defaultCheckIn,
  defaultCheckOut,
} from "../providers/serpapi-google-hotels/index.js";
import { normalizeGoogleHotelProperty } from "../providers/serpapi-google-hotels/normalize.js";
import { createSerpApiResearchCache } from "./serpapi-cache.js";
import { resolveRoomsV2 } from "./rooms-resolver-v2.js";

/**
 * Cached Google Hotels search.
 */
async function searchCached(cache, tracker, hotel, q) {
  const params = {
    request_type: "google_hotels_search",
    q,
    property_identity_id: hotel.property_identity_id,
    gl: "us",
    check_in_date: defaultCheckIn(),
    check_out_date: defaultCheckOut(),
  };
  const hit = cache.get(params);
  if (hit?.hit && hit.raw) {
    const data = hit.raw.data || hit.raw;
    // Reconstruct candidates from cached raw
    const props = Array.isArray(data?.properties) ? data.properties : [];
    const rootLooks = Boolean(data?.property_token) && Boolean(data?.name) && props.length === 0;
    const candidates = [];
    if (rootLooks) {
      const d = normalizeGoogleHotelProperty(data, { source: "search_direct_property" });
      if (d) candidates.push(d);
    }
    for (const p of props) {
      const n = normalizeGoogleHotelProperty(p, { source: "search_properties" });
      if (n) candidates.push(n);
    }
    tracker.record({
      endpoint: "google_hotels/search",
      hotelId: hotel.candidate_id,
      purpose: "property_search_cache_hit",
      credits: 0,
      result: "ok",
      useful: false,
      searchId: hit.key,
    });
    return {
      ok: true,
      candidates,
      response_shape: rootLooks ? "direct_property" : props.length ? "properties_list" : "empty",
      creditsCharged: 0,
      from_dealality_cache: true,
    };
  }

  if (!tracker.canSpend(1)) {
    return { ok: false, blocked: true, candidates: [], creditsCharged: 0 };
  }

  const search = await searchGoogleHotels({ q, gl: "us" }, { tracker, hotelId: hotel.candidate_id });
  // Persist underlying raw if we can re-fetch via serpapi for cache — use search reconstruction
  // Store normalized envelope
  cache.set(params, {
    ok: search.ok,
    data: {
      properties: (search.candidates || [])
        .filter((c) => c.source_shape === "search_properties")
        .map((c) => ({
          name: c.name,
          property_token: c.property_token,
          address: c.address,
          gps_coordinates: { latitude: c.latitude, longitude: c.longitude },
          type: c.property_type_raw,
          hotel_class: c.hotel_class_raw,
          link: c.google_property_url || c.website,
          phone: c.phone,
          amenities: c.amenities_raw,
          excluded_amenities: c.excluded_amenities_raw,
        })),
      ...(search.candidates?.[0]?.source_shape === "search_direct_property"
        ? {
            name: search.candidates[0].name,
            property_token: search.candidates[0].property_token,
            address: search.candidates[0].address,
            gps_coordinates: {
              latitude: search.candidates[0].latitude,
              longitude: search.candidates[0].longitude,
            },
            phone: search.candidates[0].phone,
            link: search.candidates[0].website,
            hotel_class: search.candidates[0].hotel_class_raw,
            type: search.candidates[0].property_type_raw,
            amenities: search.candidates[0].amenities_raw,
            excluded_amenities: search.candidates[0].excluded_amenities_raw,
            description: search.candidates[0].description_analysis_only,
          }
        : {}),
    },
  });
  return { ...search, from_dealality_cache: false };
}

function classifyFailure(hotel, result) {
  if (result.confirmation?.startsWith("INDEPENDENTLY") || result.confirmation?.includes("ENRICHMENT")) {
    return null;
  }
  if (hotel.strata?.weak_identity) return "bad Cvent identity";
  if (result.candidate_count === 0) return "Google Hotels coverage / empty";
  if ((result.match?.name_overlap || 0) < 0.35) return "query construction / name mismatch";
  if (result.best_level === "REJECT" && result.best_snapshot) return "city ambiguity / match gate";
  if (result.confirmation === "PROBABLE — NEEDS CORROBORATION") return "insufficient independent evidence";
  if (/closed|permanently closed/i.test(result.best_snapshot?.name || "")) return "closed hotel";
  return "other";
}

function discoveryMatch(hotel, cand) {
  const censusHotel = {
    name: hotel.name,
    city: hotel.city || cand.city || null,
    country: hotel.country || "Mexico",
    brand: hotel.brand || hotel.family,
    website: hotel.website,
  };
  const match = matchCensusProperty(censusHotel, cand);
  if (!hotel.city && match.name_overlap >= 0.5 && cand.address && cand.latitude != null) {
    if (match.level === "MEDIUM" || match.level === "LOW") {
      match.level = match.name_overlap >= 0.55 ? "HIGH" : match.level;
      match.enrichment_eligible = match.name_overlap >= 0.55;
      match.reasons = [...(match.reasons || []), "discovery_city_borrowed"];
    }
  }
  return match;
}

async function researchHotel(cache, tracker, hotel) {
  const q = `${hotel.name}, ${hotel.city || hotel.country || ""}, ${hotel.country || ""}`
    .replace(/\s+/g, " ")
    .trim();

  const search = await searchCached(cache, tracker, hotel, q);
  let best = null;
  for (const cand of search.candidates || []) {
    const match = discoveryMatch(hotel, cand);
    if (!best || match.score > best.match.score) best = { candidate: cand, match };
  }

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
      const rematch = discoveryMatch(hotel, details.candidate);
      best = { candidate: details.candidate, match: rematch };
    }
  }

  let confirmation = "INSUFFICIENT EVIDENCE";
  const overlap = best?.match?.name_overlap || 0;
  const eligible = best?.match?.enrichment_eligible === true;
  const level = best?.match?.level || "REJECT";

  if (hotel.strata?.existing_vic && eligible) {
    confirmation = "EXISTING VERIFIED — ENRICHMENT CANDIDATE";
  } else if (level === "EXACT") {
    confirmation = "INDEPENDENTLY CONFIRMED — EXACT";
  } else if (level === "HIGH" || (eligible && overlap >= 0.55)) {
    confirmation = "INDEPENDENTLY CONFIRMED — HIGH";
  } else if (overlap >= 0.55 && best?.candidate?.address && best?.candidate?.latitude != null) {
    confirmation = "INDEPENDENTLY CONFIRMED — HIGH";
  } else if (overlap >= 0.4) {
    confirmation = "PROBABLE — NEEDS CORROBORATION";
  } else if (hotel.strata?.cvent_origin && (search.candidates || []).length === 0) {
    confirmation = "CVENT_ONLY_UNRESOLVED";
  }

  const fields = [];
  const c = best?.candidate;
  if (c && (eligible || confirmation.startsWith("INDEPENDENTLY"))) {
    if (c.address) fields.push("Address");
    if (c.latitude != null) fields.push("Latitude");
    if (c.longitude != null) fields.push("Longitude");
    if (c.phone) fields.push("Phone");
    if (c.website) fields.push("Website");
    if (Object.keys(c.amenities_dealality || {}).length) fields.push("Amenities");
    if (c.property_type_raw) fields.push("Property Type");
    if (c.hotel_class_raw != null) fields.push("Hotel Class (raw)");
    if (c.name) fields.push("Hotel Name");
  }

  // Baseline vs final completeness (simplified Priority identity set)
  const identityFields = [
    "Hotel Name",
    "Brand",
    "Country",
    "City",
    "Address",
    "Latitude",
    "Longitude",
    "Website",
    "Phone",
    "Property Type",
    "Continent",
    "Sub-Continent",
    "Rooms / Keys",
  ];
  const baselineSupported = ["Hotel Name", "Country"].concat(
    hotel.city ? ["City"] : [],
    hotel.website ? ["Website"] : [],
    hotel.brand || hotel.family ? ["Brand"] : []
  );
  const finalSupported = new Set(baselineSupported);
  for (const f of fields) {
    if (f === "Official Property URL") finalSupported.add("Website");
    else finalSupported.add(f);
  }
  if (hotel.country) {
    finalSupported.add("Continent");
    finalSupported.add("Sub-Continent");
  }
  const baseline_pct = Math.round((100 * baselineSupported.length) / identityFields.length);
  const final_pct = Math.round((100 * finalSupported.size) / identityFields.length);

  return {
    candidate_id: hotel.candidate_id,
    name: hotel.name,
    country: hotel.country,
    strata: hotel.strata,
    query: q,
    from_dealality_cache: Boolean(search.from_dealality_cache),
    search_ok: search.ok,
    candidate_count: (search.candidates || []).length,
    best_level: level,
    confirmation,
    enrichment_eligible: eligible || confirmation.startsWith("INDEPENDENTLY"),
    fields_resolved_technically: fields,
    technically_eligible: confirmation.startsWith("INDEPENDENTLY") || confirmation.includes("ENRICHMENT"),
    rights_eligible: false,
    production_write_allowed: false,
    cvent_used_as_production_evidence: false,
    rooms_capability: SERPAPI_ROOMS_CAPABILITY,
    rooms_inferred: false,
    baseline_priority_proxy_pct: baseline_pct,
    final_priority_proxy_pct: final_pct,
    fields_gained: Math.max(0, finalSupported.size - baselineSupported.length),
    best_snapshot: c
      ? {
          name: c.name,
          address: c.address,
          latitude: c.latitude,
          longitude: c.longitude,
          phone: c.phone,
          website: c.website,
          property_token: c.property_token,
          hotel_class_raw: c.hotel_class_raw,
          amenities_dealality: c.amenities_dealality,
        }
      : null,
    match: best?.match || null,
    failure_class: null,
  };
}

/**
 * @param {object} opts
 */
export async function runControlledWave(opts) {
  const { repoRoot, cohort, ceiling, log = console.log } = opts;
  const cache = createSerpApiResearchCache(repoRoot);
  const account = await getAccount();
  const starting = account.ok ? account.total_searches_left ?? account.plan_searches_left : null;
  const tracker = new SerpApiCreditTracker({
    ceiling,
    startingSearchesLeft: starting,
  });

  const results = [];
  const started = Date.now();

  for (let i = 0; i < cohort.length; i++) {
    const hotel = cohort[i];
    if (!tracker.canSpend(1)) {
      log(`[v2.1] search ceiling reached at ${i}/${cohort.length}`);
      break;
    }
    try {
      const r = await researchHotel(cache, tracker, hotel);
      r.failure_class = classifyFailure(hotel, r);
      results.push(r);
      if ((i + 1) % 25 === 0) {
        log(`[v2.1] wave ${i + 1}/${cohort.length} confirmed=${results.filter((x) => String(x.confirmation).includes("INDEPENDENTLY")).length} searches=${tracker.charged}`);
      }
    } catch (err) {
      results.push({
        candidate_id: hotel.candidate_id,
        name: hotel.name,
        confirmation: "INSUFFICIENT EVIDENCE",
        error: safeErrorMessage(err),
        fail_closed: true,
        cvent_used_as_production_evidence: false,
        rooms_inferred: false,
        failure_class: "other",
      });
    }
    await new Promise((r) => setTimeout(r, 250));
  }

  // Rooms resolver for native-strong Mexico in wave
  const roomsResults = [];
  const roomsTargets = cohort.filter(
    (h) =>
      h.strata?.existing_vic &&
      ["IHG", "Hilton", "Choice"].includes(h.family) &&
      h.website
  );
  log(`[v2.1] Rooms Resolver V2 on ${roomsTargets.length} Mexico native-strong…`);
  for (const h of roomsTargets) {
    const rr = await resolveRoomsV2(
      {
        name: h.name,
        family: h.family,
        website: h.website,
        property_ids: h.property_ids,
        independent_record_id: h.property_identity_id,
      },
      { delayMs: 400 }
    );
    roomsResults.push({
      candidate_id: h.candidate_id,
      name: h.name,
      family: h.family,
      ...rr,
    });
    // Attach rooms to matching wave result
    const wr = results.find((r) => r.candidate_id === h.candidate_id);
    if (wr && rr.ok) {
      wr.rooms_value = rr.rooms_value;
      wr.rooms_resolved = true;
      wr.final_priority_proxy_pct = Math.min(100, (wr.final_priority_proxy_pct || 0) + 8);
      wr.fields_gained = (wr.fields_gained || 0) + 1;
      wr.fields_resolved_technically = [...(wr.fields_resolved_technically || []), "Rooms / Keys"];
    }
  }

  const accountEnd = await getAccount();
  tracker.endingSearchesLeft = accountEnd.ok
    ? accountEnd.total_searches_left ?? accountEnd.plan_searches_left
    : null;

  return {
    runtime_ms: Date.now() - started,
    results,
    roomsResults,
    credit_ledger: tracker.summary(),
    account_start: account.ok
      ? { plan_name: account.plan_name, total_searches_left: starting }
      : account,
    account_end: accountEnd.ok
      ? {
          plan_name: accountEnd.plan_name,
          total_searches_left: accountEnd.total_searches_left,
        }
      : null,
    actual_delta:
      starting != null && tracker.endingSearchesLeft != null
        ? starting - tracker.endingSearchesLeft
        : null,
    cache_stats: cache.stats(),
  };
}

export function stratifyWaveResults(cohort, results) {
  function rate(pred) {
    const ids = new Set(cohort.filter(pred).map((c) => c.candidate_id));
    const subset = results.filter((r) => ids.has(r.candidate_id));
    const conf = subset.filter((r) => String(r.confirmation || "").includes("INDEPENDENTLY"));
    return {
      n: subset.length,
      independently_confirmed: conf.length,
      rate_pct: subset.length ? Math.round((1000 * conf.length) / subset.length) / 10 : null,
    };
  }

  return {
    overall: rate(() => true),
    branded_named: rate((c) => c.strata.branded && !c.strata.weak_identity),
    independent_named: rate((c) => c.strata.independent && !c.strata.weak_identity),
    cvent_challenges: rate((c) => c.strata.cvent_origin),
    soft_collection: rate((c) => c.strata.soft_collection),
    sibling_risk: rate((c) => c.strata.sibling_risk),
    weak_identity: rate((c) => c.strata.weak_identity),
    existing_vic: rate((c) => c.strata.existing_vic),
    by_region: Object.fromEntries(
      [...new Set(cohort.map((c) => c.strata.region))].map((reg) => [
        reg,
        rate((c) => c.strata.region === reg),
      ])
    ),
    by_country: Object.fromEntries(
      [...new Set(cohort.map((c) => c.country))]
        .slice(0, 20)
        .map((country) => [country, rate((c) => c.country === country)])
    ),
  };
}
