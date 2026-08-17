/**
 * Autonomous production wave runner — official-first → EV-gated SerpApi one-call → Rooms V3.
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
import { createSerpApiResearchCache } from "../census-autopilot-v2-1/serpapi-cache.js";
import {
  resolveFromOfficialSources,
  officialGapsForSerpApi,
} from "./official-first-resolver.js";
import { scoreSerpApiExpectedValue, analyzeOneCallCompleteness } from "./serpapi-eligibility.js";
import { resolveRoomsV3 } from "./rooms-resolver-v3.js";
import { resolveDealalityGeography } from "./geography-expansion.js";
import { VERIFIED_LIFECYCLE, WRITE_CLASS } from "./constants.js";

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

function completenessProxy(hotel, fields, roomsOk) {
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
    "Market",
    "Submarket",
    "Rooms / Keys",
  ];
  const supported = new Set(["Hotel Name", "Country"]);
  if (hotel.city) supported.add("City");
  if (hotel.website) supported.add("Website");
  if (hotel.brand || hotel.family) supported.add("Brand");
  for (const f of fields) {
    if (f === "Official Property URL") supported.add("Website");
    else supported.add(f);
  }
  if (hotel.country) {
    supported.add("Continent");
    supported.add("Sub-Continent");
  }
  if (roomsOk) supported.add("Rooms / Keys");
  const withRooms = Math.round((100 * supported.size) / identityFields.length);
  const withoutRoomsSet = new Set([...supported].filter((f) => f !== "Rooms / Keys"));
  const exclRoomsFields = identityFields.filter((f) => f !== "Rooms / Keys");
  const exclRooms = Math.round((100 * withoutRoomsSet.size) / exclRoomsFields.length);
  return { withRooms, exclRooms, supported: [...supported] };
}

function classifyLifecycle(confirmation, roomsOk, pct, pctExclRooms) {
  const confirmed =
    String(confirmation || "").includes("INDEPENDENTLY") ||
    String(confirmation || "").includes("ENRICHMENT");
  if (!confirmed) return VERIFIED_LIFECYCLE.UNRESOLVED;
  if (pct >= 95 && roomsOk) return VERIFIED_LIFECYCLE.VERIFIED_GOLDEN_COMPLETE;
  if (pctExclRooms >= 95 && !roomsOk) return VERIFIED_LIFECYCLE.VERIFIED_ROOMS_PENDING;
  if (confirmed && pct >= 70) return VERIFIED_LIFECYCLE.VERIFIED_MATERIAL_GAPS;
  if (confirmed) return VERIFIED_LIFECYCLE.VERIFIED_FIRST_PARTY_PENDING;
  return VERIFIED_LIFECYCLE.PARTIAL_IDENTITY_RESEARCH;
}

/**
 * Process one hotel through the autonomous loop.
 */
async function processHotel(cache, tracker, hotel, opts = {}) {
  const steps = [];

  // 1–3 load + family (already on hotel)
  steps.push("load_candidate", "dedupe_identity", "brand_family");

  // 4 official resolver
  let official = null;
  if (hotel.website && hotel.family && hotel.family !== "Independent") {
    official = await resolveFromOfficialSources(hotel, { delayMs: opts.officialDelayMs ?? 200 });
    steps.push("official_resolver");
  } else {
    steps.push("official_resolver_skipped");
  }

  const gaps = officialGapsForSerpApi(
    official || {
      address: { value: null },
      lat: { value: null },
      phone: { value: null },
      website: { value: hotel.website || null },
      amenities: { value: null },
      official_name: { value: null },
    }
  );
  // If no official run, all core gaps open
  if (!official) {
    gaps.length = 0;
    gaps.push("identity", "address", "coordinates", "phone", "website", "amenities");
  }

  const ev = scoreSerpApiExpectedValue(hotel, official, gaps);
  steps.push("ev_score");

  let search = { ok: false, candidates: [], from_dealality_cache: false };
  let best = null;
  let oneCall = {
    one_call_complete: false,
    second_call_required: false,
    second_call_reason: "serpapi_skipped",
    fields_present: [],
  };
  let secondCallAvoided = false;
  let serpapiUsed = false;
  let confirmationPath = "none";

  const q = `${hotel.name}, ${hotel.city || hotel.country || ""}, ${hotel.country || ""}`
    .replace(/\s+/g, " ")
    .trim();

  if (ev.eligible && tracker.canSpend(1)) {
    search = await searchCached(cache, tracker, hotel, q);
    serpapiUsed = !search.from_dealality_cache && (search.creditsCharged > 0 || search.ok);
    steps.push("serpapi_search");

    for (const cand of search.candidates || []) {
      const match = discoveryMatch(hotel, cand);
      if (!best || match.score > best.match.score) best = { candidate: cand, match };
    }

    if (best?.candidate) {
      oneCall = analyzeOneCallCompleteness(best.candidate, gaps);
      if (oneCall.second_call_required && tracker.canSpend(1)) {
        const details = await getGoogleHotelDetails(
          { property_token: best.candidate.property_token, q },
          { tracker, hotelId: hotel.candidate_id, useful: true }
        );
        steps.push("serpapi_details");
        if (details.ok && details.candidate) {
          const rematch = discoveryMatch(hotel, details.candidate);
          best = { candidate: details.candidate, match: rematch };
          oneCall = {
            ...analyzeOneCallCompleteness(best.candidate, gaps),
            second_call_required: true,
            second_call_reason: oneCall.second_call_reason,
          };
        }
      } else if (oneCall.one_call_complete || !oneCall.second_call_required) {
        secondCallAvoided = Boolean(best.candidate.property_token) && !oneCall.second_call_required;
        steps.push("serpapi_one_call_stop");
      }
    }
    confirmationPath = official?.fields_resolved?.length >= 3 ? "serpapi_assisted" : "serpapi";
  } else if (!ev.eligible) {
    steps.push("serpapi_skipped_low_ev");
    if (official?.fields_resolved?.length >= 3) confirmationPath = "official_only";
  } else {
    steps.push("serpapi_ceiling");
  }

  // Merge official into field list
  const fields = [];
  if (official?.official_name?.value) fields.push("Hotel Name");
  if (official?.address?.value) fields.push("Address");
  if (official?.lat?.value != null) fields.push("Latitude", "Longitude");
  if (official?.phone?.value) fields.push("Phone");
  if (official?.website?.value) fields.push("Website");
  if (official?.official_property_id?.value) fields.push("Official Property ID");
  if (official?.city?.value) fields.push("City");

  let confirmation = "INSUFFICIENT EVIDENCE";
  const overlap = best?.match?.name_overlap || 0;
  const eligible = best?.match?.enrichment_eligible === true;
  const level = best?.match?.level || "REJECT";
  const c = best?.candidate;

  if (c && (eligible || confirmation.startsWith("INDEPENDENTLY") || true)) {
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

  if (hotel.strata?.existing_vic && (eligible || official?.fields_resolved?.length >= 2)) {
    confirmation = "EXISTING VERIFIED — ENRICHMENT CANDIDATE";
    if (confirmationPath === "none") confirmationPath = official ? "official_only" : "serpapi";
  } else if (level === "EXACT") {
    confirmation = "INDEPENDENTLY CONFIRMED — EXACT";
  } else if (level === "HIGH" || (eligible && overlap >= 0.55)) {
    confirmation = "INDEPENDENTLY CONFIRMED — HIGH";
  } else if (overlap >= 0.55 && c?.address && c?.latitude != null) {
    confirmation = "INDEPENDENTLY CONFIRMED — HIGH";
  } else if (overlap >= 0.4) {
    confirmation = "PROBABLE — NEEDS CORROBORATION";
  } else if (
    !c &&
    official?.fields_resolved?.length >= 4 &&
    official.official_name?.value &&
    official.address?.value
  ) {
    confirmation = "INDEPENDENTLY CONFIRMED — HIGH";
    confirmationPath = "official_only";
  } else if (hotel.strata?.cvent_origin && (search.candidates || []).length === 0 && !official) {
    confirmation = "CVENT_ONLY_UNRESOLVED";
  }

  // Geography
  const geo = resolveDealalityGeography({
    ...hotel,
    city: hotel.city || official?.city?.value || null,
  });
  if (geo.market) fields.push("Market");
  if (geo.submarket && geo.submarket_confidence !== "No Match") fields.push("Submarket");
  steps.push("geography");

  // Rooms V3 — for native families with URL, or confirmed hotels
  let rooms = null;
  const shouldRooms =
    hotel.website &&
    (["IHG", "Hilton", "Choice", "Marriott", "Accor", "Hyatt", "Wyndham"].includes(hotel.family) ||
      String(confirmation).includes("INDEPENDENTLY") ||
      String(confirmation).includes("ENRICHMENT"));
  if (shouldRooms) {
    rooms = await resolveRoomsV3(
      {
        name: hotel.name,
        family: hotel.family,
        website: hotel.website || official?.website?.value,
        property_ids: hotel.property_ids,
        independent_record_id: hotel.property_identity_id,
        property_identity_id: hotel.property_identity_id,
      },
      { delayMs: opts.roomsDelayMs ?? 300 }
    );
    steps.push("rooms_v3");
    if (rooms.ok) fields.push("Rooms / Keys");
  } else {
    steps.push("rooms_skipped");
  }

  const uniqFields = [...new Set(fields)];
  const pct = completenessProxy(hotel, uniqFields, Boolean(rooms?.ok));
  const lifecycle = classifyLifecycle(confirmation, Boolean(rooms?.ok), pct.withRooms, pct.exclRooms);

  const operatorSeed =
    official?.attempts?.length && hotel.family && hotel.family !== "Independent"
      ? {
          operator: null,
          management_company: null,
          brand_family: hotel.family,
          source: official.website?.source || hotel.website,
          confidence: "LOW",
          effective_date: null,
          note: "placeholder_seed_only_no_broad_operator_research",
        }
      : null;

  return {
    candidate_id: hotel.candidate_id,
    property_identity_id: hotel.property_identity_id,
    name: hotel.name,
    country: hotel.country,
    family: hotel.family,
    wave_priority: hotel.wave_priority,
    strata: hotel.strata,
    query: q,
    steps,
    official_fields_resolved: official?.fields_resolved || [],
    official_property_id: official?.official_property_id?.value || null,
    official_reason: official?.reason || null,
    serpapi_ev: ev,
    serpapi_used: serpapiUsed || Boolean(search.ok && !search.from_dealality_cache),
    from_dealality_cache: Boolean(search.from_dealality_cache),
    confirmation_path: confirmationPath,
    one_call: oneCall,
    second_call_avoided: secondCallAvoided,
    search_ok: search.ok,
    candidate_count: (search.candidates || []).length,
    best_level: level,
    confirmation,
    enrichment_eligible: eligible || String(confirmation).includes("INDEPENDENTLY"),
    fields_resolved_technically: uniqFields,
    technically_eligible:
      String(confirmation).includes("INDEPENDENTLY") || String(confirmation).includes("ENRICHMENT"),
    rights_eligible: false,
    production_write_allowed: false,
    cvent_used_as_production_evidence: false,
    rooms_capability: SERPAPI_ROOMS_CAPABILITY,
    rooms_inferred: false,
    rooms_result: rooms
      ? {
          ok: rooms.ok,
          rooms_value: rooms.rooms_value,
          confidence: rooms.confidence,
          classification: rooms.classification,
          reason: rooms.reason,
          source: rooms.rooms_source,
        }
      : null,
    geography: geo,
    baseline_priority_proxy_pct: completenessProxy(hotel, [], false).withRooms,
    final_priority_proxy_pct: pct.withRooms,
    final_priority_proxy_excl_rooms_pct: pct.exclRooms,
    lifecycle,
    operator_seed: operatorSeed,
    best_snapshot: c
      ? {
          name: c.name,
          address: c.address || official?.address?.value || null,
          latitude: c.latitude ?? official?.lat?.value ?? null,
          longitude: c.longitude ?? official?.lng?.value ?? null,
          phone: c.phone || official?.phone?.value || null,
          website: c.website || official?.website?.value || hotel.website || null,
          property_token: c.property_token,
          hotel_class_raw: c.hotel_class_raw,
          amenities_dealality: c.amenities_dealality,
        }
      : official
        ? {
            name: official.official_name?.value || hotel.name,
            address: official.address?.value || null,
            latitude: official.lat?.value ?? null,
            longitude: official.lng?.value ?? null,
            phone: official.phone?.value || null,
            website: official.website?.value || hotel.website || null,
            property_token: null,
            hotel_class_raw: null,
            amenities_dealality: {},
          }
        : null,
    match: best?.match || null,
  };
}

export async function runProductionWave(opts) {
  const { repoRoot, cohort, ceiling, log = console.log } = opts;
  const cache = createSerpApiResearchCache(repoRoot);
  const account = await getAccount();
  const starting = account.ok ? account.total_searches_left ?? account.plan_searches_left : null;
  const tracker = new SerpApiCreditTracker({
    ceiling,
    startingSearchesLeft: starting,
  });

  const results = [];
  const roomsResults = [];
  const started = Date.now();
  let oneCallComplete = 0;
  let secondCallAvoided = 0;
  let officialOnly = 0;
  let serpapiAssisted = 0;

  for (let i = 0; i < cohort.length; i++) {
    const hotel = cohort[i];
    try {
      const r = await processHotel(cache, tracker, hotel, opts);
      results.push(r);
      if (r.rooms_result) {
        roomsResults.push({
          candidate_id: r.candidate_id,
          name: r.name,
          family: r.family,
          ...r.rooms_result,
        });
      }
      if (r.one_call?.one_call_complete) oneCallComplete += 1;
      if (r.second_call_avoided) secondCallAvoided += 1;
      if (r.confirmation_path === "official_only") officialOnly += 1;
      if (r.confirmation_path === "serpapi_assisted" || r.confirmation_path === "serpapi") {
        if (String(r.confirmation).includes("INDEPENDENTLY") || String(r.confirmation).includes("ENRICHMENT")) {
          serpapiAssisted += 1;
        }
      }
      if ((i + 1) % 25 === 0) {
        const conf = results.filter(
          (x) =>
            String(x.confirmation).includes("INDEPENDENTLY") ||
            String(x.confirmation).includes("ENRICHMENT")
        ).length;
        log(
          `[v2.2] wave ${i + 1}/${cohort.length} confirmed=${conf} searches=${tracker.charged} one_call=${oneCallComplete}`
        );
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
        lifecycle: VERIFIED_LIFECYCLE.UNRESOLVED,
      });
    }
    await new Promise((r) => setTimeout(r, 200));
  }

  const accountEnd = await getAccount();
  tracker.endingSearchesLeft = accountEnd.ok
    ? accountEnd.total_searches_left ?? accountEnd.plan_searches_left
    : null;

  return {
    runtime_ms: Date.now() - started,
    results,
    roomsResults,
    counters: {
      one_call_complete: oneCallComplete,
      second_call_avoided: secondCallAvoided,
      official_only_path: officialOnly,
      serpapi_assisted_confirmed: serpapiAssisted,
    },
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

export function classifyFieldWrites(results) {
  const counts = {
    [WRITE_CLASS.AUTO_WRITE_SAFE]: 0,
    [WRITE_CLASS.CORROBORATED_WRITE]: 0,
    [WRITE_CLASS.STEWARD_REVIEW]: 0,
    [WRITE_CLASS.FIRST_PARTY_VALIDATION]: 0,
    [WRITE_CLASS.BLOCKED_RIGHTS]: 0,
    [WRITE_CLASS.PROHIBITED]: 0,
  };
  const payloads = [];

  for (const r of results) {
    if (!r.technically_eligible && !String(r.confirmation || "").includes("ENRICHMENT")) continue;
    const snap = r.best_snapshot || {};
    const geo = r.geography || {};

    const add = (field, value, cls) => {
      if (value == null || value === "") return;
      counts[cls] += 1;
      payloads.push({
        property_identity_id: r.property_identity_id,
        field,
        value,
        write_class: cls,
        dry_run: true,
      });
    };

    add("Country", r.country, WRITE_CLASS.AUTO_WRITE_SAFE);
    add("Continent", geo.continent, WRITE_CLASS.AUTO_WRITE_SAFE);
    add("Sub-Continent", geo.sub_continent, WRITE_CLASS.AUTO_WRITE_SAFE);
    add("Market", geo.market, WRITE_CLASS.AUTO_WRITE_SAFE);
    add("Submarket", geo.submarket, WRITE_CLASS.AUTO_WRITE_SAFE);
    add("City", geo.city || r.city, WRITE_CLASS.AUTO_WRITE_SAFE);

    add("Property Name", snap.name || r.name, WRITE_CLASS.CORROBORATED_WRITE);
    add("Address", snap.address, WRITE_CLASS.CORROBORATED_WRITE);
    add("Latitude", snap.latitude, WRITE_CLASS.CORROBORATED_WRITE);
    add("Longitude", snap.longitude, WRITE_CLASS.CORROBORATED_WRITE);
    add("Phone", snap.phone, WRITE_CLASS.CORROBORATED_WRITE);
    add("Official Property URL", snap.website, WRITE_CLASS.CORROBORATED_WRITE);

    // SerpApi-derived → blocked rights until clarification
    if (r.serpapi_used) {
      for (const f of ["Address", "Latitude", "Longitude", "Phone", "Official Property URL", "Amenities"]) {
        if (r.fields_resolved_technically?.includes(f) || r.fields_resolved_technically?.includes("Website")) {
          counts[WRITE_CLASS.BLOCKED_RIGHTS] += 1;
        }
      }
    }

    if (r.rooms_result?.ok) {
      add("Rooms / Keys", r.rooms_result.rooms_value, WRITE_CLASS.CORROBORATED_WRITE);
    } else if (r.rooms_result?.classification === "FIRST-PARTY VALIDATION") {
      counts[WRITE_CLASS.FIRST_PARTY_VALIDATION] += 1;
      payloads.push({
        property_identity_id: r.property_identity_id,
        field: "Rooms / Keys",
        value: null,
        write_class: WRITE_CLASS.FIRST_PARTY_VALIDATION,
        dry_run: true,
      });
    }

    counts[WRITE_CLASS.PROHIBITED] += 1; // Rooms from SerpApi never
    payloads.push({
      property_identity_id: r.property_identity_id,
      field: "Rooms / Keys from SerpApi",
      value: null,
      write_class: WRITE_CLASS.PROHIBITED,
      dry_run: true,
    });
  }

  return { counts, payloads: payloads.slice(0, 2000), dry_run: true, airtable_writes: 0 };
}
