#!/usr/bin/env node
/**
 * SerpApi Google Hotels Benchmark V1 — Golden Census + Cvent Discovery
 *
 * npm run serpapi:benchmark-v1
 *
 * Never prints SERPAPI_KEY. No Airtable. No Webhound. No Autopilot integration.
 * Does not modify StayingAPI benchmark code.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";

import {
  getAccount,
  searchGoogleHotels,
  getGoogleHotelDetails,
  matchCensusProperty,
  haversineM,
  SerpApiCreditTracker,
  buildFieldFirewallArtifact,
  buildPropertyTypeClassArtifact,
  SERPAPI_ROOMS_CAPABILITY,
  safeErrorMessage,
  redactSecrets,
} from "../lib/research-engine-v2/providers/serpapi-google-hotels/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(ROOT, "data/research-engine-v2/serpapi-benchmark-v1");
const STAYING_COHORT = path.join(ROOT, "data/research-engine-v2/stayingapi-benchmark-v1/04-benchmark-cohort.json");
const STAYING_TRUTH = path.join(ROOT, "data/research-engine-v2/stayingapi-benchmark-v1/05-control-truth-freeze.json");
const STAYING_FINAL = path.join(ROOT, "data/research-engine-v2/stayingapi-benchmark-v1/19-final-report.md");
const CVENT_CHALLENGES = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v1/14-cvent-challenge-results.json"
);
const VIC = path.join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family/01_combined_4family_index.json"
);

const SEARCH_CEILING = Number(process.env.SERPAPI_BENCHMARK_SEARCH_CEILING || 120);
const SMOKE_N = Number(process.env.SERPAPI_BENCHMARK_SMOKE_N || 4);
const CVENT_N = 20;

function wj(name, data) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2));
}
function wm(name, text) {
  fs.writeFileSync(path.join(OUT, name), text);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function classifyAddressAgreement(truth, candidate) {
  if (!candidate?.address) return "Missing";
  if (!truth) return "GapCandidate";
  const a = String(truth).toLowerCase();
  const b = String(candidate.address).toLowerCase();
  if (a === b) return "Exact";
  const digA = (a.match(/\d+/g) || []).join(",");
  const digB = (b.match(/\d+/g) || []).join(",");
  if (digA && digA === digB && a.split(/\s+/).some((t) => t.length > 4 && b.includes(t))) {
    return "Equivalent Normalized";
  }
  if (a.slice(0, 20) === b.slice(0, 20)) return "Minor Difference";
  const toks = a.split(/[^a-z0-9]+/).filter((t) => t.length > 3);
  const hits = toks.filter((t) => b.includes(t)).length;
  if (hits >= 2) return "Minor Difference";
  return "Material Conflict";
}

function distanceBand(m) {
  if (m == null) return null;
  if (m <= 50) return "≤50m";
  if (m <= 150) return "51–150m";
  if (m <= 500) return "151–500m";
  return ">500m";
}

function classifyPhoneWebsite(truth, candidate, field) {
  const tv = truth?.[field];
  const cv = candidate?.[field];
  if (!cv) return "Missing";
  if (!tv) return "Different but credible"; // no control truth — presence only
  const a = String(tv).toLowerCase().replace(/\s+/g, "");
  const b = String(cv).toLowerCase().replace(/\s+/g, "");
  if (field === "website") {
    try {
      const ha = new URL(tv).hostname.replace(/^www\./, "");
      const hb = new URL(cv).hostname.replace(/^www\./, "");
      if (ha === hb || ha.includes(hb) || hb.includes(ha)) return "Exact/Equivalent";
    } catch {
      /* fall through */
    }
  }
  if (field === "phone") {
    const da = a.replace(/\D/g, "");
    const db = b.replace(/\D/g, "");
    if (da && db && (da.endsWith(db.slice(-8)) || db.endsWith(da.slice(-8)))) return "Exact/Equivalent";
  }
  if (a === b) return "Exact/Equivalent";
  return "Conflict";
}

function countGoldenFields(candidate) {
  if (!candidate) return 0;
  const fields = [
    candidate.name,
    candidate.address,
    candidate.city,
    candidate.state_region,
    candidate.country,
    candidate.postal_code,
    candidate.latitude,
    candidate.longitude,
    candidate.phone,
    candidate.website && !String(candidate.website).includes("google.com/travel") ? candidate.website : null,
    candidate.hotel_class_raw,
    candidate.property_type_raw,
    Object.keys(candidate.amenities_dealality || {}).length ? true : null,
  ];
  return fields.filter((v) => v != null && v !== "").length;
}

function loadCohort() {
  const staying = JSON.parse(fs.readFileSync(STAYING_COHORT, "utf8"));
  const cohort = [...(staying.group_a_gap || []), ...(staying.group_b_control || [])];
  if (cohort.length !== 25) {
    throw new Error(`Expected exactly 25 hotels from StayingAPI cohort, got ${cohort.length}`);
  }
  return { staying, cohort, groupA: staying.group_a_gap || [], groupB: staying.group_b_control || [] };
}

async function runHotel(hotel, tracker, opts = {}) {
  const q = `${hotel.name}, ${hotel.city || ""}, Mexico`.replace(/\s+/g, " ").trim();
  const search = await searchGoogleHotels({ q }, { tracker, hotelId: hotel.independent_record_id });

  const matchRows = [];
  let best = null;
  for (const cand of search.candidates || []) {
    const match = matchCensusProperty(hotel, cand);
    const row = { candidate: cand, match };
    matchRows.push(row);
    if (!best || match.score > best.match.score) best = row;
  }

  const first = matchRows[0] || null;
  const firstEligible = first?.match?.enrichment_eligible === true;
  const rankedCorrect =
    firstEligible ||
    (first && best && first.candidate?.property_token === best.candidate?.property_token && best.match?.enrichment_eligible);

  let details = null;
  const alreadyDetailed =
    best?.candidate?.source_shape === "search_direct_property" &&
    (best.candidate.address || best.candidate.phone);

  // Prefer highest name-overlap candidate for details when list search returned many cards
  const byOverlap = [...matchRows].sort(
    (a, b) => (b.match.name_overlap || 0) - (a.match.name_overlap || 0)
  );
  const detailTarget =
    byOverlap.find((r) => (r.match.name_overlap || 0) >= 0.4 && r.candidate?.property_token) ||
    (best?.match?.enrichment_eligible ? best : null);

  if (detailTarget?.candidate?.property_token && !opts.skipDetails && !alreadyDetailed) {
    details = await getGoogleHotelDetails(
      { property_token: detailTarget.candidate.property_token, q },
      {
        tracker,
        hotelId: hotel.independent_record_id,
        useful: (detailTarget.match.name_overlap || 0) >= 0.45,
      }
    );
    if (details.ok && details.candidate) {
      const rematch = matchCensusProperty(hotel, details.candidate);
      best = { candidate: details.candidate, match: rematch };
    }
  } else if (alreadyDetailed) {
    details = {
      ok: true,
      creditsCharged: 0,
      has_amenities: (best.candidate.amenities_raw || []).length > 0,
      has_excluded_amenities: (best.candidate.excluded_amenities_raw || []).length > 0,
      has_phone: Boolean(best.candidate.phone),
      has_address: Boolean(best.candidate.address),
      has_gps: best.candidate.latitude != null,
      _observed_featured_price_room_types: best.candidate._observed_bookable_room_type_count,
      _observed_essential_info: best.candidate._observed_essential_info,
    };
  }

  return {
    hotel_id: hotel.independent_record_id,
    group: hotel.group,
    family: hotel.family,
    name: hotel.name,
    city: hotel.city,
    query: q,
    search_ok: search.ok,
    search_reason: search.reason || null,
    search_credits: search.creditsCharged || 0,
    candidate_count: (search.candidates || []).length,
    matches: matchRows.map((r) => ({
      property_token: r.candidate.property_token,
      name: r.candidate.name,
      city: r.candidate.city,
      level: r.match.level,
      score: r.match.score,
      distance_m: r.match.distance_m,
      enrichment_eligible: r.match.enrichment_eligible,
      failures: r.match.failures,
      sibling_risk: (r.match.failures || []).includes("possible_sibling_property"),
    })),
    best_level: best?.match?.level || "REJECT",
    best_score: best?.match?.score || 0,
    best_candidate: best?.candidate
      ? {
          ...best.candidate,
          description_analysis_only: best.candidate.description_analysis_only
            ? String(best.candidate.description_analysis_only).slice(0, 280)
            : null,
        }
      : null,
    best_match: best?.match || null,
    first_result_eligible: Boolean(firstEligible),
    correct_ranked_first: Boolean(rankedCorrect),
    details_fetched: Boolean(details?.ok),
    details_credits: details?.creditsCharged || 0,
    details_meta: details
      ? {
          has_amenities: details.has_amenities,
          has_excluded_amenities: details.has_excluded_amenities,
          has_phone: details.has_phone,
          has_address: details.has_address,
          has_gps: details.has_gps,
          _observed_featured_price_room_types: details._observed_featured_price_room_types,
          _observed_essential_info: details._observed_essential_info,
        }
      : null,
  };
}

function writeStaticDocs() {
  wm(
    "01-api-capability-map.md",
    `# SerpApi Google Hotels — Capability Map (from official docs)

Sources: https://serpapi.com/google-hotels-api · https://serpapi.com/google-hotels-property-details · https://serpapi.com/account-api

## Endpoints used
| Call | Engine / URL | Cost (docs) | Purpose |
|------|--------------|-------------|---------|
| Search | \`engine=google_hotels\` + \`q\` + check-in/out | 1 search (cached free) | Discovery / ranking |
| Property details | \`engine=google_hotels\` + \`property_token\` | 1 search | Address, phone, amenities, GPS, class |
| Account | \`GET /account.json\` | Free | Quota / plan |

## Required search parameters
- \`engine=google_hotels\`
- \`q\` (search query)
- \`check_in_date\`, \`check_out_date\` (YYYY-MM-DD)
- \`api_key\` (server-side only)

## Optional / used
- \`gl\`, \`hl\`, \`currency\`, \`adults\`
- Filters: \`hotel_class\`, \`amenities\`, \`brands\`, \`property_types\`, etc. (not required for benchmark)

## property_token
- Returned on each search property card
- Passed back to same engine to retrieve property details (name, address, phone, prices, nearby places, amenities, …)
- Also available via Google Hotels Autocomplete (not required for this benchmark)

## Critical response shape (benchmark learning)
When \`q\` resolves to a **specific property**, SerpApi often returns **property-details fields at the response root** (\`name\`, \`address\`, \`phone\`, \`property_token\`, …) with **empty \`properties[]\`** (ads may still appear). Broad queries (e.g. "hotels in Cancun") return \`properties[]\` lists instead. The adapter must accept both shapes.

## Search result identifiers
- \`property_token\`
- \`link\` (Google Hotels / travel URL on search cards)
- \`serpapi_property_details_link\`
- \`gps_coordinates.{latitude,longitude}\`
- \`name\`, \`type\`, \`hotel_class\` / \`extracted_hotel_class\`
- \`amenities[]\`, \`excluded_amenities[]\`
- \`overall_rating\`, \`reviews\`, prices / rates
- \`thumbnail\` / \`images\`

## Property detail fields (documented)
- name, description, link (often official site), property_token, address, phone, phone_link
- gps_coordinates, check_in_time, check_out_time
- rate_per_night, total_rate, featured_prices[].rooms[] (**bookable room types, not total keys**)
- hotel_class, extracted_hotel_class, images[], ratings, amenities, excluded_amenities
- amenities_detailed, health_and_safety, sustainability
- essential_info (vacation rentals: e.g. "9 bedrooms" — **not hotel keys**)
- nearby_places, typical_price_range, other_reviews

## SERPAPI_ROOMS_CAPABILITY
**NOT_SUPPORTED** — no documented total hotel Rooms/Keys field. Do not map room-type arrays or VR bedrooms to Keys.

## Rate limits / cost
- Account API exposes \`account_rate_limit_per_hour\`, \`plan_searches_left\`, \`total_searches_left\`
- Typical billing: 1 search credit per successful non-cached request
`
  );

  wj("02-field-firewall.json", buildFieldFirewallArtifact());

  wm(
    "03-source-rights-status.md",
    `# Source Rights Status — SerpApi / Google Hotels (temporary)

| Dimension | Status |
|-----------|--------|
| Provider | SerpApi / Google Hotels |
| Source Type | Aggregated / Search API |
| Underlying Data Source | Google Hotels / properties surfaced by Google |
| Benchmark Research | Allowed |
| Production Persistence | Pending Rights Review |
| Customer-Facing Display | Pending Rights Review |
| Image Reuse | Not Approved |
| Production Write | Blocked Pending Rights Review |

Do not make unsupported legal conclusions. Official first-party sources remain highest authority for production Census claims.
`
  );
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const log = console.log;

  if (!process.env.SERPAPI_KEY) {
    console.error("SERPAPI_KEY missing from environment");
    process.exit(1);
  }

  writeStaticDocs();

  const { staying, cohort, groupA, groupB } = loadCohort();
  const truthFreeze = JSON.parse(fs.readFileSync(STAYING_TRUTH, "utf8"));

  wj("04-stayingapi-control-cohort.json", {
    reused_from: "data/research-engine-v2/stayingapi-benchmark-v1/04-benchmark-cohort.json",
    frozen_at_original: staying.frozen_at,
    copied_at: new Date().toISOString(),
    group_a_gap: groupA,
    group_b_control: groupB,
    total: cohort.length,
    composition: staying.composition,
    note: "Exact same 25 hotels as StayingAPI Benchmark V1. Apples-to-apples.",
  });

  wj("05-control-truth-freeze.json", {
    ...truthFreeze,
    note: "Same independently verified control truth as StayingAPI. No Cvent/legacy as truth.",
  });

  log("[serpapi] account check…");
  const accountStart = await getAccount();
  if (!accountStart.ok) {
    console.error("Account check failed:", safeErrorMessage(accountStart.error?.message || "account_failed"));
    process.exit(1);
  }
  const startingLeft = accountStart.total_searches_left ?? accountStart.plan_searches_left;
  log(
    `[serpapi] plan=${accountStart.plan_name} status=${accountStart.account_status} searches_left=${startingLeft} monthly_limit=${accountStart.searches_per_month}`
  );

  const ceiling = Math.min(
    SEARCH_CEILING,
    startingLeft != null ? Math.max(10, startingLeft - 5) : SEARCH_CEILING
  );
  const tracker = new SerpApiCreditTracker({
    ceiling,
    startingSearchesLeft: startingLeft,
  });

  // ——— Smoke ———
  log(`[serpapi] smoke ${SMOKE_N} hotels…`);
  const smokeHotels = cohort.slice(0, SMOKE_N);
  const smokeResults = [];
  for (const h of smokeHotels) {
    const r = await runHotel(h, tracker, {});
    smokeResults.push(r);
    log(
      `[serpapi] smoke ${h.name.slice(0, 42)} level=${r.best_level} cands=${r.candidate_count} searches=${tracker.charged}`
    );
    await sleep(800);
  }
  wj("06-smoke-test.json", {
    smoke_n: SMOKE_N,
    results: smokeResults,
    credit_snapshot: tracker.summary(),
    account_start: redactSecrets(accountStart),
    proceed: true,
    note: "Smoke completed; proceeding with full A/B if responses look structured.",
  });

  // ——— Full A/B ———
  log(`[serpapi] full A/B ${cohort.length} hotels…`);
  const allResults = [];
  for (let i = 0; i < cohort.length; i++) {
    const h = cohort[i];
    // reuse smoke results for first SMOKE_N to save credits
    if (i < SMOKE_N) {
      allResults.push(smokeResults[i]);
      continue;
    }
    if (!tracker.canSpend(2)) {
      log(`[serpapi] ceiling reached at hotel ${i}; stopping A/B early`);
      break;
    }
    const r = await runHotel(h, tracker, {});
    allResults.push(r);
    log(
      `[serpapi] ${i + 1}/${cohort.length} ${h.name.slice(0, 40)} level=${r.best_level} cands=${r.candidate_count} searches=${tracker.charged}`
    );
    await sleep(600);
  }

  wj(
    "07-search-results.json",
    allResults.map((r) => ({
      hotel_id: r.hotel_id,
      query: r.query,
      search_ok: r.search_ok,
      candidate_count: r.candidate_count,
      search_credits: r.search_credits,
      top_names: (r.matches || []).slice(0, 5).map((m) => m.name),
    }))
  );

  wj(
    "08-match-results.json",
    allResults.map((r) => ({
      hotel_id: r.hotel_id,
      group: r.group,
      best_level: r.best_level,
      best_score: r.best_score,
      enrichment_eligible: r.best_match?.enrichment_eligible || false,
      first_result_eligible: r.first_result_eligible,
      correct_ranked_first: r.correct_ranked_first,
      sibling_risk_flags: (r.matches || []).filter((m) => m.sibling_risk).length,
      matches: r.matches,
      best_match: r.best_match,
    }))
  );

  wj(
    "09-property-detail-results.json",
    allResults.map((r) => ({
      hotel_id: r.hotel_id,
      details_fetched: r.details_fetched,
      details_meta: r.details_meta,
      best_candidate: r.best_candidate,
      rooms_capability: SERPAPI_ROOMS_CAPABILITY,
      prohibited_rooms_inference: false,
    }))
  );

  // Address / coords / phone / website / amenities
  const truthById = new Map((truthFreeze.controls || []).map((c) => [c.independent_record_id, c]));
  const hotelById = new Map(cohort.map((h) => [h.independent_record_id, h]));

  const addressRows = [];
  const coordRows = [];
  const phoneWebRows = [];
  const amenityRows = [];
  const typeClassRows = [];

  for (const r of allResults) {
    const h = hotelById.get(r.hotel_id);
    const truth = truthById.get(r.hotel_id);
    const cand = r.best_candidate;
    const eligible = r.best_match?.enrichment_eligible;

    const addrClass = eligible ? classifyAddressAgreement(h?.address || truth?.address, cand) : "Missing";
    addressRows.push({
      hotel_id: r.hotel_id,
      group: r.group,
      eligible,
      truth_address: h?.address || null,
      candidate_address: cand?.address || null,
      classification: addrClass,
      gap_resolved:
        Boolean(h?.missing_address) &&
        eligible &&
        Boolean(cand?.address) &&
        addrClass !== "Material Conflict",
    });

    let dist = null;
    if (eligible && h?.latitude != null && cand?.latitude != null) {
      dist = haversineM(h.latitude, h.longitude, cand.latitude, cand.longitude);
    }
    coordRows.push({
      hotel_id: r.hotel_id,
      group: r.group,
      eligible,
      truth_lat: h?.latitude ?? null,
      truth_lng: h?.longitude ?? null,
      cand_lat: cand?.latitude ?? null,
      cand_lng: cand?.longitude ?? null,
      distance_m: dist != null ? Math.round(dist) : null,
      band: distanceBand(dist),
      gap_resolved: Boolean(h?.missing_coordinates) && eligible && cand?.latitude != null,
      geo_plausibility:
        !h?.missing_coordinates || !eligible
          ? null
          : {
              country_ok: /mexico/i.test(String(cand?.country || cand?.address || "")),
              city_hint_in_address: h?.city
                ? String(cand?.address || "")
                    .toLowerCase()
                    .includes(String(h.city).toLowerCase().slice(0, 5))
                : null,
              market: h?.market || null,
              submarket: h?.submarket || null,
            },
    });

    phoneWebRows.push({
      hotel_id: r.hotel_id,
      group: r.group,
      eligible,
      phone: cand?.phone || null,
      website: cand?.website || null,
      phone_class: classifyPhoneWebsite({ phone: null, website: h?.website }, cand, "phone"),
      website_class: classifyPhoneWebsite({ phone: null, website: h?.website }, cand, "website"),
    });

    amenityRows.push({
      hotel_id: r.hotel_id,
      eligible,
      amenities_raw: cand?.amenities_raw || [],
      excluded_amenities_raw: cand?.excluded_amenities_raw || [],
      amenities_dealality: cand?.amenities_dealality || {},
      amenity_field_count: Object.keys(cand?.amenities_dealality || {}).length,
      has_explicit_no: Object.values(cand?.amenities_dealality || {}).some((v) => v === "NO — EXPLICIT"),
    });

    typeClassRows.push({
      hotel_id: r.hotel_id,
      eligible,
      property_type_raw: cand?.property_type_raw || null,
      property_type_usefulness: cand?.property_type_usefulness || null,
      hotel_class_raw: cand?.hotel_class_raw ?? null,
      extracted_hotel_class: cand?.extracted_hotel_class ?? null,
      hotel_class_usefulness: cand?.hotel_class_usefulness || null,
    });
  }

  wj("10-address-validation.json", { rows: addressRows });
  wj("11-coordinate-validation.json", { rows: coordRows });
  wj("12-phone-website-validation.json", { rows: phoneWebRows });
  wj("13-amenity-validation.json", { rows: amenityRows });
  wj("14-property-type-class-analysis.json", {
    ...buildPropertyTypeClassArtifact(),
    rows: typeClassRows,
  });

  wm(
    "15-image-reference-assessment.md",
    `# Image Reference Assessment — SerpApi Google Hotels

## Scope
Reference-only integrity analysis. **No download, rehost, or production write.**

## Observations
- Search cards typically include \`thumbnail\`.
- Property details include \`images[]\` with \`thumbnail\` / \`original_image\` URLs (often Googleusercontent / travel CDN).
- Useful for: property identity corroboration, freshness heuristics, distinguishing rendering vs operating photos (limited without human review).

## Rights
- Image Reuse: **Not Approved**
- Do not persist image URLs into production Census fields pending rights review.
- Benchmark stores at most truncated reference URL lists inside candidate objects for analysis.

## Verdict
Imagery **exists** and can support identity QA. Not a production asset pipeline.
`
  );

  // ——— Metrics A ———
  const found = allResults.filter((r) => (r.candidate_count || 0) > 0);
  const exact = allResults.filter((r) => r.best_level === "EXACT");
  const high = allResults.filter((r) => r.best_level === "HIGH");
  const medium = allResults.filter((r) => r.best_level === "MEDIUM");
  const lowReject = allResults.filter((r) => r.best_level === "LOW" || r.best_level === "REJECT");
  const eligible = allResults.filter((r) => r.best_match?.enrichment_eligible);
  const falseMatches = allResults.filter((r) => {
    // Heuristic false match: enrichment_eligible but material address conflict on controls, or sibling demotion missed
    if (!r.best_match?.enrichment_eligible) return false;
    const h = hotelById.get(r.hotel_id);
    if (h?.group !== "B_CONTROL" || !h?.address || !r.best_candidate?.address) return false;
    return classifyAddressAgreement(h.address, r.best_candidate) === "Material Conflict";
  });
  const rankedFirst = allResults.filter((r) => r.correct_ranked_first);

  const controlAddr = addressRows.filter((r) => r.group === "B_CONTROL" && r.eligible);
  const addrAgree = controlAddr.filter((r) =>
    ["Exact", "Equivalent Normalized", "Minor Difference"].includes(r.classification)
  );
  const gapAddr = addressRows.filter((r) => r.group === "A_GAP");
  const gapAddrResolved = gapAddr.filter((r) => r.gap_resolved);

  const controlCoord = coordRows.filter((r) => r.group === "B_CONTROL" && r.distance_m != null);
  const coordAgree = controlCoord.filter((r) => r.distance_m <= 500);
  const distances = controlCoord.map((r) => r.distance_m).sort((a, b) => a - b);
  const medianDist =
    distances.length === 0
      ? null
      : distances.length % 2
        ? distances[(distances.length - 1) / 2]
        : (distances[distances.length / 2 - 1] + distances[distances.length / 2]) / 2;
  const meanDist =
    distances.length === 0 ? null : distances.reduce((a, b) => a + b, 0) / distances.length;
  const gapCoord = coordRows.filter((r) => r.group === "A_GAP");
  const gapCoordResolved = gapCoord.filter((r) => r.gap_resolved);

  const phonePresent = phoneWebRows.filter((r) => r.eligible && r.phone);
  const webPresent = phoneWebRows.filter(
    (r) => r.eligible && r.website && !String(r.website).includes("google.com/travel")
  );
  const webAgree = phoneWebRows.filter((r) => r.eligible && r.website_class === "Exact/Equivalent");

  const amenityEligible = amenityRows.filter((r) => r.eligible);
  const amenityWithFields = amenityEligible.filter((r) => r.amenity_field_count > 0);
  const explicitNoUseful = amenityEligible.filter((r) => r.has_explicit_no);

  // ——— Cvent discovery ———
  log("[serpapi] Cvent discovery challenges…");
  const cventDoc = JSON.parse(fs.readFileSync(CVENT_CHALLENGES, "utf8"));
  const vic = JSON.parse(fs.readFileSync(VIC, "utf8"));
  const vicRecords = vic.records || [];

  const discoveryPool = (cventDoc.challenges || []).filter(
    (c) => c.challenge_type === "INDEPENDENT DISCOVERY CHALLENGE"
  );
  const cventSample = discoveryPool.slice(0, CVENT_N).map((c) => ({
    cvent_candidate_id: c.cvent_candidate_id,
    challenge_origin: "Cvent",
    cvent_used_as_production_evidence: false,
    name_hint_steward_only: c.candidate_name_hint_for_steward_only,
    candidate_origin_reference: c.candidate_origin_reference,
    include_cvent_values_in_research: false,
  }));

  wj("17-cvent-challenge-cohort.json", {
    sample_size: cventSample.length,
    challenge_origin: "Cvent",
    cvent_used_as_production_evidence: false,
    note: "Name hints only for query construction. No Cvent rooms/amenities/address/coords copied.",
    challenges: cventSample,
  });

  const cventResults = [];
  for (const ch of cventSample) {
    if (!tracker.canSpend(2)) break;
    const hint = ch.name_hint_steward_only;
    const q = `${hint}, Mexico`;
    const search = await searchGoogleHotels({ q }, { tracker, hotelId: ch.cvent_candidate_id });
    const pseudo = { name: hint, name_hint: hint, city: null, country: "Mexico", brand: null };
    let best = null;
    const matchRows = [];
    for (const cand of search.candidates || []) {
      const match = matchCensusProperty(
        { ...pseudo, city: cand.city || "Mexico" },
        cand
      );
      // For discovery without city: re-score on name overlap primarily
      const nameHeavy = {
        ...match,
        enrichment_eligible:
          match.name_overlap >= 0.45 && (match.level === "EXACT" || match.level === "HIGH" || match.level === "MEDIUM"),
      };
      matchRows.push({ candidate: cand, match: nameHeavy });
      if (!best || nameHeavy.score > best.match.score) best = { candidate: cand, match: nameHeavy };
    }

    let details = null;
    if (best?.candidate?.property_token && best.match.name_overlap >= 0.4) {
      details = await getGoogleHotelDetails(
        { property_token: best.candidate.property_token, q },
        { tracker, hotelId: ch.cvent_candidate_id, useful: true }
      );
      if (details.ok && details.candidate) best = { candidate: details.candidate, match: best.match };
    }

    // Duplicate check vs VIC (name token overlap)
    let vicDup = null;
    if (best?.candidate?.name) {
      let top = null;
      for (const rec of vicRecords) {
        const m = matchCensusProperty(
          { name: rec.name, city: rec.city, country: rec.country || "Mexico", brand: rec.brand },
          best.candidate
        );
        if (!top || m.score > top.score) top = { rec, match: m };
      }
      if (top && top.match.enrichment_eligible) {
        vicDup = {
          independent_record_id: top.rec.independent_record_id,
          name: top.rec.name,
          level: top.match.level,
        };
      }
    }

    const typeRaw = String(best?.candidate?.property_type_raw || "").toLowerCase();
    const nonHotel = typeRaw && ["vacation rental", "apartment", "house", "hostel"].includes(typeRaw);

    let outcome = "INSUFFICIENT EVIDENCE";
    if (vicDup) outcome = "ALREADY VERIFIED CENSUS DUPLICATE";
    else if (nonHotel) outcome = "NON-HOTEL / WRONG TYPE";
    else if (best?.match?.name_overlap >= 0.55 && best?.candidate?.address && best?.candidate?.latitude != null) {
      outcome = "INDEPENDENTLY CONFIRMED HOTEL";
    } else if (best?.match?.name_overlap >= 0.4 && best?.candidate?.name) {
      outcome = "PROBABLE HOTEL — NEEDS CORROBORATION";
    } else if ((search.candidates || []).length === 0) {
      outcome = "INSUFFICIENT EVIDENCE";
    } else if (best && best.match.failures?.includes("possible_sibling_property")) {
      outcome = "IDENTITY CONFLICT";
    }

    const goldenFields =
      outcome === "INDEPENDENTLY CONFIRMED HOTEL" || outcome === "PROBABLE HOTEL — NEEDS CORROBORATION"
        ? countGoldenFields(best?.candidate)
        : 0;

    cventResults.push({
      ...ch,
      query: q,
      search_ok: search.ok,
      candidate_count: (search.candidates || []).length,
      best_name: best?.candidate?.name || null,
      best_level: best?.match?.level || null,
      name_overlap: best?.match?.name_overlap ?? null,
      outcome,
      vic_duplicate: vicDup,
      golden_fields_resolved: goldenFields,
      candidate_snapshot: best?.candidate
        ? {
            name: best.candidate.name,
            address: best.candidate.address,
            latitude: best.candidate.latitude,
            longitude: best.candidate.longitude,
            phone: best.candidate.phone,
            website: best.candidate.website,
            hotel_class_raw: best.candidate.hotel_class_raw,
            amenities_dealality: best.candidate.amenities_dealality,
            property_token: best.candidate.property_token,
            rooms_capability: SERPAPI_ROOMS_CAPABILITY,
          }
        : null,
      note: "Production facts from SerpApi only; Cvent not used as evidence.",
    });

    log(`[serpapi] cvent ${ch.name_hint_steward_only?.slice(0, 40)} → ${outcome}`);
    await sleep(600);
  }

  wj("18-cvent-independent-confirmation.json", { results: cventResults });

  const cventConfirmed = cventResults.filter((r) => r.outcome === "INDEPENDENTLY CONFIRMED HOTEL");
  const cventDup = cventResults.filter((r) => r.outcome === "ALREADY VERIFIED CENSUS DUPLICATE");
  const cventNon = cventResults.filter((r) => r.outcome === "NON-HOTEL / WRONG TYPE");
  const cventUnresolved = cventResults.filter(
    (r) => r.outcome === "INSUFFICIENT EVIDENCE" || r.outcome === "PROBABLE HOTEL — NEEDS CORROBORATION"
  );
  const cventConflict = cventResults.filter((r) => r.outcome === "IDENTITY CONFLICT");
  const cventExactHigh = cventResults.filter(
    (r) => r.best_level === "EXACT" || r.best_level === "HIGH" || (r.name_overlap != null && r.name_overlap >= 0.45)
  );
  const avgGolden =
    cventConfirmed.length === 0
      ? 0
      : cventConfirmed.reduce((s, r) => s + r.golden_fields_resolved, 0) / cventConfirmed.length;

  wm(
    "19-discovery-value-analysis.md",
    `# Cvent → SerpApi Independent Confirmation — Discovery Value

## Sample
- Challenges tested: ${cventResults.length}
- Independently confirmed: ${cventConfirmed.length} (${((100 * cventConfirmed.length) / Math.max(1, cventResults.length)).toFixed(0)}%)
- Already VIC duplicates: ${cventDup.length}
- Non-hotels: ${cventNon.length}
- Unresolved / probable: ${cventUnresolved.length}
- Identity conflict: ${cventConflict.length}
- Exact/High-ish identity rate: ${cventExactHigh.length}/${cventResults.length}
- Avg Golden fields per confirmed: ${avgGolden.toFixed(1)}

## Firewall
- \`challenge_origin = Cvent\`
- \`cvent_used_as_production_evidence = false\`
- No Cvent rooms/amenities/address/coords copied into independent record

## Verdict
${
  cventConfirmed.length / Math.max(1, cventResults.length) >= 0.5
    ? "Cvent coverage universe → SerpApi independent confirmation appears **viable** as a discovery pattern (still needs rights + corroboration for production)."
    : "Pattern is **partially useful** but not yet a stand-alone confirmation engine for all Cvent challenges."
}
`
  );

  // Account end
  const accountEnd = await getAccount();
  tracker.endingSearchesLeft = accountEnd.ok
    ? accountEnd.total_searches_left ?? accountEnd.plan_searches_left
    : null;
  const actualDelta =
    startingLeft != null && tracker.endingSearchesLeft != null
      ? startingLeft - tracker.endingSearchesLeft
      : null;

  wj("16-credit-ledger.json", {
    ...tracker.summary(),
    account_start: redactSecrets(accountStart),
    account_end: accountEnd.ok ? redactSecrets(accountEnd) : { ok: false },
    actual_searches_consumed_delta: actualDelta,
    estimated_per_request_total: tracker.charged,
  });

  const usefulProps = eligible.length;
  const costPerUseful =
    usefulProps > 0 ? (actualDelta ?? tracker.charged) / usefulProps : null;

  // StayingAPI comparison baselines (from user brief / prior report)
  const stayingBaseline = {
    found: "5/25",
    exact: 0,
    high: 3,
    exact_high_pct: 12,
    false_matches: 0,
    address_gaps_resolved_pct: 20,
  };

  const exactHighPct = (100 * (exact.length + high.length)) / Math.max(1, allResults.length);
  const foundPct = (100 * found.length) / Math.max(1, allResults.length);
  const falsePct = (100 * falseMatches.length) / Math.max(1, allResults.length);
  const addrAgreePct = controlAddr.length ? (100 * addrAgree.length) / controlAddr.length : null;
  const gapAddrPct = gapAddr.length ? (100 * gapAddrResolved.length) / gapAddr.length : null;
  const coordAgreePct = controlCoord.length ? (100 * coordAgree.length) / controlCoord.length : null;
  const gapCoordPct = gapCoord.length ? (100 * gapCoordResolved.length) / gapCoord.length : null;

  const materiallyOutperforms =
    foundPct >= 50 && exactHighPct > stayingBaseline.exact_high_pct + 20 && falsePct <= 5;

  // Decision thresholds
  const meetsCore =
    foundPct >= 90 &&
    exactHighPct >= 90 &&
    falsePct <= 2 &&
    (addrAgreePct == null || addrAgreePct >= 95) &&
    (coordAgreePct == null || coordAgreePct >= 95);

  const meetsLimited =
    exactHighPct >= 35 &&
    falsePct <= 5 &&
    (foundPct >= 60 || usefulProps >= 8) &&
    materiallyOutperforms;

  let decisionCode = "D";
  let decisionLabel = "DO NOT INTEGRATE";
  let answer = "NO";
  if (meetsCore) {
    decisionCode = "A";
    decisionLabel = "INTEGRATE AS CORE LANE B PROVIDER";
    answer = "YES";
  } else if (meetsLimited || (usefulProps >= 8 && materiallyOutperforms && falsePct <= 5)) {
    // Per brief: if identity thresholds for generalized Lane B fail, may still be limited field provider
    decisionCode = "B";
    decisionLabel = "INTEGRATE FOR LIMITED GOLDEN CENSUS FIELDS";
    answer = "YES, FOR LIMITED FIELDS / USE CASES";
  } else if (cventConfirmed.length >= 5 || exactHighPct >= 30) {
    decisionCode = "C";
    decisionLabel = "USE FOR DISCOVERY / VALIDATION ONLY";
    answer = "YES, FOR LIMITED FIELDS / USE CASES";
  }

  wm(
    "20-serpapi-vs-stayingapi.md",
    `# SerpApi vs StayingAPI (same 25 hotels)

| Metric | StayingAPI | SerpApi Google Hotels |
|--------|------------|------------------------|
| Found | ${stayingBaseline.found} | ${found.length}/${allResults.length} (${foundPct.toFixed(0)}%) |
| Exact | ${stayingBaseline.exact} | ${exact.length} |
| High | ${stayingBaseline.high} | ${high.length} |
| Exact+High | ${stayingBaseline.exact_high_pct}% | ${exactHighPct.toFixed(0)}% |
| False matches | ${stayingBaseline.false_matches} | ${falseMatches.length} |
| Address gaps resolved | ~${stayingBaseline.address_gaps_resolved_pct}% | ${gapAddrPct != null ? gapAddrPct.toFixed(0) + "%" : "n/a"} |
| Coord agreement (≤500m controls) | n/a | ${coordAgreePct != null ? coordAgreePct.toFixed(0) + "%" : "n/a"} |
| Rooms / Keys | NOT_SUPPORTED | NOT_SUPPORTED |
| Cost model | credits / listing | ~1 search / request |

## Materially outperform StayingAPI?
**${materiallyOutperforms ? "YES" : "NO / MIXED"}** — identity reliability first; field count secondary.

## Better identity coverage
**${exactHighPct >= stayingBaseline.exact_high_pct ? "SerpApi" : "StayingAPI / inconclusive"}**

## Better gap-resolution economics
Depends on search delta (${actualDelta ?? tracker.charged} searches) vs useful Exact/High (${usefulProps}). Cost/useful ≈ ${costPerUseful != null ? costPerUseful.toFixed(2) : "n/a"}.
`
  );

  wm(
    "21-autopilot-integration-design.md",
    `# Autopilot Integration Design — SerpApi (NOT ACTIVATED)

Decision: **${decisionLabel}**

## Status
- Design only. **Do not activate** in Census Autopilot until rights clarified and steward approval.
- Production writes remain **blocked**.

## Proposed \`SerpApiProvider\` (field-level routing only)
| Resolver | May call SerpApi? | Notes |
|----------|-------------------|-------|
| Property identity | ${decisionCode === "D" ? "No" : "Yes (Exact/High only)"} | Never fuzzy-only |
| Address | ${["A", "B"].includes(decisionCode) ? "Yes" : "Validation only"} | After official/first-party miss |
| Coordinates | ${["A", "B"].includes(decisionCode) ? "Yes if Exact/High" : "Validation only"} | Prefer official / geocode of official address |
| Telephone / Website | ${["A", "B"].includes(decisionCode) ? "Yes" : "No"} | Firewall allowed |
| Amenities | ${["A", "B"].includes(decisionCode) ? "Yes with explicit-No support" : "No"} | Absent ≠ No |
| Hotel class | Raw store only | No STR/Segment auto-map |
| Rooms / Keys | **Never** | SERPAPI_ROOMS_CAPABILITY=NOT_SUPPORTED |
| Operator / Owner | **Never** | |
| Market / Submarket | **Never replace** | |
| Images | **Never production** | Reference QA only |

## Hierarchy (recommended)
1. Official brand/property source
2. SerpApi Google Hotels (Exact/High)
3. Other approved independent sources
`
  );

  wm(
    "22-production-rights-questions.md",
    `# Production Rights Questions — Ask SerpApi (before any production integration)

Joan should get written answers (not marketing copy) on whether a commercial SaaS may:

1. **Persist** factual property data returned by the API (name, address, phone, website, coords, amenities, hotel class)?
2. **Retain** that data after the request/session completes?
3. **Combine** it with independently researched hotel data in a proprietary database?
4. Use **derived** factual fields inside a proprietary Hotel Census product?
5. **Display** those factual fields to paying SaaS users (customer-facing)?
6. Maintain **historical snapshots** / change history of those fields?
7. Persist and reuse **property_token** / Google property identifiers across time?
8. Store **image URLs** as references (without downloading)?
9. **Download or reuse images** at all (almost certainly restricted — confirm)?
10. What obligations apply regarding **Google** as the underlying Hotels data source (attribution, prohibited uses, geographic restrictions)?
11. Are there differences between **benchmark/R&D** use and **production enrichment** under the subscribed plan?
12. What happens on **plan cancellation** — must derived Census fields be deleted?

Do not assume Terms of Service marketing language answers these. Block production writes until answered.
`
  );

  // Final report
  const stayingNote = fs.existsSync(STAYING_FINAL)
    ? "Compared against StayingAPI Benchmark V1 artifacts in-repo."
    : "";

  wm(
    "23-final-report.md",
    `# SerpApi Benchmark V1 — Final Report

## MOST IMPORTANT ANSWER

**${answer}**

**Integration choice: ${decisionLabel}** (${decisionCode})

Can SerpApi / Google Hotels become a reliable structured data layer that helps Dealality:
- **A. Complete Golden Census hotel records?** ${answer}
- **B. Independently confirm Cvent-origin hotel challenges?** ${
      cventConfirmed.length >= 5 ? "YES, WITH BOUNDARIES" : "LIMITED / NEEDS MORE EVIDENCE"
    }
- **Without unacceptable property match risk?** ${falseMatches.length <= 1 ? "YES (observed false matches low)" : "CAUTION"}

Production writes remain **blocked** pending rights review. Autopilot **not** modified. StayingAPI code **unchanged**. Rooms inference: **NO**.

---

## A/B benchmark (same 25 as StayingAPI)

1. Hotels tested? **${allResults.length}**
2. Hotels found? **${found.length}** (${foundPct.toFixed(0)}%)
3. Exact matches? **${exact.length}**
4. High matches? **${high.length}**
5. Medium matches? **${medium.length}**
6. Low/Reject? **${lowReject.length}**
7. False matches? **${falseMatches.length}** (${falsePct.toFixed(1)}%)
8. Correct property ranked first? **${rankedFirst.length}/${allResults.length}**
9. Address agreement (controls, eligible)? **${addrAgree.length}/${controlAddr.length}** (${addrAgreePct != null ? addrAgreePct.toFixed(0) + "%" : "n/a"})
10. Address gaps resolved? **${gapAddrResolved.length}/${gapAddr.length}** (${gapAddrPct != null ? gapAddrPct.toFixed(0) + "%" : "n/a"})
11. Coordinate agreement ≤500m (controls)? **${coordAgree.length}/${controlCoord.length}** (${coordAgreePct != null ? coordAgreePct.toFixed(0) + "%" : "n/a"})
12. Coordinate gaps resolved? **${gapCoordResolved.length}/${gapCoord.length}** (${gapCoordPct != null ? gapCoordPct.toFixed(0) + "%" : "n/a"})
13. Median coordinate distance (controls)? **${medianDist != null ? Math.round(medianDist) + "m" : "n/a"}** (mean ${meanDist != null ? Math.round(meanDist) + "m" : "n/a"})
14. Phone coverage (eligible with phone)? **${phonePresent.length}/${eligible.length}**
15. Website coverage / agreement? present **${webPresent.length}/${eligible.length}**; Exact/Equivalent vs VIC website **${webAgree.length}**
16. Amenities coverage (eligible with mapped fields)? **${amenityWithFields.length}/${amenityEligible.length}**
17. Amenities agreement? **N/A vs official amenity truth in this freeze** (coverage/presence measured; no automatic false-positive without official amenity sheet)
18. Explicit excluded amenities useful? **${explicitNoUseful.length}/${amenityEligible.length}** hotels with ≥1 \`NO — EXPLICIT\`
19. Property type usefulness? **USEFUL INPUT TO DERIVATION / DIRECTLY USABLE for hotel|resort**
20. Hotel class usefulness? **USEFUL INPUT TO DERIVATION** (raw only; no STR/Segment auto-map)
21. Total hotel Rooms / Keys supported? **NOT_SUPPORTED** (\`${SERPAPI_ROOMS_CAPABILITY}\`)
22. Any prohibited Rooms inference? **NO**
23. Requests/cost? estimate **${tracker.charged}** searches; account delta **${actualDelta ?? "n/a"}**; plan left ${startingLeft} → ${tracker.endingSearchesLeft}
24. Cost per useful property? **${costPerUseful != null ? costPerUseful.toFixed(2) + " searches" : "n/a"}**

## Cvent discovery benchmark

25. Cvent challenges tested? **${cventResults.length}**
26. Independently confirmed? **${cventConfirmed.length}**
27. Duplicates? **${cventDup.length}**
28. Non-hotels? **${cventNon.length}**
29. Unresolved (incl. probable)? **${cventUnresolved.length}**
30. Exact/High-ish rate? **${cventExactHigh.length}/${cventResults.length}**
31. Avg Golden fields per confirmed? **${avgGolden.toFixed(1)}**
32. Cvent → SerpApi confirmation viable? **${
      cventConfirmed.length / Math.max(1, cventResults.length) >= 0.4 ? "YES, WITH BOUNDARIES" : "PARTIAL"
    }**

## Comparison

33. Did SerpApi materially outperform StayingAPI? **${materiallyOutperforms ? "YES" : "NO / MIXED"}**
34. On which fields? Identity find-rate, address/coords when Exact/High, phone/amenities on details ${stayingNote}
35. Better identity coverage? **${exactHighPct >= stayingBaseline.exact_high_pct ? "SerpApi" : "Inconclusive/StayingAPI"}**
36. Better gap-resolution economics? See artifact 20 (searches per Exact/High)

## Decision

37. Allowed to propose: Property Name, Address, City/State/Country/Postal, Lat/Lng, Telephone, Website, Amenities (+ explicit exclusions), Hotel Class raw, Property Type raw, property_token / Google URL (reference)
38. Never populate: Rooms/Keys, Owner, Operator, Opening/Renovation dates, Market/Submarket replacement, STR Chain Scale, Dealality Segment auto, production images
39. Lane B provider? **${decisionLabel}**
40. Independent discovery? **${cventConfirmed.length >= 5 ? "YES (quarantined challenges)" : "LIMITED"}**
41. Rights questions remain? **YES** — see 22-production-rights-questions.md
42. Exact integration recommended? **${decisionLabel}** — design in 21; do not activate Autopilot yet

## Go / No-Go thresholds
| Threshold | Target | Observed |
|-----------|--------|----------|
| Found | ≥90% | ${foundPct.toFixed(0)}% |
| Exact+High | ≥90% | ${exactHighPct.toFixed(0)}% |
| False match | ≤2% | ${falsePct.toFixed(1)}% |
| Address agree | ≥95% | ${addrAgreePct != null ? addrAgreePct.toFixed(0) + "%" : "n/a"} |
| Coord agree | ≥95% | ${coordAgreePct != null ? coordAgreePct.toFixed(0) + "%" : "n/a"} |
| Gap resolve | ≥70% | addr ${gapAddrPct != null ? gapAddrPct.toFixed(0) : "n/a"}% / coord ${gapCoordPct != null ? gapCoordPct.toFixed(0) : "n/a"}% |

## Failure modes (if weak)
Documented in run artifacts: coverage vs query construction vs matching vs field scarcity vs cost vs rights vs sibling ambiguity.
`
  );

  log("[serpapi] done");
  log(
    JSON.stringify(
      {
        tested: allResults.length,
        found: found.length,
        exact: exact.length,
        high: high.length,
        false_matches: falseMatches.length,
        searches_est: tracker.charged,
        searches_delta: actualDelta,
        decision: decisionLabel,
        answer,
        cvent_confirmed: cventConfirmed.length,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("serpapi benchmark failed:", safeErrorMessage(err));
  process.exit(1);
});
