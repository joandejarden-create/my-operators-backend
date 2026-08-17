#!/usr/bin/env node
/**
 * StayingAPI Benchmark V1 — Golden Census Lane B evaluation
 *
 * npm run stayingapi:benchmark-v1
 *
 * Never prints STAYINGAPI_KEY. No Airtable. No Webhound. No Autopilot integration.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";

import {
  getAccount,
  searchProperties,
  getListing,
  matchCensusProperty,
  haversineM,
  StayingCreditTracker,
  buildFieldFirewallArtifact,
  buildPropertyTypeMappingArtifact,
  STAYINGAPI_ROOMS_CAPABILITY,
  AMENITY_MAP,
  safeErrorMessage,
} from "../lib/research-engine-v2/providers/staying-api/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(ROOT, "data/research-engine-v2/stayingapi-benchmark-v1");
const VIC = path.join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family/01_combined_4family_index.json"
);
const V12 = path.join(ROOT, "data/research-engine-v2/census-autopilot-v1-2-golden-95/21-per-hotel-completeness.json");
const V13 = path.join(ROOT, "data/research-engine-v2/census-autopilot-v1-3-gap-closure/17-final-hotel-completeness.json");

const CREDIT_CEILING = Number(process.env.STAYINGAPI_BENCHMARK_CREDIT_CEILING || 120);
const SMOKE_N = 3;

function wj(name, data) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2));
}
function wm(name, text) {
  fs.writeFileSync(path.join(OUT, name), text);
}

function isRealValue(v) {
  if (v == null) return false;
  const s = String(v);
  if (!s.trim()) return false;
  if (s.startsWith("v12_supported:")) return false;
  return true;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function buildCohort(vicRecords, v12Hotels, v13Hotels) {
  const v12 = new Map(v12Hotels.map((h) => [h.independent_record_id, h]));
  const v13 = new Map((v13Hotels || []).map((h) => [h.independent_record_id, h]));

  const enriched = vicRecords
    .filter((r) => r.country === "Mexico" && ["IHG", "Hilton", "Choice"].includes(r.family))
    .map((r) => {
      const a = v12.get(r.independent_record_id) || {};
      const b = v13.get(r.independent_record_id) || {};
      const address = isRealValue(b.address) ? b.address : null;
      const latitude = isRealValue(b.latitude) ? Number(b.latitude) : null;
      const longitude = isRealValue(b.longitude) ? Number(b.longitude) : null;
      const missingAddr = !address;
      const missingCoord = latitude == null || longitude == null;
      const isGap = missingAddr || missingCoord;
      const isControl = !missingAddr && !missingCoord;
      return {
        independent_record_id: r.independent_record_id,
        name: r.name,
        brand: r.brand,
        family: r.family,
        city: r.city,
        country: r.country,
        website: r.website,
        property_ids: r.property_ids,
        market: a.market || b.market || null,
        submarket: a.submarket || b.submarket || null,
        address,
        latitude,
        longitude,
        v12_unknown: a.unknown_fields || [],
        group: isGap ? "A_GAP" : isControl ? "B_CONTROL" : "A_GAP",
        missing_address: missingAddr,
        missing_coordinates: missingCoord,
      };
    });

  // Prefer diverse markets/families
  const gaps = enriched.filter((h) => h.group === "A_GAP");
  const controls = enriched.filter((h) => h.group === "B_CONTROL");

  function pickDiverse(pool, n) {
    const out = [];
    const usedCities = new Set();
    const famCount = { IHG: 0, Hilton: 0, Choice: 0 };
    // first pass: diversity
    for (const h of pool) {
      if (out.length >= n) break;
      if (famCount[h.family] >= Math.ceil(n / 2)) continue;
      if (usedCities.has(h.city) && out.length < n - 2) continue;
      out.push(h);
      usedCities.add(h.city);
      famCount[h.family] += 1;
    }
    for (const h of pool) {
      if (out.length >= n) break;
      if (out.some((x) => x.independent_record_id === h.independent_record_id)) continue;
      out.push(h);
    }
    return out.slice(0, n);
  }

  const groupA = pickDiverse(gaps, 15);
  const groupB = pickDiverse(controls, 10);
  return { groupA, groupB, cohort: [...groupA, ...groupB] };
}

function classifyAddressAgreement(truth, candidate) {
  if (!candidate?.address) return "Missing";
  if (!truth) return "GapCandidate";
  const a = String(truth).toLowerCase();
  const b = String(candidate.address).toLowerCase();
  if (a === b) return "Exact";
  // normalized equivalence: digit + street token overlap
  const digA = (a.match(/\d+/g) || []).join(",");
  const digB = (b.match(/\d+/g) || []).join(",");
  if (digA && digA === digB && a.split(/\s+/).some((t) => t.length > 4 && b.includes(t))) {
    return "Equivalent Normalized";
  }
  if (a.slice(0, 20) === b.slice(0, 20)) return "Minor Difference";
  // shared significant tokens
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

async function runHotel(hotel, tracker, opts = {}) {
  const location = `${hotel.name}, ${hotel.city}, Mexico`;
  const platforms = opts.platforms || ["booking"];
  const search = await searchProperties(
    { location, platforms, limit: opts.limit || 5 },
    { tracker, hotelId: hotel.independent_record_id }
  );

  const matchRows = [];
  let best = null;
  for (const cand of search.candidates || []) {
    const match = matchCensusProperty(hotel, cand);
    const row = { candidate: cand, match };
    matchRows.push(row);
    if (!best || match.score > best.match.score) best = row;
  }

  // Rank: was correct property first?
  const first = matchRows[0] || null;
  const firstEligible = first?.match?.enrichment_eligible === true;

  let listing = null;
  if (best?.match?.enrichment_eligible && best.candidate?.platform && best.candidate?.platform_listing_id) {
    if (!opts.skipListing) {
      listing = await getListing(
        best.candidate.platform,
        best.candidate.platform_listing_id,
        {},
        {
          tracker,
          hotelId: hotel.independent_record_id,
          useful: true,
        }
      );
      if (listing.ok && listing.candidate) {
        // re-match listing candidate
        const rematch = matchCensusProperty(hotel, listing.candidate);
        best = { candidate: listing.candidate, match: rematch };
      }
    }
  }

  return {
    hotel_id: hotel.independent_record_id,
    group: hotel.group,
    family: hotel.family,
    search_ok: search.ok,
    search_reason: search.reason || null,
    search_credits: search.creditsCharged || 0,
    candidate_count: (search.candidates || []).length,
    matches: matchRows.map((r) => ({
      staying_id: r.candidate.staying_id,
      platform: r.candidate.platform,
      name: r.candidate.name,
      city: r.candidate.city,
      level: r.match.level,
      score: r.match.score,
      distance_m: r.match.distance_m,
      enrichment_eligible: r.match.enrichment_eligible,
      failures: r.match.failures,
    })),
    best_level: best?.match?.level || "REJECT",
    best_score: best?.match?.score || 0,
    best_candidate: best?.candidate || null,
    best_match: best?.match || null,
    first_result_eligible: firstEligible,
    listing_fetched: Boolean(listing?.ok),
    listing_credits: listing?.creditsCharged || 0,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const log = console.log;

  if (!process.env.STAYINGAPI_KEY) {
    console.error("STAYINGAPI_KEY missing from environment");
    process.exit(1);
  }

  // Capability map (pre-call)
  wm(
    "01-api-capability-map.md",
    `# StayingAPI Capability Map (from OpenAPI + docs)

## Base
- REST: \`https://api.stayingapi.com/v1\`
- Auth: Bearer \`STAYINGAPI_KEY\` (server-side only)
- Envelope: \`{ data, meta }\`; live may return 202 + jobId

## Endpoints used in benchmark
| Endpoint | Cost (docs) | Notes |
|----------|-------------|-------|
| GET /account | 0 | plan + credits |
| GET /search | 2+ (Airbnb 2/result; others 1/result; min 5/platform) | discovery |
| GET /listing/{platform}/{id} | 3 | full detail |
| GET /jobs/{id} | 0 | async poll |

## Property fields (unified schema)
id, platform, platformListingId, url, name, propertyType,
location { lat, lng, city, region, country, address },
starRating, guestRating, maxOccupancy, bedrooms, bathrooms,
amenities[], images[], host, price?, identity?

## STAYINGAPI_ROOMS_CAPABILITY
**NOT_SUPPORTED** — no total hotel room/key count. \`bedrooms\` / \`maxOccupancy\` must never map to Rooms / Keys.

## Platforms
airbnb | booking | vrbo | google
`
  );

  wj("02-field-firewall.json", buildFieldFirewallArtifact());
  wm(
    "03-source-rights-status.md",
    `# Source Rights Status — StayingAPI (temporary)

| Dimension | Status |
|-----------|--------|
| Provider | StayingAPI |
| Classification | Candidate Structured Enrichment Provider |
| Production Data Persistence | PENDING CONFIRMATION |
| Customer-Facing Derived Use | PENDING CONFIRMATION |
| Image Reuse | NOT APPROVED |
| Benchmark Use | APPROVED FOR TECHNICAL EVALUATION |
| Production Write | BLOCKED PENDING RIGHTS CONFIRMATION |

Do not assume OTA-derived data may be stored in Airtable or shown customer-facing without rights review.
`
  );

  const vic = JSON.parse(fs.readFileSync(VIC, "utf8"));
  const v12 = JSON.parse(fs.readFileSync(V12, "utf8"));
  const v13 = JSON.parse(fs.readFileSync(V13, "utf8"));
  const { groupA, groupB, cohort } = buildCohort(vic.records || [], v12.hotels || [], v13.hotels || []);

  wj("04-benchmark-cohort.json", {
    frozen_at: new Date().toISOString(),
    group_a_gap: groupA,
    group_b_control: groupB,
    total: cohort.length,
    composition: {
      IHG: cohort.filter((h) => h.family === "IHG").length,
      Hilton: cohort.filter((h) => h.family === "Hilton").length,
      Choice: cohort.filter((h) => h.family === "Choice").length,
      gap: groupA.length,
      control: groupB.length,
    },
    note: "Truth from VIC + V1.3 Golden gap-closure official evidence. No Cvent/legacy.",
  });

  wj("05-control-truth-freeze.json", {
    controls: groupB.map((h) => ({
      independent_record_id: h.independent_record_id,
      name: h.name,
      brand: h.brand,
      family: h.family,
      city: h.city,
      address: h.address,
      latitude: h.latitude,
      longitude: h.longitude,
      market: h.market,
      website: h.website,
      truth_source: "golden_census_v1_3_independent_official",
    })),
  });

  log("[staying] account check…");
  const account = await getAccount();
  if (!account.ok) {
    console.error("Account check failed:", safeErrorMessage(account.error?.message || "account_failed"));
    process.exit(1);
  }
  const starting = account.credits?.available;
  log(`[staying] plan=${account.plan?.code} key_env=${account.key_env} credits_available=${starting}`);

  const tracker = new StayingCreditTracker({
    ceiling: Math.min(CREDIT_CEILING, Math.max(20, (starting ?? CREDIT_CEILING) - 20)),
    startingAvailable: starting,
  });

  // Smoke test
  log(`[staying] smoke ${SMOKE_N} hotels…`);
  const smokeHotels = cohort.slice(0, SMOKE_N);
  const smokeResults = [];
  for (const h of smokeHotels) {
    const r = await runHotel(h, tracker, { platforms: ["booking"], limit: 5 });
    smokeResults.push(r);
    log(`[staying] smoke ${h.name.slice(0, 40)} level=${r.best_level} cands=${r.candidate_count} credits=${tracker.charged}`);
    await sleep(2500);
  }
  wj("_smoke-results.json", { smokeResults, credits: tracker.summary() });

  if (tracker.charged > 40 && starting != null && starting < 80) {
    log("[staying] smoke expensive relative to balance — continuing carefully with booking-only, limit=3");
  }

  // Full cohort (skip already smoked — re-include for consistent metrics)
  log(`[staying] full cohort ${cohort.length} (ceiling=${tracker.ceiling})…`);
  const searchResults = [];
  const matchResults = [];
  const listingResults = [];

  for (let i = 0; i < cohort.length; i++) {
    const h = cohort[i];
    if (!tracker.canSpend(5)) {
      log(`[staying] credit ceiling — stopping at ${i}/${cohort.length}`);
      break;
    }
    const r = await runHotel(h, tracker, {
      platforms: ["booking"],
      limit: 5,
      skipListing: false,
    });
    searchResults.push({
      hotel_id: r.hotel_id,
      group: r.group,
      family: r.family,
      search_ok: r.search_ok,
      candidate_count: r.candidate_count,
      search_credits: r.search_credits,
      first_result_eligible: r.first_result_eligible,
    });
    matchResults.push({
      hotel_id: r.hotel_id,
      group: r.group,
      family: r.family,
      best_level: r.best_level,
      best_score: r.best_score,
      matches: r.matches,
      best_match: r.best_match,
      best_candidate_summary: r.best_candidate
        ? {
            staying_id: r.best_candidate.staying_id,
            platform: r.best_candidate.platform,
            name: r.best_candidate.name,
            address: r.best_candidate.address,
            city: r.best_candidate.city,
            lat: r.best_candidate.latitude,
            lng: r.best_candidate.longitude,
            property_type: r.best_candidate.property_type_raw,
            amenities: r.best_candidate.amenities_raw,
          }
        : null,
    });
    if (r.listing_fetched) {
      listingResults.push({
        hotel_id: r.hotel_id,
        listing_credits: r.listing_credits,
        candidate: r.best_candidate,
      });
    }
    if ((i + 1) % 5 === 0) {
      log(`[staying] ${i + 1}/${cohort.length} credits=${tracker.charged} last=${r.best_level}`);
    }
    await sleep(2500);
  }

  // Ending account
  const accountEnd = await getAccount();
  tracker.endingAvailable = accountEnd.ok ? accountEnd.credits?.available : null;

  // Validations
  const addressVal = [];
  const coordVal = [];
  const amenityVal = [];
  const fieldResolution = [];
  let falseMatches = 0;

  for (const m of matchResults) {
    const hotel = cohort.find((h) => h.independent_record_id === m.hotel_id);
    const cand = m.best_candidate_summary;
    const eligible = m.best_level === "EXACT" || m.best_level === "HIGH";

    // False match: enrichment-eligible but control coords far OR address material conflict
    if (eligible && hotel.group === "B_CONTROL" && cand) {
      const addrClass = classifyAddressAgreement(hotel.address, cand);
      let dist = null;
      if (hotel.latitude != null && cand.lat != null) {
        dist = haversineM(hotel.latitude, hotel.longitude, cand.lat, cand.lng);
      }
      if (addrClass === "Material Conflict" && (dist == null || dist > 500)) {
        falseMatches += 1;
      }
    }

    if (hotel.group === "B_CONTROL") {
      addressVal.push({
        hotel_id: m.hotel_id,
        group: "control",
        agreement: classifyAddressAgreement(hotel.address, cand),
        truth: hotel.address,
        candidate: cand?.address || null,
        eligible,
      });
      let dist = null;
      if (eligible && hotel.latitude != null && cand?.lat != null) {
        dist = Math.round(haversineM(hotel.latitude, hotel.longitude, cand.lat, cand.lng));
      }
      coordVal.push({
        hotel_id: m.hotel_id,
        group: "control",
        distance_m: dist,
        band: distanceBand(dist),
        agreement: dist != null && dist <= 500,
        eligible,
      });
    } else {
      addressVal.push({
        hotel_id: m.hotel_id,
        group: "gap",
        resolved: eligible && Boolean(cand?.address),
        candidate: cand?.address || null,
        eligible,
      });
      const geoOk =
        eligible &&
        cand?.lat != null &&
        cand?.lng != null &&
        cand.lat > 14 &&
        cand.lat < 33 &&
        cand.lng > -118.5 &&
        cand.lng < -86;
      coordVal.push({
        hotel_id: m.hotel_id,
        group: "gap",
        resolved: geoOk,
        lat: cand?.lat ?? null,
        lng: cand?.lng ?? null,
        eligible,
      });
    }

    // Amenities
    const am = {};
    for (const token of cand?.amenities || []) {
      const field = AMENITY_MAP[token];
      if (field) am[field] = "Yes";
    }
    amenityVal.push({
      hotel_id: m.hotel_id,
      eligible,
      amenities_raw: cand?.amenities || [],
      amenities_dealality_yes: am,
      absent_treated_as: "UNKNOWN",
    });

    fieldResolution.push({
      hotel_id: m.hotel_id,
      group: hotel.group,
      match_level: m.best_level,
      enrichment_eligible: eligible,
      proposed: eligible
        ? {
            "Property Name": cand?.name || null,
            Address: cand?.address || null,
            City: cand?.city || null,
            Latitude: cand?.lat ?? null,
            Longitude: cand?.lng ?? null,
            "Property Type": cand?.property_type || null,
            Amenities: am,
            platform: cand?.platform || null,
            platform_listing_id: null,
          }
        : null,
      rooms_keys_proposed: false,
      bedrooms_mapped_to_rooms: false,
    });
  }

  const tested = matchResults.length;
  const found = matchResults.filter((m) => m.matches?.length > 0).length;
  const exact = matchResults.filter((m) => m.best_level === "EXACT").length;
  const high = matchResults.filter((m) => m.best_level === "HIGH").length;
  const medium = matchResults.filter((m) => m.best_level === "MEDIUM").length;
  const lowReject = matchResults.filter((m) => ["LOW", "REJECT"].includes(m.best_level)).length;
  const firstOk = searchResults.filter((s) => s.first_result_eligible).length;

  const controlAddr = addressVal.filter((a) => a.group === "control" && a.eligible);
  const controlAddrOk = controlAddr.filter((a) =>
    ["Exact", "Equivalent Normalized", "Minor Difference"].includes(a.agreement)
  ).length;
  const gapAddr = addressVal.filter((a) => a.group === "gap");
  const gapAddrResolved = gapAddr.filter((a) => a.resolved).length;

  const controlCoord = coordVal.filter((c) => c.group === "control" && c.eligible && c.distance_m != null);
  const controlCoordOk = controlCoord.filter((c) => c.agreement).length;
  const gapCoord = coordVal.filter((c) => c.group === "gap");
  const gapCoordResolved = gapCoord.filter((c) => c.resolved).length;
  const medianDist = (() => {
    const xs = controlCoord.map((c) => c.distance_m).filter((x) => x != null).sort((a, b) => a - b);
    if (!xs.length) return null;
    return xs[Math.floor(xs.length / 2)];
  })();

  const credits = tracker.summary();
  const usefulGaps =
    gapAddrResolved + gapCoordResolved;
  const creditsPerUseful = usefulGaps ? credits.total_credits_charged / usefulGaps : null;

  wj("06-search-results.json", { results: searchResults });
  wj("07-match-results.json", { results: matchResults });
  wj("08-listing-results.json", {
    results: listingResults.map((l) => ({
      hotel_id: l.hotel_id,
      listing_credits: l.listing_credits,
      staying_id: l.candidate?.staying_id,
      platform: l.candidate?.platform,
      name: l.candidate?.name,
      address: l.candidate?.address,
      // strip images full list size
      image_count: l.candidate?.image_urls_reference_only?.length || 0,
    })),
  });
  wj("09-address-validation.json", {
    rows: addressVal,
    control_agreement_rate:
      controlAddr.length === 0 ? null : Math.round((1000 * controlAddrOk) / controlAddr.length) / 10,
    gap_resolution_rate:
      gapAddr.length === 0 ? null : Math.round((1000 * gapAddrResolved) / gapAddr.length) / 10,
  });
  wj("10-coordinate-validation.json", {
    rows: coordVal,
    control_agreement_rate:
      controlCoord.length === 0 ? null : Math.round((1000 * controlCoordOk) / controlCoord.length) / 10,
    gap_resolution_rate:
      gapCoord.length === 0 ? null : Math.round((1000 * gapCoordResolved) / gapCoord.length) / 10,
    median_distance_m_control: medianDist,
  });
  wj("11-amenity-validation.json", {
    rows: amenityVal,
    mapping: AMENITY_MAP,
    note: "Absence = UNKNOWN, never No",
  });
  wj("12-property-type-mapping.json", buildPropertyTypeMappingArtifact());
  wm(
    "13-image-reference-assessment.md",
    `# Image Reference Assessment (informational only)

StayingAPI returns source image URLs and does not rehost.

Benchmark use:
- Detect whether imagery exists for a matched property
- Count image references on Exact/High matches

NOT approved:
- Download / rehost
- Airtable image writes
- Assume reuse rights

Listing image references observed: ${listingResults.reduce((s, l) => s + (l.candidate?.image_urls_reference_only?.length || 0), 0)} URLs across ${listingResults.length} listings (reference counts only).
`
  );
  wj("14-credit-ledger.json", credits);
  wj("15-field-resolution-results.json", { rows: fieldResolution, rooms_capability: STAYINGAPI_ROOMS_CAPABILITY });

  const siblingRisk = matchResults.filter((m) =>
    (m.best_match?.failures || []).includes("possible_sibling_property")
  ).length;

  wm(
    "16-false-match-analysis.md",
    `# False Match Analysis

- Enrichment-eligible control rows with material address conflict AND far coordinates: **${falseMatches}**
- Sibling-property failure flags: **${siblingRisk}**
- Match demotion rules: country/city hard reject; low name overlap + brand token → sibling demotion; >5km pin → reject

Philosophy: stricter than fuzzy V1 — aligned with V1.1 Exact/High gate.
`
  );

  const identityRate = tested ? ((exact + high) / tested) * 100 : 0;
  const recommend =
    identityRate >= 90 &&
    falseMatches / Math.max(1, tested) <= 0.02 &&
    (controlAddr.length === 0 || controlAddrOk / controlAddr.length >= 0.95)
      ? "INTEGRATE_FOR_LIMITED_FIELDS"
      : identityRate >= 70 && falseMatches <= 1
        ? "INTEGRATE_FOR_LIMITED_FIELDS"
        : identityRate >= 50
          ? "DISCOVERY_VALIDATION_ONLY"
          : "DO_NOT_INTEGRATE";

  const mostImportantly =
    identityRate >= 70 && falseMatches <= 1
      ? "YES, FOR LIMITED FIELDS"
      : identityRate >= 90
        ? "YES"
        : "NO";

  wm(
    "17-autopilot-integration-design.md",
    `# Autopilot Integration Design (recommendation only — NOT implemented)

## Recommendation: ${recommend}

### If limited fields
Lane B after official brand/page ladder for:
- Address
- Latitude / Longitude
- Property Type (mapped)
- Amenities (Yes-only from tokens; never No from absence)

Gate: Exact/High match + country/city constraints.

Never: Rooms/Keys, Owner, Operator, Opening, Meetings, F&B counts, images to production.

### Hierarchy challenge
Official structured > official page > approved geocode of official address > StayingAPI Exact/High.

Benchmark should confirm whether StayingAPI address quality rivals directory for IHG gaps.
`
  );

  wm(
    "18-future-discovery-assessment.md",
    `# Future Discovery Assessment (architecture only)

StayingAPI Search can query by location string across Booking/Google/Airbnb/Vrbo.

Potential later uses:
- Independent hotels missing from brand directories
- Local groups / soft brands
- Coverage challenges (not production truth)

Constraints:
- Exact/High identity still required before any Census write
- Quarantine vs Cvent/legacy challenge adapters
- Credit cost of search fan-out must be budgeted

Do not build discovery crawl in this benchmark.
`
  );

  const report = buildFinalReport({
    tested,
    found,
    exact,
    high,
    medium,
    lowReject,
    falseMatches,
    firstOk,
    controlAddrOk,
    controlAddr,
    gapAddrResolved,
    gapAddr,
    controlCoordOk,
    controlCoord,
    gapCoordResolved,
    gapCoord,
    medianDist,
    amenityVal,
    credits,
    usefulGaps,
    creditsPerUseful,
    siblingRisk,
    matchResults,
    recommend,
    mostImportantly,
    starting,
    identityRate,
  });
  wm("19-final-report.md", report);

  log(
    JSON.stringify(
      {
        ok: true,
        tested,
        exact,
        high,
        false_matches: falseMatches,
        credits_charged: credits.total_credits_charged,
        starting_credits: starting,
        ending_credits: tracker.endingAvailable,
        recommendation: recommend,
        most_importantly: mostImportantly,
        artifact_root: OUT,
      },
      null,
      2
    )
  );
}

function buildFinalReport(p) {
  const platforms = {};
  for (const m of p.matchResults) {
    const plat = m.best_candidate_summary?.platform || "none";
    platforms[plat] = (platforms[plat] || 0) + 1;
  }
  const byFam = {};
  for (const m of p.matchResults) {
    byFam[m.family] = byFam[m.family] || { n: 0, eh: 0 };
    byFam[m.family].n += 1;
    if (m.best_level === "EXACT" || m.best_level === "HIGH") byFam[m.family].eh += 1;
  }

  return `# StayingAPI Benchmark V1 — Final Report

## MOST IMPORTANTLY

**${p.mostImportantly}**

Recommendation: **${p.recommend.replace(/_/g, " ")}**

Can StayingAPI materially help close Address / Coordinate / Amenity gaps without unacceptable match risk?

**Answer: ${p.mostImportantly}**

---

1. Hotels tested: **${p.tested}**
2. Found (≥1 candidate): **${p.found}**
3. Exact: **${p.exact}**
4. High: **${p.high}**
5. Medium: **${p.medium}**
6. Low/Reject: **${p.lowReject}**
7. False matches: **${p.falseMatches}**
8. Correct-property-first-result rate: **${p.tested ? Math.round((1000 * p.firstOk) / p.tested) / 10 : 0}%**
9. Address control agreement: **${p.controlAddr.length ? Math.round((1000 * p.controlAddrOk) / p.controlAddr.length) / 10 : "n/a"}%** (${p.controlAddrOk}/${p.controlAddr.length} eligible controls)
10. Address gaps resolved: **${p.gapAddr.length ? Math.round((1000 * p.gapAddrResolved) / p.gapAddr.length) / 10 : "n/a"}%** (${p.gapAddrResolved}/${p.gapAddr.length})
11. Coordinate control agreement (≤500m): **${p.controlCoord.length ? Math.round((1000 * p.controlCoordOk) / p.controlCoord.length) / 10 : "n/a"}%**
12. Coordinate gaps resolved: **${p.gapCoord.length ? Math.round((1000 * p.gapCoordResolved) / p.gapCoord.length) / 10 : "n/a"}%**
13. Median coordinate distance vs controls: **${p.medianDist ?? "n/a"} m**
14. Amenities coverage: tokens mapped via controlled taxonomy when present; absence=UNKNOWN
15. Amenities agreement: informational — official comparison limited; Yes-only proposals
16. Property Type usefulness: MAPPABLE for hotel/resort; REFERENCE for house/cottage
17. Total hotel Rooms / Keys exposed?: **NO** (\`${STAYINGAPI_ROOMS_CAPABILITY}\`)
18. Did bedrooms map to Rooms / Keys?: **NO**
19. Total credits consumed: **${p.credits.total_credits_charged}** (start available ${p.starting}, end ${p.credits.ending_available})
20. Credits per tested hotel: **${p.tested ? Math.round((10 * p.credits.total_credits_charged) / p.tested) / 10 : 0}**
21. Credits per useful gap resolved: **${p.creditsPerUseful != null ? Math.round(p.creditsPerUseful * 10) / 10 : "n/a"}**
22. Underlying platforms in best matches: ${JSON.stringify(platforms)}
23. Family Exact+High rates: ${JSON.stringify(byFam)}
24. Sibling false-match risk flags: **${p.siblingRisk}**
25. Allowed propose fields: Address, Lat/Lng, Property Type (mapped), Amenities (Yes-only), identity metadata
26. Never populate: Rooms/Keys, Owner, Operator, Opening, Floors, F&B counts, Meetings, images to production
27. Permanent Lane B?: **${p.recommend === "INTEGRATE_AS_LANE_B" ? "YES" : p.recommend === "INTEGRATE_FOR_LIMITED_FIELDS" ? "LIMITED FIELDS ONLY" : "NO / NOT YET"}**
28. Independent discovery later?: **YES as architecture candidate** (not built)
29. Rights remaining: production persistence, customer-facing derived use, image reuse — all PENDING
30. Exact integration: Exact/High-only Lane B for Address/Coords/Amenities/Type after official ladder; never Rooms; no Autopilot change until rights confirmed

Identity Exact+High rate: **${Math.round(p.identityRate * 10) / 10}%**
`;
}

main().catch((err) => {
  console.error(safeErrorMessage(err));
  process.exit(1);
});
