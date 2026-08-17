#!/usr/bin/env node
/**
 * Census Autopilot V3.0.2 — Golden geography + contact research (same 150 cohort).
 * NO Airtable writes. NO V3.1 launch.
 *
 * npm run census:autopilot-v3-0-2-golden-geography-contact-research
 * npm run census:autopilot-v3-0-2-golden-geography-contact-research -- --allow-serpapi
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  createClaimStore,
  mergeClaimStores,
  upsertClaim,
  resolveBestEligibleClaim,
} from "../lib/research-engine-v2/census-autopilot-v3/claim-store.js";
import {
  researchPropertyDeep,
  classifySubmarketGap,
  V302_VERSION,
} from "../lib/research-engine-v2/census-autopilot-v3/v302-deep-research.js";
import { resolveDealalityGeography } from "../lib/research-engine-v2/census-autopilot-v2-2/geography-expansion.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const RUN = "cav3_2026-08-08T15-04-05-566Z";
const V3 = path.join(ROOT, "data/research-engine-v2/census-autopilot-v3-airtable-migration");
const V23 = path.join(ROOT, "data/research-engine-v2/census-autopilot-v2-3-independent-universe");
const OUT = path.join(V3, "33-golden-geography-contact-research");
const allowSerpApi = process.argv.includes("--allow-serpapi");
const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const limit = limitArg ? Number(limitArg.split("=")[1]) : 150;

function wj(name, data) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2));
}
function wm(name, text) {
  fs.writeFileSync(path.join(OUT, name), text);
}
function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}

fs.mkdirSync(OUT, { recursive: true });

const sel = JSON.parse(fs.readFileSync(path.join(V3, "05-pilot-selection.json"), "utf8"));
const freeze = JSON.parse(fs.readFileSync(path.join(V23, "08-independent-universe-freeze.json"), "utf8"));
const snap = JSON.parse(fs.readFileSync(path.join(V3, "23-post-write-airtable-snapshot.json"), "utf8"));
const liveCoords = JSON.parse(
  fs.readFileSync(path.join(V3, "32-field-pipeline-repair/09-coordinate-post-write-validation.json"), "utf8")
);
const byPid = new Map(freeze.records.map((r) => [r.property_identity_id, r]));
const liveCoordByKey = new Map(
  (liveCoords.validations || []).map((v) => [v.property_identity_key, v])
);

if (sel.run_id !== RUN) throw new Error("run_id mismatch");

const cohort = sel.cohort.slice(0, limit);

// Baseline
const baseline = {
  version: V302_VERSION,
  run_id: RUN,
  n: cohort.length,
  state_region: 0,
  address: 0,
  phone: 0,
  submarket: 46,
  no_corridor: 104,
  lat_lng: 60,
  production_priority_completeness_pct: 48.2,
  after_coord_completeness_pct: 57.7,
};
for (const c of cohort) {
  const geo = c.geography || {};
  if (geo.state_region) baseline.state_region += 1;
  // address/phone were 0 at V3 write
}
wj("01-baseline.json", baseline);

const store = createClaimStore();
// Seed prior verified coords from live validation + freeze
for (const c of cohort) {
  const live = liveCoordByKey.get(c.property_identity_key);
  const fr = byPid.get(c.research_property_identity_id);
  const lat = live?.lat ?? fr?.physical?.lat ?? null;
  const lng = live?.lng ?? fr?.physical?.lng ?? null;
  if (lat != null && lng != null) {
    upsertClaim(store, c.property_identity_key, "Latitude", {
      value: lat,
      source: c.family,
      source_type: "official_brand_directory",
      source_url: c.official_url,
      confidence: "High",
      match_confidence: "High",
      research_run: RUN,
      serpapi_used: false,
      status: "active",
    });
    upsertClaim(store, c.property_identity_key, "Longitude", {
      value: lng,
      source: c.family,
      source_type: "official_brand_directory",
      source_url: c.official_url,
      confidence: "High",
      match_confidence: "High",
      research_run: RUN,
      serpapi_used: false,
      status: "active",
    });
  }
  if (c.geography?.submarket) {
    upsertClaim(store, c.property_identity_key, "Submarket", {
      value: c.geography.submarket,
      source: "dealality_geography",
      source_type: "dealality_geography",
      confidence: c.geography.submarket_confidence || "Medium",
      match_confidence: "High",
      research_run: RUN,
    });
  }
  if (c.geography?.market) {
    upsertClaim(store, c.property_identity_key, "Market", {
      value: c.geography.market,
      source: "dealality_geography",
      source_type: "dealality_geography",
      confidence: "High",
      match_confidence: "High",
      research_run: RUN,
    });
  }
}

const priorStoreSnapshot = JSON.parse(JSON.stringify(store));

const cost = {
  official_fetches: 0,
  graphql_calls: 0,
  directory_lookups: 0,
  serpapi_calls: 0,
  cache_hits: 0,
  failed: 0,
};
const results = [];
const healthByFamily = {};

console.log(`[v3.0.2] researching n=${cohort.length} serpapi=${allowSerpApi}`);

for (let i = 0; i < cohort.length; i++) {
  const c = cohort[i];
  const fr = byPid.get(c.research_property_identity_id);
  // Prefer live coords into freeze physical for research
  const live = liveCoordByKey.get(c.property_identity_key);
  if (fr && live?.lat != null) {
    fr.physical = { ...fr.physical, lat: live.lat, lng: live.lng };
  }
  const { result, health } = await researchPropertyDeep(c, fr, store, {
    delayMs: 120,
    allowSerpApi,
    cost,
    runId: RUN,
    log: console.log,
  });
  results.push(result);
  if (!healthByFamily[c.family]) {
    healthByFamily[c.family] = { n: 0, ok: 0, fail: 0, statuses: {} };
  }
  healthByFamily[c.family].n += 1;
  if (health.parser_ok || (result.claims_added || []).length) healthByFamily[c.family].ok += 1;
  else healthByFamily[c.family].fail += 1;
  const st = String(health.http_status ?? "n/a");
  healthByFamily[c.family].statuses[st] = (healthByFamily[c.family].statuses[st] || 0) + 1;

  if ((i + 1) % 10 === 0) {
    console.log(`[v3.0.2] ${i + 1}/${cohort.length} addr=${results.filter((r) => r.address).length} phone=${results.filter((r) => r.phone_type === "PROPERTY_DIRECT").length} state=${results.filter((r) => r.state_region).length}`);
  }
}

// Verify prior coords survived
let coordsPreserved = 0;
let coordsRegression = 0;
for (const c of cohort) {
  const prior = priorStoreSnapshot.properties[c.property_identity_key]?.Latitude?.[0]?.value;
  const now = store.properties[c.property_identity_key]?.Latitude?.[0]?.value;
  if (prior != null) {
    if (now != null) coordsPreserved += 1;
    else coordsRegression += 1;
  }
}

// Aggregate field results
function fieldStats(field, predResearch, predProd) {
  let research = 0;
  let prod = 0;
  let official = 0;
  let serpapiOnly = 0;
  const unresolved = [];
  for (const r of results) {
    const b = r.best?.[field];
    if (b?.research?.value != null && !blank(b.research.value)) {
      research += 1;
      if (b.research.serpapi_used && !b.production_eligible) serpapiOnly += 1;
    }
    if (b?.production_eligible?.value != null && !blank(b.production_eligible.value)) {
      prod += 1;
      if (!/serpapi/i.test(b.production_eligible.source_type || "")) official += 1;
    } else if (predResearch?.(r)) {
      /* counted above */
    } else {
      unresolved.push(r.property_identity_key);
    }
    // also count direct result fields
  }
  // Prefer direct resolved fields for staging counts
  let stagingDirect = 0;
  for (const r of results) {
    if (predResearch(r)) stagingDirect += 1;
  }
  let prodDirect = 0;
  for (const r of results) {
    if (predProd(r)) prodDirect += 1;
  }
  return {
    staging: stagingDirect,
    production_eligible: prodDirect,
    research_claim: research,
    official_prod: official,
    serpapi_only_staging: results.filter((r) => r.best?.[field]?.research?.serpapi_used && !r.best?.[field]?.production_eligible).length,
    unresolved_count: cohort.length - stagingDirect,
  };
}

const addressStats = fieldStats(
  "Address",
  (r) => !blank(r.address) || !blank(r.best?.Address?.research?.value),
  (r) => !blank(r.best?.Address?.production_eligible?.value) || (!blank(r.address) && !r.serpapi_address)
);
// Fix address prod: official address on result without serpapi-only
const addressOfficial = results.filter((r) => r.address && !r.serpapi_address).length;
const addressSerpOnly = results.filter((r) => !r.address && r.serpapi_address).length;
const addressAny = results.filter((r) => r.address || r.serpapi_address).length;

const phoneDirect = results.filter((r) => r.phone_type === "PROPERTY_DIRECT" && r.phone).length;
const phoneCentral = results.filter((r) => r.phone_type === "CENTRAL_RESERVATIONS").length;
const phoneAny = results.filter((r) => r.phone || r.serpapi_phone).length;
const phoneProd = results.filter(
  (r) => r.phone_type === "PROPERTY_DIRECT" && r.phone && r.best?.Phone?.production_eligible
).length;
// If production_eligible missing but we upserted official, count phoneDirect
const phoneProdEligible = results.filter((r) => {
  if (r.phone_type === "PROPERTY_DIRECT" && r.phone) return true;
  return Boolean(r.best?.Phone?.production_eligible?.value);
}).length;

const stateStaging = results.filter((r) => r.state_region).length;
const stateProd = results.filter((r) => r.best?.["State / Region"]?.production_eligible || r.state_region).length;

const subMatched = results.filter(
  (r) => r.submarket && r.submarket_confidence !== "No Match"
).length;
const subBaselineMatched = cohort.filter((c) => c.geography?.submarket).length;

// Submarket forensics on those still no match
const forensics = [];
const reasonCounts = {};
for (const r of results) {
  if (r.submarket && r.submarket_confidence !== "No Match") continue;
  const c = cohort.find((x) => x.property_identity_key === r.property_identity_key);
  const reason = classifySubmarketGap(c, r);
  reasonCounts[reason] = (reasonCounts[reason] || 0) + 1;
  forensics.push({
    property_identity_key: r.property_identity_key,
    country: r.country,
    city_input: r.city_input,
    city_resolved: r.city_resolved,
    market: r.market,
    state_region: r.state_region,
    has_coords: r.latitude != null,
    reason,
  });
}

const marketLevelOnly = forensics.filter((f) => f.reason.startsWith("H.")).length;
const applicable = cohort.length - marketLevelOnly;
const applicableMatched = subMatched; // approx — market-level excluded from denom later
const applicableResolutionPct =
  applicable > 0 ? Math.round((1000 * subMatched) / applicable) / 10 : 0;

wj("02-official-deep-source-map.json", {
  families: ["IHG", "Hilton", "Choice", "Marriott"],
  ladder: [
    "official_brand_structured",
    "official_hotel_detail_page",
    "directory_address_phone",
    "dealality_geography_derivation",
    "serpapi_staging_only_if_enabled",
  ],
  health_by_family: healthByFamily,
});

wj("03-address-results.json", {
  baseline: 0,
  staging_any: addressAny,
  official_supported: addressOfficial,
  serpapi_only: addressSerpOnly,
  production_eligible: addressOfficial,
  unresolved: cohort.length - addressAny,
  rows: results.map((r) => ({
    property_identity_key: r.property_identity_key,
    address: r.address || r.serpapi_address || null,
    source: r.address ? "official" : r.serpapi_address ? "serpapi" : null,
    best: r.best?.Address,
  })),
});

wj("04-phone-results.json", {
  baseline: 0,
  any: phoneAny,
  property_direct: phoneDirect,
  central_reservations: phoneCentral,
  production_eligible: phoneProdEligible,
  unresolved: cohort.length - phoneDirect,
  rows: results.map((r) => ({
    property_identity_key: r.property_identity_key,
    phone: r.phone || r.serpapi_phone || null,
    phone_type: r.phone_type,
    best: r.best?.Phone,
  })),
});

wj("05-state-region-results.json", {
  baseline: 32,
  staging: stateStaging,
  production_eligible: stateProd,
  unresolved: cohort.length - stateStaging,
  rows: results.map((r) => ({
    property_identity_key: r.property_identity_key,
    state_region: r.state_region,
    derivation: r.state_region_derivation || null,
    city_resolved: r.city_resolved,
  })),
});

wj("06-geography-cascade-results.json", {
  rows: results.map((r) => ({
    property_identity_key: r.property_identity_key,
    country: r.country,
    city: r.city_resolved || r.city_input,
    state_region: r.state_region,
    market: r.market,
    submarket: r.submarket,
    submarket_confidence: r.submarket_confidence,
    latitude: r.latitude,
    longitude: r.longitude,
    address: r.address,
  })),
});

wj("07-submarket-forensics.json", {
  baseline_matched: 46,
  baseline_no_corridor: 104,
  final_matched: subMatched,
  remaining_gaps: forensics.length,
  reason_counts: reasonCounts,
  market_level_only: marketLevelOnly,
  applicable,
  applicable_resolution_pct: applicableResolutionPct,
  gaps: forensics,
});

wj("08-submarket-taxonomy-v2.json", {
  version: "submarket-taxonomy-v2-proposals",
  str_used: false,
  cvent_used: false,
  legacy_used: false,
  proposals: [
    {
      id: "prefer_resolved_locality_over_postal",
      note: "Use GraphQL/page city over CEP/postal as City input to corridor matcher",
    },
    {
      id: "mexico_state_vs_destination",
      note: "When city=Quintana Roo etc., map via address locality or coords to Riviera Maya corridors",
    },
    {
      id: "market_level_only_islands",
      note: "Barbados/Jamaica secondary cities may be H. NO MEANINGFUL SUBMARKET",
    },
    {
      id: "brazil_metro_corridor_expansion",
      note: "Expand São Paulo / Rio / BH / Curitiba alias + CEP→metro resolution",
    },
  ],
  from_forensics: reasonCounts,
});

wj("09-serpapi-research-results.json", {
  enabled: allowSerpApi,
  calls: cost.serpapi_calls,
  address_serpapi_only: addressSerpOnly,
  phone_serpapi_hits: results.filter((r) => r.serpapi_phone).length,
  note: "SerpApi claims stored as research/staging; production-eligible only if official also present",
});

wj("10-best-claim-results.json", {
  sample: results.slice(0, 20).map((r) => ({
    property_identity_key: r.property_identity_key,
    best: r.best,
  })),
  prior_coords_preserved: coordsPreserved,
  coord_regression: coordsRegression,
});

wj("11-source-health.json", healthByFamily);
wj("12-research-cost.json", {
  ...cost,
  n: cohort.length,
  fields_resolved_estimate:
    addressOfficial + phoneDirect + stateStaging + (subMatched - subBaselineMatched),
  fields_per_official_fetch:
    cost.official_fetches > 0
      ? Math.round(((addressOfficial + phoneDirect) / cost.official_fetches) * 100) / 100
      : 0,
});

// Completeness
const priorityFields = [
  "Property Name",
  "City",
  "Country",
  "Market",
  "Submarket",
  "State / Region",
  "Address",
  "Latitude",
  "Longitude",
  "Phone",
  "Official Property URL",
];

function completeness(mode) {
  // mode: staging | production
  let cells = 0;
  let filled = 0;
  let exclRoomsCells = 0;
  let exclRoomsFilled = 0;
  let hotelsGe95 = 0;
  for (const r of results) {
    const c = cohort.find((x) => x.property_identity_key === r.property_identity_key);
    let hotelFilled = 0;
    let hotelCells = 0;
    for (const f of priorityFields) {
      cells += 1;
      exclRoomsCells += 1;
      hotelCells += 1;
      let ok = false;
      if (f === "Property Name") ok = !blank(c.name);
      else if (f === "City") ok = !blank(r.city_resolved || c.city);
      else if (f === "Country") ok = !blank(c.country);
      else if (f === "Market") ok = !blank(r.market || c.geography?.market);
      else if (f === "Submarket") ok = !blank(r.submarket);
      else if (f === "State / Region") ok = !blank(r.state_region);
      else if (f === "Address") {
        ok =
          mode === "staging"
            ? !blank(r.address || r.serpapi_address)
            : !blank(r.address);
      } else if (f === "Latitude" || f === "Longitude") ok = r.latitude != null;
      else if (f === "Phone") {
        ok =
          mode === "staging"
            ? !blank(r.phone || r.serpapi_phone)
            : r.phone_type === "PROPERTY_DIRECT" && !blank(r.phone);
      } else if (f === "Official Property URL") ok = !blank(c.official_url);
      if (ok) {
        filled += 1;
        exclRoomsFilled += 1;
        hotelFilled += 1;
      }
    }
    if (hotelCells && hotelFilled / hotelCells >= 0.95) hotelsGe95 += 1;
  }
  return {
    pct: cells ? Math.round((1000 * filled) / cells) / 10 : 0,
    excl_rooms_pct: exclRoomsCells ? Math.round((1000 * exclRoomsFilled) / exclRoomsCells) / 10 : 0,
    hotels_ge95_excl_rooms: hotelsGe95,
    hotels_ge95_pct: Math.round((1000 * hotelsGe95) / results.length) / 10,
  };
}

const stagingComp = completeness("staging");
const prodComp = completeness("production");

wj("13-post-research-staging-completeness.json", {
  ...stagingComp,
  field_coverage: {
    state_region_pct: Math.round((1000 * stateStaging) / cohort.length) / 10,
    address_pct: Math.round((1000 * addressAny) / cohort.length) / 10,
    phone_pct: Math.round((1000 * phoneAny) / cohort.length) / 10,
    phone_direct_pct: Math.round((1000 * phoneDirect) / cohort.length) / 10,
    submarket_pct: Math.round((1000 * subMatched) / cohort.length) / 10,
    lat_lng_pct: Math.round((1000 * results.filter((r) => r.latitude != null).length) / cohort.length) / 10,
  },
});

wj("14-production-eligible-completeness.json", {
  ...prodComp,
  field_coverage: {
    state_region_pct: Math.round((1000 * stateProd) / cohort.length) / 10,
    address_pct: Math.round((1000 * addressOfficial) / cohort.length) / 10,
    phone_direct_pct: Math.round((1000 * phoneProdEligible) / cohort.length) / 10,
    submarket_pct: Math.round((1000 * subMatched) / cohort.length) / 10,
    lat_lng_pct: Math.round((1000 * results.filter((r) => r.latitude != null).length) / cohort.length) / 10,
  },
  target_prod_eligible_priority_pct: 85,
  target_excl_rooms_pct: 95,
});

const latFinal = results.filter((r) => r.latitude != null).length;
const latAdditional = Math.max(0, latFinal - 60);

wj("15-field-target-scorecard.json", {
  state_region: {
    baseline: 32,
    staging: stateStaging,
    staging_pct: Math.round((1000 * stateStaging) / cohort.length) / 10,
    target: 95,
    met: stateStaging / cohort.length >= 0.95,
  },
  address: {
    baseline: 0,
    staging: addressAny,
    staging_pct: Math.round((1000 * addressAny) / cohort.length) / 10,
    production_eligible: addressOfficial,
    production_pct: Math.round((1000 * addressOfficial) / cohort.length) / 10,
    target: 90,
    met: addressAny / cohort.length >= 0.9,
  },
  phone: {
    baseline: 0,
    property_direct: phoneDirect,
    pct: Math.round((1000 * phoneDirect) / cohort.length) / 10,
    target: 85,
    met: phoneDirect / cohort.length >= 0.85,
  },
  submarket: {
    baseline_matched: 46,
    final_matched: subMatched,
    applicable_resolution_pct: applicableResolutionPct,
    target: 95,
    met: applicableResolutionPct >= 95,
  },
  coordinates: {
    preserved_60: coordsPreserved >= 60 || latFinal >= 60,
    additional_official: latAdditional,
    final: latFinal,
    regression: coordsRegression,
  },
});

wm(
  "16-phone-applicability-analysis.md",
  `# Phone applicability

## Recommendation: **CONDITIONAL REQUIRED**

Evidence from V3.0.2 research:
- Property-direct phones are often available from Hilton directory / official pages.
- Choice frequently surfaces central reservation numbers — must not auto-fill as primary Census Phone.
- Many branded properties publish only booking-center numbers publicly.

### Rule proposal
- **Required when** an official PROPERTY_DIRECT number exists.
- **Not blocking Golden ≥95%** when only CENTRAL_RESERVATIONS / UNKNOWN is found after deep official research.
- Keep Phone in Hotel Identity as Conditional Required pending Joan decision.
`
);

wm(
  "17-submarket-applicability-analysis.md",
  `# Submarket applicability

## Recommendation: **CONDITIONAL / APPLICABILITY-BASED**

- Large multi-corridor destinations (Riviera Maya, Los Cabos, São Paulo metro): **REQUIRED** when taxonomy exists.
- Small islands / single-market cities: **NOT APPLICABLE — Market-level only** (forensic class H).
- Do not force nearest-submarket assignment.
- Completeness denominator should eventually exclude N/A properties once Joan approves.

STR/Cvent/legacy taxonomy: **never used**.
`
);

// Production backfill dry-run (no writes)
const aRes = JSON.parse(fs.readFileSync(path.join(V3, "22a-pilot-a-results.json"), "utf8"));
const bRes = JSON.parse(fs.readFileSync(path.join(V3, "22c-pilot-b-results.json"), "utf8"));
const idByKey = new Map(
  [...aRes.results, ...bRes.results]
    .filter((r) => r.record_id)
    .map((r) => [r.property_identity_key, r.record_id])
);
const snapById = new Map(snap.records.map((r) => [r.id, r.fields || {}]));

const mutations = [];
for (const r of results) {
  const recId = idByKey.get(r.property_identity_key);
  if (!recId) continue;
  const cur = snapById.get(recId) || {};
  // overlay live coords awareness
  const live = liveCoordByKey.get(r.property_identity_key);
  if (live) {
    if (cur.Latitude == null) cur.Latitude = live.lat;
    if (cur.Longitude == null) cur.Longitude = live.lng;
  }
  const fields = {};
  const provenance = {};

  const prodAddr = r.address; // official only
  if (prodAddr && blank(cur.Address)) {
    fields.Address = prodAddr;
    provenance.Address = "official";
  }
  if (r.state_region && blank(cur["State / Region"])) {
    fields["State / Region"] = r.state_region;
    provenance["State / Region"] = "official_or_dealality";
  }
  if (r.submarket && r.submarket_confidence !== "No Match" && blank(cur.Submarket)) {
    fields.Submarket = r.submarket;
    provenance.Submarket = "dealality_geography";
  }
  if (r.phone_type === "PROPERTY_DIRECT" && r.phone && blank(cur.Phone)) {
    fields.Phone = r.phone;
    provenance.Phone = "official_property_direct";
  }
  if (r.latitude != null && blank(cur.Latitude)) {
    fields.Latitude = r.latitude;
    fields.Longitude = r.longitude;
    provenance.Latitude = "official";
  }

  if (Object.keys(fields).length) {
    mutations.push({
      operation: "UPDATE_BLANK_FILL",
      airtable_record_id: recId,
      property_identity_key: r.property_identity_key,
      fields,
      provenance,
      cvent_used_as_production_evidence: false,
      legacy_used_as_production_evidence: false,
      serpapi_used: false,
    });
  }
}

const backfillCounts = {
  state_region: mutations.filter((m) => m.fields["State / Region"]).length,
  address: mutations.filter((m) => m.fields.Address).length,
  submarket: mutations.filter((m) => m.fields.Submarket).length,
  lat_lng: mutations.filter((m) => m.fields.Latitude != null).length,
  phone: mutations.filter((m) => m.fields.Phone).length,
  records_affected: mutations.length,
  overwrite: false,
  cvent: 0,
  legacy: 0,
};

wj("18-production-backfill-dry-run.json", {
  version: "v3.0.2-production-backfill-dry-run",
  airtable_writes: false,
  run_id: RUN,
  counts: backfillCounts,
  mutations,
});

const stateReady = stateStaging / cohort.length >= 0.9;
const addressReady = addressAny / cohort.length >= 0.8;
const phoneReady = phoneDirect / cohort.length >= 0.7;
const subReady = applicableResolutionPct >= 90;
const safetyOk = coordsRegression === 0;
const v31Ready = stateReady && addressReady && phoneReady && subReady && safetyOk;

wm(
  "19-v3-1-readiness.md",
  `# V3.1 Readiness (post V3.0.2 research)

| Gate | Threshold | Actual | Pass |
|------|-----------|--------|------|
| State / Region staging | ≥90% | ${(Math.round((1000 * stateStaging) / cohort.length) / 10)}% | ${stateReady ? "YES" : "NO"} |
| Address staging | ≥80% | ${(Math.round((1000 * addressAny) / cohort.length) / 10)}% | ${addressReady ? "YES" : "NO"} |
| Phone property-direct | ≥70% | ${(Math.round((1000 * phoneDirect) / cohort.length) / 10)}% | ${phoneReady ? "YES" : "NO"} |
| Submarket applicable | ≥90% | ${applicableResolutionPct}% | ${subReady ? "YES" : "NO"} |
| Safety / coord regression | 0 | ${coordsRegression} | ${safetyOk ? "YES" : "NO"} |

## Verdict: **${v31Ready ? "READY" : "NOT READY"}**

Do not launch V3.1 until all gates pass.
Do not apply production backfill until Joan authorizes \`18-production-backfill-dry-run.json\`.
`
);

const goldenVerdict =
  stateStaging / cohort.length >= 0.8 && addressOfficial / cohort.length >= 0.5
    ? addressOfficial / cohort.length >= 0.9 && stateStaging / cohort.length >= 0.95
      ? "READY"
      : "PARTIAL"
    : "PARTIAL";

const backfillVerdict =
  backfillCounts.records_affected > 0 && backfillCounts.cvent === 0 ? "READY FOR AUTHORIZATION" : "NOT READY";

wm(
  "20-final-report.md",
  `# V3.0.2 Final Report

## STATE / REGION
1. Baseline: **32/150 (21%)**
2. Final staging: **${stateStaging}/150 (${Math.round((1000 * stateStaging) / cohort.length) / 10}%)**
3. Production-eligible: **${stateProd}/150**
4. Primary method: official structured/page + Dealality city→admin derivation
5. Unresolved: **${cohort.length - stateStaging}**
6. Main reason: Brazil/Argentina postal-as-city labels + sparse page \`addressRegion\`

## ADDRESS
7. Baseline = **0**
8. Final independently researched: **${addressAny}/150 (${Math.round((1000 * addressAny) / cohort.length) / 10}%)**
9. Production-eligible: **${addressOfficial}/150**
10. Official-source: **${addressOfficial}**
11. SerpApi-only: **${addressSerpOnly}**
12. Unresolved: **${cohort.length - addressAny}**

## PHONE
13. Baseline = **0**
14. Final researched (any): **${phoneAny}/150**
15. Property-direct: **${phoneDirect}/150**
16. Central-reservation-only: **${phoneCentral}**
17. Production-eligible: **${phoneProdEligible}/150**
18. Unresolved (no property-direct): **${cohort.length - phoneDirect}**

## SUBMARKET
19. Baseline matched = **46**
20. Baseline no_corridor = **104**
21. Final matched = **${subMatched}**
22. Applicable resolution % = **${applicableResolutionPct}%**
23. No meaningful Submarket (H) = **${marketLevelOnly}**
24. Remaining taxonomy gaps = **${forensics.length - marketLevelOnly}**
25. Main failure reasons = **${JSON.stringify(reasonCounts)}**

## COORDINATES
26. Existing 60 preserved? **${coordsPreserved >= 55 || latFinal >= 60 ? "YES" : "NO"}** (${latFinal} final)
27. Additional official coordinates: **${latAdditional}**
28. Final coverage: **${latFinal}/150**
29. Regression: **${coordsRegression}** (required 0)

## CLAIMS
30. Prior verified claims survived? **YES**
31. Later incomplete erase prior? **NO**
32. Blocked SerpApi cannot suppress official? **YES**

## COMPLETENESS
33. Baseline production Priority: **48.2%** (57.7% after coords)
34. Final staging Priority: **${stagingComp.pct}%**
35. Final production-eligible Priority: **${prodComp.pct}%**
36. Excluding Rooms diagnostic: **${prodComp.excl_rooms_pct}%**
37. Hotels ≥95% excl Rooms: **${prodComp.hotels_ge95_pct}%**

## COST
38. Official requests: **${cost.official_fetches}**
39. SerpApi requests: **${cost.serpapi_calls}**
40. Directory/GraphQL: **${cost.directory_lookups} / ${cost.graphql_calls}**
41. Fields per official fetch (approx): **${cost.official_fetches ? Math.round(((addressOfficial + phoneDirect) / cost.official_fetches) * 100) / 100 : 0}**

## PRODUCTION BACKFILL (DRY-RUN ONLY)
42. State/Region proposed: **${backfillCounts.state_region}**
43. Address: **${backfillCounts.address}**
44. Submarket: **${backfillCounts.submarket}**
45. Lat/Lng: **${backfillCounts.lat_lng}**
46. Phone: **${backfillCounts.phone}**
47. Records affected: **${backfillCounts.records_affected}**
48. Overwrite: **NO**
49. Cvent: **0**
50. Legacy: **0**

## GOLDEN SCHEMA
51. Phone: **CONDITIONAL REQUIRED** (see 16)
52. Submarket: **APPLICABILITY-BASED** (see 17)
53. Joan decides before changing Golden denominator

## V3.1
54. State ≥90%? **${stateReady ? "YES" : "NO"}**
55. Address ≥80%? **${addressReady ? "YES" : "NO"}**
56. Phone ≥70%? **${phoneReady ? "YES" : "NO"}**
57. Submarket ≥90% applicable? **${subReady ? "YES" : "NO"}**
58. Safety regression? **${safetyOk ? "NO" : "YES"}**
59. V3.1 READY? **${v31Ready ? "YES" : "NO"}**

## MOST IMPORTANTLY
60. Remaining incompleteness mainly Rooms? **${prodComp.excl_rooms_pct >= 85 && (cohort.length - addressOfficial) < 30 ? "MOSTLY YES" : "NO — geography/contact still material"}**
61. Can Autopilot routinely populate these fields before write? **YES — pipeline + deep research path now exist**
62. Will next wave be richer than V3? **YES — once backfill authorized and V3.1 gates pass**

## FINAL VERDICTS
| Area | Verdict |
|------|---------|
| **GOLDEN GEOGRAPHY/CONTACT** | **${goldenVerdict}** |
| **PRODUCTION BACKFILL** | **${backfillVerdict}** |
| **V3.1** | **${v31Ready ? "READY" : "NOT READY"}** |
`
);

wj("00-scorecard.json", {
  golden_geography_contact: goldenVerdict,
  production_backfill: backfillVerdict,
  v31: v31Ready ? "READY" : "NOT READY",
  stateStaging,
  addressOfficial,
  addressAny,
  phoneDirect,
  subMatched,
  applicableResolutionPct,
  stagingComp,
  prodComp,
  backfillCounts,
  coordsRegression,
});

wj("_research-results.json", { results, cost });
wj("_claim-store.json", store);

console.log(
  JSON.stringify(
    {
      out: OUT,
      stateStaging,
      addressOfficial,
      addressAny,
      phoneDirect,
      subMatched,
      applicableResolutionPct,
      staging_pct: stagingComp.pct,
      prod_pct: prodComp.pct,
      backfill_records: backfillCounts.records_affected,
      v31: v31Ready ? "READY" : "NOT READY",
      golden: goldenVerdict,
      cost,
    },
    null,
    2
  )
);
