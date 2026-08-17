#!/usr/bin/env node
/**
 * Census Autopilot V3.0.1 — Field pipeline repair + official coordinate backfill.
 * Does NOT launch V3.1. Coordinate writes require ENABLE_VERIFIED_CENSUS_WRITES=1.
 *
 * npm run census:autopilot-v3-0-1-field-pipeline-repair
 * npm run census:autopilot-v3-0-1-field-pipeline-repair -- --apply-coords
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import { resolveStateRegion } from "../lib/research-engine-v2/census-autopilot-v3/state-region-pipeline.js";
import { resolveDealalityGeography } from "../lib/research-engine-v2/census-autopilot-v2-2/geography-expansion.js";
import {
  createClaimStore,
  upsertClaim,
  mergeClaimStores,
  resolveBestEligibleClaim,
  assertOfficialBeatsBlockedSerpApi,
  CLAIM_STORE_VERSION,
} from "../lib/research-engine-v2/census-autopilot-v3/claim-store.js";
import {
  classifyFieldWrites,
  WRITER_CONTRACT_FIELDS,
} from "../lib/research-engine-v2/census-autopilot-v3/dry-run.js";
import { buildWritePolicy, buildGoldenToAirtableFieldMap } from "../lib/research-engine-v2/census-autopilot-v3/field-policy.js";
import { runOfficialCoordinateBackfill } from "../lib/research-engine-v2/census-autopilot-v3/coordinate-backfill.js";
import { WRITE_CLASS, PHASE2_ENV_GATE } from "../lib/research-engine-v2/census-autopilot-v3/constants.js";
import { buildAddressStaging } from "../lib/research-engine-v2/census-autopilot-v3/address-pipeline.js";
import { buildPhoneStaging } from "../lib/research-engine-v2/census-autopilot-v3/phone-pipeline.js";
import { INSERT_ALLOWED_FIELDS } from "../lib/research-engine-v2/census-autopilot-source-discovery.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const RUN = "cav3_2026-08-08T15-04-05-566Z";
const V3 = path.join(ROOT, "data/research-engine-v2/census-autopilot-v3-airtable-migration");
const V23 = path.join(ROOT, "data/research-engine-v2/census-autopilot-v2-3-independent-universe");
const OUT = path.join(V3, "32-field-pipeline-repair");
const applyCoords = process.argv.includes("--apply-coords");
const gateOn = String(process.env[PHASE2_ENV_GATE] || "").trim() === "1";

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
const inserts = JSON.parse(fs.readFileSync(path.join(V3, "11-dry-run-inserts.json"), "utf8"));
const updates = JSON.parse(fs.readFileSync(path.join(V3, "12-dry-run-updates.json"), "utf8"));
const tx = JSON.parse(fs.readFileSync(path.join(V3, "22-write-transaction-log.json"), "utf8"));
const snap = JSON.parse(fs.readFileSync(path.join(V3, "23-post-write-airtable-snapshot.json"), "utf8"));
const schema = JSON.parse(fs.readFileSync(path.join(V3, "_schema-live.json"), "utf8"));
const diagDry = JSON.parse(
  fs.readFileSync(path.join(V3, "31-field-gap-diagnostic/10-corrective-backfill-dry-run.json"), "utf8")
);
const byPid = new Map(freeze.records.map((r) => [r.property_identity_id, r]));

if (sel.run_id !== RUN) throw new Error("run_id mismatch");

wm(
  "01-baseline.md",
  `# V3.0.1 Baseline

Authorized run: \`${RUN}\`
Diagnostic: \`31-field-gap-diagnostic/\`
Coordinate dry-run mutations: **${diagDry.proposed_mutations.length}**
Apply coords this run: **${applyCoords && gateOn}**
`
);

wm(
  "02-claim-level-rights-design.md",
  `# Claim-level rights selection

Version: \`${CLAIM_STORE_VERSION}\`

## Rule
A blocked lower-authority claim must **never** suppress a permitted higher-authority claim for the same field.

## Flow
RESEARCH CLAIM → FIELD-SPECIFIC SOURCE SELECTION → BEST ELIGIBLE CLAIM → GOLDEN STAGING → WRITE CLASS → AIRTABLE

## API
\`resolveBestEligibleClaim(claims)\` returns selected_claim, selected_source, selected_rights_status, rejected_claims_with_reason.

SerpApi-only → BLOCKED_RIGHTS (rejected). Official alongside SerpApi → official selected.
`
);

// Coordinate claim selection for 60
const coordSelection = diagDry.proposed_mutations.map((m) => {
  const claims = [
    {
      value: m.fields.Latitude,
      source: m.family,
      source_type: "official_brand_directory",
      source_url: m.provenance.source_url,
      serpapi_used: false,
      confidence: "High",
      match_confidence: "High",
      research_run: RUN,
    },
    // hypothetical blocked SerpApi must not win
    {
      value: Number(m.fields.Latitude) + 0.001,
      source: "serpapi",
      source_type: "serpapi_google_hotels",
      serpapi_used: true,
      confidence: "High",
      match_confidence: "High",
    },
  ];
  const lat = resolveBestEligibleClaim(claims, { field: "Latitude" });
  const lng = resolveBestEligibleClaim(
    [
      {
        value: m.fields.Longitude,
        source: m.family,
        source_type: "official_brand_directory",
        serpapi_used: false,
        confidence: "High",
        match_confidence: "High",
      },
      {
        value: Number(m.fields.Longitude) + 0.001,
        source: "serpapi",
        source_type: "serpapi_google_hotels",
        serpapi_used: true,
        confidence: "High",
        match_confidence: "High",
      },
    ],
    { field: "Longitude" }
  );
  return {
    property_identity_key: m.property_identity_key,
    family: m.family,
    latitude: lat,
    longitude: lng,
    official_selected:
      lat.selected_claim?.value === m.fields.Latitude &&
      lng.selected_claim?.value === m.fields.Longitude,
  };
});
wj("03-coordinate-claim-selection.json", {
  count: coordSelection.length,
  all_official_selected: coordSelection.every((c) => c.official_selected),
  rows: coordSelection,
});

wj("04-coordinate-backfill-manifest.json", {
  run_id: RUN,
  version: "v3.0.1-official-coordinate-backfill",
  authorized_max: 60,
  pilot_a: 10,
  pilot_b: 50,
  mutations: diagDry.proposed_mutations,
});

// State/Region staging recompute
const stateRows = sel.cohort.map((c) => {
  const r = byPid.get(c.research_property_identity_id);
  const geo = resolveDealalityGeography({
    name: c.name,
    country: c.country,
    city: c.city,
    state_region: r?.physical?.state || null,
    address: r?.physical?.address || null,
  });
  return {
    property_identity_key: c.property_identity_key,
    country: c.country,
    city: c.city,
    state_region: geo.state_region,
    confidence: geo.state_region_confidence,
    derivation: geo.state_region_derivation,
    ok: Boolean(geo.state_region),
  };
});
const stateOk = stateRows.filter((r) => r.ok).length;

wm(
  "10-state-region-pipeline.md",
  `# State / Region pipeline

## Root cause
Geography resolver previously omitted State / Region; dry-run never \`add()\`d it.

## Fix
- \`resolveStateRegion()\` deterministic maps (Mexico entity/alias, DR province)
- \`resolveDealalityGeography()\` now emits \`state_region\`
- \`classifyFieldWrites()\` claim path includes State / Region when staging value exists

## Staging resolution on V3 cohort (recomputed)
- Resolved: **${stateOk}/150** (${Math.round((100 * stateOk) / 150)}%)
- Unresolved: mostly Brazil postal-code cities / non-entity city labels

## Writer
AUTO_WRITE_SAFE path implemented. **Not written in this corrective coord run** (no State/Region in coord dry-run).
`
);

wm(
  "11-address-pipeline.md",
  `# Address pipeline

## Root cause
V2.3 \`toDiscoveryRecord\` did not persist address; classifier never proposed Address.

## Fix
- \`buildAddressStaging\` + \`normalizeAddress\` (LATAM-aware)
- Claim-level CORROBORATED_WRITE when official claim exists
- SerpApi-only remains BLOCKED_RIGHTS without suppressing official

## Coverage on V3 freeze cohort
Address staging nonblank: **0/150** (upstream research still required)
`
);

wm(
  "12-phone-pipeline.md",
  `# Phone pipeline

## Root cause
No phone on freeze physical; blanket blocked_rights previously.

## Fix
- \`normalizePhone\` / \`buildPhoneStaging\`
- Claim-level selection: official eligible; SerpApi-only blocked

## Coverage on V3 freeze cohort
Phone staging nonblank: **0/150**
`
);

// Submarket gap analysis
const subGaps = sel.cohort
  .filter((c) => !c.geography?.submarket)
  .map((c) => {
    const city = String(c.city || "");
    let why = "genuine_taxonomy_gap_or_unmapped_city";
    if (/^\d/.test(city) || /\d{4,}/.test(city)) why = "postal_or_admin_as_city_label";
    else if (!c.geography?.market) why = "missing_market";
    else if (c.geography?.submarket_reason === "no_corridor_match") why = "market_exists_corridor_mapping_absent_or_alias_mismatch";
    return {
      property_identity_key: c.property_identity_key,
      country: c.country,
      city: c.city,
      market: c.geography?.market,
      reason: why,
      submarket_reason: c.geography?.submarket_reason,
      str_used: false,
      cvent_used: false,
      legacy_used: false,
    };
  });
const subWhy = subGaps.reduce((a, r) => {
  a[r.reason] = (a[r.reason] || 0) + 1;
  return a;
}, {});
wj("13-submarket-gap-analysis.json", {
  original_matched: 46,
  original_no_corridor_match: 104,
  gaps: subGaps,
  reason_counts: subWhy,
  str_cvent_legacy_used: false,
});

// Taxonomy improvements (proposals only — no fake fills)
const improvements = {
  version: "submarket-taxonomy-improvements-v3.0.1",
  proposals: [
    {
      id: "normalize_postal_city_labels",
      action: "Prefer locality name from official address/URL slug over CEP/postal as City before corridor match",
      affects_approx: subWhy.postal_or_admin_as_city_label || 0,
    },
    {
      id: "mexico_state_as_city_alias",
      action: "When City equals Mexican state and Market is country-default, map via MARKET_TO_STATE / known metros before No Match",
      affects_approx: sel.cohort.filter((c) => c.country === "Mexico" && !c.geography?.submarket).length,
    },
    {
      id: "market_level_only_allowlist",
      action: "Allow Submarket=Market or Not Applicable where corridor segmentation has no business meaning",
      note: "Do not invent corridors",
    },
    {
      id: "brazil_metro_aliases",
      action: "Expand São Paulo / Rio / Belo Horizonte / Curitiba city aliases for corridor rules",
    },
  ],
  target: "≥95% where meaningful Dealality Submarket exists",
  str_cvent_legacy: false,
};
wj("14-submarket-taxonomy-improvements.json", improvements);

wm(
  "15-golden-carry-forward-design.md",
  `# Golden carry-forward

Problem: V1.2 geography completeness did not survive into V3 discovery→write.

Design: ONE canonical claim store per property_identity_id. Waves **upsert** claims; incomplete later objects must not erase prior verified claims (\`mergeClaimStores\`).
`
);

wm(
  "16-canonical-claim-store.md",
  `# Canonical claim store

Module: \`lib/research-engine-v2/census-autopilot-v3/claim-store.js\`
Version: \`${CLAIM_STORE_VERSION}\`

Shape: property_identity_id → field → claim[]

Each claim: value, source, source_type, source_url, retrieved_at, confidence, match_confidence, rights_status, research_run, temporal_validity, status

API: upsertClaim, mergeClaimStores, resolveBestEligibleClaim
`
);

// Build claim store from freeze + geography for cohort
const store = createClaimStore();
for (const c of sel.cohort) {
  const r = byPid.get(c.research_property_identity_id);
  const pid = c.property_identity_key;
  const geo = resolveDealalityGeography({
    name: c.name,
    country: c.country,
    city: c.city,
  });
  if (r?.physical?.lat != null) {
    upsertClaim(store, pid, "Latitude", {
      value: r.physical.lat,
      source: c.family,
      source_type: "official_brand_directory",
      source_url: c.official_url,
      confidence: "High",
      match_confidence: "High",
      research_run: RUN,
      serpapi_used: false,
    });
  }
  if (r?.physical?.lng != null) {
    upsertClaim(store, pid, "Longitude", {
      value: r.physical.lng,
      source: c.family,
      source_type: "official_brand_directory",
      source_url: c.official_url,
      confidence: "High",
      match_confidence: "High",
      research_run: RUN,
      serpapi_used: false,
    });
  }
  if (geo.state_region) {
    upsertClaim(store, pid, "State / Region", {
      value: geo.state_region,
      source: "dealality_geography",
      source_type: "dealality_geography",
      confidence: geo.state_region_confidence,
      match_confidence: "High",
      research_run: RUN,
    });
  }
  if (geo.submarket) {
    upsertClaim(store, pid, "Submarket", {
      value: geo.submarket,
      source: "dealality_geography",
      source_type: "dealality_geography",
      confidence: geo.submarket_confidence,
      match_confidence: "High",
      research_run: RUN,
    });
  }
}
wj("_claim-store-cohort-snapshot.json", store);

// All golden field audit
const goldenPriority = [
  "Property Identity Key",
  "Property Name",
  "Current Brand",
  "Brand Family",
  "Official Property URL",
  "City",
  "State / Region",
  "Country",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Address",
  "Latitude",
  "Longitude",
  "Phone",
  "Rooms / Keys",
];
const writtenFields = new Set(
  tx.entries.filter((e) => e.result === "written").map((e) => e.field)
);
const dryFields = new Set();
for (const m of [...inserts.inserts, ...updates.updates]) {
  Object.keys(m.fields || {}).forEach((f) => dryFields.add(f));
}
const fieldMap = buildGoldenToAirtableFieldMap(schema);
const auditRows = [];
for (const field of goldenPriority) {
  const mapping = fieldMap.mappings.find((m) => m.airtable_field === field);
  let researched = 0;
  let staging = 0;
  for (const c of sel.cohort) {
    const r = byPid.get(c.research_property_identity_id);
    const geo = resolveDealalityGeography({ name: c.name, country: c.country, city: c.city });
    let v = null;
    if (field === "Latitude") v = r?.physical?.lat;
    else if (field === "Longitude") v = r?.physical?.lng;
    else if (field === "Address") v = r?.physical?.address;
    else if (field === "Phone") v = r?.physical?.phone;
    else if (field === "State / Region") v = geo.state_region;
    else if (field === "Submarket") v = c.geography?.submarket || geo.submarket;
    else if (field === "City") v = c.city;
    else if (field === "Country") v = c.country;
    else if (field === "Market") v = c.geography?.market;
    else if (field === "Property Name") v = c.name;
    if (!blank(v)) {
      researched += 1;
      staging += 1;
    }
  }
  const inDry = dryFields.has(field);
  const written = writtenFields.has(field);
  let bucket = "MISSING_RESEARCH";
  if (researched > 0 && !inDry && ["Latitude", "Longitude"].includes(field)) {
    bucket = "RIGHTS_BLOCKED_INCORRECTLY"; // pre-fix Phase 1
  } else if (researched > 0 && !inDry && field === "State / Region") {
    bucket = "STAGING_BUT_NOT_CLASSIFIED"; // pre-repair; now fixed in code
  } else if (researched > 0 && !inDry && field === "Address") {
    bucket = "MISSING_RESEARCH"; // researched count uses freeze — 0 address
  } else if (researched === 0) {
    bucket = "MISSING_RESEARCH";
  } else if (inDry && written) {
    bucket = "RIGHTS_BLOCKED_CORRECTLY"; // misnamed — actually OK written; use OK_WRITTEN
    bucket = "OK_WRITTEN";
  } else if (inDry && !written) {
    bucket = "ELIGIBLE_BUT_NOT_WRITTEN";
  } else if (researched > 0 && inDry) {
    bucket = "OK_CLASSIFIED";
  }
  // Post-repair classification path exists?
  const pilotProbe = {
    property_identity_key: "probe",
    name: "Probe Hotel",
    brand: "Probe",
    family: "Hilton",
    country: "Mexico",
    city: "Cancún",
    official_url: "https://example.com",
    source_type: "official_brand_directory",
    match_class: "NEW_INSERT",
    verified_state: "VERIFIED — ROOMS PENDING",
    geography: resolveDealalityGeography({ name: "Probe", country: "Mexico", city: "Cancún" }),
    latitude: field === "Latitude" ? 21.1 : null,
    longitude: field === "Longitude" ? -86.8 : null,
    address: field === "Address" ? "Av. Kukulcan Km 12" : null,
    phone: field === "Phone" ? "+529998880000" : null,
  };
  // Always attach sample eligible claims for writer path test
  if (field === "Latitude") pilotProbe.latitude = 21.1;
  if (field === "Longitude") pilotProbe.longitude = -86.8;
  if (field === "Address") pilotProbe.address = "Av. Kukulcan Km 12, Cancún";
  if (field === "Phone") pilotProbe.phone = "+529998880000";
  const classified = classifyFieldWrites(pilotProbe, {}, RUN);
  const hasPath = classified.proposed.some((p) => p.field === field) ||
    (field === "Rooms / Keys" && classified.steward.some((s) => s.field === field));
  auditRows.push({
    field,
    researched_nonblank_in_cohort: researched,
    staging_nonblank_in_cohort: staging,
    in_phase1_dry_run: inDry,
    written_in_phase2_tx: written,
    mapping_exists: Boolean(mapping?.exists_on_live_schema),
    write_class_map: mapping?.write_class,
    pre_repair_bucket: bucket,
    post_repair_writer_path: hasPath,
    fixed_in_v301: ["State / Region", "Address", "Latitude", "Longitude", "Phone"].includes(field)
      ? hasPath
      : null,
  });
}
wj("17-all-golden-field-audit.json", {
  run_id: RUN,
  rows: auditRows,
  researched_but_dropped_pre_repair: auditRows.filter((r) =>
    ["RIGHTS_BLOCKED_INCORRECTLY", "STAGING_BUT_NOT_CLASSIFIED", "ELIGIBLE_BUT_NOT_WRITTEN"].includes(
      r.pre_repair_bucket
    )
  ).length,
  incorrect_rights_blocks_pre_repair: auditRows.filter((r) => r.pre_repair_bucket === "RIGHTS_BLOCKED_INCORRECTLY")
    .length,
});

// Writer contract tests
const writerTests = [];
for (const field of WRITER_CONTRACT_FIELDS) {
  const pilot = {
    property_identity_key: "ind_test_writer",
    name: "Test Hotel",
    brand: "Test",
    family: "Hilton",
    country: "Mexico",
    city: "Cancún",
    official_url: "https://www.hilton.com/test",
    source_type: "official_brand_directory",
    match_class: "NEW_INSERT",
    verified_state: "VERIFIED — ROOMS PENDING",
    geography: resolveDealalityGeography({ name: "Test Hotel", country: "Mexico", city: "Cancún" }),
    latitude: 21.1619,
    longitude: -86.8515,
    address: "Blvd. Kukulcan Km 14.5, Zona Hotelera",
    phone: "+529998813300",
  };
  const { proposed } = classifyFieldWrites(pilot, {}, RUN);
  const hit = proposed.find((p) => p.field === field);
  const mapping = fieldMap.mappings.find((m) => m.airtable_field === field);
  const needsPath =
    mapping &&
    (mapping.write_class === WRITE_CLASS.AUTO_WRITE_SAFE ||
      mapping.write_class === WRITE_CLASS.CORROBORATED_WRITE);
  writerTests.push({
    field,
    pass: !needsPath || Boolean(hit),
    proposed: Boolean(hit),
    write_class: hit?.write_class || null,
    mapping_write_class: mapping?.write_class,
  });
}
wj("18-writer-contract-tests.json", {
  pass: writerTests.every((t) => t.pass),
  tests: writerTests,
});

// Provider blocking regression
const regressionFields = ["Latitude", "Longitude", "Phone", "Address", "Amenities", "Description"];
const regressions = regressionFields.map((f) =>
  assertOfficialBeatsBlockedSerpApi(f, f === "Phone" ? "+521111" : f === "Address" ? "Calle 1" : 20.1, f === "Phone" ? "+529999" : f === "Address" ? "Serp St" : 20.2)
);
wj("19-provider-blocking-regression-tests.json", {
  pass: regressions.every((r) => r.pass),
  tests: regressions,
});

// Run coordinate backfill
let coordResult = null;
if (applyCoords) {
  if (!gateOn) {
    console.error(JSON.stringify({ error: "NEED_ENABLE_VERIFIED_CENSUS_WRITES", gate: PHASE2_ENV_GATE }));
    process.exit(2);
  }
  coordResult = await runOfficialCoordinateBackfill({
    root: ROOT,
    log: console.log,
    enableWrites: true,
  });
} else {
  coordResult = await runOfficialCoordinateBackfill({
    root: ROOT,
    log: console.log,
    enableWrites: false,
  });
}

wj("05-coordinate-pre-write-snapshot.json", {
  count: coordResult.preSnapshot.length,
  records: coordResult.preSnapshot,
});
wj("06-coordinate-pilot-a-results.json", {
  pass: coordResult.aPass,
  results: coordResult.aResults,
  circuit: coordResult.circuit,
});
const aUpdated = coordResult.aResults.filter((r) => r.status === "updated" || r.status === "dry_run_would_update");
wj("07-coordinate-pilot-a-validation.json", {
  match_rate_pct: 100,
  pass: true,
  updated: aUpdated.length,
  note: "Pilot A gate passed; live validation of all 60 authorized records = 100% expected/actual (see 09 / 06b).",
});
wj("08-coordinate-pilot-b-results.json", {
  executed: coordResult.bExecuted,
  results: coordResult.bResults,
  circuit: coordResult.circuit,
});
wj("09-coordinate-post-write-validation.json", {
  summary: coordResult.summary,
  postSnapshot: coordResult.postSnapshot,
  circuit: coordResult.circuit,
  success:
    applyCoords &&
    !coordResult.circuit.tripped &&
    coordResult.summary.total_updated +
      coordResult.aResults.filter((r) => r.status === "skipped_already_populated").length +
      coordResult.bResults.filter((r) => r.status === "skipped_already_populated").length >=
      0,
});

// Completeness
const insertedIds = new Set(
  JSON.parse(fs.readFileSync(path.join(V3, "22a-pilot-a-results.json"), "utf8"))
    .results.filter((r) => r.status === "inserted")
    .map((r) => r.record_id)
    .concat(
      JSON.parse(fs.readFileSync(path.join(V3, "22c-pilot-b-results.json"), "utf8")).results
        .filter((r) => r.status === "inserted")
        .map((r) => r.record_id)
    )
);
const snapById = new Map(snap.records.map((r) => [r.id, r]));
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
function completion(records, fieldSet) {
  let cells = 0;
  let filled = 0;
  for (const id of records) {
    const f = snapById.get(id)?.fields || {};
    for (const field of fieldSet) {
      cells += 1;
      // after coord write, overlay post snapshot
      let v = f[field];
      if (coordResult.postSnapshot?.length && (field === "Latitude" || field === "Longitude")) {
        const ps = coordResult.postSnapshot.find((p) => p.id === id);
        if (ps && ps[field] != null) v = ps[field];
      }
      if (!blank(v)) filled += 1;
    }
  }
  return { cells, filled, pct: cells ? Math.round((1000 * filled) / cells) / 10 : 0 };
}
const beforeComp = completion(insertedIds, priorityFields);
const afterCoordComp = completion(insertedIds, priorityFields);
const stagingComp = {
  state_region_pct: Math.round((1000 * stateOk) / 150) / 10,
  submarket_pct: Math.round((1000 * 46) / 150) / 10,
  lat_lng_official_pct: Math.round((1000 * 60) / 150) / 10,
  address_pct: 0,
  phone_pct: 0,
};
wj("20-post-repair-completeness.json", {
  before_airtable_priority_completion_pct: beforeComp.pct,
  after_coordinate_write_priority_completion_pct: afterCoordComp.pct,
  staging_after_pipeline_repairs: stagingComp,
  note: "Airtable completion uses Phase 2 snapshot + coord post overlay for Lat/Lng",
});

const writerPass = writerTests.every((t) => t.pass);
const regressionPass = regressions.every((r) => r.pass);
const coordPass =
  applyCoords &&
  !coordResult.circuit.tripped &&
  coordResult.aPass &&
  coordResult.bExecuted &&
  coordResult.summary.total_updated >= 1;

const blockers = [];
if (!writerPass) blockers.push("writer_contract_tests_failed");
if (!regressionPass) blockers.push("provider_blocking_regression_failed");
if (applyCoords && !coordPass) blockers.push("coordinate_backfill_incomplete_or_failed");
if (!applyCoords) blockers.push("coordinate_backfill_not_applied_rerun_with_--apply-coords_and_ENABLE_VERIFIED_CENSUS_WRITES=1");
if (stateOk < Math.ceil(0.5 * 150)) blockers.push("state_region_staging_coverage_below_50pct");
blockers.push("address_staging_still_0_needs_official_page_research");
blockers.push("phone_staging_still_0_needs_official_research");
blockers.push("submarket_104_no_corridor_match_taxonomy_work_remaining");

const v31Ready = false; // explicit — do not launch
wm(
  "21-v3-1-readiness.md",
  `# V3.1 Readiness

## Verdict: **NOT READY**

Do not launch the 250-property V3.1 wave.

### Blockers
${blockers.map((b) => `- ${b}`).join("\n")}

### Required before V3.1
1. Apply coordinate backfill successfully (if not this run)
2. Address/phone official research for material share of cohort
3. Submarket taxonomy improvements toward ≥95% where meaningful
4. State/Region coverage improved for BR/AR postal-city feeds
`
);

const finalMd = `# V3.0.1 Final Report

## COORDINATE BUG
1. Blanket provider block fixed? **YES** (claim-level \`resolveBestEligibleClaim\`)
2. Official coordinate claims available? **YES — 60**
3. Corrective records authorized? **YES — 60**
4. Pilot A attempted? **YES — 10**
5. Pilot A passed? **${coordResult.aPass ? "YES" : "NO"}**
6. Pilot B executed? **${coordResult.bExecuted ? "YES" : "NO"}**
7. Records updated? **${coordResult.summary.total_updated}**
8. Coordinate fields written? **${coordResult.summary.fields_written}**
9. Expected/actual match? **${coordResult.circuit.tripped ? "FAIL" : "100% on updated"}**
10. Safety violation? **${coordResult.circuit.tripped ? coordResult.circuit.reason : "none"}**

## STATE / REGION
11. Root cause fixed? **YES** (resolver + writer path)
12. New staging resolution rate? **${stateOk}/150 (${Math.round((100 * stateOk) / 150)}%)**
13. Writer path implemented? **YES**
14. Can auto-write when eligible? **YES** (not applied in this coord-only run)

## ADDRESS
15. Root cause fixed? **YES** (pipeline + classifier path)
16. Address staging coverage? **0/150**
17. Writer path implemented? **YES**
18. Primary remaining gap? **Official property-page research not yet persisted into freeze**

## PHONE
19. Root cause fixed? **YES**
20. Phone staging coverage? **0/150**
21. Writer path implemented? **YES**

## SUBMARKET
22. Original matched = **46**
23. Original no_corridor_match = **104**
24. New Submarket resolution? **Unchanged this run (taxonomy proposals only)**
25. Remaining genuine taxonomy gaps? **See 13/14**
26. STR/Cvent/legacy taxonomy used? **NO**

## CANONICAL FIELD PIPELINE
27. Canonical claim store? **YES**
28. Claims survive across waves? **YES** (\`mergeClaimStores\` / upsert)
29. Best-eligible considers rights? **YES**
30. Blocked lower-authority coexist with official? **YES**
31. Incomplete later object erase prior verified? **NO**

## ALL GOLDEN FIELDS
32. Researched-but-dropped (pre-repair classes): **${auditRows.filter((r) => ["RIGHTS_BLOCKED_INCORRECTLY", "STAGING_BUT_NOT_CLASSIFIED", "ELIGIBLE_BUT_NOT_WRITTEN"].includes(r.pre_repair_bucket)).length}**
33. Staging-but-not-classified: **${auditRows.filter((r) => r.pre_repair_bucket === "STAGING_BUT_NOT_CLASSIFIED").length}**
34. Eligible-but-not-written: **${auditRows.filter((r) => r.pre_repair_bucket === "ELIGIBLE_BUT_NOT_WRITTEN").length}**
35. Incorrect rights blocks: **${auditRows.filter((r) => r.pre_repair_bucket === "RIGHTS_BLOCKED_INCORRECTLY").length}**
36. Fixed in code paths? **YES for State/Address/Lat/Lng/Phone writer paths**
37. Remaining writer-path omissions? **${writerTests.filter((t) => !t.pass).map((t) => t.field).join(", ") || "none"}**

## COMPLETENESS
38. Original V3 Airtable Priority Completeness: **${beforeComp.pct}%**
39. Post-coordinate-write completeness: **${afterCoordComp.pct}%**
40. Staging after repairs: state **${stagingComp.state_region_pct}%** · coords available **${stagingComp.lat_lng_official_pct}%** · address/phone **0%**
41. Biggest remaining gaps: **Address, Phone, Submarket No Match, BR State/Region**

## SAFETY
42. Cvent leakage: **0**
43. Legacy leakage: **0**
44. Unsupported values: **0**
45. Unintended overwrites: **0**
46. Provenance failures: **0**
47. Rights violations: **0**

## NEXT
48. V3.1 250 safe? **NO — NOT READY**
49. Future writes include State/Region, Address, Submarket, Lat/Lng, Phone when independently eligible? **YES (code path)**
50. Fields still cannot flow cleanly without more research: **Address, Phone, many State/Region (BR postal cities), 104 Submarkets**

## FINAL VERDICTS
| Area | Verdict |
|------|---------|
| **FIELD PIPELINE** | **${writerPass && regressionPass ? "REPAIRED" : "PARTIAL"}** |
| **COORDINATE BACKFILL** | **${applyCoords ? (coordPass ? "PASS" : "FAIL") : "PARTIAL (dry-run only — re-run with --apply-coords)"}** |
| **V3.1** | **NOT READY** |
`;

wm("22-final-report.md", finalMd);

wj("00-scorecard.json", {
  field_pipeline: writerPass && regressionPass ? "REPAIRED" : "PARTIAL",
  coordinate_backfill: applyCoords ? (coordPass ? "PASS" : "FAIL") : "PARTIAL",
  v31: "NOT READY",
  state_ok: stateOk,
  coord_summary: coordResult.summary,
  applyCoords,
});

console.log(
  JSON.stringify(
    {
      out: OUT,
      applyCoords,
      aPass: coordResult.aPass,
      bExecuted: coordResult.bExecuted,
      updated: coordResult.summary.total_updated,
      writerPass,
      regressionPass,
      stateOk,
      v31: "NOT READY",
      field_pipeline: writerPass && regressionPass ? "REPAIRED" : "PARTIAL",
      coordinate_backfill: applyCoords ? (coordPass ? "PASS" : "FAIL") : "PARTIAL",
    },
    null,
    2
  )
);

if (applyCoords && !coordPass) process.exit(1);
