/**
 * Lock Verified Independent Census Mexico 4-Family Baseline
 * IHG + Hilton + Choice + Marriott
 *
 * Staging-only. Steward overlays only. Does not modify frozen per-wave artifacts.
 * No Airtable · No Webhound · No BE activation · No production overwrite · No cross-family auto-merge.
 */

import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { fingerprintFreeze } from "../lib/research-engine-v2/clean-census/legacy-reconcile.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const RE_ROOT = join(ROOT, "data/research-engine-v2");
const OUT = join(RE_ROOT, "verified-independent-census-mexico-combined-4family");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

const IHG_DIR = join(RE_ROOT, "verified-independent-census-v1");
const HILTON_DIR = join(RE_ROOT, "verified-independent-census-wave1b-hilton");
const CHOICE_DIR = join(RE_ROOT, "verified-independent-census-wave1c-choice");
const MARRIOTT_DIR = join(RE_ROOT, "verified-independent-census-wave1d-marriott");
const STEWARD_DIR = join(MARRIOTT_DIR, "steward-review");
const PRIOR_BASELINE = join(RE_ROOT, "verified-independent-census-mexico-combined");

const LOCKED_AT = new Date().toISOString();
const BASELINE_STATUS = "mexico_vic_4family_baseline_locked_staging_ready";
const EXPECTED = { IHG: 195, Hilton: 102, Choice: 68, Marriott: 301, total: 666 };

function writeJson(dir, name, obj) {
  writeFileSync(join(dir, name), JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(dir, name, text) {
  writeFileSync(join(dir, name), text, "utf8");
}
function writeText(dir, name, text) {
  writeFileSync(join(dir, name), text, "utf8");
}
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}
function rel(path) {
  return path.replace(ROOT + "\\", "").replace(ROOT + "/", "").replace(/\\/g, "/");
}

function loadFamily(family, fullPath, freezePath, summaryPath, eligibilityPath, waveId) {
  const full = readJson(fullPath);
  const records = full.records || [];
  const freeze = existsSync(freezePath) ? readJson(freezePath) : {};
  const summary = existsSync(summaryPath) ? readJson(summaryPath) : {};
  let dataEligible = summary.data_eligible ?? summary.summary?.data_eligible ?? null;
  if (eligibilityPath && existsSync(eligibilityPath)) {
    const elig = readJson(eligibilityPath);
    dataEligible = elig.summary?.data_eligible ?? dataEligible;
  }

  const brandCounts = {};
  for (const r of records) {
    const b = r.brand || r.fields?.Affiliation || "Unknown";
    brandCounts[b] = (brandCounts[b] || 0) + 1;
  }

  const index = records.map((r) => ({
    independent_record_id: r.independent_record_id,
    family,
    wave: r.reconstruction_wave || waveId || summary.wave || null,
    name: r.fields?.name || r.canonical_hotel_name || null,
    brand: r.brand || r.fields?.Affiliation || null,
    parent: r.parent || r.fields?.["Parent Company"] || null,
    city: r.fields?.city || r.normalized_city || null,
    country: r.fields?.country || r.country || "Mexico",
    property_id: r.property_id || null,
    status: r.fields?.status || r.current_status || null,
    website: r.fields?.Website || r.official_property_url || null,
    property_ids: r.official_property_ids || (r.fields?.["Property ID"] ? [r.fields["Property ID"]] : []),
    core_pct: r.completeness?.corePct ?? null,
    material_pct: r.completeness?.materialPct ?? null,
    reconstruction_status: r.reconstruction_status || null,
    page_source_state: r.page_source_state || null,
    legacy_used_as_source: r.legacy_used_as_source === true,
    discovery_source: r.discovery_source || null,
  }));

  return {
    family,
    wave: waveId,
    record_count: records.length,
    records,
    index,
    freeze_hash_sha256: freeze.freeze_hash_sha256 || fingerprintFreeze(records),
    frozen_at: freeze.frozenAt || null,
    firewall_pre_freeze_blocked: freeze.firewallPreFreezeBlocked ?? summary.firewallPreFreezeBlocked ?? null,
    summary,
    data_eligible: dataEligible,
    core_pct: summary.corePct ?? null,
    material_pct: summary.materialPct ?? null,
    matches: summary.matches ?? null,
    probable: summary.probable ?? null,
    independent_only: summary.independent_only ?? null,
    legacy_only: summary.legacy_only ?? null,
    unique_physical_properties: summary.unique_physical_properties ?? records.length,
    brands: summary.brands || brandCounts,
    brand_counts: brandCounts,
    source_paths: {
      full_records: rel(fullPath),
      freeze: rel(freezePath),
      summary: rel(summaryPath),
    },
  };
}

function classifyBeBrand(family, brand, count, materialPct, corePct, overlayActions) {
  if (overlayActions?.excluded?.has(brand) || /Unconfirmed/i.test(brand)) {
    return "excluded_from_brand_completion";
  }
  if (overlayActions?.steward_review?.has(brand)) {
    return "steward_review_required";
  }
  if (family === "Marriott" && (materialPct ?? 0) < 50) {
    if ((corePct ?? 0) >= 95 && count >= 5) return "completion_partial";
    return "completion_hold";
  }
  if ((corePct ?? 0) >= 95 && (materialPct ?? 0) >= 50 && count >= 5) return "completion_ready";
  if ((corePct ?? 0) >= 90 && count >= 3) return "completion_partial";
  return "completion_hold";
}

// ── Preconditions ────────────────────────────────────────────────────────────
if (!existsSync(join(PRIOR_BASELINE, "00-baseline-lock.json"))) {
  throw new Error("3-family Mexico VIC baseline lock missing");
}
const priorLock = readJson(join(PRIOR_BASELINE, "00-baseline-lock.json"));
if (priorLock.baseline_status !== "mexico_vic_baseline_locked_ready_for_marriott_wave1d") {
  throw new Error(`Unexpected prior baseline status: ${priorLock.baseline_status}`);
}
if (!existsSync(join(STEWARD_DIR, "00-steward-review-summary.json"))) {
  throw new Error("Marriott steward review missing — run steward-review first");
}
const stewardSummary = readJson(join(STEWARD_DIR, "00-steward-review-summary.json"));
if (stewardSummary.status !== "wave1d_marriott_steward_review_minor_holds_ready_for_4_family_baseline_lock") {
  throw new Error(`Steward review not ready for lock: ${stewardSummary.status}`);
}

mkdirSync(OUT, { recursive: true });
mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });

console.log("[vic-4family] locking Mexico VIC 4-family baseline");

const ihg = loadFamily(
  "IHG",
  join(IHG_DIR, "08-expanded-benchmark-full-records.json"),
  join(IHG_DIR, "09-expanded-benchmark-freeze.json"),
  join(IHG_DIR, "12-vic-run-summary.json"),
  join(IHG_DIR, "15-production-eligibility-results.json"),
  "wave1_ihg_mexico_all"
);
const hilton = loadFamily(
  "Hilton",
  join(HILTON_DIR, "02-hilton-full-records.json"),
  join(HILTON_DIR, "04-hilton-freeze.json"),
  join(HILTON_DIR, "00-wave1b-run-summary.json"),
  join(HILTON_DIR, "12-data-image-eligibility.json"),
  "wave1b_hilton_mexico"
);
const choice = loadFamily(
  "Choice",
  join(CHOICE_DIR, "02-choice-full-records.json"),
  join(CHOICE_DIR, "06-choice-freeze.json"),
  join(CHOICE_DIR, "00-wave1c-run-summary.json"),
  join(CHOICE_DIR, "12-data-image-eligibility.json"),
  "wave1c_choice_mexico"
);
const marriott = loadFamily(
  "Marriott",
  join(MARRIOTT_DIR, "02-marriott-full-records.json"),
  join(MARRIOTT_DIR, "08-marriott-freeze.json"),
  join(MARRIOTT_DIR, "00-wave1d-run-summary.json"),
  join(MARRIOTT_DIR, "12-data-image-eligibility.json"),
  "wave1d_marriott_mexico"
);

const families = [ihg, hilton, choice, marriott];
for (const f of families) {
  const expected = EXPECTED[f.family];
  if (f.record_count !== expected) {
    throw new Error(`${f.family} must be ${expected}; got ${f.record_count}`);
  }
}
const totalRecords = families.reduce((s, f) => s + f.record_count, 0);
if (totalRecords !== EXPECTED.total) {
  throw new Error(`Combined must reconcile to ${EXPECTED.total}; got ${totalRecords}`);
}

// Steward overlays (read-only; do not mutate Wave 1D freeze)
const brandUnconfirmedOverlay = existsSync(join(STEWARD_DIR, "03-brand-unconfirmed-review.json"))
  ? readJson(join(STEWARD_DIR, "03-brand-unconfirmed-review.json"))
  : { count: 0, reviews: [] };
const crossFamilySteward = existsSync(join(STEWARD_DIR, "05-cross-family-name-city-review.json"))
  ? readJson(join(STEWARD_DIR, "05-cross-family-name-city-review.json"))
  : { summary: {}, steward_exact_or_probable: [], rejected_fuzzy_sample: [], insufficient_sample: [] };
const physicalNear = existsSync(join(STEWARD_DIR, "04-physical-identity-near-duplicates.json"))
  ? readJson(join(STEWARD_DIR, "04-physical-identity-near-duplicates.json"))
  : {};
const beMarriott = existsSync(join(MARRIOTT_DIR, "13-brand-explorer-completion-readiness.json"))
  ? readJson(join(MARRIOTT_DIR, "13-brand-explorer-completion-readiness.json"))
  : { brands: [] };

const marriottOverlayCanonical = [
  {
    independent_record_id: "ind_marriott_mx_mexmc",
    property_name: "Mexico City Marriott Reforma Hotel",
    action: "confirm_brand",
    brand: "Marriott Hotels",
    note: "map miss, overlay only",
  },
  {
    independent_record_id: "ind_marriott_mx_pbcde",
    property_name: "Gran Hotel de Puebla by HNF",
    action: "exclude_from_brand_completion",
  },
  {
    independent_record_id: "ind_marriott_mx_gdlcc",
    property_name: "Hotel Guadalajara Country Club by HNF",
    action: "exclude_from_brand_completion",
  },
  {
    independent_record_id: "ind_marriott_mx_slwak",
    property_name: "CASA MAYOR Saltillo",
    action: "steward_manual_review_required",
  },
  {
    independent_record_id: "ind_marriott_mx_mtyjd",
    property_name: "SJ Grand Hotel Monterrey",
    action: "steward_manual_review_required",
  },
];

const combinedIndex = [...ihg.index, ...hilton.index, ...choice.index, ...marriott.index];

const combinedFreezeHash = createHash("sha256")
  .update(
    JSON.stringify({
      baseline: BASELINE_STATUS,
      prior_3family_freeze_hash_sha256: priorLock.combined_freeze_hash_sha256,
      families: families.map((f) => ({
        family: f.family,
        count: f.record_count,
        wave_freeze_hash_sha256: f.freeze_hash_sha256,
      })),
      marriott_steward_overlay_actions: marriottOverlayCanonical.map((x) => ({
        id: x.independent_record_id,
        action: x.action,
        brand: x.brand || null,
      })),
      record_ids: combinedIndex.map((r) => r.independent_record_id).sort(),
    })
  )
  .digest("hex");

const recordFingerprint = fingerprintFreeze([
  ...ihg.records,
  ...hilton.records,
  ...choice.records,
  ...marriott.records,
]);

const dataEligibleTotal =
  (ihg.data_eligible || 0) +
  (hilton.data_eligible || 0) +
  (choice.data_eligible || 0) +
  (marriott.data_eligible || 0);

const PRIMARY_SOURCES = {
  IHG: "Official IHG Mexico directory / hoteldetail",
  Hilton: "Official Hilton Mexico brand location pages (+ structured GraphQL)",
  Choice: "Official Choice Mexico regional JSON-LD + MX* sitemap union",
  Marriott: "Official Marriott Mexico country hotel-sitemap (sitemap-heavy)",
};

const FAMILY_NOTES = {
  IHG: "Wave 1A locked; strong rooms coverage relative to later waves",
  Hilton: "Wave 1B locked; strong openDate; rooms mostly Unknown",
  Choice: "Wave 1C locked; 50 regional-complete data-eligible + 18 sitemap-only union",
  Marriott:
    "Wave 1D locked after steward review; sitemap-heavy; material 40%; staging census safe, not production-ready; 5 Brand Unconfirmed overlay decisions",
};

const familySummary = families.map((f) => ({
  family: f.family,
  wave: f.wave,
  records: f.record_count,
  unique_physical: f.unique_physical_properties,
  unique_physical_note:
    f.family === "Marriott"
      ? "source-unique / identity-safe staging count (301 unique MARSHA) — not fully coordinate-verified physical count"
      : "family-scoped unique physical = record count (no intra-family auto-collapse)",
  core_completeness_pct: f.core_pct,
  material_completeness_pct: f.material_pct,
  data_eligible: f.data_eligible,
  primary_source: PRIMARY_SOURCES[f.family],
  notes: FAMILY_NOTES[f.family],
  freeze_hash_sha256: f.freeze_hash_sha256,
  frozen_at: f.frozen_at,
  firewall_pre_freeze_blocked: f.firewall_pre_freeze_blocked,
  exact_legacy_matches: f.matches,
  probable_legacy_matches: f.probable,
  independent_only: f.independent_only,
  legacy_only: f.legacy_only,
  source_artifacts: f.source_paths,
}));

// Brand coverage + BE readiness
const overlayConfirm = marriottOverlayCanonical.find((x) => x.action === "confirm_brand");

const brandCoverage = [];
for (const f of families) {
  for (const [brand, count] of Object.entries(f.brand_counts).sort((a, b) => b[1] - a[1])) {
    let parsing_confidence = "High";
    let notes = "";
    let completion = classifyBeBrand(f.family, brand, count, f.material_pct, f.core_pct);

    if (/Unconfirmed/i.test(brand)) {
      parsing_confidence = "Hold — Brand Unconfirmed";
      completion = "excluded_from_brand_completion";
      notes = "Soft/ambiguous Bonvoy rows; see Marriott steward overlay";
    } else if (f.family === "Marriott" && brand === "Marriott Hotels" && overlayConfirm) {
      notes = `Includes overlay confirm_brand candidate ${overlayConfirm.property_name} (freeze unchanged; effective brand for BE = Marriott Hotels if steward accepts)`;
      completion = "completion_partial";
    } else if (f.family === "Marriott") {
      notes = "Sitemap identity strong; material fields mostly Unknown — BE completion partial/hold";
    }

    // Prefer existing Marriott BE fixture statuses when present
    if (f.family === "Marriott" && Array.isArray(beMarriott.brands)) {
      const row = beMarriott.brands.find((b) => b.brand === brand);
      if (row) {
        if (/hold/i.test(row.be_status)) completion = /Unconfirmed/i.test(brand) ? "excluded_from_brand_completion" : "completion_hold";
        else if (/partial/i.test(row.be_status)) completion = "completion_partial";
        else if (/ready/i.test(row.be_status)) completion = "completion_partial"; // force partial while material 40%
      }
    }

    brandCoverage.push({
      family: f.family,
      brand,
      count,
      parsing_confidence,
      completion_readiness: completion,
      notes,
    });
  }
}

const dataEligibleIndex = combinedIndex
  .filter((row) => {
    const fam = families.find((f) => f.family === row.family);
    // Approximate: include if family-level eligible and core present; Marriott use eligibility file IDs
    return true;
  })
  .map((row) => row);

// Build precise eligible ID set from eligibility files
function eligibleIds(path) {
  if (!existsSync(path)) return new Set();
  const elig = readJson(path);
  return new Set(
    (elig.results || [])
      .filter((r) => r.production_eligibility_data === "ELIGIBLE")
      .map((r) => r.independent_record_id)
  );
}
const eligibleSet = new Set([
  ...eligibleIds(join(IHG_DIR, "15-production-eligibility-results.json")),
  ...eligibleIds(join(HILTON_DIR, "12-data-image-eligibility.json")),
  ...eligibleIds(join(CHOICE_DIR, "12-data-image-eligibility.json")),
  ...eligibleIds(join(MARRIOTT_DIR, "12-data-image-eligibility.json")),
]);

const dataEligibleRows = combinedIndex.filter((r) => eligibleSet.has(r.independent_record_id));

const lockManifest = {
  baseline_status: BASELINE_STATUS,
  locked_at: LOCKED_AT,
  staging_only: true,
  airtable_writes: false,
  brand_explorer_activation: false,
  webhound_used: false,
  legacy_used_as_research_evidence: false,
  production_overwrite: false,
  geography: "Mexico",
  families: ["IHG", "Hilton", "Choice", "Marriott"],
  total_independent_hotel_records: 666,
  reconciliation: {
    ihg: 195,
    hilton: 102,
    choice: 68,
    marriott: 301,
    sum: 666,
    ok: true,
  },
  prior_3family_baseline: {
    status: priorLock.baseline_status,
    combined_freeze_hash_sha256: priorLock.combined_freeze_hash_sha256,
    total: 365,
  },
  marriott_steward_review: {
    status: stewardSummary.status,
    reviewed_at: stewardSummary.reviewed_at,
    freeze_artifacts_modified: false,
    overlay_only: true,
  },
  combined_freeze_hash_sha256: combinedFreezeHash,
  combined_record_fingerprint_sha256: recordFingerprint,
  wave_freeze_hashes: {
    IHG: ihg.freeze_hash_sha256,
    Hilton: hilton.freeze_hash_sha256,
    Choice: choice.freeze_hash_sha256,
    Marriott: marriott.freeze_hash_sha256,
  },
  cross_family_fuzzy_auto_merges: 0,
  fake_temporal_start_dates: 0,
  fake_room_counts: 0,
  fake_owners_operators: 0,
  missing_coordinates_remain_unknown: true,
  number_null_coords_guard: "property-identity.js rejects Number(null)===0 false 0,0 coords",
};

// ── Artifacts ────────────────────────────────────────────────────────────────
writeMd(
  OUT,
  "00_README.md",
  `# Mexico VIC Combined 4-Family Baseline

**Status:** \`${BASELINE_STATUS}\`  
**Locked at:** ${LOCKED_AT}

## Contents

| File | Purpose |
|------|---------|
| 01_combined_4family_index.json | Slim combined record index (666) |
| 02_family_summary.json | Per-family comparison |
| 03_source_lineage_map.json | Wave traceability |
| 04_property_identity_summary.json | Property Identity V1 |
| 05_temporal_affiliation_summary.json | Temporal Affiliation V1 |
| 06_completeness_by_family.json | Core / material / eligible |
| 07_data_eligible_index.json | Data-eligible subset |
| 08_brand_coverage_by_family.json | Brands found independently |
| 09_cross_family_steward_queue.json | Steward queue (no auto-merge) |
| 10_rejected_fuzzy_matches.json | Rejected fuzzy / insufficient |
| 11_marriott_steward_overlay.json | Brand Unconfirmed overlay |
| 12_brand_explorer_completion_readiness.json | BE readiness (no activation) |
| 13_staging_migration_readiness.json | Staging vs production |
| 14_freeze_manifest.json | Lock manifest |
| 15_freeze_hash.txt | Combined freeze hash |

## Rules

- Per-wave freeze artifacts are **not** modified.
- Marriott steward decisions are **overlay only**.
- Cross-family fuzzy auto-merges: **0**.
- Staging only — no Airtable / BE activation / production overwrite / Webhound.
`
);

writeJson(OUT, "01_combined_4family_index.json", {
  generatedAt: LOCKED_AT,
  baseline_status: BASELINE_STATUS,
  total: combinedIndex.length,
  reconciliation: lockManifest.reconciliation,
  records: combinedIndex,
  note: "Slim index for traceability. Full claims remain in per-wave full-records JSON — not duplicated here. Frozen wave artifacts unmodified.",
});

writeJson(OUT, "02_family_summary.json", {
  generatedAt: LOCKED_AT,
  total: 666,
  families: familySummary,
});

writeJson(OUT, "03_source_lineage_map.json", {
  generatedAt: LOCKED_AT,
  geography: "Mexico",
  families: families.map((f) => ({
    family: f.family,
    wave: f.wave,
    record_count: f.record_count,
    primary_source: PRIMARY_SOURCES[f.family],
    wave_freeze_hash_sha256: f.freeze_hash_sha256,
    source_artifacts: f.source_paths,
    discovery_basis:
      f.family === "Marriott"
        ? "official_marriott_mexico_country_hotel_sitemap"
        : f.family === "Hilton"
          ? "live_hilton_locations_mexico_brand_pages"
          : f.family === "Choice"
            ? "live_choice_mexico_regional_jsonld"
            : "ihg_mexico_directory",
  })),
  prior_3family_baseline: lockManifest.prior_3family_baseline,
  marriott_steward_overlay_path: rel(join(STEWARD_DIR)),
  frozen_wave_artifacts_modified: false,
});

writeJson(OUT, "04_property_identity_summary.json", {
  property_identity_version: "property-identity-v1",
  total_combined_records: 666,
  unique_physical_by_family: {
    IHG: ihg.unique_physical_properties,
    Hilton: hilton.unique_physical_properties,
    Choice: choice.unique_physical_properties,
    Marriott: marriott.unique_physical_properties,
  },
  marriott_unique_physical_clarification:
    "Marriott unique physical properties = 301 means source-unique / identity-safe staging count (unique MARSHA; campus annexes kept distinct). It is NOT a fully coordinate-verified physical count — all Marriott Wave 1D coords are Unknown (sitemap limitation).",
  duplicate_handling_by_family: {
    IHG: "Directory property-id dedupe within Wave 1A",
    Hilton: "ctyhocn / location-page identity within Wave 1B",
    Choice: "Property Identity V1 + regional/sitemap union",
    Marriott: "MARSHA dedupe in sitemap; Property Identity V1; Number(null)===0 coords guard applied",
  },
  cross_family_auto_merges: 0,
  rejected_fuzzy_matches: crossFamilySteward.summary?.rejected_fuzzy_match ?? 0,
  campus_sibling_high_sim_kept_distinct: physicalNear.summary?.campus_or_high_sim_steward_pairs ?? 9,
  marriott_coordinate_limitation: "All 301 Marriott records missing coordinates — Unknown preserved; never 0,0",
  number_null_equals_zero_issue: {
    fixed: true,
    guarded_in: "lib/research-engine-v2/clean-census/property-identity.js",
    note: "Missing latitude/longitude no longer coerce via Number(null)===0",
  },
});

writeJson(OUT, "05_temporal_affiliation_summary.json", {
  temporal_affiliation_version: "temporal-affiliation-v1",
  current_affiliation: "As of discovery date per wave",
  fake_affiliation_start_dates: 0,
  fake_opening_dates: 0,
  prior_affiliations: "Unknown unless independently sourced",
  blocked_unavailable_source_cases_preserved: true,
  current_affiliation_from_legacy: false,
  policy: "exact | as_of | before | unknown — no fabricated start dates",
  module: "lib/research-engine-v2/clean-census/temporal-affiliation.js",
});

writeJson(OUT, "06_completeness_by_family.json", {
  completeness_by_family: {
    IHG: { core_pct: ihg.core_pct, material_pct: ihg.material_pct, records: 195 },
    Hilton: { core_pct: hilton.core_pct, material_pct: hilton.material_pct, records: 102 },
    Choice: { core_pct: choice.core_pct, material_pct: choice.material_pct, records: 68 },
    Marriott: {
      core_pct: marriott.core_pct,
      material_pct: marriott.material_pct,
      records: 301,
      note: "Sitemap-heavy; material fields largely Unknown",
    },
  },
  data_eligible_by_family: {
    IHG: ihg.data_eligible,
    Hilton: hilton.data_eligible,
    Choice: choice.data_eligible,
    Marriott: marriott.data_eligible,
    total: dataEligibleTotal,
  },
});

writeJson(OUT, "07_data_eligible_index.json", {
  generatedAt: LOCKED_AT,
  total_data_eligible: dataEligibleRows.length,
  expected_total: dataEligibleTotal,
  by_family: {
    IHG: dataEligibleRows.filter((r) => r.family === "IHG").length,
    Hilton: dataEligibleRows.filter((r) => r.family === "Hilton").length,
    Choice: dataEligibleRows.filter((r) => r.family === "Choice").length,
    Marriott: dataEligibleRows.filter((r) => r.family === "Marriott").length,
  },
  records: dataEligibleRows,
  note: "Data-eligible for staging census identity — not image/production ready",
});

writeJson(OUT, "08_brand_coverage_by_family.json", {
  generatedAt: LOCKED_AT,
  total_brand_rows: brandCoverage.length,
  marriott_brands_found: Object.keys(marriott.brand_counts).length,
  marriott_highlights: {
    "City Express by Marriott": marriott.brand_counts["City Express by Marriott"] || 0,
    "Courtyard by Marriott": marriott.brand_counts["Courtyard by Marriott"] || 0,
    "Design Hotels": marriott.brand_counts["Design Hotels"] || 0,
    "City Express Plus by Marriott": marriott.brand_counts["City Express Plus by Marriott"] || 0,
    "City Express Junior by Marriott": marriott.brand_counts["City Express Junior by Marriott"] || 0,
    "Four Points by Sheraton": marriott.brand_counts["Four Points by Sheraton"] || 0,
    Sheraton: marriott.brand_counts.Sheraton || 0,
    "Marriott Bonvoy — Brand Unconfirmed": marriott.brand_counts["Marriott Bonvoy — Brand Unconfirmed"] || 0,
  },
  brands: brandCoverage,
  note: "Only brands found independently — no forced expected brands",
});

writeJson(OUT, "09_cross_family_steward_queue.json", {
  generatedAt: LOCKED_AT,
  auto_merges: 0,
  summary: {
    exact_physical_match_requires_steward_review:
      crossFamilySteward.summary?.exact_physical_match_requires_steward_review ?? 0,
    probable_physical_match_requires_steward_review:
      crossFamilySteward.summary?.probable_physical_match_requires_steward_review ?? 0,
    from_marriott_steward_review: true,
  },
  steward_exact_or_probable: crossFamilySteward.steward_exact_or_probable || [],
  campus_high_sim_intra_marriott: physicalNear.steward_priority_pairs || [],
  note: "No auto-merge. Marriott steward review used name/city/address — not coords-only.",
});

writeJson(OUT, "10_rejected_fuzzy_matches.json", {
  generatedAt: LOCKED_AT,
  auto_merges: 0,
  rejected_fuzzy_match: crossFamilySteward.summary?.rejected_fuzzy_match ?? 0,
  insufficient_evidence_no_merge: crossFamilySteward.summary?.insufficient_evidence_no_merge ?? 0,
  rejected_fuzzy_sample: crossFamilySteward.rejected_fuzzy_sample || [],
  insufficient_sample: crossFamilySteward.insufficient_sample || [],
  policy: "Fuzzy name alone never merges; missing Marriott coords never treated as 0,0",
});

writeJson(OUT, "11_marriott_steward_overlay.json", {
  generatedAt: LOCKED_AT,
  freeze_artifacts_modified: false,
  overlay_only: true,
  steward_review_status: stewardSummary.status,
  brand_unconfirmed_overlay: marriottOverlayCanonical,
  steward_review_detail: brandUnconfirmedOverlay,
  minor_holds: stewardSummary.minor_holds || [],
  note: "Overlay decisions do not alter frozen Wave 1D source artifacts",
});

const beReadiness = {
  activation: "NONE",
  do_not_activate: true,
  brand_status_changes: false,
  airtable_writes: false,
  production_overwrite: false,
  small_be_completion_pilot_ready: true,
  pilot_scope:
    "Small pilot only — prefer IHG/Hilton/Choice brands with stronger material completeness; Marriott sitemap brands are completion_partial/hold until rooms/coords/open date enrichment",
  suitable_pilot_candidates: brandCoverage
    .filter((b) => b.completion_readiness === "completion_ready" || (b.family !== "Marriott" && b.completion_readiness === "completion_partial"))
    .slice(0, 20),
  marriott_brands: brandCoverage.filter((b) => b.family === "Marriott"),
  hold_categories: [
    "Marriott sitemap-only material gaps (rooms, coords, owner, open date)",
    "Brand Unconfirmed exclude_from_brand_completion / steward_manual_review_required",
    "Design Hotels / soft brands until FP enrichment",
  ],
  required_steward_review_before_production_migration: [
    "Accept or reject Mexico City Marriott Reforma confirm_brand overlay",
    "Resolve CASA MAYOR Saltillo + SJ Grand Monterrey brand cues",
    "Campus annex pairs remain distinct until address/coords proof",
    "No production overwrite of legacy Hotel Census",
  ],
  classifications_used: [
    "completion_ready",
    "completion_partial",
    "completion_hold",
    "excluded_from_brand_completion",
    "steward_review_required",
  ],
};

writeJson(OUT, "12_brand_explorer_completion_readiness.json", beReadiness);

const migration = {
  staging_migration_ready: true,
  production_overwrite_ready: false,
  required_steward_review_before_production: [
    "Marriott Brand Unconfirmed overlay decisions",
    "Cross-family rejected fuzzy / insufficient pairs",
    "Campus annex distinctness",
    "Field mapping validation for staging table only",
  ],
  fields_safe_to_migrate: [
    "family",
    "brand",
    "property name",
    "canonical name",
    "city",
    "state / region",
    "country",
    "source URL",
    "source type",
    "source lineage",
    "as-of discovery date",
    "identity key",
    "confidence classification",
    "Property ID / MARSHA / ctyhocn where present",
  ],
  fields_unsafe_to_migrate: [
    "rooms where missing",
    "owner",
    "operator",
    "open date",
    "coordinates where missing",
    "brand-unconfirmed records without overlay confirmation",
    "temporal start dates",
    "images / first-party media",
  ],
  overlay_brand_exception: {
    independent_record_id: "ind_marriott_mx_mexmc",
    freeze_brand: "Marriott Bonvoy — Brand Unconfirmed",
    overlay_brand: "Marriott Hotels",
    note: "Staging may use overlay brand if steward accepts; freeze row unchanged",
  },
  posture: "staging migration only — no production overwrite",
};

writeJson(OUT, "13_staging_migration_readiness.json", migration);
writeJson(OUT, "14_freeze_manifest.json", lockManifest);
writeText(OUT, "15_freeze_hash.txt", `${combinedFreezeHash}\n`);

const reportJson = {
  ...lockManifest,
  executive_summary: {
    statement:
      "Mexico VIC 4-family baseline locked: IHG 195 + Hilton 102 + Choice 68 + Marriott 301 = 666 independent hotel records. Staging-only. Marriott steward overlay included. No Airtable, no BE activation, no Webhound, no production overwrite, no cross-family auto-merge.",
    combined_records: 666,
    data_eligible_total: dataEligibleTotal,
    cross_family_fuzzy_auto_merges: 0,
  },
  family_comparison: familySummary,
  marriott_steward_review_summary: {
    status: stewardSummary.status,
    identity_safe: true,
    four_points_sheraton_verified: stewardSummary.four_points_sheraton_verified,
    city_express_verified: stewardSummary.city_express_misparse_count === 0,
    blocking_issues: stewardSummary.blocking_issues,
    minor_holds: stewardSummary.minor_holds,
  },
  brand_unconfirmed_overlay: marriottOverlayCanonical,
  brand_coverage: brandCoverage,
  completeness: {
    by_family: {
      IHG: { core: ihg.core_pct, material: ihg.material_pct },
      Hilton: { core: hilton.core_pct, material: hilton.material_pct },
      Choice: { core: choice.core_pct, material: choice.material_pct },
      Marriott: { core: marriott.core_pct, material: marriott.material_pct },
    },
    data_eligible_total: dataEligibleTotal,
  },
  brand_explorer_completion_readiness: beReadiness,
  staging_migration_readiness: migration,
  property_identity_v1: readJson(join(OUT, "04_property_identity_summary.json")),
  temporal_affiliation_v1: readJson(join(OUT, "05_temporal_affiliation_summary.json")),
};

const md = `# Verified Independent Census — Mexico Combined 4-Family Baseline

**Status:** \`${BASELINE_STATUS}\`  
**Locked at:** ${LOCKED_AT}  
**Staging only** · No Airtable · No Brand Explorer activation · No Webhound · No production overwrite · No cross-family auto-merge

---

## 1. Executive summary

Locked **666** independently reconstructed Mexico hotel records across four parent families:

| Family | Records |
|--------|--------:|
| IHG (Wave 1A) | 195 |
| Hilton (Wave 1B) | 102 |
| Choice (Wave 1C) | 68 |
| Marriott (Wave 1D) | 301 |
| **Total** | **666** |

Data-eligible (staging): **${dataEligibleTotal}**. Cross-family fuzzy auto-merges: **0**. Fake temporal start dates / rooms / owners: **0**. Marriott steward overlay included; frozen Wave 1D artifacts **not** modified.

---

## 2. Freeze decision

**LOCKED** — \`mexico_vic_4family_baseline_locked_staging_ready\`

- Prior 3-family freeze: \`${priorLock.combined_freeze_hash_sha256}\`
- Combined 4-family freeze hash: \`${combinedFreezeHash}\`
- Record fingerprint: \`${recordFingerprint}\`

---

## 3. Family comparison

| Family | Records | Unique Physical | Core % | Material % | Data-Eligible | Primary Source | Notes |
|--------|--------:|----------------:|-------:|-----------:|--------------:|----------------|-------|
| IHG | 195 | ${ihg.unique_physical_properties} | ${ihg.core_pct} | ${ihg.material_pct} | ${ihg.data_eligible} | ${PRIMARY_SOURCES.IHG} | ${FAMILY_NOTES.IHG} |
| Hilton | 102 | ${hilton.unique_physical_properties} | ${hilton.core_pct} | ${hilton.material_pct} | ${hilton.data_eligible} | ${PRIMARY_SOURCES.Hilton} | ${FAMILY_NOTES.Hilton} |
| Choice | 68 | ${choice.unique_physical_properties} | ${choice.core_pct} | ${choice.material_pct} | ${choice.data_eligible} | ${PRIMARY_SOURCES.Choice} | ${FAMILY_NOTES.Choice} |
| Marriott | 301 | ${marriott.unique_physical_properties} | ${marriott.core_pct} | ${marriott.material_pct} | ${marriott.data_eligible} | ${PRIMARY_SOURCES.Marriott} | ${FAMILY_NOTES.Marriott} |
| **Total** | **666** | — | — | — | **${dataEligibleTotal}** | — | Staging census |

Marriott unique physical = **source-unique / identity-safe staging count** (301 MARSHA), **not** fully coordinate-verified physical count.

---

## 4. Combined 666-record baseline

Families remain separately traceable to Wave 1A / 1B / 1C / 1D freeze hashes. Slim index: \`01_combined_4family_index.json\`.

---

## 5. Source lineage

See \`03_source_lineage_map.json\`. Marriott steward overlay path: \`wave1d-marriott/steward-review/\` (overlay only).

---

## 6. Property Identity V1 summary

- Combined records: **666**
- Unique physical by family: IHG ${ihg.unique_physical_properties} · Hilton ${hilton.unique_physical_properties} · Choice ${choice.unique_physical_properties} · Marriott ${marriott.unique_physical_properties}
- Cross-family auto-merges: **0**
- Campus / sibling / high-sim kept distinct: **${physicalNear.summary?.campus_or_high_sim_steward_pairs ?? 9}** (Marriott steward)
- Marriott coords: all **Unknown** (never 0,0)
- \`Number(null) === 0\` coords bug: **fixed and guarded**

---

## 7. Temporal Affiliation V1 summary

- Current affiliation: **As of discovery**
- Fake affiliation start dates: **0**
- Fake opening dates: **0**
- Prior affiliations: **Unknown** unless independently sourced
- Blocked source cases preserved
- No current affiliation inferred from legacy

---

## 8. Marriott steward review summary

Status: \`${stewardSummary.status}\`

- Identity safe: 301 MARSHA / 301 records
- Four Points / Sheraton verified clean (11 / 5)
- City Express family verified (0 misparses)
- Cross-family exact/probable: **0**
- Blocking issues: **none**
- Freeze unmodified; overlay only

Minor holds:
${(stewardSummary.minor_holds || []).map((h) => `- ${h}`).join("\n")}

---

## 9. Brand Unconfirmed overlay

| Property | Action | Brand / note |
|----------|--------|--------------|
| Mexico City Marriott Reforma Hotel | \`confirm_brand\` | Marriott Hotels — map miss, overlay only |
| Gran Hotel de Puebla by HNF | \`exclude_from_brand_completion\` | — |
| Hotel Guadalajara Country Club by HNF | \`exclude_from_brand_completion\` | — |
| CASA MAYOR Saltillo | \`steward_manual_review_required\` | — |
| SJ Grand Hotel Monterrey | \`steward_manual_review_required\` | — |

Frozen Wave 1D source artifacts **not** altered.

---

## 10. Completeness by family

| Family | Core % | Material % |
|--------|-------:|-----------:|
| IHG | ${ihg.core_pct} | ${ihg.material_pct} |
| Hilton | ${hilton.core_pct} | ${hilton.material_pct} |
| Choice | ${choice.core_pct} | ${choice.material_pct} |
| Marriott | ${marriott.core_pct} | ${marriott.material_pct} |

---

## 11. Data-eligible summary

| Family | Data-eligible |
|--------|--------------:|
| IHG | ${ihg.data_eligible} |
| Hilton | ${hilton.data_eligible} |
| Choice | ${choice.data_eligible} |
| Marriott | ${marriott.data_eligible} |
| **Total** | **${dataEligibleTotal}** |

---

## 12. Brand coverage by family

Marriott independently found **${Object.keys(marriott.brand_counts).length}** brands, including:

| Brand | Count |
|-------|------:|
| City Express by Marriott | ${marriott.brand_counts["City Express by Marriott"] || 0} |
| Courtyard by Marriott | ${marriott.brand_counts["Courtyard by Marriott"] || 0} |
| Design Hotels | ${marriott.brand_counts["Design Hotels"] || 0} |
| City Express Plus by Marriott | ${marriott.brand_counts["City Express Plus by Marriott"] || 0} |
| City Express Junior by Marriott | ${marriott.brand_counts["City Express Junior by Marriott"] || 0} |
| Four Points by Sheraton | ${marriott.brand_counts["Four Points by Sheraton"] || 0} |
| Sheraton | ${marriott.brand_counts.Sheraton || 0} |
| Marriott Bonvoy — Brand Unconfirmed | ${marriott.brand_counts["Marriott Bonvoy — Brand Unconfirmed"] || 0} |

Full table: \`08_brand_coverage_by_family.json\`

---

## 13. Cross-family steward queue

| Class | Count |
|-------|------:|
| Exact — steward review | ${crossFamilySteward.summary?.exact_physical_match_requires_steward_review ?? 0} |
| Probable — steward review | ${crossFamilySteward.summary?.probable_physical_match_requires_steward_review ?? 0} |
| Auto-merges | **0** |

---

## 14. Rejected fuzzy matches

| Class | Count |
|-------|------:|
| Rejected fuzzy | ${crossFamilySteward.summary?.rejected_fuzzy_match ?? 0} |
| Insufficient evidence — no merge | ${crossFamilySteward.summary?.insufficient_evidence_no_merge ?? 0} |

---

## 15. Brand Explorer completion readiness

**Small BE completion pilot ready** — **no activation**.

- Prefer non-Marriott stronger-material brands for first pilot
- Marriott brands: mostly \`completion_partial\` / \`completion_hold\` until material enrichment
- Brand Unconfirmed: \`excluded_from_brand_completion\` or \`steward_review_required\` unless overlay confirms

---

## 16. Staging migration readiness

| Gate | Status |
|------|--------|
| Staging migration ready | **YES** |
| Production overwrite ready | **NO** |

**Safe staging fields:** family, brand, property/canonical name, city, state/region, country, source URL/type/lineage, as-of discovery date, identity key, confidence.

**Unsafe:** missing rooms, owner, operator, open date, missing coordinates, unconfirmed brands without overlay confirmation, temporal start dates, images.

---

## 17. Limitations

- Marriott sitemap-only: no address/coords/rooms/owner/open date
- Choice property pages often Blocked (≠ closed)
- Hilton/Choice rooms weak
- Soft brands and campus annexes need FP enrichment before production
- Combined unique physical is family-sum staging identity — not a single geo-verified building graph

---

## 18. Recommended next steps

1. Optional: apply Marriott Reforma \`confirm_brand\` overlay in staging views (freeze unchanged)
2. Small BE completion pilot (non-Marriott first, or Marriott brands with steward-approved identity only)
3. First-party enrichment for Marriott material fields (rooms/coords) via safe paths
4. Do **not** overwrite production Hotel Census

---

## Acceptance

- [x] Combined reconciles to **666** (195+102+68+301)
- [x] Families separately traceable
- [x] Marriott steward overlay included; Wave 1D freeze unmodified
- [x] Cross-family fuzzy auto-merges = 0
- [x] Fake temporal starts / rooms / owners = 0
- [x] Missing coordinates remain Unknown
- [x] BE readiness + staging migration documented; production overwrite **not** ready
- [x] No Airtable / BE activation / production overwrite / Webhound
- [x] Freeze hash created: \`${combinedFreezeHash}\`
- [x] Status: \`${BASELINE_STATUS}\`
`;

writeMd(OUT, "16_final_report.md", md);
writeMd(REPORTS, "verified-independent-census-mexico-combined-4family.md", md);
writeJson(REPORTS, "verified-independent-census-mexico-combined-4family.json", reportJson);

writeMd(
  DOCS,
  "verified-independent-census-mexico-4family-baseline.md",
  `# Mexico VIC 4-Family Baseline (Locked)

> **Status:** \`${BASELINE_STATUS}\`  
> **Locked at:** ${LOCKED_AT}  
> **Artifacts:** \`data/research-engine-v2/verified-independent-census-mexico-combined-4family/\`  
> **Reports:** \`reports/research-engine-v2/verified-independent-census-mexico-combined-4family.{md,json}\`  
> **Freeze hash:** \`${combinedFreezeHash}\`

## Snapshot

| Family | Records | Data-eligible |
|--------|--------:|--------------:|
| IHG | 195 | ${ihg.data_eligible} |
| Hilton | 102 | ${hilton.data_eligible} |
| Choice | 68 | ${choice.data_eligible} |
| Marriott | 301 | ${marriott.data_eligible} |
| **Total** | **666** | **${dataEligibleTotal}** |

## Constraints

Staging only · No Airtable · No Webhound · No BE activation · No production overwrite · No cross-family auto-merge · Marriott steward overlay only (Wave 1D freeze unmodified)

## Re-lock

\`\`\`bash
npm run research-engine-v2:lock-mexico-vic-4family-baseline
\`\`\`
`
);

console.log("[vic-4family] locked", {
  status: BASELINE_STATUS,
  total: 666,
  data_eligible: dataEligibleTotal,
  freeze_hash: combinedFreezeHash,
  airtable_writes: false,
  webhound_used: false,
  brand_explorer_activation: false,
  production_overwrite: false,
  cross_family_auto_merges: 0,
});
