/**
 * CALA Census ↔ Benchmark (legacy Hotel Census) coverage reconciliation v1
 *
 * BENCHMARK_ONLY — coverage geography diagnostics.
 * READ-ONLY for Hotel Property Census. No production writes.
 *
 * NEVER persist benchmark property-level records, names, IDs, or unmatched lists.
 * Property matching is ephemeral / in-memory for aggregate overlap only.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { getPlatformBase } from "../hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS, STATUS_OPEN } from "../hotel-census/fields.js";
import {
  listDealalityCalaGeographies,
  resolveDealalityCalaGeography,
  normalizeGeographyLabel,
} from "./dealality-cala-geography-registry-v1.js";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { CENSUS_TABLE_ID } from "./full-cala-15k-census-shell-insert-v1.js";
import { isDirtyStateRegionValue } from "./census-city-to-state-map.js";
import { STATE_REGION_NOT_APPLICABLE } from "./full-cala-core-identity-foundation-closure-v1.js";
import { normalizeKey, nameSimilarity } from "../independent-census/match-current-census.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const RECONCILIATION_OBJECTIVE =
  "cala-census-benchmark-coverage-reconciliation-v1";
export const RECONCILIATION_VERSION =
  "cala-census-benchmark-coverage-reconciliation-v1";

export const BENCHMARK_ROLE = "BENCHMARK_ONLY";
export const BENCHMARK_SOURCE_LABEL =
  "legacy_hotel_census_airtable_read_only_licensing_unconfirmed";

export const GAP_CLASS = Object.freeze({
  BENCHMARK_ALIGNED: "BENCHMARK_ALIGNED",
  POSSIBLE_MINOR_GAP: "POSSIBLE_MINOR_GAP",
  POSSIBLE_MODERATE_GAP: "POSSIBLE_MODERATE_GAP",
  POSSIBLE_MAJOR_GAP: "POSSIBLE_MAJOR_GAP",
  DEALALITY_HIGHER_THAN_BENCHMARK: "DEALALITY_HIGHER_THAN_BENCHMARK",
  INSUFFICIENT_COMPARISON_DATA: "INSUFFICIENT_COMPARISON_DATA",
  ZERO_DEALALITY_BENCHMARK_NONZERO: "ZERO_DEALALITY_BENCHMARK_NONZERO",
});

const MATRIX_PATH = path.join(
  ROOT,
  "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.json"
);
const HOLDS_PATH = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
);
const TMP_DIR = path.join(
  ROOT,
  "data/research-engine-v2/tmp-benchmark-coverage-ephemeral"
);

const REPORT_JSON = path.join(
  ROOT,
  "reports/research-engine-v2/cala-census-benchmark-coverage-reconciliation.json"
);
const REPORT_MD = path.join(
  ROOT,
  "reports/research-engine-v2/cala-census-benchmark-coverage-reconciliation.md"
);
const PRIORITY_JSON = path.join(
  ROOT,
  "reports/research-engine-v2/cala-geography-discovery-priority-from-benchmark.json"
);
const PRIORITY_MD = path.join(
  ROOT,
  "reports/research-engine-v2/cala-geography-discovery-priority-from-benchmark.md"
);

/** Fields allowed from legacy Hotel Census for in-memory aggregation only. */
const BENCHMARK_READ_FIELDS = Object.freeze([
  CENSUS_FIELDS.name,
  CENSUS_FIELDS.country,
  CENSUS_FIELDS.city,
  CENSUS_FIELDS.market,
  CENSUS_FIELDS.status,
]);

const DEALALITY_READ_FIELDS = Object.freeze([
  "Property Name",
  "Canonical Property Name",
  "Country",
  "City",
  "State / Region",
]);

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}
function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || String(v).trim() === "";
}

function isStateApplicable(country) {
  const c = String(country || "").trim();
  if (!c) return false;
  if (STATE_REGION_NOT_APPLICABLE.has(c)) return false;
  const g = resolveDealalityCalaGeography(c);
  if (g && STATE_REGION_NOT_APPLICABLE.has(g.name)) return false;
  return true;
}

export function classifyCoverageGap({ dealalityCount, benchmarkCount }) {
  const d = Number(dealalityCount) || 0;
  const b = Number(benchmarkCount) || 0;
  if (b === 0 && d === 0) return GAP_CLASS.INSUFFICIENT_COMPARISON_DATA;
  if (b === 0 && d > 0) return GAP_CLASS.DEALALITY_HIGHER_THAN_BENCHMARK;
  if (d === 0 && b > 0) return GAP_CLASS.ZERO_DEALALITY_BENCHMARK_NONZERO;
  const ratio = d / b;
  if (d > b * 1.1) return GAP_CLASS.DEALALITY_HIGHER_THAN_BENCHMARK;
  if (ratio >= 0.85) return GAP_CLASS.BENCHMARK_ALIGNED;
  if (ratio >= 0.65) return GAP_CLASS.POSSIBLE_MINOR_GAP;
  if (ratio >= 0.4) return GAP_CLASS.POSSIBLE_MODERATE_GAP;
  return GAP_CLASS.POSSIBLE_MAJOR_GAP;
}

export function coverageRatio(dealalityCount, benchmarkCount) {
  const b = Number(benchmarkCount) || 0;
  if (b <= 0) return null;
  return Math.round((1000 * (Number(dealalityCount) || 0)) / b) / 1000;
}

/**
 * Ephemeral approximate match: exact name+country, else high Jaccard within country.
 * Returns aggregate matched count only — no identity lists.
 */
/**
 * Ephemeral approximate match: exact name first, then limited fuzzy within country slice.
 * Returns aggregate matched count only — no identity lists.
 */
export function approximateAggregateMatchCount(dealalityRows, benchmarkRows) {
  const usedB = new Set();
  let matched = 0;

  const byExact = new Map();
  for (let i = 0; i < benchmarkRows.length; i++) {
    const key = normalizeKey(benchmarkRows[i].name);
    if (!key) continue;
    if (!byExact.has(key)) byExact.set(key, []);
    byExact.get(key).push(i);
  }

  const unresolvedD = [];
  for (const d of dealalityRows) {
    const dn = normalizeKey(d.name);
    if (!dn) continue;
    const idxs = byExact.get(dn) || [];
    const hit = idxs.find((i) => !usedB.has(i));
    if (hit != null) {
      usedB.add(hit);
      matched += 1;
    } else {
      unresolvedD.push(d);
    }
  }

  // Fuzzy only for unresolved Dealality rows vs unused benchmark (cap work)
  const remainingB = [];
  for (let i = 0; i < benchmarkRows.length; i++) {
    if (usedB.has(i)) continue;
    remainingB.push({
      i,
      nameKey: normalizeKey(benchmarkRows[i].name),
      cityKey: normalizeKey(benchmarkRows[i].city),
      name: benchmarkRows[i].name,
    });
  }

  const fuzzyLimit = Math.min(unresolvedD.length, 2500);
  for (let di = 0; di < fuzzyLimit; di++) {
    const d = unresolvedD[di];
    const dn = normalizeKey(d.name);
    let bestIdx = -1;
    let bestScore = 0;
    for (const b of remainingB) {
      if (usedB.has(b.i) || !b.nameKey) continue;
      let score = nameSimilarity(d.name, b.name);
      if (d.city && b.cityKey) {
        const dCity = normalizeKey(d.city);
        const cityOk =
          dCity === b.cityKey || dCity.includes(b.cityKey) || b.cityKey.includes(dCity);
        if (cityOk && score >= 0.55) score = Math.max(score, 0.82);
      }
      if (score > bestScore) {
        bestScore = score;
        bestIdx = b.i;
      }
    }
    if (bestIdx >= 0 && bestScore >= 0.78) {
      usedB.add(bestIdx);
      matched += 1;
    }
  }

  return matched;
}

function tourismPriorityWeight(p) {
  if (p === "S") return 50;
  if (p === "A") return 30;
  if (p === "B") return 15;
  return 5;
}

export function scoreDiscoveryPriority(row) {
  let score = 0;
  const d = row.dealality_census_count || 0;
  const b = row.BENCHMARK_COUNT || 0;
  const abs = Math.max(0, b - d);
  const ratio = b > 0 ? d / b : 1;

  if (d === 0 && b > 0) score += 1000 + Math.min(b, 400);
  else if (ratio < 0.4) score += 450 + Math.min(abs, 300);
  else if (ratio < 0.65) score += 250 + Math.min(abs, 200);
  else if (ratio < 0.85) score += 100 + Math.min(abs, 100);

  score += tourismPriorityWeight(row.tourism_priority);

  const status = row.dealality_coverage_status || "";
  if (status === "ZERO_CONFIRMED_PROPERTIES") score += 80;
  else if (status === "SOURCE_GAP") score += 50;
  else if (status === "CORE_COVERAGE_WEAK" || status === "NEEDS_TARGETED_DISCOVERY")
    score += 35;
  else if (status === "DISCOVERY_NOT_COMPLETE") score += 25;

  score += Math.min(40, Math.floor((row.active_holds || 0) / 8));

  if (d > 0) {
    if ((row.city_completeness_pct ?? 100) < 90) score += 15;
    if (
      row.state_region_applicable > 0 &&
      (row.state_region_completeness_of_applicable_pct ?? 100) < 50
    ) {
      score += 15;
    }
  }

  // Prefer true geographic undercoverage over Dealality-higher geos
  if (row.gap_class === GAP_CLASS.DEALALITY_HIGHER_THAN_BENCHMARK) score = Math.min(score, 40);

  return score;
}

async function listDealalityCensus(baseId, token) {
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of DEALALITY_READ_FIELDS) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`dealality_census_list_failed:${res.status}:${json?.error?.message || ""}`);
    }
    records.push(...(json.records || []));
    offset = json.offset;
    await sleep(110);
  } while (offset);
  return records.map((r) => {
    const f = r.fields || {};
    return {
      name: String(f["Canonical Property Name"] || f["Property Name"] || "").trim(),
      country: String(f.Country || "").trim(),
      city: String(f.City || "").trim(),
      state: String(f["State / Region"] || "").trim(),
    };
  });
}

/**
 * Load legacy Hotel Census for in-memory aggregation only.
 * Does not write files with property rows.
 */
async function loadBenchmarkCensusEphemeral() {
  const base = getPlatformBase();
  if (!base) {
    throw new Error("Missing Airtable credentials for legacy Hotel Census (benchmark) read");
  }
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: [...BENCHMARK_READ_FIELDS], pageSize: 100 })
    .all();

  const rows = [];
  let open = 0;
  let pipeline = 0;
  let other = 0;
  for (const rec of records) {
    const f = rec.fields || {};
    const status = String(f[CENSUS_FIELDS.status] || "").trim();
    if (status === STATUS_OPEN) open += 1;
    else if (/pipeline/i.test(status)) pipeline += 1;
    else other += 1;
    // Primary benchmark universe = Open (operating) properties
    if (status && status !== STATUS_OPEN) continue;
    rows.push({
      name: String(f[CENSUS_FIELDS.name] || "").trim(),
      country: String(f[CENSUS_FIELDS.country] || "").trim(),
      city: String(f[CENSUS_FIELDS.city] || "").trim(),
      // Market used as coarse admin/destination bucket — NOT claimed as State
      market: String(f[CENSUS_FIELDS.market] || "").trim(),
    });
  }
  return {
    table: HOTEL_CENSUS_TABLE,
    total_rows_loaded: records.length,
    open_count: open,
    pipeline_count: pipeline,
    other_status_count: other,
    benchmark_universe: "status_open_only",
    rows,
  };
}

function resolveGeo(countryRaw) {
  return resolveDealalityCalaGeography(countryRaw);
}

function groupByGeography(rows, getCountry) {
  const map = new Map();
  for (const r of rows) {
    const g = resolveGeo(getCountry(r));
    const key = g?.name || normalizeGeographyLabel(getCountry(r)) || "(unmapped)";
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(r);
  }
  return map;
}

function pct(n, d) {
  if (!d) return null;
  return Math.round((100 * n) / d);
}

function assertAggregateOnly(obj, pathHint = "root") {
  const banned = /hotel.?name|property.?name|address|phone|website|record.?id|hbx|str.?number|unmatched/i;
  const walk = (v, p) => {
    if (v == null) return;
    if (Array.isArray(v)) {
      for (let i = 0; i < v.length; i++) walk(v[i], `${p}[${i}]`);
      return;
    }
    if (typeof v === "object") {
      for (const [k, val] of Object.entries(v)) {
        if (banned.test(k) && !/completeness|coverage|count|status|priority/i.test(k)) {
          throw new Error(`aggregate_safety_violation:${p}.${k}`);
        }
        walk(val, `${p}.${k}`);
      }
    }
  };
  walk(obj, pathHint);
}

function cleanupTmp() {
  try {
    if (fs.existsSync(TMP_DIR)) {
      fs.rmSync(TMP_DIR, { recursive: true, force: true });
    }
  } catch {
    // best-effort
  }
}

export async function runCalaCensusBenchmarkCoverageReconciliationV1(opts = {}) {
  const log = opts.log || console.log;
  const generated_at = new Date().toISOString();

  if (CENSUS_TABLE_ID !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
    throw new Error("dealality_table_id_mismatch");
  }

  cleanupTmp();
  fs.mkdirSync(TMP_DIR, { recursive: true });

  try {
    log(`[benchmark-recon] loading Dealality Hotel Property Census (read-only)…`);
    const token = resolvePat();
    const base = resolveTargetBase();
    const baseId = base?.target_base_id || base?.baseId;
    const dealalityRows = await listDealalityCensus(baseId, token);
    const DEALALITY_CENSUS_COUNT = dealalityRows.length;

    log(`[benchmark-recon] loading legacy Hotel Census as BENCHMARK_ONLY…`);
    const bench = await loadBenchmarkCensusEphemeral();
    const BENCHMARK_CENSUS_COUNT = bench.rows.length;

    const matrixDoc = readJson(MATRIX_PATH, { matrix: [] });
    const matrixByGeo = new Map(
      (matrixDoc.matrix || []).map((r) => [r.geography, r])
    );
    const holds = readJson(HOLDS_PATH, { by_candidate_id: {} });
    const holdsByCountry = {};
    for (const h of Object.values(holds.by_candidate_id || {})) {
      const c = String(h.country || "").trim() || "(unknown)";
      holdsByCountry[c] = (holdsByCountry[c] || 0) + 1;
    }

    const dealByGeo = groupByGeography(dealalityRows, (r) => r.country);
    const benchByGeo = groupByGeography(bench.rows, (r) => r.country);

    const geos = listDealalityCalaGeographies({ inScopeOnly: true });
    const geography_rows = [];
    const state_region_comparisons = [];
    const city_comparisons = [];

    for (const g of geos) {
      const dRows = dealByGeo.get(g.name) || [];
      const bRows = benchByGeo.get(g.name) || [];
      const dealality_census_count = dRows.length;
      const BENCHMARK_COUNT = bRows.length;
      const abs_diff = BENCHMARK_COUNT - dealality_census_count;
      const ratio = coverageRatio(dealality_census_count, BENCHMARK_COUNT);
      const gap_class = classifyCoverageGap({
        dealalityCount: dealality_census_count,
        benchmarkCount: BENCHMARK_COUNT,
      });

      let approx_matched = 0;
      if (dRows.length && bRows.length) {
        // Cap matching cost for huge countries: sample not allowed for accuracy —
        // run full ephemeral match (Brazil/Mexico ~few thousand; acceptable once).
        approx_matched = approximateAggregateMatchCount(dRows, bRows);
      }

      let city_filled = 0;
      let state_applicable = 0;
      let state_filled = 0;
      for (const r of dRows) {
        if (!isBlank(r.city)) city_filled += 1;
        if (isStateApplicable(g.name)) {
          state_applicable += 1;
          if (!isBlank(r.state) && !isDirtyStateRegionValue(r.state)) state_filled += 1;
        }
      }

      const matrix = matrixByGeo.get(g.name) || {};
      const row = {
        geography: g.name,
        geography_id: g.geography_id,
        tourism_priority: g.tourism_priority,
        dealality_region: g.region,
        dealality_census_count,
        BENCHMARK_COUNT,
        absolute_count_difference: abs_diff,
        dealality_coverage_ratio_vs_benchmark: ratio,
        approximate_aggregate_matched_count: approx_matched,
        potential_coverage_gap: gap_class !== GAP_CLASS.BENCHMARK_ALIGNED &&
          gap_class !== GAP_CLASS.DEALALITY_HIGHER_THAN_BENCHMARK &&
          gap_class !== GAP_CLASS.INSUFFICIENT_COMPARISON_DATA,
        gap_class,
        dealality_coverage_status: matrix.coverage_status || null,
        active_holds: holdsByCountry[g.name] || 0,
        city_completeness_pct: pct(city_filled, dealality_census_count),
        state_region_applicable: state_applicable,
        state_region_completeness_of_applicable_pct: pct(state_filled, state_applicable),
        sources_searched_hint: matrix.HBX_SEARCHED || null,
        discovery_instruction:
          gap_class === GAP_CLASS.ZERO_DEALALITY_BENCHMARK_NONZERO ||
          gap_class === GAP_CLASS.POSSIBLE_MAJOR_GAP ||
          gap_class === GAP_CLASS.POSSIBLE_MODERATE_GAP
            ? `SEARCH ${g.name.toUpperCase()} MORE DEEPLY (independent sources only; do not copy benchmark records)`
            : null,
      };
      row.priority_score = scoreDiscoveryPriority(row);
      geography_rows.push(row);

      // State/Region vs benchmark Market bucket (aggregate only)
      if (isStateApplicable(g.name) && (dRows.length || bRows.length)) {
        const dState = new Map();
        for (const r of dRows) {
          const s = String(r.state || "").trim() || "(blank_or_missing)";
          dState.set(s, (dState.get(s) || 0) + 1);
        }
        const bMarket = new Map();
        for (const r of bRows) {
          const m = String(r.market || "").trim() || "(blank_or_missing)";
          bMarket.set(m, (bMarket.get(m) || 0) + 1);
        }
        const keys = new Set([...dState.keys(), ...bMarket.keys()]);
        for (const key of keys) {
          if (key === "(blank_or_missing)") continue;
          const dc = dState.get(key) || 0;
          const bc = bMarket.get(key) || 0;
          if (dc === 0 && bc === 0) continue;
          // Only emit when both sides have signal or clear potential gap
          if (bc === 0 && dc > 0) continue; // skip Dealality-only markets noise at this grain
          if (bc < 5 && dc < 5) continue; // suppress tiny buckets
          state_region_comparisons.push({
            geography: g.name,
            admin_or_market_bucket: key,
            bucket_source_note:
              "Dealality uses State/Region; benchmark uses Market field as coarse destination/admin proxy (not identical vocabularies)",
            dealality_count: dc,
            BENCHMARK_COUNT: bc,
            relative_coverage_pct: bc > 0 ? pct(dc, bc) : null,
            potential_coverage_gap_class: classifyCoverageGap({
              dealalityCount: dc,
              benchmarkCount: bc,
            }),
          });
        }
      }

      // City aggregates
      const dCity = new Map();
      for (const r of dRows) {
        const c = String(r.city || "").trim();
        if (!c) continue;
        dCity.set(normalizeKey(c), { label: c, n: (dCity.get(normalizeKey(c))?.n || 0) + 1 });
      }
      const bCity = new Map();
      for (const r of bRows) {
        const c = String(r.city || "").trim();
        if (!c) continue;
        const k = normalizeKey(c);
        bCity.set(k, { label: c, n: (bCity.get(k)?.n || 0) + 1 });
      }
      const cityKeys = new Set([...dCity.keys(), ...bCity.keys()]);
      for (const k of cityKeys) {
        const dc = dCity.get(k)?.n || 0;
        const bc = bCity.get(k)?.n || 0;
        const label = bCity.get(k)?.label || dCity.get(k)?.label;
        if (bc < 8 && !(dc === 0 && bc >= 5)) continue;
        const gap = classifyCoverageGap({ dealalityCount: dc, benchmarkCount: bc });
        if (
          gap === GAP_CLASS.BENCHMARK_ALIGNED ||
          gap === GAP_CLASS.DEALALITY_HIGHER_THAN_BENCHMARK
        ) {
          continue;
        }
        const priority =
          gap === GAP_CLASS.ZERO_DEALALITY_BENCHMARK_NONZERO
            ? "HIGH"
            : gap === GAP_CLASS.POSSIBLE_MAJOR_GAP
              ? "HIGH"
              : gap === GAP_CLASS.POSSIBLE_MODERATE_GAP
                ? "MEDIUM"
                : "LOW";
        city_comparisons.push({
          geography: g.name,
          city_or_destination: label,
          dealality_count: dc,
          BENCHMARK_COUNT: bc,
          absolute_count_difference: bc - dc,
          dealality_coverage_ratio_vs_benchmark: coverageRatio(dc, bc),
          potential_coverage_gap: priority,
          gap_class: gap,
          discovery_instruction: `SEARCH ${label.toUpperCase()} / ${g.name.toUpperCase()} MORE DEEPLY (independent sources only; do not copy benchmark records)`,
          priority_score:
            (gap === GAP_CLASS.ZERO_DEALALITY_BENCHMARK_NONZERO ? 800 : 0) +
            (gap === GAP_CLASS.POSSIBLE_MAJOR_GAP ? 400 : 0) +
            (gap === GAP_CLASS.POSSIBLE_MODERATE_GAP ? 200 : 0) +
            Math.min(bc - dc, 200) +
            tourismPriorityWeight(g.tourism_priority),
        });
      }
    }

    geography_rows.sort(
      (a, b) =>
        (b.priority_score || 0) - (a.priority_score || 0) ||
        (b.BENCHMARK_COUNT || 0) - (a.BENCHMARK_COUNT || 0)
    );
    city_comparisons.sort(
      (a, b) =>
        (b.priority_score || 0) - (a.priority_score || 0) ||
        (b.BENCHMARK_COUNT || 0) - (a.BENCHMARK_COUNT || 0)
    );
    state_region_comparisons.sort(
      (a, b) =>
        (b.BENCHMARK_COUNT || 0) - (a.BENCHMARK_COUNT || 0) ||
        a.geography.localeCompare(b.geography)
    );

    const countClass = (c) =>
      geography_rows.filter((r) => r.gap_class === c).length;

    const GEOGRAPHY_DISCOVERY_PRIORITY = geography_rows
      .filter(
        (r) =>
          r.gap_class === GAP_CLASS.ZERO_DEALALITY_BENCHMARK_NONZERO ||
          r.gap_class === GAP_CLASS.POSSIBLE_MAJOR_GAP ||
          r.gap_class === GAP_CLASS.POSSIBLE_MODERATE_GAP ||
          r.gap_class === GAP_CLASS.POSSIBLE_MINOR_GAP
      )
      .map((r) => ({
        geography: r.geography,
        dealality_census_count: r.dealality_census_count,
        BENCHMARK_COUNT: r.BENCHMARK_COUNT,
        absolute_count_difference: r.absolute_count_difference,
        dealality_coverage_ratio_vs_benchmark: r.dealality_coverage_ratio_vs_benchmark,
        gap_class: r.gap_class,
        dealality_coverage_status: r.dealality_coverage_status,
        active_holds: r.active_holds,
        tourism_priority: r.tourism_priority,
        priority_score: r.priority_score,
        discovery_instruction: r.discovery_instruction,
      }));

    const CITY_DESTINATION_DISCOVERY_PRIORITY = city_comparisons.slice(0, 75).map((r) => ({
      geography: r.geography,
      city_or_destination: r.city_or_destination,
      dealality_count: r.dealality_count,
      BENCHMARK_COUNT: r.BENCHMARK_COUNT,
      absolute_count_difference: r.absolute_count_difference,
      dealality_coverage_ratio_vs_benchmark: r.dealality_coverage_ratio_vs_benchmark,
      potential_coverage_gap: r.potential_coverage_gap,
      gap_class: r.gap_class,
      priority_score: r.priority_score,
      discovery_instruction: r.discovery_instruction,
    }));

    const report = {
      ok: true,
      RECONCILIATION_STATUS:
        "production_census_benchmark_coverage_reconciliation_complete_aggregate_only",
      objective: RECONCILIATION_OBJECTIVE,
      version: RECONCILIATION_VERSION,
      generated_at,
      policy: {
        benchmark_role: BENCHMARK_ROLE,
        benchmark_source_label: BENCHMARK_SOURCE_LABEL,
        licensing: "usage_and_licensing_terms_NOT_confirmed — do not import or derive records",
        dealality_writes: false,
        benchmark_writes: false,
        property_level_benchmark_data_persisted: false,
        benchmark_records_written_to_dealality: 0,
        benchmark_used_as_production_provenance: false,
        matching: "ephemeral_in_memory_aggregate_overlap_only",
        workflow:
          "BENCHMARK suggests geographic gap → Dealality independent discovery → HBX/SerpAPI/Cvent/official → CORE_IDENTITY_CONFIRMED → Census",
      },
      DEALALITY_CENSUS_COUNT,
      BENCHMARK_CENSUS_COUNT,
      benchmark_universe_note: bench.benchmark_universe,
      benchmark_table: bench.table,
      benchmark_open_count: bench.open_count,
      benchmark_pipeline_excluded: bench.pipeline_count,
      GEOGRAPHIES_COMPARED: geography_rows.length,
      GEOGRAPHIES_BENCHMARK_ALIGNED: countClass(GAP_CLASS.BENCHMARK_ALIGNED),
      POSSIBLE_MINOR_GAPS: countClass(GAP_CLASS.POSSIBLE_MINOR_GAP),
      POSSIBLE_MODERATE_GAPS: countClass(GAP_CLASS.POSSIBLE_MODERATE_GAP),
      POSSIBLE_MAJOR_GAPS: countClass(GAP_CLASS.POSSIBLE_MAJOR_GAP),
      ZERO_DEALALITY_BENCHMARK_NONZERO: countClass(
        GAP_CLASS.ZERO_DEALALITY_BENCHMARK_NONZERO
      ),
      DEALALITY_HIGHER_THAN_BENCHMARK: countClass(
        GAP_CLASS.DEALALITY_HIGHER_THAN_BENCHMARK
      ),
      INSUFFICIENT_COMPARISON_DATA: countClass(GAP_CLASS.INSUFFICIENT_COMPARISON_DATA),
      geography_matrix: geography_rows,
      state_region_or_market_bucket_comparisons: state_region_comparisons.slice(0, 200),
      TOP_15_GEOGRAPHIC_DISCOVERY_PRIORITIES: GEOGRAPHY_DISCOVERY_PRIORITY.slice(0, 15),
      TOP_CITY_DESTINATION_DISCOVERY_PRIORITIES: CITY_DESTINATION_DISCOVERY_PRIORITY.slice(0, 25),
      GEOGRAPHY_DISCOVERY_PRIORITY,
      CITY_DESTINATION_DISCOVERY_PRIORITY,
      PROPERTY_LEVEL_BENCHMARK_DATA_PERSISTED: "NO",
      BENCHMARK_RECORDS_WRITTEN_TO_DEALALITY: 0,
      BENCHMARK_USED_AS_PRODUCTION_PROVENANCE: "NO",
      NEXT_RECOMMENDED_ACTION:
        "Run independent multi-source discovery for TOP geographic / city-destination priorities (SerpAPI, HBX where quota allows, official/public directories). Do not import or copy legacy Hotel Census records. Do not treat BENCHMARK_COUNT as true hotel inventory.",
      FOUNDER_DECISION_REQUIRED: "NO",
    };

    assertAggregateOnly(report);

    writeJson(REPORT_JSON, report);
    writeJson(PRIORITY_JSON, {
      generated_at,
      source_report: "reports/research-engine-v2/cala-census-benchmark-coverage-reconciliation.json",
      policy: report.policy,
      GEOGRAPHY_DISCOVERY_PRIORITY,
      CITY_DESTINATION_DISCOVERY_PRIORITY,
      PROPERTY_LEVEL_BENCHMARK_DATA_PERSISTED: "NO",
    });

    const md = [
      `# CALA Census ↔ Benchmark Coverage Reconciliation`,
      ``,
      `**Status:** \`${report.RECONCILIATION_STATUS}\``,
      `**Generated:** ${generated_at}`,
      ``,
      `## Policy`,
      ``,
      `- Benchmark role: **BENCHMARK_ONLY** (licensing unconfirmed)`,
      `- Dealality table: Hotel Property Census (\`${CENSUS_TABLE_ID}\`) — **read-only**`,
      `- No property-level benchmark persistence`,
      `- No Census inserts from this reconciliation`,
      `- Wording: **potential coverage gap** (not “missing hotels”)`,
      ``,
      `## Totals`,
      ``,
      `| Metric | Value |`,
      `| --- | ---: |`,
      `| DEALALITY_CENSUS_COUNT | ${DEALALITY_CENSUS_COUNT} |`,
      `| BENCHMARK_CENSUS_COUNT (Open) | ${BENCHMARK_CENSUS_COUNT} |`,
      `| Geographies compared | ${report.GEOGRAPHIES_COMPARED} |`,
      `| BENCHMARK_ALIGNED | ${report.GEOGRAPHIES_BENCHMARK_ALIGNED} |`,
      `| POSSIBLE_MINOR_GAP | ${report.POSSIBLE_MINOR_GAPS} |`,
      `| POSSIBLE_MODERATE_GAP | ${report.POSSIBLE_MODERATE_GAPS} |`,
      `| POSSIBLE_MAJOR_GAP | ${report.POSSIBLE_MAJOR_GAPS} |`,
      `| ZERO_DEALALITY_BENCHMARK_NONZERO | ${report.ZERO_DEALALITY_BENCHMARK_NONZERO} |`,
      `| DEALALITY_HIGHER_THAN_BENCHMARK | ${report.DEALALITY_HIGHER_THAN_BENCHMARK} |`,
      ``,
      `## Top 15 geographic discovery priorities`,
      ``,
      `| Geography | Dealality | BENCHMARK_COUNT | Ratio | Gap class | Priority |`,
      `| --- | ---: | ---: | ---: | --- | ---: |`,
      ...report.TOP_15_GEOGRAPHIC_DISCOVERY_PRIORITIES.map(
        (r) =>
          `| ${r.geography} | ${r.dealality_census_count} | ${r.BENCHMARK_COUNT} | ${r.dealality_coverage_ratio_vs_benchmark ?? "—"} | ${r.gap_class} | ${r.priority_score} |`
      ),
      ``,
      `## Top city / destination discovery priorities`,
      ``,
      `| Geography | City/destination | Dealality | Benchmark | Gap | Instruction |`,
      `| --- | --- | ---: | ---: | --- | --- |`,
      ...report.TOP_CITY_DESTINATION_DISCOVERY_PRIORITIES.map(
        (r) =>
          `| ${r.geography} | ${r.city_or_destination} | ${r.dealality_count} | ${r.BENCHMARK_COUNT} | ${r.potential_coverage_gap} | SEARCH ${r.city_or_destination} / ${r.geography} MORE DEEPLY |`
      ),
      ``,
      `## Next recommended action`,
      ``,
      report.NEXT_RECOMMENDED_ACTION,
      ``,
      `PROPERTY_LEVEL_BENCHMARK_DATA_PERSISTED: **NO**  `,
      `BENCHMARK_RECORDS_WRITTEN_TO_DEALALITY: **0**  `,
      `BENCHMARK_USED_AS_PRODUCTION_PROVENANCE: **NO**`,
    ].join("\n");
    writeMd(REPORT_MD, md);
    writeMd(
      PRIORITY_MD,
      [
        `# Geography discovery priority (from benchmark coverage recon)`,
        ``,
        `Aggregate-only. No benchmark hotel names or IDs.`,
        ``,
        `See \`${path.relative(ROOT, REPORT_JSON).replace(/\\/g, "/")}\`.`,
      ].join("\n")
    );

    log(
      `[benchmark-recon] DONE dealality=${DEALALITY_CENSUS_COUNT} benchmark_open=${BENCHMARK_CENSUS_COUNT} major=${report.POSSIBLE_MAJOR_GAPS} zero=${report.ZERO_DEALALITY_BENCHMARK_NONZERO}`
    );

    return report;
  } finally {
    cleanupTmp();
  }
}
