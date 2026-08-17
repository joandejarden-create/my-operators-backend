/**
 * Full CALA Geography Coverage Registry Audit v1 — READ-ONLY.
 *
 * Answers: have we actually searched every relevant CALA country/territory?
 * Does NOT equate zero source records with "searched and found zero".
 *
 * No production writes. No enrichment. No shell inserts.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import {
  CENSUS_TABLE_ID,
  MATCH,
  loadMasterUniverseCandidates,
  loadHbxCandidates,
  mergeCandidateUniverses,
  classifyAgainstCensus,
  listCensusIndex,
} from "./full-cala-15k-census-shell-insert-v1.js";
import {
  DEALALITY_CALA_GEOGRAPHY_REGISTRY_VERSION,
  DEALALITY_CALA_GEOGRAPHIES,
  HBX_WAVE1_SEARCHED_GEOGRAPHIES,
  listDealalityCalaGeographies,
  resolveDealalityCalaGeography,
  normalizeGeographyLabel,
  getParentEncodingLeakageHints,
} from "./dealality-cala-geography-registry-v1.js";
import {
  CVENT_LATAM_CARIBBEAN_COUNTRIES,
  CVENT_LATAM_COUNTRY_REGISTRY_VERSION,
} from "./census-cvent-latam-country-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const GEO_COVERAGE_OBJECTIVE =
  "full-cala-geography-coverage-registry-audit-v1";
export const GEO_COVERAGE_VERSION =
  "full-cala-geography-coverage-registry-audit-v1";

export const GEO_COVERAGE_STATUS = Object.freeze({
  COMPLETE:
    "production_census_full_cala_geography_coverage_registry_audit_complete",
  BLOCKED:
    "production_census_full_cala_geography_coverage_registry_audit_blocked",
});

export const GEO_STATUS = Object.freeze({
  DISCOVERY_STRONG: "DISCOVERY_STRONG",
  DISCOVERY_PARTIAL: "DISCOVERY_PARTIAL",
  SOURCE_GAP: "SOURCE_GAP",
  NO_SOURCE_STOCK: "NO_SOURCE_STOCK",
  NOT_YET_SEARCHED: "NOT_YET_SEARCHED",
  NORMALIZATION_PROBLEM: "NORMALIZATION_PROBLEM",
  SCOPE_REVIEW: "SCOPE_REVIEW",
});

const STATUS_SORT = {
  [GEO_STATUS.NOT_YET_SEARCHED]: 0,
  [GEO_STATUS.NO_SOURCE_STOCK]: 1,
  [GEO_STATUS.SOURCE_GAP]: 2,
  [GEO_STATUS.NORMALIZATION_PROBLEM]: 3,
  [GEO_STATUS.DISCOVERY_PARTIAL]: 4,
  [GEO_STATUS.DISCOVERY_STRONG]: 5,
  [GEO_STATUS.SCOPE_REVIEW]: 6,
};

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
function bump(map, key, n = 1) {
  const k = key == null || key === "" ? "(blank)" : String(key);
  map[k] = (map[k] || 0) + n;
}

function isShellRecord(fields) {
  const batch = String(fields["Shell Insert Batch ID"] || "");
  const idKey = String(fields["Property Identity Key"] || "");
  return (
    batch === "full-cala-15k-census-shell-insert-v1" ||
    idKey.startsWith("shell_")
  );
}


function cventSearchEvidence() {
  const registryNames = new Set(
    CVENT_LATAM_CARIBBEAN_COUNTRIES.map((c) => c.country)
  );
  const summary = readJson(
    path.join(
      ROOT,
      "reports/research-engine-v2/cvent-latam-harvest-inventory-summary.json"
    ),
    {}
  );
  const excluded = new Set(
    (summary.probe?.excluded || []).map((x) =>
      String(x).replace(/\s*\(.*\)$/, "").trim()
    )
  );
  // Cuba explicitly excluded after probe (searched, empty)
  return {
    registry_version: CVENT_LATAM_COUNTRY_REGISTRY_VERSION,
    registry_countries: [...registryNames],
    countries_seeded: summary.probe?.countries_seeded ?? registryNames.size,
    viable_countries: summary.probe?.viable_countries ?? null,
    excluded_after_probe: [...excluded],
    summary_note: summary.coverage_note || null,
  };
}

function hbxSearchEvidence() {
  const wave1 = [...HBX_WAVE1_SEARCHED_GEOGRAPHIES];
  const ledger = readJson(
    path.join(
      ROOT,
      "data/research-engine-v2/full-cala-hbx-geography-discovery/hbx-geography-discovery-ledger.json"
    ),
    null
  );
  const fromLedger = [];
  const failed = [];
  if (ledger?.geographies) {
    for (const e of Object.values(ledger.geographies)) {
      if (
        e.hbx_status === "COMPLETE" ||
        e.hbx_status === "COMPLETE_ZERO_RESULTS"
      ) {
        fromLedger.push(e.canonical_geography);
      } else if (
        e.hbx_status === "FAILED_RETRYABLE" ||
        e.hbx_status === "FAILED_REQUIRES_REVIEW"
      ) {
        failed.push({
          geography: e.canonical_geography,
          status: e.hbx_status,
        });
      }
    }
  }
  const searched = [...new Set([...wave1, ...fromLedger])];
  return {
    wave: "hbx-content-api-cala-wave1 + full-cala-hbx-geography-discovery-wave-v1",
    searched_geographies: searched,
    failed_geographies: failed,
    evidence:
      "Wave1 pack + hbx-geography-discovery-ledger.json COMPLETE/COMPLETE_ZERO_RESULTS only (auth failures are not searched)",
  };
}

/**
 * Assign coverage status for one geography row.
 */
export function assignGeographyCoverageStatus(row) {
  if (row.scope === "scope_review") return GEO_STATUS.SCOPE_REVIEW;

  const searchedAny =
    row.HBX_SEARCHED === "YES" ||
    row.CVENT_SEARCHED === "YES" ||
    row.OTHER_SOURCE_SEARCHED === "YES";

  if (row.normalization_issue) return GEO_STATUS.NORMALIZATION_PROBLEM;

  if (!searchedAny) return GEO_STATUS.NOT_YET_SEARCHED;

  const sourceStock =
    (row.hbx_candidates || 0) +
    (row.cvent_candidates || 0) +
    (row.other_candidates || 0);

  if (searchedAny && sourceStock === 0 && (row.census_count || 0) === 0) {
    return GEO_STATUS.NO_SOURCE_STOCK;
  }

  const census = row.census_count || 0;
  const shells = row.shells_inserted || 0;
  const tourism = row.tourism_priority || "C";

  // Major tourism market with suspiciously thin Census → SOURCE_GAP even if some Cvent exists
  if (
    (tourism === "S" || tourism === "A") &&
    census < 40 &&
    (row.HBX_SEARCHED !== "YES" || (row.hbx_candidates || 0) === 0)
  ) {
    return GEO_STATUS.SOURCE_GAP;
  }

  if (census >= 200 || shells >= 200) return GEO_STATUS.DISCOVERY_STRONG;
  if (census >= 40 || shells >= 50 || sourceStock >= 150) {
    return GEO_STATUS.DISCOVERY_PARTIAL;
  }
  if (census > 0 || sourceStock > 0) return GEO_STATUS.DISCOVERY_PARTIAL;
  return GEO_STATUS.SOURCE_GAP;
}

function recommendActionForRow(row) {
  switch (row.coverage_status) {
    case GEO_STATUS.NOT_YET_SEARCHED:
      return "queue_first_pass_multi_source_discovery";
    case GEO_STATUS.NO_SOURCE_STOCK:
      return "confirm_search_evidence_then_alternate_sources";
    case GEO_STATUS.SOURCE_GAP:
      return "expand_hbx_and_official_parent_discovery";
    case GEO_STATUS.NORMALIZATION_PROBLEM:
      return "normalize_parent_country_encodings_before_discovery";
    case GEO_STATUS.DISCOVERY_PARTIAL:
      return "targeted_gap_fill_then_enrichment_later";
    case GEO_STATUS.DISCOVERY_STRONG:
      return "hold_for_enrichment_phase_after_universe_complete";
    case GEO_STATUS.SCOPE_REVIEW:
      return "founder_decide_bermuda_in_or_out_of_cala";
    default:
      return "review";
  }
}

function discoveryQueuePriority(row) {
  let score = 0;
  if (row.coverage_status === GEO_STATUS.NOT_YET_SEARCHED) score += 1000;
  if (row.coverage_status === GEO_STATUS.NO_SOURCE_STOCK) score += 800;
  if (row.coverage_status === GEO_STATUS.SOURCE_GAP) score += 600;
  if (row.coverage_status === GEO_STATUS.NORMALIZATION_PROBLEM) score += 500;
  if (row.tourism_priority === "S") score += 200;
  if (row.tourism_priority === "A") score += 120;
  if (row.tourism_priority === "B") score += 60;
  if (row.HBX_SEARCHED !== "YES") score += 80;
  if (row.CVENT_SEARCHED !== "YES") score += 40;
  if ((row.census_count || 0) === 0) score += 50;
  if ((row.census_count || 0) > 0 && (row.census_count || 0) < 25) score += 30;
  // Deprioritize Brazil weak-hold mass for "never searched" work — already over-inventoried in Cvent
  if (row.name === "Brazil") score -= 400;
  if (row.scope === "scope_review") score -= 200;
  return score;
}

export async function runFullCalaGeographyCoverageRegistryAuditV1(opts = {}) {
  const log = opts.log || (() => {});
  const generated_at = new Date().toISOString();

  let token;
  let baseId;
  try {
    token = resolvePat();
    const base = resolveTargetBase();
    baseId = base?.target_base_id || base?.baseId || process.env.AIRTABLE_BASE_ID_ALT;
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
    if (CENSUS_TABLE_ID !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
      throw new Error("target_table_id_mismatch");
    }
  } catch (err) {
    return {
      ok: false,
      AUDIT_STATUS: GEO_COVERAGE_STATUS.BLOCKED,
      STOP_REASON: String(err?.message || err).slice(0, 400),
      FOUNDER_DECISION_REQUIRED: "YES",
      production_writes: false,
      generated_at,
    };
  }

  const geos = listDealalityCalaGeographies({ includeScopeReview: true });
  const hbxEv = hbxSearchEvidence();
  const cventEv = cventSearchEvidence();
  const hbxSearchedSet = new Set(
    hbxEv.searched_geographies.map((n) => normalizeGeographyLabel(n))
  );
  const cventRegistrySet = new Set(
    cventEv.registry_countries.map((n) => normalizeGeographyLabel(n))
  );
  const cventExcludedSet = new Set(
    (cventEv.excluded_after_probe || []).map((n) => normalizeGeographyLabel(n))
  );

  log("[geo-coverage] listing Census (read-only)…");
  const censusIndex = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
  const censusRecords = censusIndex.records;
  const productionCount = censusRecords.length;

  /** @type {Record<string, object>} */
  const perGeo = {};
  for (const g of geos) {
    perGeo[g.geography_id] = {
      geography_id: g.geography_id,
      name: g.name,
      iso_code: g.iso_code,
      dealality_region: g.region,
      tourism_priority: g.tourism_priority,
      scope: g.scope,
      aliases: g.aliases,
      census_count: 0,
      hbx_candidates: 0,
      cvent_candidates: 0,
      cvent_plus_hbx_candidates: 0,
      other_candidates: 0,
      hold_candidates: 0,
      existing_match_count: 0,
      shells_inserted: 0,
      invalid_non_hotel_count: 0,
      duplicate_count: 0,
      unresolved_candidate_count: 0,
      HBX_SEARCHED: "NO",
      CVENT_SEARCHED: "NO",
      OTHER_SOURCE_SEARCHED: "NO",
      normalization_issue: false,
      normalization_notes: [],
      parent_encoding_leakage_hits: 0,
    };
  }

  // Unresolved / leakage buckets
  const unresolvedCensusCountries = {};
  const parentLeakHits = {};
  const leakageHints = getParentEncodingLeakageHints();

  for (const r of censusRecords) {
    const f = r.fields || {};
    const rawCountry = String(f.Country || "").trim();
    const g = resolveDealalityCalaGeography(rawCountry);
    if (!g) {
      bump(unresolvedCensusCountries, rawCountry || "(blank)");
      // Parent-country encoding leakage check
      for (const [parent, kids] of Object.entries(leakageHints)) {
        if (normalizeGeographyLabel(rawCountry) === normalizeGeographyLabel(parent)) {
          bump(parentLeakHits, parent);
          // Cannot attribute to a single territory without city heuristics — flag all kids lightly later
        }
      }
      continue;
    }
    const row = perGeo[g.geography_id];
    row.census_count += 1;
    if (isShellRecord(f)) row.shells_inserted += 1;
  }

  // Soft-flag territories that may hide under parent encodings when Census has parent-country rows
  for (const [parent, count] of Object.entries(parentLeakHits)) {
    for (const kidName of leakageHints[parent] || []) {
      const g = resolveDealalityCalaGeography(kidName);
      if (!g) continue;
      const row = perGeo[g.geography_id];
      row.normalization_issue = true;
      row.parent_encoding_leakage_hits = count;
      row.normalization_notes.push(
        `Census has ${count} row(s) with Country='${parent}' which may hide ${kidName} (market not collapsed).`
      );
    }
  }

  log("[geo-coverage] loading candidate universes…");
  const universe = loadMasterUniverseCandidates();
  const hbx = loadHbxCandidates();
  const { merged } = mergeCandidateUniverses(universe, hbx);

  for (const c of merged) {
    const g = resolveDealalityCalaGeography(c.country);
    if (!g) continue;
    const row = perGeo[g.geography_id];
    const sources = c.merged_sources || [c.source_type];
    const isCvent =
      c.source_type === "cvent_candidate" || sources.includes("cvent_candidate");
    const isHbx =
      Boolean(c.external_ids?.hbx_code) ||
      c.source_type === "hbx_content_api" ||
      sources.includes("hbx_content_api");
    const isOther = !isCvent && !isHbx;

    if (isHbx && isCvent) row.cvent_plus_hbx_candidates += 1;
    else if (isHbx) row.hbx_candidates += 1;
    else if (isCvent) row.cvent_candidates += 1;
    else if (isOther) row.other_candidates += 1;

    if (isOther) row.OTHER_SOURCE_SEARCHED = "YES";
  }

  // Holds ledger
  const holds = readJson(
    path.join(
      ROOT,
      "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
    ),
    { by_candidate_id: {} }
  );
  const holdByCountry = {};
  for (const h of Object.values(holds.by_candidate_id || {})) {
    bump(holdByCountry, h.country || "(blank)");
    const g = resolveDealalityCalaGeography(h.country);
    if (g) perGeo[g.geography_id].hold_candidates += 1;
  }

  // Classify match classes vs census (for existing/dup/invalid/unresolved)
  log("[geo-coverage] classifying candidates vs Census…");
  for (const c of merged) {
    const g = resolveDealalityCalaGeography(c.country);
    if (!g) continue;
    const row = perGeo[g.geography_id];
    c.merged_sources = c.merged_sources || [c.source_type];
    const cls = classifyAgainstCensus(c, censusIndex);
    if (
      cls.match_class === MATCH.EXISTING_HIGH ||
      cls.match_class === MATCH.EXISTING_MEDIUM
    ) {
      row.existing_match_count += 1;
    } else if (
      cls.match_class === MATCH.PROBABLE_DUP ||
      cls.match_class === MATCH.POSSIBLE_DUP
    ) {
      row.duplicate_count += 1;
    } else if (cls.match_class === MATCH.REJECT_NON_HOTEL) {
      row.invalid_non_hotel_count += 1;
    } else if (
      cls.match_class === MATCH.NEW_HIGH ||
      cls.match_class === MATCH.NEW_MEDIUM ||
      cls.match_class === MATCH.NEW_LOW
    ) {
      row.unresolved_candidate_count += 1;
    }
  }

  // Search flags
  for (const g of geos) {
    const row = perGeo[g.geography_id];
    const n = normalizeGeographyLabel(g.name);
    const aliasHit = [g.name, ...(g.aliases || [])].some((a) =>
      hbxSearchedSet.has(normalizeGeographyLabel(a))
    );
    row.HBX_SEARCHED = aliasHit || hbxSearchedSet.has(n) ? "YES" : "NO";

    // Cvent: in registry = intentionally seeded/searched; excluded after probe still counts as searched
    const inRegistry = [g.name, ...(g.aliases || [])].some((a) =>
      cventRegistrySet.has(normalizeGeographyLabel(a))
    );
    // Turks and Caicos naming: registry uses "Turks and Caicos"
    const turks =
      g.geography_id === "turks_and_caicos" &&
      cventRegistrySet.has(normalizeGeographyLabel("Turks and Caicos"));
    if (inRegistry || turks || cventExcludedSet.has(n)) {
      row.CVENT_SEARCHED = "YES";
    } else if ((row.cvent_candidates || 0) > 0) {
      row.CVENT_SEARCHED = "YES";
    } else {
      row.CVENT_SEARCHED = "NO";
    }

    // If HBX pack has candidates but geography not in Wave1 map → treat as UNKNOWN (leak/normalization), not YES
    if (row.HBX_SEARCHED === "NO" && (row.hbx_candidates || 0) > 0) {
      row.HBX_SEARCHED = "UNKNOWN";
      row.normalization_notes.push(
        "HBX candidates present but geography not in documented Wave1 search map."
      );
      row.normalization_issue = true;
    }
  }

  // Finalize statuses + actions
  const matrix = [];
  for (const g of geos) {
    const row = perGeo[g.geography_id];
    row.coverage_status = assignGeographyCoverageStatus(row);
    row.recommended_next_action = recommendActionForRow(row);
    row.discovery_queue_score = discoveryQueuePriority(row);
    matrix.push(row);
  }

  matrix.sort((a, b) => {
    const sa = STATUS_SORT[a.coverage_status] ?? 99;
    const sb = STATUS_SORT[b.coverage_status] ?? 99;
    if (sa !== sb) return sa - sb;
    return (b.discovery_queue_score || 0) - (a.discovery_queue_score || 0);
  });

  // Macro buckets
  const macro = {
    Mexico: 0,
    "Central America ex-Mexico": 0,
    Caribbean: 0,
    "South America ex-Brazil": 0,
    Brazil: 0,
    other_or_unresolved: 0,
  };
  for (const r of censusRecords) {
    const raw = String(r.fields?.Country || "").trim();
    const g = resolveDealalityCalaGeography(raw);
    if (!g) {
      macro.other_or_unresolved += 1;
      continue;
    }
    if (g.name === "Mexico") macro.Mexico += 1;
    else if (g.name === "Brazil") macro.Brazil += 1;
    else if (g.region === "Central America") macro["Central America ex-Mexico"] += 1;
    else if (g.region === "Caribbean") macro.Caribbean += 1;
    else if (g.region === "South America") macro["South America ex-Brazil"] += 1;
    else macro.other_or_unresolved += 1;
  }

  const top15 = [...matrix]
    .sort((a, b) => b.census_count - a.census_count)
    .slice(0, 15)
    .map((r) => ({ geography: r.name, census_count: r.census_count }));

  const zeroRecord = matrix
    .filter((r) => r.scope === "in_scope" && r.census_count === 0)
    .map((r) => r.name);
  const low1to10 = matrix
    .filter((r) => r.census_count >= 1 && r.census_count <= 10)
    .map((r) => ({ geography: r.name, census_count: r.census_count }));
  const low11to50 = matrix
    .filter((r) => r.census_count >= 11 && r.census_count <= 50)
    .map((r) => ({ geography: r.name, census_count: r.census_count }));

  // HOLD concentration
  const holdEntries = Object.entries(holdByCountry).sort((a, b) => b[1] - a[1]);
  const holdTotal = holdEntries.reduce((a, [, n]) => a + n, 0) || 1;
  const top1 = holdEntries.slice(0, 1);
  const top3 = holdEntries.slice(0, 3);
  const top5 = holdEntries.slice(0, 5);
  const pct = (rows) =>
    Number(
      (
        (100 * rows.reduce((a, [, n]) => a + n, 0)) /
        holdTotal
      ).toFixed(1)
    );

  const queue = [...matrix]
    .filter((r) => r.scope === "in_scope")
    .sort((a, b) => b.discovery_queue_score - a.discovery_queue_score)
    .slice(0, 10)
    .map((r, i) => ({
      rank: i + 1,
      geography: r.name,
      coverage_status: r.coverage_status,
      tourism_priority: r.tourism_priority,
      census_count: r.census_count,
      HBX_SEARCHED: r.HBX_SEARCHED,
      CVENT_SEARCHED: r.CVENT_SEARCHED,
      hbx_candidates: r.hbx_candidates,
      cvent_candidates: r.cvent_candidates,
      score: r.discovery_queue_score,
      recommended_next_action: r.recommended_next_action,
    }));

  const bermuda = matrix.find((r) => r.geography_id === "bermuda");

  const counts = {
    CANONICAL_GEOGRAPHIES_TOTAL: geos.length,
    GEOGRAPHIES_WITH_CENSUS_RECORDS: matrix.filter((r) => r.census_count > 0)
      .length,
    GEOGRAPHIES_WITH_ZERO_CENSUS_RECORDS: matrix.filter(
      (r) => r.census_count === 0
    ).length,
    GEOGRAPHIES_HBX_SEARCHED: matrix.filter((r) => r.HBX_SEARCHED === "YES")
      .length,
    GEOGRAPHIES_CVENT_SEARCHED: matrix.filter((r) => r.CVENT_SEARCHED === "YES")
      .length,
    GEOGRAPHIES_NOT_YET_SEARCHED: matrix.filter(
      (r) => r.coverage_status === GEO_STATUS.NOT_YET_SEARCHED
    ).length,
    GEOGRAPHIES_WITH_SOURCE_GAPS: matrix.filter(
      (r) => r.coverage_status === GEO_STATUS.SOURCE_GAP
    ).length,
    GEOGRAPHIES_WITH_NORMALIZATION_PROBLEMS: matrix.filter(
      (r) => r.coverage_status === GEO_STATUS.NORMALIZATION_PROBLEM
    ).length,
  };

  const nextAction =
    counts.GEOGRAPHIES_NOT_YET_SEARCHED > 0 ||
    counts.GEOGRAPHIES_WITH_SOURCE_GAPS > 0
      ? "PROCEED_SOURCE_GAP_DISCOVERY_BY_GEOGRAPHY_QUEUE"
      : "PROCEED_HBX_EXPANSION_THEN_ENRICHMENT";

  const founderDecisionRequired =
    bermuda?.coverage_status === GEO_STATUS.SCOPE_REVIEW ? "YES" : "NO";

  const report = {
    ok: true,
    AUDIT_STATUS: GEO_COVERAGE_STATUS.COMPLETE,
    objective: GEO_COVERAGE_OBJECTIVE,
    version: GEO_COVERAGE_VERSION,
    registry_version: DEALALITY_CALA_GEOGRAPHY_REGISTRY_VERSION,
    production_writes: false,
    production_table_id: CENSUS_TABLE_ID,
    PRODUCTION_CENSUS_COUNT: productionCount,
    generated_at,
    search_evidence: {
      hbx: hbxEv,
      cvent: cventEv,
      distinction:
        "Zero source records ≠ searched-and-found-zero. HBX Wave1 searched only 5 countries. Cvent registry seeded 49 (Cuba excluded after empty probe). BES islands Sint Eustatius/Saba are not in Cvent registry.",
    },
    ...counts,
    TOP_15_CENSUS_GEOGRAPHIES: top15,
    ZERO_RECORD_GEOGRAPHIES: zeroRecord,
    LOW_COVERAGE_GEOGRAPHIES: {
      "1_to_10": low1to10,
      "11_to_50": low11to50,
    },
    MACRO_CENSUS_BUCKETS: macro,
    HOLD_CONCENTRATION_TOP_1: {
      pct: pct(top1),
      countries: top1.map(([c, n]) => ({ country: c, held: n })),
    },
    HOLD_CONCENTRATION_TOP_3: {
      pct: pct(top3),
      countries: top3.map(([c, n]) => ({ country: c, held: n })),
    },
    HOLD_CONCENTRATION_TOP_5: {
      pct: pct(top5),
      countries: top5.map(([c, n]) => ({ country: c, held: n })),
    },
    HOLD_TOTAL: holdTotal,
    FULL_GEOGRAPHY_COVERAGE_MATRIX: matrix,
    TOP_10_SOURCE_GAP_DISCOVERY_PRIORITIES: queue,
    BERMUDA_SCOPE_RECOMMENDATION: {
      status: GEO_STATUS.SCOPE_REVIEW,
      recommendation:
        "Do not silently include Bermuda in CALA shell/enrichment until founder decides. Reasonable hospitality-commercial inclusion candidate; not in prior Cvent LATAM registry.",
      census_count: bermuda?.census_count || 0,
      CVENT_SEARCHED: bermuda?.CVENT_SEARCHED,
      HBX_SEARCHED: bermuda?.HBX_SEARCHED,
    },
    unresolved_census_countries_sample: Object.entries(unresolvedCensusCountries)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 30)
      .map(([country, count]) => ({ country, count })),
    parent_country_encoding_leakage: parentLeakHits,
    NEXT_RECOMMENDED_ACTION: nextAction,
    FOUNDER_DECISION_REQUIRED: founderDecisionRequired,
    FOUNDER_DECISION:
      founderDecisionRequired === "YES"
        ? "Decide whether Bermuda is in-scope for Dealality CALA hospitality universe (include / exclude / defer)."
        : null,
  };

  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/full-cala-geography-coverage-registry-audit.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/full-cala-geography-coverage-registry-audit.md"
  );
  const docPath = path.join(
    ROOT,
    "docs/data-intelligence/full-cala-geography-coverage-registry-audit.md"
  );
  const registryDoc = path.join(
    ROOT,
    "docs/data-intelligence/dealality-cala-geography-registry-v1.md"
  );

  writeJson(jsonPath, report);
  const md = renderGeoCoverageMd(report);
  writeMd(mdPath, md);
  writeMd(docPath, md);
  writeMd(registryDoc, renderRegistryDoc());

  report.report_paths = {
    json: "reports/research-engine-v2/full-cala-geography-coverage-registry-audit.json",
    md: "reports/research-engine-v2/full-cala-geography-coverage-registry-audit.md",
    docs: "docs/data-intelligence/full-cala-geography-coverage-registry-audit.md",
    registry: "docs/data-intelligence/dealality-cala-geography-registry-v1.md",
    registry_module:
      "lib/research-engine-v2/dealality-cala-geography-registry-v1.js",
  };

  log(
    `[geo-coverage] STATUS=${report.AUDIT_STATUS} geos=${counts.CANONICAL_GEOGRAPHIES_TOTAL} not_searched=${counts.GEOGRAPHIES_NOT_YET_SEARCHED} gaps=${counts.GEOGRAPHIES_WITH_SOURCE_GAPS}`
  );
  return report;
}

function renderRegistryDoc() {
  const rows = DEALALITY_CALA_GEOGRAPHIES.map(
    (g) =>
      `| ${g.name} | ${g.iso_code || "—"} | ${g.region} | ${g.tourism_priority} | ${g.scope} | ${(g.aliases || []).slice(0, 4).join("; ")} |`
  ).join("\n");
  return `# Dealality CALA Geography Registry v1

**Version:** \`${DEALALITY_CALA_GEOGRAPHY_REGISTRY_VERSION}\`  
**Module:** \`lib/research-engine-v2/dealality-cala-geography-registry-v1.js\`

This registry is the canonical Dealality hospitality-commercial CALA geography universe.

It is **not** derived from HBX/Cvent inventory presence.

Territories are preserved as distinct hotel markets (e.g. Puerto Rico, Aruba, Sint Maarten vs Saint Martin).

Bermuda is \`scope_review\` until founder decision.

## Geographies

| Name | ISO | Region | Tourism priority | Scope | Aliases (sample) |
| --- | --- | --- | --- | --- | --- |
${rows}

## HBX Wave 1 searched (documented)

${HBX_WAVE1_SEARCHED_GEOGRAPHIES.map((c) => `- ${c}`).join("\n")}
`;
}

function renderGeoCoverageMd(r) {
  const matrixRows = (r.FULL_GEOGRAPHY_COVERAGE_MATRIX || [])
    .map(
      (x) =>
        `| ${x.name} | ${x.iso_code || "—"} | ${x.dealality_region} | ${x.census_count} | ${x.hbx_candidates} | ${x.cvent_candidates} | ${x.other_candidates} | ${x.hold_candidates} | ${x.shells_inserted} | ${x.HBX_SEARCHED} | ${x.CVENT_SEARCHED} | ${x.normalization_issue ? "YES" : "NO"} | ${x.coverage_status} | ${x.recommended_next_action} |`
    )
    .join("\n");

  const queueRows = (r.TOP_10_SOURCE_GAP_DISCOVERY_PRIORITIES || [])
    .map(
      (q) =>
        `| ${q.rank} | ${q.geography} | ${q.coverage_status} | ${q.tourism_priority} | ${q.census_count} | ${q.HBX_SEARCHED}/${q.CVENT_SEARCHED} | ${q.recommended_next_action} |`
    )
    .join("\n");

  return `# Full CALA Geography Coverage Registry Audit

**AUDIT_STATUS:** \`${r.AUDIT_STATUS}\`  
**Production writes:** **false**  
**Census:** **${r.PRODUCTION_CENSUS_COUNT}**  
**Generated:** ${r.generated_at}

## Executive return

| Field | Value |
| --- | ---: |
| CANONICAL_GEOGRAPHIES_TOTAL | ${r.CANONICAL_GEOGRAPHIES_TOTAL} |
| GEOGRAPHIES_WITH_CENSUS_RECORDS | ${r.GEOGRAPHIES_WITH_CENSUS_RECORDS} |
| GEOGRAPHIES_WITH_ZERO_CENSUS_RECORDS | ${r.GEOGRAPHIES_WITH_ZERO_CENSUS_RECORDS} |
| GEOGRAPHIES_HBX_SEARCHED | ${r.GEOGRAPHIES_HBX_SEARCHED} |
| GEOGRAPHIES_CVENT_SEARCHED | ${r.GEOGRAPHIES_CVENT_SEARCHED} |
| GEOGRAPHIES_NOT_YET_SEARCHED | ${r.GEOGRAPHIES_NOT_YET_SEARCHED} |
| GEOGRAPHIES_WITH_SOURCE_GAPS | ${r.GEOGRAPHIES_WITH_SOURCE_GAPS} |
| GEOGRAPHIES_WITH_NORMALIZATION_PROBLEMS | ${r.GEOGRAPHIES_WITH_NORMALIZATION_PROBLEMS} |
| HOLD_CONCENTRATION_TOP_1 | ${r.HOLD_CONCENTRATION_TOP_1?.pct}% |
| HOLD_CONCENTRATION_TOP_3 | ${r.HOLD_CONCENTRATION_TOP_3?.pct}% |
| HOLD_CONCENTRATION_TOP_5 | ${r.HOLD_CONCENTRATION_TOP_5?.pct}% |
| NEXT_RECOMMENDED_ACTION | ${r.NEXT_RECOMMENDED_ACTION} |
| FOUNDER_DECISION_REQUIRED | ${r.FOUNDER_DECISION_REQUIRED} |

${r.FOUNDER_DECISION ? `**Founder decision:** ${r.FOUNDER_DECISION}` : ""}

## Critical distinction

${r.search_evidence?.distinction || ""}

- HBX Wave1 searched: ${(r.search_evidence?.hbx?.searched_geographies || []).join(", ")}
- Cvent registry seeded ~${r.search_evidence?.cvent?.countries_seeded}; excluded after probe: ${(r.search_evidence?.cvent?.excluded_after_probe || []).join(", ") || "—"}

## Macro Census distortion check

| Bucket | Count |
| --- | ---: |
${Object.entries(r.MACRO_CENSUS_BUCKETS || {})
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join("\n")}

## Top 15 Census geographies

${(r.TOP_15_CENSUS_GEOGRAPHIES || []).map((x, i) => `${i + 1}. **${x.geography}** — ${x.census_count}`).join("\n")}

## Zero-record geographies

${(r.ZERO_RECORD_GEOGRAPHIES || []).map((x) => `- ${x}`).join("\n") || "_none_"}

## HOLD concentration

- Top 1: **${r.HOLD_CONCENTRATION_TOP_1?.pct}%** — ${(r.HOLD_CONCENTRATION_TOP_1?.countries || []).map((c) => `${c.country} (${c.held})`).join(", ")}
- Top 3: **${r.HOLD_CONCENTRATION_TOP_3?.pct}%**
- Top 5: **${r.HOLD_CONCENTRATION_TOP_5?.pct}%**

A large HOLD universe dominated by Brazil/Mexico does **not** prove Caribbean + rest-of-LATAM discovery coverage.

## Top 10 source-gap discovery priorities

| Rank | Geography | Status | Tourism | Census | HBX/Cvent searched | Action |
| --- | --- | --- | --- | ---: | --- | --- |
${queueRows}

## Bermuda

${JSON.stringify(r.BERMUDA_SCOPE_RECOMMENDATION, null, 2)}

## Full geography coverage matrix

| Geography | ISO | Region | Census | HBX | Cvent | Other | HOLD | Shells | HBX searched? | Cvent searched? | Norm issue? | Status | Next action |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- |
${matrixRows}

## Safety

- Read-only — no Census / Brand Explorer / Brand Setup / VIC writes
- No shell inserts, enrichment, brand promotion, or HOLD enrichment
- SAFE+HBX identity gate not weakened
`;
}
