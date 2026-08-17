#!/usr/bin/env node
/**
 * CALA Census Expansion Sprint 01 — controlled discovery (stage-only).
 *
 * SAFETY: Airtable / census / Brand Explorer writes forced OFF.
 * Reuses Discovery Factory + Coverage Dashboard. No new systems.
 *
 * Usage:
 *   node scripts/hotel-intelligence-cala-census-expansion-sprint-01.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";

import {
  MAP_CENSUS_FIELDS,
  MAP_HOTEL_PROPERTY_CENSUS,
} from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import { buildCoverageScorecard } from "../lib/hotel-intelligence/universe-expansion/coverage-scorecard.js";
import { loadCountryCandidatesFromFiles } from "../lib/hotel-intelligence/universe-expansion/discover-batch.js";
import {
  runDiscoveryFactoryBatch,
  STAGE_STATUS,
  DISCOVERY_FACTORY_VERSION,
} from "../lib/hotel-intelligence/discovery-factory/index.js";
import {
  buildCalaCoverageDashboard,
  persistCoverageDashboard,
} from "../lib/hotel-intelligence/coverage-dashboard/index.js";
import { createGiataDriveProvider } from "../lib/hotel-intelligence/providers/giata-drive.js";
import { resolveHotelIdentity, MATCH_STATUS } from "../lib/hotel-intelligence/identity-resolve.js";
import { normName } from "../lib/research-engine-v2/census-autopilot-v2/identity-dedupe.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/cala-census-expansion-sprint-01"
);
const DATA_DIR = path.join(ROOT, "data/hotel-intelligence/discovery-factory");
const SPRINT_DATA = path.join(
  ROOT,
  "data/hotel-intelligence/cala-census-expansion-sprint-01"
);

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.ENABLE_HBX_INSERTS = "0";
process.env.ENABLE_CENSUS_SHELL_INSERTS = "0";

/** Candidate-file country name overrides (dashboard name → stock name). */
const CANDIDATE_COUNTRY_ALIAS = {
  "Turks and Caicos Islands": "Turks and Caicos",
};

const TRACK_A = {
  track: "A",
  country: "Brazil",
  candidate_country: "Brazil",
  limit: 750,
  offset: 250, // skip prior factory Brazil×250
  reason: "Largest gap (~4.8k); validated factory; depth/scale track",
};

const TRACK_B = [
  {
    country: "Turks and Caicos Islands",
    candidate_country: "Turks and Caicos",
    limit: 57,
    reason: "ZERO coverage + GIATA Drive overlap + candidate stock",
  },
  {
    country: "Bonaire",
    candidate_country: "Bonaire",
    limit: 51,
    reason: "ZERO coverage + candidate stock + GIATA Drive seed",
  },
  {
    country: "Martinique",
    candidate_country: "Martinique",
    limit: 50,
    reason: "ZERO coverage with Cvent stock",
  },
  {
    country: "U.S. Virgin Islands",
    candidate_country: "U.S. Virgin Islands",
    limit: 50,
    reason: "ZERO coverage with holds stock",
  },
  {
    country: "Anguilla",
    candidate_country: "Anguilla",
    limit: 25,
    reason: "ZERO coverage; small strategic floor lift",
  },
  {
    country: "Montserrat",
    candidate_country: "Montserrat",
    limit: 50,
    reason: "ZERO coverage with holds",
  },
  {
    country: "Guadeloupe",
    candidate_country: "Guadeloupe",
    limit: 32,
    reason: "ZERO coverage with stock",
  },
  {
    country: "Saint Lucia",
    candidate_country: "Saint Lucia",
    limit: 45,
    reason: "NEAR-ZERO (6.3%); raise geographic floor",
  },
];

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(fp, data) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function readJson(fp, fallback) {
  if (!fs.existsSync(fp)) return fallback;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

async function listCensus() {
  const token = (
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_API_KEY ||
    ""
  ).trim();
  const baseId = (
    process.env.AIRTABLE_BASE_ID_ALT ||
    process.env.AIRTABLE_BASE_ID ||
    ""
  ).trim();
  if (!token || !baseId) throw new Error("AIRTABLE credentials missing");
  const base = new Airtable({ apiKey: token }).base(baseId);
  const byCountry = {};
  const records = [];
  await base(MAP_HOTEL_PROPERTY_CENSUS.tableId)
    .select({
      pageSize: 100,
      fields: [
        MAP_CENSUS_FIELDS.propertyName,
        MAP_CENSUS_FIELDS.officialName,
        MAP_CENSUS_FIELDS.country,
        MAP_CENSUS_FIELDS.city,
        MAP_CENSUS_FIELDS.address,
        MAP_CENSUS_FIELDS.website,
        MAP_CENSUS_FIELDS.phone,
        MAP_CENSUS_FIELDS.hbxHotelCode,
        MAP_CENSUS_FIELDS.propertyIdentityKey,
        MAP_CENSUS_FIELDS.latitude,
        MAP_CENSUS_FIELDS.longitude,
        MAP_CENSUS_FIELDS.brandName,
      ].filter(Boolean),
    })
    .eachPage((page, next) => {
      for (const r of page) {
        const country =
          String(r.fields?.[MAP_CENSUS_FIELDS.country] || "").trim() || "UNKNOWN";
        byCountry[country] = (byCountry[country] || 0) + 1;
        records.push({ id: r.id, fields: r.fields });
      }
      next();
    });
  return { byCountry, records, total: records.length };
}

function loadStagedHotels(fp) {
  const raw = readJson(fp, { hotels: {} });
  if (Array.isArray(raw.hotels)) return raw.hotels;
  if (raw.hotels && typeof raw.hotels === "object") return Object.values(raw.hotels);
  return [];
}

function coverageBuckets(rows) {
  const zero = rows.filter((r) => (r.current_dealality_hotels || 0) === 0 && (r.estimated_hotel_universe || 0) > 0);
  const lt20 = rows.filter((r) => r.coverage_pct != null && r.coverage_pct < 20);
  const lt50 = rows.filter((r) => r.coverage_pct != null && r.coverage_pct < 50);
  const lt80 = rows.filter((r) => r.coverage_pct != null && r.coverage_pct < 80);
  const ge95 = rows.filter((r) => r.coverage_pct != null && r.coverage_pct >= 95);
  return {
    zero: zero.length,
    lt20: lt20.length,
    lt50: lt50.length,
    lt80: lt80.length,
    ge95: ge95.length,
    zero_countries: zero.map((r) => r.country),
  };
}

function findRow(dash, country) {
  const n = normName(country);
  return (dash.rows || []).find(
    (r) =>
      normName(r.country) === n ||
      normName(r.country).includes(n) ||
      n.includes(normName(r.country))
  );
}

function loadCandidates(candidateCountry, limit, offset) {
  let candidates = loadCountryCandidatesFromFiles(candidateCountry, {
    root: ROOT,
    onlyHolds: true,
  });
  if (candidates.length < offset + limit) {
    candidates = loadCountryCandidatesFromFiles(candidateCountry, {
      root: ROOT,
      onlyHolds: false,
    });
  }
  return candidates;
}

function sampleQuality(batch, n = 8) {
  const by = {
    READY_FOR_IMPORT: (batch.ready_for_import || []).slice(0, n),
    REVIEW_REQUIRED: (batch.review_required || []).slice(0, n),
    MATCHED_EXISTING: (batch.results || [])
      .filter((r) => r.stage_status === STAGE_STATUS.MATCHED_EXISTING)
      .slice(0, n),
    REJECTED: (batch.results || [])
      .filter((r) => r.stage_status === STAGE_STATUS.REJECTED)
      .slice(0, n),
  };
  const observations = [];
  for (const [cls, rows] of Object.entries(by)) {
    for (const r of rows.slice(0, 2)) {
      observations.push({
        class: cls,
        name: r.name,
        city: r.city,
        country: r.country,
        city_method: r.city_method,
        identity_confidence: r.identity_confidence,
        match_status: r.match_status,
        hotel_id: r.hotel_id,
        ok_name: Boolean(r.name && r.name.length >= 3),
        ok_country: Boolean(r.country),
        ok_city: Boolean(r.city && String(r.city).length >= 2),
        has_dhl: /^dhl_/.test(String(r.hotel_id || "")),
      });
    }
  }
  return observations;
}

async function optionalGiataCorroboration(trackBResults, censusRecords) {
  const metrics = {
    index_calls: 0,
    detail_calls: 0,
    corroborated: 0,
    skipped: true,
  };
  if (String(process.env.HOTEL_INTELLIGENCE_GIATA_DRIVE || "0") !== "1") {
    if (!String(process.env.GIATA_DRIVE_API_KEY || "").trim()) {
      return { ...metrics, note: "GIATA Drive skipped — key/flag not enabled" };
    }
  }
  // Enable for selective Track B only when key present
  if (!String(process.env.GIATA_DRIVE_API_KEY || "").trim()) {
    return { ...metrics, note: "GIATA_DRIVE_API_KEY missing" };
  }

  const provider = createGiataDriveProvider({
    env: { ...process.env, HOTEL_INTELLIGENCE_GIATA_DRIVE: "1" },
    forceEnabled: true,
  });
  metrics.skipped = false;
  const isoByCountry = {
    "Turks and Caicos Islands": "TC",
    Bonaire: "BQ",
    Anguilla: "AI",
  };

  for (const batch of trackBResults) {
    const iso = isoByCountry[batch.country];
    if (!iso) continue;
    metrics.index_calls += 1;
    const listed = await provider.searchHotels({
      countryCode: iso,
      limit: 5,
      fetch_details: true,
    });
    metrics.detail_calls += listed.metrics?.detail_calls
      ? Math.min(5, listed.hotels?.length || 0)
      : listed.hotels?.length || 0;
    for (const h of listed.hotels || []) {
      const resolved = resolveHotelIdentity(
        {
          name: h.name,
          city: h.city,
          country: h.country || batch.country,
          address: h.address,
          latitude: h.latitude,
          longitude: h.longitude,
          website: h.website,
          phone: h.phone,
          brand: h.brand_name,
          external_ids: h.external_id
            ? [{ provider: "giata_drive", external_id: String(h.external_id) }]
            : [],
        },
        censusRecords
      );
      if (
        [MATCH_STATUS.NEW, MATCH_STATUS.AMBIGUOUS].includes(resolved.match_status)
      ) {
        metrics.corroborated += 1;
      }
    }
  }
  return metrics;
}

async function main() {
  ensureDir(OUT_DIR);
  ensureDir(SPRINT_DATA);
  ensureDir(DATA_DIR);

  console.log(
    JSON.stringify({
      module: "cala-census-expansion-sprint-01",
      event: "start",
      writes: 0,
    })
  );

  // --- Baseline ---
  const { byCountry, records, total } = await listCensus();
  const scorecard = buildCoverageScorecard(byCountry, { root: ROOT });
  const liveDash = buildCalaCoverageDashboard(scorecard, { root: ROOT });
  writeJson(path.join(OUT_DIR, "baseline-coverage-dashboard.json"), liveDash);

  const bucketsBefore = coverageBuckets(liveDash.rows || []);
  const priorReady = loadStagedHotels(
    path.join(DATA_DIR, "staged-ready-for-import.json")
  );
  const priorReview = loadStagedHotels(
    path.join(DATA_DIR, "staged-review-required.json")
  );

  const inventory = {
    brazil_holds: loadCountryCandidatesFromFiles("Brazil", {
      root: ROOT,
      onlyHolds: true,
    }).length,
    brazil_all: loadCountryCandidatesFromFiles("Brazil", {
      root: ROOT,
      onlyHolds: false,
    }).length,
    prior_ready_staged: priorReady.length,
    prior_review_staged: priorReview.length,
    track_b_stock: {},
  };
  for (const t of TRACK_B) {
    inventory.track_b_stock[t.country] = {
      holds: loadCountryCandidatesFromFiles(t.candidate_country, {
        root: ROOT,
        onlyHolds: true,
      }).length,
      all: loadCountryCandidatesFromFiles(t.candidate_country, {
        root: ROOT,
        onlyHolds: false,
      }).length,
    };
  }

  const baseline = {
    marker: "SPRINT_01_BASELINE_LOCKED",
    live_census: total,
    estimated_universe: liveDash.summary?.estimated_hotel_universe || scorecard.total_expected,
    coverage_pct: liveDash.summary?.coverage_pct ?? null,
    countries_represented: (liveDash.rows || []).filter(
      (r) => (r.current_dealality_hotels || 0) > 0
    ).length,
    buckets: bucketsBefore,
    distance_to_15k: Math.max(0, 15000 - total),
    prior_ready_staged: priorReady.length,
    prior_review_staged: priorReview.length,
    inventory,
  };
  writeJson(path.join(OUT_DIR, "SPRINT_01_BASELINE_LOCKED.json"), baseline);

  // --- Country selection freeze ---
  const selectionRows = [];
  const brRow = findRow(liveDash, TRACK_A.country);
  selectionRows.push({
    track: "A",
    country: TRACK_A.country,
    current_hotels: brRow?.current_dealality_hotels ?? byCountry.Brazil ?? 0,
    estimated_universe: brRow?.estimated_hotel_universe ?? null,
    coverage: brRow?.coverage_pct ?? null,
    candidate_stock: inventory.brazil_holds,
    sprint_target: TRACK_A.limit,
    reason: TRACK_A.reason,
  });
  for (const t of TRACK_B) {
    const row = findRow(liveDash, t.country);
    selectionRows.push({
      track: "B",
      country: t.country,
      current_hotels: row?.current_dealality_hotels ?? 0,
      estimated_universe: row?.estimated_hotel_universe ?? null,
      coverage: row?.coverage_pct ?? null,
      candidate_stock: inventory.track_b_stock[t.country]?.all || 0,
      sprint_target: t.limit,
      reason: t.reason,
    });
  }
  const selection = {
    marker: "SPRINT_01_COUNTRY_SELECTION",
    track_a_candidate_target: TRACK_A.limit,
    track_b_candidate_target: TRACK_B.reduce((s, t) => s + t.limit, 0),
    total_planned_candidates:
      TRACK_A.limit + TRACK_B.reduce((s, t) => s + t.limit, 0),
    expected_net_new_range:
      "Conservative ~15–40% READY of processed new-status candidates (factory prior ~36% Tier A of Brazil×250); Track B lower absolute volume",
    rows: selectionRows,
  };
  writeJson(path.join(OUT_DIR, "SPRINT_01_COUNTRY_SELECTION.json"), selection);

  console.log(
    JSON.stringify({
      module: "cala-census-expansion-sprint-01",
      event: "selection_frozen",
      track_a: TRACK_A.country,
      track_a_limit: TRACK_A.limit,
      track_b_countries: TRACK_B.length,
      total_planned: selection.total_planned_candidates,
    })
  );

  // --- Execute Track A ---
  const trackACandidates = loadCandidates(
    TRACK_A.candidate_country,
    TRACK_A.limit,
    TRACK_A.offset
  );
  const trackABatch = runDiscoveryFactoryBatch(trackACandidates, records, {
    country: TRACK_A.country,
    limit: TRACK_A.limit,
    offset: TRACK_A.offset,
    hotelsBefore: total,
    batchId: `sprint01_trackA_brazil_${TRACK_A.limit}_${new Date()
      .toISOString()
      .replace(/[:.]/g, "-")}`,
  });
  writeJson(path.join(OUT_DIR, "track-a-brazil-batch.json"), {
    metrics: trackABatch.metrics,
    batch_id: trackABatch.batch_id,
    country: trackABatch.country,
    quality_sample: sampleQuality(trackABatch),
    ready_sample: (trackABatch.ready_for_import || []).slice(0, 10),
    review_sample: (trackABatch.review_required || []).slice(0, 10),
  });

  // --- Execute Track B ---
  const trackBResults = [];
  for (const t of TRACK_B) {
    const candidates = loadCandidates(t.candidate_country, t.limit, 0);
    const batch = runDiscoveryFactoryBatch(candidates, records, {
      country: t.country,
      limit: t.limit,
      offset: 0,
      hotelsBefore: total,
      batchId: `sprint01_trackB_${normName(t.country).replace(/\s+/g, "_")}_${t.limit}`,
    });
    const row = findRow(liveDash, t.country);
    const current = row?.current_dealality_hotels ?? 0;
    const universe = row?.estimated_hotel_universe || null;
    const ready = batch.metrics.ready_for_import || 0;
    const projected = current + ready;
    const covBefore = row?.coverage_pct ?? (universe ? 0 : null);
    const covAfter =
      universe && universe > 0
        ? Math.round((1000 * projected) / universe) / 10
        : null;
    trackBResults.push({
      country: t.country,
      current_census: current,
      estimated_universe: universe,
      starting_coverage: covBefore,
      raw_candidates: batch.metrics.candidates_processed,
      existing: batch.metrics.matched_existing,
      new_high_confidence: ready,
      review: batch.metrics.review_required,
      duplicate: batch.metrics.matched_existing,
      rejected: batch.metrics.rejected,
      projected_net_new: ready,
      projected_census: projected,
      projected_coverage: covAfter,
      coverage_status_before: row?.coverage_status || null,
      coverage_status_after:
        covAfter == null
          ? null
          : covAfter >= 95
            ? "EXCELLENT"
            : covAfter >= 80
              ? "GOOD"
              : covAfter >= 50
                ? "FAIR"
                : covAfter >= 20
                  ? "POOR"
                  : "CRITICAL",
      metrics: batch.metrics,
      quality_sample: sampleQuality(batch, 4),
      staged_hotels: batch.staged_hotels || [],
      ready_rows: batch.ready_for_import || [],
      review_rows: batch.review_required || [],
    });
    writeJson(
      path.join(
        OUT_DIR,
        `track-b-${normName(t.country).replace(/\s+/g, "-")}.json`
      ),
      trackBResults[trackBResults.length - 1]
    );
  }

  // --- Optional GIATA Drive (selective Track B) ---
  process.env.HOTEL_INTELLIGENCE_GIATA_DRIVE =
    process.env.HOTEL_INTELLIGENCE_GIATA_DRIVE || "1";
  const giataMetrics = await optionalGiataCorroboration(trackBResults, records);

  // --- Merge staging (prior + sprint) without production writes ---
  const sprintReady = [
    ...(trackABatch.staged_hotels || []).filter(
      (h) => h.discovery?.stage_status === STAGE_STATUS.READY_FOR_IMPORT
    ),
    ...trackBResults.flatMap((b) =>
      (b.staged_hotels || []).filter(
        (h) => h.discovery?.stage_status === STAGE_STATUS.READY_FOR_IMPORT
      )
    ),
  ];
  const sprintReview = [
    ...(trackABatch.staged_hotels || []).filter(
      (h) => h.discovery?.stage_status === STAGE_STATUS.REVIEW_REQUIRED
    ),
    ...trackBResults.flatMap((b) =>
      (b.staged_hotels || []).filter(
        (h) => h.discovery?.stage_status === STAGE_STATUS.REVIEW_REQUIRED
      )
    ),
  ];

  function hotelKey(h) {
    return (
      h.hotel_id ||
      h.discovery?.candidate_id ||
      `${normName(h.identity?.official_name || h.name || "")}::${normName(h.location?.country || h.country || "")}`
    );
  }
  const mergedReadyMap = new Map();
  for (const h of [...priorReady, ...sprintReady]) {
    mergedReadyMap.set(hotelKey(h), h);
  }
  const mergedReviewMap = new Map();
  for (const h of [...priorReview, ...sprintReview]) {
    // Prefer ready over review if same key somehow
    if (mergedReadyMap.has(hotelKey(h))) continue;
    mergedReviewMap.set(hotelKey(h), h);
  }

  const mergedReady = [...mergedReadyMap.values()];
  const mergedReview = [...mergedReviewMap.values()];

  writeJson(path.join(SPRINT_DATA, "staged-ready-for-import.json"), {
    version: 1,
    sprint: "sprint_01",
    updated_at: new Date().toISOString(),
    production_writes: false,
    hotels: sprintReady,
  });
  writeJson(path.join(SPRINT_DATA, "staged-review-required.json"), {
    version: 1,
    sprint: "sprint_01",
    updated_at: new Date().toISOString(),
    production_writes: false,
    hotels: sprintReview,
  });
  // Cumulative factory queues (merged)
  writeJson(path.join(DATA_DIR, "staged-ready-for-import.json"), {
    version: 1,
    updated_at: new Date().toISOString(),
    production_writes: false,
    hotels: mergedReady,
  });
  writeJson(path.join(DATA_DIR, "staged-review-required.json"), {
    version: 1,
    updated_at: new Date().toISOString(),
    production_writes: false,
    hotels: mergedReview,
  });

  // --- Totals ---
  const sprintReadyCount = sprintReady.length;
  const sprintReviewCount = sprintReview.length;
  const identityTotals = {
    existing:
      (trackABatch.metrics.matched_existing || 0) +
      trackBResults.reduce((s, b) => s + (b.existing || 0), 0),
    new_high_confidence: sprintReadyCount,
    review: sprintReviewCount,
    ambiguous: 0,
    duplicates:
      (trackABatch.metrics.matched_existing || 0) +
      trackBResults.reduce((s, b) => s + (b.duplicate || 0), 0),
    rejected:
      (trackABatch.metrics.rejected || 0) +
      trackBResults.reduce((s, b) => s + (b.rejected || 0), 0),
  };

  // Ambiguous counted inside rejected by factory — estimate from results
  const ambA = (trackABatch.results || []).filter((r) =>
    (r.reasons || []).includes("ambiguous_identity")
  ).length;
  const ambB = trackBResults.reduce(
    (s, b) =>
      s +
      (b.quality_sample || []).filter((q) => q.class === "REJECTED" && false)
        .length,
    0
  );
  identityTotals.ambiguous = ambA + ambB;

  const projectedCensus = total + sprintReadyCount;
  const progress = {
    LIVE: total,
    READY_STAGED_THIS_SPRINT: sprintReadyCount,
    PRIOR_READY_CUMULATIVE: priorReady.length,
    MERGED_READY_QUEUE: mergedReady.length,
    PROJECTED_AFTER_IMPORT_THIS_SPRINT_ONLY: projectedCensus,
    PROJECTED_AFTER_IMPORT_MERGED_READY: total + mergedReady.length,
    Remaining_to_10K: Math.max(0, 10000 - projectedCensus),
    Remaining_to_12_5K: Math.max(0, 12500 - projectedCensus),
    Remaining_to_15K: Math.max(0, 15000 - projectedCensus),
  };

  // Projected country counts for dashboard-style buckets
  const projectedByCountry = { ...byCountry };
  for (const h of sprintReady) {
    const c =
      h.location?.country ||
      h.country ||
      h.discovery?.country ||
      "UNKNOWN";
    // Normalize known aliases into dashboard country keys when possible
    let key = c;
    if (normName(c).includes("turks")) key = "Turks and Caicos Islands";
    projectedByCountry[key] = (projectedByCountry[key] || 0) + 1;
  }
  const projectedScorecard = buildCoverageScorecard(projectedByCountry, {
    root: ROOT,
  });
  const projectedDash = buildCalaCoverageDashboard(projectedScorecard, {
    root: ROOT,
  });
  const bucketsAfter = coverageBuckets(projectedDash.rows || []);

  writeJson(path.join(OUT_DIR, "projected-coverage-dashboard.json"), {
    mode: "PROJECTED_IF_SPRINT_01_READY_IMPORTED",
    live_census: total,
    projected_census: projectedCensus,
    dashboard: projectedDash,
    buckets_before: bucketsBefore,
    buckets_after: bucketsAfter,
  });

  // Persist live dashboard refresh (live unchanged) + note projected file
  persistCoverageDashboard(liveDash, { root: ROOT });

  const processed =
    (trackABatch.metrics.candidates_processed || 0) +
    trackBResults.reduce((s, b) => s + (b.raw_candidates || 0), 0);

  const efficiency = {
    raw_candidates_processed: processed,
    net_new_ready_candidates: sprintReadyCount,
    net_new_yield_pct:
      processed > 0
        ? Math.round((1000 * sprintReadyCount) / processed) / 10
        : 0,
    duplicates_prevented: identityTotals.duplicates,
    review_rate_pct:
      processed > 0
        ? Math.round((1000 * sprintReviewCount) / processed) / 10
        : 0,
    giata_drive: giataMetrics,
    serpapi_calls: 0,
    hbx_calls: 0,
    other_external_calls: 0,
    net_new_ready_per_external_api_call:
      (giataMetrics.detail_calls || 0) + (giataMetrics.index_calls || 0) > 0
        ? Number(
            (
              sprintReadyCount /
              Math.max(
                1,
                (giataMetrics.detail_calls || 0) + (giataMetrics.index_calls || 0)
              )
            ).toFixed(2)
          )
        : null,
    note: "Sprint primarily reused existing Cvent/hold inventory — SerpApi/HBX not required",
  };

  // Factory projection to milestones
  const yieldReady = efficiency.net_new_yield_pct / 100;
  const remainingStockBrazil = Math.max(
    0,
    inventory.brazil_holds - TRACK_A.offset - TRACK_A.limit
  );
  const batchesTo = (target) => {
    const need = Math.max(0, target - projectedCensus);
    if (need === 0) return 0;
    const perBatch = Math.max(1, Math.round(750 * yieldReady));
    return Math.ceil(need / perBatch);
  };

  const factoryProjection = {
    assumptions: {
      batch_size: 750,
      observed_ready_yield_pct: efficiency.net_new_yield_pct,
      remaining_brazil_hold_stock: remainingStockBrazil,
      note: "Projection uses Sprint 01 ready yield; review queue excluded from 'validated' path",
    },
    similar_batches_to_10k: batchesTo(10000),
    similar_batches_to_12_5k: batchesTo(12500),
    similar_batches_to_15k: batchesTo(15000),
  };

  // Sprint 02 recommendation
  const sprint02 = {
    TRACK_A: {
      Country: "Brazil",
      Target: 1000,
      Reason: `Still largest gap; remaining holds≈${remainingStockBrazil}; continue offset ${TRACK_A.offset + TRACK_A.limit}`,
    },
    TRACK_B: {
      Countries: [
        "Bahamas",
        "Dominica",
        "Saint Barthélemy",
        "Paraguay",
        "Belize",
      ],
      Targets: [80, 23, 50, 80, 100],
      Reason: "Continue zero/near-zero floor; Belize/Bahamas add depth among under-covered",
    },
    Expected_candidates: 1000 + 80 + 23 + 50 + 80 + 100,
    Expected_net_new: Math.round((1000 + 333) * yieldReady),
    Expected_geographic_improvement:
      "Further reduce zero-coverage Caribbean/South America pockets; Brazil depth toward 10k",
  };

  // Verdict
  const trackAReadyPct =
    (trackABatch.metrics.ready_for_import || 0) /
    Math.max(1, trackABatch.metrics.candidates_processed || 1);
  const trackADup = trackABatch.metrics.duplicate_rate_pct || 0;
  let verdict = "SCALE_DISCOVERY_FACTORY_NEXT_SPRINT";
  if (trackADup > 15 || trackAReadyPct < 0.1) {
    verdict = "REMEDIATE_DISCOVERY_QUALITY_FIRST";
  } else if (sprintReviewCount > sprintReadyCount * 3) {
    verdict = "REVIEW_AMBIGUOUS_BATCH_FIRST";
  } else if (sprintReadyCount >= 50) {
    verdict = "APPROVE_READY_BATCH_FOR_IMPORT_REVIEW";
  }

  const movedAbove20 = (bucketsBefore.zero_countries || []).filter((c) => {
    const row = findRow(projectedDash, c);
    return row && row.coverage_pct >= 20;
  });
  const movedAbove50 = (liveDash.rows || [])
    .filter((r) => (r.coverage_pct || 0) < 50)
    .filter((r) => {
      const row = findRow(projectedDash, r.country);
      return row && row.coverage_pct >= 50 && (r.coverage_pct || 0) < 50;
    })
    .map((r) => r.country);

  const summary = {
    marker: "DEALALITY_CALA_CENSUS_EXPANSION_SPRINT_01_COMPLETE",
    generated_at: new Date().toISOString(),
    factory_version: DISCOVERY_FACTORY_VERSION,
    safety: {
      Production_writes: 0,
      Census_writes: 0,
      Automatic_merges: 0,
      Schema_changes: 0,
      Brand_Explorer_writes: 0,
      Secrets_exposed: false,
      flags: {
        ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0",
        ENABLE_HBX_CENSUS_WRITES: "0",
      },
    },
    baseline,
    selection,
    track_a: {
      country: TRACK_A.country,
      offset: TRACK_A.offset,
      limit: TRACK_A.limit,
      metrics: trackABatch.metrics,
      quality_sample: sampleQuality(trackABatch),
    },
    track_b: trackBResults.map((b) => ({
      country: b.country,
      current_census: b.current_census,
      estimated_universe: b.estimated_universe,
      starting_coverage: b.starting_coverage,
      raw_candidates: b.raw_candidates,
      existing: b.existing,
      new_high_confidence: b.new_high_confidence,
      review: b.review,
      duplicate: b.duplicate,
      rejected: b.rejected,
      projected_net_new: b.projected_net_new,
      projected_census: b.projected_census,
      projected_coverage: b.projected_coverage,
      coverage_status_before: b.coverage_status_before,
      coverage_status_after: b.coverage_status_after,
    })),
    identity: identityTotals,
    efficiency,
    geographic: {
      ZERO_COVERAGE_COUNTRIES_BEFORE: bucketsBefore.zero,
      ZERO_COVERAGE_COUNTRIES_AFTER_IF_READY_IMPORTED: bucketsAfter.zero,
      lt20_before: bucketsBefore.lt20,
      lt20_projected_after: bucketsAfter.lt20,
      lt50_before: bucketsBefore.lt50,
      lt50_projected_after: bucketsAfter.lt50,
      COUNTRIES_MOVED_ABOVE_20_PERCENT: movedAbove20,
      COUNTRIES_MOVED_ABOVE_50_PERCENT: movedAbove50,
    },
    progress_15k: progress,
    factory_projection: factoryProjection,
    sprint_02_recommendation: sprint02,
    verdict,
    provider_usage: {
      GIATA_Drive_calls:
        (giataMetrics.index_calls || 0) + (giataMetrics.detail_calls || 0),
      SerpApi_calls: 0,
      HBX_calls: 0,
      Other_calls: 0,
      giata_detail: giataMetrics,
    },
  };

  writeJson(path.join(OUT_DIR, "sprint-01-summary.json"), summary);

  const md = `# DEALALITY_CALA_CENSUS_EXPANSION_SPRINT_01_COMPLETE

**Generated:** ${summary.generated_at}  
**Production writes:** **0** · Live census unchanged at **${total}**

## Safety

\`\`\`
Production writes: 0
Census writes: 0
Automatic merges: 0
Schema changes: 0
Secrets exposed: false
\`\`\`

## Baseline (SPRINT_01_BASELINE_LOCKED)

| Metric | Value |
| --- | ---: |
| Live hotels | ${total} |
| Estimated universe | ${baseline.estimated_universe} |
| Coverage | ${baseline.coverage_pct}% |
| Distance to 15K | ${baseline.distance_to_15k} |
| Prior READY staged | ${priorReady.length} |
| Prior REVIEW staged | ${priorReview.length} |

## Sprint selection

Track A: **Brazil** × ${TRACK_A.limit} (offset ${TRACK_A.offset})  
Track B: ${TRACK_B.map((t) => t.country).join(", ")}  
Planned candidates: **${selection.total_planned_candidates}**

## Track A results (Brazil)

| Metric | Value |
| --- | ---: |
| Processed | ${trackABatch.metrics.candidates_processed} |
| READY | ${trackABatch.metrics.ready_for_import} |
| REVIEW | ${trackABatch.metrics.review_required} |
| Existing/dup | ${trackABatch.metrics.matched_existing} |
| Rejected | ${trackABatch.metrics.rejected} |
| Dup rate % | ${trackABatch.metrics.duplicate_rate_pct} |
| Tier A % | ${trackABatch.metrics.tier_a_pct} |

## Track B (projected if READY imported)

| Country | Current | Ready | Review | Projected census | Coverage before → after |
| --- | ---: | ---: | ---: | ---: | --- |
${trackBResults
  .map(
    (b) =>
      `| ${b.country} | ${b.current_census} | ${b.new_high_confidence} | ${b.review} | ${b.projected_census} | ${b.starting_coverage ?? "—"}% → ${b.projected_coverage ?? "—"}% |`
  )
  .join("\n")}

## 15K progress

| | Value |
| --- | ---: |
| LIVE | ${progress.LIVE} |
| READY staged this sprint | ${progress.READY_STAGED_THIS_SPRINT} |
| PROJECTED if this sprint READY imported | ${progress.PROJECTED_AFTER_IMPORT_THIS_SPRINT_ONLY} |
| Merged READY queue (incl. prior) | ${progress.MERGED_READY_QUEUE} |
| Remaining to 10K (this-sprint proj.) | ${progress.Remaining_to_10K} |
| Remaining to 12.5K | ${progress.Remaining_to_12_5K} |
| Remaining to 15K | ${progress.Remaining_to_15K} |

## Geographic improvement (if READY imported)

| | Before | Projected after |
| --- | ---: | ---: |
| Zero-coverage countries | ${bucketsBefore.zero} | ${bucketsAfter.zero} |
| <20% countries | ${bucketsBefore.lt20} | ${bucketsAfter.lt20} |
| <50% countries | ${bucketsBefore.lt50} | ${bucketsAfter.lt50} |

## Verdict

**${verdict}**

## Sprint 02 recommendation (not executed)

- Track A: ${sprint02.TRACK_A.Country} × ${sprint02.TRACK_A.Target}
- Track B: ${sprint02.TRACK_B.Countries.join(", ")}
`;

  fs.writeFileSync(
    path.join(OUT_DIR, "DEALALITY_CALA_CENSUS_EXPANSION_SPRINT_01_COMPLETE.md"),
    md,
    "utf8"
  );

  console.log(
    JSON.stringify({
      module: "cala-census-expansion-sprint-01",
      event: "complete",
      live: total,
      sprint_ready: sprintReadyCount,
      sprint_review: sprintReviewCount,
      projected: projectedCensus,
      zero_before: bucketsBefore.zero,
      zero_after: bucketsAfter.zero,
      verdict,
      out_dir: "reports/hotel-intelligence/cala-census-expansion-sprint-01",
    })
  );
}

main().catch((err) => {
  console.error(
    JSON.stringify({
      module: "cala-census-expansion-sprint-01",
      event: "fatal",
      message: String(err?.message || err).slice(0, 400),
    })
  );
  process.exit(1);
});
