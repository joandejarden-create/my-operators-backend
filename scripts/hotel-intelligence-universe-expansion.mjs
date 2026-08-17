#!/usr/bin/env node
/**
 * Dealality Hotel Universe Expansion — discovery only (coverage, not enrichment).
 *
 * SAFETY: ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES forced to 0.
 * Stages discoveries locally. Never inserts/updates/deletes census.
 *
 * Usage:
 *   node scripts/hotel-intelligence-universe-expansion.mjs
 *   node scripts/hotel-intelligence-universe-expansion.mjs --country Brazil --limit 250
 *   node scripts/hotel-intelligence-universe-expansion.mjs --scorecard-only
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";

import { MAP_CENSUS_FIELDS, MAP_HOTEL_PROPERTY_CENSUS } from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import { createLocalStore } from "../lib/hotel-intelligence/local-store.js";
import {
  buildCoverageScorecard,
  buildDiscoveryQueue,
  loadCountryCandidatesFromFiles,
  runDiscoveryBatch,
} from "../lib/hotel-intelligence/universe-expansion/index.js";
import { DEALALITY_CALA_GEOGRAPHIES } from "../lib/research-engine-v2/dealality-cala-geography-registry-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

// --- SAFETY LOCK ---
process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.ENABLE_HBX_INSERTS = "0";
process.env.ENABLE_CENSUS_SHELL_INSERTS = "0";

const args = process.argv.slice(2);
function argVal(name, fallback = null) {
  const i = args.indexOf(name);
  if (i < 0) return fallback;
  return args[i + 1] != null ? args[i + 1] : fallback;
}
function hasFlag(name) {
  return args.includes(name);
}

const SCORECARD_ONLY = hasFlag("--scorecard-only");
const COUNTRY = argVal("--country", "Brazil");
const LIMIT = Number(argVal("--limit", "250")) || 250;
const OUT_DIR = path.join(ROOT, "reports/hotel-intelligence/universe-expansion-v1");
const DATA_DIR = path.join(ROOT, "data/hotel-intelligence/universe-expansion");

const PREVIOUS_AUDITS = [
  {
    id: "full-cala-geography-coverage-registry-audit",
    path: "docs/data-intelligence/full-cala-geography-coverage-registry-audit.md",
    drove: "Zero-record geographies, HOLD concentration, source-gap priority queue",
  },
  {
    id: "full-cala-15k-shell-universe-exhausted",
    path: "docs/data-intelligence/full-cala-15k-shell-universe-exhausted.md",
    drove: "Stop at 5956; next unresolved pool = Brazil; HBX-safe candidates exhausted",
  },
  {
    id: "full-cala-15k-shell-orchestrator-final",
    path: "reports/research-engine-v2/full-cala-15k-shell-orchestrator-final.json",
    drove: "9,630 weak holds by country (Brazil 4,842); no production inserts remaining",
  },
  {
    id: "full-cala-15k-source-inventory",
    path: "reports/research-engine-v2/full-cala-15k-source-inventory.md",
    drove: "Cvent ~14k + HBX Wave1 3,385 candidate sources",
  },
  {
    id: "full-cala-hbx-geography-discovery-final",
    path: "reports/research-engine-v2/full-cala-hbx-geography-discovery-final.md",
    drove: "47 non-Wave1 geographies HBX-blocked (HTTP 403) — cannot rely on HBX for Brazil+",
  },
  {
    id: "full-cala-15k-census-shell-insert-v1",
    path: "docs/data-intelligence/full-cala-15k-census-shell-insert-v1.md",
    drove: "Eligible shell plan ~10.7k; quality gate held Cvent-only missing-city",
  },
  {
    id: "holds-ledger",
    path: "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json",
    drove: "Reopenable discovery pool (cvent_only_missing_city dominant)",
  },
];

async function listCensusByCountry() {
  const token = (
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_API_KEY ||
    ""
  ).trim();
  // Hotel Property Census lives on the Deal Capture Platform base (ALT in this repo).
  const baseId = (
    process.env.AIRTABLE_BASE_ID_ALT ||
    process.env.AIRTABLE_BASE_ID ||
    ""
  ).trim();
  if (!token || !baseId) {
    throw new Error(
      "AIRTABLE_PAT/API_KEY and AIRTABLE_BASE_ID_ALT required for live census read"
    );
  }
  const base = new Airtable({ apiKey: token }).base(baseId);
  const tableId = MAP_HOTEL_PROPERTY_CENSUS.tableId;
  const byCountry = {};
  const records = [];
  await base(tableId)
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
        const country = String(r.fields?.[MAP_CENSUS_FIELDS.country] || "").trim() || "UNKNOWN";
        byCountry[country] = (byCountry[country] || 0) + 1;
        records.push({ id: r.id, fields: r.fields });
      }
      next();
    });
  return { byCountry, records, total: records.length };
}

function ensureDirs() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function mdEscape(s) {
  return String(s ?? "").replace(/\|/g, "\\|");
}

function renderReport({
  safety,
  audits,
  scorecard,
  queue,
  batch,
  progress,
  nextBatch,
}) {
  const scoreRows = (scorecard.rows || [])
    .map(
      (r) =>
        `| ${mdEscape(r.country)} | ${r.hotels_in_dealality} | ${r.expected_approximate_universe ?? "—"} | ${r.coverage_pct ?? "—"}% | ${r.confidence} | ${r.priority} | ${r.flag} |`
    )
    .join("\n");

  const queueRows = (queue.items || [])
    .slice(0, 30)
    .map(
      (q) =>
        `| ${q.rank} | T${q.tier} | ${mdEscape(q.country)} | ${q.expected_gain} | ${mdEscape(q.why_prioritized)} |`
    )
    .join("\n");

  const m = batch?.metrics || {};
  const v = batch?.validation || {};

  return `# DEALALITY_HOTEL_UNIVERSE_EXPANSION_COMPLETE

**Generated:** ${new Date().toISOString()}  
**Airtable writes:** **${safety.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES}** (locked)  
**Enrichment:** not run (discovery only)

## Previous Audits Reused

HOTEL_UNIVERSE_PREVIOUS_AUDITS_FOUND: **${audits.length}**

| Audit | Path | Drove |
| --- | --- | --- |
${audits.map((a) => `| ${a.id} | \`${a.path}\` | ${a.drove} |`).join("\n")}

### Current coverage (live)
- Hotels in Dealality: **${progress.hotels_current}**
- Known-source expected (Cvent/HBX/holds upper bound): **${scorecard.total_expected_from_known_sources}**
- Largest gaps: ${(scorecard.rows || []).slice(0, 5).map((r) => `${r.country} (~${r.gap_estimate})`).join("; ")}
- Countries needing expansion: zero-record + POOR/PARTIAL flags (see scorecard)
- Prior recommended priorities: geography audit top-10 source gaps + orchestrator **Brazil** next pool

### Do not repeat
- Wave1 HBX shell inserts already applied (MX/DO/CO/CR/PA safe HBX pool exhausted)
- Do not re-run HBX geography discovery until credentials/licensing fix (403)

## Coverage Scorecard

Sorted worst → best (priority then gap).

| Country | Hotels in Dealality | Expected approx | Coverage % | Confidence | Priority | Flag |
| --- | ---: | ---: | ---: | --- | ---: | --- |
${scoreRows}

## Discovery Queue

| Rank | Tier | Country | Expected gain | Why prioritized |
| ---: | --- | --- | ---: | --- |
${queueRows}

## Batch Results

Country: **${batch?.country || "—"}** · Batch: \`${batch?.batch_id || "—"}\` · Limit: ${LIMIT}

| Metric | Value |
| --- | ---: |
| Hotels before (production) | ${m.hotels_before ?? "—"} |
| Hotels after (production) | ${m.hotels_after_production ?? "—"} |
| Hotels after (provisional staged) | ${m.hotels_after_provisional_staged ?? "—"} |
| NEW_HOTEL (explicit city) | ${m.new_hotels_staged ?? 0} |
| REVIEW_REQUIRED staged shells | ${m.review_required_staged_shells ?? 0} |
| Matched existing | ${m.matched_existing ?? 0} |
| Duplicates prevented | ${m.duplicates_prevented ?? 0} |
| Ambiguous | ${m.ambiguous ?? 0} |
| Review queue items | ${m.review_queue ?? 0} |
| Rejected | ${m.rejected ?? 0} |
| City inferred from Cvent URL | ${m.city_inferred ?? 0} |
| Batch validation | ${v.pass ? "PASS" : "REJECT"} ${v.reject_reason || ""} |
| Duplicate rate % | ${v.duplicate_rate ?? "—"} |
| Review burden % | ${v.review_burden_rate ?? "—"} |

Status counts: \`${JSON.stringify(m.status_counts || {})}\`

## Country Improvements

Production census unchanged (read-only). Provisional Brazil staged shells: **${m.review_required_staged_shells || m.new_hotels_staged || 0}** (identity discovery pending review — city inferred from Cvent URL).

## Remaining Gaps

- **HBX blocked** for 47 geographies (auth 403) — largest structural blocker for high-confidence city/address shells outside Wave1.
- **Brazil** still the largest reopenable hold pool (~4.8k) after this 250 batch.
- **Zero-record geographies** (22): Bermuda, Sint Eustatius, Saba, Cuba, Turks and Caicos Islands, U.S. Virgin Islands, Anguilla, Bonaire, Guadeloupe, Martinique, Saint Barthélemy, Saint Martin, Sint Maarten, Bolivia, Venezuela, Haiti, Saint Vincent and the Grenadines, French Guiana, Guyana, Paraguay, Suriname, Montserrat.
- Cvent is meetings-venue inventory — not a complete national census; official brand/government directories still needed for true universe completeness.

## Progress Toward Target

| Milestone | Target | Current production | % of target |
| --- | ---: | ---: | ---: |
| First | 10,000 | ${progress.hotels_current} | ${progress.pct_10k}% |
| Second | 12,500 | ${progress.hotels_current} | ${progress.pct_125k}% |
| North star | 15,000+ | ${progress.hotels_current} | ${progress.pct_15k}% |

Provisional staged (not production): +${m.review_required_staged_shells || 0} review shells this batch.

## Recommended Next Batch

**Do not auto-start.**

${nextBatch}

## Safety confirmation

\`\`\`
ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=${safety.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES}
ENABLE_HBX_CENSUS_WRITES=${safety.ENABLE_HBX_CENSUS_WRITES}
\`\`\`

Artifacts:
- \`${path.relative(ROOT, path.join(OUT_DIR, "coverage-scorecard.json"))}\`
- \`${path.relative(ROOT, path.join(OUT_DIR, "discovery-queue.json"))}\`
- \`${path.relative(ROOT, path.join(OUT_DIR, "batch-brazil-250.json")).replace("brazil", String(COUNTRY).toLowerCase().replace(/\\s+/g, "-"))}\`
- \`${path.relative(ROOT, path.join(DATA_DIR, "staged-hotels.json"))}\`
`;
}

async function main() {
  ensureDirs();
  console.log(
    JSON.stringify({
      module: "hotel-intelligence-universe-expansion",
      event: "start",
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES:
        process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES,
      country: COUNTRY,
      limit: LIMIT,
      scorecard_only: SCORECARD_ONLY,
    })
  );

  console.log("[universe] loading live census (read-only)…");
  const { byCountry, records, total } = await listCensusByCountry();
  console.log(`[universe] census total=${total}`);

  const scorecard = buildCoverageScorecard(byCountry, { root: ROOT });
  writeJson(path.join(OUT_DIR, "coverage-scorecard.json"), scorecard);

  const queue = buildDiscoveryQueue(scorecard, { pinBrazilFirst: true });
  writeJson(path.join(OUT_DIR, "discovery-queue.json"), queue);

  let batch = null;
  if (!SCORECARD_ONLY) {
    console.log(`[universe] loading ${COUNTRY} candidates (holds-first)…`);
    let candidates = loadCountryCandidatesFromFiles(COUNTRY, {
      root: ROOT,
      onlyHolds: true,
    });
    if (candidates.length < LIMIT) {
      console.log(
        `[universe] holds-only=${candidates.length}; expanding to all country candidates`
      );
      candidates = loadCountryCandidatesFromFiles(COUNTRY, {
        root: ROOT,
        onlyHolds: false,
      });
    }
    console.log(`[universe] candidates=${candidates.length}; batch limit=${LIMIT}`);

    // Full census for identity quality on first validation batches.
    const resolvePool = records;
    batch = runDiscoveryBatch(candidates, resolvePool, {
      country: COUNTRY,
      limit: LIMIT,
      hotelsBefore: total,
      batchId: `discovery_${String(COUNTRY).toLowerCase().replace(/\s+/g, "_")}_${LIMIT}_${new Date()
        .toISOString()
        .replace(/[:.]/g, "-")}`,
    });

    const batchName = `batch-${String(COUNTRY).toLowerCase().replace(/\s+/g, "-")}-${LIMIT}.json`;
    writeJson(path.join(OUT_DIR, batchName), {
      ...batch,
      staged_hotels: batch.staged_hotels,
      results: batch.results,
    });

    const store = createLocalStore({
      root: path.join(DATA_DIR),
    });
    const stagedDoc = {
      version: 1,
      updated_at: new Date().toISOString(),
      production_writes: false,
      batches: [batch.batch_id],
      hotels: batch.staged_hotels,
    };
    writeJson(path.join(DATA_DIR, "staged-hotels.json"), stagedDoc);
    writeJson(path.join(DATA_DIR, "review-queue.json"), {
      version: 1,
      updated_at: new Date().toISOString(),
      items: batch.review_items,
    });
    writeJson(path.join(DATA_DIR, `batch-${batch.batch_id}.json`), {
      metrics: batch.metrics,
      validation: batch.validation,
      country: batch.country,
      batch_id: batch.batch_id,
    });

    // also mirror into default HI store staged path without enabling writes
    try {
      const hiStore = createLocalStore();
      const prev = hiStore.readStagedHotels() || { version: 1, hotels: [] };
      const prevList = Array.isArray(prev.hotels)
        ? prev.hotels
        : Object.values(prev.hotels || {});
      hiStore.writeStagedHotels({
        version: 1,
        updated_at: new Date().toISOString(),
        hotels: [...prevList, ...batch.staged_hotels],
      });
    } catch (err) {
      console.warn(
        JSON.stringify({
          module: "hotel-intelligence-universe-expansion",
          event: "staged_mirror_skip",
          message: String(err?.message || err).slice(0, 200),
        })
      );
    }
  }

  const progress = {
    hotels_current: total,
    target_15k: 15000,
    pct_10k: Math.round((1000 * total) / 10000) / 10,
    pct_125k: Math.round((1000 * total) / 12500) / 10,
    pct_15k: Math.round((1000 * total) / 15000) / 10,
  };

  const nextItem = (queue.items || []).find(
    (q) => q.country !== COUNTRY || (batch && (batch.metrics?.review_required_staged_shells || 0) < 1000)
  );
  // Prefer continue Brazil until validation of city-inference accepted
  const continueBrazil =
    COUNTRY === "Brazil" &&
    (queue.items || []).find((q) => q.country === "Brazil");
  const nextBatch = continueBrazil
    ? `**Brazil — next 500** (after founder/review accepts Cvent URL city-inference shells). Why: orchestrator next pool; ~${continueBrazil.expected_gain} remaining gain; HBX still blocked. Do not production-insert until sample review of inferred cities (Rio de Janeiro, São Paulo, etc.) passes duplicate + geo sanity checks.`
    : nextItem
      ? `**${nextItem.country} — batch ${nextItem.recommended_batch_size}** — ${nextItem.why_prioritized}`
      : "Re-run scorecard after review acceptance.";

  const safety = {
    ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES:
      process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES || "0",
    ENABLE_HBX_CENSUS_WRITES: process.env.ENABLE_HBX_CENSUS_WRITES || "0",
  };

  const md = renderReport({
    safety,
    audits: PREVIOUS_AUDITS,
    scorecard,
    queue,
    batch,
    progress,
    nextBatch,
  });
  writeJson(path.join(OUT_DIR, "expansion-summary.json"), {
    marker: "DEALALITY_HOTEL_UNIVERSE_EXPANSION_COMPLETE",
    safety,
    progress,
    scorecard_flag_counts: scorecard.flag_counts,
    queue_top10: (queue.items || []).slice(0, 10),
    batch_metrics: batch?.metrics || null,
    batch_validation: batch?.validation || null,
    recommended_next_batch: nextBatch,
    previous_audits: PREVIOUS_AUDITS,
    cala_geography_count: DEALALITY_CALA_GEOGRAPHIES?.length || null,
  });
  fs.writeFileSync(path.join(OUT_DIR, "DEALALITY_HOTEL_UNIVERSE_EXPANSION_COMPLETE.md"), md, "utf8");

  console.log("DEALALITY_HOTEL_UNIVERSE_EXPANSION_COMPLETE");
  console.log(
    JSON.stringify(
      {
        hotels_before: total,
        batch: batch?.metrics || null,
        validation: batch?.validation || null,
        out_dir: path.relative(ROOT, OUT_DIR),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
