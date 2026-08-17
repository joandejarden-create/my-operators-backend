#!/usr/bin/env node
/**
 * Dealality Discovery Factory V1 — read-only census expansion pipeline.
 *
 * SAFETY: ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES forced to 0.
 * Stages READY_FOR_IMPORT / REVIEW locally. Never writes Airtable.
 *
 * Usage:
 *   node scripts/hotel-intelligence-discovery-factory.mjs
 *   node scripts/hotel-intelligence-discovery-factory.mjs --country Brazil --limit 250
 *   node scripts/hotel-intelligence-discovery-factory.mjs --dashboard-only
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
  buildCountryDashboard,
  persistDashboard,
  DISCOVERY_FACTORY_VERSION,
  STAGE_STATUS,
} from "../lib/hotel-intelligence/discovery-factory/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports/hotel-intelligence/discovery-factory-v1");
const DATA_DIR = path.join(ROOT, "data/hotel-intelligence/discovery-factory");

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

const DASHBOARD_ONLY = hasFlag("--dashboard-only");
const COUNTRY = argVal("--country", null); // null → use priority #1
const LIMIT = Number(argVal("--limit", "250")) || 250;
const OFFSET = Number(argVal("--offset", "0")) || 0;

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
  if (!token || !baseId) {
    throw new Error("AIRTABLE_PAT + AIRTABLE_BASE_ID_ALT required");
  }
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

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function sampleValidation(batch, n = 25) {
  const ready = batch.ready_for_import || [];
  const review = batch.review_required || [];
  const pool = [...ready, ...review];
  const step = Math.max(1, Math.floor(pool.length / n));
  const sample = [];
  for (let i = 0; i < pool.length && sample.length < n; i += step) {
    sample.push(pool[i]);
  }
  const cityOk = sample.filter((s) => s.city && String(s.city).length >= 3).length;
  const hasId = sample.filter((s) => /^dhl_/.test(String(s.hotel_id || ""))).length;
  const confOk = sample.filter((s) => (s.identity_confidence || 0) >= 0.7).length;
  const agreeBoost = sample.filter((s) =>
    String(s.city_method || "").includes("agree")
  ).length;

  const quality_pass =
    sample.length >= 10 &&
    cityOk / sample.length >= 0.95 &&
    hasId / sample.length === 1 &&
    (ready.length / Math.max(1, batch.metrics.candidates_processed)) >= 0.15;

  return {
    sample_size: sample.length,
    city_present_pct: Math.round((cityOk / Math.max(1, sample.length)) * 1000) / 10,
    canonical_id_pct: Math.round((hasId / Math.max(1, sample.length)) * 1000) / 10,
    conf_ge_0_70_pct: Math.round((confOk / Math.max(1, sample.length)) * 1000) / 10,
    url_name_agree_in_sample: agreeBoost,
    tier_a_share_pct: batch.metrics.tier_a_pct,
    duplicate_rate_pct: batch.metrics.duplicate_rate_pct,
    quality_pass,
    sample: sample.slice(0, 15).map((s) => ({
      name: s.name,
      city: s.city,
      city_method: s.city_method,
      tier: s.tier,
      stage_status: s.stage_status,
      identity_confidence: s.identity_confidence,
      hotel_id: s.hotel_id,
    })),
    verdict: quality_pass
      ? "APPROVE_EXPANSION_SCALING"
      : "HOLD_FOR_THRESHOLD_TUNING",
  };
}

function estimateGrowth(total, readyCumulative, reviewShare) {
  // Conservative: only Tier A counts toward "validated canonical" path without review
  const to10k = Math.max(0, 10000 - total);
  const to125 = Math.max(0, 12500 - total);
  const to15k = Math.max(0, 15000 - total);
  return {
    production_now: total,
    if_import_ready_only: total + readyCumulative,
    milestones: {
      m1_10000: { remaining: to10k, pct: Math.round((1000 * total) / 10000) / 10 },
      m2_12500: { remaining: to125, pct: Math.round((1000 * total) / 12500) / 10 },
      m3_15000: { remaining: to15k, pct: Math.round((1000 * total) / 15000) / 10 },
    },
    note: "Growth assumes future approved imports of READY_FOR_IMPORT only; review items excluded.",
    prior_review_burden_baseline_pct: 98.8,
    current_review_burden_pct: reviewShare,
    review_reduction_pp:
      Math.round((98.8 - reviewShare) * 10) / 10,
  };
}

function renderReport(ctx) {
  const { safety, dashboard, batch, validation, growth, nextCountry, batchPlan } = ctx;
  const top = (dashboard.rows || []).slice(0, 15);
  const dashRows = top
    .map(
      (r) =>
        `| ${r.rank} | ${r.country} | ${r.current_census} | ${r.estimated_universe ?? "—"} | ${r.coverage_pct ?? "—"}% | ${r.discovery_candidates} | ${r.ready_for_import} | ${r.needs_review} | ${r.rejected} | ${r.duplicate_rate ?? "—"} | ${r.priority_score} |`
    )
    .join("\n");
  const queueRows = (dashboard.queue || [])
    .slice(0, 12)
    .map(
      (q) =>
        `| ${q.rank} | ${q.country} | ${q.priority_score} | ${q.components?.missing_hotels_estimate ?? "—"} | ${q.why} |`
    )
    .join("\n");
  const m = batch?.metrics || {};

  return `# DEALALITY_DISCOVERY_FACTORY_V1_COMPLETE

**Generated:** ${new Date().toISOString()}  
**Factory version:** \`${DISCOVERY_FACTORY_VERSION}\`  
**Airtable writes:** **${safety.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES}** (locked)

## Safety

\`\`\`
ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=${safety.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES}
ENABLE_HBX_CENSUS_WRITES=${safety.ENABLE_HBX_CENSUS_WRITES}
\`\`\`

- No production census writes
- No Brand Explorer writes
- No automatic merges
- No schema changes
- Discoveries staged locally under \`data/hotel-intelligence/discovery-factory/\`

## Discovery confidence model

| Tier | Identity confidence | Stage status | Manual review |
| --- | --- | --- | --- |
| **A** | ≥ 0.90 + strong name + city conf ≥ 0.85 + no multi-city + match=new | \`READY_FOR_IMPORT\` | No |
| **B** | 0.70–0.89 (or soft risks) | \`REVIEW_REQUIRED\` | Yes |
| **C** | < 0.70, ambiguous, duplicate, missing city/country | \`REJECTED\` / \`MATCHED_EXISTING\` | No (drop or already known) |

City inference upgrades: URL slug decode, accent alias canonicalization (São Paulo, San José, Ciudad de Panamá, …), name↔URL corroboration boost, multi-city conflict demotion.

## Country dashboard

Persistent: \`data/hotel-intelligence/discovery-factory/country-dashboard.json\`

| Rank | Country | Census | Est. universe | Coverage % | Candidates | Ready | Review | Rejected | Dup % | Priority |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
${dashRows}

## Updated discovery queue

| Rank | Country | Priority score | Gap | Why |
| ---: | --- | ---: | ---: | --- |
${queueRows}

## Brazil validation

Country run: **${batch?.country || "—"}** · Batch \`${batch?.batch_id || "—"}\`

| Metric | Value |
| --- | ---: |
| Candidates processed | ${m.candidates_processed ?? "—"} |
| READY_FOR_IMPORT (Tier A) | ${m.ready_for_import ?? "—"} |
| REVIEW_REQUIRED (Tier B) | ${m.review_required ?? "—"} |
| Rejected (Tier C) | ${m.rejected ?? "—"} |
| Matched existing | ${m.matched_existing ?? "—"} |
| Duplicate rate % | ${m.duplicate_rate_pct ?? "—"} |
| Avg identity confidence | ${m.avg_identity_confidence ?? "—"} |
| Avg processing ms | ${m.avg_processing_ms ?? "—"} |
| Tier A % | ${m.tier_a_pct ?? "—"} |
| Review burden % | ${m.review_burden_pct ?? "—"} |

### Sample QA

- Verdict: **${validation?.verdict || "—"}**
- Quality pass: **${validation?.quality_pass ?? "—"}**
- Sample size: ${validation?.sample_size ?? "—"}
- City present: ${validation?.city_present_pct ?? "—"}%
- Canonical dhl_id: ${validation?.canonical_id_pct ?? "—"}%
- Confidence ≥ 0.70: ${validation?.conf_ge_0_70_pct ?? "—"}%

Sample rows:
${(validation?.sample || [])
  .slice(0, 8)
  .map(
    (s) =>
      `- ${s.name} · ${s.city} · ${s.stage_status} · conf ${s.identity_confidence} · ${s.hotel_id}`
  )
  .join("\n")}

## Batch recommendations

Do **not** auto-import.

${batchPlan}

## Expected hotel growth

| | Value |
| --- | ---: |
| Production now | ${growth.production_now} |
| If import Tier A from this batch | ${growth.if_import_ready_only} |
| Milestone 10k remaining | ${growth.milestones.m1_10000.remaining} (${growth.milestones.m1_10000.pct}%) |
| Milestone 12.5k remaining | ${growth.milestones.m2_12500.remaining} (${growth.milestones.m2_12500.pct}%) |
| Milestone 15k remaining | ${growth.milestones.m3_15000.remaining} (${growth.milestones.m3_15000.pct}%) |

## Estimated review reduction

- Prior Brazil-250 baseline review burden: **${growth.prior_review_burden_baseline_pct}%**
- Factory review burden this batch: **${growth.current_review_burden_pct}%**
- Reduction: **${growth.review_reduction_pp} percentage points**

## Progress toward 10,000 / 12,500 / 15,000

Production unchanged until explicit import approval. Tier A staging creates the safe import queue.

## Highest-value next country

**${nextCountry?.country || "—"}** (priority ${nextCountry?.priority_score ?? "—"}) — ${nextCountry?.why || ""}

After Brazil scaling ladder completes, re-rank dashboard and take the then-#1 country automatically (still no auto-import).
`;
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(DATA_DIR, { recursive: true });

  console.log(
    JSON.stringify({
      module: "discovery-factory-v1",
      event: "start",
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES:
        process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES,
      limit: LIMIT,
      dashboard_only: DASHBOARD_ONLY,
    })
  );

  console.log("[factory] loading live census (read-only)…");
  const { byCountry, records, total } = await listCensus();
  console.log(`[factory] census total=${total}`);

  const scorecard = buildCoverageScorecard(byCountry, { root: ROOT });
  writeJson(path.join(OUT_DIR, "coverage-scorecard.json"), scorecard);

  let batch = null;
  let validation = null;
  const factoryMetricsByCountry = {};

  const dashboardPreview = buildCountryDashboard(scorecard, {}, { root: ROOT });
  const topCountry = dashboardPreview.queue?.[0]?.country || "Brazil";
  const runCountry = COUNTRY || topCountry;

  if (!DASHBOARD_ONLY) {
    console.log(`[factory] priority#1=${topCountry}; running country=${runCountry} limit=${LIMIT}`);
    let candidates = loadCountryCandidatesFromFiles(runCountry, {
      root: ROOT,
      onlyHolds: true,
    });
    if (candidates.length < LIMIT) {
      candidates = loadCountryCandidatesFromFiles(runCountry, {
        root: ROOT,
        onlyHolds: false,
      });
    }
    console.log(`[factory] candidates=${candidates.length}`);

    batch = runDiscoveryFactoryBatch(candidates, records, {
      country: runCountry,
      limit: LIMIT,
      offset: OFFSET,
      hotelsBefore: total,
      batchId: `factory_${String(runCountry)
        .toLowerCase()
        .replace(/\s+/g, "_")}_${LIMIT}_${new Date().toISOString().replace(/[:.]/g, "-")}`,
    });

    factoryMetricsByCountry[runCountry] = batch.metrics;
    validation = sampleValidation(batch, 25);

    writeJson(
      path.join(
        OUT_DIR,
        `batch-${String(runCountry).toLowerCase().replace(/\s+/g, "-")}-${LIMIT}.json`
      ),
      {
        ...batch,
        results: batch.results,
      }
    );
    writeJson(path.join(DATA_DIR, "staged-ready-for-import.json"), {
      version: 1,
      updated_at: new Date().toISOString(),
      production_writes: false,
      hotels: batch.staged_hotels.filter(
        (h) => h.discovery?.stage_status === STAGE_STATUS.READY_FOR_IMPORT
      ),
    });
    writeJson(path.join(DATA_DIR, "staged-review-required.json"), {
      version: 1,
      updated_at: new Date().toISOString(),
      production_writes: false,
      hotels: batch.staged_hotels.filter(
        (h) => h.discovery?.stage_status === STAGE_STATUS.REVIEW_REQUIRED
      ),
    });
    writeJson(path.join(DATA_DIR, `batch-${batch.batch_id}.json`), {
      metrics: batch.metrics,
      country: batch.country,
      batch_id: batch.batch_id,
      validation,
    });
    writeJson(path.join(OUT_DIR, "brazil-validation.json"), validation);
  }

  const dashboard = buildCountryDashboard(scorecard, factoryMetricsByCountry, {
    root: ROOT,
  });
  persistDashboard(dashboard, { root: ROOT });
  writeJson(path.join(OUT_DIR, "discovery-queue.json"), {
    items: dashboard.queue,
  });

  const growth = estimateGrowth(
    total,
    batch?.metrics?.ready_for_import || 0,
    batch?.metrics?.review_burden_pct ?? 98.8
  );

  const nextCountry =
    dashboard.queue?.find((q) => q.country !== runCountry) || dashboard.queue?.[0];

  const batchPlan = validation?.quality_pass
    ? [
        `1. **${runCountry} +500** — factory batch, validate Tier A ≥15% and dup rate <5%`,
        `2. **${runCountry} +1,000** — validate again`,
        `3. **${runCountry} remaining holds** — checkpoint dashboard`,
        `4. Auto-select next country by priority score (currently **${nextCountry?.country}**) — still no import`,
        `5. Import gate separate: only \`READY_FOR_IMPORT\` with explicit ENABLE flag + founder approval`,
      ].join("\n")
    : [
        `1. Tune Tier A thresholds / city alias table using sample failures`,
        `2. Re-run **${runCountry} ×250** until quality_pass=true`,
        `3. Then scale +500 / +1000 / remaining`,
        `4. Do not import until APPROVE_EXPANSION_SCALING`,
      ].join("\n");

  const safety = {
    ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES:
      process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES || "0",
    ENABLE_HBX_CENSUS_WRITES: process.env.ENABLE_HBX_CENSUS_WRITES || "0",
  };

  const md = renderReport({
    safety,
    dashboard,
    batch,
    validation,
    growth,
    nextCountry,
    batchPlan,
  });
  fs.writeFileSync(
    path.join(OUT_DIR, "DEALALITY_DISCOVERY_FACTORY_V1_COMPLETE.md"),
    md,
    "utf8"
  );
  writeJson(path.join(OUT_DIR, "factory-summary.json"), {
    marker: "DEALALITY_DISCOVERY_FACTORY_V1_COMPLETE",
    safety,
    metrics: batch?.metrics || null,
    validation,
    growth,
    top_queue: (dashboard.queue || []).slice(0, 10),
    next_country: nextCountry,
    recommended_batches: batchPlan,
  });

  console.log("DEALALITY_DISCOVERY_FACTORY_V1_COMPLETE");
  console.log(
    JSON.stringify(
      {
        census: total,
        country: runCountry,
        metrics: batch?.metrics || null,
        validation_verdict: validation?.verdict || null,
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
