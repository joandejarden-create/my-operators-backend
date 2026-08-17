#!/usr/bin/env node
/**
 * CALA Hotel Census Coverage Dashboard V1 — read-only analysis/reporting.
 *
 * SAFETY: ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES forced to 0.
 * No discovery. No enrichment. No census writes.
 *
 * Usage:
 *   npm run hotel-intelligence:cala-coverage-dashboard
 *   node scripts/hotel-intelligence-cala-coverage-dashboard.mjs
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
import {
  buildCalaCoverageDashboard,
  compareCoverageTrend,
  persistCoverageDashboard,
  COVERAGE_DASHBOARD_VERSION,
} from "../lib/hotel-intelligence/coverage-dashboard/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

// --- SAFETY LOCK ---
process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.ENABLE_HBX_INSERTS = "0";
process.env.ENABLE_CENSUS_SHELL_INSERTS = "0";

async function listCensusByCountry() {
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
  let total = 0;
  await base(MAP_HOTEL_PROPERTY_CENSUS.tableId)
    .select({
      pageSize: 100,
      fields: [MAP_CENSUS_FIELDS.country],
    })
    .eachPage((page, next) => {
      for (const r of page) {
        const country =
          String(r.fields?.[MAP_CENSUS_FIELDS.country] || "").trim() || "UNKNOWN";
        byCountry[country] = (byCountry[country] || 0) + 1;
        total += 1;
      }
      next();
    });
  return { byCountry, total };
}

async function main() {
  console.log(
    JSON.stringify({
      module: "cala-coverage-dashboard-v1",
      event: "start",
      version: COVERAGE_DASHBOARD_VERSION,
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES:
        process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES,
      discovery: false,
      enrichment: false,
    })
  );

  console.log("[coverage-dashboard] loading live census counts (read-only)…");
  const { byCountry, total } = await listCensusByCountry();
  console.log(`[coverage-dashboard] census total=${total}`);

  const dashboard = buildCalaCoverageDashboard(byCountry, { root: ROOT });

  const priorFp = path.join(
    ROOT,
    "data/hotel-intelligence/coverage-dashboard/latest.json"
  );
  let prior = null;
  if (fs.existsSync(priorFp)) {
    try {
      prior = JSON.parse(fs.readFileSync(priorFp, "utf8"));
    } catch {
      prior = null;
    }
  }
  // If latest is from this same second-ish rebuild, prefer baseline for first compare
  const baselineFp = path.join(
    ROOT,
    "data/hotel-intelligence/coverage-dashboard/baseline.json"
  );
  if (!prior && fs.existsSync(baselineFp)) {
    try {
      prior = JSON.parse(fs.readFileSync(baselineFp, "utf8"));
    } catch {
      prior = null;
    }
  }

  // First-ever run: no prior → baseline
  const isFirst =
    !fs.existsSync(baselineFp) && !fs.existsSync(priorFp);
  const trend = compareCoverageTrend(dashboard, isFirst ? null : prior);

  const paths = persistCoverageDashboard(dashboard, trend, { root: ROOT });

  const next = dashboard.priority_ranking?.[0];
  console.log("DEALALITY_CALA_COVERAGE_DASHBOARD_COMPLETE");
  console.log(
    JSON.stringify(
      {
        summary: dashboard.summary,
        brazil: dashboard.brazil_detail
          ? {
              coverage_pct: dashboard.brazil_detail.coverage_pct,
              status: dashboard.brazil_detail.coverage_status,
              expected_gain: dashboard.brazil_detail.expected_gain,
            }
          : null,
        next_batch: next
          ? {
              country: next.country,
              expected_gain: next.expected_gain,
              opportunity: next.opportunity_score,
            }
          : null,
        trend: {
          baseline_established: trend.baseline_established,
          hotels_added: trend.hotels_added,
        },
        exports: {
          md: path.relative(ROOT, paths.mdFp),
          csv: path.relative(ROOT, paths.csvFp),
          json: path.relative(ROOT, paths.jsonFp),
        },
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
