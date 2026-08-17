#!/usr/bin/env node
/**
 * CALA Portfolio Coverage OS V1 — read-only planning layer.
 *
 * SAFETY: ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES forced to 0.
 * No discovery / enrichment / imports.
 *
 * Usage:
 *   npm run hotel-intelligence:portfolio-coverage-os
 */
import "dotenv/config";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";

import {
  MAP_CENSUS_FIELDS,
  MAP_HOTEL_PROPERTY_CENSUS,
} from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import { buildCalaCoverageDashboard } from "../lib/hotel-intelligence/coverage-dashboard/index.js";
import {
  buildPortfolioCoverageOs,
  persistPortfolioOs,
  PORTFOLIO_OS_VERSION,
} from "../lib/hotel-intelligence/portfolio-coverage-os/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

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
  if (!token || !baseId) throw new Error("AIRTABLE_PAT + AIRTABLE_BASE_ID_ALT required");
  const base = new Airtable({ apiKey: token }).base(baseId);
  const byCountry = {};
  let total = 0;
  await base(MAP_HOTEL_PROPERTY_CENSUS.tableId)
    .select({ pageSize: 100, fields: [MAP_CENSUS_FIELDS.country] })
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
      module: "portfolio-coverage-os-v1",
      version: PORTFOLIO_OS_VERSION,
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES:
        process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES,
      discovery: false,
    })
  );

  console.log("[portfolio-os] loading census (read-only)…");
  const { byCountry, total } = await listCensusByCountry();
  console.log(`[portfolio-os] census=${total}`);

  const coverage = buildCalaCoverageDashboard(byCountry, { root: ROOT });
  const os = buildPortfolioCoverageOs(coverage);
  const paths = persistPortfolioOs(os, { root: ROOT });

  console.log("DEALALITY_CALA_PORTFOLIO_COVERAGE_OS_COMPLETE");
  console.log(
    JSON.stringify(
      {
        kpis: os.kpis,
        allocation: os.discovery_allocation,
        sprint: {
          growth: os.recommended_next_sprint.strategic_growth.countries.map((c) => ({
            country: c.country,
            batch: c.planned_batch,
          })),
          portfolio: os.recommended_next_sprint.portfolio_completion.countries.map((c) => ({
            country: c.country,
            batch: c.planned_batch,
          })),
          estimates: os.recommended_next_sprint.estimates,
        },
        top_portfolio: os.portfolio_coverage_ranking.slice(0, 5),
        top_growth: os.growth_ranking.slice(0, 5),
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
