#!/usr/bin/env node
/**
 * Hotel-generic Scaling Companies GDI Full E2E runner CLI.
 *
 *   node scripts/gdi-scaling-companies-hotel-runner-v1.mjs --config <path.json>
 *   node scripts/gdi-scaling-companies-hotel-runner-v1.mjs --hotel-id rec… --mode preview|full-e2e
 *
 * Prefer: scripts/Invoke-GdiScalingCompanies.ps1 (single founder entry point).
 *
 * Defaults: Preview, no paid calls, no publication, no production writes.
 * Never executes Surfe / PDL / Parallel / outreach / weekly scheduling.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runScalingCompaniesHotelRunner,
  normalizeScalingCompaniesRunnerConfig,
  SCALING_COMPANIES_HOTEL_RUNNER_V1,
  SCALING_COMPANIES_FULL_E2E_STAGES,
} from "../lib/group-demand-intelligence/research/scaling-companies-hotel-runner-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  if (i < 0 || i + 1 >= process.argv.length) return null;
  return process.argv[i + 1];
}

function printUsage() {
  console.log(`Usage:
  node scripts/gdi-scaling-companies-hotel-runner-v1.mjs --config <path.json>
  node scripts/gdi-scaling-companies-hotel-runner-v1.mjs --hotel-id <rec...> [--mode preview|full-e2e]

Modes:
  preview   Validate hotel + strategies + inventory; optional saved-discovery handoff; $0; no writes
  full-e2e  Complete pipeline through publication/share verification (independent auth gates)

Independent authorizations (Node-enforced, not flag-only):
  authorizePaidResearch + budgetCeilingUsd > 0
  authorizePublication (+ authorizeProductionWrites / applyProductionWrites for Airtable)

Forbidden (always refused): authorizeSurfe, authorizePdl, authorizeParallel,
  authorizeOutreach, authorizeWeeklyScheduling

Stages:
  ${SCALING_COMPANIES_FULL_E2E_STAGES.join(" → ")}
`);
}

const configPath = argValue("--config");
const hotelIdCli = argValue("--hotel-id") || argValue("--hotelId");
const modeCli = argValue("--mode");
const savedPathCli = argValue("--saved-discovery") || argValue("--saved-discovery-path");

if (process.argv.includes("--help") || process.argv.includes("-h")) {
  printUsage();
  process.exit(0);
}

let raw = {};
if (configPath) {
  const abs = path.isAbsolute(configPath)
    ? configPath
    : path.join(process.cwd(), configPath);
  if (!fs.existsSync(abs)) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          error: "config_not_found",
          configPath: abs,
          version: SCALING_COMPANIES_HOTEL_RUNNER_V1,
        },
        null,
        2,
      ),
    );
    process.exit(2);
  }
  const rawText = fs.readFileSync(abs, "utf8").replace(/^\uFEFF/, "");
  raw = JSON.parse(rawText);
  raw._configPath = abs;
} else if (hotelIdCli) {
  raw = { hotelId: hotelIdCli };
} else {
  printUsage();
  process.exit(2);
}

if (modeCli) raw.mode = modeCli;
if (savedPathCli) raw.savedDiscoveryPath = savedPathCli;
if (process.argv.includes("--authorize-paid")) raw.authorizePaidResearch = true;
if (process.argv.includes("--authorize-publication")) raw.authorizePublication = true;
if (process.argv.includes("--authorize-production-writes")) {
  raw.authorizeProductionWrites = true;
}
if (process.argv.includes("--apply-production-writes")) {
  raw.applyProductionWrites = true;
}
if (process.argv.includes("--probe-live-share")) raw.probeLiveShare = true;

const budgetCli = argValue("--budget");
if (budgetCli != null) {
  raw.budgetCeilingUsd = Number(budgetCli);
  raw.budgetUSD = Number(budgetCli);
}

const normalized = normalizeScalingCompaniesRunnerConfig(raw);
const summary = await runScalingCompaniesHotelRunner(raw);

const out = {
  version: SCALING_COMPANIES_HOTEL_RUNNER_V1,
  configPath: raw._configPath || null,
  reportDir: summary.config?.reportDir || normalized.reportDir,
  ok: summary.ok,
  executed: summary.executed,
  fullE2eComplete: summary.fullE2eComplete === true,
  fullE2eCompleteReason: summary.fullE2eCompleteReason || null,
  mode: summary.config?.mode,
  hotelId: summary.config?.hotelId,
  hotelName: summary.hotel?.displayName || null,
  existingScaling: summary.reconciliation?.existingScalingCount ?? null,
  genuinelyNetNew: summary.genuinelyNetNewCount ?? 0,
  rawDiscoveryHits: summary.funnel?.rawDiscoveryHits ?? summary.discovery?.rawHitCount ?? 0,
  orsCount: summary.ors?.results?.length ?? 0,
  buyerIntelCount: summary.buyerIntelligence?.length ?? 0,
  publicationStatus: summary.publication?.status || null,
  publishedIds: summary.publication?.publishedIds || [],
  spentUsd: summary.spending?.spentUsd ?? 0,
  blockers: summary.blockers || [],
  runReport: summary.config?.reportDir
    ? path.join(summary.config.reportDir, "RUN_REPORT.md")
    : null,
};

console.log(JSON.stringify(out, null, 2));

if (!summary.ok && summary.reason === "forbidden_authorization_flags") {
  process.exit(3);
}
if (summary.reason === "HOTEL_PROFILE_UNRESOLVED" || summary.reason === "HOTEL_ID_REQUIRED") {
  process.exit(2);
}
process.exit(summary.ok || summary.config?.mode === "preview" ? 0 : 1);
