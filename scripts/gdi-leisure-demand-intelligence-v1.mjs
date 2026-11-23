#!/usr/bin/env node
/**
 * GDI Leisure Demand Intelligence V1 CLI
 *
 *   node scripts/gdi-leisure-demand-intelligence-v1.mjs \
 *     --hotel-id rec… --modules leisure_events,leisure_distribution --mode fixture
 *
 * Portfolio / Full E2E integration:
 *   --orchestrate with --modules all|csv and optional --disable-leisure
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runLeisureModules,
  runLeisureOrchestratorDisabledCheck,
  buildLeisureFourteenSegmentCoverageMatrix,
  listLeisureModuleIds,
  designLeisureAdpLinkage,
  proposeLeisureAirtableSchemaGaps,
} from "../lib/group-demand-intelligence/leisure-demand-intelligence/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function parseArgs(argv) {
  const out = {
    hotelId: "",
    modules: [],
    mode: "fixture",
    budgetUsd: 0,
    outDir: "",
    existingInventory: "",
    hotelProfileJson: "",
    needPeriodJson: "",
    evidenceSources: "fixture,historical_ledger",
    coverageOnly: false,
    disableLeisure: false,
    authorizePaidResearch: false,
    allowLivePilot: false,
    authorizePublication: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    if (a === "--hotel-id") out.hotelId = next();
    else if (a === "--modules") {
      const raw = next();
      out.modules =
        raw === "all"
          ? listLeisureModuleIds()
          : String(raw)
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean);
    } else if (a === "--mode") out.mode = next();
    else if (a === "--budget-usd") out.budgetUsd = Number(next());
    else if (a === "--out") out.outDir = next();
    else if (a === "--existing-inventory") out.existingInventory = next();
    else if (a === "--hotel-profile-json") out.hotelProfileJson = next();
    else if (a === "--need-period-json") out.needPeriodJson = next();
    else if (a === "--evidence-sources") out.evidenceSources = next();
    else if (a === "--coverage-only") out.coverageOnly = true;
    else if (a === "--disable-leisure") out.disableLeisure = true;
    else if (a === "--authorize-paid-research") out.authorizePaidResearch = true;
    else if (a === "--allow-live-pilot") out.allowLivePilot = true;
    else if (a === "--authorize-publication") out.authorizePublication = true;
  }
  return out;
}

function main() {
  const args = parseArgs(process.argv.slice(2));

  if (args.coverageOnly) {
    const report = {
      coverageMatrix: buildLeisureFourteenSegmentCoverageMatrix(),
      modules: listLeisureModuleIds(),
      adpLinkage: designLeisureAdpLinkage(),
      schemaGaps: proposeLeisureAirtableSchemaGaps(),
    };
    const out = path.join(
      ROOT,
      "reports/gdi-leisure-demand-intelligence-v1/COVERAGE_MATRIX.json",
    );
    fs.mkdirSync(path.dirname(out), { recursive: true });
    fs.writeFileSync(out, JSON.stringify(report, null, 2));
    console.log(JSON.stringify({ ok: true, out }, null, 2));
    return;
  }

  if (args.disableLeisure) {
    console.log(JSON.stringify(runLeisureOrchestratorDisabledCheck(), null, 2));
    return;
  }

  let hotelProfile = { hotelId: args.hotelId };
  if (args.hotelProfileJson) {
    hotelProfile = {
      ...hotelProfile,
      ...JSON.parse(args.hotelProfileJson),
    };
  }

  let needPeriodContext = null;
  if (args.needPeriodJson) {
    needPeriodContext = JSON.parse(args.needPeriodJson);
  }

  const result = runLeisureModules({
    hotelId: args.hotelId,
    hotelProfile,
    modules: args.modules,
    researchMode: args.mode,
    budgetUsd: args.budgetUsd,
    outDir: args.outDir || undefined,
    existingInventoryPath: args.existingInventory || undefined,
    needPeriodContext,
    evidenceSources: args.evidenceSources,
    authorizePaidResearch: args.authorizePaidResearch,
    allowLivePilot: args.allowLivePilot,
    authorizePublication: args.authorizePublication,
  });

  console.log(JSON.stringify(result, null, 2));
  if (!result.ok) process.exit(2);
}

main();
