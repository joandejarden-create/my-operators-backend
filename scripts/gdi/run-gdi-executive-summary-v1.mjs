#!/usr/bin/env node
/**
 * Read-only GDI Executive Summary generate / validate CLI.
 * Never triggers Exa, Surfe, Findymail, or paid research.
 *
 *   node scripts/gdi/run-gdi-executive-summary-v1.mjs --hotel-id rec… --mode validate
 *   node scripts/gdi/run-gdi-executive-summary-v1.mjs --hotel-id rec… --mode generate --out …
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelProfile } from "../../lib/group-demand-intelligence/repository.js";
import { loadFeedback } from "../../lib/group-demand-intelligence/repository.js";
import { listPursuits } from "../../lib/group-demand-intelligence/pursuit/pursuit-store-v1.js";
import {
  buildGdiExecutiveSummaryV1,
  EXEC_SUMMARY_DATA_STATE,
  GDI_EXECUTIVE_SUMMARY_AUTH,
} from "../../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v1.js";
import { resolveExecutiveSummaryInventory } from "../../api/gdi-executive-summary.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function parseArgs(argv) {
  const out = {
    hotelId: "",
    mode: "validate",
    outDir: "",
    preview: "",
    founderPreview: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    if (a === "--hotel-id") out.hotelId = next();
    else if (a === "--mode") out.mode = next();
    else if (a === "--out" || a === "--out-dir") out.outDir = next();
    else if (a === "--preview") out.preview = next();
    else if (a === "--founder-preview") out.founderPreview = true;
  }
  return out;
}

function assertNoPaidSideEffects(summary) {
  const g = summary?.governance || {};
  const errors = [];
  if (g.noPaidResearch !== true) errors.push("PAID_RESEARCH_GUARD_MISSING");
  if (g.noPaidEnrichment !== true) errors.push("PAID_ENRICHMENT_GUARD_MISSING");
  if (g.noFindymail !== true) errors.push("FINDYMAIL_GUARD_MISSING");
  if (g.noSurfeAuto !== true) errors.push("SURFE_GUARD_MISSING");
  if (g.noStagedAsCustomerFallback !== true) {
    errors.push("STAGED_FALLBACK_GUARD_MISSING");
  }
  if (summary?.pdfPreparation?.pagesRequired !== 2) {
    errors.push("PDF_PAGE_CONTRACT_NOT_TWO");
  }
  return errors;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.hotelId) {
    console.error("Usage: --hotel-id rec… [--mode validate|generate] [--preview staged] [--out dir]");
    process.exit(2);
  }

  const hotel = loadHotelProfile(args.hotelId) || { hotelId: args.hotelId };
  const inventory = resolveExecutiveSummaryInventory(args.hotelId, {
    preview: args.preview,
    founderPreview: args.founderPreview || args.preview === "staged",
  });

  let pursuits = [];
  let feedback = {};
  try {
    pursuits = listPursuits(args.hotelId) || [];
  } catch {
    pursuits = [];
  }
  try {
    feedback = loadFeedback(args.hotelId) || {};
  } catch {
    feedback = {};
  }

  const summary = buildGdiExecutiveSummaryV1({
    hotelId: args.hotelId,
    hotelName: hotel.name || hotel.hotelName || null,
    opportunities: inventory.opportunities || [],
    customerAuthorizedInventory: inventory.customerAuthorizedInventory === true,
    dataState: inventory.dataState || EXEC_SUMMARY_DATA_STATE.NO_ELIGIBLE_PUBLICATION,
    publicationMeta: inventory.publicationMeta || null,
    pursuits,
    feedback,
    founderPreview: inventory.founderPreview === true,
  });

  const guardErrors = assertNoPaidSideEffects(summary);
  const result = {
    ok: guardErrors.length === 0 && summary.ok !== false,
    mode: args.mode,
    authorizationId: GDI_EXECUTIVE_SUMMARY_AUTH,
    hotelId: args.hotelId,
    dataState: summary.dataState,
    customerFacing: summary.customerFacing,
    opportunityOfTheWeek:
      summary.sections?.priorityCommercialOpportunities?.opportunityOfTheWeek
        ?.opportunityId || null,
    priorityCount:
      summary.sections?.priorityCommercialOpportunities?.count || 0,
    researchReceiptMetricCount:
      summary.sections?.researchReceipt?.metrics?.length || 0,
    paidSideEffects: false,
    guardErrors,
    summary,
  };

  const outDir =
    args.outDir ||
    path.join(
      ROOT,
      "reports/gdi-executive-summary-v1/runs",
      `${args.hotelId}_${Date.now()}`,
    );
  if (args.mode === "generate" || args.outDir) {
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "executive-summary.json"),
      `${JSON.stringify(result, null, 2)}\n`,
    );
    result.outDir = outDir;
  }

  console.log(JSON.stringify(result, null, 2));
  if (!result.ok) process.exit(2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
