/**
 * Persist GDI Research Runs for prior AC + Spice discovery cycles (reconciliation).
 * Creates Research Run records linked to hotelId; does not invent opportunities.
 *
 *   node scripts/persist-gdi-research-runs-ac-spice.mjs --dry-run
 *   node scripts/persist-gdi-research-runs-ac-spice.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { upsertResearchRun } from "../lib/group-demand-intelligence/research-coverage/airtable-stores.js";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/decision-outcomes/airtable-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

const intel =
  getGdiOpportunitiesAirtableBaseId() ||
  process.env.ADP_AIRTABLE_BASE_ID ||
  CANONICAL_INTELLIGENCE_BASE_ID;
assertNotLegacyMvpCanonicalBase(intel, { surface: "gdi-research-run-persist" });

const RUNS = [
  {
    hotelId: "rec2PVBDavppGpenm",
    hotelName: "AC Hotel A Coruña",
    summaryPath: "reports/group-demand-intelligence/ac-hotel-a-coruna-v1/GDI_FIRST_CYCLE_SUMMARY.json",
    label: "ac_first_cycle",
  },
  {
    hotelId: "recKRJjcPnb4tVDDS",
    hotelName: "Spice Island Beach Resort",
    summaryPath: "reports/group-demand-intelligence/spice-island-beach-resort-v1/GDI_FIRST_CYCLE_SUMMARY.json",
    label: "spice_first_cycle",
  },
];

function readJson(p) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, p), "utf8"));
}

const results = [];
for (const r of RUNS) {
  const summary = readJson(r.summaryPath);
  const runId = `grun_${r.label}_${String(summary.startedAt || "")
    .replace(/[^0-9]/g, "")
    .slice(0, 14)}`;
  const raw = {
    runId,
    hotelId: r.hotelId,
    hotelName: r.hotelName,
    runType: "MANUAL",
    status: "COMPLETED",
    startedAt: summary.startedAt,
    completedAt: summary.finishedAt,
    queries: summary.discovery?.queries ?? 0,
    fetches: summary.discovery?.fetches ?? 0,
    newOpportunities: summary.customerVisible?.total ?? 0,
    targetsAttempted: summary.discovery?.candidates ?? 0,
    estimatedCost: summary.costUsd ?? summary.qualification?.researchCostUsd ?? 0,
    notes: `Reconciliation persist of prior first-cycle discovery. VALID_WATCH=${summary.discovery?.byState?.VALID_WATCH ?? 0}; promotions=${JSON.stringify(summary.promotions || {})}`,
    payload: {
      source: r.summaryPath,
      byState: summary.discovery?.byState || {},
      promotions: summary.promotions || {},
      contact: { researched: summary.contact?.researched ?? 0 },
    },
  };
  const res = await upsertResearchRun(raw, { dryRun: !APPLY });
  results.push({ hotelId: r.hotelId, runId, apply: APPLY, result: res });
}

const outDir = path.join(ROOT, "reports", "hotel-census", "ac-spice-persistence-reconciliation-v1");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, `RESEARCH_RUNS_${APPLY ? "apply" : "dry"}.json`);
fs.writeFileSync(outPath, JSON.stringify({ generatedAt: new Date().toISOString(), baseId: intel, results }, null, 2) + "\n");
console.log(JSON.stringify({ outPath, results }, null, 2));
