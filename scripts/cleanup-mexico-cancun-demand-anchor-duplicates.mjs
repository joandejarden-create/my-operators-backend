#!/usr/bin/env node
/**
 * Suppress weaker Mexico Cancún duplicate Demand Anchors (dry-run by default).
 *
 *   node scripts/cleanup-mexico-cancun-demand-anchor-duplicates.mjs
 *   node scripts/cleanup-mexico-cancun-demand-anchor-duplicates.mjs --apply --verbose
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { fetchDemandAnchorRecords } from "../lib/demand-anchors/airtable-demand-anchors-io.js";
import {
  getDemandAnchorsAirtableConfig,
  resolveDemandAnchorsTableName,
} from "../lib/demand-anchors/demand-anchors-base.js";
import {
  planMexicoCancunDuplicateCleanup,
  buildSuppressionPatch,
  resolveGovernanceFieldMap,
} from "../lib/radar-buildout/mexico-cancun-demand-anchor-duplicate-cleanup.js";
import {
  fetchAirtableTableFieldNameSet,
  filterFieldsToAirtableSchema,
} from "../lib/third-party-operator-basics-airtable-column-aliases.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function getArg(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  return idx >= 0 ? process.argv[idx + 1] : fallback;
}

const APPLY = process.argv.includes("--apply");
const VERBOSE = process.argv.includes("--verbose");
const country = getArg("--country", "Mexico");
const market = getArg("--market", "Cancún / Riviera Maya");
const strategy = getArg("--strategy", "suppress");
const output = getArg("--output", "data/mexico-cancun-da-cleanup-report.json");

if (strategy !== "suppress") {
  console.error("Only --strategy suppress is supported.");
  process.exit(1);
}

const result = await fetchDemandAnchorRecords({ country, includeHidden: true });
if (result.error) {
  console.error("Failed to fetch Demand Anchors:", result.error);
  process.exit(1);
}

const records = result.allPoints || result.points || [];
const plan = planMexicoCancunDuplicateCleanup(records, { country, market });

console.log(APPLY ? "=== APPLY duplicate suppression ===" : "=== DRY RUN duplicate suppression ===");
console.log("Country:", country, "| Market:", market);
console.log("Records scanned:", plan.scanned);
console.log("Safe definite pairs:", plan.safePairs);
console.log("Duplicate clusters:", plan.clusters);
console.log("Proposed suppressions:", plan.proposedSuppressions.length);
console.log("Proposed keep:", plan.proposedKeep.length);
console.log("Manual review pairs (not auto-suppressed):", plan.manualReviewPairs.length);

if (plan.samples.length) {
  console.log("\nSample proposed suppressions:");
  for (const s of plan.samples) {
    console.log(
      `  SUPPRESS ${s.suppressName} (${s.suppressId}) → keep ${s.keepName} (${s.keepId}) [${s.reason}] scores ${s.scoreWeak}/${s.scoreKeep}`
    );
  }
}

if (plan.manualReviewPairs.length && VERBOSE) {
  console.log("\nManual review (first 10):");
  for (const p of plan.manualReviewPairs.slice(0, 10)) {
    console.log(`  ${p.recordA.name} ↔ ${p.recordB.name} (${p.reason})`);
  }
}

const cfg = getDemandAnchorsAirtableConfig();
if (!cfg) {
  console.error("Airtable config missing");
  process.exit(1);
}
const tableName = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);
const governanceMap = resolveGovernanceFieldMap(schema);

const applyReport = {
  suppressed: [],
  skipped: [],
  errors: [],
};

const AIRTABLE_BATCH_DELAY_MS = Number(process.env.AIRTABLE_WRITE_DELAY_MS) || 220;
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

if (APPLY) {
  for (const item of plan.proposedSuppressions) {
    const patch = buildSuppressionPatch(item.record, governanceMap, schema);
    if (!Object.keys(patch).length) {
      applyReport.skipped.push({ id: item.record.id, name: item.record.name, reason: "empty_patch" });
      continue;
    }
    try {
      await cfg.base(tableName).update(item.record.id, patch, { typecast: true });
      applyReport.suppressed.push({
        id: item.record.id,
        name: item.record.name,
        keepId: item.keepId,
        keepName: item.keepName,
        reason: item.reason,
      });
      if (VERBOSE) console.log("SUPPRESSED", item.record.name, "→ keep", item.keepName);
    } catch (err) {
      applyReport.errors.push({
        id: item.record.id,
        name: item.record.name,
        message: err?.message || String(err),
      });
      console.error("FAIL", item.record.name, err?.message || err);
    }
    await sleep(AIRTABLE_BATCH_DELAY_MS);
  }
  console.log("\nApply:", `suppressed=${applyReport.suppressed.length}`, `skipped=${applyReport.skipped.length}`, `errors=${applyReport.errors.length}`);
}

const report = {
  generatedAt: new Date().toISOString(),
  mode: APPLY ? "apply" : "dry-run",
  country,
  market,
  strategy,
  plan: {
    scanned: plan.scanned,
    safePairs: plan.safePairs,
    clusters: plan.clusters,
    proposedSuppressions: plan.proposedSuppressions.length,
    proposedKeep: plan.proposedKeep.length,
    manualReviewPairs: plan.manualReviewPairs.length,
    samples: plan.samples,
  },
  apply: APPLY ? applyReport : null,
  governanceFieldsUsed: governanceMap,
};

const outPath = join(root, output);
const outDir = dirname(outPath);
if (!existsSync(outDir)) mkdirSync(outDir, { recursive: true });
writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
console.log("Report written:", output);

if (!APPLY) {
  console.log("\nNo writes performed. Re-run with --apply to suppress duplicates.");
}

if (applyReport.errors.length) process.exit(1);
