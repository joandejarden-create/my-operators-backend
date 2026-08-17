/**
 * Sync Pilot Delivery Brand & Operator Explorer tasks from live repo coverage.
 *
 *   node scripts/sync-founder-project-plan-explorer-coverage.mjs --dry-run
 *   node scripts/sync-founder-project-plan-explorer-coverage.mjs --execute
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  fetchAllRecords,
  getGtmConfig,
} from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import { MASTER_TODO_DEFAULT_TABLE_ID } from "../lib/dealality-master-todo/master-todo-field-map.js";
import {
  buildExplorerCoveragePatch,
  listExplorerCoverageRecordIds,
  summarizeChoiceBrandCoverage,
} from "../lib/dealality-master-todo/founder-project-plan-explorer-coverage-status.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-explorer-coverage-report.json");

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);
  const ids = new Set(listExplorerCoverageRecordIds());

  const toUpdate = [];
  for (const rec of records) {
    if (!ids.has(rec.id)) continue;
    const built = buildExplorerCoveragePatch(rec.id, rec.fields);
    if (!built) continue;
    toUpdate.push({
      id: rec.id,
      task: rec.fields?.Task,
      step: rec.fields?.["Step Number"],
      before: {
        status: rec.fields?.Status,
        progress: rec.fields?.Progress,
        nextAction: rec.fields?.["Next Action"],
      },
      after: built.target,
      patch: built.patch,
    });
  }

  const summary = summarizeChoiceBrandCoverage();

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    choiceBrandSummary: summary,
    updateCount: toUpdate.length,
    toUpdate,
    updated: [],
    errors: [],
  };

  if (EXECUTE && toUpdate.length) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    for (let i = 0; i < toUpdate.length; i += 10) {
      const batch = toUpdate.slice(i, i + 10).map((u) => ({
        id: u.id,
        fields: u.patch,
      }));
      try {
        const updated = await base(MASTER_TODO_DEFAULT_TABLE_ID).update(batch, {
          typecast: true,
        });
        updated.forEach((row) => {
          report.updated.push({
            id: row.id,
            status: row.fields?.Status,
            progress: row.fields?.Progress,
          });
        });
      } catch (err) {
        report.errors.push({
          batch: batch.map((b) => b.id),
          error: err?.message || String(err),
        });
      }
    }
  }

  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);
  console.log(`Choice CHI: ${summary.total} brands — L2: ${summary.l2Complete.length}, L1: ${summary.l1Generated.length}, needs L2: ${summary.needsEnrichment.length}`);
  console.log(`Explorer FPP updates: ${toUpdate.length} (${EXECUTE ? "execute" : "dry-run"})`);
  console.log(`Report: ${REPORT_PATH}`);
  if (report.errors.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
