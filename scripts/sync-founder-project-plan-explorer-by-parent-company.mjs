/**
 * Restructure Brand Explorer FPP tasks by parent company:
 * - Defer chain-scale segment tasks (steps 6–13)
 * - Create/update parent-company brand tasks (CHI, Hilton, IHG)
 * - Refresh master tracker + operator task status from coverage config
 *
 *   node scripts/sync-founder-project-plan-explorer-by-parent-company.mjs --dry-run
 *   node scripts/sync-founder-project-plan-explorer-by-parent-company.mjs --execute
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  buildMatchKey,
  fetchAllRecords,
  getGtmConfig,
} from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import {
  MAP_MASTER_TODO,
  MASTER_TODO_DEFAULT_TABLE_ID,
} from "../lib/dealality-master-todo/master-todo-field-map.js";
import {
  buildExplorerCoveragePatch,
  listExplorerCoverageRecordIds,
  summarizeChoiceBrandCoverage,
} from "../lib/dealality-master-todo/founder-project-plan-explorer-coverage-status.js";
import {
  PARENT_COMPANY_BRAND_SEEDS,
  SEGMENT_BRAND_TASK_RECORD_IDS,
  buildParentCompanyUpdatePatch,
  buildSegmentDeferPatch,
  buildParentCompanyBrandTargets,
  parentCompanySeedToCreateFields,
  seedIdFromRecord,
} from "../lib/dealality-master-todo/founder-project-plan-explorer-by-parent-company.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const REPORT_PATH = path.resolve(
  ROOT,
  "reports/founder-project-plan-explorer-by-parent-company-report.json"
);

const MASTER_TRACKER_ID = "recHls6zriLafoqJT";

function indexByMatchKey(records) {
  const map = new Map();
  for (const rec of records) {
    const task = rec.fields?.[MAP_MASTER_TODO.task] || "";
    const ws = rec.fields?.[MAP_MASTER_TODO.workstream] || "";
    const key = buildMatchKey(task, ws);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(rec);
  }
  return map;
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);
  const byId = new Map(records.map((r) => [r.id, r]));
  const byMatch = indexByMatchKey(records);
  const parentTargets = buildParentCompanyBrandTargets();

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    choiceBrandSummary: summarizeChoiceBrandCoverage(),
    segmentDeferred: [],
    parentCompanyCreated: [],
    parentCompanyUpdated: [],
    coverageUpdated: [],
    errors: [],
  };

  const toCreate = [];
  const toUpdate = [];

  // 1) Defer segment brand tasks
  for (const id of SEGMENT_BRAND_TASK_RECORD_IDS) {
    const rec = byId.get(id);
    if (!rec) {
      report.errors.push({ id, error: "Segment task record not found" });
      continue;
    }
    const patch = buildSegmentDeferPatch(rec.fields);
    if (!patch) continue;
    const entry = {
      id,
      task: rec.fields?.Task,
      step: rec.fields?.["Step Number"],
      action: "defer-segment",
      patch,
    };
    report.segmentDeferred.push(entry);
    toUpdate.push(entry);
  }

  // 2) Parent-company brand tasks — create or update
  for (const seed of PARENT_COMPANY_BRAND_SEEDS) {
    const key = buildMatchKey(seed.taskName, "Brand & Operator Explorers");
    const hits = byMatch.get(key) || [];
    const target = parentTargets[seed.seedId];

    if (hits.length === 0) {
      const fields = parentCompanySeedToCreateFields(seed);
      fields[MAP_MASTER_TODO.status] = target.status;
      fields[MAP_MASTER_TODO.progress] = target.progress;
      fields[MAP_MASTER_TODO.nextAction] = target.nextAction;
      fields[MAP_MASTER_TODO.blocker] = target.blocker || "None";
      const entry = { seedId: seed.seedId, parentCompany: seed.parentCompany, action: "create", fields };
      report.parentCompanyCreated.push(entry);
      toCreate.push(entry);
      continue;
    }

    const rec = hits[0];
    if (hits.length > 1) {
      report.errors.push({
        seedId: seed.seedId,
        error: `Ambiguous match (${hits.length} records) for ${seed.taskName}`,
      });
    }
    const patch = buildParentCompanyUpdatePatch(seed.seedId, rec.fields, target);
    if (!patch) continue;
    const entry = {
      id: rec.id,
      seedId: seed.seedId,
      parentCompany: seed.parentCompany,
      task: rec.fields?.Task,
      step: rec.fields?.["Step Number"],
      action: "update-parent",
      before: {
        status: rec.fields?.Status,
        progress: rec.fields?.Progress,
      },
      after: target,
      patch,
    };
    report.parentCompanyUpdated.push(entry);
    toUpdate.push(entry);
  }

  // 3) Master tracker + operator tasks (existing coverage sync)
  const coverageIds = new Set(listExplorerCoverageRecordIds());
  for (const rec of records) {
    if (!coverageIds.has(rec.id)) continue;
    if (SEGMENT_BRAND_TASK_RECORD_IDS.includes(rec.id)) continue;
    const built = buildExplorerCoveragePatch(rec.id, rec.fields);
    if (!built) continue;

    if (rec.id === MASTER_TRACKER_ID) {
      built.target.nextAction =
        "Tracker published (parent-company view): reports/brand-operator-explorer-coverage-tracker.md. Brand tasks: CHI step 6, Hilton step 7, IHG step 8. Joan: sign off tracker → Completed.";
      built.patch["Next Action"] = built.target.nextAction;
    }

    const entry = {
      id: rec.id,
      task: rec.fields?.Task,
      step: rec.fields?.["Step Number"],
      action: "coverage-sync",
      patch: built.patch,
      after: built.target,
    };
    report.coverageUpdated.push(entry);
    toUpdate.push(entry);
  }

  if (EXECUTE) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    const table = base(MASTER_TODO_DEFAULT_TABLE_ID);

    for (let i = 0; i < toCreate.length; i += 10) {
      const batch = toCreate.slice(i, i + 10).map((c) => ({ fields: c.fields }));
      try {
        const created = await table.create(batch, { typecast: true });
        created.forEach((row, idx) => {
          const src = toCreate[i + idx];
          report.parentCompanyCreated.find((x) => x === src).recordId = row.id;
        });
      } catch (err) {
        report.errors.push({
          action: "create",
          error: err?.message || String(err),
        });
      }
    }

    const updateRows = toUpdate.filter((u) => u.id);
    for (let i = 0; i < updateRows.length; i += 10) {
      const batch = updateRows.slice(i, i + 10).map((u) => ({
        id: u.id,
        fields: u.patch,
      }));
      try {
        await table.update(batch, { typecast: true });
      } catch (err) {
        report.errors.push({
          action: "update",
          ids: batch.map((b) => b.id),
          error: err?.message || String(err),
        });
      }
    }
  }

  report.totals = {
    segmentDeferred: report.segmentDeferred.length,
    parentCreated: report.parentCompanyCreated.length,
    parentUpdated: report.parentCompanyUpdated.length,
    coverageUpdated: report.coverageUpdated.length,
    updateRows: toUpdate.length,
  };

  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`Mode: ${EXECUTE ? "execute" : "dry-run"}`);
  console.log(
    `CHI: ${report.choiceBrandSummary.total} brands — L2: ${report.choiceBrandSummary.l2Complete.length}, L1: ${report.choiceBrandSummary.l1Generated.length}, needs L2: ${report.choiceBrandSummary.needsEnrichment.length}`
  );
  console.log(`Defer segment tasks: ${report.segmentDeferred.length}`);
  console.log(`Create parent-company tasks: ${report.parentCompanyCreated.length}`);
  console.log(`Update parent-company tasks: ${report.parentCompanyUpdated.length}`);
  console.log(`Coverage sync (tracker + operators): ${report.coverageUpdated.length}`);
  console.log(`Report: ${REPORT_PATH}`);
  if (report.errors.length) {
    console.error("Errors:", report.errors);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
