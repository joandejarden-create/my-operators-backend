/**
 * Sync chain-scale × parent-company Brand Explorer FPP rows (60 brand tasks)
 * plus operator-type tasks.
 *
 *   node scripts/sync-founder-project-plan-explorer-chain-scale.mjs --dry-run
 *   node scripts/sync-founder-project-plan-explorer-chain-scale.mjs --execute
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
import { MASTER_TODO_DEFAULT_TABLE_ID } from "../lib/dealality-master-todo/master-todo-field-map.js";
import {
  OPERATOR_EXPLORER_SEGMENTS,
  MASTER_TRACKER_RECORD_ID,
  PARENT_COMPANY_TASK_RECORD_IDS,
  EXPLORER_WORKSTREAM,
  scoreAllParentTasks,
  buildParentTaskPatch,
  buildParentTaskCreateFields,
  buildOperatorSegmentPatch,
  buildSupersededDeferPatch,
  buildMasterTrackerPatch,
  renderParentTrackerMarkdown,
  isAggregateChainScaleRecord,
  seedIdFromFields,
  operatorStepNumber,
} from "../lib/dealality-master-todo/founder-project-plan-explorer-chain-scale-parent-sync.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-explorer-chain-scale-parent-report.json");
const TRACKER_PATH = path.resolve(ROOT, "reports/brand-operator-explorer-coverage-tracker.md");

function diffPatch(existing, patch) {
  const out = {};
  for (const [k, v] of Object.entries(patch)) {
    if (existing?.[k] !== v) out[k] = v;
  }
  return Object.keys(out).length ? out : null;
}

function indexBySeedId(records) {
  const map = new Map();
  for (const rec of records) {
    const seed = seedIdFromFields(rec.fields);
    if (seed) map.set(seed, rec);
  }
  return map;
}

function indexByMatchKey(records) {
  const map = new Map();
  for (const rec of records) {
    const key = buildMatchKey(rec.fields?.Task || "", rec.fields?.Workstream || "");
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(rec);
  }
  return map;
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);
  const byId = new Map(records.map((r) => [r.id, r]));
  const bySeed = indexBySeedId(records);
  const byMatch = indexByMatchKey(records);

  const scoredParents = scoreAllParentTasks();
  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    parentTaskCount: scoredParents.length,
    parentTasks: scoredParents.map(({ task, coverage }) => ({
      step: task.stepNumber,
      seedId: task.seedId,
      parent: task.parent,
      scale: task.scaleLabel,
      brands: coverage.total,
      inRepo: coverage.inRepo,
      progress: coverage.progress,
      status: coverage.status,
    })),
    supersededDeferred: [],
    parentCreated: [],
    parentUpdated: [],
    operatorUpdated: [],
    masterTrackerUpdated: null,
    errors: [],
  };

  const toCreate = [];
  const toUpdate = [];

  const supersedeIds = new Set(PARENT_COMPANY_TASK_RECORD_IDS);
  for (const rec of records) {
    if (rec.fields?.Workstream !== EXPLORER_WORKSTREAM) continue;
    if (!rec.fields?.Task?.startsWith("Complete Brand Explorer profiles")) continue;
    if (isAggregateChainScaleRecord(rec.fields)) supersedeIds.add(rec.id);
  }

  for (const id of supersedeIds) {
    const rec = byId.get(id);
    if (!rec) continue;
    const patch = buildSupersededDeferPatch(rec.fields);
    if (!patch) continue;
    report.supersededDeferred.push({ id, task: rec.fields?.Task, patch });
    toUpdate.push({ id, patch });
  }

  for (const { task, coverage } of scoredParents) {
    let rec = bySeed.get(task.seedId);
    if (!rec) {
      const key = buildMatchKey(task.taskName, EXPLORER_WORKSTREAM);
      const hits = byMatch.get(key) || [];
      if (hits.length) rec = hits[0];
    }

    if (!rec) {
      const fields = buildParentTaskCreateFields(task, coverage);
      const entry = { seedId: task.seedId, step: task.stepNumber, fields };
      report.parentCreated.push(entry);
      toCreate.push(entry);
      continue;
    }

    const fullPatch = buildParentTaskPatch(task, coverage);
    const patch = diffPatch(rec.fields, fullPatch);
    if (!patch) continue;
    report.parentUpdated.push({
      id: rec.id,
      seedId: task.seedId,
      step: task.stepNumber,
      parent: task.parent,
      scale: task.scaleLabel,
      patch,
    });
    toUpdate.push({ id: rec.id, patch });
  }

  const opStepStart = operatorStepNumber(scoredParents.length);
  OPERATOR_EXPLORER_SEGMENTS.forEach((segment, idx) => {
    const rec = byId.get(segment.recordId);
    if (!rec) {
      report.errors.push({ id: segment.recordId, error: "Operator segment not found" });
      return;
    }
    const step = opStepStart + idx;
    const fullPatch = buildOperatorSegmentPatch(segment, step);
    const patch = diffPatch(rec.fields, fullPatch);
    if (!patch) return;
    report.operatorUpdated.push({ id: segment.recordId, step, patch });
    toUpdate.push({ id: segment.recordId, patch });
  });

  const trackerRec = byId.get(MASTER_TRACKER_RECORD_ID);
  if (trackerRec) {
    const patch = buildMasterTrackerPatch(scoredParents);
    const diff = diffPatch(trackerRec.fields, patch);
    if (diff) {
      report.masterTrackerUpdated = { id: MASTER_TRACKER_RECORD_ID, patch: diff };
      toUpdate.push({ id: MASTER_TRACKER_RECORD_ID, patch: diff });
    }
  }

  if (EXECUTE) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    const table = base(MASTER_TODO_DEFAULT_TABLE_ID);

    for (let i = 0; i < toCreate.length; i += 10) {
      const batch = toCreate.slice(i, i + 10).map((c) => ({ fields: c.fields }));
      try {
        const created = await table.create(batch, { typecast: true });
        created.forEach((row, idx) => {
          toCreate[i + idx].recordId = row.id;
        });
      } catch (err) {
        report.errors.push({ action: "create", error: err?.message || String(err) });
      }
    }

    for (let i = 0; i < toUpdate.length; i += 10) {
      const batch = toUpdate.slice(i, i + 10).map((u) => ({ id: u.id, fields: u.patch }));
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

  const trackerHeader = fs
    .readFileSync(TRACKER_PATH, "utf8")
    .split("## Chain-scale × parent-company brand tasks")[0]
    .split("## Chain-scale brand segments")[0];

  const operatorSection = [
    "## Operator Explorer segments",
    "",
    `Steps ${opStepStart}–${opStepStart + OPERATOR_EXPLORER_SEGMENTS.length - 1} (by operator type, not chain scale).`,
    "",
    "| Step | Segment | Operators |",
    "|------|---------|-----------|",
    ...OPERATOR_EXPLORER_SEGMENTS.map((s, idx) => {
      const step = opStepStart + idx;
      return `| ${step} | ${s.taskName.replace("Complete Operator Explorer profiles — ", "")} | ${s.operators.length} |`;
    }),
    "",
    "Sync: `npm run sync:fpp-explorer-chain-scale`",
    "",
  ].join("\n");

  if (EXECUTE) {
    fs.writeFileSync(
      TRACKER_PATH,
      `${trackerHeader.trimEnd()}\n\n${renderParentTrackerMarkdown(scoredParents)}\n${operatorSection}\n`
    );
  }

  report.totals = {
    superseded: report.supersededDeferred.length,
    created: report.parentCreated.length,
    updated: report.parentUpdated.length,
    operators: report.operatorUpdated.length,
  };

  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`Mode: ${EXECUTE ? "execute" : "dry-run"}`);
  console.log(`Parent×scale brand tasks: ${scoredParents.length} (${report.parentCreated.length} create, ${report.parentUpdated.length} update)`);
  console.log(`Defer superseded: ${report.supersededDeferred.length}`);
  console.log(`Operator updates: ${report.operatorUpdated.length}`);
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
