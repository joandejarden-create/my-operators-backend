/**
 * Sync Founder Project Plan Phase Number + Step Number for sortable roadmap order.
 *
 *   node scripts/sync-founder-project-plan-phase-order.mjs --dry-run
 *   node scripts/sync-founder-project-plan-phase-order.mjs --execute
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
import {
  MAP_MASTER_TODO,
  MASTER_TODO_DEFAULT_TABLE_ID,
} from "../lib/dealality-master-todo/master-todo-field-map.js";
import {
  PHASE_NUM_FIELD,
  STEP_NUM_FIELD,
  computePhaseAndStepNumbers,
  phaseOrderNeedsUpdate,
} from "../lib/dealality-master-todo/founder-project-plan-phase-order.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-phase-order-report.json");

const F = MAP_MASTER_TODO;

async function ensureStepNumberField(token, baseId) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error?.message || `Meta API ${res.status}`);

  const table = data.tables?.find((t) => t.id === MASTER_TODO_DEFAULT_TABLE_ID);
  if (!table) throw new Error("Founder Project Plan table not found");

  const hasStep = table.fields.some((f) => f.name === STEP_NUM_FIELD);
  if (hasStep) return { created: false, fieldName: STEP_NUM_FIELD };

  if (!EXECUTE) {
    return { created: false, fieldName: STEP_NUM_FIELD, wouldCreate: true };
  }

  const createRes = await fetch(
    `https://api.airtable.com/v0/meta/bases/${baseId}/tables/${MASTER_TODO_DEFAULT_TABLE_ID}/fields`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        name: STEP_NUM_FIELD,
        type: "number",
        options: { precision: 0 },
      }),
    }
  );
  const createData = await createRes.json();
  if (!createRes.ok) {
    throw new Error(createData.error?.message || `Create field failed ${createRes.status}`);
  }
  return { created: true, fieldName: STEP_NUM_FIELD, fieldId: createData.id };
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const fieldResult = await ensureStepNumberField(token, baseId);

  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);
  const schedule = computePhaseAndStepNumbers(records);

  const toUpdate = [];
  for (const rec of records) {
    const proposed = schedule.get(rec.id);
    if (!proposed) continue;
    if (!phaseOrderNeedsUpdate(rec.fields, proposed)) continue;
    toUpdate.push({
      id: rec.id,
      task: rec.fields?.[F.task],
      phase: rec.fields?.[F.phase],
      workstream: rec.fields?.[F.workstream],
      before: {
        phaseNumber: rec.fields?.[PHASE_NUM_FIELD] ?? null,
        stepNumber: rec.fields?.[STEP_NUM_FIELD] ?? null,
      },
      after: proposed,
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    tableId: MASTER_TODO_DEFAULT_TABLE_ID,
    fieldSetup: fieldResult,
    sortHint: "Airtable view sort: Phase Number (asc), then Step Number (asc)",
    scanned: records.length,
    toUpdateCount: toUpdate.length,
    toUpdate,
    updated: [],
    errors: [],
  };

  if (EXECUTE && toUpdate.length) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    for (let i = 0; i < toUpdate.length; i += 10) {
      const batch = toUpdate.slice(i, i + 10).map((u) => ({
        id: u.id,
        fields: {
          [PHASE_NUM_FIELD]: u.after.phaseNumber,
          [STEP_NUM_FIELD]: u.after.stepNumber,
        },
      }));
      try {
        const updated = await base(MASTER_TODO_DEFAULT_TABLE_ID).update(batch, { typecast: true });
        report.updated.push(
          ...updated.map((r) => ({
            id: r.id,
            phaseNumber: r.fields?.[PHASE_NUM_FIELD],
            stepNumber: r.fields?.[STEP_NUM_FIELD],
          }))
        );
      } catch (err) {
        report.errors.push({ batch: i / 10, message: err.message || String(err) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`\nFPP Phase Order (${report.mode})`);
  console.log(`Step Number field: ${fieldResult.created ? "created" : fieldResult.wouldCreate ? "will create on execute" : "exists"}`);
  console.log(`Scanned: ${report.scanned} | To update: ${report.toUpdateCount}`);
  if (EXECUTE) {
    console.log(`Updated: ${report.updated.length} | Errors: ${report.errors.length}`);
  }
  console.log(`Sort: Phase Number ↑, Step Number ↑`);
  console.log(`Report: ${REPORT_PATH}`);

  const sample = toUpdate
    .filter((u) => u.after.phaseNumber <= 5 && u.after.stepNumber <= 3)
    .slice(0, 12);
  if (sample.length) {
    console.log("\nSample (early phases):");
    sample.forEach((u) => {
      console.log(
        `  P${u.after.phaseNumber} S${String(u.after.stepNumber).padStart(2)} | ${u.phase} | ${(u.task || "").slice(0, 50)}`
      );
    });
  }

  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[sync-founder-project-plan-phase-order]", err.message || err);
  process.exit(1);
});
