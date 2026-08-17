/**
 * Validate Founder Project Plan Phase Number / Step Number / Roadmap Sort.
 * Optionally repair drift via sync.
 *
 *   node scripts/validate-founder-project-plan-phase-order.mjs
 *   node scripts/validate-founder-project-plan-phase-order.mjs --fix
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
const FIX = process.argv.includes("--fix");
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-phase-order-validate.json");

const F = MAP_MASTER_TODO;

function expectedRoadmapSort(phaseNumber, stepNumber) {
  return Math.round((phaseNumber + stepNumber / 100) * 100) / 100;
}

function validatePhases(records) {
  const byPhase = new Map();
  for (const rec of records) {
    const phase = rec.fields?.[F.phase];
    if (!phase || phase === "PHASE" || phase === "__") continue;
    if (!byPhase.has(phase)) byPhase.set(phase, []);
    byPhase.get(phase).push(rec);
  }

  const phases = [];
  let ok = true;

  for (const [phaseName, items] of [...byPhase.entries()].sort((a, b) => {
    const pa = a[1][0]?.fields?.[PHASE_NUM_FIELD] ?? 99;
    const pb = b[1][0]?.fields?.[PHASE_NUM_FIELD] ?? 99;
    return pa - pb;
  })) {
    const sorted = [...items].sort(
      (a, b) => (a.fields?.[STEP_NUM_FIELD] ?? 0) - (b.fields?.[STEP_NUM_FIELD] ?? 0)
    );
    const phaseNumber = sorted[0]?.fields?.[PHASE_NUM_FIELD];
    const steps = sorted.map((r) => r.fields?.[STEP_NUM_FIELD]);
    const missing = [];
    for (let i = 1; i <= sorted.length; i += 1) {
      if (!steps.includes(i)) missing.push(i);
    }
    const duplicateSteps = steps.filter((s, i, arr) => arr.indexOf(s) !== i);
    const phaseNumSet = [...new Set(sorted.map((r) => r.fields?.[PHASE_NUM_FIELD]))];
    const badSort = sorted.filter((r) => {
      const pn = r.fields?.[PHASE_NUM_FIELD];
      const sn = r.fields?.[STEP_NUM_FIELD];
      const rs = r.fields?.["Roadmap Sort"];
      if (pn == null || sn == null) return true;
      const exp = expectedRoadmapSort(pn, sn);
      return rs == null || Math.abs(rs - exp) > 0.001;
    });

    const phaseOk =
      missing.length === 0 &&
      duplicateSteps.length === 0 &&
      phaseNumSet.length === 1 &&
      steps.length === sorted.length &&
      badSort.length === 0;

    if (!phaseOk) ok = false;

    phases.push({
      phase: phaseName,
      phaseNumber: phaseNumSet[0] ?? null,
      taskCount: sorted.length,
      stepRange: sorted.length ? `1-${sorted.length}` : null,
      missingSteps: missing,
      duplicateSteps: [...new Set(duplicateSteps)],
      inconsistentPhaseNumbers: phaseNumSet,
      roadmapSortIssues: badSort.length,
      ok: phaseOk,
    });
  }

  return { ok, phases };
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);
  const computed = computePhaseAndStepNumbers(records);

  const drift = [];
  for (const rec of records) {
    const exp = computed.get(rec.id);
    if (!exp) continue;
    if (phaseOrderNeedsUpdate(rec.fields, exp)) {
      drift.push({
        id: rec.id,
        task: rec.fields?.[F.task],
        before: {
          phaseNumber: rec.fields?.[PHASE_NUM_FIELD],
          stepNumber: rec.fields?.[STEP_NUM_FIELD],
        },
        after: exp,
      });
    }
  }

  const phaseValidation = validatePhases(records);

  const report = {
    generatedAt: new Date().toISOString(),
    tableId: MASTER_TODO_DEFAULT_TABLE_ID,
    recordCount: records.length,
    scheduledCount: computed.size,
    driftCount: drift.length,
    phaseValidation,
    fixed: [],
    errors: [],
  };

  if (FIX && drift.length) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    for (let i = 0; i < drift.length; i += 10) {
      const batch = drift.slice(i, i + 10).map((u) => ({
        id: u.id,
        fields: {
          [PHASE_NUM_FIELD]: u.after.phaseNumber,
          [STEP_NUM_FIELD]: u.after.stepNumber,
        },
      }));
      try {
        const updated = await base(MASTER_TODO_DEFAULT_TABLE_ID).update(batch, { typecast: true });
        report.fixed.push(...updated.map((r) => ({ id: r.id })));
      } catch (err) {
        report.errors.push({ batch: i / 10, message: err.message || String(err) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log("\nFPP Phase Order Validation");
  console.log(`Records: ${report.recordCount} | Scheduled: ${report.scheduledCount}`);
  console.log(`Phase contiguity: ${phaseValidation.ok ? "PASS" : "FAIL"}`);
  console.log(`Drift vs logical order: ${drift.length} row(s)`);

  for (const p of phaseValidation.phases) {
    const flag = p.ok ? "OK" : "FAIL";
    console.log(
      `  ${flag} P${p.phaseNumber} ${p.phase} — ${p.taskCount} tasks, steps ${p.stepRange}, Roadmap ${p.phaseNumber}.01–${p.phaseNumber}.${String(p.taskCount).padStart(2, "0")}`
    );
    if (!p.ok) {
      if (p.missingSteps.length) console.log(`       missing steps: ${p.missingSteps.join(", ")}`);
      if (p.duplicateSteps.length) console.log(`       duplicate steps: ${p.duplicateSteps.join(", ")}`);
      if (p.roadmapSortIssues) console.log(`       Roadmap Sort issues: ${p.roadmapSortIssues}`);
    }
  }

  if (FIX && drift.length) {
    console.log(`\nFixed: ${report.fixed.length} | Errors: ${report.errors.length}`);
  } else if (drift.length) {
    console.log("\nRun with --fix to repair drift: node scripts/validate-founder-project-plan-phase-order.mjs --fix");
  }

  console.log(`Report: ${REPORT_PATH}`);

  if (!phaseValidation.ok || report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[validate-founder-project-plan-phase-order]", err.message || err);
  process.exit(1);
});
