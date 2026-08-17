/**
 * Apply FPP task updates from agent workers — Completed requires Joan approval.
 *
 *   node scripts/apply-fpp-agent-task-update.mjs --record-id recXXX --status "Needs Review" --progress "75%" --next-action "..." --worker cursor --dry-run
 *   node scripts/apply-fpp-agent-task-update.mjs --record-id recXXX --status Completed --progress "100%" --approved-by "Joan D." --worker cursor --execute
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { fetchAllRecords, getGtmConfig } from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import {
  MAP_MASTER_TODO,
  MASTER_TODO_DEFAULT_TABLE_ID,
} from "../lib/dealality-master-todo/master-todo-field-map.js";
import {
  buildAgentTaskPatch,
  formatValidationResult,
  validateAgentTaskWrite,
} from "../lib/dealality-master-todo/fpp-agent-approval.js";
import { FPP_COMPLETION_STATUS } from "../lib/dealality-master-todo/fpp-agent-workflow-config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const F = MAP_MASTER_TODO;
const EXECUTE = process.argv.includes("--execute");
const REPORT_PATH = path.resolve(ROOT, "reports/fpp-agent-task-update-report.json");

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

async function main() {
  const recordId = argValue("--record-id");
  if (!recordId) {
    console.error("Required: --record-id recXXXXXXXX");
    process.exit(1);
  }

  const status = argValue("--status");
  const progress = argValue("--progress");
  const nextAction = argValue("--next-action");
  const blocker = argValue("--blocker");
  const completedDate = argValue("--completed-date");
  const worker = argValue("--worker");
  const approvedBy = argValue("--approved-by");

  const input = {};
  if (status) input.status = status;
  if (progress) input.progress = progress;
  if (nextAction) input.nextAction = nextAction;
  if (blocker) input.blocker = blocker;
  if (completedDate) input.completedDate = completedDate;

  if (status === FPP_COMPLETION_STATUS && !completedDate) {
    input.completedDate = new Date().toISOString().slice(0, 10);
  }

  const patch = buildAgentTaskPatch(input);
  if (Object.keys(patch).length === 0) {
    console.error("No fields to update. Pass --status, --progress, and/or --next-action.");
    process.exit(1);
  }

  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);
  const rec = records.find((r) => r.id === recordId);
  if (!rec) {
    console.error(`Record not found: ${recordId}`);
    process.exit(1);
  }

  const validation = validateAgentTaskWrite({
    existingFields: rec.fields,
    patch,
    worker,
    approvedBy,
  });

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    recordId,
    task: rec.fields?.[F.task],
    phase: rec.fields?.[F.phase],
    worker,
    approvedBy: approvedBy || null,
    validation,
    fieldMapping: {
      status: F.status,
      progress: F.progress,
      nextAction: F.nextAction,
      blocker: F.blocker,
      completedDate: F.completedDate,
    },
    before: {
      status: rec.fields?.[F.status],
      progress: rec.fields?.[F.progress],
      nextAction: rec.fields?.[F.nextAction],
      completedDate: rec.fields?.[F.completedDate],
    },
    patch,
    updated: null,
    errors: [],
  };

  console.log(formatValidationResult(validation));
  console.log("\nSanitized payload preview:");
  console.log(JSON.stringify(patch, null, 2));

  if (!validation.pass) {
    fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);
    process.exit(1);
  }

  if (EXECUTE) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    try {
      const updated = await base(MASTER_TODO_DEFAULT_TABLE_ID).update(
        [{ id: recordId, fields: patch }],
        { typecast: true }
      );
      report.updated = updated[0]?.fields
        ? {
            status: updated[0].fields[F.status],
            progress: updated[0].fields[F.progress],
            nextAction: updated[0].fields[F.nextAction],
            completedDate: updated[0].fields[F.completedDate],
          }
        : null;
      console.log("\nAirtable update: OK");
    } catch (err) {
      report.errors.push(err.message || String(err));
      console.error("\nAirtable update failed:", err.message || err);
      fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);
      process.exit(1);
    }
  } else {
    console.log("\nDry-run only. Re-run with --execute to write.");
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);
  console.log(`Report: ${REPORT_PATH}`);
}

main().catch((err) => {
  console.error("[apply-fpp-agent-task-update]", err.message || err);
  process.exit(1);
});
