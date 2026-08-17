/**
 * npm run census:queue-run -- --queue rooms_keys_missing --dry-run --limit 100
 * npm run census:queue-run -- --list-queues
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseQueueRunArgs,
  listCensusQueues,
  getCensusQueue,
  QUEUE_ENGINE_VERSION,
} from "../lib/research-engine-v2/production-census-queue-engine.js";
import {
  PATHS as LEARNING_PATHS,
  buildLedgerDocument,
  buildSeedLearningEntries,
  renderLedgerMarkdown,
  runBatchLearningAudit,
  renderAuditMarkdown,
} from "../lib/data-intelligence/dealality-batch-learning-system.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

function appendRoomsLearning(report) {
  const today = new Date().toISOString().slice(0, 10);
  const seed = buildSeedLearningEntries();
  const entry = {
    id: "census-rooms-keys-queue-engine",
    date: today,
    process: "census",
    batch_name: "rooms_keys_missing_queue",
    source_report: "reports/research-engine-v2/production-census-rooms-keys-queue.json",
    issue_type: "learned_code_rule",
    example_records: [
      `high=${report.summary?.high_confidence_proposals}`,
      `medium=${report.summary?.medium_confidence_candidates}`,
      `hold=${report.summary?.hold_records}`,
      "vic_ihg_22_false_positive_rejected",
    ],
    reusable_pattern:
      "Rooms/Keys is an early Census queue but High-only for writes; reject JS \\x22rooms false positives; Hold on units/residences/pipeline; hotel+residences split writes hotel count only.",
    proposed_code_change:
      "census:queue-run + rooms_keys_missing queue + extractor fixtures; v1.1.4 provenance plan for Source Type/Notes/Hold.",
    module_to_update: "lib/research-engine-v2/production-census-rooms-keys-queue.js",
    fixture_added: true,
    test_added: true,
    status: "proposed",
    next_action: report.next_step,
    lane: "rooms_keys_extraction",
  };
  const i = seed.findIndex((e) => e.id === entry.id);
  if (i >= 0) seed[i] = { ...seed[i], ...entry };
  else seed.push(entry);
  const ledger = buildLedgerDocument(seed);
  writeJson(join(ROOT, LEARNING_PATHS.ledgerJson), ledger);
  writeMd(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger));
  const audit = runBatchLearningAudit(ledger);
  writeJson(join(ROOT, LEARNING_PATHS.systemReportJson), audit);
  writeMd(join(ROOT, LEARNING_PATHS.systemReportMd), renderAuditMarkdown(audit));
  return { entry_id: entry.id, ledger_entries: seed.length, audit_status: audit.status };
}

function writeV114Plan(schema) {
  const plan = {
    version: "production-census-schema-v114-rooms-keys-provenance-plan-v1",
    generated_at: new Date().toISOString(),
    status: "plan_only_no_schema_create",
    current_field_count: schema.field_count_live,
    existing_rooms_fields: schema.existing_field_names,
    proposed_fields: schema.planned_v114,
    missing: schema.missing_planned_fields,
    note: schema.note,
    create_command_later:
      "npm run research-engine-v2:production-census-schema-v114-rooms-keys-provenance -- --dry-run (not shipped until founder approves schema create)",
  };
  writeJson(join(REPORTS, "production-census-schema-v114-rooms-keys-provenance-plan.json"), plan);
  writeMd(
    join(REPORTS, "production-census-schema-v114-rooms-keys-provenance-plan.md"),
    `# Production Census Schema v1.1.4 — Rooms / Keys Provenance Plan

**Status:** plan_only_no_schema_create  
**Generated:** ${plan.generated_at}

## Existing (live)

${(schema.existing_field_names || []).map((f) => `- ${f}`).join("\n")}

## Proposed adds / alignments

\`\`\`json
${JSON.stringify(schema.planned_v114, null, 2)}
\`\`\`

## Missing now

${(schema.missing_planned_fields || []).map((f) => `- ${f}`).join("\n")}

## Notes

${schema.note}

Do **not** create fields until founder explicitly approves schema create.
`
  );
  return plan;
}

async function main() {
  const args = parseQueueRunArgs();
  console.log(`[census-queue] engine=${QUEUE_ENGINE_VERSION}`);

  if (args.listQueues) {
    console.log(JSON.stringify({ queues: listCensusQueues() }, null, 2));
    return;
  }

  if (!args.queue) {
    console.error("Usage: npm run census:queue-run -- --queue rooms_keys_missing --dry-run --limit 100");
    console.error("       npm run census:queue-run -- --list-queues");
    process.exitCode = 1;
    return;
  }

  const queue = getCensusQueue(args.queue);

  if (args.apply) {
    console.log(
      JSON.stringify(
        {
          status: "apply_not_executed",
          reason:
            "Rooms/Keys apply requires separate founder approval in this task. Re-run with --dry-run only.",
          required_confirms: args.confirms,
          command: queue.id === "rooms_keys_missing" ? undefined : null,
        },
        null,
        2
      )
    );
    process.exitCode = 1;
    return;
  }

  console.log(`[census-queue] dry-run queue=${args.queue} limit=${args.limit}`);
  const report = await queue.runDryRun({ limit: args.limit });
  report.learning_ledger_update = appendRoomsLearning(report);

  if (args.queue === "rooms_keys_missing") {
    writeJson(join(REPORTS, "production-census-rooms-keys-queue.json"), report);
    writeMd(join(REPORTS, "production-census-rooms-keys-queue.md"), queue.renderMarkdown(report));
    writeMd(join(DOCS, "production-census-rooms-keys-queue.md"), queue.renderMarkdown(report));
    if (report.schema?.needs_v114_schema) {
      report.v114_plan = writeV114Plan(report.schema);
      writeJson(join(REPORTS, "production-census-rooms-keys-queue.json"), report);
    }
  } else {
    writeJson(join(REPORTS, `production-census-queue-${args.queue}-dry-run.json`), report);
  }

  console.log(
    JSON.stringify(
      {
        status: report.status,
        queue: args.queue,
        eligible: report.summary?.records_eligible,
        high: report.summary?.high_confidence_proposals,
        medium: report.summary?.medium_confidence_candidates,
        hold: report.summary?.hold_records,
        updates_if_applied: report.summary?.exact_airtable_update_count_if_applied,
        needs_v114: report.schema?.needs_v114_schema,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("[census-queue] fatal:", err);
  process.exitCode = 1;
});
