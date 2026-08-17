/**
 * Production Census schema v1.1.4 — Rooms / Keys provenance fields.
 *
 *   npm run research-engine-v2:production-census-schema-v114-rooms-keys-provenance -- --dry-run
 *
 *   ALLOW_PRODUCTION_CENSUS_SCHEMA_V114=1 \
 *   CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES=1 \
 *   CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
 *   npm run research-engine-v2:production-census-schema-v114-rooms-keys-provenance -- --apply \
 *     --confirm-add-rooms-keys-provenance-fields-only \
 *     --confirm-no-record-writes \
 *     --confirm-no-brand-explorer-writes \
 *     --confirm-no-brand-setup-writes \
 *     --confirm-no-field-deletes \
 *     --confirm-no-field-renames \
 *     --confirm-no-field-population \
 *     --confirm-add-hold-to-rooms-confidence
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseV114Args,
  checkV114EnvFlags,
  runV114DryRun,
  runV114Apply,
  renderV114DryRunMarkdown,
  renderV114ApplyMarkdown,
  STATUS,
  V114_NEW_FIELD_NAMES,
  ROOMS_CONFIDENCE_HOLD,
} from "../lib/research-engine-v2/production-census-schema-v114-rooms-keys-provenance.js";
import { MAP_ROOMS } from "../lib/research-engine-v2/production-census-rooms-keys-queue.js";
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

const REPORT_BASENAME = "production-census-schema-v114-rooms-keys-provenance-apply";

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

function appendLearningLedgerEntry(report) {
  const today = new Date().toISOString().slice(0, 10);
  const seed = buildSeedLearningEntries();

  for (const e of seed) {
    if (e.id === "census-rooms-keys-queue-engine") {
      e.status = "implemented";
      e.fixture_added = true;
      e.test_added = true;
      e.next_action =
        "Run Autopilot controlled fastest-safe with schema v1.1.4; High-only rooms writes with provenance.";
      e.source_report = `reports/research-engine-v2/${REPORT_BASENAME}.json`;
      e.proposed_code_change =
        "Schema v1.1.4 Rooms Source Type / Reviewed Date / Notes + Hold on Rooms Confidence applied; Autopilot rooms queue unblocked.";
    }
  }

  const newEntry = {
    id: "census-schema-v114-rooms-keys-provenance-applied",
    date: today,
    process: "census",
    batch_name: "production_census_schema_v114_rooms_keys_provenance",
    source_report: `reports/research-engine-v2/${REPORT_BASENAME}.json`,
    issue_type: "learned_validation_rule",
    example_records: [
      ...V114_NEW_FIELD_NAMES,
      `${MAP_ROOMS.confidenceExisting}+${ROOMS_CONFIDENCE_HOLD}`,
    ],
    reusable_pattern:
      "Rooms / Keys writes require provenance (Source Type, Reviewed Date, Notes) and Hold on confidence before Autopilot apply.",
    proposed_code_change:
      "Schema v1.1.4 fields added on Hotel Property Census; leave blank until approved rooms Autopilot apply.",
    module_to_update:
      "lib/research-engine-v2/production-census-schema-v114-rooms-keys-provenance.js",
    fixture_added: true,
    test_added: true,
    status: "implemented",
    next_action:
      "Controlled Autopilot: --scope active-brand-setup --mode controlled --strategy fastest-safe (no production writes until founder confirm).",
    lane: "rooms_keys_extraction",
    metrics: {
      fields_created: (report.fields_created || []).length,
      hold_applied: Boolean(report.rooms_confidence_hold?.applied || report.rooms_confidence_hold?.already_had_hold),
      field_count_after: report.field_count_after,
      status: report.status,
    },
  };

  if (!seed.some((e) => e.id === newEntry.id)) seed.push(newEntry);

  const ledger = buildLedgerDocument(seed);
  writeJson(join(ROOT, LEARNING_PATHS.ledgerJson), ledger);
  writeMd(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger));
  writeJson(join(ROOT, LEARNING_PATHS.seedEntries), {
    version: ledger.version,
    note: "Updated after production-census-schema-v114 rooms-keys provenance apply",
    entry_count: seed.length,
    entries: seed,
  });

  const audit = runBatchLearningAudit(ledger);
  writeJson(join(ROOT, LEARNING_PATHS.systemReportJson), audit);
  writeMd(join(ROOT, LEARNING_PATHS.systemReportMd), renderAuditMarkdown(audit));

  return {
    entry_id: newEntry.id,
    ledger_entries: seed.length,
    audit_status: audit.status,
    process_actually_learned: audit.process_actually_learned,
  };
}

function writeDurableDoc(report) {
  const created = (report.fields_created || [])
    .map((f) => `- **${f.name}** (\`${f.type}\`${f.id ? ` · \`${f.id}\`` : ""})`)
    .join("\n");
  const existing = (report.fields_already_existing || [])
    .map((f) => (typeof f === "string" ? f : f.name))
    .map((n) => `- ${n}`)
    .join("\n");
  const doc = `# Production Census Schema v1.1.4 — Rooms / Keys Provenance Apply

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** ${report.apply_executed}  
**Base:** Deal Capture Platform (\`${report.base_id_masked}\`)  
**Table:** Hotel Property Census (\`${report.table_id}\`)

## 1. Executive summary

Schema-only: add Rooms / Keys provenance fields + Hold on Rooms Confidence. No record population. No Brand Explorer / Brand Setup writes.

| Metric | Value |
| --- | ---: |
| Fields created | ${(report.fields_created || []).length} |
| Fields already existing | ${(report.fields_already_existing || []).length} |
| Hold on Rooms Confidence | ${report.rooms_confidence_hold?.applied || report.rooms_confidence_hold?.already_had_hold || report.rooms_confidence_hold?.skipped || "—"} |
| Field count | ${report.field_count_before} → ${report.field_count_after} |
| Census records | ${report.validation?.record_count ?? "—"} |
| Existing cell value drift | ${report.existing_cell_value_drift_count ?? "—"} |
| Validation pass | ${report.validation_pass} |

## 2. Fields created

${created || "- (none)"}

## 3. Fields already existing (skipped)

${existing || "- (none)"}

## 4. Rooms Confidence — Hold option

\`\`\`json
${JSON.stringify(report.rooms_confidence_hold || {}, null, 2)}
\`\`\`

## 5. Final Census field count

**${report.field_count_after}**

## 6. Census validation

- Records: ${report.validation?.record_count} (expected 666)
- Rooms / Keys field intact: ${report.validation?.rooms_keys_field_exists}
- Rooms Confidence intact: ${report.validation?.rooms_confidence_field_exists}
- Rooms Source URL intact: ${report.validation?.rooms_source_url_field_exists}
- Hold present: ${report.validation?.rooms_confidence?.has_hold}
- New provenance populated: ${report.validation?.new_provenance_any_populated}
- Owner / operator filled: ${report.validation?.owner_filled} / ${report.validation?.operator_filled}

## 7. Rename recommendation (not applied)

Optional later rename \`Rooms Confidence\` / \`Rooms Source URL\` → \`Rooms / Keys*\` for naming parity — deferred; not applied in this task.

## 8. Learning ledger

\`\`\`json
${JSON.stringify(report.learning_ledger_update || {}, null, 2)}
\`\`\`

## 9. Recommended next step

${report.next_recommended_step || ""}

## Apply command (reference)

\`\`\`bash
ALLOW_PRODUCTION_CENSUS_SCHEMA_V114=1 \\
CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES=1 \\
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \\
npm run research-engine-v2:production-census-schema-v114-rooms-keys-provenance -- --apply \\
  --confirm-add-rooms-keys-provenance-fields-only \\
  --confirm-no-record-writes \\
  --confirm-no-brand-explorer-writes \\
  --confirm-no-brand-setup-writes \\
  --confirm-no-field-deletes \\
  --confirm-no-field-renames \\
  --confirm-no-field-population \\
  --confirm-add-hold-to-rooms-confidence
\`\`\`
`;
  writeMd(join(DOCS, "production-census-schema-v114-rooms-keys-provenance-apply.md"), doc);
}

async function main() {
  const args = parseV114Args();
  const env = checkV114EnvFlags();
  mkdirSync(REPORTS, { recursive: true });
  mkdirSync(DOCS, { recursive: true });

  console.log(`[census-v114] mode=${args.apply ? "apply" : "dry-run"} env_ok=${env.allOk}`);

  if (!args.apply) {
    const dry = await runV114DryRun();
    writeJson(join(REPORTS, `${REPORT_BASENAME}-dry-run.json`), dry);
    writeMd(join(REPORTS, `${REPORT_BASENAME}-dry-run.md`), renderV114DryRunMarkdown(dry));
    writeJson(join(REPORTS, `${REPORT_BASENAME}.json`), dry);
    writeMd(join(REPORTS, `${REPORT_BASENAME}.md`), renderV114DryRunMarkdown(dry));
    console.log(
      JSON.stringify(
        {
          status: dry.status,
          dry_run_pass: dry.dry_run_pass,
          to_add: dry.fields_to_add?.length,
          existed: dry.fields_already_existed?.length,
          field_count_before: dry.field_count_before,
          expected_after: dry.expected_field_count_after,
          has_hold: dry.rooms_confidence_option_add?.already_has_hold,
          census: dry.census_record_count,
        },
        null,
        2
      )
    );
    if (!dry.dry_run_pass) process.exitCode = 1;
    return;
  }

  const report = await runV114Apply(process.argv.slice(2));
  report.brand_explorer_writes = false;
  report.brand_setup_writes = false;

  if (report.apply_executed && report.status === STATUS.APPLIED) {
    try {
      report.learning_ledger_update = appendLearningLedgerEntry(report);
      console.log("[census-v114] learning ledger updated + audit run");
    } catch (err) {
      report.learning_ledger_update = {
        ok: false,
        error: err?.message || String(err),
      };
    }
  }

  writeJson(join(REPORTS, `${REPORT_BASENAME}.json`), report);
  writeMd(join(REPORTS, `${REPORT_BASENAME}.md`), renderV114ApplyMarkdown(report));
  writeDurableDoc(report);

  console.log(
    JSON.stringify(
      {
        status: report.status,
        fields_created: report.fields_created?.length ?? 0,
        hold: report.rooms_confidence_hold,
        field_count_after: report.field_count_after,
        validation_pass: report.validation_pass,
        census_records: report.validation?.record_count,
        learning: report.learning_ledger_update?.entry_id ?? null,
      },
      null,
      2
    )
  );

  if (
    report.status === STATUS.BLOCKED ||
    report.status === STATUS.CONFIRMATION_MISSING ||
    report.status === STATUS.PARTIAL
  ) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("[census-v114] FAILED", err);
  process.exitCode = 1;
});
