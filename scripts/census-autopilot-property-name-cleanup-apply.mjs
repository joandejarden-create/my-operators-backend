/**
 * Approval-bundle-bound Property Name cleanup production apply.
 *
 *   npm run census:autopilot-property-name-cleanup-apply -- --dry-run \
 *     --run-dir reports/research-engine-v2/autopilot/2026-08-05_20-40-47-CALA-active-brands
 *
 *   ALLOW_CENSUS_AUTOPILOT_APPLY=1 \
 *   CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
 *   CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
 *   CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
 *   npm run census:autopilot-property-name-cleanup-apply -- --apply \
 *     --run-dir reports/research-engine-v2/autopilot/2026-08-05_20-40-47-CALA-active-brands \
 *     --confirm-approval-bundle-bound --confirm-property-name-only --confirm-five-records-only \
 *     --confirm-no-replan --confirm-no-brand-explorer-writes --confirm-no-brand-setup-writes \
 *     --confirm-no-owner-operator --confirm-no-date-writes --confirm-no-rooms-writes \
 *     --confirm-no-geocode-writes --confirm-no-description-writes \
 *     --confirm-write-to-production-census --confirm-safe-writes
 */

import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { existsSync, readFileSync } from "node:fs";
import "dotenv/config";
import {
  parseNameCleanupApplyArgs,
  checkNameCleanupApplyEnv,
  runPropertyNameCleanupApprovalBundleApply,
  renderNameCleanupApplyMarkdown,
  writeJson,
  writeMd,
  STATUS,
} from "../lib/research-engine-v2/census-autopilot-property-name-cleanup-apply.js";
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
const REPORT_BASE = "production-census-property-name-cleanup-apply";

function appendLearningLedger(report) {
  const today = new Date().toISOString().slice(0, 10);
  const seed = buildSeedLearningEntries();
  for (const e of seed) {
    if (e.id === "census-property-name-cleanup-queue") {
      e.status = "implemented";
      e.next_action =
        "Continue Autopilot controlled queues; keep geocode blocked until provider decision.";
      e.source_report = `reports/research-engine-v2/${REPORT_BASE}.json`;
    }
  }
  const entry = {
    id: "census-property-name-cleanup-apply",
    date: today,
    process: "census",
    batch_name: "production_census_property_name_cleanup_apply",
    source_report: `reports/research-engine-v2/${REPORT_BASE}.json`,
    issue_type: "learned_validation_rule",
    example_records: (report.write_results || []).map(
      (w) => `${w.identity_key}→${w.proposed_property_name}`
    ),
    reusable_pattern:
      "Property Name cleanup applies must be approval-bundle-bound to founder-approved identity→name map; re-read + confirm malformed before write.",
    proposed_code_change:
      "census-autopilot-property-name-cleanup-apply.js — 5 avid hotels name cleanup",
    module_to_update:
      "lib/research-engine-v2/census-autopilot-property-name-cleanup-apply.js",
    fixture_added: true,
    test_added: true,
    status: report.status === STATUS.CLEAN ? "implemented" : "proposed",
    next_action:
      report.status === STATUS.CLEAN
        ? "Continue Autopilot controlled queues; geocode still blocked."
        : "Review partial/blocked name cleanup apply.",
    lane: "property_name_cleanup",
    metrics: {
      records_updated: report.records_updated,
      rooms_remain_at_5: report.census_validation?.rooms_remain_at_5,
      status: report.status,
    },
  };
  if (!seed.some((e) => e.id === entry.id)) seed.push(entry);
  else {
    const i = seed.findIndex((e) => e.id === entry.id);
    seed[i] = { ...seed[i], ...entry };
  }
  const ledger = buildLedgerDocument(seed);
  writeJson(join(ROOT, LEARNING_PATHS.ledgerJson), ledger);
  writeMd(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger));
  writeJson(join(ROOT, LEARNING_PATHS.seedEntries), {
    version: ledger.version,
    note: "Updated after property-name-cleanup approval-bundle apply",
    entry_count: seed.length,
    entries: seed,
  });
  const audit = runBatchLearningAudit(ledger);
  writeJson(join(ROOT, LEARNING_PATHS.systemReportJson), audit);
  writeMd(join(ROOT, LEARNING_PATHS.systemReportMd), renderAuditMarkdown(audit));
  return { entry_id: entry.id, ledger_entries: seed.length, audit_status: audit.status };
}

function updateRunCheckpoint(runDir, report) {
  const cpPath = join(runDir, "checkpoint.json");
  let checkpoint = {};
  if (existsSync(cpPath)) {
    try {
      checkpoint = JSON.parse(readFileSync(cpPath, "utf8"));
    } catch {
      checkpoint = {};
    }
  }
  writeJson(cpPath, {
    ...checkpoint,
    property_name_cleanup_apply: {
      status: report.status,
      apply_executed: report.apply_executed,
      records_updated: report.records_updated,
      completed_record_ids: (report.write_results || [])
        .filter((w) => w.ok)
        .map((w) => w.record_id),
      generated_at: report.generated_at,
    },
  });
  const bundlePath = join(runDir, "approval-bundle.json");
  if (existsSync(bundlePath)) {
    const bundle = JSON.parse(readFileSync(bundlePath, "utf8"));
    writeJson(bundlePath, {
      ...bundle,
      status:
        report.status === STATUS.CLEAN
          ? "applied_property_name_cleanup"
          : report.status,
      apply_report: `reports/research-engine-v2/${REPORT_BASE}.json`,
      records_updated: report.records_updated ?? 0,
      applied_at: report.generated_at,
    });
  }
  writeJson(join(runDir, "property-name-cleanup-apply-summary.json"), report);
  writeMd(join(runDir, "property-name-cleanup-apply-summary.md"), renderNameCleanupApplyMarkdown(report));
}

function writeDurableDoc(report) {
  const doc = `# Property Name Cleanup Production Apply

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** ${report.apply_executed}  
**Bound to:** \`2026-08-05_20-40-47-CALA-active-brands\`

## Results

| Metric | Value |
| --- | ---: |
| Records updated | ${report.records_updated ?? 0} |
| Rooms filled | ${report.census_validation?.rooms_filled_before} → ${report.census_validation?.rooms_filled_after} |
| Other name changes | ${report.census_validation?.other_property_name_changes ?? "—"} |
| Brand Explorer writes | false |
| Brand Setup writes | false |

## Fields written

${(report.fields_written_union || []).map((f) => `- ${f}`).join("\n") || "- (none)"}

## Next

${report.next_step || ""}
`;
  writeMd(join(DOCS, "production-census-property-name-cleanup-apply.md"), doc);
}

async function main() {
  const args = parseNameCleanupApplyArgs();
  const env = checkNameCleanupApplyEnv();
  console.log(
    `[name-cleanup-apply] mode=${args.apply ? "apply" : "dry-run"} confirms=${args.allConfirmsOk} env=${env.allOk}`
  );

  const report = await runPropertyNameCleanupApprovalBundleApply(process.argv.slice(2));
  writeJson(join(REPORTS, `${REPORT_BASE}.json`), report);
  writeMd(join(REPORTS, `${REPORT_BASE}.md`), renderNameCleanupApplyMarkdown(report));

  if (report.run_dir) updateRunCheckpoint(report.run_dir, report);

  if (report.apply_executed && (report.status === STATUS.CLEAN || report.status === STATUS.PARTIAL)) {
    try {
      report.learning_ledger_update = appendLearningLedger(report);
      writeJson(join(REPORTS, `${REPORT_BASE}.json`), report);
    } catch (err) {
      report.learning_ledger_update = { ok: false, error: err?.message || String(err) };
      writeJson(join(REPORTS, `${REPORT_BASE}.json`), report);
    }
    writeDurableDoc(report);
  }

  console.log(
    JSON.stringify(
      {
        status: report.status,
        apply_executed: report.apply_executed,
        records_updated: report.records_updated ?? 0,
        rooms_remain_at_5: report.census_validation?.rooms_remain_at_5 ?? null,
        other_name_changes: report.census_validation?.other_property_name_changes ?? null,
        brand_explorer_writes: false,
        brand_setup_writes: false,
      },
      null,
      2
    )
  );

  if (report.status === STATUS.BLOCKED || report.status === STATUS.PARTIAL) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("[name-cleanup-apply] FAILED", err);
  process.exitCode = 1;
});
