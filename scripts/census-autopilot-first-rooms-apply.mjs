/**
 * First live Autopilot Rooms / Keys apply — approval-bundle-bound.
 *
 * Dry-run:
 *   npm run census:autopilot-first-rooms-apply -- --dry-run \
 *     --approval-bundle reports/research-engine-v2/autopilot/2026-08-05_20-24-38-CALA-active-brands/approval-bundle.json
 *
 * Apply:
 *   ALLOW_CENSUS_AUTOPILOT_APPLY=1 \
 *   CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
 *   CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
 *   CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
 *   npm run census:autopilot-first-rooms-apply -- --apply \
 *     --approval-bundle reports/research-engine-v2/autopilot/2026-08-05_20-24-38-CALA-active-brands/approval-bundle.json \
 *     --confirm-approval-bundle-bound --confirm-rooms-keys-only --confirm-five-records-only \
 *     --confirm-no-replan --confirm-no-brand-explorer-writes --confirm-no-brand-setup-writes \
 *     --confirm-no-owner-operator --confirm-no-date-writes --confirm-no-geocode-writes \
 *     --confirm-no-description-writes --confirm-write-to-production-census --confirm-safe-writes
 */

import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseFirstRoomsApplyArgs,
  checkFirstRoomsApplyEnv,
  runFirstRoomsApprovalBundleApply,
  renderFirstRoomsApplyMarkdown,
  writeJson,
  writeMd,
  STATUS,
  loadApprovalBundleProposals,
} from "../lib/research-engine-v2/census-autopilot-approval-bundle-apply.js";
import {
  PATHS as LEARNING_PATHS,
  buildLedgerDocument,
  buildSeedLearningEntries,
  renderLedgerMarkdown,
  runBatchLearningAudit,
  renderAuditMarkdown,
} from "../lib/data-intelligence/dealality-batch-learning-system.js";
import { writeFileSync, mkdirSync, readFileSync, existsSync } from "node:fs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");
const REPORT_BASE = "production-census-autopilot-first-rooms-apply";

function appendLearningLedger(report) {
  const today = new Date().toISOString().slice(0, 10);
  const seed = buildSeedLearningEntries();
  const entry = {
    id: "census-autopilot-first-rooms-keys-apply",
    date: today,
    process: "census",
    batch_name: "production_census_autopilot_first_rooms_apply",
    source_report: `reports/research-engine-v2/${REPORT_BASE}.json`,
    issue_type: "learned_validation_rule",
    example_records: (report.write_results || []).map(
      (w) => `${w.identity_key}:${w.rooms}`
    ),
    reusable_pattern:
      "First live Autopilot writes must be approval-bundle-bound: freeze dry-run High patches, re-read, idempotent blank write, no re-plan.",
    proposed_code_change:
      "census-autopilot-approval-bundle-apply.js — rooms-only first apply from controlled run dry-run.json",
    module_to_update:
      "lib/research-engine-v2/census-autopilot-approval-bundle-apply.js",
    fixture_added: true,
    test_added: false,
    status: report.status === STATUS.CLEAN ? "implemented" : "proposed",
    next_action:
      report.status === STATUS.CLEAN
        ? "Continue controlled Autopilot queues; keep geocode blocked until provider decision."
        : "Review partial/blocked apply before further production writes.",
    lane: "rooms_keys_extraction",
    metrics: {
      records_updated: report.records_updated,
      rooms_delta: report.census_validation?.rooms_delta,
      geocode_still_blocked: report.geocode_gate?.remain_blocked,
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
    note: "Updated after first Autopilot rooms approval-bundle apply",
    entry_count: seed.length,
    entries: seed,
  });
  const audit = runBatchLearningAudit(ledger);
  writeJson(join(ROOT, LEARNING_PATHS.systemReportJson), audit);
  writeMd(join(ROOT, LEARNING_PATHS.systemReportMd), renderAuditMarkdown(audit));
  return {
    entry_id: entry.id,
    ledger_entries: seed.length,
    audit_status: audit.status,
  };
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
  const next = {
    ...checkpoint,
    first_rooms_apply: {
      status: report.status,
      apply_executed: report.apply_executed,
      records_updated: report.records_updated,
      completed_record_ids: (report.write_results || [])
        .filter((w) => w.ok)
        .map((w) => w.record_id),
      generated_at: report.generated_at,
    },
    approval_bundle_status:
      report.status === STATUS.CLEAN ? "applied" : report.status,
  };
  writeJson(cpPath, next);

  const bundlePath = join(runDir, "approval-bundle.json");
  if (existsSync(bundlePath)) {
    const bundle = JSON.parse(readFileSync(bundlePath, "utf8"));
    writeJson(bundlePath, {
      ...bundle,
      status:
        report.status === STATUS.CLEAN
          ? "applied_first_rooms_keys"
          : report.status,
      apply_report: `reports/research-engine-v2/${REPORT_BASE}.json`,
      records_updated: report.records_updated ?? 0,
      applied_at: report.generated_at,
    });
  }

  writeJson(join(runDir, "first-rooms-apply-summary.json"), report);
  writeMd(join(runDir, "first-rooms-apply-summary.md"), renderFirstRoomsApplyMarkdown(report));
}

function writeDurableDoc(report) {
  const doc = `# First Autopilot Rooms / Keys Production Apply

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** ${report.apply_executed}  
**Bound to:** \`2026-08-05_20-24-38-CALA-active-brands\` approval bundle / dry-run

## Contract

- Exactly **5** High Rooms / Keys proposals from the controlled dry-run
- No re-plan, no descriptions, no coordinates, no amenities, no property type, no name cleanup
- No Brand Explorer / Brand Setup writes
- No owner/operator/developer/date writes
- Rooms Notes only if present in frozen patch (not present → not written)

## Results

| Metric | Value |
| --- | ---: |
| Records updated | ${report.records_updated ?? 0} |
| Records failed | ${report.records_failed ?? 0} |
| Rooms filled before → after | ${report.census_validation?.rooms_filled_before} → ${report.census_validation?.rooms_filled_after} |
| Census record count | ${report.census_validation?.record_count_after} |
| Geocode 34 still blocked | ${report.geocode_gate?.remain_blocked} |

## Fields written

${(report.fields_written_union || []).map((f) => `- ${f}`).join("\n") || "- (none)"}

## Next

${report.next_step || ""}
`;
  writeMd(join(DOCS, "production-census-autopilot-first-rooms-apply.md"), doc);
}

async function main() {
  const args = parseFirstRoomsApplyArgs();
  const env = checkFirstRoomsApplyEnv();
  console.log(
    `[first-rooms-apply] mode=${args.apply ? "apply" : "dry-run"} confirms=${args.allConfirmsOk} env=${env.allOk}`
  );

  const report = await runFirstRoomsApprovalBundleApply(process.argv.slice(2));

  writeJson(join(REPORTS, `${REPORT_BASE}.json`), report);
  writeMd(join(REPORTS, `${REPORT_BASE}.md`), renderFirstRoomsApplyMarkdown(report));

  if (report.run_dir) {
    updateRunCheckpoint(report.run_dir, report);
  }

  if (report.apply_executed && (report.status === STATUS.CLEAN || report.status === STATUS.PARTIAL)) {
    try {
      report.learning_ledger_update = appendLearningLedger(report);
      writeJson(join(REPORTS, `${REPORT_BASE}.json`), report);
    } catch (err) {
      report.learning_ledger_update = { ok: false, error: err?.message || String(err) };
      writeJson(join(REPORTS, `${REPORT_BASE}.json`), report);
    }
    writeDurableDoc(report);
  } else if (!args.apply) {
    writeDurableDoc(report);
  }

  console.log(
    JSON.stringify(
      {
        status: report.status,
        apply_executed: report.apply_executed,
        records_updated: report.records_updated ?? 0,
        rooms_delta: report.census_validation?.rooms_delta ?? null,
        geocode_blocked: report.geocode_gate?.remain_blocked ?? null,
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
  console.error("[first-rooms-apply] FAILED", err);
  process.exitCode = 1;
});
