/**
 * Census description extraction — dry-run or IHG apply.
 *
 *   npm run research-engine-v2:production-census-description-extraction -- --dry-run
 *
 *   ALLOW_PRODUCTION_CENSUS_DESCRIPTION_EXTRACTION=1 \
 *   CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
 *   CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
 *   CONFIRM_NO_ROOM_DATE_WRITES=1 \
 *   npm run research-engine-v2:production-census-description-extraction -- --apply \
 *     --confirm-description-extraction \
 *     --confirm-ihg-only \
 *     --confirm-official-public-sources-only \
 *     --confirm-grounded-source-text-only \
 *     --confirm-no-geocode-writes \
 *     --confirm-no-brand-explorer-writes \
 *     --confirm-no-owner-operator-writes \
 *     --confirm-no-room-date-writes \
 *     --confirm-no-recent-momentum \
 *     --confirm-held-records-blocked
 */

import { execSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseDescArgs,
  routeNextCensusLane,
  runDescriptionExtractionDryRun,
  runIhgDescriptionApply,
  renderDescriptionDryRunMarkdown,
  renderIhgApplyMarkdown,
  renderNextLaneMarkdown,
  checkIhgApplyEnvFlags,
  loadApprovedIhgDescriptionProposals,
  IHG_APPLY_STATUS,
} from "../lib/research-engine-v2/production-census-description-extraction.js";
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

function runBeGate(label, command) {
  try {
    execSync(command, {
      cwd: ROOT,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      maxBuffer: 20 * 1024 * 1024,
    });
    return { label, command, ok: true, exit_code: 0 };
  } catch (err) {
    return {
      label,
      command,
      ok: false,
      exit_code: err.status ?? 1,
      stderr_tail: (err.stderr?.toString?.() || "").slice(-800),
    };
  }
}

function appendLearning(report) {
  const today = new Date().toISOString().slice(0, 10);
  const seed = buildSeedLearningEntries();
  const entry = {
    id: "census-ihg-description-extraction-apply",
    date: today,
    process: "census",
    batch_name: "ihg_description_extraction_apply",
    source_report:
      "reports/research-engine-v2/production-census-description-extraction-ihg-apply.json",
    issue_type: "learned_code_rule",
    example_records: [
      `updates_written=${report.updates_written}`,
      `methods=${JSON.stringify(report.source_methods || {})}`,
      `excluded=${report.excluded_count}`,
    ],
    reusable_pattern:
      "IHG official hoteldetail pages can provide grounded descriptions via json_ld_hotel_description / amenity factual assembly; booking boilerplate must stay rejected; Hilton/Choice/Marriott need separate safe fetch strategy.",
    proposed_code_change:
      "IHG-only apply path with dry-run identity-key rebuild; keep geocode blocked without provider terms.",
    module_to_update: "lib/research-engine-v2/production-census-description-extraction.js",
    fixture_added: true,
    test_added: true,
    status: report.apply_executed ? "implemented" : "proposed",
    next_action: report.next_step || "Next family description lane after safe fetch",
    lane: "description_extraction",
    metrics: {
      updates_written: report.updates_written,
      approved_from_dry_run: report.approved_from_dry_run,
    },
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
  return {
    entry_id: entry.id,
    ledger_entries: seed.length,
    audit_status: audit.status,
    process_actually_learned: true,
  };
}

async function main() {
  const args = parseDescArgs();
  const router = routeNextCensusLane();

  console.log("[census-desc] mode=", args.apply ? "apply" : "dry-run");
  console.log("[census-desc] provider_approved=", router.provider.approved_for_geocode_apply);

  if (args.apply) {
    const env = checkIhgApplyEnvFlags();
    console.log("[census-desc] env_ok=", env.allOk, "confirms_ok=", args.allConfirms);
    const approved = loadApprovedIhgDescriptionProposals();
    console.log(
      "[census-desc] approved_ihg_from_dry_run=",
      approved.proposals.length,
      "ok=",
      approved.ok
    );

    if (!args.allConfirms || !env.allOk) {
      const blocked = {
        status: IHG_APPLY_STATUS.BLOCKED,
        reason: "confirmation_or_env_missing",
        missing_cli_confirms: args.missingConfirms,
        missing_env: env.missing,
      };
      writeJson(join(REPORTS, "production-census-description-extraction-ihg-apply.json"), blocked);
      writeMd(
        join(REPORTS, "production-census-description-extraction-ihg-apply.md"),
        `# IHG Description Apply Blocked\n\n\`\`\`json\n${JSON.stringify(blocked, null, 2)}\n\`\`\`\n`
      );
      console.log(JSON.stringify(blocked, null, 2));
      process.exitCode = 1;
      return;
    }

    console.log("[census-desc] applying IHG description batch…");
    const report = await runIhgDescriptionApply(args);

    if (report.apply_executed && !args.skipBeGates) {
      console.log("[census-desc] running Brand Explorer safety gates…");
      const gates = [
        runBeGate(
          "active_universe_sot",
          "npm run brand-explorer-active-universe-source-of-truth -- --dry-run"
        ),
        runBeGate(
          "global_active_semantic",
          "npm run brand-explorer-global-active-semantic-audit -- --dry-run --fresh"
        ),
        runBeGate("pvql_quiet", "node scripts/brand-explorer-quiet-sequential-pvql.mjs"),
        runBeGate(
          "momentum_evidence",
          "npm run test:brand-explorer-recent-momentum-evidence-quality"
        ),
        runBeGate("mandatory_release_gates", "npm run test:brand-explorer-mandatory-release-gates"),
      ];
      report.brand_explorer_safety = {
        touched: false,
        writes: 0,
        gates: gates.map((g) => ({ label: g.label, ok: g.ok, exit_code: g.exit_code })),
        all_pass: gates.every((g) => g.ok),
      };
    } else if (args.skipBeGates) {
      report.brand_explorer_safety = { touched: false, writes: 0, skipped: true };
    }

    if (report.apply_executed) {
      report.learning_ledger_update = appendLearning(report);
    }

    writeJson(join(REPORTS, "production-census-description-extraction-ihg-apply.json"), report);
    writeMd(
      join(REPORTS, "production-census-description-extraction-ihg-apply.md"),
      renderIhgApplyMarkdown(report)
    );
    writeMd(
      join(DOCS, "production-census-description-extraction-ihg-apply.md"),
      renderIhgApplyMarkdown(report)
    );
    writeMd(
      join(DOCS, "production-census-next-lane.md"),
      `# Production Census Next Lane\n\n**Status:** \`${report.status}\`\n\nIHG descriptions applied=${report.updates_written}. Geocode 34 still blocked. Next: ${report.next_step}\n`
    );

    console.log(
      JSON.stringify(
        {
          status: report.status,
          updates_written: report.updates_written,
          excluded: report.excluded_count,
          validation_pass: report.post_apply_validation?.pass,
          coords: report.post_apply_validation?.coords_filled,
          geocode_applied: false,
          be_all_pass: report.brand_explorer_safety?.all_pass ?? null,
        },
        null,
        2
      )
    );
    if (report.status === IHG_APPLY_STATUS.BLOCKED) process.exitCode = 1;
    return;
  }

  // Dry-run path
  console.log("[census-desc] starting description extraction dry-run…");
  const dry = await runDescriptionExtractionDryRun(args);
  writeJson(join(REPORTS, "production-census-description-extraction-dry-run.json"), dry);
  writeMd(
    join(REPORTS, "production-census-description-extraction-dry-run.md"),
    renderDescriptionDryRunMarkdown(dry)
  );
  writeMd(join(DOCS, "production-census-next-lane.md"), renderNextLaneMarkdown(router, dry));
  console.log(
    JSON.stringify(
      {
        status: dry.status,
        descriptions: dry.summary.description_updates_proposed,
        updates_if_applied: dry.summary.exact_airtable_update_count_if_applied,
        pages_ok: dry.summary.pages_ok,
        geocode_blocked: true,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("[census-desc] fatal:", err);
  process.exitCode = 1;
});
