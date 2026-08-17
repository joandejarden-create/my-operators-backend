/**
 * Production Census Population Lane 2 — provenance backfill + safe enrichment.
 *
 *   npm run research-engine-v2:production-census-population-lane-2 -- --dry-run
 *
 *   ALLOW_PRODUCTION_CENSUS_POPULATION_LANE_2=1 \
 *   CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
 *   CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
 *   CONFIRM_NO_ROOM_DATE_WRITES=1 \
 *   npm run research-engine-v2:production-census-population-lane-2 -- --apply \
 *     --confirm-census-population-lane-2 \
 *     --confirm-official-public-sources-only \
 *     --confirm-no-brand-explorer-writes \
 *     --confirm-no-owner-operator-writes \
 *     --confirm-no-room-date-writes \
 *     --confirm-no-recent-momentum \
 *     --confirm-held-records-blocked \
 *     --confirm-no-fake-completeness
 */

import { execSync } from "node:child_process";
import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseLane2Args,
  checkLane2EnvFlags,
  runLane2DryRun,
  runLane2Apply,
  renderLane2DryRunMarkdown,
  renderLane2ApplyMarkdown,
  STATUS,
  providerDecisionStatus,
} from "../lib/research-engine-v2/production-census-population-lane-2.js";
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

function loadBeSummary(gates) {
  return {
    touched: false,
    writes: 0,
    gates: gates.map((g) => ({ label: g.label, ok: g.ok, exit_code: g.exit_code })),
    all_pass: gates.every((g) => g.ok),
  };
}

function appendLearning(report) {
  const today = new Date().toISOString().slice(0, 10);
  const seed = buildSeedLearningEntries();
  const entry = {
    id: "census-population-lane-2-provenance-enrichment",
    date: today,
    process: "census",
    batch_name: "production_census_population_lane_2",
    source_report:
      report.mode === "apply"
        ? "reports/research-engine-v2/production-census-population-lane-2-apply.json"
        : "reports/research-engine-v2/production-census-population-lane-2-dry-run.json",
    issue_type: "learned_code_rule",
    example_records: [
      `provenance_backfills=${report.summary?.provenance_backfills_proposed ?? report.summary_from_dry_run?.provenance_backfills_proposed}`,
      `geocode_blocked=${report.geocode_lane?.ready_but_blocked ?? report.geocode_lane?.blocked}`,
    ],
    reusable_pattern:
      "Backfill coordinate provenance from first-pass evidence without changing lat/lng; keep geocode apply behind provider/storage terms; enrich only VIC-grounded gaps.",
    proposed_code_change:
      "production-census-population-lane-2 module + npm command; Mapbox Permanent or Google terms before geocode writes.",
    module_to_update: "lib/research-engine-v2/production-census-population-lane-2.js",
    fixture_added: false,
    test_added: false,
    status: report.apply_executed ? "implemented" : "proposed",
    next_action: report.next_step || "Founder review / provider decision",
    lane: "geocoding_fallback",
  };
  if (!seed.some((e) => e.id === entry.id)) seed.push(entry);
  else {
    const i = seed.findIndex((e) => e.id === entry.id);
    seed[i] = { ...seed[i], ...entry };
  }
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
    process_actually_learned: audit.process_actually_learned,
  };
}

function writeDurableDoc(report) {
  const s = report.summary || report.summary_from_dry_run || {};
  const doc = `# Production Census Population Lane 2

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** ${report.apply_executed}

## Executive summary

Lane 2 backfills coordinate provenance for first-pass pins and fills safe VIC-grounded enrichment gaps. Geocode proposals stay blocked until provider/storage decision.

| Metric | Value |
| --- | ---: |
| Provenance backfills | ${s.provenance_backfills_proposed ?? report.post_apply_validation?.provenance_populated ?? "—"} |
| Descriptions | ${s.description_updates_proposed ?? report.post_apply_validation?.description_filled ?? "—"} |
| Amenities | ${s.amenity_updates_proposed ?? report.post_apply_validation?.amenities_filled ?? "—"} |
| Geocode proposals | ${report.geocode_lane?.count ?? 34} blocked=${report.geocode_lane?.ready_but_blocked ?? report.geocode_lane?.blocked} |
| Updates written | ${report.updates_written ?? 0} |

## Provider decision

\`\`\`json
${JSON.stringify(providerDecisionStatus(), null, 2)}
\`\`\`

## Commands

\`\`\`bash
npm run research-engine-v2:production-census-population-lane-2 -- --dry-run
\`\`\`

## Next

${report.next_step || ""}
`;
  writeMd(join(DOCS, "production-census-population-lane-2.md"), doc);
}

async function main() {
  const args = parseLane2Args();
  const env = checkLane2EnvFlags();
  mkdirSync(REPORTS, { recursive: true });
  mkdirSync(DOCS, { recursive: true });

  console.log(`[census-lane2] mode=${args.apply ? "apply" : "dry-run"} env_ok=${env.allOk}`);
  console.log(
    `[census-lane2] provider_approved=${providerDecisionStatus().approved_for_coordinate_apply}`
  );

  if (!args.apply) {
    const dry = await runLane2DryRun();
    dry.learning_ledger_update = appendLearning(dry);
    writeJson(join(REPORTS, "production-census-population-lane-2-dry-run.json"), dry);
    writeMd(join(REPORTS, "production-census-population-lane-2-dry-run.md"), renderLane2DryRunMarkdown(dry));
    writeDurableDoc(dry);
    console.log(
      JSON.stringify(
        {
          status: dry.status,
          provenance: dry.summary.provenance_backfills_proposed,
          unclear: dry.summary.provenance_left_blank_unclear,
          amenities: dry.summary.amenity_updates_proposed,
          descriptions: dry.summary.description_updates_proposed,
          property_type: dry.summary.property_type_updates_proposed,
          updates_if_applied: dry.summary.exact_airtable_update_count_if_applied,
          geocode_blocked: dry.geocode_lane.ready_but_blocked,
        },
        null,
        2
      )
    );
    return;
  }

  const dry = await runLane2DryRun();
  writeJson(join(REPORTS, "production-census-population-lane-2-dry-run.json"), dry);
  writeMd(join(REPORTS, "production-census-population-lane-2-dry-run.md"), renderLane2DryRunMarkdown(dry));

  const report = await runLane2Apply(dry);

  if (report.apply_executed && report.status === STATUS.APPLIED && !args.skipBeGates) {
    console.log("[census-lane2] running Brand Explorer safety gates…");
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
    report.brand_explorer_safety = loadBeSummary(gates);
  } else if (args.skipBeGates) {
    report.brand_explorer_safety = { touched: false, writes: 0, skipped: true };
  }

  if (report.apply_executed) {
    report.learning_ledger_update = appendLearning(report);
  }

  writeJson(join(REPORTS, "production-census-population-lane-2-apply.json"), report);
  writeMd(join(REPORTS, "production-census-population-lane-2-apply.md"), renderLane2ApplyMarkdown(report));
  writeDurableDoc(report);

  console.log(
    JSON.stringify(
      {
        status: report.status,
        updates_written: report.updates_written,
        validation_pass: report.post_apply_validation?.pass,
        provenance_populated: report.post_apply_validation?.provenance_populated,
        geocode_applied: report.geocode_lane?.applied,
        be_all_pass: report.brand_explorer_safety?.all_pass ?? null,
      },
      null,
      2
    )
  );

  if (
    report.status === STATUS.BLOCKED_SOURCE ||
    report.status === STATUS.CONFIRMATION_MISSING ||
    report.brand_explorer_safety?.all_pass === false
  ) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("[census-lane2] FAILED", err);
  process.exitCode = 1;
});
