/**
 * Production Census schema v1.1.3 — address + coordinate provenance fields.
 *
 *   npm run research-engine-v2:production-census-schema-v113-coordinate-provenance -- --dry-run
 *
 *   ALLOW_PRODUCTION_CENSUS_SCHEMA_V113=1 \
 *   CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES=1 \
 *   CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
 *   npm run research-engine-v2:production-census-schema-v113-coordinate-provenance -- --apply \
 *     --confirm-add-coordinate-provenance-fields-only \
 *     --confirm-no-record-writes \
 *     --confirm-no-brand-explorer-writes \
 *     --confirm-no-field-deletes \
 *     --confirm-no-field-renames \
 *     --confirm-no-field-population
 */

import { execSync } from "node:child_process";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseV113Args,
  checkV113EnvFlags,
  runV113DryRun,
  runV113Apply,
  renderV113DryRunMarkdown,
  renderV113ApplyMarkdown,
  STATUS,
  V113_FIELD_NAMES,
} from "../lib/research-engine-v2/production-census-schema-v113-coordinate-provenance-fields.js";
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
      stderr_tail: (err.stderr?.toString?.() || "").slice(-1000),
    };
  }
}

function loadBeSummary(gates) {
  const safety = {
    touched: false,
    writes: 0,
    gates: gates.map((g) => ({
      label: g.label,
      ok: g.ok,
      exit_code: g.exit_code,
      command: g.command,
    })),
    all_pass: gates.every((g) => g.ok),
  };
  try {
    const u = JSON.parse(
      readFileSync(join(ROOT, "reports/brand-explorer-active-universe-source-of-truth.json"), "utf8")
    );
    safety.active_universe = u.activeSourceOfTruth?.totalCount ?? 62;
  } catch {
    safety.active_universe = null;
  }
  safety.summary = {
    active_universe: safety.active_universe,
    pvql: gates.find((g) => g.label === "pvql_quiet")?.ok ? "PASS" : "FAIL",
    momentum: gates.find((g) => g.label === "momentum_evidence")?.ok ? "PASS" : "FAIL",
    mandatory_gates: gates.find((g) => g.label === "mandatory_release_gates")?.ok
      ? "PASS"
      : "FAIL",
    semantic: gates.find((g) => g.label === "global_active_semantic")?.ok ? "PASS" : "FAIL",
  };
  return safety;
}

function appendLearningLedgerEntry(report) {
  const today = new Date().toISOString().slice(0, 10);
  const seed = buildSeedLearningEntries();
  // Mark prior recommendation as implemented
  for (const e of seed) {
    if (e.id === "census-schema-v113-geocode-provenance") {
      e.status = "implemented";
      e.fixture_added = true;
      e.test_added = false;
      e.next_action =
        "Use provenance fields on next approved address-geocode apply; provider/terms decision still required.";
      e.source_report =
        "reports/research-engine-v2/production-census-schema-v113-coordinate-provenance-fields.json";
    }
  }

  const newEntry = {
    id: "census-schema-v113-provenance-fields-applied",
    date: today,
    process: "census",
    batch_name: "production_census_schema_v113_coordinate_provenance",
    source_report:
      "reports/research-engine-v2/production-census-schema-v113-coordinate-provenance-fields.json",
    issue_type: "learned_validation_rule",
    example_records: [...V113_FIELD_NAMES],
    reusable_pattern:
      "Coordinates and addresses require provenance fields before production geocode writes.",
    proposed_code_change:
      "Schema v1.1.3 fields added on Hotel Property Census; leave blank until approved geocode apply.",
    module_to_update:
      "lib/research-engine-v2/production-census-schema-v113-coordinate-provenance-fields.js",
    fixture_added: true,
    test_added: false,
    status: "implemented",
    next_action:
      "Founder provider/storage decision → address-geocode dry-run with provenance mapping → apply under confirm flags.",
    lane: "geocoding_fallback",
    metrics: {
      fields_created: (report.fields_created || []).length,
      field_count_after: report.field_count_after,
      status: report.status,
    },
  };

  if (!seed.some((e) => e.id === newEntry.id)) seed.push(newEntry);

  const ledger = buildLedgerDocument(seed);
  writeJson(join(ROOT, LEARNING_PATHS.ledgerJson), ledger);
  writeMd(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger));
  writeJson(
    join(ROOT, LEARNING_PATHS.seedEntries),
    JSON.stringify(
      {
        version: ledger.version,
        note: "Updated after production-census-schema-v113 apply",
        entry_count: seed.length,
        entries: seed,
      },
      null,
      2
    )
  );

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
  const doc = `# Production Census Schema v1.1.3 — Address & Coordinate Provenance Fields

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** ${report.apply_executed}  
**Base:** Deal Capture Platform (\`${report.base_id_masked}\`)  
**Table:** Hotel Property Census (\`${report.table_id}\`)

## 1. Executive summary

Schema-only create of coordinate/address provenance fields. No record population. No Brand Explorer writes.

| Metric | Value |
| --- | ---: |
| Fields created | ${(report.fields_created || []).length} |
| Fields already existing | ${(report.fields_already_existing || []).length} |
| Field count | ${report.field_count_before} → ${report.field_count_after} |
| Census records | ${report.validation?.record_count ?? "—"} |
| Validation pass | ${report.validation_pass} |
| BE gates all_pass | ${report.brand_explorer_safety?.all_pass ?? "—"} |

## 2. Fields created

${created || "- (none)"}

## 3. Fields already existing

${existing || "- (none)"}

## 4. Final Census field count

**${report.field_count_after}**

## 5. Census validation

- Records: ${report.validation?.record_count}
- Held (Human Review Required): ${report.validation?.human_review_true}
- Coords filled: ${report.validation?.coords_filled}
- Provenance populated: ${report.validation?.provenance_any_populated}
- Owner/operator/rooms/dates filled: ${report.validation?.owner_filled}/${report.validation?.operator_filled}/${report.validation?.rooms_filled}/dates=${(report.validation?.opening_filled || 0) + (report.validation?.renovation_filled || 0)}

## 6. Brand Explorer safety

all_pass: **${report.brand_explorer_safety?.all_pass ?? "pending"}**

## 7. Learning ledger

\`\`\`json
${JSON.stringify(report.learning_ledger_update || {}, null, 2)}
\`\`\`

## 8. Recommended next step

${report.next_recommended_step || ""}

## Apply command (reference)

\`\`\`bash
ALLOW_PRODUCTION_CENSUS_SCHEMA_V113=1 \\
CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES=1 \\
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \\
npm run research-engine-v2:production-census-schema-v113-coordinate-provenance -- --apply \\
  --confirm-add-coordinate-provenance-fields-only \\
  --confirm-no-record-writes \\
  --confirm-no-brand-explorer-writes \\
  --confirm-no-field-deletes \\
  --confirm-no-field-renames \\
  --confirm-no-field-population
\`\`\`
`;
  writeMd(join(DOCS, "production-census-schema-v113-coordinate-provenance-fields.md"), doc);
}

async function main() {
  const args = parseV113Args();
  const env = checkV113EnvFlags();
  mkdirSync(REPORTS, { recursive: true });
  mkdirSync(DOCS, { recursive: true });

  console.log(`[census-v113] mode=${args.apply ? "apply" : "dry-run"} env_ok=${env.allOk}`);

  if (!args.apply) {
    const dry = await runV113DryRun();
    writeJson(join(REPORTS, "production-census-schema-v113-coordinate-provenance-fields-dry-run.json"), dry);
    writeMd(
      join(REPORTS, "production-census-schema-v113-coordinate-provenance-fields-dry-run.md"),
      renderV113DryRunMarkdown(dry)
    );
    // Also write the canonical report names requested (dry-run snapshot)
    writeJson(join(REPORTS, "production-census-schema-v113-coordinate-provenance-fields.json"), dry);
    writeMd(
      join(REPORTS, "production-census-schema-v113-coordinate-provenance-fields.md"),
      renderV113DryRunMarkdown(dry)
    );
    console.log(
      JSON.stringify(
        {
          status: dry.status,
          dry_run_pass: dry.dry_run_pass,
          to_add: dry.fields_to_add?.length,
          existed: dry.fields_already_existed?.length,
          field_count_before: dry.field_count_before,
          expected_after: dry.expected_field_count_after,
          census: dry.census_record_count,
        },
        null,
        2
      )
    );
    if (!dry.dry_run_pass && dry.status !== STATUS.DRY_RUN_PASS) process.exitCode = 1;
    return;
  }

  const report = await runV113Apply(process.argv.slice(2));

  if (
    report.apply_executed &&
    (report.status === STATUS.APPLIED || report.status === STATUS.PARTIAL) &&
    !args.skipBeGates
  ) {
    console.log("[census-v113] running Brand Explorer safety gates…");
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
    report.brand_explorer_safety = {
      touched: false,
      writes: 0,
      skipped: true,
      note: "Passed --skip-be-gates",
    };
  }

  if (report.apply_executed && report.status === STATUS.APPLIED) {
    try {
      report.learning_ledger_update = appendLearningLedgerEntry(report);
      console.log("[census-v113] learning ledger updated + audit run");
    } catch (err) {
      report.learning_ledger_update = {
        ok: false,
        error: err?.message || String(err),
      };
    }
  }

  writeJson(join(REPORTS, "production-census-schema-v113-coordinate-provenance-fields.json"), report);
  writeMd(
    join(REPORTS, "production-census-schema-v113-coordinate-provenance-fields.md"),
    renderV113ApplyMarkdown(report)
  );
  writeDurableDoc(report);

  console.log(
    JSON.stringify(
      {
        status: report.status,
        fields_created: report.fields_created?.length ?? 0,
        field_count_after: report.field_count_after,
        validation_pass: report.validation_pass,
        be_all_pass: report.brand_explorer_safety?.all_pass ?? null,
        learning: report.learning_ledger_update?.entry_id ?? null,
      },
      null,
      2
    )
  );

  if (report.status === STATUS.BLOCKED || report.status === STATUS.CONFIRMATION_MISSING) {
    process.exitCode = 1;
  }
  if (report.brand_explorer_safety && report.brand_explorer_safety.all_pass === false) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("[census-v113] FAILED", err);
  process.exitCode = 1;
});
