/**
 * Production Census schema v1.1.2 — add Radar/public display fields.
 *
 *   npm run research-engine-v2:production-census-schema-v112-radar-fields -- --dry-run
 *   npm run research-engine-v2:production-census-schema-v112-radar-fields -- --apply \
 *     --confirm-add-radar-public-fields-only \
 *     --confirm-no-record-writes \
 *     --confirm-no-brand-explorer-writes \
 *     --confirm-no-field-deletes \
 *     --confirm-no-field-renames
 *
 * Requires env: ALLOW_PRODUCTION_CENSUS_SCHEMA_V112=1
 *               CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES=1
 *               CONFIRM_NO_BRAND_EXPLORER_WRITES=1
 */

import { execSync } from "node:child_process";
import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseV112Args,
  checkV112EnvFlags,
  runV112DryRun,
  runV112Apply,
  renderV112DryRunMarkdown,
  renderV112ApplyMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-schema-v112-radar-fields.js";

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
  try {
    const s = JSON.parse(
      readFileSync(
        join(ROOT, "reports/brand-explorer-global-active-semantic-audit-refresh.json"),
        "utf8"
      )
    );
    const sev = s.severityTotals || {};
    safety.semantic = {
      activeCount: s.activeCount,
      severityTotals: sev,
      freezeDecision: s.freezeDecision,
    };
    safety.summary = {
      active_universe: safety.active_universe,
      semantic_c_h_m: `${sev.critical || 0}/${sev.high || 0}/${sev.medium || 0}`,
      pvql: gates.find((g) => g.label === "pvql_quiet")?.ok ? "PASS" : "FAIL",
      momentum: gates.find((g) => g.label === "momentum_evidence")?.ok ? "PASS" : "FAIL",
      mandatory_gates: gates.find((g) => g.label === "mandatory_release_gates")?.ok
        ? "PASS"
        : "FAIL",
    };
  } catch {
    safety.summary = {
      active_universe: safety.active_universe,
      semantic_c_h_m: null,
      pvql: gates.find((g) => g.label === "pvql_quiet")?.ok ? "PASS" : "FAIL",
      momentum: gates.find((g) => g.label === "momentum_evidence")?.ok ? "PASS" : "FAIL",
      mandatory_gates: gates.find((g) => g.label === "mandatory_release_gates")?.ok
        ? "PASS"
        : "FAIL",
    };
  }
  safety.expected = {
    active_universe: 62,
    semantic_c_h_m: "0/0/0",
    pvql: "PASS",
    momentum: "PASS",
    mandatory_gates: "PASS",
  };
  return safety;
}

function writeDocs(report) {
  const created = (report.fields_created || []).map((f) => `- ${f.name} (${f.type})`).join("\n");
  const doc = `# Production Census Schema v1.1.2 — Radar/Public Display Fields

**Status:** \`${report.status}\`
**Generated:** ${report.generated_at}
**Apply executed:** ${report.apply_executed}

## What was added

${created || "- (none)"}

## Counts

- Field count: ${report.field_count_before} → ${report.field_count_after}
- Census records: ${report.validation?.record_count}
- Validation pass: ${report.validation_pass}

## Safety

- No record population of Radar fields
- No enrichment writes
- No Brand Explorer writes
- Brand Explorer all_pass: ${report.brand_explorer_safety?.all_pass ?? "pending"}

## Next

${report.next_recommended_step || ""}
`;
  writeMd(join(DOCS, "production-census-schema-v112-radar-fields.md"), doc);
}

async function main() {
  const args = parseV112Args();
  const env = checkV112EnvFlags();
  console.log(`[census-v112] mode=${args.apply ? "apply" : "dry-run"} env_ok=${env.allOk}`);

  if (!args.apply) {
    const dry = await runV112DryRun();
    writeJson(join(REPORTS, "production-census-schema-v112-radar-fields-dry-run.json"), dry);
    writeMd(join(REPORTS, "production-census-schema-v112-radar-fields-dry-run.md"), renderV112DryRunMarkdown(dry));
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
    if (!dry.dry_run_pass) process.exitCode = 1;
    return;
  }

  const report = await runV112Apply(process.argv.slice(2));

  writeJson(join(REPORTS, "production-census-schema-v112-radar-fields-apply.json"), report);
  writeMd(join(REPORTS, "production-census-schema-v112-radar-fields-apply.md"), renderV112ApplyMarkdown(report));
  writeDocs(report);

  if (
    report.apply_executed &&
    (report.status === STATUS.APPLIED || report.status === STATUS.PARTIAL)
  ) {
    console.log("[census-v112] running Brand Explorer safety gates…");
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
    writeJson(join(REPORTS, "production-census-schema-v112-radar-fields-apply.json"), report);
    writeMd(
      join(REPORTS, "production-census-schema-v112-radar-fields-apply.md"),
      renderV112ApplyMarkdown(report)
    );
    writeDocs(report);
  }

  console.log(
    JSON.stringify(
      {
        status: report.status,
        fields_created: report.fields_created?.length ?? 0,
        field_count_after: report.field_count_after,
        validation_pass: report.validation_pass,
        readiness_next: report.radar_public_readiness_classification_can_run_next,
        be_all_pass: report.brand_explorer_safety?.all_pass ?? null,
        be_universe: report.brand_explorer_safety?.active_universe ?? null,
        semantic: report.brand_explorer_safety?.summary?.semantic_c_h_m ?? null,
      },
      null,
      2
    )
  );

  if (report.status === STATUS.BLOCKED || report.status === STATUS.CONFIRMATION_MISSING) {
    process.exitCode = 1;
  }
  if (report.brand_explorer_safety && !report.brand_explorer_safety.all_pass) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[census-v112] FAILED", err);
  process.exitCode = 1;
});
