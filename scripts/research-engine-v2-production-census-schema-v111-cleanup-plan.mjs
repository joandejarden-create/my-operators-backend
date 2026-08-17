/**
 * Read-only Production Census schema v1.1.1 cleanup plan.
 *
 *   npm run research-engine-v2:production-census-schema-v111-cleanup-plan
 *   npm run research-engine-v2:production-census-schema-v111-cleanup-plan -- --skip-be-gates
 */

import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import "dotenv/config";
import {
  runV111CleanupPlan,
  renderV111PlanMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-schema-v111-cleanup-plan.js";

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

function runBeGates() {
  const cmds = [
    ["npm", ["run", "brand-explorer-active-universe-source-of-truth", "--", "--dry-run"]],
    ["npm", ["run", "brand-explorer-global-active-semantic-audit", "--", "--dry-run", "--fresh"]],
    ["node", ["scripts/brand-explorer-quiet-sequential-pvql.mjs"]],
    ["npm", ["run", "test:brand-explorer-recent-momentum-evidence-quality"]],
    ["npm", ["run", "test:brand-explorer-mandatory-release-gates"]],
  ];
  const results = [];
  for (const [cmd, args] of cmds) {
    console.log(`[v111-plan] BE gate: ${cmd} ${args.join(" ")}`);
    const r = spawnSync(cmd, args, {
      cwd: ROOT,
      shell: true,
      encoding: "utf8",
      maxBuffer: 20 * 1024 * 1024,
    });
    results.push({ cmd: `${cmd} ${args.join(" ")}`, exit_code: r.status, ok: r.status === 0 });
    if (r.status !== 0) break;
  }
  return results;
}

function loadBeSafety(gateResults) {
  const out = {
    gate_results: gateResults,
    overall_pass: gateResults.length > 0 && gateResults.every((g) => g.ok),
  };
  try {
    const u = JSON.parse(
      readFileSync(join(ROOT, "reports/brand-explorer-active-universe-source-of-truth.json"), "utf8")
    );
    out.active_universe = u.activeSourceOfTruth?.totalCount;
  } catch {
    /* ignore */
  }
  try {
    const s = JSON.parse(
      readFileSync(join(ROOT, "reports/brand-explorer-global-active-semantic-audit-refresh.json"), "utf8")
    );
    out.semantic = {
      activeCount: s.activeCount,
      severityTotals: s.severityTotals,
      freezeDecision: s.freezeDecision,
    };
  } catch {
    /* ignore */
  }
  try {
    const p = JSON.parse(
      readFileSync(join(ROOT, "reports/brand-explorer-public-visibility-quality-lock-quiet.json"), "utf8")
    );
    out.pvql = p.summary;
  } catch {
    /* ignore */
  }
  return out;
}

async function main() {
  const skipBe = process.argv.includes("--skip-be-gates");
  console.log("[v111-plan] read-only cleanup plan");

  let beSafety;
  if (skipBe) {
    beSafety = { skipped: true };
  } else {
    beSafety = loadBeSafety(runBeGates());
  }

  const report = await runV111CleanupPlan({ beSafety });
  if (beSafety?.overall_pass === false) {
    report.status = STATUS.REQUIRES_DECISIONS;
    report.recommended_next_step =
      "Brand Explorer gates failed — investigate before any Census schema cleanup apply.";
  }

  const md = renderV111PlanMarkdown(report);
  writeJson(join(REPORTS, "production-census-schema-v111-cleanup-plan.json"), report);
  writeMd(join(REPORTS, "production-census-schema-v111-cleanup-plan.md"), md);
  writeMd(
    join(DOCS, "production-census-schema-v111-cleanup-plan.md"),
    `${md}\n\n## Scope\n\nRead-only plan. No Airtable field renames/deletes, no record writes, no Brand Explorer patches, no enrichment.\n`
  );

  console.log(
    JSON.stringify(
      {
        status: report.status,
        census_fields: report.schema_status.field_count,
        records: report.schema_status.record_count,
        founder_decisions: report.founder_decisions.length,
        overmodeled_blank: report.overmodeled_amenity_fields.every((a) => a.blank_across_all_666),
        be: {
          universe: beSafety?.active_universe,
          chm: beSafety?.semantic?.severityTotals,
          pvql: beSafety?.pvql?.overallPass,
          overall: beSafety?.overall_pass,
        },
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("[v111-plan] FAILED", err);
  process.exitCode = 1;
});
