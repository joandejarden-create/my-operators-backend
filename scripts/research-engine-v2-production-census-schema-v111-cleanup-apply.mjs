/**
 * Apply approved Production Census schema v1.1.1 cleanup (renames + optional views).
 *
 *   npm run research-engine-v2:production-census-schema-v111-cleanup-apply -- --dry-run
 *   npm run research-engine-v2:production-census-schema-v111-cleanup-apply -- --apply ...confirms
 */

import { execSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseV111ApplyArgs,
  checkV111ApplyEnv,
  runV111CleanupApply,
  renderV111ApplyMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-schema-v111-cleanup-apply.js";

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
    const out = execSync(command, {
      cwd: ROOT,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      maxBuffer: 20 * 1024 * 1024,
    });
    return { label, command, ok: true, exit_code: 0, stdout_tail: out.slice(-4000) };
  } catch (err) {
    const stdout = err.stdout?.toString?.() || "";
    const stderr = err.stderr?.toString?.() || "";
    return {
      label,
      command,
      ok: false,
      exit_code: err.status ?? 1,
      stdout_tail: stdout.slice(-3000),
      stderr_tail: stderr.slice(-2000),
    };
  }
}

function parseUniverse(stdout) {
  const m = stdout.match(/active[_\s-]?universe[^\d]*(\d+)/i) || stdout.match(/"active_count"\s*:\s*(\d+)/);
  return m ? Number(m[1]) : null;
}

function parseSemantic(stdout) {
  const c = stdout.match(/critical[^\d]*(\d+)/i);
  const h = stdout.match(/high[^\d]*(\d+)/i);
  const med = stdout.match(/\bmedium[^\d]*(\d+)/i);
  return {
    critical: c ? Number(c[1]) : null,
    high: h ? Number(h[1]) : null,
    medium: med ? Number(med[1]) : null,
  };
}

async function main() {
  const args = parseV111ApplyArgs();
  const env = checkV111ApplyEnv();
  console.log(`[census-v111-apply] mode=${args.apply ? "apply" : "dry-run"} env_ok=${env.allOk}`);

  const report = await runV111CleanupApply(process.argv.slice(2));

  let brandExplorerSafety = null;
  if (report.apply_executed && report.status !== STATUS.BLOCKED && report.status !== STATUS.CONFIRMATION_MISSING) {
    console.log("[census-v111-apply] running Brand Explorer safety gates…");
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

    const universeOut = gates[0].stdout_tail || "";
    const semanticOut = gates[1].stdout_tail || "";
    brandExplorerSafety = {
      gates,
      all_pass: gates.every((g) => g.ok),
      active_universe: parseUniverse(universeOut),
      semantic: parseSemantic(semanticOut),
      expected: {
        active_universe: 62,
        semantic_c_h_m: "0/0/0",
        pvql: "PASS",
        momentum: "PASS",
        mandatory_gates: "PASS",
      },
    };
    report.brand_explorer_safety = brandExplorerSafety;
    if (!brandExplorerSafety.all_pass && report.status === STATUS.APPLIED) {
      report.status = STATUS.PARTIAL;
      report.remaining_cleanup_items = [
        ...(report.remaining_cleanup_items || []),
        "Brand Explorer safety gate failed — investigate before enrichment",
      ];
    }
  }

  writeJson(join(REPORTS, "production-census-schema-v111-cleanup-apply.json"), report);
  writeMd(join(REPORTS, "production-census-schema-v111-cleanup-apply.md"), renderV111ApplyMarkdown(report));

  const doc = [
    `# Production Census Schema v1.1.1 Cleanup Apply`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Generated:** ${report.generated_at}`,
    `**Apply executed:** ${report.apply_executed}`,
    ``,
    `## What was applied`,
    ``,
    `- Renamed: Last Verified Date → Last Reviewed Date`,
    `- Renamed: Resort Amenities Flag → Resort / Leisure Flag`,
    `- Renamed: Extended Stay Amenity Flag → Extended Stay Flag`,
    `- Kept: Rooms / Keys, Operator / Management Company, Owner Name, Source URL, State / Region`,
    `- Over-modeled amenity flags: **not deleted**; hide via Airtable UI (API unsupported)`,
    ``,
    `## Manual steps (if status is partial)`,
    ``,
    `1. Hide from Grid view: Fitness Flag, Pool Flag, Parking Flag, Airport Shuttle Flag, Spa Flag, Beach / Waterfront Flag`,
    `2. Create founder views: Census - Core Identity / Enrichment / Owner Operator / Steward Review (field lists in apply report)`,
    ``,
    `## Safety`,
    ``,
    `- No record writes`,
    `- No field deletes`,
    `- Brand Explorer not patched`,
    ``,
    `## Next`,
    ``,
    report.next_recommended_step ||
      "Complete manual view steps, then freeze Census field contract and begin descriptions + amenities enrichment.",
    ``,
  ].join("\n");
  writeMd(join(DOCS, "production-census-schema-v111-cleanup-apply.md"), doc);

  console.log(
    JSON.stringify(
      {
        status: report.status,
        apply_executed: report.apply_executed,
        renames: report.fields_renamed?.length ?? 0,
        validation_pass: report.validation_pass,
        views_created: report.views?.created?.length ?? 0,
        views_failed: report.views?.failed?.length ?? 0,
        be_all_pass: brandExplorerSafety?.all_pass ?? null,
        be_universe: brandExplorerSafety?.active_universe ?? null,
      },
      null,
      2
    )
  );

  if (report.status === STATUS.BLOCKED || report.status === STATUS.CONFIRMATION_MISSING) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("[census-v111-apply] FAILED", err);
  process.exitCode = 1;
});
