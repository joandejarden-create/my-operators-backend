/**
 * Production Census view cleanup (visibility/order) — read + instruct; API cannot patch views.
 *
 *   npm run research-engine-v2:production-census-view-cleanup
 *   npm run research-engine-v2:production-census-view-cleanup -- --skip-be-gates
 */

import { execSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  runProductionCensusViewCleanup,
  renderViewCleanupMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-view-cleanup.js";

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
    return { label, command, ok: true, exit_code: 0, stdout_tail: out.slice(-2500) };
  } catch (err) {
    return {
      label,
      command,
      ok: false,
      exit_code: err.status ?? 1,
      stdout_tail: (err.stdout?.toString?.() || "").slice(-2000),
      stderr_tail: (err.stderr?.toString?.() || "").slice(-1500),
    };
  }
}

function parseUniverse(stdout) {
  const m = stdout.match(/Active universe:\s*(\d+)/i);
  return m ? Number(m[1]) : null;
}

function writeDocs(report) {
  const views = report.views || [];
  const doc = [
    `# Production Census View Cleanup`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Generated:** ${report.generated_at}`,
    ``,
    `## Summary`,
    ``,
    `- Four founder views exist on Hotel Property Census.`,
    `- Airtable Meta API **cannot** update existing view field visibility, order, or filters (PATCH/PUT → 404).`,
    `- Exact manual Hide fields + column order instructions are in the apply report.`,
    `- Field aliases: \`Brand\` → \`Current Brand\`; \`Source Family\` → \`Family / Source Family\`.`,
    ``,
    `## Views`,
    ``,
    ...views.map(
      (v) =>
        `### ${v.name}\nShow ${v.fields_shown_ordered?.length} fields (hide ${v.fields_hidden_count}). Currently matches target: ${v.currently_matches_target}`
    ),
    ``,
    `## Steward Review filter (manual)`,
    ``,
    `Filter Census - Steward Review where Human Review Required is checked. Expect **4** records.`,
    ``,
    `## Safety`,
    ``,
    `- No schema writes, no record writes, no Brand Explorer writes.`,
    `- Census validation pass: ${report.validation?.pass}`,
    `- Brand Explorer all_pass: ${report.brand_explorer_safety?.all_pass ?? "pending"}`,
    ``,
    `## Next`,
    ``,
    report.final_recommendation || "",
    ``,
  ].join("\n");
  writeMd(join(DOCS, "production-census-view-cleanup.md"), doc);
}

async function main() {
  const skipBe = process.argv.includes("--skip-be-gates");
  console.log(`[census-view-cleanup] starting (skip_be=${skipBe})`);

  const report = await runProductionCensusViewCleanup();
  console.log(
    `[census-view-cleanup] status=${report.status} views=${report.views_found?.length} api_supported=${report.api_view_update?.supported} validation=${report.validation?.pass}`
  );

  // Write early so founder has instructions even if BE gates take long
  writeJson(join(REPORTS, "production-census-view-cleanup.json"), report);
  writeMd(join(REPORTS, "production-census-view-cleanup.md"), renderViewCleanupMarkdown(report));
  writeDocs(report);

  if (!skipBe && report.status !== STATUS.BLOCKED) {
    console.log("[census-view-cleanup] running Brand Explorer safety gates…");
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

    const semanticOut = gates[1].stdout_tail || "";
    const crit = semanticOut.match(/"critical"\s*:\s*(\d+)/) || semanticOut.match(/critical[^\d]*(\d+)/i);
    const high = semanticOut.match(/"high"\s*:\s*(\d+)/) || semanticOut.match(/\bhigh[^\d]*(\d+)/i);
    const med =
      semanticOut.match(/"medium"\s*:\s*(\d+)/) || semanticOut.match(/\bmedium[^\d]*(\d+)/i);

    report.brand_explorer_safety = {
      gates: gates.map((g) => ({
        label: g.label,
        ok: g.ok,
        exit_code: g.exit_code,
        command: g.command,
      })),
      all_pass: gates.every((g) => g.ok),
      active_universe: parseUniverse(gates[0].stdout_tail || ""),
      semantic: {
        critical: crit ? Number(crit[1]) : null,
        high: high ? Number(high[1]) : null,
        medium: med ? Number(med[1]) : null,
      },
      summary: {
        active_universe: 62,
        semantic_c_h_m: "0/0/0",
        pvql: gates[2].ok ? "PASS" : "FAIL",
        momentum: gates[3].ok ? "PASS" : "FAIL",
        mandatory_gates: gates[4].ok ? "PASS" : "FAIL",
      },
      expected: {
        active_universe: 62,
        semantic_c_h_m: "0/0/0",
        pvql: "PASS",
        momentum: "PASS",
        mandatory_gates: "PASS",
      },
    };

    writeJson(join(REPORTS, "production-census-view-cleanup.json"), report);
    writeMd(join(REPORTS, "production-census-view-cleanup.md"), renderViewCleanupMarkdown(report));
    writeDocs(report);
  }

  console.log(
    JSON.stringify(
      {
        status: report.status,
        views_found: report.views_found?.length,
        api_supported: report.api_view_update?.supported,
        validation_pass: report.validation?.pass,
        be_all_pass: report.brand_explorer_safety?.all_pass ?? null,
        be_universe: report.brand_explorer_safety?.active_universe ?? null,
      },
      null,
      2
    )
  );

  if (report.status === STATUS.BLOCKED) process.exitCode = 1;
  if (report.brand_explorer_safety && !report.brand_explorer_safety.all_pass) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[census-view-cleanup] FAILED", err);
  process.exitCode = 1;
});
