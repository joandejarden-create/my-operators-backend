/**
 * Freeze Production Census field contract v1.1.1 (read-only).
 *
 *   npm run research-engine-v2:production-census-field-contract-v111
 *   npm run research-engine-v2:production-census-field-contract-v111 -- --skip-be-gates
 */

import { execSync } from "node:child_process";
import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  runFieldContractFreeze,
  renderFieldContractMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-field-contract-v111.js";

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
    return { label, command, ok: true, exit_code: 0, stdout_tail: out.slice(-2000) };
  } catch (err) {
    return {
      label,
      command,
      ok: false,
      exit_code: err.status ?? 1,
      stdout_tail: (err.stdout?.toString?.() || "").slice(-1500),
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
    safety.active_universe = u.activeSourceOfTruth?.totalCount ?? u.activeCount ?? null;
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
    safety.semantic = {
      activeCount: s.activeCount,
      severityTotals: s.severityTotals,
      freezeDecision: s.freezeDecision,
    };
  } catch {
    safety.semantic = null;
  }
  safety.summary = {
    active_universe: safety.active_universe,
    semantic_c_h_m: safety.semantic?.severityTotals
      ? `${safety.semantic.severityTotals.critical || 0}/${safety.semantic.severityTotals.high || 0}/${safety.semantic.severityTotals.medium || 0}`
      : null,
    pvql: gates.find((g) => g.label === "pvql_quiet")?.ok ? "PASS" : "FAIL",
    momentum: gates.find((g) => g.label === "momentum_evidence")?.ok ? "PASS" : "FAIL",
    mandatory_gates: gates.find((g) => g.label === "mandatory_release_gates")?.ok
      ? "PASS"
      : "FAIL",
  };
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
  // Durable schema authority copy — same content as the full report.
  writeMd(join(DOCS, "production-census-field-contract-v111.md"), renderFieldContractMarkdown(report));
}

async function main() {
  const skipBe = process.argv.includes("--skip-be-gates");
  console.log(`[census-contract-v111] starting (skip_be=${skipBe})`);

  const report = await runFieldContractFreeze();
  console.log(
    `[census-contract-v111] status=${report.status} validation=${report.validation?.pass} radar_missing=${report.radar_public_display?.missing?.length} coverage_unlisted=${report.contract_coverage?.unlisted_live_fields?.length} coverage_missing=${report.contract_coverage?.missing_from_live?.length}`
  );

  writeJson(join(REPORTS, "production-census-field-contract-v111.json"), report);
  writeMd(join(REPORTS, "production-census-field-contract-v111.md"), renderFieldContractMarkdown(report));
  writeDocs(report);

  if (!skipBe && report.status !== STATUS.HOLD) {
    console.log("[census-contract-v111] running Brand Explorer safety gates…");
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
    if (!report.brand_explorer_safety.all_pass) {
      report.status = STATUS.HOLD;
      report.final_recommendation =
        "Hold enrichment: Brand Explorer safety gates failed. Investigate before first enrichment lane.";
      report.hold_reason = "brand_explorer_safety_failed";
    }
    writeJson(join(REPORTS, "production-census-field-contract-v111.json"), report);
    writeMd(
      join(REPORTS, "production-census-field-contract-v111.md"),
      renderFieldContractMarkdown(report)
    );
    writeDocs(report);
  }

  console.log(
    JSON.stringify(
      {
        status: report.status,
        validation_pass: report.validation?.pass,
        radar_missing: report.radar_public_display?.missing?.length,
        first_enrichment_allowed: report.first_enrichment_lane?.allowed?.length,
        be_all_pass: report.brand_explorer_safety?.all_pass ?? null,
        be_universe: report.brand_explorer_safety?.active_universe ?? null,
        semantic: report.brand_explorer_safety?.summary?.semantic_c_h_m ?? null,
      },
      null,
      2
    )
  );

  if (report.status === STATUS.HOLD) process.exitCode = 1;
  if (report.brand_explorer_safety && !report.brand_explorer_safety.all_pass) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[census-contract-v111] FAILED", err);
  process.exitCode = 1;
});
