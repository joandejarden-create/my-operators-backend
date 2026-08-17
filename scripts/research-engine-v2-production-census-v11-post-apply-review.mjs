/**
 * Read-only Production Census v1.1 post-apply review.
 *
 *   npm run research-engine-v2:production-census-v11-post-apply-review
 *
 * Optional: --skip-be-gates to skip long Brand Explorer regression (not recommended).
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import "dotenv/config";
import {
  runPostApplyReview,
  renderPostApplyMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-v11-post-apply-review.js";

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
    console.log(`[post-apply-review] BE gate: ${cmd} ${args.join(" ")}`);
    const r = spawnSync(cmd, args, {
      cwd: ROOT,
      shell: true,
      encoding: "utf8",
      maxBuffer: 20 * 1024 * 1024,
    });
    results.push({
      cmd: `${cmd} ${args.join(" ")}`,
      exit_code: r.status,
      ok: r.status === 0,
    });
    if (r.status !== 0) {
      console.error(`[post-apply-review] gate failed exit=${r.status}`);
      break;
    }
  }
  return results;
}

function loadBeSafetyFromReports(gateResults) {
  const out = {
    gate_results: gateResults,
    active_universe: null,
    semantic: null,
    pvql: null,
    overall_pass: gateResults.every((g) => g.ok),
  };
  try {
    const u = JSON.parse(
      readFileSync(join(ROOT, "reports/brand-explorer-active-universe-source-of-truth.json"), "utf8")
    );
    out.active_universe = u.activeSourceOfTruth?.totalCount ?? null;
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
  console.log("[post-apply-review] read-only Census v1.1 review");

  let beSafety;
  if (skipBe) {
    beSafety = { skipped: true, note: "BE gates skipped via --skip-be-gates" };
  } else {
    const gateResults = runBeGates();
    beSafety = loadBeSafetyFromReports(gateResults);
  }

  const report = await runPostApplyReview({ beSafety });
  // If BE failed, escalate to hold
  if (beSafety && beSafety.overall_pass === false) {
    report.status = STATUS.HOLD;
    report.final_recommendation =
      "Hold enrichment — Brand Explorer safety gates failed after Census v1.1.";
  }

  const md = renderPostApplyMarkdown(report);
  writeJson(join(REPORTS, "production-census-v11-post-apply-review.json"), report);
  writeMd(join(REPORTS, "production-census-v11-post-apply-review.md"), md);
  writeMd(
    join(DOCS, "production-census-v11-post-apply-review.md"),
    `${md}\n\n## Scope\n\nRead-only. No Airtable schema/record writes. No Brand Explorer writes. No Webhound.\n`
  );

  console.log(
    JSON.stringify(
      {
        status: report.status,
        census: report.foundation?.hotel_property_census,
        steward: report.foundation?.steward_review,
        amenity_overmodel: report.amenities_review?.filter((a) => a.classification === "possible_overmodeling")
          .length,
        cleanup_items: report.v11_1_cleanup_plan?.items?.length,
        be: {
          universe: beSafety?.active_universe,
          semantic: beSafety?.semantic?.severityTotals,
          pvql: beSafety?.pvql?.overallPass,
          overall: beSafety?.overall_pass,
        },
        recommendation: report.recommended_first_enrichment_lane,
      },
      null,
      2
    )
  );

  if (
    report.status !== STATUS.CLEAN &&
    report.status !== STATUS.MINOR_CLEANUP
  ) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("[post-apply-review] FAILED", err);
  process.exitCode = 1;
});
