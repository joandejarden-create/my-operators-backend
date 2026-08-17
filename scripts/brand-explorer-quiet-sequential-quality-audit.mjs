#!/usr/bin/env node
/**
 * Quiet sequential quality audit — one brand at a time with delay (avoids 429 thrash).
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AUDIT_VERSION,
  write24TabSectionQualityReports,
  auditBrandTabSectionQuality,
} from "../lib/partner-intelligence/brand-explorer-24-tab-section-quality-audit.js";
import { listActiveUniverseSlugs } from "../lib/partner-intelligence/brand-explorer-active-universe.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const argv = process.argv.slice(2);
const brandsIdx = argv.indexOf("--brands");
const only =
  brandsIdx >= 0 && argv[brandsIdx + 1]
    ? argv[brandsIdx + 1]
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : null;

const allSlugs = await listActiveUniverseSlugs();
const slugs = (only || allSlugs).filter(Boolean);

console.log(`[quiet-quality] version=${AUDIT_VERSION} brands=${slugs.length}`);
const brandResults = [];
for (const slug of slugs) {
  process.stdout.write(`[quiet-quality] ${slug}... `);
  try {
    const row = await auditBrandTabSectionQuality(slug, {});
    brandResults.push(row);
    console.log(`${row.overallRecommendation} blockers=${row.scores?.blockerCount ?? "?"}`);
  } catch (err) {
    console.log(`ERR ${err.message}`);
    brandResults.push({
      slug,
      brand: slug,
      recordId: null,
      brandStatus: null,
      publicDisplayState: null,
      shouldRenderFullProfile: false,
      pvqlStatus: "error",
      overallRecommendation: "remediation_required",
      scores: { blockerCount: 99, composite: 0 },
      gates: {},
      tabFindings: [{ status: "fail", message: err.message }],
      imageFindings: [],
      scenarioRoles: {},
      error: err.message,
    });
  }
  await sleep(2500);
}

const recommendationCounts = brandResults.reduce((acc, b) => {
  acc[b.overallRecommendation] = (acc[b.overallRecommendation] || 0) + 1;
  return acc;
}, {});
const needsRemediation = (recommendationCounts.remediation_required || 0) > 0;
const needsMinor = (recommendationCounts.approve_after_minor_cleanup || 0) > 0;
const fullUniverse = !only && brandResults.length === allSlugs.length;
const allApprove =
  brandResults.length === slugs.length &&
  brandResults.every((b) => b.overallRecommendation === "approve_for_baseline_freeze");

let baselineFreezeDecision = allApprove
  ? fullUniverse
    ? "ready_to_freeze_45_active_public_full_baseline"
    : "approve_subset_pass"
  : needsRemediation
    ? "do_not_freeze_remediation_required"
    : needsMinor
      ? "freeze_after_minor_cleanup_pass"
      : "not_ready_to_freeze_45_active_public_full_baseline";

const report = {
  version: `${AUDIT_VERSION}-quiet`,
  generatedAt: new Date().toISOString(),
  dryRun: true,
  writePerformed: false,
  activeCount: allSlugs.length,
  auditedCount: brandResults.length,
  recommendationCounts,
  baselineFreezeDecision,
  baselineFreezeRationale: allApprove
    ? "All audited Active/Live brands recommend approve_for_baseline_freeze."
    : `${recommendationCounts.remediation_required || 0} remediation / ${recommendationCounts.approve_after_minor_cleanup || 0} minor.`,
  brandResults,
  crossBrandImageIssues: [],
  excludedFromUniverse: [],
  activeUniverseSource: "quiet-sequential",
  activeUniverseVersion: "quiet",
  quietSequential: true,
  brandsFilter: only,
};

const quietOut = path.join(ROOT, "reports", "brand-explorer-24-tab-section-quality-audit-quiet.json");
fs.writeFileSync(quietOut, `${JSON.stringify(report, null, 2)}\n`);

if (fullUniverse) {
  const paths = write24TabSectionQualityReports(report);
  console.log("Wrote canonical", paths.jsonPath);
}

console.log(JSON.stringify({ baselineFreezeDecision, recommendationCounts, quietOut }, null, 2));
process.exit(allApprove ? 0 : 3);
