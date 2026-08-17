#!/usr/bin/env node
/**
 * Regression lock for protected 45 Active/Live public-full baseline.
 *
 *   npm run test:brand-explorer-45-active-public-full-baseline
 *
 * Requires:
 * - reports/brand-explorer-45-active-public-full-baseline.json (freeze artifact)
 * - reports/brand-explorer-24-tab-section-quality-audit.json (latest quality audit)
 *
 * Live-checks: Active/Live universe, held/excluded probes (SO/, House of Originals,
 * Morgans, Radisson), fresh public-full PVQL (default), evidence quality for
 * wave brands, protected Brand Status / Company Validated vs freeze.
 *
 * Optional: --allow-cached-pvql-if-pass (only if on-disk PVQL is public-full
 *   lockPass-clean for all 45). Wave preflight must run fresh PVQL first and
 *   must not rely on a stale cached pass alone.
 * Optional: --force-live-pvql (always re-run live PVQL)
 * Optional: --skip-pvql / --skip-evidence (not for CI acceptance)
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import {
  BASELINE_VERSION_45,
  EXPECTED_ACTIVE_COUNT_45,
  REPORT_JSON_45,
  ROOT,
  run45ActivePublicFullBaselineRegression,
} from "../lib/partner-intelligence/brand-explorer-45-active-public-full-baseline.js";

async function main() {
  const argv = process.argv.slice(2);
  const skipPvql = argv.includes("--skip-pvql");
  const skipEvidence = argv.includes("--skip-evidence");

  console.log(
    `[${BASELINE_VERSION_45}] regression test (expected Active/Live=${EXPECTED_ACTIVE_COUNT_45})`
  );
  if (skipPvql) console.log("  WARN: --skip-pvql set (not for CI acceptance)");
  if (skipEvidence) console.log("  WARN: --skip-evidence set (not for CI acceptance)");

  const frozenPath = path.join(ROOT, "reports", REPORT_JSON_45);
  if (!fs.existsSync(frozenPath)) {
    console.error(`[FAIL] missing freeze artifact ${REPORT_JSON_45}`);
    console.error("  Run: npm run brand-explorer-45-active-public-full-baseline -- --dry-run");
    process.exit(1);
  }

  const frozen = JSON.parse(fs.readFileSync(frozenPath, "utf8"));
  if (frozen.baselineType !== "active_live_public_full") {
    console.error(
      `[FAIL] freeze artifact is not a public-full baseline (baselineType=${frozen.baselineType || "missing"}). Re-run freeze.`
    );
    process.exit(1);
  }
  if (frozen.freezeDecision !== "frozen_45_active_public_full_baseline") {
    console.error(
      `[FAIL] freezeDecision is not frozen_45_active_public_full_baseline (${frozen.freezeDecision}). Re-run freeze.`
    );
    process.exit(1);
  }

  const result = await run45ActivePublicFullBaselineRegression({
    reassessPvql: !skipPvql,
    requireQualityReport: true,
    forceLivePvql: argv.includes("--force-live-pvql") || !argv.includes("--allow-cached-pvql-if-pass"),
    allowCachedPvqlIfPass: argv.includes("--allow-cached-pvql-if-pass"),
    evaluateEvidence: !skipEvidence,
  });

  const { regression } = result;
  console.log(`Live universe count: ${result.liveUniverseCount}`);
  console.log(`Frozen decision: ${result.frozenDecision}`);
  console.log(`PVQL source: ${result.pvqlSource}`);
  for (const ex of result.liveExcluded || []) {
    console.log(`  excluded ${ex.slug}: status=${ex.brandStatus} activeLive=${ex.isActiveLive}`);
  }

  let failed = 0;
  for (const c of regression.checks || []) {
    if (!c.pass) {
      failed += 1;
      console.log(`[FAIL] ${c.slug}`);
      for (const f of c.failures) console.log(`  - ${f}`);
    } else {
      console.log(
        `[PASS] ${c.slug} status=${c.liveBrandStatus} pvql=${c.pvqlPass} quality=${c.qualityRecommendation} evidence=${c.evidencePass}`
      );
    }
  }

  if (!regression.pass) {
    failed += 1;
    console.log("[FAIL] baseline regression aggregate");
    for (const f of regression.failures) console.log(`  - ${f}`);
  } else {
    console.log("[PASS] baseline regression aggregate");
  }

  if (failed || !regression.pass) {
    console.error(`\n${BASELINE_VERSION_45} regression FAILED.`);
    process.exit(1);
  }

  console.log(
    `\nAll ${EXPECTED_ACTIVE_COUNT_45} Active/Live public-full baseline checks passed. writePerformed=false.`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
