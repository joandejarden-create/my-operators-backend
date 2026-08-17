#!/usr/bin/env node
/**
 * Regression lock for protected 24 Active/Live public-full baseline.
 *
 *   npm run test:brand-explorer-24-active-public-full-baseline
 *
 * Requires:
 * - reports/brand-explorer-24-active-public-full-baseline.json (freeze artifact)
 * - reports/brand-explorer-24-tab-section-quality-audit.json (latest quality audit)
 *
 * Live-checks: Active/Live universe, exclusions, PVQL for all public-full brands,
 * protected Brand Status / Company Validated vs freeze.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import {
  BASELINE_VERSION,
  EXPECTED_ACTIVE_COUNT,
  REPORT_JSON,
  ROOT,
  run24ActivePublicFullBaselineRegression,
} from "../lib/partner-intelligence/brand-explorer-24-active-public-full-baseline.js";

async function main() {
  const argv = process.argv.slice(2);
  const skipPvql = argv.includes("--skip-pvql");

  console.log(`[${BASELINE_VERSION}] regression test (expected Active/Live=${EXPECTED_ACTIVE_COUNT})`);
  if (skipPvql) console.log("  WARN: --skip-pvql set (not for CI acceptance)");

  const frozenPath = path.join(ROOT, "reports", REPORT_JSON);
  if (!fs.existsSync(frozenPath)) {
    console.error(`[FAIL] missing freeze artifact ${REPORT_JSON}`);
    console.error("  Run: npm run brand-explorer-24-active-public-full-baseline -- --dry-run");
    process.exit(1);
  }

  const result = await run24ActivePublicFullBaselineRegression({
    reassessPvql: !skipPvql,
    requireQualityReport: true,
    forceLivePvql: argv.includes("--force-live-pvql"),
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
        `[PASS] ${c.slug} status=${c.liveBrandStatus} pvql=${c.pvqlPass} quality=${c.qualityRecommendation}`
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
    console.error(`\n${BASELINE_VERSION} regression FAILED.`);
    process.exit(1);
  }

  console.log(
    `\nAll ${EXPECTED_ACTIVE_COUNT} Active/Live baseline checks passed. writePerformed=false.`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
