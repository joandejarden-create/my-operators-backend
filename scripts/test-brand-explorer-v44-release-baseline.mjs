#!/usr/bin/env node
/**
 * v44 regression test — fails if released brands regress or incompletes unlock.
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  V44_DEFAULT_BRANDS,
  V44_RELEASED_GOLDEN_SLUGS,
  V44_INCOMPLETE_ROUTED_SLUGS,
  REPORT_JSON,
  ROOT,
  runV44ReleaseBaseline,
  evaluateV44Regression,
} from "../lib/partner-intelligence/brand-explorer-v44-release-baseline.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...V44_DEFAULT_BRANDS];
  return { brands };
}

async function main() {
  const { brands } = parseArgs(process.argv.slice(2));
  const report = await runV44ReleaseBaseline({ brands, dryRun: true });
  const regression = evaluateV44Regression(report.snapshots);

  let failed = 0;

  for (const slug of brands.filter((s) => V44_RELEASED_GOLDEN_SLUGS.includes(s))) {
    const check = regression.checks.find((c) => c.brandSlug === slug);
    if (!check?.pass) {
      failed += 1;
      console.log(`[FAIL] ${slug} (released)`);
      for (const f of check?.failures || ["missing_check"]) console.log(`  - ${f}`);
    } else {
      console.log(`[PASS] ${slug} (released baseline intact)`);
    }
  }

  for (const slug of brands.filter((s) => V44_INCOMPLETE_ROUTED_SLUGS.includes(s))) {
    const check = regression.checks.find((c) => c.brandSlug === slug);
    if (!check?.pass) {
      failed += 1;
      console.log(`[FAIL] ${slug} (incomplete)`);
      for (const f of check?.failures || ["missing_check"]) console.log(`  - ${f}`);
    } else {
      console.log(
        `[PASS] ${slug} (locked · action=${check.liveAction})`
      );
    }
  }

  const baselinePath = path.join(ROOT, "reports", REPORT_JSON);
  if (fs.existsSync(baselinePath)) {
    try {
      const frozen = JSON.parse(fs.readFileSync(baselinePath, "utf8"));
      if (frozen?.summary?.regressionPass === false) {
        failed += 1;
        console.log("[FAIL] committed baseline report marks regressionPass=false");
      } else {
        console.log("[PASS] committed baseline report present");
      }
    } catch (err) {
      failed += 1;
      console.log(`[FAIL] baseline report unreadable: ${err.message}`);
    }
  } else {
    console.log(
      `[WARN] ${REPORT_JSON} missing — run npm run brand-explorer-v44-release-baseline -- --dry-run first`
    );
  }

  if (failed) {
    console.error(`\n${failed} v44 baseline check(s) failed.`);
    process.exit(1);
  }
  console.log(`\nAll v44 release-baseline checks passed (${brands.length} brand(s)).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
