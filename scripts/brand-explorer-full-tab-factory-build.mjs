#!/usr/bin/env node
/**
 * Full Tab Factory Presentation build (dry-run by default).
 *
 *   npm run brand-explorer-full-tab-factory-build -- --brands dazzler-by-wyndham
 *   npm run brand-explorer-full-tab-factory-build -- --brands dazzler-by-wyndham --apply \
 *     --approve-full-tab-factory-build \
 *     --confirm-no-company-validation-changes \
 *     --confirm-no-source-library-status-changes \
 *     --confirm-no-registry-approval-changes \
 *     --confirm-no-release-field-writes \
 *     --confirm-tab-factory-contracts \
 *     --confirm-source-provenance-by-tab \
 *     --confirm-image-uniqueness \
 *     --confirm-image-role-match \
 *     --confirm-section-pattern-parity
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  FULL_BUILD_VERSION,
  FULL_BUILD_REQUIRED_APPLY_FLAGS,
  planFullTabFactoryBuild,
  applyFullTabFactoryBuild,
  writeFullBuildBrandReport,
} from "../lib/partner-intelligence/brand-explorer-full-tab-factory-build.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS_DIR = path.join(ROOT, "reports");

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [];
  return {
    brands,
    apply: argv.includes("--apply"),
    argv,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (!opts.brands.length) {
    console.error("Usage: --brands <slug[,slug…]> [--apply + required flags]");
    process.exit(1);
  }

  console.log(`[${FULL_BUILD_VERSION}] brands=${opts.brands.join(",")} mode=${opts.apply ? "APPLY" : "dry-run"}`);
  const plan = await planFullTabFactoryBuild({ brands: opts.brands, reportsDir: REPORTS_DIR });

  for (const brand of plan.brandResults || []) {
    writeFullBuildBrandReport(brand, { reportsDir: REPORTS_DIR });
    console.log(
      `  ${brand.brandSlug}: existing=${brand.existingPresentationCount ?? 0} planned=${brand.patches?.length ?? 0} blocked=${brand.blocked === true}`
    );
    if (brand.blockers?.length) {
      for (const b of brand.blockers.slice(0, 12)) console.log(`    blocker: ${b}`);
      if (brand.blockers.length > 12) console.log(`    … +${brand.blockers.length - 12} more`);
    }
  }

  let applyResult = null;
  if (opts.apply) {
    applyResult = await applyFullTabFactoryBuild({
      plan,
      apply: true,
      argv: opts.argv,
    });
    if (!applyResult.applied) {
      console.error(`Apply refused: ${applyResult.reason}`);
      if (applyResult.missing?.length) {
        for (const f of applyResult.missing) console.error(`  missing ${f}`);
      }
      process.exitCode = 1;
    } else {
      for (const [slug, row] of Object.entries(applyResult.resultsByBrand || {})) {
        console.log(
          `  apply ${slug}: created=${row.created?.length || 0} updated=${row.updated?.length || 0} errors=${row.errors?.length || 0}`
        );
        if (row.errors?.length) {
          for (const e of row.errors.slice(0, 8)) console.error(`    ${e.slotKey}: ${e.error}`);
          process.exitCode = 1;
        }
      }
    }
  } else {
    console.log(
      `Dry-run plannedWrites=${plan.summary.plannedWriteCount} post=${plan.summary.postCount} patch=${plan.summary.patchCount}`
    );
    console.log("Apply not performed — pass --apply + required flags.");
  }

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const outPath = path.join(REPORTS_DIR, "brand-explorer-full-tab-factory-build-last-run.json");
  fs.writeFileSync(
    outPath,
    `${JSON.stringify({ ...plan, applyResult, dryRun: !opts.apply }, null, 2)}\n`,
    "utf8"
  );
  console.log(`Wrote ${outPath}`);
  console.log("Required apply flags:");
  for (const f of FULL_BUILD_REQUIRED_APPLY_FLAGS) console.log(`  ${f}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
