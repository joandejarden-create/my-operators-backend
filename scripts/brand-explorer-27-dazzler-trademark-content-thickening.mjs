#!/usr/bin/env node
/**
 * Dazzler + Trademark content thickness cleanup for 27-brand PVQL.
 *
 *   npm run brand-explorer-27-dazzler-trademark-content-thickening -- --brands dazzler-by-wyndham,trademark-collection-by-wyndham --dry-run
 *
 *   npm run brand-explorer-27-dazzler-trademark-content-thickening -- --brands dazzler-by-wyndham,trademark-collection-by-wyndham --apply \
 *     --approve-content-thickening \
 *     --confirm-target-brands-only \
 *     --confirm-targeted-field-fixes-only \
 *     --confirm-no-company-validation-changes \
 *     --confirm-no-source-library-status-changes \
 *     --confirm-no-registry-approval-changes \
 *     --confirm-no-brand-status-changes \
 *     --confirm-no-release-field-changes \
 *     --confirm-no-image-writes \
 *     --confirm-no-protected-24-brand-changes \
 *     --confirm-no-tapestry-changes \
 *     --confirm-no-broad-rewrites
 */
import "../load-env.js";
import {
  CONTENT_THICKEN_VERSION,
  REQUIRED_APPLY_FLAGS,
  planContentThickening,
  applyContentThickening,
  writeContentThickeningReports,
  resolveTargetBrands,
} from "../lib/partner-intelligence/brand-explorer-27-dazzler-trademark-content-thickening.js";

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
    brands: resolveTargetBrands(brands),
    apply: argv.includes("--apply"),
    argv,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(
    `[${CONTENT_THICKEN_VERSION}] brands=${opts.brands.join(",")} mode=${opts.apply ? "APPLY" : "dry-run"}`
  );

  const plan = await planContentThickening({ brands: opts.brands });
  for (const b of plan.brandResults) {
    console.log(
      `  ${b.brandSlug}: patches=${b.patches.length} basics=${b.counts.basicsPatches} momentumCreates=${b.counts.momentumCreates} blocked=${b.blocked}`
    );
    if (b.blockers?.length) {
      for (const x of b.blockers.slice(0, 8)) console.log(`    blocker: ${x}`);
    }
    for (const p of b.patches.slice(0, 12)) {
      console.log(`    ${p.action} ${p.slotKey || "basics"} · ${p.reason}`);
    }
    if (b.patches.length > 12) console.log(`    … +${b.patches.length - 12} more`);
  }

  let applyResult = null;
  if (opts.apply) {
    applyResult = await applyContentThickening({
      plan,
      apply: true,
      argv: opts.argv,
    });
    if (!applyResult.applied) {
      console.error(`Apply refused: ${applyResult.reason}`);
      if (applyResult.missing?.length) {
        for (const f of applyResult.missing) console.error(`  missing ${f}`);
      }
      console.error("Required:");
      for (const f of REQUIRED_APPLY_FLAGS) console.error(`  ${f}`);
      process.exitCode = 1;
    } else {
      console.log("Apply completed.");
      for (const [slug, row] of Object.entries(applyResult.resultsByBrand || {})) {
        console.log(
          `  ${slug}: created=${row.created?.length || 0} updated=${row.updated?.length || 0} errors=${row.errors?.length || 0}`
        );
        if (row.errors?.length) process.exitCode = 1;
      }
    }
  } else {
    console.log("Dry-run only — pass --apply + required flags to write.");
  }

  const paths = writeContentThickeningReports(plan, applyResult);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  for (const p of Object.values(paths.perBrand || {})) console.log(`Wrote ${p}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
