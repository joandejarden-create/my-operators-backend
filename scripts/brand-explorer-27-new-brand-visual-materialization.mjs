#!/usr/bin/env node
/**
 * Visual / gallery materialization for Tapestry, Dazzler, Trademark.
 *
 *   npm run brand-explorer-27-new-brand-visual-materialization -- --brands tapestry-collection-by-hilton,dazzler-by-wyndham,trademark-collection-by-wyndham --dry-run
 *
 *   npm run brand-explorer-27-new-brand-visual-materialization -- --brands tapestry-collection-by-hilton,dazzler-by-wyndham,trademark-collection-by-wyndham --apply \
 *     --approve-visual-materialization \
 *     --confirm-target-brands-only \
 *     --confirm-no-company-validation-changes \
 *     --confirm-no-source-library-status-changes \
 *     --confirm-no-registry-approval-changes \
 *     --confirm-no-brand-status-changes \
 *     --confirm-no-release-field-changes \
 *     --confirm-no-protected-24-brand-changes \
 *     --confirm-image-uniqueness \
 *     --confirm-image-role-match \
 *     --confirm-no-logo-only-filler \
 *     --confirm-no-wrong-brand-images
 */
import "../load-env.js";
import {
  VISUAL_MAT_VERSION,
  REQUIRED_APPLY_FLAGS,
  planVisualMaterialization,
  applyVisualMaterialization,
  writeVisualMaterializationReports,
  resolveTargetBrands,
} from "../lib/partner-intelligence/brand-explorer-27-new-brand-visual-materialization.js";

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
    `[${VISUAL_MAT_VERSION}] brands=${opts.brands.join(",")} mode=${opts.apply ? "APPLY" : "dry-run"}`
  );

  const plan = await planVisualMaterialization({ brands: opts.brands });
  for (const b of plan.brandResults) {
    console.log(
      `  ${b.brandSlug}: asset=${b.assetPack.status} imagePatches=${b.imagePlan.presentationPatches?.length || 0} displayPatches=${b.displayGatePatches?.length || 0} blocked=${b.blocked} uniq=${b.before.uniquenessPass} external=${b.before.externalOwnerPass}`
    );
    if (b.blockers?.length) {
      for (const x of b.blockers.slice(0, 8)) console.log(`    blocker: ${x}`);
    }
  }

  let applyResult = null;
  if (opts.apply) {
    applyResult = await applyVisualMaterialization({
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
      for (const [slug, row] of Object.entries(applyResult.imageApply?.resultsByBrand || {})) {
        const created = row.presentationCreated?.length || row.results?.presentationCreated?.length || 0;
        const updated = row.presentationUpdated?.length || row.results?.presentationUpdated?.length || 0;
        const errors = row.errors?.length || row.results?.errors?.length || 0;
        console.log(`  image ${slug}: created=${created} updated=${updated} errors=${errors}`);
        if (errors) process.exitCode = 1;
      }
      for (const [slug, row] of Object.entries(applyResult.displayResults || {})) {
        console.log(
          `  display ${slug}: updated=${row.updated?.length || 0} errors=${row.errors?.length || 0}`
        );
        if (row.errors?.length) process.exitCode = 1;
      }
    }
  } else {
    console.log("Dry-run only — pass --apply + required flags to write.");
  }

  const paths = writeVisualMaterializationReports(plan, applyResult);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  for (const p of Object.values(paths.perBrand || {})) console.log(`Wrote ${p}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
