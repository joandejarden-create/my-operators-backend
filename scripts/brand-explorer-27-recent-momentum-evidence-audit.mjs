#!/usr/bin/env node
/**
 * Audit + optional apply: Recent Momentum / Openings evidence quality (27-wave).
 */
import "../load-env.js";
import {
  AUDIT_VERSION,
  REQUIRED_APPLY_FLAGS,
  planEvidenceAudit,
  applyEvidenceFixes,
  writeEvidenceAuditReports,
  resolveTargetBrands,
} from "../lib/partner-intelligence/brand-explorer-27-recent-momentum-evidence-audit.js";

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
    `[${AUDIT_VERSION}] brands=${opts.brands.join(",")} mode=${opts.apply ? "APPLY" : "dry-run"}`
  );

  const plan = await planEvidenceAudit({ brands: opts.brands });
  for (const b of plan.brandResults) {
    console.log(
      `  ${b.brandSlug}: beforePass=${b.before.pass} fails=${b.before.failures.length} patches=${b.patches.length} cala=${b.calaAvailable}`
    );
    for (const f of b.before.failures.slice(0, 6)) {
      console.log(`    fail: ${f.id}${f.title ? ` · ${f.title}` : ""}`);
    }
  }

  let applyResult = null;
  if (opts.apply) {
    applyResult = await applyEvidenceFixes({ plan, apply: true, argv: opts.argv });
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

  const paths = writeEvidenceAuditReports(plan, applyResult);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.openingsMd}`);
  console.log(`Wrote ${paths.fixesMd}`);
  console.log(`Wrote ${paths.docsPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
