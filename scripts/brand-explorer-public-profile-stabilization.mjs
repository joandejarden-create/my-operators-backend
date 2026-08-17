#!/usr/bin/env node
/**
 * Public Profile Stabilization — dry-run by default; --apply with required flags.
 * Fixes only public-full profiles that fail PVQL (not locked / founder-preview).
 */
import "dotenv/config";
import {
  STABILIZATION_VERSION,
  PUBLIC_STABILIZATION_TARGETS,
  PUBLIC_STABILIZATION_PRIMARY_TARGETS,
  parseStabilizationApplyFlags,
  planPublicProfileStabilization,
  applyPublicProfileStabilization,
  writeStabilizationReports,
  freezePublicVisibilityBaseline,
  verifyPublicProfileStabilization,
} from "../lib/partner-intelligence/brand-explorer-public-profile-stabilization.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : null;
  const primaryOnly = argv.includes("--primary-only");
  const freezeBaseline = argv.includes("--freeze-baseline");
  const verifyOnly = argv.includes("--verify-only");
  return {
    brands: brands
      ? brands
      : primaryOnly
        ? [...PUBLIC_STABILIZATION_PRIMARY_TARGETS]
        : [...PUBLIC_STABILIZATION_TARGETS],
    apply: argv.includes("--apply"),
    freezeBaseline,
    verifyOnly,
    argv,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${STABILIZATION_VERSION}] public profile stabilization`);

  if (opts.verifyOnly || opts.freezeBaseline) {
    const verification = await verifyPublicProfileStabilization(opts.brands);
    console.log(`Verify allPass=${verification.allPass}`);
    for (const b of verification.brands) {
      console.log(
        `  [${b.lockPass ? "PASS" : "FAIL"}] ${b.slug} ${(b.failures || []).join(",") || "—"}`
      );
    }
    if (opts.freezeBaseline) {
      const frozen = await freezePublicVisibilityBaseline({ publicFullSlugs: opts.brands });
      console.log(`Baseline freezeReady=${frozen.baseline.freezeReady}`);
      console.log(`Wrote ${frozen.jsonPath}`);
      console.log(`Wrote ${frozen.mdPath}`);
      if (!frozen.baseline.freezeReady) process.exit(1);
    } else if (!verification.allPass) {
      process.exit(1);
    }
    return;
  }

  const report = await planPublicProfileStabilization({ brands: opts.brands });
  let applyResult = null;
  if (opts.apply) {
    const flags = parseStabilizationApplyFlags(opts.argv);
    if (!flags.ok) {
      console.error(`Missing apply flags: ${(flags.missing || []).join(", ")}`);
      process.exit(1);
    }
    applyResult = await applyPublicProfileStabilization({
      report,
      apply: true,
      argv: opts.argv,
    });
    console.log(`Apply: applied=${applyResult.applied} reason=${applyResult.reason || "ok"} results=${applyResult.results?.length || 0}`);
    if (!applyResult.applied) process.exit(1);
  }

  const paths = writeStabilizationReports(report, applyResult);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: brands=${report.summary.brandCount} patches=${report.summary.patchCount} blocked=${report.summary.blockedCount} validation=${report.validation.pass}`
  );
  for (const b of report.brands || []) {
    console.log(
      `  ${b.brandSlug}: patches=${(b.patches || []).length} beforeFails=${b.before?.failFindings} blocked=${b.blocked}`
    );
  }
  if (!report.validation.pass && !opts.apply) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
