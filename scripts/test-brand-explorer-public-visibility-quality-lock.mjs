#!/usr/bin/env node
/**
 * Public Visibility Quality Lock — read-only test/reporting gate.
 * Covers every Brand Explorer profile that can render full tabs externally.
 */
import "dotenv/config";
import {
  runPublicVisibilityQualityLock,
  writePublicVisibilityQualityLockReports,
  PUBLIC_VISIBILITY_QUALITY_LOCK_VERSION,
} from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : null;
  return {
    brands,
    /** Stabilization mode: evaluate only public-full profiles; fail if any fail. */
    publicFullOnly: argv.includes("--public-full-only"),
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${PUBLIC_VISIBILITY_QUALITY_LOCK_VERSION}] read-only public visibility quality lock`);

  const report = await runPublicVisibilityQualityLock({ slugs: opts.brands });
  const paths = writePublicVisibilityQualityLockReports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.primaryPath}`);
  console.log(`Wrote ${paths.legacyPath}`);
  console.log(`Wrote ${paths.hiddenPath}`);

  const s = report.summary;
  console.log(
    `Summary: overallPass=${s.overallPass} publicFull=${s.publicFullProfileCount} primaryPass=${s.primaryPass} legacyPass=${s.legacyPass} legacyFlagged=${s.legacyFlaggedCount} lockedOk=${s.lockedRemainLocked} cvUntouched=${s.companyValidatedUntouched}`
  );
  if (s.hardFails?.length) console.log(`Hard fails: ${s.hardFails.join("; ")}`);

  const publicFull = (report.brands || []).filter((b) => b.publicFullProfile === true);
  for (const b of report.brands || []) {
    if (opts.publicFullOnly && !b.publicFullProfile) continue;
    if (!opts.publicFullOnly && !b.inPublicVisibilityLockScope && b.cohort !== "restored_legacy_public") {
      continue;
    }
    const status = b.lockPass
      ? "PASS"
      : !opts.publicFullOnly && b.cohort === "restored_legacy_public"
        ? "FLAGGED"
        : "FAIL";
    console.log(
      `  [${status}] ${b.slug} cohort=${b.cohort} full=${b.publicFullProfile} failures=${(b.failures || []).join(",") || "—"}`
    );
  }

  if (opts.publicFullOnly) {
    const allPublicPass = publicFull.every((b) => b.lockPass === true);
    console.log(
      `Public-full-only: count=${publicFull.length} allPass=${allPublicPass}`
    );
    if (!allPublicPass) process.exit(1);
    return;
  }

  // Exit non-zero only on hard fails (primary fail, coverage gap, lock leak, CV change).
  // Legacy failures are explicit flags and do not alone fail the process.
  if (!report.summary.overallPass) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
