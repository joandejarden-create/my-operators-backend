#!/usr/bin/env node
/**
 * Final Public Restore Readiness Audit — read-only dry-run by default.
 * Does not apply public restore or write release / CV / Source / Registry fields.
 */
import "dotenv/config";
import {
  READINESS_VERSION,
  ALL_RESTORE_CANDIDATES,
  resolveRestoreCandidateList,
  runFinalPublicRestoreReadiness,
} from "../lib/partner-intelligence/brand-explorer-final-public-restore-readiness.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const raw =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...ALL_RESTORE_CANDIDATES];
  if (argv.includes("--apply")) {
    throw new Error(
      "This audit is read-only. Do not pass --apply. Use brand-explorer-public-restore-governance after readiness passes."
    );
  }
  return {
    brands: resolveRestoreCandidateList(raw),
    dryRun: true,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${READINESS_VERSION}] Final public restore readiness (read-only)`);
  console.log(`  brands (${opts.brands.length}): ${opts.brands.join(", ")}`);
  console.log("  guardrails: no restore · no release · no CV/source/registry · baseline untouched");

  const result = await runFinalPublicRestoreReadiness({
    brands: opts.brands,
    dryRun: true,
  });

  console.log(`Wrote ${result.paths.jsonPath}`);
  console.log(`Wrote ${result.paths.mdPath}`);
  console.log(`Wrote ${result.paths.lane1Path}`);
  console.log(`Wrote ${result.paths.lane2Path}`);
  console.log(
    `Summary: ready=${result.summary.readyCount}/${result.summary.brandCount} acceptance=${result.acceptance.pass}`
  );
  for (const b of result.brandResults) {
    console.log(
      `  ${b.brandSlug}: ${b.recommendation} gates=${b.gateSuitePass} visibility=${b.visibility?.publicVisibility || "—"}` +
        ((b.failures || []).length ? ` fails=${(b.failures || []).slice(0, 3).join(",")}` : "")
    );
  }

  if (!result.acceptance.pass) {
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
