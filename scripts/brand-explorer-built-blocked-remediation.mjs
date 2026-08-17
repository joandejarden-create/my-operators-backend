#!/usr/bin/env node
/**
 * Built-but-Blocked Tab Factory Remediation — dry-run by default; --apply with required flags.
 * Field-level patches for 7 content_remediation_needed brands only.
 */
import "dotenv/config";
import {
  REMEDIATION_VERSION,
  BUILT_BLOCKED_TARGETS,
  BUILT_BLOCKED_WAVE1,
  BUILT_BLOCKED_WAVE2,
  parseBuiltBlockedApplyFlags,
  planBuiltBlockedRemediation,
  applyBuiltBlockedRemediation,
  writeBuiltBlockedReports,
  verifyBuiltBlockedBrand,
} from "../lib/partner-intelligence/brand-explorer-built-blocked-remediation.js";

/** Accept short CLI aliases (country → country-inn-suites, etc.). */
const SLUG_ALIASES = Object.freeze({
  country: "country-inn-suites",
  "country-inn": "country-inn-suites",
  "country-inn-suites": "country-inn-suites",
  "quality-inn": "quality-inn",
  quality: "quality-inn",
  radisson: "radisson",
  "radisson-blu": "radisson-blu",
  blu: "radisson-blu",
  "radisson-red": "radisson-red",
  red: "radisson-red",
  suburban: "suburban-studios",
  "suburban-studios": "suburban-studios",
  woodspring: "woodspring-suites",
  "woodspring-suites": "woodspring-suites",
  wave1: "__wave1__",
  wave2: "__wave2__",
});

function resolveBrands(rawList) {
  if (!rawList?.length) return [...BUILT_BLOCKED_TARGETS];
  const out = [];
  for (const raw of rawList) {
    const key = String(raw).trim().toLowerCase();
    const mapped = SLUG_ALIASES[key] || key;
    if (mapped === "__wave1__") {
      out.push(...BUILT_BLOCKED_WAVE1);
      continue;
    }
    if (mapped === "__wave2__") {
      out.push(...BUILT_BLOCKED_WAVE2);
      continue;
    }
    out.push(mapped);
  }
  return [...new Set(out)];
}

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
    brands: resolveBrands(brands),
    apply: argv.includes("--apply"),
    verifyOnly: argv.includes("--verify-only"),
    dryRun: argv.includes("--dry-run") || !argv.includes("--apply"),
    argv,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${REMEDIATION_VERSION}] built-but-blocked remediation`);
  console.log(`Brands: ${opts.brands.join(", ")}`);

  if (opts.verifyOnly) {
    let allOk = true;
    for (const slug of opts.brands) {
      const v = await verifyBuiltBlockedBrand(slug);
      const ok = v.contentReady === true;
      if (!ok) allOk = false;
      console.log(
        `  [${ok ? "PASS" : "FAIL"}] ${slug} fails=${v.failFindings} empty=${v.empty} uniq=${v.uniquenessPass} role=${v.rolePass} g/s/p=${v.gallery}/${v.scenario}/${v.property} contentReady=${v.contentReady} fullyReady=${v.fullyReady}`
      );
    }
    if (!allOk) process.exit(1);
    return;
  }

  const report = await planBuiltBlockedRemediation({ brands: opts.brands });
  let applyResult = null;
  if (opts.apply) {
    const flags = parseBuiltBlockedApplyFlags(opts.argv);
    if (!flags.ok) {
      console.error(`Missing apply flags: ${(flags.missing || []).join(", ")}`);
      process.exit(1);
    }
    applyResult = await applyBuiltBlockedRemediation({
      report,
      apply: true,
      argv: opts.argv,
    });
    console.log(
      `Apply: applied=${applyResult.applied} reason=${applyResult.reason || "ok"} results=${applyResult.results?.length || 0}`
    );
    if (!applyResult.applied) process.exit(1);
  }

  const paths = writeBuiltBlockedReports(report, applyResult);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docPath}`);
  console.log(
    `Summary: brands=${report.summary.brandCount} patches=${report.summary.patchCount} blocked=${report.summary.blockedCount} validation=${report.validation.pass} dryRun=${!applyResult?.applied}`
  );
  for (const b of report.brands || []) {
    console.log(
      `  wave${b.wave} ${b.brandSlug}: patches=${(b.patches || []).length} beforeFails=${b.before?.failFindings} defects=${(b.defectTable || []).length} blocked=${b.blocked} imageBlockers=${(b.imageBlockers || []).length}`
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
