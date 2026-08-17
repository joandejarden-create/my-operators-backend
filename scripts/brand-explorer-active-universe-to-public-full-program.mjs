#!/usr/bin/env node
/**
 * Active Universe → Public-Full Program CLI.
 */
import "dotenv/config";
import {
  PROGRAM_VERSION,
  PROGRAM_LANES,
  parseProgramArgs,
  runActiveUniverseToPublicFullProgram,
  writeProgramReports,
} from "../lib/partner-intelligence/brand-explorer-active-universe-to-public-full-program.js";

async function main() {
  const opts = parseProgramArgs(process.argv.slice(2));
  if (opts.lane === null && process.argv.includes("--lane")) {
    console.error(`Unknown --lane. Valid: ${PROGRAM_LANES.join(", ")}`);
    process.exit(2);
  }

  console.log(`[${PROGRAM_VERSION}]`);
  console.log(
    `Lanes: ${opts.lane || "ALL"} | mode=${opts.apply || opts.applyPublicRestore ? "APPLY" : "dry-run"}`
  );

  const report = await runActiveUniverseToPublicFullProgram({
    dryRun: opts.dryRun,
    apply: opts.apply,
    applyPublicRestore: opts.applyPublicRestore,
    lane: opts.lane,
    argv: opts.argv,
  });
  const paths = writeProgramReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docPath}`);

  for (const lr of report.laneResults || []) {
    const applied = lr.applyResult?.applied;
    console.log(
      `  [${lr.lane}] targets=${(lr.targets || []).length} applied=${applied === true ? "yes" : "no"} ${lr.applyResult?.reason || ""}`
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
