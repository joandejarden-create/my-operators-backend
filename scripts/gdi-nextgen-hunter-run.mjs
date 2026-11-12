#!/usr/bin/env node
/**
 * Unified Node entry for specialist hunters (PowerShell wrappers call this).
 * Default: dry-run, cache-first, $0 paid.
 *
 * Usage:
 *   node scripts/gdi-nextgen-hunter-run.mjs --hunter government_procurement --hotel-id rec… --dry-run
 */

import path from "node:path";
import { fileURLToPath } from "node:url";
import { getHunterRunner, listImplementedHunterIds } from "../lib/group-demand-intelligence/nextgen-discovery/hunters/index.js";
import { parseHunterCliArgs } from "../lib/group-demand-intelligence/nextgen-discovery/hunter-base-v1.js";
import { isSpecialistHunterId } from "../lib/group-demand-intelligence/nextgen-discovery/specialist-hunter-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function parseArgv(argv) {
  const base = parseHunterCliArgs(argv);
  let hunter = "";
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === "--hunter" && argv[i + 1]) {
      hunter = argv[i + 1];
      i += 1;
    }
  }
  return { ...base, hunter };
}

async function main() {
  const args = parseArgv(process.argv.slice(2));
  if (!args.hunter || !isSpecialistHunterId(args.hunter)) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          error: " --hunter <id> required",
          implemented: listImplementedHunterIds(),
        },
        null,
        2
      )
    );
    process.exit(2);
  }
  const runner = getHunterRunner(args.hunter);
  if (!runner) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          error: `hunter not implemented yet: ${args.hunter}`,
          implemented: listImplementedHunterIds(),
        },
        null,
        2
      )
    );
    process.exit(3);
  }

  const reportDir =
    args.reportDir ||
    path.join(
      ROOT,
      "reports/gdi-nextgen-discovery-architecture-v1/hunter-runs",
      args.hunter
    );

  const report = await runner.run({
    hotelId: args.hotelId,
    hotelName: args.hotelName,
    dryRun: args.dryRun !== false,
    cacheFirst: args.cacheFirst !== false,
    budgetUsd: 0,
    reportDir,
    rootDir: ROOT,
  });

  console.log(
    JSON.stringify(
      {
        ok: report.safety?.ok !== false,
        hunterId: report.hunterId,
        hotelId: report.hotelId,
        metrics: report.metrics,
        paidAuthorized: report.paidAuthorized,
        reportPath: report.reportPath || null,
        rejected: (report.rejected || []).map((r) => ({
          organizationName: r.organizationName,
          rejectReason: r.rejectReason,
        })),
      },
      null,
      2
    )
  );
  process.exit(report.safety?.ok === false ? 1 : 0);
}

main().catch((err) => {
  console.error(JSON.stringify({ ok: false, error: String(err?.stack || err) }));
  process.exit(1);
});
