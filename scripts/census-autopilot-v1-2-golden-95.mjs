#!/usr/bin/env node
/**
 * Census Autopilot V1.2 — Golden Census 95% completion program
 *
 * npm run census:autopilot-v1-2-golden-95
 *
 * No Webhound. No paid credits. No Airtable writes.
 */

import { runGolden95Benchmark } from "../lib/research-engine-v2/census-autopilot-v1/golden/golden-orchestrator.js";

function parseArgs(argv) {
  const out = {
    delayMs: 300,
    concurrency: 3,
    maxRecords: null,
    pass2Limit: null,
    pass3Limit: 120,
    timeoutMs: 25000,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    if (a === "--delay-ms" || a.startsWith("--delay-ms=")) {
      out.delayMs = Number(a.includes("=") ? a.split("=")[1] : next());
    } else if (a === "--concurrency" || a.startsWith("--concurrency=")) {
      out.concurrency = Number(a.includes("=") ? a.split("=")[1] : next());
    } else if (a === "--max-records" || a.startsWith("--max-records=")) {
      out.maxRecords = Number(a.includes("=") ? a.split("=")[1] : next());
    } else if (a === "--pass2-limit" || a.startsWith("--pass2-limit=")) {
      out.pass2Limit = Number(a.includes("=") ? a.split("=")[1] : next());
    } else if (a === "--pass3-limit" || a.startsWith("--pass3-limit=")) {
      out.pass3Limit = Number(a.includes("=") ? a.split("=")[1] : next());
    } else if (a === "--timeout-ms" || a.startsWith("--timeout-ms=")) {
      out.timeoutMs = Number(a.includes("=") ? a.split("=")[1] : next());
    } else if (a === "--help" || a === "-h") {
      out.help = true;
    }
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log(`Usage: npm run census:autopilot-v1-2-golden-95 -- [options]
  --delay-ms=300
  --concurrency=3
  --max-records=N
  --pass2-limit=N
  --pass3-limit=120`);
    return;
  }

  const result = await runGolden95Benchmark({
    delayMs: args.delayMs,
    concurrency: args.concurrency,
    maxRecords: args.maxRecords,
    pass2Limit: args.pass2Limit ?? undefined,
    pass3Limit: args.pass3Limit,
    timeoutMs: args.timeoutMs,
    log: console.log,
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        run_id: result.run_id,
        hotels: result.hotels,
        baseline_avg: result.baseline.average_raw_priority_completeness_pct,
        final_avg: result.final.average_raw_priority_completeness_pct,
        hotels_ge_95_pct: result.final.hotels_at_or_above_95_share_pct,
        rooms_pct: result.rooms_completion_pct,
        artifact_root: result.artifact_root,
        cost_usd: 0,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
