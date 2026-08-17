#!/usr/bin/env node
/**
 * Census Autopilot V1.1 — Live deep-research Mexico benchmark
 *
 * npm run census:autopilot-v1-1-deep-mexico -- --dry-run
 *
 * No Webhound. No paid credits. No Airtable writes.
 */

import { runDeepMexicoBenchmark } from "../lib/research-engine-v2/census-autopilot-v1/deep-mexico-orchestrator.js";

function parseArgs(argv) {
  const out = {
    delayMs: 350,
    concurrency: 3,
    maxRecords: null,
    resume: null,
    cventSampleLimit: 100,
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
    } else if (a === "--resume" || a.startsWith("--resume=")) {
      out.resume = a.includes("=") ? a.split("=").slice(1).join("=") : next();
    } else if (a === "--cvent-sample-limit" || a.startsWith("--cvent-sample-limit=")) {
      out.cventSampleLimit = Number(a.includes("=") ? a.split("=")[1] : next());
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
    console.log(`Usage: npm run census:autopilot-v1-1-deep-mexico -- [options]
  --delay-ms=350
  --concurrency=3
  --max-records=N
  --resume <run_id>
  --cvent-sample-limit=100`);
    return;
  }

  const result = await runDeepMexicoBenchmark({
    delayMs: args.delayMs,
    concurrency: args.concurrency,
    maxRecords: args.maxRecords,
    resume: args.resume,
    cventSampleLimit: args.cventSampleLimit,
    timeoutMs: args.timeoutMs,
    log: console.log,
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        run_id: result.run_id,
        researched: result.observability.hotels_deeply_researched,
        material_after: result.observability.material_completeness_avg_after,
        production_candidates: result.observability.production_candidates,
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
  process.exitCode = 1;
});
