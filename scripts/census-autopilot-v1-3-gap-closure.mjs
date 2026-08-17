#!/usr/bin/env node
/**
 * Census Autopilot V1.3 — Golden Census gap closure (Rooms + Address + Coords)
 * npm run census:autopilot-v1-3-gap-closure
 */

import { runGapClosureV13 } from "../lib/research-engine-v2/census-autopilot-v1/golden-gap-v13/orchestrator.js";

function parseArgs(argv) {
  const out = { delayMs: 200, concurrency: 3, maxRecords: null, pass3Limit: 80, skipGeocode: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    if (a.startsWith("--delay-ms=")) out.delayMs = Number(a.split("=")[1]);
    else if (a === "--delay-ms") out.delayMs = Number(next());
    else if (a.startsWith("--concurrency=")) out.concurrency = Number(a.split("=")[1]);
    else if (a === "--concurrency") out.concurrency = Number(next());
    else if (a.startsWith("--max-records=")) out.maxRecords = Number(a.split("=")[1]);
    else if (a === "--max-records") out.maxRecords = Number(next());
    else if (a.startsWith("--pass3-limit=")) out.pass3Limit = Number(a.split("=")[1]);
    else if (a === "--skip-geocode") out.skipGeocode = true;
    else if (a === "--help" || a === "-h") out.help = true;
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log("Usage: npm run census:autopilot-v1-3-gap-closure -- [--max-records=N] [--skip-geocode]");
    return;
  }
  const result = await runGapClosureV13({
    delayMs: args.delayMs,
    concurrency: args.concurrency,
    maxRecords: args.maxRecords,
    pass3Limit: args.pass3Limit,
    skipGeocode: args.skipGeocode,
    log: console.log,
  });
  console.log(
    JSON.stringify(
      {
        ok: true,
        run_id: result.run_id,
        final_avg: result.final.average_raw_priority_completeness_pct,
        hotels_ge_95_pct: result.final.hotels_at_or_above_95_share_pct,
        rooms_pct: result.rooms_pct,
        address_pct: result.address_pct,
        lat_pct: result.lat_pct,
        geocode_calls: result.geocode_calls,
        cost_usd: result.external_cost_usd,
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
