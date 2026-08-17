#!/usr/bin/env node
/**
 * DEALALITY CENSUS AUTOPILOT V1 CLI
 *
 * npm run census:autopilot-v1 -- --mode=unified_benchmark --group=IHG,Hilton,Choice --country=Mexico --dry-run
 *
 * No Webhound. No Airtable writes. No credit spend by default.
 */

import { runCensusAutopilotV1 } from "../lib/research-engine-v2/census-autopilot-v1/orchestrator.js";

function parseArgs(argv) {
  const out = {
    mode: "unified_benchmark",
    group: "IHG,Hilton,Choice",
    brand: null,
    country: "Mexico",
    region: null,
    priority: null,
    maxRecords: null,
    dryRun: true,
    resume: null,
    cventChallengeLimit: 400,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = argv[i + 1];
    const take = () => {
      i += 1;
      return next;
    };
    if (a === "--mode" || a.startsWith("--mode=")) {
      out.mode = a.includes("=") ? a.split("=").slice(1).join("=") : take();
    } else if (a === "--group" || a.startsWith("--group=")) {
      out.group = a.includes("=") ? a.split("=").slice(1).join("=") : take();
    } else if (a === "--brand" || a.startsWith("--brand=")) {
      out.brand = a.includes("=") ? a.split("=").slice(1).join("=") : take();
    } else if (a === "--country" || a.startsWith("--country=")) {
      out.country = a.includes("=") ? a.split("=").slice(1).join("=") : take();
    } else if (a === "--region" || a.startsWith("--region=")) {
      out.region = a.includes("=") ? a.split("=").slice(1).join("=") : take();
    } else if (a === "--priority" || a.startsWith("--priority=")) {
      out.priority = a.includes("=") ? a.split("=").slice(1).join("=") : take();
    } else if (a === "--max-records" || a.startsWith("--max-records=")) {
      out.maxRecords = Number(a.includes("=") ? a.split("=")[1] : take());
    } else if (a === "--dry-run") {
      out.dryRun = true;
    } else if (a === "--no-dry-run") {
      // V1 still refuses writes; flag accepted for future compatibility
      out.dryRun = true;
      console.warn("[autopilot-v1] writes disabled in V1 — forcing dry-run");
    } else if (a === "--resume" || a.startsWith("--resume=")) {
      out.resume = a.includes("=") ? a.split("=").slice(1).join("=") : take();
    } else if (a === "--cvent-challenge-limit" || a.startsWith("--cvent-challenge-limit=")) {
      out.cventChallengeLimit = Number(a.includes("=") ? a.split("=")[1] : take());
    } else if (a === "--help" || a === "-h") {
      out.help = true;
    }
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log(`Usage:
  npm run census:autopilot-v1 -- [options]

Options:
  --mode=unified_benchmark|discovery|reconstruction|full_record|freshness|reconciliation|activation|image_integrity|escalation
  --group=IHG,Hilton,Choice
  --brand="Hotel Indigo"
  --country=Mexico
  --priority=P0,P1
  --max-records=N
  --dry-run
  --resume <run_id>
`);
    return;
  }

  const result = await runCensusAutopilotV1({
    mode: args.mode,
    group: args.group,
    brand: args.brand,
    country: args.country,
    region: args.region,
    priority: args.priority,
    maxRecords: args.maxRecords,
    dryRun: args.dryRun,
    resume: args.resume,
    cventChallengeLimit: args.cventChallengeLimit,
    log: console.log,
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        run_id: result.run_id,
        hotels: result.observability.hotels_researched,
        artifact_root: result.artifact_root,
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
