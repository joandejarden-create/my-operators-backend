#!/usr/bin/env node
/**
 * Record an Apify Actor run into the Hotel Intelligence usage ledger.
 * READ-ONLY toward census / Airtable. Does not start new Actor runs.
 *
 * Usage:
 *   node scripts/apify-usage-record-run.mjs --use-case=ROOM_COUNT --run-json=path/to/run.json
 *   node scripts/apify-usage-record-run.mjs --use-case=ROOM_COUNT --stdin < run.json
 *
 * run.json = Apify GET /v2/actor-runs/{id} body ({ data: {...} } or bare data object).
 * Optional outcome flags:
 *   --records-requested=N
 *   --successful-matches=N
 *   --successful-enrichments=N
 *   --verified-enrichments=N
 *   --label=...
 *   --auth=mcp|local_token
 *   --notes=...
 *
 * Never pass tokens on the CLI.
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  APIFY_AUTH_METHODS,
  APIFY_USE_CASES,
  createApifyUsageStore,
} from "../lib/hotel-intelligence/apify-usage/index.js";

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
  const out = {
    useCase: null,
    runJson: null,
    stdin: false,
    recordsRequested: null,
    successfulMatches: null,
    successfulEnrichments: null,
    verifiedEnrichments: null,
    label: null,
    auth: APIFY_AUTH_METHODS.MCP,
    notes: null,
    actorName: null,
  };
  for (const a of argv.slice(2)) {
    if (a === "--stdin") out.stdin = true;
    else if (a.startsWith("--use-case=")) out.useCase = a.slice(11);
    else if (a.startsWith("--run-json=")) out.runJson = a.slice(11);
    else if (a.startsWith("--records-requested="))
      out.recordsRequested = Number(a.slice(20));
    else if (a.startsWith("--successful-matches="))
      out.successfulMatches = Number(a.slice(21));
    else if (a.startsWith("--successful-enrichments="))
      out.successfulEnrichments = Number(a.slice(25));
    else if (a.startsWith("--verified-enrichments="))
      out.verifiedEnrichments = Number(a.slice(23));
    else if (a.startsWith("--label=")) out.label = a.slice(8);
    else if (a.startsWith("--auth=")) out.auth = a.slice(7);
    else if (a.startsWith("--notes=")) out.notes = a.slice(8);
    else if (a.startsWith("--actor-name=")) out.actorName = a.slice(13);
  }
  return out;
}

async function readStdin() {
  const chunks = [];
  for await (const c of process.stdin) chunks.push(c);
  return Buffer.concat(chunks).toString("utf8");
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.useCase || !Object.prototype.hasOwnProperty.call(APIFY_USE_CASES, args.useCase)) {
    console.error(
      `Required --use-case= one of: ${Object.keys(APIFY_USE_CASES).join(", ")}`
    );
    process.exit(1);
  }
  let raw;
  if (args.stdin) {
    raw = await readStdin();
  } else if (args.runJson) {
    raw = fs.readFileSync(path.resolve(args.runJson), "utf8");
  } else {
    console.error("Provide --run-json=path or --stdin");
    process.exit(1);
  }
  const apify_run = JSON.parse(raw);
  const store = createApifyUsageStore();
  const result = store.recordRun({
    use_case: args.useCase,
    apify_run,
    actor_name: args.actorName || undefined,
    records_requested: args.recordsRequested,
    successful_matches: args.successfulMatches,
    successful_enrichments: args.successfulEnrichments,
    verified_enrichments: args.verifiedEnrichments,
    auth_method: args.auth,
    label: args.label,
    notes: args.notes,
  });

  const safe = {
    created: result.created,
    ledger_path: result.ledger_path,
    run_id: result.row.run_id,
    actor_name: result.row.actor_name,
    dealality_use_case: result.row.dealality_use_case,
    apify_run_cost_usd: result.row.apify_run_cost_usd,
    records_returned: result.row.records_returned,
    cost_per_returned_record: result.row.cost_per_returned_record,
    cost_source: result.row.cost_source,
  };
  console.log(JSON.stringify(safe, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
