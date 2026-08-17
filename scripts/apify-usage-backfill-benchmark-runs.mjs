#!/usr/bin/env node
/**
 * Backfill Apify usage ledger from known Tripadvisor benchmark run IDs.
 * READ-ONLY: does not start Actor runs; expects local run JSON snapshots
 * under data/hotel-intelligence/apify-usage/run-snapshots/ (fetched via MCP).
 *
 * Usage:
 *   node scripts/apify-usage-backfill-benchmark-runs.mjs
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  APIFY_AUTH_METHODS,
  APIFY_USE_CASES,
  DEFAULT_TRIPADVISOR_ACTOR,
  createApifyUsageStore,
} from "../lib/hotel-intelligence/apify-usage/index.js";

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const SNAP_DIR = path.join(
  ROOT,
  "data/hotel-intelligence/apify-usage/run-snapshots"
);

/** Metadata for historical Dealality Tripadvisor runs (no new Actor volume). */
const BENCHMARK_RUNS = [
  {
    run_id: "XGiAax82eDMWfQLfS",
    label: "dest_bogota_query_FAILED_GEO",
    use_case: APIFY_USE_CASES.HOTEL_DISCOVERY,
    records_requested: null,
    notes: "Wrong geography Athens GA — billed results excluded from CALA pool",
  },
  {
    run_id: "Yh0dDIvuREOBoqbar",
    label: "dest_puntacana_query",
    use_case: APIFY_USE_CASES.HOTEL_DISCOVERY,
    notes: "Destination free-text returned 0 items",
  },
  {
    run_id: "vSQBlw3boZqfDIBs0",
    label: "dest_cartagena_query",
    use_case: APIFY_USE_CASES.HOTEL_DISCOVERY,
    notes: "Destination free-text returned 0 items",
  },
  {
    run_id: "5KrXXk06ZAiBHLszQ",
    label: "known_batch0_search",
    use_case: APIFY_USE_CASES.ROOM_COUNT,
    records_requested: 20,
    notes: "Per-hotel Search?q= batch for known-room hotels",
  },
  {
    run_id: "3pPNTjaPzISjeJD1a",
    label: "hotels_g_multi_city",
    use_case: APIFY_USE_CASES.HOTEL_DISCOVERY,
    notes: "Hotels-g listing crawl; aborted mid-run (maxTotalChargeUsd)",
  },
];

function stripSignedOutput(run) {
  const data = run?.data ? { ...run, data: { ...run.data } } : { ...run };
  const body = data.data || data;
  if (body.output) {
    body.output = { dataset: body.defaultDatasetId || "[redacted]" };
  }
  if (body.containerUrl) delete body.containerUrl;
  return data;
}

function main() {
  if (!fs.existsSync(SNAP_DIR)) {
    console.error(`Missing snapshots dir: ${SNAP_DIR}`);
    console.error(
      "Save MCP GET /v2/actor-runs/{id} JSON as {runId}.json first (no tokens)."
    );
    process.exit(1);
  }
  const store = createApifyUsageStore();
  const results = [];
  for (const meta of BENCHMARK_RUNS) {
    const snapPath = path.join(SNAP_DIR, `${meta.run_id}.json`);
    if (!fs.existsSync(snapPath)) {
      results.push({ run_id: meta.run_id, status: "missing_snapshot" });
      continue;
    }
    const apify_run = stripSignedOutput(
      JSON.parse(fs.readFileSync(snapPath, "utf8"))
    );
    const recorded = store.recordRun({
      use_case: meta.use_case,
      apify_run,
      actor_id: DEFAULT_TRIPADVISOR_ACTOR.actor_id,
      actor_name: DEFAULT_TRIPADVISOR_ACTOR.actor_name,
      records_requested: meta.records_requested,
      auth_method: APIFY_AUTH_METHODS.MCP,
      label: meta.label,
      notes: meta.notes,
    });
    results.push({
      run_id: meta.run_id,
      status: recorded.created ? "created" : "updated",
      apify_run_cost_usd: recorded.row.apify_run_cost_usd,
      records_returned: recorded.row.records_returned,
      use_case: recorded.row.dealality_use_case,
    });
  }
  console.log(
    JSON.stringify(
      {
        production_writes: false,
        ledger: store.paths.ledger,
        summary: store.summary(),
        results,
      },
      null,
      2
    )
  );
}

main();
