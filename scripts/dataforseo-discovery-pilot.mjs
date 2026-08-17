#!/usr/bin/env node
/**
 * DataForSEO discovery pilot — candidates only, no Census writes.
 *
 *   DATAFORSEO_WRITE_CANDIDATES_ONLY=1
 *   ENABLE_DATAFORSEO_VALIDATED_WRITES=0
 *   npm run census:dataforseo-discovery-pilot
 */

import "dotenv/config";
import { runDataForSeoDiscoveryPilot } from "../lib/research-engine-v2/dataforseo-discovery-pilot.js";

const report = await runDataForSeoDiscoveryPilot({
  env: process.env,
  log: (msg) => console.log(msg),
});

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      status: report.status,
      recommendation: report.recommendation,
      records_piloted: report.records_piloted,
      queries_run: report.queries_run,
      estimated_cost_usd: report.estimated_cost_usd,
      useful_candidates_found: report.useful_candidates_found,
      cost_per_useful_candidate: report.cost_per_useful_candidate,
      official_hotel_urls_found: report.official_hotel_urls_found,
      rooms_evidence_pages_found: report.rooms_evidence_pages_found,
      address_candidates_found: report.address_candidates_found,
      phone_candidates_found: report.phone_candidates_found,
      google_maps_candidates_found: report.google_maps_candidates_found,
      census_writes: report.census_writes,
      run_dir: report.run_dir,
      reason: report.reason || null,
    },
    null,
    2
  )
);

process.exit(report.ok ? 0 : 1);
