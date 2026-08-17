#!/usr/bin/env node
/**
 * Census Autopilot V2.3 — Independent Universe Discovery + Cvent Decoupling
 *
 * npm run census:autopilot-v2-3-independent-universe
 *
 * No Airtable. No Webhound. Cvent never production evidence.
 * Independent discovery cannot see Cvent hotels before independent freeze.
 */

import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import { runCensusAutopilotV23 } from "../lib/research-engine-v2/census-autopilot-v2-3/index.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

const result = await runCensusAutopilotV23({
  root: ROOT,
  log: console.log,
  skipEnrichment: process.env.CAV23_SKIP_ENRICHMENT === "1",
});

console.log(
  JSON.stringify(
    {
      outDir: result.outDir,
      discovered: result.scorecard.discovered,
      rediscovery_pct: result.scorecard.rediscovery_pct,
      independent_universe: result.scorecard.independent_universe,
      cvent: result.scorecard.cvent_dependency,
      verified: result.scorecard.verified_census,
      serpapi: result.scorecard.serpapi,
      airtable: result.scorecard.airtable,
      enrichment_feeds_cleanly: result.scorecard.enrichment_feeds_cleanly,
    },
    null,
    2
  )
);
