#!/usr/bin/env node
/**
 * Guard: open-universe / live query generation must not contain evaluation benchmark names.
 *   node scripts/test-gdi-bethesda-live-recall-benchmark-isolation-v1.mjs
 */
import assert from "assert";
import { generateOpenUniverseQueries } from "../lib/group-demand-intelligence/open-universe-query-generator.js";
import { generateEventSeriesExpansionQueries } from "../lib/group-demand-intelligence/open-universe-query-generator.js";

const FORBIDDEN = [
  "TOPMed",
  "CTN Annual",
  "NINDS CTE",
  "High-Risk, High-Reward",
  "AMWA 112",
  "AMWA 113",
  "iCAN 2027",
  "AAOS 2027 Combined",
  "Crabtown Showdown",
  "Capital Showdown",
  "Fuel Medical",
  "FedHealth",
  "Quantum Readiness",
];

const profile = {
  market: "Bethesda",
  metro: "Washington DC metro",
  archetype: "full-service medical research campus adjacent meeting hotel",
  demandSectors: ["medical", "scientific", "healthcare", "advocacy", "sports"],
  institutionalAnchors: ["federal research campus", "NIH", "Walter Reed"],
};

const ou = generateOpenUniverseQueries(profile);
const series = generateEventSeriesExpansionQueries({
  organization: "National Institutes of Health",
  eventSeries: "scientific meeting",
});
const blob = JSON.stringify({ ou, series });
for (const f of FORBIDDEN) {
  assert.ok(!blob.toLowerCase().includes(f.toLowerCase()), `leak: ${f}`);
}
console.log(JSON.stringify({ ok: true, test: "BENCHMARK_ISOLATION", queryCount: ou.queries.length }, null, 2));
