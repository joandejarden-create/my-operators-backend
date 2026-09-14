/**
 * Explicit routing matrix for Brand Presence HPC V2 (P8.5B / P8.7 / P8.8).
 * FLAG alone must never switch Scout/Radar to HPC.
 * Radar requires RADAR_HPC_V2=1 in addition to BRAND_PRESENCE_HPC_V2=1.
 * Scout census requires SCOUT_HPC_V2=1 + product=scout (separate gate).
 */
import assert from "node:assert/strict";
import {
  shouldUseHpcBrandPresence,
  shouldUseHpcScoutCensus,
} from "../lib/hotel-census/brand-presence-hpc-request.js";

function req(query = {}, headers = {}) {
  return { query, headers };
}

const FLAG_OFF = { BRAND_PRESENCE_HPC_V2: "0", RADAR_HPC_V2: "0", SCOUT_HPC_V2: "0" };
const FLAG_BP_ONLY = { BRAND_PRESENCE_HPC_V2: "1", RADAR_HPC_V2: "0", SCOUT_HPC_V2: "0" };
const FLAG_BOTH = { BRAND_PRESENCE_HPC_V2: "1", RADAR_HPC_V2: "1", SCOUT_HPC_V2: "0" };
const FLAG_SCOUT = { BRAND_PRESENCE_HPC_V2: "1", RADAR_HPC_V2: "1", SCOUT_HPC_V2: "1" };
const FLAG_UNSET = {};

const brandPresenceCases = [
  { name: "FLAG=0 + HE", env: FLAG_OFF, req: req({ censusSource: "hpc", product: "hotel-explorer" }), expect: false },
  { name: "FLAG=0 + Scout", env: FLAG_OFF, req: req({ limit: "100000" }), expect: false },
  { name: "FLAG=0 + Radar", env: FLAG_OFF, req: req({ limit: "100000" }), expect: false },
  { name: "BP=1 RADAR=0 + HE opt-in", env: FLAG_BP_ONLY, req: req({ censusSource: "hpc", product: "hotel-explorer" }), expect: true },
  { name: "BP=1 RADAR=0 + HE header only", env: FLAG_BP_ONLY, req: req({}, { "x-dealality-census-source": "hpc" }), expect: true },
  { name: "BP=1 RADAR=0 + generic/no opt-in", env: FLAG_BP_ONLY, req: req({ limit: "400" }), expect: false },
  { name: "BP=1 RADAR=0 + Scout", env: FLAG_BP_ONLY, req: req({ limit: "100000" }), expect: false },
  { name: "BP=1 RADAR=0 + Radar no opt-in", env: FLAG_BP_ONLY, req: req({ limit: "100000" }), expect: false },
  { name: "BP=1 RADAR=0 + Radar product (rollback)", env: FLAG_BP_ONLY, req: req({ product: "radar", censusSource: "hpc" }), expect: false },
  { name: "FLAG unset + HE", env: FLAG_UNSET, req: req({ censusSource: "hpc" }), expect: false },
  { name: "BP=1 + product=hotel-explorer only", env: FLAG_BP_ONLY, req: req({ product: "hotel-explorer" }), expect: true },
  { name: "BOTH + Radar opt-in product", env: FLAG_BOTH, req: req({ product: "radar" }), expect: true },
  { name: "BOTH + Radar censusSource+product", env: FLAG_BOTH, req: req({ censusSource: "hpc", product: "radar" }), expect: true },
  { name: "BOTH + Radar header product", env: FLAG_BOTH, req: req({}, { "x-dealality-product": "radar" }), expect: true },
  { name: "BOTH + Scout still Legacy on BP gate", env: FLAG_BOTH, req: req({ limit: "100000" }), expect: false },
  { name: "BOTH + product=scout never BP HPC", env: FLAG_SCOUT, req: req({ product: "scout", censusSource: "hpc" }), expect: false },
  { name: "BOTH + HE still HPC", env: FLAG_BOTH, req: req({ product: "hotel-explorer" }), expect: true },
];

const scoutCases = [
  { name: "Scout gate OFF + product=scout", env: FLAG_BOTH, req: req({ product: "scout", censusSource: "hpc" }), expect: false },
  { name: "Scout gate ON + product=scout", env: FLAG_SCOUT, req: req({ product: "scout" }), expect: true },
  { name: "Scout gate ON + headers", env: FLAG_SCOUT, req: req({}, { "x-dealality-product": "scout" }), expect: true },
  { name: "Scout gate ON + no product", env: FLAG_SCOUT, req: req({ country: "Mexico" }), expect: false },
  { name: "Scout gate ON + HE product", env: FLAG_SCOUT, req: req({ product: "hotel-explorer" }), expect: false },
  { name: "Scout gate ON + radar product", env: FLAG_SCOUT, req: req({ product: "radar" }), expect: false },
];

const results = [];
for (const c of brandPresenceCases) {
  const got = shouldUseHpcBrandPresence(c.req, c.env);
  results.push({
    gate: "brand-presence",
    name: c.name,
    expected: c.expect ? "HPC" : "LEGACY",
    got: got ? "HPC" : "LEGACY",
    pass: got === c.expect,
  });
  assert.equal(got, c.expect, c.name);
}

for (const c of scoutCases) {
  const got = shouldUseHpcScoutCensus(c.req, c.env);
  results.push({
    gate: "scout-census",
    name: c.name,
    expected: c.expect ? "HPC" : "LEGACY",
    got: got ? "HPC" : "LEGACY",
    pass: got === c.expect,
  });
  assert.equal(got, c.expect, c.name);
}

console.log(JSON.stringify({ pass: true, results }, null, 2));
console.log("test:brand-presence-hpc-routing-matrix OK");
