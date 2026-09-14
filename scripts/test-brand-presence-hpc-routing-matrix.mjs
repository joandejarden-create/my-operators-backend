/**
 * Explicit routing matrix for Brand Presence HPC V2 (P8.5B / P8.7).
 * FLAG alone must never switch Scout/Radar to HPC.
 * Radar requires RADAR_HPC_V2=1 in addition to BRAND_PRESENCE_HPC_V2=1.
 */
import assert from "node:assert/strict";
import { shouldUseHpcBrandPresence } from "../lib/hotel-census/brand-presence-hpc-request.js";

function req(query = {}, headers = {}) {
  return { query, headers };
}

const FLAG_OFF = { BRAND_PRESENCE_HPC_V2: "0", RADAR_HPC_V2: "0" };
const FLAG_BP_ONLY = { BRAND_PRESENCE_HPC_V2: "1", RADAR_HPC_V2: "0" };
const FLAG_BOTH = { BRAND_PRESENCE_HPC_V2: "1", RADAR_HPC_V2: "1" };
const FLAG_UNSET = {};

const cases = [
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
  { name: "BOTH + Scout still Legacy", env: FLAG_BOTH, req: req({ limit: "100000" }), expect: false },
  { name: "BOTH + HE still HPC", env: FLAG_BOTH, req: req({ product: "hotel-explorer" }), expect: true },
];

const results = [];
for (const c of cases) {
  const got = shouldUseHpcBrandPresence(c.req, c.env);
  const pass = got === c.expect;
  results.push({
    name: c.name,
    expected: c.expect ? "HPC" : "LEGACY",
    got: got ? "HPC" : "LEGACY",
    pass,
  });
  assert.equal(got, c.expect, c.name);
}

console.log(JSON.stringify({ pass: true, results }, null, 2));
console.log("test:brand-presence-hpc-routing-matrix OK");
