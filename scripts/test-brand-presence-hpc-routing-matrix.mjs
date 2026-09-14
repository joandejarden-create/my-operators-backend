/**
 * Explicit routing matrix for Brand Presence HPC V2 (P8.5B).
 * FLAG alone must never switch Scout/Radar to HPC.
 */
import assert from "node:assert/strict";
import { shouldUseHpcBrandPresence } from "../lib/hotel-census/brand-presence-hpc-request.js";

function req(query = {}, headers = {}) {
  return { query, headers };
}

const FLAG_OFF = { BRAND_PRESENCE_HPC_V2: "0" };
const FLAG_ON = { BRAND_PRESENCE_HPC_V2: "1" };
const FLAG_UNSET = {};

const cases = [
  { name: "FLAG=0 + HE", env: FLAG_OFF, req: req({ censusSource: "hpc", product: "hotel-explorer" }), expect: false },
  { name: "FLAG=0 + Scout", env: FLAG_OFF, req: req({ limit: "100000" }), expect: false },
  { name: "FLAG=0 + Radar", env: FLAG_OFF, req: req({ limit: "100000" }), expect: false },
  { name: "FLAG=1 + HE opt-in query", env: FLAG_ON, req: req({ censusSource: "hpc", product: "hotel-explorer" }), expect: true },
  { name: "FLAG=1 + HE header only", env: FLAG_ON, req: req({}, { "x-dealality-census-source": "hpc" }), expect: true },
  { name: "FLAG=1 + generic/no opt-in", env: FLAG_ON, req: req({ limit: "400" }), expect: false },
  { name: "FLAG=1 + Scout", env: FLAG_ON, req: req({ limit: "100000" }), expect: false },
  { name: "FLAG=1 + Radar", env: FLAG_ON, req: req({ limit: "100000" }), expect: false },
  { name: "FLAG unset + HE", env: FLAG_UNSET, req: req({ censusSource: "hpc" }), expect: false },
  { name: "FLAG=1 + product=hotel-explorer only", env: FLAG_ON, req: req({ product: "hotel-explorer" }), expect: true },
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
