import assert from "node:assert/strict";
import { listCanonicalAdp31PropertyIds } from "../lib/ai-demand-positioning/governance/adp-operational-registry-hygiene-v1.js";

const ids = listCanonicalAdp31PropertyIds();
assert.equal(ids.length, 31);
assert.equal(new Set(ids).size, 31);
assert.ok(ids.includes("adp_bethesda_marriott"));
assert.deepEqual(ids, [...ids].sort());
console.log(JSON.stringify({ ok: true, hotels: ids.length, bethesda: true }));
