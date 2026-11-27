#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recnPB3Q02NO8Ma1o";

const store = createGdiPublicationPackageStore({});
const ptr = store.readPointer(HOTEL, "STAGED");
assert.ok(ptr?.publicationVersion);
const loaded = store.loadPackage(HOTEL, ptr.publicationVersion);
assert.equal(loaded.ok, true);
const opps = loaded.package.opportunities;
assert.equal(opps.length, 26);

const lmc = opps.filter((o) =>
  /LOCAL_CATERING/i.test(o.opportunityTypeFilterCategory || ""),
);
assert.ok(lmc.length >= 2);
for (const o of lmc) {
  assert.equal(o.salesActionPillLabel, "Pursue Catering");
}

for (const o of opps) {
  assert.ok(o.displayTitle);
  assert.ok(o.opportunityTypeFilterCategory);
  assert.ok(o.datePillDisplayLabel || o.eventDateDisplay);
  assert.ok(Array.isArray(o.salesOpportunityQuickFilterKeys));
  assert.ok(o.salesOpportunityQuickFilterKeys.length > 0);
  assert.ok(o.hotelSourcingStatusLabel);
  const role = o.primaryContact?.title || o.primaryContact?.role || "";
  assert.equal(/^(events|gala)/i.test(String(role)), false);
}

assert.equal(
  opps.filter((o) => o.competitiveMarketScopeEligible === true).length,
  0,
);

const run = spawnSync(
  process.execPath,
  [path.join(ROOT, "scripts/gdi-wyndham-26-commercial-certification-matrix-v1.mjs")],
  { encoding: "utf8", cwd: ROOT },
);
assert.equal(run.status, 0, run.stderr || run.stdout);

console.log(
  JSON.stringify(
    {
      ok: true,
      publicationVersion: ptr.publicationVersion,
      pursueCatering: lmc.length,
    },
    null,
    2,
  ),
);
