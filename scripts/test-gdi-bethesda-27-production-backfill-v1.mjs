#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  BETHESDA_27_AUTHORIZED_CREATE_OPPORTUNITY_IDS,
  BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
  BETHESDA_HOTEL_ID,
  buildBethesda27ProductionManifest,
  validateBethesda27Authorization,
  alignCanonicalBethesdaOpportunitiesTo86,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-research-consolidation-v1.js";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

assert.equal(BETHESDA_27_AUTHORIZED_CREATE_OPPORTUNITY_IDS.length, 27);
assert.equal(
  new Set(BETHESDA_27_AUTHORIZED_CREATE_OPPORTUNITY_IDS).size,
  27,
);

const blocked = validateBethesda27Authorization({
  authorizeProductionWrites: true,
  exactWriteSetAuthorization: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
  apply: true,
});
assert.equal(blocked.ok, true);
assert.equal(blocked.apply, true);

const wrong = validateBethesda27Authorization({
  authorizeProductionWrites: true,
  exactWriteSetAuthorization: "wrong",
  apply: true,
});
assert.equal(wrong.ok, false);

const manifest = buildBethesda27ProductionManifest(ROOT);
assert.equal(manifest.hotelId, BETHESDA_HOTEL_ID);
assert.equal(manifest.entryCount, 27);
assert.equal(manifest.authorizationId, BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION);
assert.equal(manifest.gdoWrites, 0);
assert.equal(manifest.publicationWrites, 0);
assert.equal(manifest.blockers.length, 0);

for (const e of manifest.entries) {
  assert.equal(e.plannedAction, "CREATE");
  assert.equal(e.gdoWrite, false);
  assert.ok(e.fields?.[F.researchPayloadJson]);
  const s = e.fields?.[F.sourceSummary];
  if (typeof s === "string") {
    assert.ok(!s.includes("[object Object]"), e.opportunityId);
  }
}

const align = alignCanonicalBethesdaOpportunitiesTo86(ROOT, { write: false });
assert.equal(align.ok, true);
assert.equal(align.afterCount, 86);
assert.equal(align.wrote, false);

console.log("PASS test:gdi-bethesda-27-production-backfill-v1");
