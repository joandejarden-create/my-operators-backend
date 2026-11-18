#!/usr/bin/env node
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runBethesdaAutomatedResearchPersistencePilot,
  BETHESDA_HOTEL_ID,
  PILOT_AUTHORIZATION_ID,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-automated-research-persistence-pilot-v1.js";
import {
  dedupeResearchCandidatesByCompositeKey,
  evaluateResearchPersistenceAuthorization,
  RESEARCH_PERSISTENCE_ACTIVATION,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { MATERIAL_RESEARCH_FIELDS } from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

assert.equal(RESEARCH_PERSISTENCE_ACTIVATION.disabledByDefault, true);
assert.ok(!MATERIAL_RESEARCH_FIELDS.includes(F.lastResearchedAt));

const dedupe = dedupeResearchCandidatesByCompositeKey(
  [
    { opportunityId: "a", hotelId: BETHESDA_HOTEL_ID, segment: "s1" },
    { opportunityId: "a", hotelId: BETHESDA_HOTEL_ID, segment: "s2" },
    { opportunityId: "b", hotelId: BETHESDA_HOTEL_ID, segment: "s1" },
  ],
  BETHESDA_HOTEL_ID,
);
assert.equal(dedupe.uniqueCount, 2);
assert.equal(dedupe.duplicates.length, 1);

const locked = evaluateResearchPersistenceAuthorization({
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: PILOT_AUTHORIZATION_ID,
});
assert.equal(locked.productionUnlocked, false);

const result = await runBethesdaAutomatedResearchPersistencePilot(ROOT);
assert.equal(result.hotelId, BETHESDA_HOTEL_ID);
assert.equal(result.productionAirtableWrites, 0);
assert.equal(result.gdoWrites, 0);
assert.equal(result.publicationWrites, 0);
assert.equal(result.baselineResearchInventory, 86);

for (const [k, v] of Object.entries(result.acceptance || {})) {
  assert.equal(v, true, `acceptance.${k}`);
}

console.log("PASS test:gdi-bethesda-automated-research-persistence-pilot-v1");
