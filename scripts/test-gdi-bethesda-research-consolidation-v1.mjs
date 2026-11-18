#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  reconcileBethesdaResearchInventory,
  buildBethesda27ProductionManifest,
  buildRockCreekCorrectionManifest,
  BETHESDA_HOTEL_ID,
  BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-research-consolidation-v1.js";
import {
  evaluateResearchPersistenceAuthorization,
  persistFullE2eResearchInventory,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-research-consolidation-and-persistence-activation-v1",
);

const recon = reconcileBethesdaResearchInventory(ROOT, { scanLedgers: true });
assert.equal(recon.hotelId, BETHESDA_HOTEL_ID);
assert.ok(
  recon.counts.canonical === 59 || recon.counts.canonical === 86,
  `canonical Bethesda must be 59 (pre) or 86 (post-align), got ${recon.counts.canonical}`,
);
assert.equal(recon.counts.preserved, 86, "preserved snapshot must be 86");
if (recon.counts.canonical === 59) {
  assert.equal(recon.counts.onlyPreserved, 27);
  assert.equal(recon.counts.shared, 59);
} else {
  assert.equal(recon.counts.onlyPreserved, 0);
  assert.equal(recon.counts.shared, 86);
}
assert.equal(recon.counts.onlyCanonical, 0);
assert.equal(recon.productionWritesAuthorized, false);

const manifest = buildBethesda27ProductionManifest(ROOT);
assert.equal(manifest.authorizationId, BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION);
assert.equal(manifest.entryCount, 27);
assert.equal(manifest.expectedCreates, 27);
assert.equal(manifest.applied, false);
assert.equal(manifest.gdoWrites, 0);
assert.equal(manifest.publicationWrites, 0);
assert.equal(manifest.readyForFounderAuthorization, true);
assert.equal(manifest.blockers.length, 0);

for (const e of manifest.entries) {
  assert.ok(e.fields, e.opportunityId);
  assert.equal(e.fields[F.hotelId], BETHESDA_HOTEL_ID);
  assert.equal(e.fields[F.opportunityId], e.opportunityId);
  assert.equal(e.fields[F.compositeKey], e.compositeKey);
  assert.equal(e.gdoWrite, false);
  assert.equal(e.publicationWrite, false);
  // Do not invent first discovery when source lacked it
  if (!e.evidenceSummary.firstDiscoveredAt) {
    assert.equal(e.fields[F.firstDiscoveredAt], undefined);
  }
  if (e.evidenceSummary.demandThesis == null || e.evidenceSummary.demandThesis === "") {
    assert.equal(e.fields[F.demandThesis], undefined);
  }
}

const rock = buildRockCreekCorrectionManifest();
assert.equal(rock.airtableRecordId, "recf35Q9B6jpX3Ivm");
assert.equal(rock.after, "pev_53b689545914754d");
assert.equal(rock.applied, false);
assert.equal(rock.preserveNoLodging, true);
assert.equal(rock.separateFromBethesdaResearchMigration, true);

const locked = evaluateResearchPersistenceAuthorization({
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
});
assert.equal(locked.productionUnlocked, false);

const dry = await persistFullE2eResearchInventory({
  hotelId: BETHESDA_HOTEL_ID,
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
  opportunities: [],
  researchCompleted: true,
  repoRoot: ROOT,
});
assert.equal(dry.dryRun, true);
assert.equal(dry.blockedApply, true);
assert.equal(dry.gdoWrites, 0);

assert.ok(fs.existsSync(path.join(OUT, "EXACT_27_RECORD_PRODUCTION_MANIFEST.json")));
assert.ok(fs.existsSync(path.join(OUT, "FOUNDER_DECISIONS.md")));

console.log("PASS test:gdi-bethesda-research-consolidation-v1");
