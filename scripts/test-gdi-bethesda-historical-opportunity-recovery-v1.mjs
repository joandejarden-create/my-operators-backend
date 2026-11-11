#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { BETHESDA_CANONICAL_HOTEL_ID } from "../lib/group-demand-intelligence/research/bethesda-full-demand-database-reconciliation-v1.js";
import {
  BETHESDA_HISTORICAL_RECOVERY_V1,
  buildBethesdaHistoricalRecoveryManifest,
  loadCanonicalBethesdaBaseline,
  hashBethesdaCanonicalStore,
  CURATED_HISTORICAL_ATTESTATIONS,
  RECONCILE_GROUP,
} from "../lib/group-demand-intelligence/research/bethesda-historical-opportunity-recovery-v1.js";

const baseline = loadCanonicalBethesdaBaseline();
assert.equal(baseline.hotelId, BETHESDA_CANONICAL_HOTEL_ID);
assert.equal(baseline.count, 59, "canonical Bethesda baseline must remain 59");
assert.ok(baseline.ids.includes("gdi_opp_acts_ts27"));
assert.ok(baseline.ids.includes("gdi_opp_loudoun_college_showcase_2027"));

const hashes = hashBethesdaCanonicalStore();
assert.ok(hashes["opportunities.json"].sha256);
assert.equal(hashes["opportunities.json"].sha256.length, 64);

const before = hashes["opportunities.json"].sha256;
const manifest = buildBethesdaHistoricalRecoveryManifest();
assert.equal(manifest.version, BETHESDA_HISTORICAL_RECOVERY_V1);
assert.equal(manifest.safety.hotelStoreWrites, false);
assert.equal(manifest.safety.publication, false);
assert.equal(manifest.safety.paidResearch, false);
assert.equal(manifest.dryRunRestorationManifest.importAuthorized, false);
assert.equal(manifest.baseline.opportunityCount, 59);

assert.ok(CURATED_HISTORICAL_ATTESTATIONS.length >= 20);
assert.ok(
  manifest.coverage.cateringOpportunitiesRecovered >= 1,
  "must recover at least one local catering historical row"
);
assert.ok(
  manifest.coverage.partnershipOpportunitiesRecovered >= 1,
  "must recover at least one partnership historical row"
);
assert.ok(manifest.coverage.defensibleHistoricalPopulation >= 59);
assert.ok(manifest.missingArtifacts.length >= 3);
assert.ok(manifest.wipeRootCause.includes("FS opportunities.json"));

// No dry-run row may claim overwrite or publication
for (const row of manifest.dryRunRestorationManifest.rows) {
  assert.equal(row.overwriteCanonicalStore, false);
  assert.equal(row.customerPublication, false);
  assert.equal(row.importBlockedUntilApproval, true);
  assert.equal(row.targetHotelId, BETHESDA_CANONICAL_HOTEL_ID);
}

// Canonical store must be unchanged by building the manifest
const after = hashBethesdaCanonicalStore()["opportunities.json"].sha256;
assert.equal(before, after, "building recovery manifest must not mutate opportunities.json");

// Group A must equal baseline count
assert.equal(manifest.groups.A_existingCanonical.count, 59);

// Duplicates must not be offered as import rows
const importIds = new Set(manifest.dryRunRestorationManifest.rows.map((r) => r.originalRecordId));
for (const dup of manifest.groups.E_duplicatesSuperseded.rows) {
  assert.equal(importIds.has(dup.id), false);
}

const oppPath = path.join(
  "data/group-demand-intelligence/hotels",
  BETHESDA_CANONICAL_HOTEL_ID,
  "opportunities.json"
);
assert.ok(fs.existsSync(oppPath));

console.log(
  JSON.stringify(
    {
      ok: true,
      version: BETHESDA_HISTORICAL_RECOVERY_V1,
      baselineCount: 59,
      catering: manifest.coverage.cateringOpportunitiesRecovered,
      partnerships: manifest.coverage.partnershipOpportunitiesRecovered,
      historicalAdditional: manifest.coverage.historicalAdditionalUniqueOpportunities,
      population: manifest.coverage.defensibleHistoricalPopulation,
      groups: Object.fromEntries(
        Object.entries(manifest.groups).map(([k, v]) => [k, v.count])
      ),
      reconcileGroups: Object.values(RECONCILE_GROUP),
    },
    null,
    2
  )
);
