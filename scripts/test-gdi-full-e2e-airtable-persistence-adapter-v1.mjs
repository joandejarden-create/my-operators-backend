#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_AIRTABLE_TABLES,
  AIRTABLE_PRODUCTION_WRITE_GUARD,
  FULL_E2E_APPROVED_WRITE_TABLES,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-contract-v1.js";
import {
  evaluateFullE2eAirtableWriteEligibility,
  FULL_E2E_AIRTABLE_DENY_LIST,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-eligibility-v1.js";
import { executeFullE2eAirtablePersistence } from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-adapter-v1.js";
import {
  classifyIntelligenceProvenance,
  INTELLIGENCE_PROVENANCE_CLASS,
} from "../lib/group-demand-intelligence/workflow/gdi-synthetic-provisional-isolation-v1.js";
import { dryRunFullE2eAirtablePersistence } from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-dry-run-v1.js";
import { loadBethesdaLiveExaProcessedCandidates } from "../lib/group-demand-intelligence/workflow/gdi-bethesda-exa-airtable-reconciliation-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-full-e2e-airtable-persistence-adapter-v1",
);

assert.equal(
  GDI_AIRTABLE_TABLES.GDI_NEED_PERIOD_OPPORTUNITY_MATCHES.identityField,
  "Match Key",
);
assert.ok(FULL_E2E_APPROVED_WRITE_TABLES.includes("Demand Generators"));
assert.ok(FULL_E2E_AIRTABLE_DENY_LIST.includes("GDI Hotel Need Periods"));
assert.ok(FULL_E2E_AIRTABLE_DENY_LIST.includes("Decisions"));
assert.equal(AIRTABLE_PRODUCTION_WRITE_GUARD.blocked, true);

const watch = evaluateFullE2eAirtableWriteEligibility({
  kind: "HOTEL_OPPORTUNITY",
  candidate: {
    disposition: "WATCHLIST_RESEARCH",
    hotelQualified: true,
    organizationName: "Example Org",
    salesReadyContacts: 0,
  },
},);
assert.ok(watch.allowedTables.includes("Demand Generators"));
assert.ok(!watch.allowedTables.includes("Group Demand Opportunities"));

const qualified = evaluateFullE2eAirtableWriteEligibility({
  kind: "HOTEL_OPPORTUNITY",
  candidate: {
    disposition: "WATCHLIST_RESEARCH",
    hotelQualified: true,
    demandThesis: "RELIABLE_INFERRED_DEMAND",
    salesReadyContacts: 1,
    organizationName: "Qualified Org",
  },
});
assert.ok(qualified.allowedTables.includes("Group Demand Opportunities"));

const synth = classifyIntelligenceProvenance({
  provenance: "Synthetic Test",
  status: "Test Scenario",
});
assert.equal(synth.class, INTELLIGENCE_PROVENANCE_CLASS.SYNTHETIC_TEST);
assert.equal(synth.customerEligible, false);

const blocked = await executeFullE2eAirtablePersistence({
  hotelId: "recLuxvwwxID7U2B8",
  apply: true,
  authorizeProductionWrites: true,
  exactWriteSetAuthorization: "test-should-still-block",
  candidates: [],
});
assert.equal(blocked.ok, false);
assert.equal(blocked.code, "AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED");

const loaded = loadBethesdaLiveExaProcessedCandidates();
const dry = dryRunFullE2eAirtablePersistence({
  hotelId: loaded.hotelId,
  city: "Bethesda",
  runId: loaded.runId,
  candidates: loaded.processed,
});
assert.equal(dry.plannedWriteCount, 7);

const manifest = JSON.parse(
  fs.readFileSync(path.join(OUT, "BETHESDA_EXACT_WRITE_MANIFEST.json"), "utf8"),
);
assert.equal(manifest.structuralProposedOperationCount, 7);
assert.equal(manifest.eligibleWriteCount, 4);
assert.equal(manifest.deferredCount, 3);
assert.equal(manifest.productionWritesAuthorized, false);

for (const name of [
  "YOTEL_ID_RECONCILIATION.json",
  "RADISSON_ID_RECONCILIATION.json",
  "GUADALAJARA_ID_RECONCILIATION.json",
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  "PRODUCTION_PILOT_AUTHORIZATION_REQUEST.md",
]) {
  assert.ok(fs.existsSync(path.join(OUT, name)), name);
}

const yotel = JSON.parse(
  fs.readFileSync(path.join(OUT, "YOTEL_ID_RECONCILIATION.json"), "utf8"),
);
assert.equal(yotel.counts.SHARED, 0);
assert.equal(yotel.filesystemCount, 42);
assert.equal(yotel.airtableCount, 108);

console.log("PASS test:gdi-full-e2e-airtable-persistence-adapter-v1");
