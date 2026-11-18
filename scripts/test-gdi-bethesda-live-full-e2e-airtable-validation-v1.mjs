#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AUTH_PAID_DISCOVERY,
  AUTH_RESEARCH_PERSISTENCE,
  BETHESDA_HOTEL_ID,
  FOURTEEN_SEGMENTS,
  classifyPersistenceOperation,
  PERSISTENCE_OPERATION_CLASS,
  runOfflineValidationSuite,
  writeValidationReportPack,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-live-full-e2e-airtable-validation-v1.js";
import {
  GDI_AIRTABLE_TABLES,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-live-full-e2e-airtable-validation-v1",
);

assert.equal(BETHESDA_HOTEL_ID, "recLuxvwwxID7U2B8");
assert.equal(FOURTEEN_SEGMENTS.length, 14);
assert.equal(AUTH_PAID_DISCOVERY.maxUsd, 18);
assert.equal(
  AUTH_RESEARCH_PERSISTENCE.destinationTableId,
  GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES.tableId,
);
assert.equal(
  classifyPersistenceOperation({ action: "WOULD_CREATE" }),
  PERSISTENCE_OPERATION_CLASS.CREATE_NEW,
);
assert.equal(
  classifyPersistenceOperation({ disposition: "EXCLUDE", excluded: true }),
  PERSISTENCE_OPERATION_CLASS.EXCLUDED,
);

const suite = await runOfflineValidationSuite({ repoRoot: ROOT });
assert.equal(suite.paidResearchExecuted, false);
assert.equal(suite.productionAirtableWrites, false);
assert.equal(suite.persistenceAuth.paidDoesNotUnlockWrites, true);
assert.equal(suite.idempotency.repeatCreatesZero, true);
assert.equal(suite.weeklyChange.descriptionRewriteNonMaterial, true);
assert.equal(suite.sharedIntelligenceReuse.ok, true);
assert.equal(
  suite.sharedIntelligenceReuse.checks.stableDemandGeneratorIdAcrossHotels,
  true,
);
assert.equal(
  suite.sharedIntelligenceReuse.checks.compositeKeysDifferByHotel,
  true,
);
assert.equal(
  suite.sharedIntelligenceReuse.checks.noNewTablesInvented,
  true,
);
assert.ok(suite.dedupe.after < suite.dedupe.before);

writeValidationReportPack(OUT, suite);

const required = [
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  "AIRTABLE_SCHEMA_AND_WRITE_ROUTING.md",
  "FOURTEEN_SEGMENT_LIVE_EXECUTION_PLAN.md",
  "PAID_DISCOVERY_AUTHORIZATION.md",
  "RESEARCH_PERSISTENCE_AUTHORIZATION.md",
  "EXACT_PRODUCTION_WRITE_MANIFEST.json",
  "COMMERCIAL_QUALIFICATION_SCORECARD.md",
  "CONTACTS_AND_EVIDENCE.md",
  "DEDUPLICATION_RESULTS.md",
  "WEEKLY_CHANGE_RESULTS.md",
  "AIRTABLE_READBACK_VERIFICATION.md",
  "IDEMPOTENCY_TEST_RESULTS.md",
  "WEEKLY_AUTOMATION_DESIGN.md",
  "FOUNDER_NEXT_DECISIONS.md",
  "SHARED_INTELLIGENCE_REUSE_VERIFICATION.md",
];
for (const name of required) {
  assert.ok(fs.existsSync(path.join(OUT, name)), `missing ${name}`);
}

const manifest = JSON.parse(
  fs.readFileSync(path.join(OUT, "EXACT_PRODUCTION_WRITE_MANIFEST.json"), "utf8"),
);
assert.equal(manifest.operations.length, 0);
assert.match(manifest.status, /PENDING_LIVE_RESEARCH/);
assert.equal(manifest.gdoWritePlan.mutationsAllowed, false);

console.log(
  JSON.stringify(
    {
      ok: true,
      outDir: OUT,
      hotelId: BETHESDA_HOTEL_ID,
      paidMaxUsd: AUTH_PAID_DISCOVERY.maxUsd,
      persistenceAuthorizationId: AUTH_RESEARCH_PERSISTENCE.authorizationId,
    },
    null,
    2,
  ),
);
