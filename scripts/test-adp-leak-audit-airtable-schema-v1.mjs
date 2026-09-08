#!/usr/bin/env node
/**
 * npm run test:adp-leak-audit-airtable-schema-v1
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync, mkdtempSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  AIRTABLE_TABLE_CONTRACTS,
  LEAK_AUDIT_AIRTABLE_SCHEMA_VERSION,
  LEAK_AUDIT_TABLE_FIELDS,
  listRequiredLeakAuditTables,
  createLeakAuditStore,
  createLeakAuditRepository,
  detectLeakAuditStorageBackend,
  isLeakAuditAirtableConfigured,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const doc = join(
  root,
  "docs/ai-demand-positioning/adp-leak-audit-demo-pack/AIRTABLE_SCHEMA_V1.md"
);
assert.ok(existsSync(doc));
assert.match(readFileSync(doc, "utf8"), /AdpLeakAuditRequests/);
assert.ok(LEAK_AUDIT_AIRTABLE_SCHEMA_VERSION);

const required = listRequiredLeakAuditTables();
assert.equal(required.length, 13);
for (const name of required) {
  assert.ok(AIRTABLE_TABLE_CONTRACTS[name], `missing contract ${name}`);
  assert.ok(LEAK_AUDIT_TABLE_FIELDS[name], `missing field map ${name}`);
}

const storage = detectLeakAuditStorageBackend();
const airtableConfigured = isLeakAuditAirtableConfigured();
assert.equal(storage.backend === "airtable", airtableConfigured);

const tmp = mkdtempSync(join(tmpdir(), "leak-schema-"));
const store = createLeakAuditStore({ root: tmp });
const request = store.createRequest({
  hotelName: "Schema Test Hotel",
  source: "manual",
  prioritySource: "inferred_from_results",
});
assert.equal(request.prioritySource, "inferred_from_results");
assert.equal(request.researchMode, "leak_audit_lite");
assert.match(request.id, /^leak_req_/);

const hotel = store.createHotel({
  requestId: request.id,
  hotelName: "Schema Test Hotel",
});
assert.equal(hotel.productionMatchReadOnly, true);
assert.ok(hotel.shortHotelName);
assert.match(hotel.id, /^leak_hotel_/);

const portfolio = store.createPortfolio({ portfolioName: "Schema Portfolio" });
assert.match(portfolio.id, /^leak_port_/);

const repo = createLeakAuditRepository({ root: join(tmp, "live-repo"), dualWrite: false });
assert.ok(repo.storageInfo);
assert.ok(["filesystem", "airtable"].includes(repo.storageBackend));
const repoRequest = repo.createRequest({
  hotelName: "Repo Hotel",
  source: "manual",
});
assert.equal(repoRequest.researchMode, "leak_audit_lite");

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_AIRTABLE_SCHEMA_V1",
    tables: required.length,
    prioritySourceDefault: request.prioritySource,
    researchModeDefault: request.researchMode,
    storageBackendUsed: repo.storageBackend,
    airtableConfigured,
    filesystemFallback: repo.storageBackend === "filesystem",
  })
);
