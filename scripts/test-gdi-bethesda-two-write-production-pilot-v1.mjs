#!/usr/bin/env node
/**
 * Validates Bethesda two-write production pilot artifacts + comparison fix.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  PAYLOAD_VOLATILE_KEYS,
  diffMaterialResearchFields,
  canonicalizeJson,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-bethesda-two-write-production-pilot-v1");

const REQUIRED = [
  "EXACT_WRITE_AUTHORIZATION.json",
  "PRODUCTION_PREFLIGHT.json",
  "PRODUCTION_WRITE_RESULTS.json",
  "AIRTABLE_READ_BACK.json",
  "IDEMPOTENCY_VERIFICATION.json",
  "SIXTY_THREE_DRIFT_ROOT_CAUSE.json",
  "SIXTY_THREE_DRIFT_ROOT_CAUSE.md",
  "GDO_PUBLICATION_ISOLATION.json",
  "CONTROLLED_EXA_DISCOVERY_PROPOSAL.md",
  "TEST_RESULTS.md",
  "FOUNDER_NEXT_APPROVAL.md",
  "FOUNDER_EXECUTIVE_SUMMARY.md",
];

for (const name of REQUIRED) {
  assert.ok(fs.existsSync(path.join(OUT, name)), `missing ${name}`);
}

assert.ok(PAYLOAD_VOLATILE_KEYS.includes("firstDiscoveredRunId"));
assert.ok(PAYLOAD_VOLATILE_KEYS.includes("firstDiscoveredAt"));
assert.ok(PAYLOAD_VOLATILE_KEYS.includes("lastResearchedAt"));

// Provenance stamp alone must not force material UPDATE.
const basePayload = {
  opportunityId: "demo",
  title: "Same research content",
  sources: [{ url: "https://example.com/a" }],
};
const existing = {
  [F.researchPayloadJson]: JSON.stringify(basePayload),
  [F.title]: "Same research content",
  [F.compositeKey]: "h::demo",
  [F.opportunityId]: "demo",
  [F.hotelId]: "h",
};
const proposed = {
  ...existing,
  [F.researchPayloadJson]: JSON.stringify({
    ...basePayload,
    firstDiscoveredRunId: "injected_run",
    lastResearchedAt: new Date().toISOString(),
  }),
};
const diff = diffMaterialResearchFields(proposed, existing);
assert.equal(diff.materialChanged, false, "volatile provenance must be non-material");

const results = JSON.parse(
  fs.readFileSync(path.join(OUT, "PRODUCTION_WRITE_RESULTS.json"), "utf8"),
);
const idem = JSON.parse(
  fs.readFileSync(path.join(OUT, "IDEMPOTENCY_VERIFICATION.json"), "utf8"),
);
const isolation = JSON.parse(
  fs.readFileSync(path.join(OUT, "GDO_PUBLICATION_ISOLATION.json"), "utf8"),
);
const drift = JSON.parse(
  fs.readFileSync(path.join(OUT, "SIXTY_THREE_DRIFT_ROOT_CAUSE.json"), "utf8"),
);
const auth = JSON.parse(
  fs.readFileSync(path.join(OUT, "EXACT_WRITE_AUTHORIZATION.json"), "utf8"),
);

assert.equal(auth.authorizationId, "gdi-bethesda-live-14-segment-persistence-pilot-write-v1");
assert.equal(auth.recurringPersistenceEnabled, false);
assert.equal(results.gdoWrites || 0, 0);
assert.equal(results.publicationWrites || 0, 0);
assert.equal(isolation.gdoWrites, 0);
assert.equal(isolation.publicationWrites, 0);
assert.equal(isolation.rockCreekWrites, 0);
assert.ok((results.counts?.CREATED || 0) === 1);
assert.ok((results.counts?.UPDATED || 0) === 1);
assert.equal(idem.counts?.UNCHANGED, 2);
assert.ok(drift.wouldUpdateCount <= 5, "mass false positives must be gone");
assert.ok(
  !canonicalizeJson(null),
);

console.log(
  JSON.stringify(
    {
      ok: true,
      created: results.counts.CREATED,
      updated: results.counts.UPDATED,
      idemUnchanged: idem.counts.UNCHANGED,
      residualWouldUpdate: drift.wouldUpdateCount,
      gdoWrites: 0,
      publicationWrites: 0,
    },
    null,
    2,
  ),
);
