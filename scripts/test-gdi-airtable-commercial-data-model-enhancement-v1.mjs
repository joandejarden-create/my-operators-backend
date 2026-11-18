#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-airtable-commercial-data-model-enhancement-v1",
);

const REQUIRED = [
  "README.md",
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  "EXISTING_FIELD_COVERAGE.md",
  "DATA_POPULATION_QUALITY.md",
  "COMMERCIAL_INTELLIGENCE_GAP_MATRIX.md",
  "CANONICAL_DEMAND_IDENTITY_REVIEW.md",
  "WEEKLY_CHANGE_DATA_CONTRACT.md",
  "COMPETITOR_PLACEMENT_DATA_REVIEW.md",
  "CONTACT_RELATIONSHIP_DATA_REVIEW.md",
  "MULTI_HOTEL_REUSE_AND_PRIVACY.md",
  "PROPOSED_MINIMAL_SCHEMA_CHANGES.md",
  "EXISTING_FIELD_REUSE_PLAN.md",
  "FULL_E2E_INTEGRATION_IMPACT.md",
  "IMPLEMENTATION_PRIORITY_AND_SEQUENCE.md",
  "FOUNDER_DECISIONS_REQUIRED.md",
];

for (const name of REQUIRED) {
  const p = path.join(OUT, name);
  assert.ok(fs.existsSync(p), `missing ${name}`);
  assert.ok(fs.statSync(p).size > 40, `too small ${name}`);
}

const summary = fs.readFileSync(
  path.join(OUT, "FOUNDER_EXECUTIVE_SUMMARY.md"),
  "utf8",
);
assert.match(summary, /STOP FOR FOUNDER REVIEW/);
assert.match(summary, /prefer zero/i);

const decisions = fs.readFileSync(
  path.join(OUT, "FOUNDER_DECISIONS_REQUIRED.md"),
  "utf8",
);
assert.match(decisions, /CD-1/);
assert.match(decisions, /CD-8/);

const matrix = fs.readFileSync(
  path.join(OUT, "COMMERCIAL_INTELLIGENCE_GAP_MATRIX.md"),
  "utf8",
);
assert.match(matrix, /EXISTS_STRUCTURED/);
assert.match(matrix, /PARTIALLY_IMPLEMENTED/);
assert.match(matrix, /Weekly Change/);

console.log("PASS test:gdi-airtable-commercial-data-model-enhancement-v1");
