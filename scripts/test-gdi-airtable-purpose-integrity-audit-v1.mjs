#!/usr/bin/env node
/**
 * Deterministic gate for purpose-integrity audit package (no Airtable mutations).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-airtable-purpose-integrity-audit-v1");

const REQUIRED = [
  "README.md",
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  "COMPLETE_TABLE_INVENTORY.md",
  "TABLE_PURPOSE_AND_OWNERSHIP_MATRIX.md",
  "RECORD_QUALITY_AND_DUPLICATION_AUDIT.md",
  "EXISTING_WORKFLOW_READ_WRITE_MAP.md",
  "IDENTITY_AND_RELATIONSHIP_ANALYSIS.md",
  "SYNTHETIC_AND_PROVISIONAL_DATA_AUDIT.md",
  "MULTI_HOTEL_RECONCILIATION.md",
  "PROPOSED_CANONICAL_PERSISTENCE_MAPPING.md",
  "TABLE_CONSOLIDATION_AND_CLEANUP_RECOMMENDATIONS.md",
  "FOUNDER_DECISIONS_REQUIRED.md",
  "IMPLEMENTATION_PLAN.md",
  "EVIDENCE_REGISTER.json",
];

const REQUIRED_TABLES = [
  "Group Demand Opportunities",
  "Demand Generators",
  "Demand Programs",
  "Demand Generator Signals",
  "Hotel Demand Generator Fit",
  "Private Event Venues",
  "Private Event Signals",
  "Hotel Venue Fit",
  "GDI Research Targets",
  "GDI Research Runs",
  "GDI Research Target Runs",
  "GDI Hotel Commercial Profiles",
  "GDI Hotel Need Periods",
  "GDI Need-Date Campaign Runs",
  "GDI Need Period Opportunity Matches",
  "Decisions",
  "Decision Events",
  "Hotel Commercial Profiles",
  "Hotel Event Spaces",
  "Hotel Demand Nodes",
  "Hotel Seasonality & Need Periods",
  "Hotel Intelligence Evidence",
];

for (const name of REQUIRED) {
  const p = path.join(OUT, name);
  assert.ok(fs.existsSync(p), `missing ${name}`);
  assert.ok(fs.statSync(p).size > 50, `too small ${name}`);
}

const evidence = JSON.parse(
  fs.readFileSync(path.join(OUT, "EVIDENCE_REGISTER.json"), "utf8"),
);
assert.equal(evidence.version, "gdi-airtable-purpose-integrity-audit-v1");
assert.equal(evidence.baseId, "appa2cE7FTRmIbB32");
assert.equal(evidence.mutation, false);
assert.equal(evidence.paidResearch, false);
assert.equal(evidence.schemaChanges, false);
assert.equal(evidence.publicationChanges, false);
assert.ok(Array.isArray(evidence.tableInventory));
assert.ok(evidence.tableInventory.length >= REQUIRED_TABLES.length);

const names = new Set(evidence.tableInventory.map((t) => t.name));
for (const t of REQUIRED_TABLES) {
  assert.ok(names.has(t), `evidence missing table ${t}`);
}

const gdo = evidence.tableInventory.find(
  (t) => t.name === "Group Demand Opportunities",
);
assert.ok(gdo && gdo.recordCount >= 86, "GDO count unexpected");

const bethesda = evidence.multiHotelReconciliation?.find(
  (h) => h.hotelId === "recLuxvwwxID7U2B8",
);
assert.ok(bethesda, "Bethesda reconciliation missing");
assert.equal(bethesda.fsOpportunityCount, 86);
assert.equal(bethesda.airtable.groupDemandOpportunities.count, 86);

const summary = fs.readFileSync(
  path.join(OUT, "FOUNDER_EXECUTIVE_SUMMARY.md"),
  "utf8",
);
assert.match(summary, /STOP FOR FOUNDER REVIEW/);
assert.match(summary, /Do not activate production Airtable persistence/);

const decisions = fs.readFileSync(
  path.join(OUT, "FOUNDER_DECISIONS_REQUIRED.md"),
  "utf8",
);
assert.match(decisions, /D1/);
assert.match(decisions, /GDI Hotel Need Periods/);

const mapping = fs.readFileSync(
  path.join(OUT, "PROPOSED_CANONICAL_PERSISTENCE_MAPPING.md"),
  "utf8",
);
assert.match(mapping, /Block Full E2E/i);
assert.match(mapping, /Demand Generators/);

const script = fs.readFileSync(
  path.join(ROOT, "scripts/gdi-airtable-purpose-integrity-audit-v1.mjs"),
  "utf8",
);
assert.match(script, /No schema changes/);
assert.doesNotMatch(script, /\.create\(|\.update\(|\.destroy\(/);

console.log("PASS test:gdi-airtable-purpose-integrity-audit-v1");
