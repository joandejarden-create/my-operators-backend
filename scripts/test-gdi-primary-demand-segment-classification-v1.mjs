#!/usr/bin/env node
/**
 * Regression fixtures for primary demand-segment classification (Full E2E).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateScalingCompaniesEligibility,
  resolvePrimaryDemandSegment,
  applyPrimaryDemandSegmentClassification,
} from "../lib/group-demand-intelligence/demand-segments/primary-demand-segment-classification-v1.js";
import { classifyOpportunityTypeFilter } from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const casesPath = path.join(
  ROOT,
  "fixtures/gdi-primary-demand-segment-classification-v1/cases.json",
);
const doc = JSON.parse(fs.readFileSync(casesPath, "utf8"));

for (const c of doc.cases) {
  const gate = evaluateScalingCompaniesEligibility(c);
  assert.equal(
    gate.ok,
    c.expectScalingEligible,
    `${c.id}: scaling eligibility`,
  );
  if (c.expectRejectCode && !gate.ok) {
    assert.equal(gate.code, c.expectRejectCode, `${c.id}: reject code`);
  }
  const routed = resolvePrimaryDemandSegment(c);
  if (c.expectSegmentId == null) {
    assert.ok(
      !routed.demandSegmentId ||
        routed.demandSegmentId !== "corporate_accounts_scaling",
      `${c.id}: must not resolve to scaling`,
    );
  } else {
    assert.equal(routed.demandSegmentId, c.expectSegmentId, `${c.id}: segment`);
    assert.equal(routed.opportunityTypeFilterCategory, c.expectFilter, `${c.id}: filter`);
  }
  if (c.expectFilter) {
    const applied = applyPrimaryDemandSegmentClassification(c);
    assert.equal(
      classifyOpportunityTypeFilter(applied),
      c.expectFilter,
      `${c.id}: classifyOpportunityTypeFilter after apply`,
    );
  }
}

// Baked SCALING label on a conference must not survive classify.
assert.equal(
  classifyOpportunityTypeFilter({
    id: "rx_d_918bc65e2d44",
    displayTitle: "OVMA True North Veterinary Conference 2027",
    title: "OVMA True North Veterinary Conference 2027",
    opportunityType: "corporate_accounts_scaling",
    opportunityTypeFilterCategory: "SCALING_COMPANY",
    whatIsHappening:
      "OVMA True North Veterinary Conference, January 21–23, 2027, MTCC.",
  }),
  "ASSOCIATION_CONFERENCE_GROUP",
);

console.log(
  JSON.stringify(
    {
      ok: true,
      cases: doc.cases.length,
      version: "gdi-primary-demand-segment-classification-v1",
    },
    null,
    2,
  ),
);
