#!/usr/bin/env node
/**
 * Unit tests for Phase 2C reconstruction helpers (no Airtable required).
 */
import assert from "node:assert/strict";
import {
  BE_PHASE2C_CURIO_RECONSTRUCTION_V1,
  CURIO_RECONSTRUCTION_TARGET,
  NOT_VALIDATED,
  evaluateCandidateQuality,
  generateCandidateFromEvidence,
  listRequiredTemplateSlots,
  compareCandidateToBaseline,
  assessPaidResearchNeeds,
} from "../lib/brand-explorer/be-phase2c-curio-reconstruction-v1.js";
import { BE_CANONICAL_TAB_IDS } from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";
import { CURIO_BRAND_ID } from "../lib/partner-intelligence/curio-fact-contamination.js";

{
  assert.equal(BE_PHASE2C_CURIO_RECONSTRUCTION_V1, "BE_PHASE2C_CURIO_RECONSTRUCTION_V1");
  assert.equal(CURIO_RECONSTRUCTION_TARGET.recordId, CURIO_BRAND_ID);
  assert.equal(CURIO_RECONSTRUCTION_TARGET.recordId, "receQkxgjlezsc1xg");
  assert.equal(BE_CANONICAL_TAB_IDS.length, 10);
  console.log("PASS identity + tab contract anchors");
}

{
  const slots = listRequiredTemplateSlots();
  assert.ok(slots.length >= 20, `expected many slots, got ${slots.length}`);
  assert.ok(slots.some((s) => s.slotKey === "identity.brand_name"));
  assert.ok(slots.some((s) => String(s.slotKey).includes("flexibility") || s.purpose === "Flexibility"));
  console.log(`PASS required template slot enumeration (${slots.length})`);
}

{
  const pack = {
    evidence: [
      { id: "identity.brand_name", kind: "identity", claim: "Curio Collection by Hilton", sourceRef: "basics", confidence: "high" },
      { id: "identity.parent", kind: "identity", claim: "Hilton Worldwide", sourceRef: "basics", confidence: "high" },
      { id: "loyalty.program", kind: "loyalty", claim: "Hilton Honors", sourceRef: "honors", confidence: "high" },
      { id: "metric.open_hotels", kind: "hpc", claim: "HPC Open hotels matched=5", sourceRef: "hpc", confidence: "medium" },
    ],
    hpc: { certState: "LIVE_UNVERIFIED_INVENTORY", certified: false, metrics: { totalOpenHotels: 5, totalOpenKeys: 0, totalPipelineHotels: 0, totalPipelineKeys: 0 } },
    paidResearchUsed: false,
  };
  const candidate = generateCandidateFromEvidence(pack);
  assert.equal(candidate.usedPresentationAsInput, false);
  assert.equal(candidate.usedCompletedFixtureAsInput, false);
  assert.ok(candidate.rows.some((r) => r.slotKey === "identity.brand_name" && /Curio/.test(r.body)));
  assert.ok(candidate.rows.some((r) => r.disposition === "RESEARCH_REQUIRED"));
  const quality = evaluateCandidateQuality(candidate, pack);
  assert.equal(quality.scores.correctPlacement, NOT_VALIDATED);
  assert.equal(quality.scores.visualRendering, NOT_VALIDATED);
  assert.ok(quality.scores.actualFieldCompleteness > 0);
  const paid = assessPaidResearchNeeds(candidate, pack);
  assert.equal(paid.paidResearchAuthorized, false);
  assert.equal(paid.stopForAuthorization, true);
  console.log("PASS independent generation + scoring honesty");
}

{
  const pack = {
    evidence: [
      { id: "identity.brand_name", kind: "identity", claim: "Curio Collection by Hilton", sourceRef: "x", confidence: "high" },
      { id: "identity.parent", kind: "identity", claim: "Hilton Worldwide", sourceRef: "x", confidence: "high" },
    ],
    hpc: null,
    paidResearchUsed: false,
  };
  const candidate = generateCandidateFromEvidence(pack);
  const baseline = {
    presentation: {
      rowCount: 1,
      blocks: [{ slotKey: "identity.brand_name", body: "Curio Collection by Hilton" }],
    },
  };
  const cmp = compareCandidateToBaseline(candidate, baseline);
  assert.ok(cmp.comparisons.length > 0);
  assert.ok(cmp.summary);
  console.log("PASS baseline comparison helper");
}

console.log("\nAll Phase 2C Curio reconstruction unit tests passed.");
