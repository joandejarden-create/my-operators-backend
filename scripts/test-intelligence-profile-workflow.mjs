#!/usr/bin/env node
/**
 * Unit checks for intelligence-profile-workflow v1 helpers.
 */
import assert from "node:assert/strict";
import {
  determineWorkflowStage,
  assertNoCompanyValidatedWritePath,
  buildNextCommands,
  buildIntelligenceProfileWorkflowPlan,
  WORKFLOW_STAGES,
} from "../lib/partner-intelligence/intelligence-profile-workflow.js";

function emptyPkg() {
  return { sources: [], facts: [] };
}

// Company Validated never in publish block list
assert.equal(assertNoCompanyValidatedWritePath(), true);

// Stage 1 — no sources
{
  const stage = determineWorkflowStage({
    entityType: "operator",
    targetRecId: "recTEST0000000001",
    pkg: emptyPkg(),
    readiness: { eligible: false, blockReasons: ["no_linked_sources"] },
    factCounts: { total: 0, approved: 0, pending: 0 },
    governanceNormalized: {},
  });
  assert.equal(stage.stageId, 1);
  assert.equal(stage.stageKey, "source_discovery");
}

// Stage 4 — sources approved, no facts
{
  const stage = determineWorkflowStage({
    entityType: "operator",
    targetRecId: "recTEST0000000002",
    pkg: {
      sources: [
        {
          id: "recSRC1",
          approvedForExplorerUse: "Yes",
          status: "Approved",
        },
      ],
      facts: [],
    },
    readiness: { eligible: false, blockReasons: ["no_approved_facts"] },
    factCounts: { total: 0, approved: 0, pending: 0 },
    governanceNormalized: {},
  });
  assert.equal(stage.stageId, 4);
  assert.equal(stage.stageKey, "extraction_dry_run");
}

// Stage 5 — facts but none approved
{
  const stage = determineWorkflowStage({
    entityType: "operator",
    targetRecId: "recTEST0000000003",
    pkg: {
      sources: [
        {
          id: "recSRC1",
          approvedForExplorerUse: "Yes",
          status: "Extracted",
        },
      ],
      facts: [{ id: "recFACT1", humanReviewStatus: "Pending" }],
    },
    readiness: { eligible: false, blockReasons: ["no_approved_facts"] },
    factCounts: { total: 1, approved: 0, pending: 1, pendingCandidates: 1 },
    governanceNormalized: {},
  });
  assert.equal(stage.stageId, 5);
  assert.equal(stage.stageKey, "fact_stewardship");
}

// Stage 8 — published no_op
{
  const stage = determineWorkflowStage({
    entityType: "operator",
    targetRecId: "reciI2tYQBfMoMK9G",
    pkg: {
      sources: [
        {
          id: "recSRC1",
          approvedForExplorerUse: "Yes",
          status: "Extracted",
        },
      ],
      facts: [{ id: "recFACT1", humanReviewStatus: "Approved" }],
    },
    readiness: { eligible: true, changeClass: "no_op" },
    readinessReportEntry: { changeClass: "no_op", eligible: true },
    factCounts: { total: 5, approved: 5, pending: 2 },
    governanceNormalized: { validationStatus: "Company Published" },
  });
  assert.equal(stage.stageId, 8);
  assert.equal(stage.stageKey, "platform_usage");
}

// Stage 8 — stronger live governance preserved (downgrade protection)
{
  const stage = determineWorkflowStage({
    entityType: "brand",
    targetRecId: "recCKuXCmGvxHPfb3",
    pkg: {
      sources: [{ id: "recSRC1", approvedForExplorerUse: "Yes", status: "Extracted" }],
      facts: [{ id: "recFACT1", humanReviewStatus: "Approved" }],
    },
    readiness: {
      eligible: false,
      changeClass: "downgrade",
      publishScopeBlockers: ["would_downgrade_existing_validation"],
      blockReasons: ["would_downgrade_existing_validation"],
    },
    readinessReportEntry: { changeClass: "downgrade", eligible: false },
    factCounts: { total: 48, approved: 4, pending: 0 },
    governanceNormalized: {
      validationStatus: "Company Published",
      externalDisplayStatus: "Show Trust Label",
      displayLabel: "Source-Informed Profile",
    },
  });
  assert.equal(stage.stageId, 8);
  assert.equal(stage.stageKey, "platform_usage");
  assert.ok(stage.blockers.includes("stronger_live_governance_preserved"));
}

// Next commands never include Company Validated
{
  const stage = determineWorkflowStage({
    entityType: "operator",
    targetRecId: "reciI2tYQBfMoMK9G",
    pkg: emptyPkg(),
    factCounts: { total: 0, approved: 0 },
    governanceNormalized: {},
  });
  const next = buildNextCommands({
    entityType: "operator",
    targetRecId: "reciI2tYQBfMoMK9G",
    stage,
    pkg: emptyPkg(),
    targetProfile: { name: "GHL Hoteles (GHL Holding)" },
  });
  const joined = next.commands.join(" ");
  assert.ok(!/Company Validated/i.test(joined));
  assert.ok(next.commands.some((c) => c.includes("partner-reference")));
}

assert.equal(WORKFLOW_STAGES.length, 9);

// buildIntelligenceProfileWorkflowPlan normalizes live Airtable fields (not pre-extracted raw)
{
  const plan = buildIntelligenceProfileWorkflowPlan({
    entityType: "brand",
    targetRecId: "recCKuXCmGvxHPfb3",
    targetProfile: {
      name: "Kimpton Hotels",
      fields: {
        "Validation Status": "Company Published",
        "Usage Permission": "Platform Display Allowed",
        "Source Type": "Public Sources + AI Extraction",
        "External Display Status": "Show Trust Label",
        "Last Reviewed Date": "2026-06-12",
        "Confidence Level": "High",
      },
    },
    sources: [
      { id: "recSRC1", approvedForExplorerUse: "Yes", status: "Extracted" },
    ],
    facts: [{ id: "recFACT1", humanReviewStatus: "Approved" }],
    published: [],
    readinessReport: {
      packages: [
        {
          entityType: "brand",
          targetRecId: "recCKuXCmGvxHPfb3",
          changeClass: "downgrade",
          eligible: false,
        },
      ],
    },
  });
  assert.equal(plan.governance.live.validationStatus, "Company Published");
  assert.equal(plan.governance.normalized.validationStatus, "Company Published");
  assert.equal(plan.governance.normalized.displayLabel, "AI-Assisted Profile");
  assert.equal(plan.governance.normalized.sourceBasis, "Company Materials");
}

console.log("test-intelligence-profile-workflow: ok");
