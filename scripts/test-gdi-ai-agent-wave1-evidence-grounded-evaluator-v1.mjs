#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateMaterialCommercialLift,
  buildEvidenceGroundedDeltas,
  decideFromWave1EvidenceGrounded,
  rescoreWave1Report,
  analyzeAdaptiveStopBehavior,
  EVALUATOR_VERSION,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-wave1-evidence-grounded-evaluator-v1.js";
import { decideFromWave1 } from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-incremental-value-experiment-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function arm({ named = 0, neu = 0, drawerReady = false, fetches = 0, salesReady = false, challengerBlocked = true, disposition = "OPEN", eligibility = "RESEARCH_CONTINUATION", recovered = 0, active = true } = {}) {
  return {
    activeResearch: active,
    commercial: {
      verifiedNamedCount: named,
      genuinelyNewVerifiedCount: neu,
      drawerReady,
      fetchCount: fetches,
      durationMs: fetches * 100,
      actualPaidUsd: 0,
      salesReady,
      challengerBlocked,
      disposition,
      eligibilityStatus: eligibility,
    },
    independentChallenger: {
      buckets: {
        genuinelyNewVerifiedEvidence: Array.from({ length: neu }, () => ({ kind: "genuinely_new_verified" })),
        existingRecoveredEvidence: Array.from({ length: recovered }, () => ({ kind: "existing_recovered" })),
      },
      contact: { verifiedNamedCount: named },
    },
  };
}

// drawerReady alone must NOT be material lift
{
  const lift = evaluateMaterialCommercialLift(
    arm({ named: 1, neu: 2, drawerReady: false, fetches: 12 }),
    arm({ named: 1, neu: 2, drawerReady: true, fetches: 4 }),
  );
  assert.equal(lift.materialLift, false);
  assert.ok(lift.rejectedNonMaterial.includes("drawerReady_only"));
}

// verified named gain IS material lift
{
  const lift = evaluateMaterialCommercialLift(arm({ named: 0 }), arm({ named: 1 }));
  assert.equal(lift.materialLift, true);
  assert.ok(lift.reasons.includes("verified_named_contact_gain"));
}

// genuinely new verified evidence gain IS material lift
{
  const lift = evaluateMaterialCommercialLift(arm({ neu: 1 }), arm({ neu: 3 }));
  assert.equal(lift.materialLift, true);
  assert.ok(lift.reasons.includes("genuinely_new_verified_demand_evidence_gain"));
}

// recovery overlay alone is not material lift
{
  const lift = evaluateMaterialCommercialLift(
    arm({ recovered: 0, neu: 0 }),
    arm({ recovered: 5, neu: 0 }),
  );
  assert.equal(lift.materialLift, false);
  assert.ok(lift.rejectedNonMaterial.includes("recovery_overlay_only"));
}

// fewer retrievals alone is not material lift
{
  const deltas = buildEvidenceGroundedDeltas({
    slot: "incomplete_demand",
    armA: arm({ named: 1, neu: 4, fetches: 12, drawerReady: false }),
    armB: arm({ named: 1, neu: 2, fetches: 4, drawerReady: true }),
  });
  assert.equal(deltas.materialLift, false);
  assert.equal(deltas.legacyDrawerReadyWouldHaveLifted, true);
  assert.equal(deltas.efficiency.fetchDelta, -8);
  assert.ok(deltas.rejectedNonMaterialLiftReasons.includes("drawerReady_only"));
}

// ADOPT forbidden when zero material lifts (even with safe demotions)
{
  const results = [
    {
      ok: true,
      opportunityId: "spec1",
      slot: "rejected_speculative",
      armA: arm({ active: true, challengerBlocked: true, salesReady: false }),
      armB: arm({ active: true, challengerBlocked: true, salesReady: false }),
      deltas: { materialLift: false, budgetAsymmetry: false },
    },
    {
      ok: true,
      opportunityId: "comp1",
      slot: "competitor_cross_hotel",
      armA: arm({ active: true, challengerBlocked: true, salesReady: false }),
      armB: arm({ active: true, challengerBlocked: true, salesReady: false }),
      deltas: { materialLift: false, budgetAsymmetry: false },
    },
  ];
  // ensure deltas rebuilt
  for (const r of results) {
    r.deltas = buildEvidenceGroundedDeltas(r);
  }
  const d = decideFromWave1EvidenceGrounded(results);
  assert.equal(d.decision, "ITERATE");
  assert.equal(d.reason, "QUALITY_DEMOTIONS_WITHOUT_MATERIAL_LIFT");
  assert.notEqual(d.decision, "ADOPT");
  assert.deepEqual(decideFromWave1(results).decision, d.decision);
}

// ADOPT still possible with >=2 evidence-grounded lifts + safe gates
{
  const results = [
    {
      ok: true,
      opportunityId: "a",
      slot: "missing_verified_contact",
      armA: arm({ named: 0, active: true }),
      armB: arm({ named: 1, active: true }),
    },
    {
      ok: true,
      opportunityId: "b",
      slot: "incomplete_demand",
      armA: arm({ neu: 0, active: true }),
      armB: arm({ neu: 2, active: true }),
    },
    {
      ok: true,
      opportunityId: "spec1",
      slot: "rejected_speculative",
      armA: arm({ active: true, salesReady: false, challengerBlocked: true }),
      armB: arm({ active: true, salesReady: false, challengerBlocked: true }),
    },
    {
      ok: true,
      opportunityId: "comp1",
      slot: "competitor_cross_hotel",
      armA: arm({ active: true, salesReady: false, challengerBlocked: true }),
      armB: arm({ active: true, salesReady: false, challengerBlocked: true }),
    },
  ].map((r) => ({ ...r, deltas: buildEvidenceGroundedDeltas(r) }));
  const d = decideFromWave1EvidenceGrounded(results);
  assert.equal(d.decision, "ADOPT");
  assert.equal(d.materialCaseIds.length, 2);
}

// Offline rescore of preserved Wave 1 artifact (if present)
const wave1Path = path.join(
  ROOT,
  "reports/gdi-ai-agent-incremental-value-experiment-v1/WAVE1_RESULTS.json",
);
if (fs.existsSync(wave1Path)) {
  const original = JSON.parse(fs.readFileSync(wave1Path, "utf8"));
  assert.equal(original.decision?.decision, "ADOPT", "preserved Wave1 harness was ADOPT");
  const rescored = rescoreWave1Report(original);
  assert.equal(rescored.networkUsed, false);
  assert.equal(rescored.paidUsd, 0);
  assert.equal(rescored.materialCaseIds.length, 0);
  assert.notEqual(rescored.decision.decision, "ADOPT");
  assert.equal(rescored.decision.decision, "ITERATE");
  assert.ok(rescored.drawerOnlyPreviouslyLifted.length >= 2);
  assert.equal(rescored.evaluatorVersion, EVALUATOR_VERSION);

  const stop = analyzeAdaptiveStopBehavior(original);
  assert.ok(stop.cases.length === 8);
  assert.ok(stop.summary.prematureStopEvidenceLoss >= 1);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      evaluatorVersion: EVALUATOR_VERSION,
      drawerReadyAloneRejected: true,
      adoptBlockedOnZeroLifts: true,
      wave1ArtifactRescored: fs.existsSync(wave1Path),
    },
    null,
    2,
  ),
);
