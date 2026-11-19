#!/usr/bin/env node
/**
 * E1 harness unit gates — never spend. Auth fail-closed + fairness + paid-decision option.
 */
import assert from "node:assert/strict";
import {
  assertE1Preflight,
  assertE1LiveAuthorization,
  E1_AUTHORIZATION_ID,
  E1_HARNESS_VERSION,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-exa-parity-e1-harness-v1.js";
import { createLiveExaExecutor } from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-exa-parity-e1-live-exa-v1.js";
import {
  createArmBudgetLedger,
  createExperimentBudgetLedger,
  E0_LIMITS,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-exa-parity-e0-budget-ledger-v1.js";
import { decideFromWave1EvidenceGrounded } from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-wave1-evidence-grounded-evaluator-v1.js";
import { assertE0FairnessCapability } from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-exa-parity-e0-harness-v1.js";

assert.equal(E0_LIMITS.e1TotalCeilingUsd, 20);
assert.equal(E0_LIMITS.perArmPerCaseUsd, 1.25);
assert.ok(E1_HARNESS_VERSION);
assert.ok(E1_AUTHORIZATION_ID);

const fairness = assertE0FairnessCapability();
assert.equal(fairness.ok, true, JSON.stringify(fairness.missing));

// Missing auth → blocked
{
  const pf = assertE1Preflight({});
  assert.equal(pf.ok, false);
  assert.ok(pf.blockers.includes("GDI_AI_AGENT_EXA_PARITY_AUTH=1_REQUIRED"));
  assert.ok(pf.blockers.includes("GDI_EXA_AGENT_LIVE=1_REQUIRED"));
  assert.ok(pf.blockers.includes("EXA_API_KEY_ABSENT"));
}

{
  const auth = assertE1LiveAuthorization({
    GDI_AI_AGENT_EXA_PARITY_AUTH: "1",
    GDI_EXA_AGENT_LIVE: "1",
  });
  assert.equal(auth.ok, false);
  assert.ok(auth.blockers.includes("EXA_API_KEY_ABSENT"));
}

// Live executor refuses without auth (no HTTP)
{
  const ledger = createArmBudgetLedger({
    arm: "A",
    opportunityId: "t",
    forceMockOnly: false,
  });
  const exa = createLiveExaExecutor({
    ledger,
    arm: "A",
    env: {},
  });
  const out = await exa.invoke({ operation: "search", query: "test" });
  assert.equal(out.ok, false);
  assert.equal(out.refused, true);
  assert.equal(ledger.snapshot().actualPaidUsd, 0);
}

// Experiment ledger hard ceiling
{
  const exp = createExperimentBudgetLedger({ maxUsd: 0.01 });
  const arm = createArmBudgetLedger({
    arm: "A",
    opportunityId: "t",
    maxUsd: 1.25,
    forceMockOnly: false,
    experimentLedger: exp,
  });
  const r1 = arm.reserve({
    operation: "search",
    provider: "exa_live",
    quote: { bounded: true, maxUsd: 0.008 },
    attemptLiveHttp: true,
  });
  assert.equal(r1.ok, true);
  arm.settle(r1.reservation.reservationId, { actualCostUsd: 0.008, paid: true });
  const r2 = arm.reserve({
    operation: "search",
    provider: "exa_live",
    quote: { bounded: true, maxUsd: 0.008 },
    attemptLiveHttp: true,
  });
  assert.equal(r2.ok, false);
  assert.equal(r2.reason, "EXPERIMENT_USD_CAP_EXCEEDED");
}

// Paid spend allowed for E1 decision path (within ceiling)
{
  const decide = decideFromWave1EvidenceGrounded(
    [
      {
        ok: true,
        opportunityId: "a",
        slot: "incomplete_demand",
        armA: {
          activeResearch: true,
          commercial: {
            actualPaidUsd: 0.2,
            verifiedNamedCount: 0,
            genuinelyNewVerifiedCount: 0,
            drawerReady: false,
            salesReady: false,
          },
        },
        armB: {
          activeResearch: true,
          adaptivePlanner: true,
          commercial: {
            actualPaidUsd: 0.3,
            verifiedNamedCount: 1,
            genuinelyNewVerifiedCount: 2,
            drawerReady: true,
            salesReady: false,
            challengerBlocked: false,
          },
        },
        deltas: { materialLift: true, budgetAsymmetry: false },
      },
      {
        ok: true,
        opportunityId: "b",
        slot: "missing_verified_contact",
        armA: {
          activeResearch: true,
          commercial: {
            actualPaidUsd: 0.1,
            verifiedNamedCount: 0,
            genuinelyNewVerifiedCount: 1,
            drawerReady: false,
            salesReady: false,
          },
        },
        armB: {
          activeResearch: true,
          adaptivePlanner: true,
          commercial: {
            actualPaidUsd: 0.15,
            verifiedNamedCount: 1,
            genuinelyNewVerifiedCount: 3,
            drawerReady: false,
            salesReady: false,
            challengerBlocked: false,
          },
        },
        deltas: { materialLift: true, budgetAsymmetry: false },
      },
      {
        ok: true,
        opportunityId: "c",
        slot: "rejected_speculative",
        armA: {
          activeResearch: true,
          commercial: {
            actualPaidUsd: 0.05,
            verifiedNamedCount: 0,
            genuinelyNewVerifiedCount: 0,
            salesReady: false,
          },
        },
        armB: {
          activeResearch: true,
          adaptivePlanner: true,
          commercial: {
            actualPaidUsd: 0.05,
            verifiedNamedCount: 0,
            genuinelyNewVerifiedCount: 0,
            salesReady: false,
            challengerBlocked: true,
          },
        },
        deltas: { materialLift: false, budgetAsymmetry: false },
      },
      {
        ok: true,
        opportunityId: "d",
        slot: "competitor_cross_hotel",
        armA: {
          activeResearch: true,
          commercial: {
            actualPaidUsd: 0.05,
            verifiedNamedCount: 0,
            genuinelyNewVerifiedCount: 0,
            salesReady: false,
          },
        },
        armB: {
          activeResearch: true,
          adaptivePlanner: true,
          commercial: {
            actualPaidUsd: 0.05,
            verifiedNamedCount: 0,
            genuinelyNewVerifiedCount: 0,
            salesReady: false,
            challengerBlocked: true,
          },
        },
        deltas: { materialLift: false, budgetAsymmetry: false },
      },
    ],
    { allowPaidSpend: true, maxPaidUsd: 20 },
  );
  assert.equal(decide.decision, "ADOPT");
}

// Without allowPaidSpend, paid > 0 still REJECT (Wave1 lock)
{
  const decide = decideFromWave1EvidenceGrounded([
    {
      ok: true,
      opportunityId: "a",
      armA: {
        activeResearch: true,
        commercial: { actualPaidUsd: 0.1, verifiedNamedCount: 0, genuinelyNewVerifiedCount: 0 },
      },
      armB: {
        activeResearch: true,
        commercial: { actualPaidUsd: 0.1, verifiedNamedCount: 1, genuinelyNewVerifiedCount: 2 },
      },
      deltas: { materialLift: true },
    },
  ]);
  assert.equal(decide.decision, "REJECT");
  assert.equal(decide.reason, "PAID_SPEND_NOT_ZERO");
}

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-ai-agent-exa-parity-e1-harness-v1",
      e1CeilingUsd: E0_LIMITS.e1TotalCeilingUsd,
      authorizationId: E1_AUTHORIZATION_ID,
    },
    null,
    2,
  ),
);
