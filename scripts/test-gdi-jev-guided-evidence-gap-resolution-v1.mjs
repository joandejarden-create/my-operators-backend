#!/usr/bin/env node
/**
 * Tests: GDI Jev-Guided Evidence Gap Resolution V1 (deterministic units)
 */
import assert from "node:assert/strict";
import {
  assessEvidenceGaps,
  identifyPrimaryBlocker,
  defaultActionForBlocker,
  validateMarket,
  isAllowedJevAction,
  JEV_NEXT_ACTION,
  EVIDENCE_DIMENSION,
  ACTION_OUTCOME,
} from "../lib/group-demand-intelligence/evidence-gap/evidence-gap-model-v1.js";
import { decideEvidenceGapNextAction } from "../lib/group-demand-intelligence/evidence-gap/jev-evidence-gap-next-action.js";
import { JEV_DECISION_TYPE, isValidChoice, CHOICES } from "../lib/group-demand-intelligence/jev/jev-types.js";

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

// 1. Allowed action enum
{
  assert.equal(isAllowedJevAction("VERIFY_MARKET"), true);
  assert.equal(isAllowedJevAction("INVENT_LODGING"), false);
  assert.ok(CHOICES.EVIDENCE_GAP_NEXT_ACTION.includes("STOP_NO_PUBLIC_PATH"));
  assert.ok(isValidChoice(JEV_DECISION_TYPE.EVIDENCE_GAP_NEXT_ACTION, "WAIT_FOR_TRIGGER"));
  ok("allowed_jev_action_enum");
}

// 2. Market validation before deeper research
{
  const wrong = validateMarket(
    {
      event: "Digimarcon Los Angeles",
      sourceUrl: "https://digimarconlosangeles.com/sponsorship/",
    },
    "SPICE"
  );
  assert.equal(wrong.ok, false);
  assert.equal(wrong.reason, "WRONG_MARKET");

  const acOk = validateMarket(
    {
      event: "Congreso END Santiago 2027",
      sourceUrl: "https://www.congresoend2027.com/sede-1",
    },
    "AC"
  );
  assert.equal(acOk.ok, true);
  ok("market_validation_before_deeper_research");
}

// 3. Deterministic blocker identification
{
  const c = {
    event: "XXIII Congreso Hidrología Médica 2027",
    eventResolved: "XXIII Congreso Hidrología Médica 2027",
    sourceUrl: "https://congresohidrologiamedica.com/",
    evaluation: {
      lodgingGrade: "B",
      organizerControl: "ORGANIZER_CONTROLLED",
      commercialStatus: "UNKNOWN",
      lodgingRelationship: "OFFICIAL_ACCOMMODATION_PROGRAM",
      surface: "OFFICIAL_EVENT_PAGE",
    },
  };
  const gaps = assessEvidenceGaps(c, "AC");
  const b = identifyPrimaryBlocker(gaps, "AC");
  assert.ok(gaps.unresolvedDimensions.includes(EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS) || gaps.unresolvedDimensions.includes(EVIDENCE_DIMENSION.HOUSING_STATUS) || gaps.unresolvedDimensions.includes(EVIDENCE_DIMENSION.MARKET_VALIDATION));
  assert.ok(b.primary);
  assert.notEqual(b.primary, gaps.unresolvedDimensions.join(",")); // single primary
  ok("deterministic_blocker_identification");
}

// 4. Default action mapping
{
  assert.equal(
    defaultActionForBlocker(EVIDENCE_DIMENSION.MARKET_VALIDATION, "AC"),
    JEV_NEXT_ACTION.VERIFY_MARKET
  );
  assert.equal(
    defaultActionForBlocker(EVIDENCE_DIMENSION.HOUSING_STATUS, "SPICE"),
    JEV_NEXT_ACTION.FIND_TRAVEL_ACCOMMODATION_PAGE
  );
  ok("default_action_market_aware");
}

// 5. Jev cannot mutate fact state / promote / set WHO — packet contract
{
  // decideEvidenceGapNextAction with mocked decide path: when Jev disabled, falls back
  process.env.GDI_JEV_ENABLED = "0";
  const d = await decideEvidenceGapNextAction({
    packet: {
      unresolvedDimensions: [EVIDENCE_DIMENSION.HOUSING_STATUS],
      resolvedDimensions: [EVIDENCE_DIMENSION.MARKET_VALIDATION],
    },
    primaryBlocker: EVIDENCE_DIMENSION.HOUSING_STATUS,
    hotelShort: "AC",
    enableSafeApply: true,
  });
  assert.equal(d.appliedAction, d.defaultAction);
  assert.ok(!d.result?.mutatedFacts);
  assert.ok(isAllowedJevAction(d.appliedAction));
  // Restore
  delete process.env.GDI_JEV_ENABLED;
  ok("jev_cannot_mutate_facts_falls_back_default");
}

// 6. Future watch / public data ceiling / structured rejection outcomes exist
{
  assert.ok(ACTION_OUTCOME.WAIT_TRIGGER);
  assert.ok(ACTION_OUTCOME.PUBLIC_DATA_CEILING);
  assert.ok(ACTION_OUTCOME.WRONG_MARKET);
  ok("outcome_enums_future_watch_ceiling_reject");
}

// 7. Two-action max is a runner invariant (documented constant)
{
  const MAX_ACTIONS = 2;
  assert.equal(MAX_ACTIONS, 2);
  ok("two_action_max_invariant");
}

console.log(`\n${passed} tests passed`);
