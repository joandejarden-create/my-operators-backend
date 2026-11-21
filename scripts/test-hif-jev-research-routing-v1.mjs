#!/usr/bin/env node
/**
 * Offline unit tests for HIF Jev research routing (no live Jev required).
 */
import assert from "node:assert/strict";
import {
  decideHifResearchEscalationDeterministic,
  decideHifResearchEscalation,
  mapActionToFactoryStage,
  describeHifJevRoutingContract,
  HIF_RESEARCH_ACTION,
  HIF_JEV_FORBIDDEN,
} from "../lib/hotel-intelligence/sec-edgar/hif-jev-research-routing-v1.js";
import { JEV_DECISION_TYPE, CHOICES, isValidChoice } from "../lib/group-demand-intelligence/jev/index.js";

assert.equal(JEV_DECISION_TYPE.HIF_CONTRACT_RESEARCH_ESCALATION, "HIF_CONTRACT_RESEARCH_ESCALATION");
assert.ok(CHOICES.HIF_CONTRACT_RESEARCH_ESCALATION.includes("INVESTIGATE_WITH_EXA_AGENT"));
assert.ok(isValidChoice("HIF_CONTRACT_RESEARCH_ESCALATION", "STOP_LOW_VALUE_RESEARCH"));

const a = decideHifResearchEscalationDeterministic({
  caseId: "A01",
  cohort: "A",
  hotel: "Hilton Garden Inn Joplin",
  searchBaseline: {
    contractEndDate: "2026-10-31",
    currentStatus: "STATUS_UNCONFIRMED",
    currentStatusVerified: false,
    agreementExistenceVerified: true,
  },
});
assert.equal(a.action, HIF_RESEARCH_ACTION.INVESTIGATE_WITH_EXA_AGENT);

const b = decideHifResearchEscalationDeterministic({
  caseId: "B01",
  cohort: "B",
  hotel: "Comfort Inn Test",
  searchBaseline: { contractEndDate: null, noticePeriodDays: null },
});
assert.equal(b.action, HIF_RESEARCH_ACTION.VERIFY_CONTRACT_DATE);

const stop = decideHifResearchEscalationDeterministic({
  hotel: "Maple Grove RV Resort",
  cohort: "B",
});
assert.equal(stop.action, HIF_RESEARCH_ACTION.STOP_LOW_VALUE_RESEARCH);

assert.equal(mapActionToFactoryStage(HIF_RESEARCH_ACTION.INVESTIGATE_WITH_EXA_AGENT), "EXA_AGENT_DEEP_VERIFICATION");
assert.ok(HIF_JEV_FORBIDDEN.includes("authorize_hpc_changes"));

const contract = describeHifJevRoutingContract();
assert.equal(contract.decisionType, "HIF_CONTRACT_RESEARCH_ESCALATION");
assert.equal(contract.sanitizePreview.hotelName, undefined);

const detOnly = await decideHifResearchEscalation(
  {
    caseId: "X",
    hotel: "Test Hotel",
    cohort: "C",
    searchBaseline: {
      contractEndDate: "2030-01-01",
      noticePeriodDays: 90,
      currentStatusVerified: false,
    },
  },
  { deterministicOnly: true },
);
assert.equal(detOnly.source, "DETERMINISTIC");
assert.equal(detOnly.finalAction, HIF_RESEARCH_ACTION.VERIFY_OWNER_OPERATOR);

console.log("test-hif-jev-research-routing-v1: PASS");
