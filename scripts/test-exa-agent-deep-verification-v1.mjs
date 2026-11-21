#!/usr/bin/env node
/**
 * Offline unit tests for Exa Agent deep verification helpers (no live API).
 */

import assert from "node:assert/strict";
import {
  caseQualifiesForAgent,
  buildResearchQuestion,
  independentlyVerifyAgentFinding,
  assertManualAgentAuthorization,
  runExaAgentDeepVerification,
  AGENT_FINDING_CLASS,
  EXA_AGENT_DEFAULT_ROUTING,
  EXA_AGENT_ROUTING_MODE,
} from "../lib/hotel-intelligence/sec-edgar/exa-agent-deep-verification-v1.js";
import { VERIFICATION_GATE } from "../lib/hotel-intelligence/sec-edgar/contract-intelligence-identity-layers-v1.js";
import { createExaAgentPilotBudget } from "../lib/hotel-intelligence/sec-edgar/exa-agent-pilot-budget-v1.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const caseRow = {
  caseId: "A01_test",
  cohort: "A",
  hotel: "Hilton Garden Inn Joplin",
  agreementType: "FRANCHISE",
  verifiedEndDate: "2026-10-31",
  searchBaseline: {
    contractEndDate: "2026-10-31",
    noticePeriodDays: null,
    agreementExistenceVerified: true,
    contractTermsVerified: true,
    currentStatusVerified: false,
  },
};

assert.equal(caseQualifiesForAgent(caseRow).ok, true);
assert.match(buildResearchQuestion(caseRow), /renewed|extended|terminated/i);

const good = independentlyVerifyAgentFinding(caseRow, {
  structured: {
    hotel_or_agreement_identity: "Hilton Garden Inn Joplin franchise agreement",
    research_question: "status?",
    proposed_answer: "A 2018 exhibit discloses a franchise end date of 2028-01-15 with 90 days notice.",
    source_url: "https://www.sec.gov/Archives/edgar/data/1/ex10.htm",
    source_date: "2018-01-01",
    supporting_excerpt: "The Franchise Agreement shall expire on January 15, 2028 unless notice of nonrenewal is given 90 days prior.",
    evidence_confidence: "MEDIUM",
    unresolved_limitations: "Does not prove current renewal status.",
    claimed_end_date: "2028-01-15",
    claimed_notice_period_days: "90",
    claimed_renewal_or_termination_status: "status_unconfirmed",
  },
});

assert.equal(good.agentAloneSatisfiesAnyGate, false);
assert.equal(good.gates[VERIFICATION_GATE.CURRENT_STATUS_VERIFIED], false);
// TERMS/EXISTENCE may carry forward from Search baseline — Agent alone never newly satisfies them
assert.equal(good.gates[VERIFICATION_GATE.CONTRACT_TERMS_VERIFIED], true);
assert.equal(good.incremental.newEndDate, "2028-01-15");
assert.equal(good.incremental.newNoticeDays, 90);
assert.equal(good.classificationHint, AGENT_FINDING_CLASS.VERIFIED_END_DATE_STATUS_UNCONFIRMED);

const bad = independentlyVerifyAgentFinding(caseRow, {
  structured: {
    hotel_or_agreement_identity: "Some other hotel",
    research_question: "status?",
    proposed_answer: "unknown",
    source_url: "",
    supporting_excerpt: "",
    evidence_confidence: "NONE",
    unresolved_limitations: "none",
  },
});
assert.equal(bad.incremental.falseOrUnsupported, true);
assert.equal(bad.classificationHint, AGENT_FINDING_CLASS.RESEARCH_REQUIRED);

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "exa-agent-budget-"));
const budget = createExaAgentPilotBudget({
  cwd: tmp,
  ledgerPath: path.join(tmp, "ledger.json"),
  ceilingUsd: 0.05,
  maxPerCaseUsd: 0.03,
});
assert.equal(budget.canSpend(0.025, "c1").ok, true);
assert.equal(budget.record({ usd: 0.025, caseId: "c1", operation: "t" }).ok, true);
assert.equal(budget.canSpend(0.025, "c1").ok, false); // per-case
assert.equal(budget.canSpend(0.025, "c2").ok, true);
assert.equal(budget.record({ usd: 0.025, caseId: "c2", operation: "t2" }).ok, true);
assert.equal(budget.canSpend(0.01, "c3").ok, false); // ceiling

// Default routing: MANUAL_ESCALATION_ONLY — factory-wide / missing auth blocked
assert.equal(EXA_AGENT_DEFAULT_ROUTING, EXA_AGENT_ROUTING_MODE.MANUAL_ESCALATION_ONLY);
assert.equal(assertManualAgentAuthorization({}).ok, false);
assert.equal(assertManualAgentAuthorization({ explicitManualEscalation: true }).ok, false);
assert.equal(
  assertManualAgentAuthorization({
    explicitManualEscalation: true,
    spendAuthorization: true,
    factoryWideAutoDiscovery: true,
  }).ok,
  false,
);
assert.equal(
  assertManualAgentAuthorization({
    explicitManualEscalation: true,
    spendAuthorization: true,
    factoryWideAutoDiscovery: false,
  }).ok,
  true,
);

const blocked = await runExaAgentDeepVerification(caseRow, {
  factoryWideAutoDiscovery: true,
  explicitManualEscalation: true,
  spendAuthorization: true,
});
assert.equal(blocked.ok, false);
assert.equal(blocked.skipped, true);
assert.equal(blocked.reason, "EXA_AGENT_BLOCKED_ON_FACTORY_WIDE_RUN");

const noAuth = await runExaAgentDeepVerification(caseRow, {});
assert.equal(noAuth.ok, false);
assert.equal(noAuth.skipped, true);
assert.equal(noAuth.reason, "EXA_AGENT_REQUIRES_MANUAL_ESCALATION");

console.log("test-exa-agent-deep-verification-v1: PASS");
