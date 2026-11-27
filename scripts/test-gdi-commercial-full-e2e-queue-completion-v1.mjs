#!/usr/bin/env node
/**
 * $0 tests: operating modes, screening dispositions, competitive classification,
 * Exa Agent deep job-store idempotency, ledger settled-kind detection.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveDeepResearchOperatingMode,
  deriveResearchCompletionStatus,
  DEEP_RESEARCH_OPERATING_MODE,
  RESEARCH_COMPLETION_STATUS,
  CERTIFICATION_SAMPLE_DEFAULT_MAX_CANDIDATES,
} from "../lib/group-demand-intelligence/research/gdi-commercial-deep-research-operating-modes-v1.js";
import {
  mapToCommercialScreeningDisposition,
  COMMERCIAL_SCREENING_DISPOSITION,
  contactGapBlocksDeepResearch,
} from "../lib/group-demand-intelligence/research/gdi-commercial-deep-research-screening-dispositions-v1.js";
import {
  classifyCompetitiveBusiness,
  COMPETITIVE_BUSINESS_CLASS,
} from "../lib/group-demand-intelligence/commercial-consistency/gdi-competitive-business-classification-v1.js";
import {
  buildCommercialResearchCompletionQueue,
  buildResearchCompletenessReport,
} from "../lib/group-demand-intelligence/research/gdi-commercial-research-completion-queue-v1.js";
import {
  completedDeepResearchOpportunityIds,
  isSettledDeepResearchOperation,
  settleReservedCall,
} from "../lib/group-demand-intelligence/research/gdi-authorized-paid-budget-ledger-v1.js";
import {
  buildExaAgentDeepJobKey,
  beginOrResumeExaAgentDeepJob,
  markExaAgentDeepJobSubmitted,
  markExaAgentDeepJobCompleted,
  EXA_AGENT_DEEP_JOB_STATUS,
} from "../lib/group-demand-intelligence/research/exa-agent/gdi-exa-agent-deep-job-store-v1.js";
import { maybeSeedExaDeepResearch } from "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

check("certification_sample_defaults_max_8", () => {
  const m = resolveDeepResearchOperatingMode({});
  assert.equal(m.operatingMode, DEEP_RESEARCH_OPERATING_MODE.CERTIFICATION_SAMPLE);
  assert.equal(m.maxCandidates, CERTIFICATION_SAMPLE_DEFAULT_MAX_CANDIDATES);
});

check("commercial_full_e2e_null_cap", () => {
  const m = resolveDeepResearchOperatingMode({
    operatingMode: DEEP_RESEARCH_OPERATING_MODE.COMMERCIAL_FULL_E2E,
  });
  assert.equal(m.isCommercialFullE2e, true);
  assert.equal(m.maxCandidates, null);
});

check("budget_exhaustion_is_partial_not_complete", () => {
  const s = deriveResearchCompletionStatus({
    eligibleCount: 100,
    deepCompletedCount: 8,
    deferredEligibleCount: 92,
    budgetExhausted: true,
    paidAuthorized: true,
  });
  assert.equal(s.status, RESEARCH_COMPLETION_STATUS.PARTIAL_RESEARCH_BUDGET_LIMIT);
  assert.equal(s.comprehensiveResearchComplete, false);
});

check("contact_gap_does_not_block_deep", () => {
  assert.equal(contactGapBlocksDeepResearch(), false);
  const d = mapToCommercialScreeningDisposition({
    preDeepOutcome: "ELIGIBLE_FOR_DEEP_RESEARCH",
  });
  assert.equal(d.disposition, COMMERCIAL_SCREENING_DISPOSITION.DEEP_RESEARCH_REQUIRED);
  assert.equal(d.activeQueue, true);
});

check("competitor_committed_stays_on_active_queue", () => {
  const d = mapToCommercialScreeningDisposition({
    competitorCommittedVerified: true,
  });
  assert.equal(
    d.disposition,
    COMMERCIAL_SCREENING_DISPOSITION.VERIFIED_COMPETITOR_COMMITTED,
  );
  assert.equal(d.activeQueue, true);
  assert.equal(d.paidDeepRequired, true);
});

check("venue_only_is_not_confirmed_hotel_contract", () => {
  const c = classifyCompetitiveBusiness({
    competitorHotel: "San Diego Convention Center",
    venue: "San Diego Convention Center",
    hotelSourcingDecisionEvidence: "Event at San Diego Convention Center",
    intakeDisposition: { commercialDisposition: "COMPETITOR_COMMITTED" },
  });
  assert.notEqual(c.classification, COMPETITIVE_BUSINESS_CLASS.HISTORICAL_OR_COMMITTED);
  assert.equal(c.venueOnlyMention === true || c.classification === COMPETITIVE_BUSINESS_CLASS.RESEARCH_REQUIRED, true);
});

check("direct_pursuit_without_competitor_lock", () => {
  const c = classifyCompetitiveBusiness({
    title: "Local training lodging",
    intakeDisposition: { commercialDisposition: "OPEN" },
    targetHotelSourcingStatus: "OPEN_CONFIRMED",
  });
  assert.equal(c.classification, COMPETITIVE_BUSINESS_CLASS.DIRECT_PURSUIT);
  assert.equal(c.fqoAutoPromote, false);
});

check("ledger_kind_exa_agent_counts_as_settled", () => {
  assert.equal(
    isSettledDeepResearchOperation({ kind: "EXA_AGENT_DEEP_RESEARCH" }),
    true,
  );
  assert.equal(isSettledDeepResearchOperation({ stage: "DEEP_RESEARCH" }), true);
  assert.equal(isSettledDeepResearchOperation({ stage: "DISCOVERY" }), false);
  const ledger = {
    operations: [
      { kind: "EXA_AGENT_DEEP_RESEARCH", opportunityId: "disc_a" },
      { stage: "DISCOVERY", opportunityId: "disc_b" },
    ],
  };
  const ids = completedDeepResearchOpportunityIds(ledger);
  assert.equal(ids.has("disc_a"), true);
  assert.equal(ids.has("disc_b"), false);
});

check("settle_normalizes_stage_for_exa_kind", () => {
  const tmp = path.join(os.tmpdir(), `gdi-ledger-${process.pid}.json`);
  const ledger = {
    hardCeilingUsd: 25,
    spentUsd: 0,
    reservedUsd: 0.1,
    quoteUnitMaxUsd: 0.1,
    quoteBounded: true,
    frozen: false,
    freezeReason: null,
    operations: [],
    blockedOperations: [],
    outPath: tmp,
  };
  settleReservedCall(ledger, {
    quotedMaxUsd: 0.1,
    actualCostUsd: 0.1,
    kind: "EXA_AGENT_DEEP_RESEARCH",
    opportunityId: "disc_x",
  });
  assert.equal(ledger.operations[0].stage, "DEEP_RESEARCH");
  assert.equal(ledger.operations[0].kind, "EXA_AGENT_DEEP_RESEARCH");
  assert.equal(completedDeepResearchOpportunityIds(ledger).has("disc_x"), true);
  try {
    fs.unlinkSync(tmp);
  } catch {
    /* ignore */
  }
});

check("job_store_does_not_resubmit_known_run", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "exa-deep-jobs-"));
  process.env.GDI_EXA_AGENT_DEEP_JOB_STORE_PATH = dir;
  const jobKey = buildExaAgentDeepJobKey({
    authorizationId: "AUTH_TEST",
    hotelId: "recHotel",
    opportunityId: "disc_1",
  });
  const first = beginOrResumeExaAgentDeepJob({ jobKey, opportunityId: "disc_1" });
  assert.equal(first.shouldSubmit, true);
  markExaAgentDeepJobSubmitted(jobKey, { providerRunId: "agent_run_abc" });
  markExaAgentDeepJobCompleted(jobKey, { providerRunId: "agent_run_abc" });
  const second = beginOrResumeExaAgentDeepJob({ jobKey, opportunityId: "disc_1" });
  assert.equal(second.shouldSubmit, false);
  assert.equal(second.reuseCompleted, true);
  assert.equal(second.providerRunId, "agent_run_abc");
  assert.equal(second.job.status, EXA_AGENT_DEEP_JOB_STATUS.COMPLETED);
  delete process.env.GDI_EXA_AGENT_DEEP_JOB_STORE_PATH;
});

check("queue_accounts_all_candidates_and_blocks_cert_complete_claim", () => {
  const q = buildCommercialResearchCompletionQueue({
    hotelId: "recnPB3Q02NO8Ma1o",
    operatingMode: DEEP_RESEARCH_OPERATING_MODE.CERTIFICATION_SAMPLE,
    discoveryCandidates: [
      { id: "a", organizationName: "A", researchSegment: "associations_conferences_events" },
      { id: "b", organizationName: "B", researchSegment: "projects_infrastructure" },
      { id: "c", organizationName: "C", researchSegment: "sports_entertainment" },
    ],
    preDeepDecisions: [
      { candidateId: "a", outcome: "ELIGIBLE_FOR_DEEP_RESEARCH" },
      { candidateId: "b", outcome: "ELIGIBLE_FOR_TARGETED_RESEARCH" },
      { candidateId: "c", outcome: "DUPLICATE_NO_NEW_EVIDENCE" },
    ],
    selectionRegister: {
      register: [
        { candidateId: "a", selectionDecision: "SELECTED_FOR_DEEP", preliminaryScore: 90 },
        { candidateId: "b", selectionDecision: "DEFERRED_LOWER_RANK", preliminaryScore: 70 },
      ],
    },
    completedDeepIds: ["a"],
    paidAuthorized: true,
  });
  assert.equal(q.totalUniqueCandidates, 3);
  assert.equal(q.duplicateReuseCount, 1);
  assert.equal(q.completion.comprehensiveResearchComplete, false);
  assert.ok(q.pendingPaidDeepCount >= 1);
});

check("completeness_report_flags_mock_leisure", () => {
  const r = buildResearchCompletenessReport({
    leisureMode: "mock_live",
    predictiveSourcingStatus: "PARTIAL_THROUGH_CAD_AND_DEEP_RESEARCH",
    comprehensiveResearchComplete: false,
  });
  assert.equal(r.leisureMockOnly, true);
  assert.ok(r.notes.some((n) => /mock_live/i.test(n)));
  assert.ok(r.notes.some((n) => /predictive/i.test(n)));
});

await checkAsync("maybeSeed_skips_ledger_settled_without_repost", async () => {
  const seed = await maybeSeedExaDeepResearch(
    { id: "disc_settled", hotelId: "recX", researchSegment: "projects_infrastructure" },
    {
      authorizeExaAgentLive: true,
      allowLiveExaDeepSeed: true,
      paidBudgetLedger: {
        operations: [{ kind: "EXA_AGENT_DEEP_RESEARCH", opportunityId: "disc_settled" }],
      },
    },
  );
  assert.equal(seed.seeded, false);
  assert.equal(seed.reason, "ALREADY_SETTLED_IN_PAID_BUDGET_LEDGER");
  assert.equal(seed.costUsd, 0);
});

const failed = results.filter((r) => !r.ok);
const outDir = path.join(
  ROOT,
  "reports/gdi-commercial-full-e2e-queue-completion-v1",
);
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "TEST_RESULTS.json"),
  `${JSON.stringify({ at: new Date().toISOString(), results, failed: failed.length }, null, 2)}\n`,
);
for (const r of results) {
  console.log(`${r.ok ? "PASS" : "FAIL"} ${r.name}${r.error ? ` — ${r.error}` : ""}`);
}
if (failed.length) {
  console.error(`FAILED ${failed.length}/${results.length}`);
  process.exit(1);
}
console.log(`PASS ${results.length}/${results.length}`);
