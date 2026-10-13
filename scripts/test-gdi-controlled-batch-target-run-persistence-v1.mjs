/**
 * Assert Target Run persistence + maturity rules for controlled batch.
 *   node scripts/test-gdi-controlled-batch-target-run-persistence-v1.mjs
 */
import assert from "node:assert/strict";
import {
  buildTargetRun,
  buildResearchRun,
  EXECUTION_STATUS,
  RESULT_TYPE,
  RUN_TYPE,
} from "../lib/group-demand-intelligence/research-coverage/index.js";
import {
  assessSubstantiveResearchEvidence,
  classifyGdiResearchMaturity,
  isInitFootprintRun,
  GDI_RESEARCH_MATURITY,
} from "../lib/group-demand-intelligence/research-maturity-v1.js";
import { targetRunToFields } from "../lib/group-demand-intelligence/research-coverage/airtable-stores.js";

function testInitFootprintNotResearched() {
  const init = {
    runId: "gdir_init_test",
    notes: "Universe reconciliation — initialization footprint (seed/init; not a live discovery cycle)",
    queries: 0,
    fetches: 0,
    payload: { kind: "INIT_FOOTPRINT", source: "universe_reconciliation_v1" },
  };
  assert.equal(isInitFootprintRun(init), true);
  const ev = assessSubstantiveResearchEvidence({
    runs: [init],
    targetRuns: [],
    customerReady: 0,
  });
  assert.equal(ev.strength, "INIT_ONLY");
  const m = classifyGdiResearchMaturity({
    totalTargets: 22,
    targetsResearched: 0,
    customerReady: 0,
    evidence: ev,
  });
  assert.equal(m.maturity, GDI_RESEARCH_MATURITY.INITIALIZED_ONLY);
}

function testResearchedTargetCreatesTargetRunShape() {
  const run = buildResearchRun({
    hotelId: "recTEST",
    hotelName: "Test Hotel",
    runType: RUN_TYPE.WEEKLY,
    targetsDue: 1,
    notes: "controlled batch test",
  });
  const tr = buildTargetRun({
    hotelId: "recTEST",
    targetId: "gdirt_test_1",
    runId: run.runId,
    executionStatus: EXECUTION_STATUS.RESEARCHED,
    researchExecuted: true,
    resultType: RESULT_TYPE.NO_MATERIAL_CHANGE,
    queriesUsed: 0,
    fetchesUsed: 1,
    sourceCount: 1,
    sourceUrls: ["https://example.org/program"],
    lastResultSummary: "URL fetch completed",
  });
  assert.equal(tr.executionStatus, EXECUTION_STATUS.RESEARCHED);
  assert.equal(tr.researchExecuted, true);
  assert.ok(tr.targetRunId);
  const fields = targetRunToFields(tr);
  assert.ok(fields["Target Run ID"]);
  assert.equal(fields["Execution Status"], EXECUTION_STATUS.RESEARCHED);
  assert.equal(fields["Fetches Used"], 1);

  const ev = assessSubstantiveResearchEvidence({
    runs: [{ runId: run.runId, queries: 0, fetches: 1, notes: "weekly" }],
    targetRuns: [tr],
    customerReady: 0,
  });
  assert.equal(ev.hasTargetLedgerProof, true);
  const m = classifyGdiResearchMaturity({
    totalTargets: 1,
    targetsResearched: 1,
    customerReady: 0,
    evidence: ev,
  });
  assert.equal(m.maturity, GDI_RESEARCH_MATURITY.RESEARCHED_NO_READY);
}

function testNoFakeTargetRunWithoutResearch() {
  const tr = buildTargetRun({
    hotelId: "recTEST",
    targetId: "gdirt_skip",
    runId: "gdir_x",
    executionStatus: EXECUTION_STATUS.SKIPPED,
    researchExecuted: false,
    resultType: RESULT_TYPE.INSUFFICIENT_EVIDENCE,
    queriesUsed: 0,
    fetchesUsed: 0,
  });
  const ev = assessSubstantiveResearchEvidence({
    runs: [],
    targetRuns: [tr],
    customerReady: 0,
  });
  assert.equal(ev.hasTargetLedgerProof, false);
  assert.equal(ev.proof.researchedTargetRuns, 0);
}

function testIdempotentTargetRunId() {
  const a = buildTargetRun({
    hotelId: "recH",
    targetId: "gdirt_a",
    runId: "gdir_r1",
    executionStatus: EXECUTION_STATUS.RESEARCHED,
    researchExecuted: true,
  });
  const b = buildTargetRun({
    hotelId: "recH",
    targetId: "gdirt_a",
    runId: "gdir_r1",
    executionStatus: EXECUTION_STATUS.RESEARCHED,
    researchExecuted: true,
  });
  assert.equal(a.targetRunId, b.targetRunId);
}

function testCustomerReadyTransition() {
  const tr = buildTargetRun({
    hotelId: "recH",
    targetId: "gdirt_a",
    runId: "gdir_r1",
    executionStatus: EXECUTION_STATUS.RESEARCHED,
    researchExecuted: true,
    fetchesUsed: 1,
  });
  const ev = assessSubstantiveResearchEvidence({
    targetRuns: [tr],
    customerReady: 2,
  });
  const m = classifyGdiResearchMaturity({
    totalTargets: 10,
    targetsResearched: 10,
    customerReady: 2,
    evidence: ev,
  });
  assert.equal(m.maturity, GDI_RESEARCH_MATURITY.CUSTOMER_READY);
}

testInitFootprintNotResearched();
testResearchedTargetCreatesTargetRunShape();
testNoFakeTargetRunWithoutResearch();
testIdempotentTargetRunId();
testCustomerReadyTransition();
console.log("PASS test-gdi-controlled-batch-target-run-persistence-v1");
