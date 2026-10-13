#!/usr/bin/env node
/**
 * Synthetic response-mapping tests for International Controller Outreach V1.
 * Fixtures are TEST-only — never production HOTEL_SUPPLIED_EVIDENCE.
 */

import {
  runSyntheticResponseMappingTests,
  buildControllerOutreachCohort,
  getArchitectureStatus,
  OUTREACH_READINESS,
} from "../lib/group-demand-intelligence/international-controller-outreach-v1/index.js";

const tests = runSyntheticResponseMappingTests();
const cohort = buildControllerOutreachCohort();
const arch = getArchitectureStatus();

let fail = 0;
function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    fail += 1;
  } else {
    console.log("PASS:", msg);
  }
}

assert(tests.pass === true, "All synthetic response mapping fixtures pass");
assert(arch.EMAILS_ACTUALLY_SENT === false, "Emails not sent");
assert(arch.READY_THRESHOLD_CHANGED === false, "Ready threshold unchanged");
assert(cohort.length === 3, "Outreach cohort is exactly 3");
assert(cohort.every((c) => c.emailsActuallySent === false), "No cohort email sent");
assert(
  cohort.every((c) => c.outreachReadiness === OUTREACH_READINESS.READY_TO_SEND),
  "All three READY_TO_SEND after authority check"
);
assert(cohort[0].key === "rif" && cohort[0].outreachPriority === 1, "RIF priority 1");
assert(cohort.every((c) => c.draft?.language === "es"), "All drafts Spanish");
assert(
  tests.results.every((r) => r.readyFromResponseAlone === false),
  "No synthetic response alone causes Ready"
);
assert(
  tests.results.every((r) => r.syntheticBlockedFromProduction === true),
  "Synthetic responses blocked from production persistence"
);

if (fail) {
  console.error(`\n${fail} assertion(s) failed`);
  process.exit(1);
}
console.log("\nOK — international controller outreach V1 tests passed");
console.log(JSON.stringify({ synthetic: tests, architecture: arch }, null, 2));
