#!/usr/bin/env node
/**
 * Regression gate for GDI 20-opp adaptive investigation experiment harness.
 * Runs offline ($0). Does not publish or call paid providers.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  EXPERIMENT_ID,
  MAX_PAID_EXTERNAL_USD,
  COHORT_QUOTAS,
  classifyDemandCategory,
  buildTwentyCaseCohort,
  runEvidenceChallenger,
  runBaselineCase,
  runAgenticCase,
  compareCase,
  runFullExperiment,
} from "../lib/group-demand-intelligence/experiments/gdi-20opp-adaptive-investigation-experiment-v1.js";
import { DENIED_METERED_PROVIDERS } from "../lib/group-demand-intelligence/adaptive-public-research-controller-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

assert.equal(MAX_PAID_EXTERNAL_USD, 0);
assert.ok(DENIED_METERED_PROVIDERS.includes("exa"));
assert.equal(
  classifyDemandCategory({
    alreadyContracted: true,
    title: "Organizer — already placed",
  }),
  "cross_hotel_booked",
);
assert.equal(
  classifyDemandCategory({
    demandStatus: "SPECULATIVE_WATCH",
    title: "Weak signal",
  }),
  "rejected_unsupported",
);
assert.equal(
  classifyDemandCategory({
    segment: "education_scientific",
    title: "ISCT Annual Meeting",
  }),
  "medical_pharma",
);

const challenger = runEvidenceChallenger({
  thesis: "Might maybe possibly host a meeting",
  evidence: [],
  contact: { name: "Only Name" },
});
assert.equal(challenger.speculative, true);
assert.equal(challenger.salesReadyBlockedByChallenger, true);

const { manifest, selected } = buildTwentyCaseCohort({ root: ROOT });
assert.equal(selected.length, 20, "cohort must be exactly 20");
assert.equal(manifest.caseCount, 20);
assert.equal(manifest.schema, EXPERIMENT_ID);

const slotCounts = {};
for (const c of selected) {
  slotCounts[c.cohortSlot] = (slotCounts[c.cohortSlot] || 0) + 1;
  assert.ok(c.opportunityId, "immutable opportunity id required");
  assert.ok(c.snapshotSha256, "frozen snapshot hash required");
  assert.ok(c.evaluationLabelsHeldOut, "labels must be held out");
}
const quotaSum = Object.values(COHORT_QUOTAS).reduce((a, b) => a + b, 0);
assert.equal(quotaSum, 20);

const sample = selected[0];
const baseline = runBaselineCase(sample.frozenOpportunity, sample);
assert.equal(baseline.costs.actualPaidUsd, 0);
assert.equal(baseline.executionKind, "fresh_deterministic_evaluation_on_frozen_snapshot");
assert.ok(baseline.sharedEligibility.status);

const agentic = await runAgenticCase(sample.frozenOpportunity, sample, {
  authorizeExternalRetrieval: false,
});
assert.equal(agentic.costs.actualPaidUsd, 0);
assert.match(agentic.executionKind, /offline|frozen/);
assert.ok(Array.isArray(agentic.steps));
assert.ok(agentic.challenger);

const cmp = compareCase(baseline, agentic);
assert.equal(typeof cmp.deltaTotal, "number");
assert.equal(cmp.actualPaidUsd, 0);

const { report, outDir } = await runFullExperiment({
  root: ROOT,
  authorizeExternalRetrieval: false,
});
assert.equal(report.totals.cases, 20);
assert.equal(report.totals.actualPaidUsdTotal, 0);
assert.ok(["GO", "ITERATE", "NO-GO"].includes(report.decision.recommendation));
assert.equal(report.gatedLiveProtocol.status, "NOT_EXECUTED");
assert.ok(fs.existsSync(path.join(outDir, "EXECUTIVE_REPORT.md")));
assert.ok(fs.existsSync(path.join(outDir, "RESULTS.json")));
assert.ok(fs.existsSync(path.join(outDir, "COHORT_MANIFEST.json")));
assert.ok(fs.existsSync(path.join(outDir, "COST_AND_TOOL_LOG.json")));

const fixtureManifest = path.join(
  ROOT,
  "fixtures/gdi-experiments/gdi-20opp-adaptive-investigation-v1/cohort-manifest.public.json",
);
assert.ok(fs.existsSync(fixtureManifest));

console.log(
  JSON.stringify(
    {
      ok: true,
      experiment: EXPERIMENT_ID,
      recommendation: report.decision.recommendation,
      avgDelta: report.decision.avgDelta,
      slotCounts,
      shortfalls: report.cohort.shortfalls,
      substitutions: report.cohort.substitutions?.length || 0,
      actualPaidUsdTotal: report.totals.actualPaidUsdTotal,
    },
    null,
    2,
  ),
);
