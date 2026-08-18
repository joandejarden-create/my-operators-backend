#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  applyExecutiveCopyGovernance,
  EVIDENCE_CONSTRUCTS,
} from "../lib/ai-visibility/executive-intelligence-copy-governance.js";

const results = [];
function test(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
    console.log(`  PASS ${name}`);
  } catch (err) {
    results.push({ name, ok: false, err });
    console.log(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nExecutive Intelligence Copy Governance\n");

test("A. brand appears in response never says cited", () => {
  const out = applyExecutiveCopyGovernance([
    {
      findingType: "LARGEST_COMPETITIVE_GAP",
      headline: "Autograph is cited more consistently than Design Hotels.",
      providerCount: 4,
      observationCount: 8,
      title: "Largest Competitive Gap",
    },
  ]).findings[0];
  assert.equal(out.evidenceConstruct, EVIDENCE_CONSTRUCTS.GAP);
  assert.ok(!/\bcited\b/i.test(out.governedHeadline));
});

test("B. N=1 does not use repeated terms", () => {
  const out = applyExecutiveCopyGovernance([
    {
      findingType: "POTENTIAL_AI_PERCEPTION_GAP",
      headline: "Possible perception mismatch",
      observationCount: 1,
      title: "Potential AI Perception Gap",
    },
  ]).findings[0];
  assert.ok(!/\brepeated|recurring|consistent\b/i.test(out.governedEvidence));
  assert.ok(/\bEarly signal\b/i.test(out.governedEvidence));
});

test("C. N=2 does not claim consistently recurring", () => {
  const out = applyExecutiveCopyGovernance([
    {
      findingType: "STRONGEST_VALIDATED_ASSOCIATION",
      associationAttributeId: "DISTRIBUTION_LOYALTY",
      providerCount: 2,
      observationCount: 2,
      headline: "Brand association",
      title: "Observed AI Association",
    },
  ]).findings[0];
  assert.ok(!/\bconsisten|recurring\b/i.test(out.governedEvidence));
});

test("D. truth gap N=1 body says one observed response", () => {
  const out = applyExecutiveCopyGovernance([
    {
      findingType: "POTENTIAL_AI_PERCEPTION_GAP",
      headline: "Potential gap",
      brandName: "Westin",
      observationCount: 1,
      governedClassification: "Hard Brand",
      title: "Potential AI Perception Gap",
    },
  ]).findings[0];
  assert.ok(/\bOne observed response\b/i.test(out.governedBody));
});

test("E. Association 48 + Narrative 34 are not blended", () => {
  const out = applyExecutiveCopyGovernance([
    {
      findingType: "STRONGEST_VALIDATED_ASSOCIATION",
      associationAttributeId: "DISTRIBUTION_LOYALTY",
      providerCount: 3,
      observationCount: 48,
      title: "Observed AI Association",
      headline: "Association",
    },
    {
      findingType: "NARRATIVE_PATTERN",
      providerCount: 3,
      comparableResponseCount: 34,
      scenarioCount: 5,
      observationCount: 34,
      title: "Recurring Narrative",
      headline: "Narrative",
    },
  ]).findings;
  assert.ok(/48 qualifying association observations/i.test(out[0].governedEvidence));
  assert.ok(!/34 comparable responses/i.test(out[0].governedEvidence));
  assert.ok(/34 comparable responses/i.test(out[1].governedEvidence));
});

test("F. 3 providers / 34 responses / 5 scenarios valid narrative evidence", () => {
  const out = applyExecutiveCopyGovernance([
    {
      findingType: "NARRATIVE_PATTERN",
      providerCount: 3,
      comparableResponseCount: 34,
      scenarioCount: 5,
      headline: "Narrative",
      title: "Recurring Narrative",
    },
  ]).findings[0];
  assert.ok(/3 providers/i.test(out.governedEvidence));
  assert.ok(/34 comparable responses/i.test(out.governedEvidence));
  assert.ok(/5 owner scenarios/i.test(out.governedEvidence));
});

test("G. source can say cited but not influenced", () => {
  const out = applyExecutiveCopyGovernance([
    {
      findingType: "SOURCE_CITATION_GAP",
      evidenceSummary: "Owned + external sources cited",
      headline: "Source pattern",
      title: "Source Pattern",
    },
  ]).findings[0];
  assert.ok(/\bcited\b/i.test(out.governedEvidence));
  assert.ok(!/\binfluenc|drives|causes\b/i.test(out.governedEvidence));
});

test("H. no denominator available no fabricated fraction", () => {
  const out = applyExecutiveCopyGovernance([
    {
      findingType: "LARGEST_COMPETITIVE_GAP",
      providerCount: 4,
      observationCount: 10,
      headline: "Gap",
      title: "Largest Competitive Gap",
    },
  ]).findings[0];
  assert.ok(!/\d+\s*of\s*\d+/.test(out.governedEvidence));
});

test("I. provider percentages rendered from payload fields", () => {
  const out = applyExecutiveCopyGovernance([
    {
      findingType: "PROVIDER_DISAGREEMENT",
      providerStrongLabel: "OpenAI",
      providerWeakLabel: "Perplexity",
      providerStrongPct: "91.7%",
      providerWeakPct: "58.3%",
      headline: "Provider comparison",
      title: "Provider Comparison",
    },
  ]).findings[0];
  assert.ok(/OpenAI 91.7% vs Perplexity 58.3%/i.test(out.governedEvidence));
});

const pass = results.filter((r) => r.ok).length;
const fail = results.length - pass;
console.log(`\nExecutive copy governance tests: ${pass} passed, ${fail} failed\n`);
if (fail > 0) process.exit(1);
