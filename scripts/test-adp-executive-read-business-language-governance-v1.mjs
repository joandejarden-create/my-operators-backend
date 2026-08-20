#!/usr/bin/env node
/**
 *   npm run test:adp-executive-read-business-language-governance-v1
 */

import assert from "assert";
import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods, isTargetedMeasurementPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  buildExecutiveReadWithUx,
  translatePresenceIndex,
  EXECUTIVE_SUMMARY_TITLE,
  PROPERTY_SPECIFIC_EXECUTIVE_COPY_CODE,
  BUSINESS_LANGUAGE_GOVERNANCE_VERSION,
} from "../lib/ai-demand-positioning/customer/executive-read-v2.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";
import { join } from "path";

const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

const PROPERTIES = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
];

const BARE_JARGON = [
  /\bCORE benchmark\b/i,
  /\bAI Presence Index\s*[:=]?\s*\d+/i,
  /\bAI Scenario Presence is\b/i,
  /\bAI Consideration Rate is\b/i,
  /\bProperty Reality Coverage is\b/i,
  /\bdemand scenarios\b/i,
  /\brank-eligible\b/i,
];

const LEADING_METRIC_NAME = [
  /^AI Scenario Presence/im,
  /^AI Consideration Rate/im,
  /^Property Reality Coverage/im,
  /^AI Presence Index/im,
];

function firstTimeOwnerComprehension(ux) {
  const text = [ux.biggestStrength.body, ux.biggestConstraint.body, ux.changeSinceLastRun.body, ux.executiveSummary.narrative].join(" ");
  const hasBusinessBreadth =
    /appeared in at least one AI answer|traveler needs we tested|recognized .+ as relevant/i.test(text);
  const hasBusinessConsistency =
    /individual AI answers|consistently see|appears? in only|appeared .+ of the time/i.test(text) ||
    /still developing|no single dominant/i.test(text);
  const unexplainedCore = /\bCORE\b/.test(text) && !/comparable hotels used/i.test(text);
  const unexplainedIndex = /\bAI Presence Index\b/i.test(text) && !/times as often|more often|less often/i.test(text);
  const leadsWithMetric = LEADING_METRIC_NAME.some((re) => re.test(ux.biggestStrength.body) || re.test(ux.biggestConstraint.body));
  return {
    pass: hasBusinessBreadth && !unexplainedCore && !unexplainedIndex && !leadsWithMetric && hasBusinessConsistency,
    unexplainedCore,
    unexplainedIndex,
    leadsWithMetric,
  };
}

async function main() {
  assert.equal(EXECUTIVE_SUMMARY_TITLE, "WHAT THE DATA SAYS");
  assert.equal(PROPERTY_SPECIFIC_EXECUTIVE_COPY_CODE, 0);
  assert.ok(translatePresenceIndex(764).includes("7.6 times"));
  assert.ok(translatePresenceIndex(120).includes("20% more"));
  assert.ok(translatePresenceIndex(80).includes("20% less"));
  assert.ok(translatePresenceIndex(100).includes("as often"));

  const multi = {};
  let totalUndefined = 0;
  let totalDup = 0;
  let totalCausal = 0;
  let totalMarketing = 0;

  for (const propertyId of PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const allPeriods = loadAllPeriods(propertyId).filter((p) => !isTargetedMeasurementPeriod(p));
    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods });
    const ux = payload.executiveRead.ux;

    assert.ok(ux, `${propertyId} ux`);
    assert.equal(ux.businessLanguageVersion, BUSINESS_LANGUAGE_GOVERNANCE_VERSION);
    assert.equal(ux.PROPERTY_SPECIFIC_EXECUTIVE_COPY_CODE, 0);
    assert.equal(ux.executiveSummary.title, EXECUTIVE_SUMMARY_TITLE);
    assert.ok(ux.biggestStrength.headline && ux.biggestStrength.body);
    assert.ok(ux.biggestConstraint.headline && ux.biggestConstraint.body);
    assert.ok(ux.changeSinceLastRun.body);
    assert.ok(ux.executiveSummary.narrative);

    const combined = [ux.biggestStrength.body, ux.biggestConstraint.body, ux.changeSinceLastRun.body, ux.executiveSummary.narrative].join("\n");
    for (const re of BARE_JARGON) {
      if (re.source.includes("CORE") && /comparable hotels used/i.test(combined)) continue;
      if (re.source.includes("Presence Index") && /times as often|more often|less often/i.test(combined)) continue;
      assert.ok(!re.test(combined) || /comparable hotels used|traveler needs|individual AI answers|monitored facts/i.test(combined),
        `${propertyId} bare jargon: ${re}`);
    }

    assert.equal(ux.safety.UNDEFINED_JARGON, 0, `${propertyId} UNDEFINED_JARGON`);
    assert.equal(ux.safety.DUPLICATE_EXECUTIVE_FINDING_WITHOUT_NEW_MEANING, 0, `${propertyId} duplicates`);
    assert.equal(ux.safety.UNSUPPORTED_CAUSAL_CLAIMS, 0, `${propertyId} causal`);
    assert.equal(ux.safety.MARKETING_STYLE_LANGUAGE, 0, `${propertyId} marketing`);
    assert.equal(ux.safety.PLAIN_BUSINESS_MEANING_FIRST, 1);

    const comprehension = firstTimeOwnerComprehension(ux);
    assert.ok(comprehension.pass, `${propertyId} first-time owner fail: ${JSON.stringify(comprehension)}`);

    // Prefer business phrasing over leading with formal metric names
    assert.ok(!/^AI Scenario Presence/i.test(ux.biggestStrength.body));
    assert.ok(!/^AI Consideration Rate/i.test(ux.biggestConstraint.body));
    assert.ok(!/^Property Reality Coverage is/i.test(ux.biggestConstraint.body));

    if (/Strong visibility for/i.test(ux.biggestStrength.headline)) {
      assert.ok(/comparable hotels used in this analysis/i.test(ux.biggestStrength.body), `${propertyId} CORE explained`);
      assert.ok(/times as often|more often|less often|as often as/i.test(ux.biggestStrength.body), `${propertyId} index translated`);
    }

    totalUndefined += ux.safety.UNDEFINED_JARGON;
    totalDup += ux.safety.DUPLICATE_EXECUTIVE_FINDING_WITHOUT_NEW_MEANING;
    totalCausal += ux.safety.UNSUPPORTED_CAUSAL_CLAIMS;
    totalMarketing += ux.safety.MARKETING_STYLE_LANGUAGE;

    const published = await getPublishedOwnerReport(propertyId);
    assert.equal(published.payload.executiveRead.ux.executiveSummary.title, EXECUTIVE_SUMMARY_TITLE);

    multi[propertyId] = {
      FIRST_TIME_OWNER_COMPREHENSION: comprehension.pass ? "PASS" : "FAIL",
      UNDEFINED_JARGON: ux.safety.UNDEFINED_JARGON,
      DUPLICATE_FINDINGS: ux.safety.DUPLICATE_EXECUTIVE_FINDING_WITHOUT_NEW_MEANING,
      UNSUPPORTED_CAUSAL_CLAIMS: ux.safety.UNSUPPORTED_CAUSAL_CLAIMS,
    };
  }

  const wsProfile = loadPropertyProfile("adp_waterstone_boca_raton");
  const wsPeriod = loadLatestPeriod("adp_waterstone_boca_raton");
  const wsScenarios = buildScenarioUniverse(wsProfile);
  const wsPayload = buildOwnerPayload(wsPeriod, wsScenarios, wsProfile, {
    allPeriods: loadAllPeriods("adp_waterstone_boca_raton").filter((p) => !isTargetedMeasurementPeriod(p)),
  });
  const wsUx = wsPayload.executiveRead.ux;
  const wsText = [wsUx.biggestStrength.body, wsUx.biggestConstraint.body, wsUx.executiveSummary.narrative].join(" ");
  assert.ok(/traveler needs we tested|traveler needs tested/i.test(wsText));
  assert.ok(/individual AI answers/i.test(wsText));
  assert.ok(/comparable hotels used in this analysis/i.test(wsText) || /Still developing/i.test(wsUx.biggestStrength.headline));

  const wsAudit = auditProperty("adp_waterstone_boca_raton");
  const wsRegression = compareWaterstoneRegression(wsAudit, WATERSTONE_BASELINE);
  assert.equal(wsRegression.INDEX_DIFF, 0);

  console.log("test:adp-executive-read-business-language-governance-v1 PASS");
  console.log("  PLAIN_BUSINESS_MEANING_FIRST: YES");
  console.log("  FORMAL_METRIC_NAME_REQUIRED_FOR_COMPREHENSION: NO");
  console.log("  FIRST_TIME_OWNER_STANDARD: PASS");
  console.log("  TITLE:", EXECUTIVE_SUMMARY_TITLE);
  console.log("  UNDEFINED_JARGON:", totalUndefined);
  console.log("  DUPLICATE_EXECUTIVE_FINDING_WITHOUT_NEW_MEANING:", totalDup);
  console.log("  UNSUPPORTED_CAUSAL_CLAIMS:", totalCausal);
  console.log("  MARKETING_STYLE_LANGUAGE:", totalMarketing);
  console.log("  WATERSTONE:", multi["adp_waterstone_boca_raton"].FIRST_TIME_OWNER_COMPREHENSION);
  console.log("  RENAISSANCE:", multi["adp_renaissance_times_square"].FIRST_TIME_OWNER_COMPREHENSION);
  console.log("  CAMBRIDGE:", multi["adp_cambridge_beaches_bermuda"].FIRST_TIME_OWNER_COMPREHENSION);
  console.log("  NOW_NOW:", multi["adp_now_now_noho"].FIRST_TIME_OWNER_COMPREHENSION);
  console.log("  WATERSTONE_INDEX_DIFF:", wsRegression.INDEX_DIFF);
  console.log("  final: ADP_EXECUTIVE_READ_BUSINESS_LANGUAGE_GOVERNANCE_V1_PASS");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
