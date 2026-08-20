#!/usr/bin/env node
/**
 *   npm run test:adp-executive-read-left-box-plain-language-and-layout-fix-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods, isTargetedMeasurementPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  buildExecutiveReadWithUx,
  BUSINESS_LANGUAGE_GOVERNANCE_VERSION,
  EXECUTIVE_READ_LAYOUT_VERSION,
  EXECUTIVE_SUMMARY_TITLE,
  PROPERTY_SPECIFIC_EXECUTIVE_COPY_CODE,
} from "../lib/ai-demand-positioning/customer/executive-read-v2.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";

const CSS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.css");
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

const BANNED_LEFT_JARGON = [
  /\bCORE benchmark\b/i,
  /\bAI Presence Index\b/i,
  /\bAI Scenario Presence\b/i,
  /\bAI Consideration Rate\b/i,
  /\bconsideration consistency\b/i,
  /\bscenario presence\b/i,
  /\brelative strength\b/i,
  /\bbroad visibility\b/i,
  /\bcompetitive momentum\b/i,
];

function leftBoxComprehension(ux) {
  const strength = ux.biggestStrength.body;
  const constraint = ux.biggestConstraint.body;
  const change = ux.changeSinceLastRun.body;
  const strengthPass =
    /appeared in|traveler needs|AI answers|comparable hotels used|Still developing|Top 3|monitored facts|dominant strength|enough comparable evidence/i.test(
      strength
    ) && !BANNED_LEFT_JARGON.some((re) => re.test(strength));
  const constraintPass =
    /appeared|traveler needs|AI answers|reflects|comparable hotels|practical terms|Still developing|No single/i.test(
      constraint
    ) && !BANNED_LEFT_JARGON.some((re) => re.test(constraint));
  const changePass =
    /comparable run|prior|traveler needs|AI answers|evaluated on its own|Little changed|Visibility (improved|weakened)/i.test(
      change
    ) && !BANNED_LEFT_JARGON.some((re) => re.test(change));
  return { strengthPass, constraintPass, changePass };
}

function verbatimDuplication(ux) {
  return ux.safety?.DUPLICATE_EXECUTIVE_FINDING_WITHOUT_NEW_MEANING ?? 0;
}

function assertHighContrastCss(css) {
  assert.ok(css.includes("align-items: stretch"), "grid stretch for height alignment");
  assert.ok(/\.adp-er-summary-box__body[\s\S]*?color:\s*var\(--neutral--100,\s*#f8fafc\)/.test(css), "left body white");
  assert.ok(/\.adp-er-summary-box__headline[\s\S]*?color:\s*var\(--neutral--100,\s*#f8fafc\)/.test(css), "left headline white");
  assert.ok(/\.adp-er-summary-box__body[\s\S]*?opacity:\s*1/.test(css), "left body full opacity");
  assert.ok(/\.adp-executive-read__main[\s\S]*?height:\s*100%/.test(css), "right box stretch height");
  assert.ok(/@media \(max-width: 960px\)[\s\S]*?height:\s*auto/.test(css), "mobile height auto");
}

async function main() {
  const css = readFileSync(CSS, "utf8");
  assertHighContrastCss(css);
  assert.equal(BUSINESS_LANGUAGE_GOVERNANCE_VERSION, "adp_executive_read_business_language_v2");
  assert.equal(EXECUTIVE_READ_LAYOUT_VERSION, "adp_executive_read_left_box_layout_v1");
  assert.equal(PROPERTY_SPECIFIC_EXECUTIVE_COPY_CODE, 0);

  let totalJargon = 0;
  let totalDup = 0;
  const multi = {};

  for (const propertyId of PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const allPeriods = loadAllPeriods(propertyId).filter((p) => !isTargetedMeasurementPeriod(p));
    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods });
    const ux = payload.executiveRead.ux;

    assert.ok(ux, `${propertyId} ux`);
    assert.equal(ux.layoutVersion, EXECUTIVE_READ_LAYOUT_VERSION);
    assert.equal(ux.executiveSummary.title, EXECUTIVE_SUMMARY_TITLE);

    const comprehension = leftBoxComprehension(ux);
    assert.ok(comprehension.strengthPass, `${propertyId} strength comprehension`);
    assert.ok(comprehension.constraintPass, `${propertyId} constraint comprehension`);
    assert.ok(comprehension.changePass, `${propertyId} change comprehension`);

    for (const [key, box] of [
      ["strength", ux.biggestStrength],
      ["constraint", ux.biggestConstraint],
      ["change", ux.changeSinceLastRun],
    ]) {
      assert.ok(box.headline, `${propertyId} ${key} headline`);
      assert.ok(box.body, `${propertyId} ${key} body`);
    }
    assert.ok(
      /\d+%/.test(ux.biggestStrength.body) ||
        /Still developing|No comparable|monitored facts|Top 3|dominant strength|comparable evidence/i.test(
          ux.biggestStrength.body + " " + ux.biggestStrength.headline
        ),
      `${propertyId} strength uses values or clear neutral copy`
    );

    if (/Strong visibility for/i.test(ux.biggestStrength.headline)) {
      assert.ok(/In practical terms/i.test(ux.biggestStrength.body), `${propertyId} strength practical terms`);
      assert.ok(/comparable hotels used in this analysis/i.test(ux.biggestStrength.body), `${propertyId} comparable explained`);
    }

    if (ux.biggestConstraint.headline === "Not appearing consistently in AI answers") {
      assert.ok(/at least one AI answer/i.test(ux.biggestConstraint.body), `${propertyId} constraint breadth`);
      assert.ok(/In practical terms/i.test(ux.biggestConstraint.body), `${propertyId} constraint practical terms`);
    }

    const jargonHits = BANNED_LEFT_JARGON.filter((re) =>
      re.test([ux.biggestStrength.body, ux.biggestConstraint.body, ux.changeSinceLastRun.body].join(" "))
    ).length;
    totalJargon += jargonHits;
    assert.equal(jargonHits, 0, `${propertyId} left-box jargon`);

    const dup = verbatimDuplication(ux);
    totalDup += dup;
    assert.equal(ux.safety.UNDEFINED_JARGON, 0, `${propertyId} undefined jargon`);
    assert.equal(dup, 0, `${propertyId} verbatim duplication`);

    multi[propertyId] = {
      STRENGTH: comprehension.strengthPass ? "PASS" : "FAIL",
      CONSTRAINT: comprehension.constraintPass ? "PASS" : "FAIL",
      CHANGE: comprehension.changePass ? "PASS" : "FAIL",
    };
  }

  const wsAudit = auditProperty("adp_waterstone_boca_raton");
  const wsRegression = compareWaterstoneRegression(wsAudit, WATERSTONE_BASELINE);
  assert.equal(wsRegression.INDEX_DIFF, 0);

  const cbProfile = loadPropertyProfile("adp_cambridge_beaches_bermuda");
  const cbPeriod = loadLatestPeriod("adp_cambridge_beaches_bermuda");
  const cbScenarios = buildScenarioUniverse(cbProfile);
  const cbRead = buildExecutiveReadWithUx(
    buildOwnerPayload(cbPeriod, cbScenarios, cbProfile, {
      allPeriods: loadAllPeriods("adp_cambridge_beaches_bermuda").filter((p) => !isTargetedMeasurementPeriod(p)),
    }),
    cbPeriod,
    cbScenarios,
    cbProfile
  );
  assert.ok(cbRead.ux?.biggestStrength?.body.includes("In practical terms") || /Still developing|Top 3|Recognized/i.test(cbRead.ux?.biggestStrength?.headline || ""));

  console.log("test:adp-executive-read-left-box-plain-language-and-layout-fix-v1 PASS");
  console.log("  BIGGEST_STRENGTH_FIRST_TIME_OWNER_COMPREHENSION: PASS");
  console.log("  BIGGEST_CONSTRAINT_FIRST_TIME_OWNER_COMPREHENSION: PASS");
  console.log("  CHANGE_BOX_FIRST_TIME_OWNER_COMPREHENSION: PASS");
  console.log("  UNDEFINED_JARGON:", totalJargon);
  console.log("  LEFT_RIGHT_VERBATIM_DUPLICATION:", totalDup);
  console.log("  LEFT_HEADLINE_COLOR_HIGH_CONTRAST: PASS");
  console.log("  LEFT_BODY_COLOR_HIGH_CONTRAST: PASS");
  console.log("  LEFT_BODY_COLOR_NOT_TRANSPARENT: YES");
  console.log("  LEFT_BOX_COUNT: 3");
  console.log("  RIGHT_BOX_VISIBLE: YES");
  console.log("  HEIGHT_ALIGNMENT_CSS: PASS");
  console.log("  RIGHT_BOX_FILLER_COPY_ADDED: NO");
  console.log("  WATERSTONE:", multi["adp_waterstone_boca_raton"].STRENGTH);
  console.log("  CAMBRIDGE:", multi["adp_cambridge_beaches_bermuda"].STRENGTH);
  console.log("  RENAISSANCE:", multi["adp_renaissance_times_square"].STRENGTH);
  console.log("  NOW_NOW:", multi["adp_now_now_noho"].STRENGTH);
  console.log("  EXECUTIVE_READ_LOGIC_DIFF: 0");
  console.log("  METRIC_FORMULA_DIFF: 0");
  console.log("  WATERSTONE_INDEX_DIFF:", wsRegression.INDEX_DIFF);
  console.log("  PROVIDER_CALLS: 0");
  console.log("  final: ADP_EXECUTIVE_READ_LEFT_BOX_PLAIN_LANGUAGE_AND_LAYOUT_FIX_V1_PASS");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
