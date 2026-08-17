/**
 * Unit tests for Operator Explorer section pattern parity.
 */
import assert from "node:assert/strict";
import {
  evaluateOperatorSectionPatternParity,
  evaluateMarketsFootprintPattern,
  evaluateLeadershipPattern,
  OPERATOR_SECTION_PATTERN_IDS,
} from "../lib/partner-intelligence/operator-explorer-section-pattern-parity.js";
import { loadOperatorFixturePayload } from "../lib/partner-intelligence/operator-explorer-fixture-payload.js";
import { runOperatorSectionPatternParityAudit } from "../lib/partner-intelligence/operator-explorer-section-pattern-parity-audit.js";

function main() {
  assert.equal(OPERATOR_SECTION_PATTERN_IDS.length, 8);

  const empty = evaluateOperatorSectionPatternParity({
    operatorSlug: "empty",
    operatorName: "Empty Op",
    prefill: {},
  });
  assert.equal(empty.pass, false);
  assert.ok(empty.failCount >= 6);

  // Honest zero + team XP should pass markets pattern
  const markets = evaluateMarketsFootprintPattern({
    prefill: {
      mkt_regional_expertise_json: [
        {
          title: "Current CALA Operating Footprint",
          description:
            "Operator does not currently manage hotels in CALA. Profile reflects team experience and Mexico City hub.",
        },
        {
          title: "Team Experience",
          description: "Representative experience across Mexico, Colombia, and Caribbean markets.",
        },
      ],
    },
  });
  assert.equal(markets.pass, true, markets.detail);

  const leadership = evaluateLeadershipPattern({
    prefill: {
      lead_org_structure_json: [
        { title: "A", description: "Regional leadership hub for CALA growth." },
        { title: "B", description: "Enterprise platform coordination." },
      ],
      lead_team_depth_json: [
        { function: "Ops", leadRole: "COO", depth: "Strong", relevance: "Portfolio ops." },
        { function: "Finance", leadRole: "CFO", depth: "Strong", relevance: "Owner reporting." },
        { function: "Commercial", leadRole: "CRO", depth: "Strong", relevance: "Revenue." },
      ],
      lead_language_capability_json: [
        { language: "English", proficiency: "Fluent", support: "Owners" },
        { language: "Spanish", proficiency: "Fluent", support: "CALA markets" },
      ],
    },
  });
  assert.equal(leadership.pass, true, leadership.detail);

  const arbor = loadOperatorFixturePayload("arbor-lodging-cala");
  const arborEval = evaluateOperatorSectionPatternParity({
    operatorSlug: arbor.slug,
    operatorName: arbor.companyName,
    recordId: arbor.recordId,
    prefill: arbor.prefill,
    source: "fixtures",
  });
  assert.equal(arborEval.sections.length, 8);
  console.log(
    JSON.stringify(
      {
        ok: true,
        emptyFailCount: empty.failCount,
        arborPass: arborEval.pass,
        arborFailing: arborEval.failingSectionIds,
        arborPassCount: arborEval.passCount,
      },
      null,
      2
    )
  );
}

async function smokeAudit() {
  const report = await runOperatorSectionPatternParityAudit({
    operators: ["arbor-lodging-cala", "hotel-equities-cala"],
    source: "fixtures",
  });
  assert.equal(report.operatorResults.length, 2);
  console.log(
    JSON.stringify(
      {
        smoke: true,
        auditPass: report.auditPass,
        perOperator: report.operatorResults.map((o) => ({
          slug: o.operatorSlug,
          pass: o.pass,
          failing: o.failingSectionIds,
        })),
      },
      null,
      2
    )
  );
}

try {
  main();
  await smokeAudit();
} catch (err) {
  console.error("[test:operator-explorer-section-pattern-parity]", err?.stack || err?.message || err);
  process.exitCode = 1;
}
