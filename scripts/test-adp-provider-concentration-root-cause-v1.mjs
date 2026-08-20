#!/usr/bin/env node
/**
 *   npm run test:adp-provider-concentration-root-cause-v1
 */

import assert from "assert";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import {
  runProviderConcentrationRootCauseV1,
  CURRENT_PROVIDER_CONCENTRATION_RULE,
  PROVIDER_CONCENTRATION_AFFECTED,
  CURRENTLY_CERTIFIED_TERRITORIES,
  diagnoseTerritory,
  decomposeProviderRates,
  computeProviderLeaveOneOutFull,
  auditTrendComparisonTerminology,
} from "../lib/ai-demand-positioning/metrics/provider-concentration-root-cause-v1.js";
import { MATERIAL_PROVIDER_LOO_PP } from "../lib/ai-demand-positioning/metrics/core-benchmark-rate-contract-v1.js";
import {
  LOO_SUBJECT_PP_MAX,
  LOO_CORE_PP_MAX,
} from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import { CUSTOMER_NUMERIC_INDEX_PROMOTION } from "../lib/ai-demand-positioning/metrics/property-core-governance-data.js";
import { loadPropertyProfile, loadLatestTargetedPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { parsePeriodObservations } from "../lib/ai-demand-positioning/execution/response-parser.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { providerLeaveOneOutRates } from "../lib/ai-demand-positioning/metrics/core-benchmark-rate-contract-v1.js";

const REPORT = join(process.cwd(), "reports/ai-demand-positioning/provider-concentration-root-cause-v1.json");
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

async function main() {
  const report = runProviderConcentrationRootCauseV1();

  assert.equal(report.execution.PROVIDER_CALLS, 0);
  assert.equal(report.execution.SPEND, 0);
  assert.equal(report.existingCertifiedProtection.CURRENT_CERTIFIED_ROWS_INVALIDATED, 0);
  assert.equal(report.trendSafetyAudit.INVALID_COMPARISONS_RENDERED, 0);
  assert.equal(report.trendSafetyAudit.INVALID_COMPARISONS_DETECTED, 3);
  assert.equal(report.trendSafetyAudit.INVALID_COMPARISONS_BLOCKED, 3);
  assert.equal(report.candidatePromotionDeferred, true);
  assert.equal(report.waterstone.INDEX_DIFF, 0);

  assert.ok(CURRENT_PROVIDER_CONCENTRATION_RULE.THRESHOLD.includes(String(MATERIAL_PROVIDER_LOO_PP)));
  assert.equal(CURRENT_PROVIDER_CONCENTRATION_RULE.THRESHOLD_ORIGIN, "HEURISTIC");

  assert.equal(report.affectedTerritoryDiagnostics.length, PROVIDER_CONCENTRATION_AFFECTED.length);
  for (const row of report.affectedTerritoryDiagnostics) {
    assert.ok(row.providerDecomposition.length === 4);
    assert.ok(row.SUBJECT_PROVIDER_RANGE_PP >= 0);
    assert.ok(row.leaveOneOut?.dropProviderSubjectPp);
  }

  const MODEL_PD_STILL_BLOCKED = PROVIDER_CONCENTRATION_AFFECTED.filter(
    ({ propertyId, intent }) => {
      const d = diagnoseTerritory(propertyId, intent);
      return d.currentBlockers.includes("provider_concentration");
    }
  );
  // Under MODEL_P_D: Cambridge Couples + 3 NOW NOW remain blocked; Renaissance Business + Cambridge Leisure Travel pass
  assert.ok(MODEL_PD_STILL_BLOCKED.length >= 4, `at least 4 genuinely unstable territories remain blocked`);

  for (const { propertyId, intent } of PROVIDER_CONCENTRATION_AFFECTED) {
    const d = diagnoseTerritory(propertyId, intent);
    const period = loadLatestTargetedPeriod(propertyId);
    const profile = loadPropertyProfile(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    parsePeriodObservations(period, profile);
    const decomp = decomposeProviderRates(period.observations, scenarios, intent, profile);
    for (const p of decomp.providerRows) {
      if (p.included && p.YOUR_AI_PRESENCE != null) {
        assert.ok(p.SUBJECT_OBSERVATION_N >= 0);
      }
      assert.ok(p.FAILED_MISSING_OBSERVATION_N >= 0 || p.FAILED_MISSING_OBSERVATION_N === 0);
    }
    const loo = computeProviderLeaveOneOutFull(period.observations, scenarios, intent, profile);
    assert.ok(loo.BASE_SUBJECT_RATE != null);
    assert.ok(loo.BASE_CORE_RATE != null);
    const current = providerLeaveOneOutRates(period.observations, scenarios, intent, profile);
    if (current.PROVIDER_CONCENTRATION_RISK) {
      const maxDrop = Math.max(...Object.values(current.dropProviderSubjectPp).filter((n) => n != null));
      assert.ok(maxDrop >= MATERIAL_PROVIDER_LOO_PP);
    }
  }

  const modelA = report.candidateModels.find((m) => m.MODEL === "MODEL_P_A");
  const modelD = report.candidateModels.find((m) => m.MODEL === "MODEL_P_D");
  assert.ok(modelA.PASSING_TERRITORIES <= modelD.PASSING_TERRITORIES);

  assert.equal(report.recommendedGovernance.CURRENT_RULE_KEEP, "NO");
  assert.ok(report.recommendedGovernance.RECOMMENDED_RULE.includes("MODEL_P_D"));

  for (const row of report.existingCertifiedProtection.rows) {
    assert.equal(row.INVALIDATED, false);
    assert.equal(row.MODEL_P_D_STATUS, "PRODUCTION_VALIDATED");
  }

  assert.ok(CUSTOMER_NUMERIC_INDEX_PROMOTION.adp_renaissance_times_square);
  assert.ok(CUSTOMER_NUMERIC_INDEX_PROMOTION.adp_waterstone_boca_raton);

  const waterstone = auditProperty("adp_waterstone_boca_raton");
  const regression = compareWaterstoneRegression(waterstone, WATERSTONE_BASELINE);
  assert.equal(regression.INDEX_DIFF, 0);

  const brandSmoke = await getPublishedOwnerReport("adp_waterstone_boca_raton");
  assert.ok(brandSmoke.ok !== false);

  const trend = auditTrendComparisonTerminology();
  assert.equal(trend.INVALID_COMPARISONS_RENDERED, 0);

  assert.ok(existsSync(REPORT));

  console.log("test:adp-provider-concentration-root-cause-v1 PASS");
  console.log("  answer:", report.primaryQuestion.ANSWER.slice(0, 80) + "...");
  console.log("  structural sensitivity:", report.primaryQuestion.structuralGateSensitivityCount);
  console.log("  genuine instability:", report.primaryQuestion.genuineInstabilityCount);
  console.log("  candidate certifiable:", report.candidateNewlyCertifiableRows.length);
  console.log("  final:", report.final);
  console.log("  next:", report.next);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
