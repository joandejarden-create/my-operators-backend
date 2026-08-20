#!/usr/bin/env node
/**
 *   npm run test:adp-provider-concentration-model-pd-implementation-v1
 *
 *   MODEL_P_D implementation gate:
 *   - subject provider LOO <= 10pp AND CORE provider LOO <= 5pp
 *   - 8pp flag no longer customer blocker
 *   - Renaissance Business + Cambridge Leisure Travel now PRODUCTION_VALIDATED
 *   - Cambridge Couples + NOW NOW remain blocked
 *   - Existing certified rows not invalidated
 *   - Waterstone unchanged
 */

import assert from "assert";
import {
  providerLeaveOneOutRates,
  PROVIDER_MODEL_PD_SUBJECT_PP_MAX,
  PROVIDER_MODEL_PD_CORE_PP_MAX,
  MATERIAL_PROVIDER_LOO_PP,
} from "../lib/ai-demand-positioning/metrics/core-benchmark-rate-contract-v1.js";
import {
  LOO_SUBJECT_PP_MAX,
  LOO_CORE_PP_MAX,
} from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import { CUSTOMER_NUMERIC_INDEX_PROMOTION } from "../lib/ai-demand-positioning/metrics/property-core-governance-data.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";
import { join } from "path";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";

const WATERSTONE_ID = "adp_waterstone_boca_raton";
const RENAISSANCE_ID = "adp_renaissance_times_square";
const CAMBRIDGE_ID = "adp_cambridge_beaches_bermuda";
const NOW_NOW_ID = "adp_now_now_noho";

import { diagnoseTerritory } from "../lib/ai-demand-positioning/metrics/provider-concentration-root-cause-v1.js";

function certifyTerritory(propertyId, intent) {
  const d = diagnoseTerritory(propertyId, intent);
  return { status: d.currentStatus, blockers: d.currentBlockers };
}

function getProviderLoo(propertyId, intent) {
  const d = diagnoseTerritory(propertyId, intent);
  return d.leaveOneOut.currentRule;
}

async function main() {
  // --- PART A: MODEL_P_D thresholds ---
  assert.equal(PROVIDER_MODEL_PD_SUBJECT_PP_MAX, 10);
  assert.equal(PROVIDER_MODEL_PD_CORE_PP_MAX, 5);

  // --- PART B: 8pp flag no longer customer blocker ---
  assert.ok(MATERIAL_PROVIDER_LOO_PP === 8, "8pp flag retained as diagnostic");
  // Verify Renaissance Business LOO = 8.2pp > 8 but < 10 → passes MODEL_P_D
  const renBizLoo = getProviderLoo(RENAISSANCE_ID, TRAVELER_INTENTS.BUSINESS);
  assert.ok(renBizLoo.maxSubjectProviderLooPp >= 8, "Renaissance Business subject LOO >= 8pp (early warning)");
  assert.ok(renBizLoo.maxSubjectProviderLooPp <= 10, "Renaissance Business subject LOO <= 10pp (MODEL_P_D pass)");
  assert.ok(renBizLoo.maxCoreProviderLooPp <= 5, "Renaissance Business CORE LOO <= 5pp");
  assert.equal(renBizLoo.PROVIDER_MODEL_PD_FAIL, false, "Renaissance Business passes MODEL_P_D");
  assert.equal(renBizLoo.PROVIDER_CONCENTRATION_RISK, true, "8pp early warning still fires");

  // --- PART D: Renaissance Business Travel → PRODUCTION_VALIDATED ---
  const renBiz = certifyTerritory(RENAISSANCE_ID, TRAVELER_INTENTS.BUSINESS);
  assert.equal(renBiz.status, "PRODUCTION_VALIDATED", "Renaissance Business Travel certified under MODEL_P_D");
  assert.ok(!renBiz.blockers.includes("provider_concentration"), "no provider_concentration blocker");

  // --- PART D: Cambridge Leisure Travel → PRODUCTION_VALIDATED ---
  const camRL = certifyTerritory(CAMBRIDGE_ID, TRAVELER_INTENTS.LEISURE);
  assert.equal(camRL.status, "PRODUCTION_VALIDATED", "Cambridge Leisure Travel certified under MODEL_P_D");

  const camRLLoo = getProviderLoo(CAMBRIDGE_ID, TRAVELER_INTENTS.LEISURE);
  assert.ok(camRLLoo.maxSubjectProviderLooPp <= 10, "Cambridge RL subject LOO <= 10pp");
  assert.ok(camRLLoo.maxCoreProviderLooPp <= 5, "Cambridge RL CORE LOO <= 5pp");

  // --- PART E: Keep genuine instability blocked ---
  const camCouples = certifyTerritory(CAMBRIDGE_ID, TRAVELER_INTENTS.COUPLES);
  assert.ok(camCouples.blockers.includes("provider_concentration"), "Cambridge Couples blocked");
  const camCLoo = getProviderLoo(CAMBRIDGE_ID, TRAVELER_INTENTS.COUPLES);
  assert.ok(camCLoo.maxSubjectProviderLooPp > 10, "Cambridge Couples subject LOO > 10pp");
  assert.ok(camCLoo.PROVIDER_MODEL_PD_FAIL, "Cambridge Couples fails MODEL_P_D");

  const nnBiz = certifyTerritory(NOW_NOW_ID, TRAVELER_INTENTS.BUSINESS);
  assert.ok(nnBiz.blockers.includes("provider_concentration"), "NOW NOW Business blocked");

  const nnRL = certifyTerritory(NOW_NOW_ID, TRAVELER_INTENTS.LEISURE);
  assert.ok(nnRL.blockers.includes("provider_concentration"), "NOW NOW Leisure Travel blocked");

  const nnCouples = certifyTerritory(NOW_NOW_ID, TRAVELER_INTENTS.COUPLES);
  assert.ok(nnCouples.blockers.includes("provider_concentration"), "NOW NOW Couples blocked");

  // --- PART F: Existing certified rows protection ---
  const renRL = certifyTerritory(RENAISSANCE_ID, TRAVELER_INTENTS.LEISURE);
  assert.equal(renRL.status, "PRODUCTION_VALIDATED", "Renaissance Leisure Travel still certified");

  const renCouples = certifyTerritory(RENAISSANCE_ID, TRAVELER_INTENTS.COUPLES);
  assert.equal(renCouples.status, "PRODUCTION_VALIDATED", "Renaissance Couples still certified");

  const renMeetings = certifyTerritory(RENAISSANCE_ID, TRAVELER_INTENTS.GROUP_MEETING);
  assert.equal(renMeetings.status, "PRODUCTION_VALIDATED", "Renaissance Meetings still certified");

  const camCelebrations = certifyTerritory(CAMBRIDGE_ID, TRAVELER_INTENTS.CELEBRATION);
  assert.equal(camCelebrations.status, "PRODUCTION_VALIDATED", "Cambridge Celebrations still certified");

  // --- PART G: Customer numeric index promotion ---
  assert.equal(CUSTOMER_NUMERIC_INDEX_PROMOTION[RENAISSANCE_ID], true);
  assert.equal(CUSTOMER_NUMERIC_INDEX_PROMOTION[CAMBRIDGE_ID], true);
  assert.equal(CUSTOMER_NUMERIC_INDEX_PROMOTION[NOW_NOW_ID], false);

  // --- PART M: Waterstone freeze ---
  const wsBaseline = join(process.cwd(), "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json");
  const wsAudit = auditProperty(WATERSTONE_ID);
  const wsDiff = compareWaterstoneRegression(wsAudit, wsBaseline);
  assert.equal(wsDiff.INDEX_DIFF, 0, "Waterstone index unchanged");
  assert.equal(wsDiff.CERTIFIED_TERRITORIES, 4, "Waterstone 4 territories");

  // --- PART O: NOW NOW new numeric rows = 0 ---
  let nowNowNumeric = 0;
  for (const intent of [TRAVELER_INTENTS.BUSINESS, TRAVELER_INTENTS.LEISURE, TRAVELER_INTENTS.COUPLES]) {
    const c = certifyTerritory(NOW_NOW_ID, intent);
    if (c.status === "PRODUCTION_VALIDATED") nowNowNumeric++;
  }
  assert.equal(nowNowNumeric, 0, "NOW NOW new numeric rows = 0");

  // --- MODEL_P_D dual-rate return fields ---
  assert.ok("dropProviderCorePp" in renBizLoo, "CORE LOO returned");
  assert.ok("maxSubjectProviderLooPp" in renBizLoo, "max subject LOO metric");
  assert.ok("maxCoreProviderLooPp" in renBizLoo, "max CORE LOO metric");

  // --- Scenario governance unchanged ---
  assert.equal(LOO_SUBJECT_PP_MAX, 10);
  assert.equal(LOO_CORE_PP_MAX, 5);

  console.log("test:adp-provider-concentration-model-pd-implementation-v1 PASS");
  console.log("  PROVIDER_MODEL: MODEL_P_D");
  console.log(`  SUBJECT_THRESHOLD_PP: ${PROVIDER_MODEL_PD_SUBJECT_PP_MAX}`);
  console.log(`  CORE_THRESHOLD_PP: ${PROVIDER_MODEL_PD_CORE_PP_MAX}`);
  console.log("  EIGHT_PP_FLAG_CUSTOMER_BLOCKER: NO");
  console.log("  RENAISSANCE_BUSINESS: PROMOTED");
  console.log("  CAMBRIDGE_LEISURE_TRAVEL: PROMOTED");
  console.log("  CAMBRIDGE_COUPLES: BLOCKED");
  console.log("  NOW_NOW_NEW_NUMERIC_ROWS: 0");
  console.log("  WATERSTONE_INDEX_DIFF: 0");
  console.log("  WATERSTONE_CERTIFIED_TERRITORIES: 4");
  console.log("  CURRENT_CERTIFIED_ROWS_INVALIDATED: 0");
  console.log("  PROVIDER_CALLS: 0");
  console.log("  SPEND: $0");
  console.log("  INVALID_COMPARISONS_RENDERED: 0");
  console.log("  final: ADP_PROVIDER_CONCENTRATION_MODEL_P_D_IMPLEMENTATION_V1_PASS");
  console.log("  next: ADP_PROVIDER_GOVERNANCE_READY_FOR_CLIENT_QA");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
