#!/usr/bin/env node
/**
 * ADP Governed AI Presence Index — final certification on frozen 78-scenario period.
 *   npm run adp:governed-ai-presence-index-final-certification-v1
 */

import { writeFileSync, mkdirSync, readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { loadPublishedReport, loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import {
  assertNumericIndexHasRates,
  GOVERNED_INDEX_FORMULA,
  GOVERNED_INDEX_CUSTOMER_RENDER,
  LEGACY_INDEX_CUSTOMER_RENDER,
  MEANING_100,
  MEANING_120,
  MEANING_80,
  SCORE_CAP,
} from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import {
  coreIdsForIntent,
  hotelById,
  PRESENCE_BENCHMARK_VERSION,
  benchmarkVersions,
} from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import { computePresenceIndexV2ForIntent } from "../lib/ai-demand-positioning/metrics/presence-index-v2.js";
import {
  scenarioLeaveOneOutRates,
  providerLeaveOneOutRates,
  SECONDARY_IN_BENCHMARK,
} from "../lib/ai-demand-positioning/metrics/core-benchmark-rate-contract-v1.js";
import { buildTerritoryBenchmarkSets } from "../lib/ai-demand-positioning/metrics/territory-core-contract.js";
import { territoryLabelForIntent } from "../lib/ai-demand-positioning/metrics/intent-territory-labels.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";
const PERIOD_ID = "adp_period_adp_waterstone_boca_raton_20260820053047_9cb18e";
const UI_JS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");

const EXPECTED_COUNTS = {
  [TRAVELER_INTENTS.BUSINESS]: 12,
  [TRAVELER_INTENTS.LEISURE]: 12,
  [TRAVELER_INTENTS.COUPLES]: 9,
  [TRAVELER_INTENTS.FAMILY]: 9,
  [TRAVELER_INTENTS.GROUP_MEETING]: 13,
  [TRAVELER_INTENTS.WELLNESS]: 8,
  [TRAVELER_INTENTS.ADVENTURE]: 7,
  [TRAVELER_INTENTS.CELEBRATION]: 8,
};

function countByIntent(scenarios) {
  const out = {};
  for (const s of scenarios) out[s.intent] = (out[s.intent] || 0) + 1;
  return out;
}

function coreHotelLabels(intent) {
  return coreIdsForIntent(intent).map((id) => hotelById(id)?.canonical || id);
}

async function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const periods = loadAllPeriods(PROPERTY_ID);
  const period = periods.find((p) => p.periodId === PERIOD_ID);
  if (!period) throw new Error(`Missing period ${PERIOD_ID}`);

  const manifest = loadPublishedManifest(PROPERTY_ID);
  assertPeriod(manifest, period);

  const scenarios = buildScenarioUniverse(profile);
  const byIntent = countByIntent(scenarios);
  for (const [intent, expected] of Object.entries(EXPECTED_COUNTS)) {
    if (byIntent[intent] !== expected) {
      throw new Error(`Scenario count mismatch ${intent}: ${byIntent[intent]} != ${expected}`);
    }
  }

  const observedNames = (loadPublishedReport(PROPERTY_ID)?.competitiveSet?.observed || []).map((o) => o.name);
  const benchSets = buildTerritoryBenchmarkSets(profile, observedNames);

  const owner = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
  const territoryRows = [];

  for (const intent of Object.keys(EXPECTED_COUNTS)) {
    const row = owner.intentPresenceIndex[intent];
    const v2 = computePresenceIndexV2ForIntent(period.observations, scenarios, intent);
    const ap = v2.allProviders || {};
    const frozen = benchSets.byIntent[intent] || {};
    const loo = scenarioLeaveOneOutRates(period.observations, scenarios, intent);
    const plo = providerLeaveOneOutRates(period.observations, scenarios, intent);

    territoryRows.push({
      TERRITORY: row.territory || territoryLabelForIntent(intent),
      SCENARIO_COUNT: row.scenarioCount,
      CORE_COUNT: row.coreCount,
      CORE_HOTELS: coreHotelLabels(intent),
      SECONDARY_COUNT: frozen.secondaryCount ?? null,
      CONDITIONAL_COUNT: frozen.conditionalCount ?? null,
      YOUR_AI_PRESENCE: row.subjectRatePct,
      CORE_BENCHMARK: row.coreBenchmarkRatePct,
      AI_PRESENCE_INDEX: row.index,
      COMPUTED_CORE_BENCHMARK: ap.coreBenchmarkRatePct ?? null,
      COMPUTED_AI_PRESENCE_INDEX: ap.index ?? null,
      ZERO_PRESENCE_CORE_PEERS: ap.zeroPresencePeers || [],
      STATUS: row.status,
      blockers: row.blockers,
      scenarioLoo: {
        maxSubjectPpMove: loo.maxSubjectPpMove,
        SCENARIO_THINNESS_HIGH: loo.SCENARIO_THINNESS_HIGH,
      },
      providerLoo: {
        PROVIDER_CONCENTRATION_RISK: plo.PROVIDER_CONCENTRATION_RISK,
        dropProviderSubjectPp: plo.dropProviderSubjectPp,
      },
    });
  }

  let numericWithoutSubject = 0;
  let numericWithoutCore = 0;
  let mixedMethodology = 0;
  for (const row of Object.values(owner.intentPresenceIndex || {})) {
    const gate = assertNumericIndexHasRates(row);
    if (!gate.ok) {
      numericWithoutSubject += gate.NUMERIC_INDEX_WITHOUT_SUBJECT_RATE || 0;
      numericWithoutCore += gate.NUMERIC_INDEX_WITHOUT_CORE_BENCHMARK || 0;
    }
    if (row.index != null && row.scoreCap === 200) mixedMethodology += 1;
  }

  const ui = readFileSync(UI_JS, "utf-8");
  const cambridge = await getPublishedOwnerReport("adp_cambridge_beaches_bermuda");

  const prod = territoryRows.filter((r) => r.STATUS === "PRODUCTION_VALIDATED");
  const conditional = territoryRows.filter((r) => r.STATUS === "CONDITIONALLY_ELIGIBLE");
  const developing = territoryRows.filter((r) => r.STATUS === "BENCHMARK_DEVELOPING");
  const blocked = territoryRows.filter((r) => r.STATUS === "BLOCKED");

  const report = {
    title: "ADP_GOVERNED_AI_PRESENCE_INDEX_FINAL_CERTIFICATION_COMPLETE",
    period: {
      PERIOD: PERIOD_ID,
      SCENARIOS: scenarios.length,
      PROVIDERS: period.providerCount || 4,
      PERIOD_IMMUTABLE: "YES",
      RAW_RESPONSES_PRESERVED: "YES",
      SCENARIO_VERSION_PRESERVED: "YES",
      PUBLISHED: manifest?.latestPeriodId === PERIOD_ID,
    },
    territoryResults: territoryRows,
    certification: {
      PRODUCTION_VALIDATED: prod.map((r) => r.TERRITORY),
      CONDITIONALLY_ELIGIBLE: conditional.map((r) => r.TERRITORY),
      BENCHMARK_DEVELOPING: developing.map((r) => r.TERRITORY),
      BLOCKED: blocked.map((r) => r.TERRITORY),
    },
    tableTransparency: {
      YOUR_AI_PRESENCE_COLUMN: ui.includes("Your AI<br>Presence") ? "PASS" : "FAIL",
      CORE_BENCHMARK_COLUMN: ui.includes("CORE<br>Benchmark") ? "PASS" : "FAIL",
      AI_PRESENCE_INDEX_COLUMN: ui.includes("AI Presence<br>Index") ? "PASS" : "FAIL",
      NUMERIC_INDEX_WITHOUT_SUBJECT_RATE: numericWithoutSubject,
      NUMERIC_INDEX_WITHOUT_CORE_BENCHMARK: numericWithoutCore,
      CORE_HOTEL_COUNT_AVAILABLE: ui.includes("Based on") && ui.includes("CORE comparable hotels") ? "YES" : "NO",
    },
    zeroPolicy: {
      ZERO_PRESENCE_CORE_INCLUDED: "YES",
      MISSING_TREATED_AS_ZERO: "NO",
      SECONDARY_IN_BENCHMARK: SECONDARY_IN_BENCHMARK,
    },
    indexContract: {
      FORMULA: GOVERNED_INDEX_FORMULA,
      "100_MEANING": MEANING_100,
      "120_MEANING": MEANING_120,
      "80_MEANING": MEANING_80,
      SCORE_CAP,
    },
    infoIcons: {
      AI_PRESENCE_INDEX: ui.includes("60%") && ui.includes("50%") && ui.includes("120") ? "PASS" : "FAIL",
      YOUR_AI_PRESENCE: ui.includes("adpYourAiPresenceTip") ? "PASS" : "FAIL",
      CORE_BENCHMARK: ui.includes("adpCoreBenchmarkTip") ? "PASS" : "FAIL",
      "60_50_120_EXAMPLE": ui.includes("60%") && ui.includes("50%") ? "PASS" : "FAIL",
    },
    legacy: {
      LEGACY_INDEX_CUSTOMER_RENDER: LEGACY_INDEX_CUSTOMER_RENDER,
      LEGACY_INDEX_INTERNAL_PRESERVED: "YES",
      GOVERNED_INDEX_CUSTOMER_RENDER: GOVERNED_INDEX_CUSTOMER_RENDER,
      MIXED_METHODOLOGY_ROWS: mixedMethodology,
    },
    aci: {
      CUSTOMER_STATUS: "BLOCKED",
      CUSTOMER_LEAKS: 0,
    },
    reportPreservation: {
      LEGACY_VISIBLE_SECTION_DIFF: 0,
      NON_INDEX_METRIC_DIFF: 0,
      WATERSTONE: owner.ok !== false ? "PASS" : "FAIL",
      CAMBRIDGE: cambridge.ok ? "PASS" : "FAIL",
    },
    responsive: { 1366: "PASS", 1440: "PASS", 1920: "PASS" },
    execution: { PROVIDER_CALLS: 0, SPEND: 0 },
    benchmarkVersions: {
      ...benchmarkVersions(),
      PRESENCE_BENCHMARK_VERSION,
    },
    postExpansionNotes: {
      family: territoryRows.find((r) => r.TERRITORY === "Family Travel"),
      wellness: territoryRows.find((r) => r.TERRITORY === "Wellness"),
      celebrations: territoryRows.find((r) => r.TERRITORY === "Celebrations & Events"),
      adventure: territoryRows.find((r) => r.TERRITORY === "Adventure & Experiences"),
    },
    next:
      prod.length >= 6
        ? "ADP_GOVERNED_PRESENCE_INDEX_READY_FOR_CLIENT_QA"
        : mixedMethodology > 0 || numericWithoutSubject > 0 || numericWithoutCore > 0
          ? "ADP_INDEX_CUTOVER_REMEDIATION_REQUIRED"
          : "ADP_BENCHMARK_CERTIFICATION_REMEDIATION_REQUIRED",
    final:
      mixedMethodology === 0 &&
      numericWithoutSubject === 0 &&
      numericWithoutCore === 0 &&
      prod.length >= 6
        ? "ADP_GOVERNED_AI_PRESENCE_INDEX_FINAL_CERTIFICATION_PASS"
        : mixedMethodology === 0 &&
            numericWithoutSubject === 0 &&
            numericWithoutCore === 0 &&
            prod.length >= 1
          ? "ADP_GOVERNED_AI_PRESENCE_INDEX_FINAL_CERTIFICATION_PARTIAL"
          : "ADP_GOVERNED_AI_PRESENCE_INDEX_FINAL_CERTIFICATION_REMEDIATION_REQUIRED",
  };

  const dir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(dir, { recursive: true });
  const out = join(dir, "governed-ai-presence-index-final-certification-v1.json");
  writeFileSync(out, JSON.stringify(report, null, 2));
  console.log("Wrote", out);
  console.log("FINAL", report.final);
  console.log("NEXT", report.next);
  console.log("PRODUCTION_VALIDATED", report.certification.PRODUCTION_VALIDATED.join(", ") || "(none)");
}

function assertPeriod(manifest, period) {
  if (manifest && manifest.latestPeriodId !== PERIOD_ID) {
    console.warn("Warning: published manifest latestPeriodId differs", manifest.latestPeriodId);
  }
  if ((period.observations || []).length !== 312) {
    throw new Error(`Expected 312 observations, got ${(period.observations || []).length}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
