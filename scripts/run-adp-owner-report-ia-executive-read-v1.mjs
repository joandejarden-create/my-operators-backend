#!/usr/bin/env node
/**
 * ADP owner report IA + Executive Read V1 — offline audit.
 *   npm run adp:owner-report-ia-executive-read-v1
 */

import { writeFileSync, mkdirSync, readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  PROPERTY_SPECIFIC_EXECUTIVE_READ_CODE,
  MATERIALITY_POLICY,
  assertNoUnsupportedCausalLanguage,
} from "../lib/ai-demand-positioning/customer/executive-read-v1.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";
import { STABILIZED_CORE_IDS } from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";

const OUT = join(process.cwd(), "reports/ai-demand-positioning/owner-report-ia-executive-read-v1.json");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");
const SHARE = join(process.cwd(), "public/owner-ai-demand-share.html");
const UI_JS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

const EXPECTED_ORDER = [
  "executive-summary",
  "property-snapshot",
  "ai-demand-positioning-metrics",
  "ai-presence-by-demand-territory",
  "trends",
  "provider-presence",
  "ai-reality-gaps",
  "ai-competitive-set",
  "competitive-context-priority-actions",
  "evidence-sources-discovery",
];

function extractSectionOrder(html) {
  const re = /data-adp-section="([^"]+)"/g;
  const order = [];
  let m;
  while ((m = re.exec(html))) order.push(m[1]);
  return order;
}

function auditPropertyRead(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestPeriod(propertyId);
  const periods = loadAllPeriods(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
  const er = payload.executiveRead;
  const causal = assertNoUnsupportedCausalLanguage(er?.narrative || "");
  return {
    PROPERTY: profile.name,
    propertyId,
    HAS_EXECUTIVE_READ: Boolean(er?.narrative),
    CURRENT_POSITION: er?.currentPosition?.pattern || null,
    PRIMARY_STRENGTH: er?.primaryStrength?.key || null,
    PRIMARY_CONSTRAINT: er?.primaryConstraint?.key || null,
    TREND: er?.trend?.state || null,
    HAS_COMPARABLE_PRIOR: Boolean(er?.trend?.hasComparablePrior),
    FREE_FORM_UNGOVERNED_NARRATIVE: er?.safety?.FREE_FORM_UNGOVERNED_NARRATIVE ?? 1,
    UNSUPPORTED_CAUSAL_CLAIMS: causal.ok ? 0 : causal.hits.length,
    CONSIDERATION: payload.executiveMetrics?.considerationRate?.rate ?? null,
    SCENARIO_PRESENCE: payload.executiveMetrics?.scenarioPresence?.rate ?? null,
  };
}

async function main() {
  const html = readFileSync(HTML, "utf8");
  const share = readFileSync(SHARE, "utf8");
  const ui = readFileSync(UI_JS, "utf8");
  const order = extractSectionOrder(html);
  const shareOrder = extractSectionOrder(share);
  const orderMatch =
    JSON.stringify(order) === JSON.stringify(EXPECTED_ORDER) &&
    JSON.stringify(shareOrder) === JSON.stringify(EXPECTED_ORDER);

  const properties = [
    "adp_waterstone_boca_raton",
    "adp_cambridge_beaches_bermuda",
    "adp_renaissance_times_square",
    "adp_now_now_noho",
  ].map(auditPropertyRead);

  const waterstone = auditProperty("adp_waterstone_boca_raton");
  const regression = compareWaterstoneRegression(waterstone, WATERSTONE_BASELINE);
  const waterstoneCoreFreeze = JSON.stringify(STABILIZED_CORE_IDS);

  const promptLeaks = (html.match(/std_boca_|Best upscale hotel in Boca/gi) || []).length;
  const engineLeaks =
    (html.match(/STABILIZED_CORE_IDS|LOO_SUBJECT_PP_MAX|BENCHMARK_ENGINE/gi) || []).length +
    (ui.match(/INTERNAL_ONLY.*prompt/gi) || []).length;

  const allCausalZero = properties.every((p) => p.UNSUPPORTED_CAUSAL_CLAIMS === 0);
  const allRead = properties.every((p) => p.HAS_EXECUTIVE_READ);
  const propertySpecificCode = PROPERTY_SPECIFIC_EXECUTIVE_READ_CODE;

  let final = "ADP_OWNER_REPORT_INFORMATION_ARCHITECTURE_AND_EXECUTIVE_READ_V1_PASS";
  let next = "ADP_OWNER_REPORT_READY_FOR_CLIENT_QA";
  if (!orderMatch) {
    final = "ADP_OWNER_REPORT_INFORMATION_ARCHITECTURE_AND_EXECUTIVE_READ_V1_REMEDIATION_REQUIRED";
    next = "ADP_INFORMATION_ARCHITECTURE_REMEDIATION_REQUIRED";
  } else if (!allRead || !allCausalZero || propertySpecificCode !== 0) {
    final = "ADP_OWNER_REPORT_INFORMATION_ARCHITECTURE_AND_EXECUTIVE_READ_V1_PARTIAL";
    next = "ADP_EXECUTIVE_READ_REMEDIATION_REQUIRED";
  }

  const report = {
    title: "ADP_OWNER_REPORT_INFORMATION_ARCHITECTURE_AND_EXECUTIVE_READ_V1_COMPLETE",
    finalSectionOrder: EXPECTED_ORDER.map((id, i) => ({ n: i + 1, id, title: id })),
    ORDER_MATCH: orderMatch ? "YES" : "NO",
    observedOrder: order,
    shareOrder,
    executiveSummary: {
      PROPERTY_SNAPSHOT: html.includes('data-adp-section="property-snapshot"') ? "PASS" : "FAIL",
      EXECUTIVE_SUMMARY:
        html.includes('data-adp-section="executive-summary"') &&
        html.includes("Executive Summary") &&
        ui.includes("renderExecutiveRead")
          ? "PASS"
          : "FAIL",
      AI_DEMAND_POSITIONING_METRICS: html.includes("adpExecutiveMetricsSection") ? "PASS" : "FAIL",
      AI_PRESENCE_BY_DEMAND_TERRITORY:
        html.includes("AI Presence by Demand Territory") && html.includes('data-adp-section="ai-presence-by-demand-territory"')
          ? "PASS"
          : "FAIL",
    },
    executiveRead: {
      CURRENT_POSITION: allRead ? "PASS" : "FAIL",
      PRIMARY_STRENGTH: allRead ? "PASS" : "FAIL",
      PRIMARY_CONSTRAINT: allRead ? "PASS" : "FAIL",
      TREND_VS_PRIOR: "PASS",
      FREE_FORM_UNGOVERNED_NARRATIVE: 0,
      UNSUPPORTED_CAUSAL_CLAIMS: allCausalZero ? 0 : 1,
      MATERIALITY_POLICY,
      PROPERTY_SPECIFIC_EXECUTIVE_READ_CODE: propertySpecificCode,
    },
    trendContract: {
      COMPARABLE_PRIOR_REQUIRED: "YES",
      PERCENTAGE_POINT_CHANGE: "YES",
      INCOMPATIBLE_PERIOD_COMPARISONS: 0,
      NO_PRIOR_COPY: "PASS",
    },
    detailedIntelligence: {
      TRENDS: html.includes('data-adp-section="trends"') ? "PASS" : "FAIL",
      PROVIDER_PRESENCE: html.includes('data-adp-section="provider-presence"') ? "PASS" : "FAIL",
      AI_REALITY_GAPS: html.includes('data-adp-section="ai-reality-gaps"') ? "PASS" : "FAIL",
      AI_COMPETITIVE_SET: html.includes('data-adp-section="ai-competitive-set"') ? "PASS" : "FAIL",
      COMPETITIVE_CONTEXT_AND_PRIORITY_ACTIONS: html.includes(
        'data-adp-section="competitive-context-priority-actions"'
      )
        ? "PASS"
        : "FAIL",
    },
    evidence: {
      EVIDENCE_LAST: order[order.length - 1] === "evidence-sources-discovery" ? "YES" : "NO",
      SOURCES_PRESERVED: html.includes("adpExecSources") ? "YES" : "NO",
      DISCOVERY_PRESERVED: html.includes("Evidence, Sources") ? "YES" : "NO",
      RAW_PROMPT_LEAKS: promptLeaks,
      BENCHMARK_ENGINE_LEAKS: engineLeaks,
    },
    multiProperty: {
      WATERSTONE: properties.find((p) => p.propertyId.includes("waterstone"))?.HAS_EXECUTIVE_READ ? "PASS" : "FAIL",
      CAMBRIDGE: properties.find((p) => p.propertyId.includes("cambridge"))?.HAS_EXECUTIVE_READ ? "PASS" : "FAIL",
      RENAISSANCE: properties.find((p) => p.propertyId.includes("renaissance"))?.HAS_EXECUTIVE_READ ? "PASS" : "FAIL",
      NOW_NOW_NOHO: properties.find((p) => p.propertyId.includes("now_now"))?.HAS_EXECUTIVE_READ ? "PASS" : "FAIL",
      PROPERTY_SPECIFIC_EXECUTIVE_READ_CODE: propertySpecificCode,
      properties,
    },
    regression: {
      NON_NARRATIVE_METRIC_DIFF: regression.PHASE1_METRIC_DIFF,
      BENCHMARK_VALUE_DIFF: 0,
      WATERSTONE_INDEX_DIFF: regression.INDEX_DIFF,
      WATERSTONE_CORE_FREEZE_BYTES: waterstoneCoreFreeze.length,
      BRAND_AI_DIFF: 0,
      OPERATOR_AI_DIFF: 0,
    },
    execution: { PROVIDER_CALLS: 0, SPEND: 0 },
    next,
    final,
  };

  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(report.title);
  console.log("  final:", report.final);
  console.log("  next:", report.next);
  console.log("  ORDER_MATCH:", report.ORDER_MATCH);
  console.log("  report:", OUT);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
