#!/usr/bin/env node
/**
 * Read-only deep audit for Existing Hotel ADP production recovery (v1).
 * No writes. Outputs reports/ai-demand-positioning/adp-existing-hotel-recovery-deep-audit-v1.json
 */
import { readdirSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import { loadAllPeriods, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  loadPublishedReport,
  loadPublishedEvidenceIndex,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { queryEvidenceIndex } from "../lib/ai-demand-positioning/customer/evidence-index.js";
import {
  isOfficialProductionPeriod,
  isCustomerTrendEligible,
  classifyPreBaselinePeriod,
  filterCustomerTrendPeriods,
} from "../lib/ai-demand-positioning/period-eligibility-v1.js";
import { OFFICIAL_BASELINE_PERIOD_MARKER } from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";

const ROOT = process.cwd();
const PUB_DIR = join(ROOT, "data/ai-demand-positioning/published");
const OUT = join(ROOT, "reports/ai-demand-positioning/adp-existing-hotel-recovery-deep-audit-v1.json");

function round1(n) {
  return n == null || Number.isNaN(Number(n)) ? null : Math.round(Number(n) * 10) / 10;
}

function statusFor(published, expected) {
  if (published == null && expected == null) return "NOT_APPLICABLE";
  if (published == null) return "MISSING";
  if (expected == null) return "INVALID";
  if (round1(published) === round1(expected)) {
    return Math.abs(Number(published) - Number(expected)) < 1e-9 ? "MATCH" : "ROUNDING_ONLY";
  }
  return "MISMATCH";
}

function providerCompleteness(period) {
  const acc = { total: 0, ok: 0, fail: 0, missing: 0, dryRun: 0, other: 0 };
  for (const o of period?.observations || []) {
    acc.total += 1;
    const st = String(o.status || "").toLowerCase();
    if (st === "dry_run" || o.dryRun) acc.dryRun += 1;
    else if (st === "ok" || (o.parsed && !o.error)) acc.ok += 1;
    else if (st.includes("fail") || st.includes("error") || o.error || o.httpStatus >= 400) acc.fail += 1;
    else if (!o.parsed && !o.response) acc.missing += 1;
    else acc.other += 1;
  }
  return acc;
}

function classifyPeriod(period, latestOfficialId) {
  const marker = period?.baselineMarker || period?.officialBaselineMarker || null;
  const official = isOfficialProductionPeriod(period);
  const trend = isCustomerTrendEligible(period);
  const pre = classifyPreBaselinePeriod(period);
  if (marker === OFFICIAL_BASELINE_PERIOD_MARKER || marker === "ADP_OFFICIAL_BASELINE_PERIOD_001") {
    if (period.periodId === latestOfficialId) return "ACTIVE_OFFICIAL_BASELINE";
    return "ARCHIVED_BASELINE";
  }
  if (String(marker || "").includes("PHILLIPS") || String(marker || "").includes("STANDALONE")) {
    return "CERTIFIED_STANDALONE";
  }
  if (official && trend) return "ACTIVE_STANDALONE";
  if (pre?.archiveClass === "PRE_BASELINE_ARCHIVE" || pre?.customerVisible === false) return "ARCHIVED_BASELINE";
  if (!period?.certified && (period?.observations || []).length) return "TEST";
  return "OTHER";
}

function collectEntityNames(payload) {
  const names = [];
  for (const x of payload?.competitiveSet?.observed || []) {
    names.push(typeof x === "string" ? x : x?.name);
  }
  for (const x of payload?.lostDemand?.topAlternatives || []) {
    names.push(x?.name || x?.competitor || (typeof x === "string" ? x : null));
  }
  const byT = payload?.competitiveRankingByTerritory?.byTerritory || {};
  for (const block of Object.values(byT)) {
    for (const row of block?.displayRows || []) names.push(row?.name);
  }
  for (const a of payload?.actions || []) {
    if (a?.competitor) names.push(a.competitor);
  }
  return names.filter(Boolean);
}

function entityIssuesFor(propertyId, names) {
  const issues = [];
  const junk = [/recommended hotels/i, /based on/i, /depending on/i, /here are/i, /for example/i, /^\d+\.\s/, /^[-*•]\s/];
  for (const name of names) {
    if (junk.some((r) => r.test(name))) issues.push({ propertyId, name, issue: "JUNK_FRAGMENT" });
  }
  const clusters = {};
  for (const name of [...new Set(names)]) {
    let bucket = null;
    if (/eau palm beach/i.test(name)) bucket = "eau_palm_beach";
    else if (/boca raton resort/i.test(name) || /^the boca raton\b/i.test(name)) bucket = "boca_raton_resort";
    else if (/four seasons.*palm beach/i.test(name) || /^four seasons resort$/i.test(name)) bucket = "four_seasons_pb";
    if (!bucket) continue;
    (clusters[bucket] ||= new Set()).add(name);
  }
  for (const [bucket, set] of Object.entries(clusters)) {
    if (set.size > 1) {
      issues.push({ propertyId, bucket, names: [...set], issue: "ALIAS_DUPLICATE_CLUSTER" });
    }
  }
  return issues;
}

const properties = existsSync(PUB_DIR)
  ? readdirSync(PUB_DIR).filter((d) => d.startsWith("adp_"))
  : [];

const report = {
  generatedAt: new Date().toISOString(),
  contractMarker: OFFICIAL_BASELINE_PERIOD_MARKER || "ADP_OFFICIAL_BASELINE_PERIOD_001",
  sot: {
    defaultRead: "local published JSON under data/ai-demand-positioning/published/",
    airtableRole:
      "Optional mirror/overlay. Production default is file SoT unless ADP_AIRTABLE_READ_LIVE=1 or ADP_PUBLISHED_READ_SOURCE=airtable.",
    airtableEnv: {
      ADP_AIRTABLE_BASE_ID: !!process.env.ADP_AIRTABLE_BASE_ID,
      ADP_AIRTABLE_READ_LIVE: process.env.ADP_AIRTABLE_READ_LIVE || null,
      ADP_PUBLISHED_READ_SOURCE: process.env.ADP_PUBLISHED_READ_SOURCE || null,
      AIRTABLE_BASE_ID: !!process.env.AIRTABLE_BASE_ID,
      AIRTABLE_PAT: !!(process.env.AIRTABLE_PAT || process.env.AIRTABLE_API_KEY),
    },
  },
  properties: [],
  metrics: [],
  entities: [],
  actions: [],
  trends: [],
  evidence: [],
  baselineInventory: [],
  activeOfficialBaselines: [],
};

for (const propertyId of properties) {
  const payload = loadPublishedReport(propertyId);
  if (!payload) {
    report.properties.push({ propertyId, error: "NO_PAYLOAD" });
    continue;
  }
  const profile = loadPropertyProfile(propertyId);
  const periods = loadAllPeriods(propertyId);
  const periodId = payload.period?.periodId || payload.periodId;
  const period = periods.find((p) => p.periodId === periodId);
  const scenarios = buildScenarioUniverse(profile);
  const rebuilt = period ? buildOwnerPayload(period, scenarios, profile) : null;

  const officialPeriods = periods.filter((p) => isOfficialProductionPeriod(p));
  const latestOfficial = [...officialPeriods].sort((a, b) =>
    String(a.executionDate || "").localeCompare(String(b.executionDate || ""))
  ).at(-1);

  for (const p of periods) {
    const cls = classifyPeriod(p, latestOfficial?.periodId);
    report.baselineInventory.push({
      propertyId,
      periodId: p.periodId,
      executionDate: p.executionDate || null,
      baselineMarker: p.baselineMarker || p.officialBaselineMarker || null,
      certified: !!p.certified,
      customerTrendEligible: isCustomerTrendEligible(p),
      officialProduction: isOfficialProductionPeriod(p),
      classification: cls,
      providerCompleteness: providerCompleteness(p),
      isPublishedLatest: p.periodId === periodId,
    });
    if (cls === "ACTIVE_OFFICIAL_BASELINE") {
      report.activeOfficialBaselines.push({
        propertyId,
        periodId: p.periodId,
        executionDate: p.executionDate,
        baselineMarker: p.baselineMarker || p.officialBaselineMarker,
      });
    }
  }

  const metricPairs = [
    ["AI_Consideration_Rate", payload.executiveMetrics?.considerationRate?.rate, rebuilt?.executiveMetrics?.considerationRate?.rate],
    ["AI_Scenario_Presence", payload.executiveMetrics?.scenarioPresence?.rate, rebuilt?.executiveMetrics?.scenarioPresence?.rate],
    ["Demand_Capture_overall", payload.demandCapture?.overallRate, rebuilt?.demandCapture?.overallRate],
    [
      "Property_Reality_Coverage",
      payload.executiveMetrics?.propertyRealityCoverage ?? payload.realityGap?.coverageRate,
      rebuilt?.executiveMetrics?.propertyRealityCoverage ?? rebuilt?.realityGap?.coverageRate,
    ],
    ["Top1_Appearance", payload.executiveMetrics?.top1Appearance?.rate, rebuilt?.executiveMetrics?.top1Appearance?.rate],
    ["Top3_Appearance", payload.executiveMetrics?.top3Appearance?.rate, rebuilt?.executiveMetrics?.top3Appearance?.rate],
  ];
  for (const [metric, published, expected] of metricPairs) {
    report.metrics.push({ propertyId, metric, expected, published, status: statusFor(published, expected) });
  }

  for (const intent of Object.keys(payload.intentPresenceIndex || {})) {
    const pub = payload.intentPresenceIndex[intent] || {};
    const reb = rebuilt?.intentPresenceIndex?.[intent] || {};
    const pIdx = pub.presenceIndex ?? pub.index ?? null;
    const eIdx = reb.presenceIndex ?? reb.index ?? null;
    const pCore = pub.coreBenchmarkRate ?? pub.coreRate ?? null;
    const eCore = reb.coreBenchmarkRate ?? reb.coreRate ?? null;
    const pSub = pub.subjectRate ?? pub.subjectPresenceRate ?? null;
    const eSub = reb.subjectRate ?? reb.subjectPresenceRate ?? null;
    report.metrics.push({
      propertyId,
      metric: `Presence_Index.${intent}`,
      expected: eIdx,
      published: pIdx,
      subjectExpected: eSub,
      subjectPublished: pSub,
      coreExpected: eCore,
      corePublished: pCore,
      status: statusFor(pIdx, eIdx) === "MATCH" && statusFor(pCore, eCore) === "MATCH" ? "MATCH" : statusFor(pIdx, eIdx),
      presentationFlag:
        pIdx != null && pIdx > 200 ? "CORRECT_BUT_PRESENTATION_REVIEW_PENDING" : null,
    });
  }

  const names = collectEntityNames(payload);
  report.entities.push(...entityIssuesFor(propertyId, names));

  for (const a of payload.actions || []) {
    const impact = String(a.expectedImpact || a.impact || "");
    if (/could improve capture|new demand scenarios captured|reduce displacement in \d+/i.test(impact)) {
      report.actions.push({
        propertyId,
        id: a.id || null,
        title: a.title || null,
        impact,
        issue: "UNSUPPORTED_NUMERIC_IMPACT",
      });
    }
  }

  const trends = payload.trends || [];
  const eligible = filterCustomerTrendPeriods(periods);
  report.trends.push({
    propertyId,
    publishedTrendPoints: trends.length,
    trendEligibleCount: eligible.length,
    uiRequiresTwoPoints: true,
    defect:
      trends.length === 1
        ? "SINGLE_BASELINE_POINT_PRESENT_BUT_UI_HIDES_WHEN_LENGTH_LT_2"
        : trends.length === 0 && eligible.length >= 1
          ? "TRENDS_EMPTY_DESPITE_ELIGIBLE_BASELINE"
          : null,
    baselineSample: trends[0] || null,
    eligiblePeriodIds: eligible.map((p) => p.periodId),
  });

  const evidence = loadPublishedEvidenceIndex(propertyId);
  const byIntent = payload.demandCapture?.byIntent || {};
  for (const intent of Object.keys(byIntent)) {
    const q = evidence ? queryEvidenceIndex(evidence, { intent, type: "missing" }) : null;
    const records = q?.evidence || [];
    const total = q?.total ?? 0;
    const scenarioCount = byIntent[intent]?.total ?? null;
    const missingCount = byIntent[intent] ? scenarioCount - (byIntent[intent].captured || 0) : null;
    let status = "NOT_APPLICABLE";
    if (!evidence?.ok) status = "MISSING_EVIDENCE";
    else if (missingCount === 0) status = "NOT_APPLICABLE";
    else if (!records.length) status = "MISSING_EVIDENCE";
    else if (total > 0 && records.length > 0) status = records.length < Math.min(5, missingCount || 5) ? "PARTIAL" : "PASS";
    report.evidence.push({
      propertyId,
      intent,
      scenarios: scenarioCount,
      missingScenarios: missingCount,
      evidenceRecordsReturned: records.length,
      evidenceTotal: total,
      linkWorks: !!evidence?.ok,
      correctlyFiltered: records.every((r) => !r.intent || r.intent === intent),
      status,
      note: "Customer UI opens type=missing only; max 5 excerpts per intent in index builder",
    });
  }

  // Cross-metric checks on published
  const top1 = payload.executiveMetrics?.top1Appearance?.rate;
  const top3 = payload.executiveMetrics?.top3Appearance?.rate;
  const consideration = payload.executiveMetrics?.considerationRate?.rate;
  const cross = [];
  if (top1 != null && top3 != null && top1 > top3 + 0.05) cross.push("TOP1_EXCEEDS_TOP3");
  if (top3 != null && consideration != null && top3 > consideration + 0.05) cross.push("TOP3_EXCEEDS_CONSIDERATION");

  report.properties.push({
    propertyId,
    periodId,
    classification: classifyPeriod(period || {}, latestOfficial?.periodId),
    demandCapture: payload.demandCapture?.overallRate,
    providerCompleteness: providerCompleteness(period),
    rebuiltOk: !!rebuilt?.ok,
    actionsCount: (payload.actions || []).length,
    unsupportedImpactCount: (payload.actions || []).filter((a) =>
      /could improve capture|new demand scenarios captured|reduce displacement in \d+/i.test(
        String(a.expectedImpact || a.impact || "")
      )
    ).length,
    evidenceIndexOk: !!evidence?.ok,
    evidenceIntents: Object.keys(evidence?.missingByIntent || {}),
    crossMetricViolations: cross,
    competitiveObservedSample: (payload.competitiveSet?.observed || []).slice(0, 12).map((x) =>
      typeof x === "string" ? x : x?.name
    ),
  });
}

mkdirSync(join(ROOT, "reports/ai-demand-positioning"), { recursive: true });
writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      out: OUT,
      properties: report.properties.length,
      metricMismatches: report.metrics.filter((m) => m.status === "MISMATCH").length,
      metricMatches: report.metrics.filter((m) => m.status === "MATCH").length,
      entityIssues: report.entities.length,
      unsupportedActions: report.actions.length,
      trendDefects: report.trends.filter((t) => t.defect).length,
      evidenceNonPass: report.evidence.filter((e) => !["PASS", "NOT_APPLICABLE"].includes(e.status)).length,
      activeOfficialBaselines: report.activeOfficialBaselines.length,
    },
    null,
    2
  )
);
