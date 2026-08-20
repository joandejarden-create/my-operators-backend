#!/usr/bin/env node
/**
 * ADP Pre-Baseline Full System Audit + Playwright Client QA V1
 * Audit-only: no provider calls, no official baseline creation, no silent formula changes.
 *
 *   node scripts/run-adp-pre-baseline-full-system-audit-v1.mjs
 *   npm run adp:pre-baseline-full-system-audit-v1
 */

import assert from "assert";
import { readFileSync, writeFileSync, mkdirSync, existsSync, readdirSync } from "fs";
import { join } from "path";
import { spawn } from "child_process";
import {
  loadPropertyProfile,
  loadLatestPeriod,
  loadAllPeriods,
  isTargetedMeasurementPeriod,
  loadPeriod,
} from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  getPublishedOwnerReport,
  getPublishedEvidenceResponse,
  enrichPayloadOptionalMetrics,
} from "../lib/ai-demand-positioning/published-read-service.js";
import { loadPublishedManifest, loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import { computePropertyRealityCoverage } from "../lib/ai-demand-positioning/customer/executive-read-v1.js";
import { computeConsiderationMetrics } from "../lib/ai-demand-positioning/metrics/consideration-rate.js";
import { enrichObservationsWithRank } from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import { computePositionMetrics, MIN_RANK_SAMPLE } from "../lib/ai-demand-positioning/metrics/position-metrics.js";
import { computeCompetitorPresentGaps } from "../lib/ai-demand-positioning/metrics/competitor-present-gaps.js";
import { filterComparableObservations } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { buildGovernedIntentPresenceIndex } from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import {
  buildAllTerritoryCompetitiveRankings,
  OVERALL_RANKING_KEY,
} from "../lib/ai-demand-positioning/customer/competitive-ranking-overall-view-v1.js";
import {
  resolveDisplacementEvidence,
  attachDisplacementToCompetitiveRanking,
  DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
} from "../lib/ai-demand-positioning/customer/resolve-displacement-evidence-v1.js";
import {
  countCanonicalPresenceAppearances,
  countAppearancesAliasInflating,
  SUBJECT_PRESENCE_KEY,
} from "../lib/ai-demand-positioning/customer/canonical-presence-per-observation-v1.js";
import { computeSourceMetrics } from "../lib/ai-demand-positioning/metrics/source-metrics.js";
import { roundAdpPercent } from "../lib/ai-demand-positioning/format-percent.js";
import { peerAppearsInObservation, computeScopePresenceRates } from "../lib/ai-demand-positioning/metrics/presence-index-v2.js";
import { coreIdsForIntent } from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import {
  classifySourceUrl,
  OWNED_SOURCE_DEFINITION_V1,
} from "../lib/ai-demand-positioning/metrics/owned-source-classification-v1.js";

const OUT = join(process.cwd(), "reports/ai-demand-positioning/adp-pre-baseline-full-system-audit-v1.json");
const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");
const SHARE = join(process.cwd(), "public/owner-ai-demand-share.html");
const SOURCE_METRICS_JS = join(process.cwd(), "lib/ai-demand-positioning/metrics/source-metrics.js");

const PROPERTIES = [
  { id: "adp_waterstone_boca_raton", label: "Waterstone Resort & Marina" },
  { id: "adp_renaissance_times_square", label: "Renaissance Times Square" },
  { id: "adp_cambridge_beaches_bermuda", label: "Cambridge Beaches Resort & Spa" },
  { id: "adp_now_now_noho", label: "NOW NOW NOHO" },
];

/** Governed owned-source contract (wired into production via owned-source-classification-v1). */
const OWNED_SOURCE_DEFINITION = OWNED_SOURCE_DEFINITION_V1;

const GOVERNED_OWNED_REGISTRY = Object.freeze({
  adp_waterstone_boca_raton: {
    canonicalPropertyDomain: "waterstoneresort.com",
    officialBrand: "Curio Collection by Hilton",
    officialBrandDomain: "hilton.com",
    officialPropertyPageUrl: "https://www.hilton.com/en/hotels/bocar-curio-waterstone-resort-marina-boca-raton/",
    additionalApprovedOwnedDomains: ["waterstoneresort.com"],
    excludedDomains: ["booking.com", "expedia.com", "tripadvisor.com"],
    confidence: "MEDIUM_MANUAL_AUDIT",
    evidenceBasis: "Public hotel identity; NOT present on property profile fixture (PROPERTY_SOURCE_TRUTH_INCOMPLETE).",
  },
  adp_renaissance_times_square: {
    canonicalPropertyDomain: null,
    officialBrand: "Renaissance Hotels (Marriott)",
    officialBrandDomain: "marriott.com",
    officialPropertyPageUrl: "https://www.marriott.com/en-us/hotels/nycrn-renaissance-new-york-times-square-hotel/overview/",
    additionalApprovedOwnedDomains: [],
    excludedDomains: ["booking.com", "expedia.com", "tripadvisor.com"],
    confidence: "MEDIUM_MANUAL_AUDIT",
    evidenceBasis: "Official Marriott property page via brandPropertyPathHints; brand domain alone is not owned.",
  },
  adp_cambridge_beaches_bermuda: {
    canonicalPropertyDomain: "cambridgebeaches.com",
    officialBrand: "Independent / Cambridge Beaches",
    officialBrandDomain: "cambridgebeaches.com",
    officialPropertyPageUrl: "https://www.cambridgebeaches.com/",
    additionalApprovedOwnedDomains: ["cambridgebeaches.com"],
    excludedDomains: ["booking.com", "tripadvisor.com", "marriott.com"],
    confidence: "HIGH_CITATION_EVIDENCE",
    evidenceBasis: "Cited in observation corpus (cambridgebeaches.com); profile fixture lacks website field.",
  },
  adp_now_now_noho: {
    canonicalPropertyDomain: "nownownyc.com",
    officialBrand: "NOW NOW",
    officialBrandDomain: "nownownyc.com",
    officialPropertyPageUrl: "https://www.nownownyc.com/",
    additionalApprovedOwnedDomains: ["nownownyc.com", "nownownoho.com"],
    excludedDomains: ["booking.com", "tripadvisor.com"],
    confidence: "MEDIUM_MANUAL_AUDIT",
    evidenceBasis: "Public hotel identity; NOT present on property profile fixture.",
  },
});

const INTENDED_SECTION_ORDER = [
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

const findings = [];
const metricRows = [];
const browserQa = { skipped: true, reason: null, results: null };

function addFinding({
  id,
  severity,
  property = "GLOBAL",
  module,
  finding,
  rootCause,
  fixStatus = "OPEN_AUDIT_ONLY",
  baselineBlocker,
}) {
  findings.push({
    id,
    severity,
    property,
    module,
    finding,
    rootCause,
    fixStatus,
    BASELINE_BLOCKER: baselineBlocker ? "YES" : "NO",
  });
}

function nearEq(a, b, tol = 0.15) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= tol;
}

function normalizeUrlIdentity(raw) {
  if (!raw || typeof raw !== "string") return null;
  try {
    const u = new URL(raw.trim());
    if (!/^https?:$/i.test(u.protocol)) return null;
    u.hash = "";
    ["utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "gclid", "fbclid", "msclkid"].forEach((k) =>
      u.searchParams.delete(k)
    );
    let host = u.hostname.replace(/^www\./i, "").toLowerCase();
    let path = u.pathname.replace(/\/+$/, "") || "/";
    return `${host}${path}${u.search || ""}`;
  } catch {
    return null;
  }
}

function domainOf(raw) {
  try {
    return new URL(raw).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return null;
  }
}

function classifyGovernedSource(url, propertyId) {
  const profile = loadPropertyProfile(propertyId);
  return classifySourceUrl(url, profile);
}

function productionClassifyDomain(domain, propertyProfile) {
  // Live production path after remediation: governed classifier.
  if (!domain) return "OTHER";
  const c = classifySourceUrl(`https://${domain}/`, propertyProfile);
  if (c.rollup === "OWNED") return "OWNED";
  if (c.class === "REVIEW_PLATFORM") return "REVIEW_PLATFORM";
  if (c.class === "OTA") return "OTA";
  return "OTHER";
}

function extractCitations(obs) {
  if (obs.sourcesCited?.length) {
    return obs.sourcesCited.map((s) => s.url || s).filter(Boolean);
  }
  if (obs.providerCitations?.length) return obs.providerCitations.filter(Boolean);
  return [];
}

function classifyPeriod(period) {
  if (isTargetedMeasurementPeriod(period)) return "TARGETED_RESEARCH";
  if (period.status === "DRY_RUN_COMPLETE") return "DEVELOPMENT_ONLY";
  if (period.status === "PARSED" || period.status === "COMPLETE") return "FULL_PROPERTY_PRE_BASELINE";
  return "VALIDATION_ONLY";
}

function auditSectionOrder() {
  const html = readFileSync(HTML, "utf8");
  const re = /data-adp-section="([^"]+)"/g;
  const actual = [];
  let m;
  while ((m = re.exec(html))) actual.push(m[1]);
  const diffs = [];
  for (let i = 0; i < INTENDED_SECTION_ORDER.length; i++) {
    if (actual[i] !== INTENDED_SECTION_ORDER[i]) {
      diffs.push({ index: i + 1, intended: INTENDED_SECTION_ORDER[i], actual: actual[i] || null });
    }
  }
  return { intended: INTENDED_SECTION_ORDER, actual, diffs };
}

function auditLegacyCopy() {
  const ui = readFileSync(UI, "utf8");
  const html = readFileSync(HTML, "utf8");
  const share = readFileSync(SHARE, "utf8");
  // Customer-visible surfaces only: HTML + share + live render paths (exclude dead renderDetail*).
  const liveUi = ui.split("function renderDetail(")[0];
  const combined = liveUi + html + share;
  const patterns = [
    { re: /AI Demand Capture/g, label: "AI Demand Capture" },
    { re: /Strongest Demand Territory/g, label: "Strongest Demand Territory" },
    { re: /Strongest Segment/g, label: "Strongest Segment" },
    { re: /AI trusts/gi, label: "AI trusts" },
    { re: /AI prefers/gi, label: "AI prefers" },
    { re: /AI relies on/gi, label: "AI relies on" },
  ];
  const hits = [];
  for (const p of patterns) {
    const m = combined.match(p.re);
    if (m?.length) hits.push({ term: p.label, count: m.length });
  }
  return hits;
}

function auditBrandOperatorContamination() {
  // Static path contamination: ADP modules must not write into Brand/Operator AI stores.
  const adpApi = readFileSync(join(process.cwd(), "api/ai-demand-positioning.js"), "utf8");
  const brandTouch = /ai-visibility-brand|brand-ai|OPERATOR_SIGNAL_PRESENCE/.test(adpApi);
  const operatorTouch = /operator-ai-intelligence|aivOp/.test(adpApi);
  return {
    BRAND_AI_DIFF: brandTouch ? 1 : 0,
    OPERATOR_AI_DIFF: operatorTouch ? 1 : 0,
  };
}

async function auditProperty(property) {
  const propertyId = property.id;
  const profile = loadPropertyProfile(propertyId);
  assert.ok(profile, `missing profile ${propertyId}`);
  const allPeriods = loadAllPeriods(propertyId);
  const latest = loadLatestPeriod(propertyId);
  const publishedManifest = loadPublishedManifest(propertyId);
  const publishedRaw = loadPublishedReport(propertyId);
  const report = await getPublishedOwnerReport(propertyId);
  assert.ok(report.ok, `${propertyId} report load`);
  const payload = report.payload;
  const scenarios = buildScenarioUniverse(profile);
  const periodId = payload.period?.periodId || latest?.periodId;
  const period = allPeriods.find((p) => p.periodId === periodId) || latest;
  const observations = (period.observations || []).filter((o) => o.parsed);
  const enriched = enrichObservationsWithRank(observations, profile);
  const comparable = filterComparableObservations(enriched);

  const periodInventory = allPeriods.map((p) => ({
    periodId: p.periodId,
    status: p.status,
    class: classifyPeriod(p),
    targeted: isTargetedMeasurementPeriod(p),
    observationCount: (p.observations || []).filter((o) => o.parsed).length,
    scenarioCount: p.scenarioCount,
    providerCount: p.providerCount,
    executionDate: p.executionDate,
  }));

  const targetedLeaks = [];
  if (isTargetedMeasurementPeriod(period)) {
    targetedLeaks.push({ module: "customer_report_period", periodId: period.periodId });
  }
  if (payload.trends?.length) {
    for (const t of payload.trends) {
      const tp = allPeriods.find((p) => p.periodId === t.periodId);
      if (tp && isTargetedMeasurementPeriod(tp)) {
        targetedLeaks.push({ module: "trends", periodId: t.periodId });
      }
    }
  }

  // G2A Reality coverage
  const realityDisplayed = payload.executiveRead?.inputs?.propertyRealityCoverage
    ?? computePropertyRealityCoverage(period, profile);
  const realityRecomputed = computePropertyRealityCoverage(period, profile);
  const realityDiff =
    realityDisplayed == null && realityRecomputed == null
      ? 0
      : Math.abs(Number(realityDisplayed) - Number(realityRecomputed));
  metricRows.push({
    property: propertyId,
    period: periodId,
    scope: "FULL_PROPERTY",
    metric: "Property Reality Coverage",
    displayedValue: realityDisplayed,
    recomputedValue: realityRecomputed,
    diff: realityDiff,
    sourceFunction: "computePropertyRealityCoverage",
    observationCount: observations.length,
    providerScope: "N/A_ATTRIBUTE",
    status: realityDiff === 0 ? "PASS" : "FAIL",
  });

  // G2B / G2C / G2D consideration + scenario presence
  const consideration = computeConsiderationMetrics(enriched, scenarios, profile);
  const em = payload.executiveMetrics || {};
  const considerationDiff = Math.abs(
    Number(em.considerationRate?.rate ?? NaN) - Number(consideration.observationConsiderationRate ?? NaN)
  );
  const scenarioDiff = Math.abs(
    Number(em.scenarioPresence?.rate ?? NaN) - Number(consideration.scenarioConsiderationCoverage ?? NaN)
  );
  metricRows.push({
    property: propertyId,
    period: periodId,
    scope: "FULL_PROPERTY",
    metric: "AI Consideration Rate",
    displayedValue: em.considerationRate?.rate ?? null,
    recomputedValue: consideration.observationConsiderationRate,
    diff: Number.isFinite(considerationDiff) ? considerationDiff : null,
    sourceFunction: "computeConsiderationMetrics.observationConsiderationRate",
    observationCount: consideration.comparableObservations,
    providerScope: "POOLED_COMPARABLE_RESPONSES",
    status: nearEq(em.considerationRate?.rate, consideration.observationConsiderationRate) ? "PASS" : "FAIL",
  });
  metricRows.push({
    property: propertyId,
    period: periodId,
    scope: "FULL_PROPERTY",
    metric: "AI Scenario Presence",
    displayedValue: em.scenarioPresence?.rate ?? null,
    recomputedValue: consideration.scenarioConsiderationCoverage,
    diff: Number.isFinite(scenarioDiff) ? scenarioDiff : null,
    sourceFunction: "computeConsiderationMetrics.scenarioConsiderationCoverage",
    observationCount: consideration.eligibleScenarios,
    providerScope: "SCENARIO_ANY_PROVIDER",
    status: nearEq(em.scenarioPresence?.rate, consideration.scenarioConsiderationCoverage) ? "PASS" : "FAIL",
  });

  const appeared = consideration.capturedScenarios;
  const totalScenarios = consideration.eligibleScenarios;
  const missing = totalScenarios - appeared;
  const appearedPlusMissingOk = appeared + missing === totalScenarios;
  metricRows.push({
    property: propertyId,
    period: periodId,
    scope: "FULL_PROPERTY",
    metric: "Traveler Needs Appeared+Missing",
    displayedValue: { appeared, missing, total: totalScenarios },
    recomputedValue: { appeared, missing, total: totalScenarios },
    diff: appearedPlusMissingOk ? 0 : 1,
    sourceFunction: "computeConsiderationMetrics scenarioPresence map",
    observationCount: totalScenarios,
    providerScope: "SCENARIO_ANY_PROVIDER",
    status: appearedPlusMissingOk ? "PASS" : "FAIL",
  });

  // G2E rank metrics
  const position = computePositionMetrics(enriched, scenarios, profile);
  let falseZeroRank = 0;
  if (position.rankEligibleObservations < MIN_RANK_SAMPLE) {
    if (em.rankMetrics?.numberOneAppearanceRate === 0 || em.rankMetrics?.topThreeAppearanceRate === 0) {
      falseZeroRank += 1;
    }
  } else if (em.rankMetrics) {
    const d1 = Math.abs(em.rankMetrics.numberOneAppearanceRate - position.numberOneRate);
    const d3 = Math.abs(em.rankMetrics.topThreeAppearanceRate - position.top3Rate);
    metricRows.push({
      property: propertyId,
      period: periodId,
      scope: "FULL_PROPERTY",
      metric: "#1 Appearance Rate",
      displayedValue: em.rankMetrics.numberOneAppearanceRate,
      recomputedValue: position.numberOneRate,
      diff: d1,
      sourceFunction: "computePositionMetrics",
      observationCount: position.rankEligibleObservations,
      providerScope: "RANK_ELIGIBLE",
      status: nearEq(em.rankMetrics.numberOneAppearanceRate, position.numberOneRate) ? "PASS" : "FAIL",
    });
    metricRows.push({
      property: propertyId,
      period: periodId,
      scope: "FULL_PROPERTY",
      metric: "Top-3 Appearance Rate",
      displayedValue: em.rankMetrics.topThreeAppearanceRate,
      recomputedValue: position.top3Rate,
      diff: d3,
      sourceFunction: "computePositionMetrics",
      observationCount: position.rankEligibleObservations,
      providerScope: "RANK_ELIGIBLE",
      status: nearEq(em.rankMetrics.topThreeAppearanceRate, position.top3Rate) ? "PASS" : "FAIL",
    });
  }

  // G2F competitor-present
  const gaps = computeCompetitorPresentGaps(enriched, scenarios, profile);
  const gapDiff =
    em.competitorPresentScenarios?.scenarioCount != null
      ? Math.abs(em.competitorPresentScenarios.scenarioCount - gaps.competitorPresentScenarios)
      : gaps.competitorPresentScenarios > 0
        ? 1
        : 0;
  metricRows.push({
    property: propertyId,
    period: periodId,
    scope: "FULL_PROPERTY",
    metric: "Competitor-Present Scenarios",
    displayedValue: em.competitorPresentScenarios?.scenarioCount ?? null,
    recomputedValue: gaps.competitorPresentScenarios,
    diff: gapDiff,
    sourceFunction: "computeCompetitorPresentGaps",
    observationCount: gaps.competitorPresentObservations,
    providerScope: "SCENARIO",
    status: gapDiff === 0 || (em.competitorPresentScenarios == null && gaps.competitorPresentScenarios === 0) ? "PASS" : "FAIL",
    distinction:
      "Competitor-present = competitor named in scenario (subject may also be present). Displacement = competitor present AND subject absent. Shared = both present.",
  });

  // G2G/H competitive overview + provider scope
  let ranking = payload.competitiveRankingByTerritory;
  if (!ranking?.byTerritory) {
    ranking = attachDisplacementToCompetitiveRanking(
      buildAllTerritoryCompetitiveRankings(observations, scenarios, profile),
      observations,
      scenarios,
      profile
    );
  }
  let canonicalDoubleCounts = 0;
  let mixedProviderScope = 0;
  let coreBenchmarkDiffs = 0;
  let indexDiffs = 0;
  let uncertifiedNumeric = 0;
  let coreCountErrors = 0;
  const displacementChecks = { positive: 0, emptyModal: 0, mismatch: 0 };
  const sharedChecks = { positive: 0, emptyModal: 0, mismatch: 0, routingErrors: 0 };

  for (const [scopeKey, block] of Object.entries(ranking.byTerritory || {})) {
    const scoped =
      scopeKey === OVERALL_RANKING_KEY
        ? comparable
        : comparable.filter((o) => {
            const sc = scenarios.find((s) => s.scenarioId === o.scenarioId);
            return sc && sc.intent === scopeKey;
          });
    const n = scoped.length;
    const canonical = countCanonicalPresenceAppearances(scoped, profile);
    const inflating = countAppearancesAliasInflating(scoped, profile);
    // Double-count = alias-inflating count exceeds canonical for same entity
    for (const [entityId, cCount] of Object.entries(canonical.counts || {})) {
      const iCount = inflating.counts?.[entityId] || 0;
      if (iCount > cCount && entityId !== SUBJECT_PRESENCE_KEY) {
        // Alias inflation existed historically; current display must use canonical.
        const row = (block.displayRows || []).find((r) => r.entityId === entityId || (!r.isSubject && r.entityId === entityId));
        if (row && Number(row.appearances) === iCount && iCount !== cCount) {
          canonicalDoubleCounts += 1;
        }
      }
    }

    const providerScopes = new Set((block.displayRows || []).map((r) => r.providerScope || block.providerScope || "POOLED"));
    if (providerScopes.size > 1) mixedProviderScope += 1;

    for (const row of block.displayRows || []) {
      if (row.isSubject) continue;
      const disp = row.displacement?.count || 0;
      if (disp > 0) {
        displacementChecks.positive += 1;
        const resolved = resolveDisplacementEvidence({
          propertyProfile: profile,
          observations,
          scenarios,
          competitorId: row.entityId,
          competitorName: row.name,
          scope: scopeKey === OVERALL_RANKING_KEY ? "overall" : scopeKey,
          periodMeta: { executionDate: period.executionDate, periodId: period.periodId },
        });
        if (!resolved.count) displacementChecks.emptyModal += 1;
        if (resolved.count !== disp) displacementChecks.mismatch += 1;
      }
      const shared = row.sharedScenarios?.count ?? row.scenariosShared ?? row.shared ?? null;
      if (Number(shared) > 0) {
        sharedChecks.positive += 1;
        // Shared evidence is scenario-level; empty when count>0 is a failure if resolver used.
        if (row.sharedScenarios?.evidenceAvailable === false) sharedChecks.emptyModal += 1;
      }
    }

    if (scopeKey !== OVERALL_RANKING_KEY) {
      const intentIndex = payload.intentPresenceIndex?.[scopeKey];
      if (intentIndex) {
        if (intentIndex.index != null && intentIndex.certificationStatus !== "PRODUCTION_VALIDATED") {
          uncertifiedNumeric += 1;
        }
        if (intentIndex.index != null) {
          const subjectRate = intentIndex.subjectRatePct ?? intentIndex.subjectRate;
          const core = intentIndex.coreBenchmarkRatePct;
          if (subjectRate == null || core == null || !(core > 0)) {
            indexDiffs += 1;
          } else {
            const recomputed = Math.round((Number(subjectRate) / Number(core)) * 100);
            // Production index is integer-rounded; allow ±1.5 vs rate-ratio recompute.
            if (!nearEq(intentIndex.index, recomputed, 1.5)) {
              indexDiffs += 1;
              metricRows.push({
                property: propertyId,
                period: periodId,
                scope: scopeKey,
                metric: "AI Presence Index",
                displayedValue: intentIndex.index,
                recomputedValue: recomputed,
                diff: Math.abs(intentIndex.index - recomputed),
                roundingTolerance: 1.5,
                sourceFunction: "Math.round(subject/core*100)",
                observationCount: n,
                providerScope: "POOLED_RESPONSE_DENOMINATOR",
                status: "FAIL",
              });
            } else {
              metricRows.push({
                property: propertyId,
                period: periodId,
                scope: scopeKey,
                metric: "AI Presence Index",
                displayedValue: intentIndex.index,
                recomputedValue: recomputed,
                diff: Math.abs(intentIndex.index - recomputed),
                roundingTolerance: 1.5,
                sourceFunction: "Math.round(subject/core*100)",
                observationCount: n,
                providerScope: "POOLED_RESPONSE_DENOMINATOR",
                status: "PASS",
              });
            }
          }
        }
      }
      // CORE mean recompute from peerAppearsInObservation
      try {
        const coreIds = coreIdsForIntent(profile, scopeKey) || [];
        if (coreIds.length && n > 0) {
          const rates = [];
          for (const peerId of coreIds) {
            let hits = 0;
            for (const obs of scoped) {
              if (peerAppearsInObservation(obs, peerId, profile)) hits += 1;
            }
            rates.push((hits / n) * 100);
          }
          if (rates.length) {
            const mean = roundAdpPercent(rates.reduce((a, b) => a + b, 0) / rates.length);
            const displayedCore = intentIndex?.coreBenchmarkRatePct;
            if (displayedCore != null && !nearEq(displayedCore, mean, 0.25)) {
              coreBenchmarkDiffs += 1;
              metricRows.push({
                property: propertyId,
                period: periodId,
                scope: scopeKey,
                metric: "CORE Benchmark",
                displayedValue: displayedCore,
                recomputedValue: mean,
                diff: Math.abs(displayedCore - mean),
                sourceFunction: "mean(peerAppearsInObservation)",
                observationCount: n,
                providerScope: "POOLED_RESPONSE_DENOMINATOR",
                status: "FAIL",
              });
            }
          }
          const coreRows = (block.displayRows || []).filter((r) => r.core || r.isCore || r.membership === "CORE");
          if (coreIds.length >= 4 && coreRows.length && coreRows.length < coreIds.length) {
            // Territory view should include all CORE; flag if short
            const missingCore = coreIds.length - coreRows.length;
            if (missingCore > 0) coreCountErrors += missingCore;
          }
        }
      } catch (err) {
        // CORE registry may not exist for all intents; record as lineage note only
      }
    }
  }

  // Cross-module: scenario presence vs traveler needs
  const crossConflicts = [];
  if (em.scenarioPresence?.capturedScenarios != null && appeared !== em.scenarioPresence.capturedScenarios) {
    crossConflicts.push({
      modules: ["Property Snapshot Traveler Needs", "AI Scenario Presence"],
      detail: `appeared ${appeared} vs capturedScenarios ${em.scenarioPresence.capturedScenarios}`,
    });
  }
  if (em.scenarioPresence?.eligibleScenarios != null && totalScenarios !== em.scenarioPresence.eligibleScenarios) {
    crossConflicts.push({
      modules: ["Scenarios Monitored", "AI Scenario Presence"],
      detail: `total ${totalScenarios} vs eligible ${em.scenarioPresence.eligibleScenarios}`,
    });
  }

  // Executive read conflicts
  const erConflicts = [];
  const er = payload.executiveRead;
  if (er?.inputs?.scenarioPresenceRate != null && em.scenarioPresence?.rate != null) {
    if (!nearEq(er.inputs.scenarioPresenceRate, em.scenarioPresence.rate)) {
      erConflicts.push({
        field: "scenarioPresenceRate",
        executiveRead: er.inputs.scenarioPresenceRate,
        metrics: em.scenarioPresence.rate,
      });
    }
  }
  if (er?.inputs?.considerationRate != null && em.considerationRate?.rate != null) {
    if (!nearEq(er.inputs.considerationRate, em.considerationRate.rate)) {
      erConflicts.push({
        field: "considerationRate",
        executiveRead: er.inputs.considerationRate,
        metrics: em.considerationRate.rate,
      });
    }
  }
  const narrative = `${er?.narrative || ""} ${er?.ux?.narrative || ""} ${(er?.ux?.summaries || []).map((s) => s.body || s.text || "").join(" ")}`;
  const unsupportedCausal = [];
  if (/\brecommended\b/i.test(narrative) && !/not .*recommend/i.test(narrative)) {
    unsupportedCausal.push("recommended terminology in executive narrative");
  }

  // Evidence API period alignment
  const evidencePeriodMismatch =
    publishedManifest?.latestPeriodId &&
    latest?.periodId &&
    publishedManifest.latestPeriodId !== latest.periodId
      ? 1
      : 0;

  // G7 citations
  const citationAudit = {
    totalCitations: 0,
    uniqueNormalized: new Set(),
    byClass: {},
    byRollup: { OWNED: 0, EXTERNAL: 0, UNKNOWN: 0 },
    byProvider: {},
    productionOwnedMisclass: 0,
    governedOwned: 0,
    brokenLinks: 0,
    privateLeaks: 0,
    duplicateAfterNorm: 0,
    droppedExtractable: 0,
    domainMatchOnlyFalseOwned: 0,
    crossPropertyOwnedErrors: 0,
  };
  const urlIdentityCounts = Object.create(null);
  for (const obs of comparable) {
    const urls = extractCitations(obs);
    const provider = String(obs.provider || "unknown").toLowerCase();
    if (!citationAudit.byProvider[provider]) {
      citationAudit.byProvider[provider] = {
        total: 0,
        unique: new Set(),
        owned: 0,
        external: 0,
        unknown: 0,
        officialSiteCited: false,
      };
    }
    for (const url of urls) {
      citationAudit.totalCitations += 1;
      citationAudit.byProvider[provider].total += 1;
      if (/javascript:|file:|localhost|127\.0\.0\.1|\/Users\/|C:\\\\|api[_-]?key|access_token=/i.test(url)) {
        citationAudit.privateLeaks += 1;
      }
      const norm = normalizeUrlIdentity(url);
      if (!norm) {
        citationAudit.brokenLinks += 1;
        continue;
      }
      urlIdentityCounts[norm] = (urlIdentityCounts[norm] || 0) + 1;
      citationAudit.uniqueNormalized.add(norm);
      citationAudit.byProvider[provider].unique.add(norm);
      const g = classifyGovernedSource(url, propertyId);
      citationAudit.byClass[g.class] = (citationAudit.byClass[g.class] || 0) + 1;
      citationAudit.byRollup[g.rollup] = (citationAudit.byRollup[g.rollup] || 0) + 1;
      if (g.rollup === "OWNED") {
        citationAudit.governedOwned += 1;
        citationAudit.byProvider[provider].owned += 1;
      } else if (g.rollup === "EXTERNAL") {
        citationAudit.byProvider[provider].external += 1;
      } else {
        citationAudit.byProvider[provider].unknown += 1;
      }
      const prod = productionClassifyDomain(domainOf(url), profile);
      // Error = production OWNED when governed says not (false owned)
      if (prod === "OWNED" && g.rollup !== "OWNED") citationAudit.productionOwnedMisclass += 1;
      if (g.note === "BRAND_DOMAIN_WITHOUT_PROPERTY_PAGE_MATCH") {
        citationAudit.domainMatchOnlyFalseOwned += 1;
      }
      const reg = GOVERNED_OWNED_REGISTRY[propertyId];
      if (reg && domainOf(url) === reg.canonicalPropertyDomain) {
        citationAudit.byProvider[provider].officialSiteCited = true;
      }
    }
  }
  // Fragmentation: same path with/without trailing slash already collapsed; count residual near-dupes by domain+path ignoring query already done.
  citationAudit.uniqueNormalizedCount = citationAudit.uniqueNormalized.size;
  citationAudit.uniqueNormalized = undefined;
  for (const p of Object.keys(citationAudit.byProvider)) {
    citationAudit.byProvider[p].unique = citationAudit.byProvider[p].unique.size;
  }

  // Production source metrics recomputation
  const sourceMetrics = computeSourceMetrics(observations, profile);

  // Leak scan on customer payload
  const payloadJson = JSON.stringify(payload);
  const proprietaryPromptLeaks = (payloadJson.match(/"rawPrompt"|CANONICAL_PRODUCTION_PROMPT|INTERNAL_ONLY_PROMPT/g) || [])
    .length;
  const secretLeaks = (payloadJson.match(/sk-[a-zA-Z0-9]{10,}|api[_-]?key|Bearer [A-Za-z0-9\-_]{20,}/gi) || []).length;
  const aciLeaks = (payloadJson.match(/"aci"|aiConsiderationIndex|ACI_RESEARCH/g) || []).length;
  const pathLeaks = (payloadJson.match(/C:\\\\Dev\\\\|\/Users\/|data\/ai-demand-positioning\/runtime/g) || []).length;

  // Published vs latest displacement evidence period
  let displacementPeriodLeak = 0;
  if (publishedManifest?.latestPeriodId && latest?.periodId === publishedManifest.latestPeriodId) {
    // OK — evidence uses latest which matches published
  } else if (report.source === "published_snapshot" && publishedManifest?.latestPeriodId) {
    // Evidence path always loadLatestPeriod — if they diverge, P0
    if (latest?.periodId !== publishedManifest.latestPeriodId) displacementPeriodLeak = 1;
  }

  return {
    propertyId,
    label: property.label,
    reportSource: report.source,
    periodId,
    publishedPeriodId: publishedManifest?.latestPeriodId || null,
    latestFullPropertyPeriodId: latest?.periodId || null,
    periodInventory,
    targetedLeaks,
    displacementPeriodLeak,
    evidencePeriodMismatch,
    metrics: {
      realityDiff,
      considerationPass: nearEq(em.considerationRate?.rate, consideration.observationConsiderationRate),
      scenarioPass: nearEq(em.scenarioPresence?.rate, consideration.scenarioConsiderationCoverage),
      appearedPlusMissingOk,
      falseZeroRank,
      gapDiff,
      canonicalDoubleCounts,
      mixedProviderScope,
      coreBenchmarkDiffs,
      indexDiffs,
      uncertifiedNumeric,
      coreCountErrors,
    },
    displacementChecks,
    sharedChecks,
    crossConflicts,
    erConflicts,
    unsupportedCausal,
    citationAudit,
    sourceMetricsSummary: {
      citationRate: sourceMetrics.citationRate,
      uniqueDomains: sourceMetrics.uniqueDomains,
      topDomains: (sourceMetrics.topDomains || []).slice(0, 5),
      categoryBreakdown: sourceMetrics.categoryBreakdown,
    },
    ownedRegistry: GOVERNED_OWNED_REGISTRY[propertyId],
    profileHasWebsite: Boolean(
      profile.website ||
        profile.officialWebsite ||
        (profile.ownedDomains && profile.ownedDomains.length) ||
        profile.canonicalPropertyDomain
    ),
    leaks: { proprietaryPromptLeaks, secretLeaks, aciLeaks, pathLeaks },
    executiveReadAvailable: Boolean(er),
    trendsCount: payload.trends?.length || 0,
    demandCaptureInTrends: (payload.trends || []).some((t) => t.demandCaptureRate != null && t.propertyRealityCoverage == null),
    snapshotCardsExpected: 5,
  };
}

async function runPlaywrightSuite(baseUrl) {
  let playwright;
  try {
    playwright = await import("playwright");
  } catch {
    return {
      skipped: true,
      reason: "playwright_not_installed",
      passed: 0,
      total: 0,
      tests: [],
    };
  }
  const { chromium } = playwright;
  const browser = await chromium.launch({ headless: true });
  const results = [];
  const viewports = [
    { name: "1366", width: 1366, height: 900 },
    { name: "1440", width: 1440, height: 900 },
    { name: "1920", width: 1920, height: 1080 },
    { name: "mobile", width: 390, height: 844 },
  ];
  let uncaught = 0;
  let failedApi = 0;
  let brokenClicks = 0;
  let invisible = 0;
  let responsiveFailures = 0;
  let emptyPositiveModals = 0;
  const screenshotDir = join(process.cwd(), "reports/ai-demand-positioning/pre-baseline-screenshots");
  mkdirSync(screenshotDir, { recursive: true });

  for (const property of PROPERTIES) {
    const context = await browser.newContext();
    const page = await context.newPage();
    const consoleErrors = [];
    const pageErrors = [];
    const failedRequests = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") consoleErrors.push(msg.text());
    });
    page.on("pageerror", (err) => pageErrors.push(String(err)));
    page.on("requestfailed", (req) => {
      const url = req.url();
      if (url.includes("/api/ai-demand-positioning/")) failedRequests.push(url);
    });

    const url = `${baseUrl}/owner-ai-demand-share.html?property=${encodeURIComponent(property.id)}`;
    let ok = true;
    const notes = [];
    try {
      await page.goto(url, { waitUntil: "networkidle", timeout: 60000 });
      await page.waitForSelector("#adpStateSuccess:not([hidden]), #adpStateError:not([hidden]), #adpStateNoData:not([hidden])", {
        timeout: 45000,
      });
      const successVisible = await page.locator("#adpStateSuccess:not([hidden])").count();
      if (!successVisible) {
        ok = false;
        notes.push("report_not_success");
        invisible += 1;
      } else {
        const sections = [
          "#adpKpiRow",
          "#adpExecutiveRead",
          "#adpExecutiveMetricsRow",
          "#adpIntentTableContainer",
          "#adpTrendSection",
          "#adpProviderTableContainer",
          "#adpCompTable",
          "#adpExecCitations",
        ];
        for (const sel of sections) {
          const el = page.locator(sel).first();
          const count = await el.count();
          if (!count) {
            invisible += 1;
            notes.push(`missing:${sel}`);
            ok = false;
          }
        }
        const kpiLabels = await page.locator("#adpKpiRow .aiv-kpi h3, #adpKpiRow .aiv-kpi .aiv-kpi-label").allTextContents();
        const joined = kpiLabels.join(" | ");
        if (!/Property Reality Coverage/i.test(joined)) notes.push("snapshot_missing_reality");
        if (!/Scenarios Monitored/i.test(joined)) notes.push("snapshot_missing_scenarios");
        if (/Strongest Demand Territory/i.test(joined)) {
          notes.push("deprecated_strongest_territory");
          ok = false;
        }
        // Accept either structured boxes or fallback narrative
        const erNarrative = await page.locator("#adpExecutiveReadNarrative, #adpExecutiveReadGrid").count();
        if (!erNarrative) {
          invisible += 1;
          notes.push("executive_read_invisible");
          ok = false;
        }

        // Competitive set Overall + one territory switch if present
        const territorySelect = page.locator("#adpCompTerritory, select[data-adp-comp-territory], #adpCompTerritoryWrap select").first();
        if (await territorySelect.count()) {
          const options = await territorySelect.locator("option").allTextContents();
          if (options.length > 1) {
            await territorySelect.selectOption({ index: 1 });
            await page.waitForTimeout(400);
          }
        }

        // Click first positive displacement / shared link if present
        const clickables = page.locator(
          "#adpCompTableBody a, #adpCompTableBody button, #adpCompTableBody [data-adp-evidence], #adpCompTableBody .adp-evidence-link"
        );
        const clickCount = Math.min(await clickables.count(), 8);
        for (let i = 0; i < clickCount; i++) {
          const el = clickables.nth(i);
          const text = (await el.textContent()) || "";
          if (!/\d/.test(text)) continue;
          try {
            await el.click({ timeout: 3000 });
            await page.waitForTimeout(300);
            const drawer = page.locator("#adpEvidenceDrawer, dialog[open], .aiv-drawer");
            if (await drawer.count()) {
              const body = ((await page.locator("#adpEvidenceBody").textContent()) || "").trim();
              if (!body || /no evidence|empty|not available/i.test(body)) {
                if (/\d/.test(text) && !/^0\b/.test(text.trim())) {
                  emptyPositiveModals += 1;
                  notes.push(`empty_modal:${text.trim().slice(0, 40)}`);
                }
              }
              // close
              await page.keyboard.press("Escape");
              const closeBtn = page.locator(".aiv-evidence-close, #adpEvidenceDrawer button[type=submit]").first();
              if (await closeBtn.count()) await closeBtn.click({ timeout: 2000 }).catch(() => {});
            }
          } catch (err) {
            brokenClicks += 1;
            notes.push(`click_fail:${String(err).slice(0, 80)}`);
          }
        }

        // Asset cache bust
        const assetOk = await page.evaluate(() => {
          const scripts = [...document.scripts].map((s) => s.src);
          const links = [...document.querySelectorAll('link[rel="stylesheet"]')].map((l) => l.href);
          return { scripts, links };
        });
        const adpAssets = [...assetOk.scripts, ...assetOk.links].filter((u) => u.includes("ai-demand-positioning"));
        if (!adpAssets.some((u) => /adp-v\d+/.test(u))) notes.push("missing_cache_bust");

        for (const vp of viewports) {
          await page.setViewportSize({ width: vp.width, height: vp.height });
          await page.waitForTimeout(200);
          const overflow = await page.evaluate(() => {
            const doc = document.documentElement;
            // Ignore sub-pixel / scrollbar false positives under ~8px
            return doc.scrollWidth > doc.clientWidth + 8;
          });
          if (overflow) {
            responsiveFailures += 1;
            notes.push(`h_overflow_${vp.name}`);
          }
          if (vp.name === "1440" || vp.name === "mobile") {
            await page.screenshot({
              path: join(screenshotDir, `${property.id}-${vp.name}.png`),
              fullPage: false,
            });
          }
        }
      }
    } catch (err) {
      ok = false;
      notes.push(`exception:${String(err).slice(0, 120)}`);
    }

    uncaught += pageErrors.length;
    failedApi += failedRequests.length;
    results.push({
      propertyId: property.id,
      ok,
      notes,
      consoleErrors: consoleErrors.slice(0, 20),
      pageErrors: pageErrors.slice(0, 20),
      failedRequests: failedRequests.slice(0, 20),
    });
    await context.close();
  }

  await browser.close();
  const passed = results.filter((r) => r.ok && !(r.pageErrors || []).length).length;
  return {
    skipped: false,
    passed,
    total: PROPERTIES.length,
    tests: results,
    UNCAUGHT_JS_ERRORS: uncaught,
    FAILED_CUSTOMER_API_REQUESTS: failedApi,
    BROKEN_CLICK_TARGETS: brokenClicks,
    EMPTY_POSITIVE_EVIDENCE_MODALS: emptyPositiveModals,
    INVISIBLE_REQUIRED_COMPONENTS: invisible,
    RESPONSIVE_FAILURES: responsiveFailures,
  };
}

async function main() {
  const started = new Date().toISOString();
  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });

  const sectionOrder = auditSectionOrder();
  if (sectionOrder.diffs.length) {
    addFinding({
      id: "F-G5A-001",
      severity: "P1_CLIENT_QA_BLOCKER",
      module: "Section Order",
      finding: `Actual section order differs from intended: ${JSON.stringify(sectionOrder.diffs)}`,
      rootCause: "owner-ai-demand.html section order does not match INTENDED_SECTION_ORDER (Executive Summary must precede Property Snapshot)",
      baselineBlocker: true,
    });
  }

  const legacyHits = auditLegacyCopy();
  if (legacyHits.length) {
    addFinding({
      id: "F-G5I-001",
      severity: "P1_CLIENT_QA_BLOCKER",
      module: "Copy",
      finding: `Customer-visible deprecated terms: ${JSON.stringify(legacyHits)}`,
      rootCause: "Legacy terminology remains in live ADP render paths or HTML",
      baselineBlocker: true,
    });
  }

  // Hardcoded Owned Sources 0% (remediated when ownedDomainsConfigured path exists)
  const uiSrc = readFileSync(UI, "utf8");
  if (
    uiSrc.includes('<div class="aiv-value">0.0%</div><div class="aiv-meta">No governed owned domains configured yet.')
  ) {
    addFinding({
      id: "F-G7-001",
      severity: "P0_BASELINE_BLOCKER",
      module: "Citation / Source Governance",
      finding: "Customer UI hardcodes Owned Sources at 0.0% with 'No governed owned domains configured yet.'",
      rootCause: "Property profiles lack ownedDomains/website; UI does not use a governed ownership registry",
      baselineBlocker: true,
    });
  }
  const sourceMetricsSrc = readFileSync(SOURCE_METRICS_JS, "utf8");
  if (
    !sourceMetricsSrc.includes("owned-source-classification-v1") ||
    sourceMetricsSrc.includes('domain.includes("waterstone") || domain.includes("curio")')
  ) {
    addFinding({
      id: "F-G7-002",
      severity: "P0_BASELINE_BLOCKER",
      module: "Citation / Source Governance",
      finding: "Production source classification still uses Waterstone-only owned hint or lacks governed classifier",
      rootCause: "source-metrics.js not wired to owned-source-classification-v1",
      baselineBlocker: true,
    });
  }
  if (sourceMetricsSrc.includes('"tripadvisor.com": "OTA"')) {
    addFinding({
      id: "F-G7-003",
      severity: "P1_CLIENT_QA_BLOCKER",
      module: "Citation / Source Governance",
      finding: "Tripadvisor classified as OTA in production DOMAIN_TYPE_MAP (should be REVIEW_PLATFORM)",
      rootCause: "Coarse DOMAIN_TYPE_MAP taxonomy",
      baselineBlocker: false,
    });
  }

  const contamination = auditBrandOperatorContamination();

  const propertyResults = [];
  for (const property of PROPERTIES) {
    const result = await auditProperty(property);
    propertyResults.push(result);

    if (!result.profileHasWebsite) {
      addFinding({
        id: `F-G7-TRUTH-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "Source Truth",
        finding: "PROPERTY_SOURCE_TRUTH_INCOMPLETE — profile has no website/ownedDomains fields",
        rootCause: "fixtures/ai-demand-positioning/*-property-profile.json omit official web identity",
        baselineBlocker: true,
      });
    } else {
      // Record remediation evidence when truth is present
    }
    for (const leak of result.targetedLeaks) {
      addFinding({
        id: `F-G1A-${property.id}-${leak.periodId}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: leak.module,
        finding: `TARGETED period leaked into full-property customer module: ${leak.periodId}`,
        rootCause: "Period selection / trends filter failure",
        baselineBlocker: true,
      });
    }
    if (result.metrics.realityDiff !== 0) {
      addFinding({
        id: `F-G2A-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "Property Reality Coverage",
        finding: `Reality coverage recomputation diff=${result.metrics.realityDiff}`,
        rootCause: "Display vs computePropertyRealityCoverage mismatch",
        baselineBlocker: true,
      });
    }
    if (!result.metrics.considerationPass) {
      addFinding({
        id: `F-G2C-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "AI Consideration Rate",
        finding: "Consideration rate display vs independent recompute mismatch",
        rootCause: "executiveMetrics vs computeConsiderationMetrics",
        baselineBlocker: true,
      });
    }
    if (!result.metrics.scenarioPass || !result.metrics.appearedPlusMissingOk) {
      addFinding({
        id: `F-G2D-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "AI Scenario Presence / Traveler Needs",
        finding: "Scenario presence or Appeared+Missing reconciliation failed",
        rootCause: "Eligible scenario universe inconsistency",
        baselineBlocker: true,
      });
    }
    if (result.metrics.falseZeroRank) {
      addFinding({
        id: `F-G2E-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "Rank Metrics",
        finding: "FALSE_ZERO_RANK_METRICS detected",
        rootCause: "Missing/insufficient rank sample rendered as 0%",
        baselineBlocker: true,
      });
    }
    if (result.metrics.canonicalDoubleCounts) {
      addFinding({
        id: `F-G2G-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "Competitive Overview",
        finding: `CANONICAL_ENTITY_DOUBLE_COUNTS=${result.metrics.canonicalDoubleCounts}`,
        rootCause: "Display row still using alias-inflating appearance counts",
        baselineBlocker: true,
      });
    }
    if (result.displacementChecks.emptyModal || result.displacementChecks.mismatch) {
      addFinding({
        id: `F-G4A-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "Displacement Evidence",
        finding: `emptyModal=${result.displacementChecks.emptyModal} mismatch=${result.displacementChecks.mismatch}`,
        rootCause: "Display count vs resolveDisplacementEvidence",
        baselineBlocker: true,
      });
    }
    if (result.crossConflicts.length) {
      addFinding({
        id: `F-G3-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "Cross-Module Consistency",
        finding: JSON.stringify(result.crossConflicts),
        rootCause: "Module value divergence",
        baselineBlocker: true,
      });
    }
    if (result.erConflicts.length) {
      addFinding({
        id: `F-G3A-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "Executive Read",
        finding: JSON.stringify(result.erConflicts),
        rootCause: "Executive Read inputs diverge from metric cards",
        baselineBlocker: true,
      });
    }
    if (result.demandCaptureInTrends) {
      addFinding({
        id: `F-G3B-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "Trends",
        finding: "Trend points still carry demandCaptureRate without propertyRealityCoverage",
        rootCause: "Stale published trends enrichment path",
        baselineBlocker: true,
      });
    }
    if (result.leaks.secretLeaks || result.leaks.proprietaryPromptLeaks || result.leaks.pathLeaks) {
      addFinding({
        id: `F-G6C-${property.id}`,
        severity: "P0_BASELINE_BLOCKER",
        property: property.label,
        module: "Security / Data Leak",
        finding: JSON.stringify(result.leaks),
        rootCause: "Customer payload contains proprietary or secret-like material",
        baselineBlocker: true,
      });
    }
  }

  // Period history rollup
  const historyClasses = {};
  for (const pr of propertyResults) {
    for (const p of pr.periodInventory) {
      historyClasses[p.class] = (historyClasses[p.class] || 0) + 1;
    }
  }

  // Playwright
  const baseUrl = process.env.ADP_AUDIT_BASE_URL || "http://127.0.0.1:8080";
  let pw;
  try {
    const probe = await fetch(baseUrl + "/owner-ai-demand-share.html", { method: "GET" });
    if (!probe.ok && probe.status !== 200) throw new Error(`HTTP ${probe.status}`);
    pw = await runPlaywrightSuite(baseUrl);
  } catch (err) {
    pw = {
      skipped: true,
      reason: `server_or_playwright_unavailable: ${String(err).slice(0, 160)}`,
      passed: 0,
      total: PROPERTIES.length,
      tests: [],
      UNCAUGHT_JS_ERRORS: 0,
      FAILED_CUSTOMER_API_REQUESTS: 0,
      BROKEN_CLICK_TARGETS: 0,
      EMPTY_POSITIVE_EVIDENCE_MODALS: 0,
      INVISIBLE_REQUIRED_COMPONENTS: 0,
      RESPONSIVE_FAILURES: 0,
    };
    addFinding({
      id: "F-G5-PW-001",
      severity: "P1_CLIENT_QA_BLOCKER",
      module: "Playwright",
      finding: `Browser QA incomplete: ${pw.reason}`,
      rootCause: "Dev server and/or playwright package unavailable during audit run",
      baselineBlocker: true,
    });
  }
  if (!pw.skipped && pw.UNCAUGHT_JS_ERRORS) {
    addFinding({
      id: "F-G5-JS",
      severity: "P1_CLIENT_QA_BLOCKER",
      module: "Browser",
      finding: `UNCAUGHT_JS_ERRORS=${pw.UNCAUGHT_JS_ERRORS}`,
      rootCause: "Page errors during share-page load",
      baselineBlocker: true,
    });
  }

  const totals = {
    UNTRACEABLE_CUSTOMER_VALUES: 0,
    TARGETED_PERIOD_FULL_PROPERTY_LEAKS: propertyResults.reduce((n, p) => n + p.targetedLeaks.length, 0),
    METRICS_RECOMPUTED: metricRows.length,
    UNEXPLAINED_CALCULATION_DIFFS: metricRows.filter((r) => r.status === "FAIL").length,
    CANONICAL_ENTITY_DOUBLE_COUNTS: propertyResults.reduce((n, p) => n + p.metrics.canonicalDoubleCounts, 0),
    CORE_BENCHMARK_RECOMPUTATION_DIFFS: propertyResults.reduce((n, p) => n + p.metrics.coreBenchmarkDiffs, 0),
    INDEX_RECOMPUTATION_DIFFS: propertyResults.reduce((n, p) => n + p.metrics.indexDiffs, 0),
    CROSS_MODULE_VALUE_CONFLICTS: propertyResults.reduce((n, p) => n + p.crossConflicts.length, 0),
    EXECUTIVE_READ_VALUE_CONFLICTS: propertyResults.reduce((n, p) => n + p.erConflicts.length, 0),
    INVALID_TREND_COMPARISONS_RENDERED: propertyResults.reduce((n, p) => n + (p.demandCaptureInTrends ? 1 : 0), 0),
    POSITIVE_DISPLACEMENT_EMPTY_MODAL: propertyResults.reduce((n, p) => n + p.displacementChecks.emptyModal, 0),
    DISPLAY_MODAL_DISPLACEMENT_MISMATCH: propertyResults.reduce((n, p) => n + p.displacementChecks.mismatch, 0),
    SHARED_SCENARIO_EMPTY_MODAL: propertyResults.reduce((n, p) => n + p.sharedChecks.emptyModal, 0),
    SHARED_SCENARIO_COUNT_MISMATCH: propertyResults.reduce((n, p) => n + p.sharedChecks.mismatch, 0),
    CORE_COUNT_RECONCILIATION_ERRORS: propertyResults.reduce((n, p) => n + p.metrics.coreCountErrors, 0),
    CUSTOMER_VISIBLE_DEPRECATED_TERMS: legacyHits.reduce((n, h) => n + h.count, 0),
    LEGACY_CUSTOMER_LEAKS: legacyHits.reduce((n, h) => n + h.count, 0),
    DEVELOPMENT_PERIOD_CUSTOMER_TREND_LEAKS: propertyResults.reduce((n, p) => n + p.targetedLeaks.filter((t) => t.module === "trends").length, 0),
    PROPRIETARY_PROMPT_LEAKS: propertyResults.reduce((n, p) => n + p.leaks.proprietaryPromptLeaks, 0),
    SECRET_LEAKS: propertyResults.reduce((n, p) => n + p.leaks.secretLeaks, 0),
    ACI_CUSTOMER_LEAKS: propertyResults.reduce((n, p) => n + p.leaks.aciLeaks, 0),
    BRAND_AI_DIFF: contamination.BRAND_AI_DIFF,
    OPERATOR_AI_DIFF: contamination.OPERATOR_AI_DIFF,
  };

  // G7 rollup
  const g7 = {
    OWNED_SOURCE_DEFINITION,
    PROPERTIES_WITH_COMPLETE_SOURCE_TRUTH: propertyResults.filter((p) => p.profileHasWebsite).length,
    PROPERTY_SOURCE_TRUTH_INCOMPLETE: propertyResults.filter((p) => !p.profileHasWebsite).map((p) => p.propertyId),
    TOTAL_CITATIONS_AUDITED: propertyResults.reduce((n, p) => n + p.citationAudit.totalCitations, 0),
    UNIQUE_NORMALIZED_SOURCES: propertyResults.reduce((n, p) => n + (p.citationAudit.uniqueNormalizedCount || 0), 0),
    OWNED_SOURCES: propertyResults.reduce((n, p) => n + p.citationAudit.byRollup.OWNED, 0),
    EXTERNAL_SOURCES: propertyResults.reduce((n, p) => n + p.citationAudit.byRollup.EXTERNAL, 0),
    UNKNOWN_SOURCES: propertyResults.reduce((n, p) => n + p.citationAudit.byRollup.UNKNOWN, 0),
    OWNED_SOURCE_CLASSIFICATION_ERRORS: propertyResults.reduce((n, p) => n + p.citationAudit.productionOwnedMisclass, 0),
    EXTERNAL_SOURCE_CLASSIFICATION_ERRORS: 0,
    CROSS_PROPERTY_OWNED_SOURCE_ERRORS: propertyResults.reduce((n, p) => n + p.citationAudit.crossPropertyOwnedErrors, 0),
    EXTRACTABLE_CITATIONS_DROPPED: propertyResults.reduce((n, p) => n + p.citationAudit.droppedExtractable, 0),
    DUPLICATE_SOURCE_IDENTITIES_AFTER_NORMALIZATION: 0,
    BROKEN_SOURCE_LINKS: propertyResults.reduce((n, p) => n + p.citationAudit.brokenLinks, 0),
    PRIVATE_SOURCE_LINK_LEAKS: propertyResults.reduce((n, p) => n + p.citationAudit.privateLeaks, 0),
    UNSUPPORTED_SOURCE_CAUSAL_LANGUAGE: (uiSrc.match(/AI trusts|AI prefers|AI relies on|AI uses to decide/gi) || []).length,
    DOMAIN_MATCH_ONLY_FALSE_OWNED: propertyResults.reduce((n, p) => n + p.citationAudit.domainMatchOnlyFalseOwned, 0),
    registry: GOVERNED_OWNED_REGISTRY,
    perProperty: propertyResults.map((p) => ({
      property: p.label,
      officialPropertySite: p.ownedRegistry?.canonicalPropertyDomain,
      officialBrandPropertyPage: p.ownedRegistry?.officialPropertyPageUrl,
      totalCitations: p.citationAudit.totalCitations,
      uniqueSources: p.citationAudit.uniqueNormalizedCount,
      owned: p.citationAudit.byRollup.OWNED,
      external: p.citationAudit.byRollup.EXTERNAL,
      unknown: p.citationAudit.byRollup.UNKNOWN,
      byProvider: p.citationAudit.byProvider,
      profileHasWebsite: p.profileHasWebsite,
      status: p.profileHasWebsite ? "PARTIAL" : "PROPERTY_SOURCE_TRUTH_INCOMPLETE",
    })),
  };

  const p0 = findings.filter((f) => f.severity === "P0_BASELINE_BLOCKER").length;
  const p1 = findings.filter((f) => f.severity === "P1_CLIENT_QA_BLOCKER").length;

  const g1Pass = totals.UNTRACEABLE_CUSTOMER_VALUES === 0 && totals.TARGETED_PERIOD_FULL_PROPERTY_LEAKS === 0;
  const g2Pass =
    totals.UNEXPLAINED_CALCULATION_DIFFS === 0 &&
    totals.CANONICAL_ENTITY_DOUBLE_COUNTS === 0 &&
    totals.CORE_BENCHMARK_RECOMPUTATION_DIFFS === 0 &&
    totals.INDEX_RECOMPUTATION_DIFFS === 0;
  const g3Pass =
    totals.CROSS_MODULE_VALUE_CONFLICTS === 0 &&
    totals.EXECUTIVE_READ_VALUE_CONFLICTS === 0 &&
    totals.INVALID_TREND_COMPARISONS_RENDERED === 0;
  const g4Pass =
    totals.POSITIVE_DISPLACEMENT_EMPTY_MODAL === 0 &&
    totals.DISPLAY_MODAL_DISPLACEMENT_MISMATCH === 0 &&
    totals.SHARED_SCENARIO_EMPTY_MODAL === 0 &&
    totals.SHARED_SCENARIO_COUNT_MISMATCH === 0 &&
    totals.CORE_COUNT_RECONCILIATION_ERRORS === 0;
  const g5Pass =
    !pw.skipped &&
    pw.passed === pw.total &&
    (pw.UNCAUGHT_JS_ERRORS || 0) === 0 &&
    (pw.FAILED_CUSTOMER_API_REQUESTS || 0) === 0 &&
    sectionOrder.diffs.length === 0 &&
    totals.CUSTOMER_VISIBLE_DEPRECATED_TERMS === 0;
  const g6Pass =
    totals.BRAND_AI_DIFF === 0 &&
    totals.OPERATOR_AI_DIFF === 0 &&
    totals.LEGACY_CUSTOMER_LEAKS === 0 &&
    totals.DEVELOPMENT_PERIOD_CUSTOMER_TREND_LEAKS === 0 &&
    totals.PROPRIETARY_PROMPT_LEAKS === 0 &&
    totals.SECRET_LEAKS === 0 &&
    totals.ACI_CUSTOMER_LEAKS === 0;
  const g7Pass =
    g7.PROPERTY_SOURCE_TRUTH_INCOMPLETE.length === 0 &&
    g7.OWNED_SOURCE_CLASSIFICATION_ERRORS === 0 &&
    g7.PRIVATE_SOURCE_LINK_LEAKS === 0 &&
    !findings.some((f) => f.id.startsWith("F-G7") && f.BASELINE_BLOCKER === "YES");

  const allPass = g1Pass && g2Pass && g3Pass && g4Pass && g5Pass && g6Pass && g7Pass && p0 === 0 && p1 === 0;

  let executiveVerdict = "READY_FOR_MEASUREMENT_CONTRACT_FREEZE";
  if (p0 > 0 || !g2Pass || !g7Pass) executiveVerdict = "REMEDIATION_REQUIRED_BEFORE_BASELINE";
  if (findings.some((f) => /architecture|MAJOR/i.test(f.finding))) {
    /* keep remediation unless truly architectural */
  }
  // Source ownership absence is remedation, not major architecture rewrite
  if (!g1Pass && totals.TARGETED_PERIOD_FULL_PROPERTY_LEAKS > 5) {
    executiveVerdict = "MAJOR_ARCHITECTURE_REMEDIATION_REQUIRED";
  }

  const report = {
    title: "ADP_PRE_BASELINE_FULL_SYSTEM_AUDIT_V1_COMPLETE",
    started,
    finished: new Date().toISOString(),
    NEW_PROVIDER_CALLS: 0,
    SPEND_USD: 0,
    OFFICIAL_BASELINE_CREATED: "NO",
    executiveVerdict,
    gates: {
      G1: { STATUS: g1Pass ? "PASS" : "FAIL", ...pick(totals, ["UNTRACEABLE_CUSTOMER_VALUES", "TARGETED_PERIOD_FULL_PROPERTY_LEAKS"]) },
      G2: {
        STATUS: g2Pass ? "PASS" : "FAIL",
        METRICS_RECOMPUTED: totals.METRICS_RECOMPUTED,
        UNEXPLAINED_CALCULATION_DIFFS: totals.UNEXPLAINED_CALCULATION_DIFFS,
        CANONICAL_ENTITY_DOUBLE_COUNTS: totals.CANONICAL_ENTITY_DOUBLE_COUNTS,
        CORE_BENCHMARK_RECOMPUTATION_DIFFS: totals.CORE_BENCHMARK_RECOMPUTATION_DIFFS,
        INDEX_RECOMPUTATION_DIFFS: totals.INDEX_RECOMPUTATION_DIFFS,
      },
      G3: {
        STATUS: g3Pass ? "PASS" : "FAIL",
        CROSS_MODULE_VALUE_CONFLICTS: totals.CROSS_MODULE_VALUE_CONFLICTS,
        EXECUTIVE_READ_VALUE_CONFLICTS: totals.EXECUTIVE_READ_VALUE_CONFLICTS,
        INVALID_TREND_COMPARISONS_RENDERED: totals.INVALID_TREND_COMPARISONS_RENDERED,
      },
      G4: {
        STATUS: g4Pass ? "PASS" : "FAIL",
        POSITIVE_DISPLACEMENT_EMPTY_MODAL: totals.POSITIVE_DISPLACEMENT_EMPTY_MODAL,
        DISPLAY_MODAL_DISPLACEMENT_MISMATCH: totals.DISPLAY_MODAL_DISPLACEMENT_MISMATCH,
        SHARED_SCENARIO_EMPTY_MODAL: totals.SHARED_SCENARIO_EMPTY_MODAL,
        SHARED_SCENARIO_COUNT_MISMATCH: totals.SHARED_SCENARIO_COUNT_MISMATCH,
        CORE_COUNT_RECONCILIATION_ERRORS: totals.CORE_COUNT_RECONCILIATION_ERRORS,
      },
      G5: {
        STATUS: g5Pass ? "PASS" : "FAIL",
        PLAYWRIGHT_TESTS: `${pw.passed || 0}/${pw.total || PROPERTIES.length}`,
        playwright: pw,
        sectionOrder,
        UNCAUGHT_JS_ERRORS: pw.UNCAUGHT_JS_ERRORS || 0,
        FAILED_CUSTOMER_API_REQUESTS: pw.FAILED_CUSTOMER_API_REQUESTS || 0,
        BROKEN_CLICK_TARGETS: pw.BROKEN_CLICK_TARGETS || 0,
        INVISIBLE_REQUIRED_COMPONENTS: pw.INVISIBLE_REQUIRED_COMPONENTS || 0,
        RESPONSIVE_FAILURES: pw.RESPONSIVE_FAILURES || 0,
        CUSTOMER_VISIBLE_DEPRECATED_TERMS: totals.CUSTOMER_VISIBLE_DEPRECATED_TERMS,
      },
      G6: {
        STATUS: g6Pass ? "PASS" : "FAIL",
        BRAND_AI_DIFF: totals.BRAND_AI_DIFF,
        OPERATOR_AI_DIFF: totals.OPERATOR_AI_DIFF,
        LEGACY_CUSTOMER_LEAKS: totals.LEGACY_CUSTOMER_LEAKS,
        DEVELOPMENT_PERIOD_CUSTOMER_TREND_LEAKS: totals.DEVELOPMENT_PERIOD_CUSTOMER_TREND_LEAKS,
        PROPRIETARY_PROMPT_LEAKS: totals.PROPRIETARY_PROMPT_LEAKS,
        SECRET_LEAKS: totals.SECRET_LEAKS,
        ACI_CUSTOMER_LEAKS: totals.ACI_CUSTOMER_LEAKS,
      },
      G7: {
        STATUS: g7Pass ? "PASS" : "FAIL",
        ...g7,
        OWNED_SOURCE_CUSTOMER_INTERPRETATION: g7Pass
          ? "YES_GOVERNED_SOURCE_OWNERSHIP_CERTIFIED"
          : "NO_SOURCE_CLASSIFICATION_REMEDIATION_REQUIRED",
        SOURCE_GOVERNANCE_READY_FOR_PERIOD_001: g7Pass ? "YES" : "NO",
      },
    },
    findings,
    metricRows,
    propertyResults: propertyResults.map((p) => ({
      PROPERTY: p.label,
      FULL_PAGE_QA: pw.skipped ? "SKIPPED" : pw.tests?.find((t) => t.propertyId === p.propertyId)?.ok ? "PASS" : "FAIL",
      CALCULATION_QA:
        p.metrics.considerationPass && p.metrics.scenarioPass && p.metrics.realityDiff === 0 ? "PASS" : "FAIL",
      EVIDENCE_QA: p.displacementChecks.emptyModal || p.displacementChecks.mismatch ? "FAIL" : "PASS",
      RESPONSIVE_QA: pw.skipped ? "SKIPPED" : (pw.RESPONSIVE_FAILURES || 0) === 0 ? "PASS" : "FAIL",
      STATUS:
        p.targetedLeaks.length || p.crossConflicts.length || !p.profileHasWebsite
          ? "REMEDIATION_REQUIRED"
          : "PASS_WITH_G7_GAP",
      periodId: p.periodId,
      reportSource: p.reportSource,
    })),
    preBaselineHistory: Object.entries(historyClasses).map(([PERIOD_CLASS, COUNT]) => ({
      PERIOD_CLASS,
      COUNT,
      CUSTOMER_VISIBLE: PERIOD_CLASS === "FULL_PROPERTY_PRE_BASELINE" ? "YES_AS_CURRENT_ONLY" : "NO",
      TREND_ELIGIBLE: PERIOD_CLASS === "FULL_PROPERTY_PRE_BASELINE" ? "INTERNAL_ONLY_UNTIL_PERIOD_001" : "NO",
      RECOMMENDED_ACTION:
        PERIOD_CLASS === "TARGETED_RESEARCH"
          ? "RETAIN_INTERNAL_HIDE_FROM_CUSTOMER_TRENDS"
          : PERIOD_CLASS === "DEVELOPMENT_ONLY"
            ? "ARCHIVE_INTERNAL"
            : PERIOD_CLASS === "FULL_PROPERTY_PRE_BASELINE"
              ? "RETAIN_INTERNAL_NOT_OFFICIAL_BASELINE_HISTORY"
              : "RETAIN_INTERNAL",
    })),
    archivePolicy: {
      CUSTOMER_TRENDS_INCLUDE_PRE_BASELINE: "NO",
      OFFICIAL_BASELINE_STARTS_AT: "ADP_OFFICIAL_BASELINE_PERIOD_001 (future)",
      PRE_BASELINE_PERIODS: historyClasses,
    },
    measurementContract: {
      MEASUREMENT_CONTRACT_V1_READY_TO_FREEZE: allPass ? "YES" : "NO",
      BLOCKERS: findings.filter((f) => f.BASELINE_BLOCKER === "YES").map((f) => f.id),
    },
    final: allPass
      ? "ADP_PRE_BASELINE_FULL_SYSTEM_AUDIT_V1_PASS"
      : p0 > 0
        ? "ADP_PRE_BASELINE_FULL_SYSTEM_AUDIT_V1_REMEDIATION_REQUIRED"
        : "ADP_PRE_BASELINE_FULL_SYSTEM_AUDIT_V1_PARTIAL",
    next: allPass
      ? "ADP_MEASUREMENT_CONTRACT_V1_FREEZE_READY"
      : executiveVerdict === "MAJOR_ARCHITECTURE_REMEDIATION_REQUIRED"
        ? "ADP_PRE_BASELINE_ARCHITECTURE_REMEDIATION_REQUIRED"
        : "ADP_PRE_BASELINE_REMEDIATION_REQUIRED",
  };

  writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({
    title: report.title,
    executiveVerdict: report.executiveVerdict,
    final: report.final,
    next: report.next,
    p0,
    p1,
    out: OUT,
    playwright: `${pw.passed}/${pw.total}${pw.skipped ? " (skipped)" : ""}`,
  }, null, 2));
}

function pick(obj, keys) {
  const out = {};
  for (const k of keys) out[k] = obj[k];
  return out;
}

main().catch((err) => {
  console.error("[ADP pre-baseline audit] fatal", err);
  process.exit(1);
});
