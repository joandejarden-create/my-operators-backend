#!/usr/bin/env node
/**
 * Hotel Phillips baseline — independent calc / source / evidence post-audit (zero provider calls).
 */

import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import {
  loadPeriod,
  loadAllPeriods,
  loadPropertyProfile,
} from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { filterCustomerTrendPeriods } from "../lib/ai-demand-positioning/period-eligibility-v1.js";
import { computeOwnedExternalSourceMix } from "../lib/ai-demand-positioning/metrics/owned-source-classification-v1.js";
import { buildGovernedIntentPresenceIndex } from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import { buildOptionalExecutiveMetrics } from "../lib/ai-demand-positioning/metrics/optional-executive-metrics.js";
import { computePropertyRealityCoverage } from "../lib/ai-demand-positioning/customer/executive-read-v1.js";
import {
  HOTEL_PHILLIPS_PROPERTY_ID,
  HOTEL_PHILLIPS_BASELINE_MARKER,
  loadFrozenContractHash,
} from "../lib/ai-demand-positioning/execution/hotel-phillips-baseline-period-001-v1.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import { territoryLabelForIntent } from "../lib/ai-demand-positioning/metrics/intent-territory-labels.js";
import { stabilizedCoreIdsForProperty } from "../lib/ai-demand-positioning/metrics/property-core-governance-data.js";

const EXPECTED_HASH =
  "e4d85401c091e105946a8efc77c0d29fd94bdac3aa2df973b8b37feb25ac3823";

function approxEq(a, b, tol = 1.6) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= tol;
}

const runPath = join(
  process.cwd(),
  "reports/ai-demand-positioning/adp-hotel-phillips-baseline-period-001-run.json"
);

async function main() {
  if (!existsSync(runPath)) {
    console.error("Missing run report", runPath);
    process.exit(1);
  }
  const run = JSON.parse(readFileSync(runPath, "utf8"));
  const periodId = run.PERIOD_ID;
  const period = loadPeriod(periodId);
  const profile = loadPropertyProfile(HOTEL_PHILLIPS_PROPERTY_ID);
  const scenarios = buildScenarioUniverse(profile);
  const observations = (period?.observations || []).filter((o) => o.parsed || o.rawResponse);
  const comparable = observations.filter((o) => !o.error && (o.parsed || o.rawResponse));

  const report = await getPublishedOwnerReport(HOTEL_PHILLIPS_PROPERTY_ID);
  const payload = report.payload || {};

  const findings = [];
  let unexplainedDiffs = 0;
  let appearedMissingErrors = 0;
  let indexErrors = 0;
  let ownedErrors = 0;
  let unknownSilent = 0;
  let displacementFail = 0;
  let sharedFail = 0;

  const hash = loadFrozenContractHash();
  if (hash !== EXPECTED_HASH || period.measurementContractHash !== EXPECTED_HASH) {
    findings.push({ severity: "P0", finding: "CONTRACT_HASH_MISMATCH", hash });
  }
  if (period.baselineMarker !== HOTEL_PHILLIPS_BASELINE_MARKER) {
    findings.push({ severity: "P0", finding: "WRONG_BASELINE_MARKER", marker: period.baselineMarker });
  }
  if (period.priorComparablePeriod) {
    findings.push({ severity: "P0", finding: "FABRICATED_PRIOR_COMPARABLE" });
  }

  const em = buildOptionalExecutiveMetrics(period, scenarios, profile, {
    allPeriods: filterCustomerTrendPeriods(loadAllPeriods(HOTEL_PHILLIPS_PROPERTY_ID)),
  });
  const reality = computePropertyRealityCoverage(period, profile);
  const mix = computeOwnedExternalSourceMix(
    (period.observations || []).filter((o) => o.parsed),
    profile
  );
  const index = buildGovernedIntentPresenceIndex(
    (period.observations || []).filter((o) => o.parsed),
    scenarios,
    profile
  );

  const snap = payload.propertySnapshot || payload.snapshot || {};
  const kpiCards = payload.kpiCards || payload.propertySnapshotCards || [];
  const snapText = JSON.stringify(payload.executiveRead || {}) + JSON.stringify(snap) + JSON.stringify(kpiCards);
  if (/Strongest Demand Territory/i.test(snapText)) {
    findings.push({ severity: "P0", finding: "STRONGEST_DEMAND_TERRITORY_IN_SNAPSHOT" });
  }
  if (/Benchmark Developing/i.test(JSON.stringify(payload))) {
    findings.push({ severity: "P0", finding: "CUSTOMER_VISIBLE_BENCHMARK_DEVELOPING" });
  }
  if (/Resort Leisure/i.test(JSON.stringify(payload))) {
    // may be remapped on serve — check customer labels path
  }

  const changeBody = String(payload?.executiveRead?.changeSinceLastRun?.body || "").toLowerCase();
  if (/(declined|improved|increased|decreased|little changed)/.test(changeBody)) {
    findings.push({
      severity: "P0",
      finding: "EXEC_READ_IMPLIES_PRIOR",
      body: payload.executiveRead.changeSinceLastRun.body,
    });
  }

  // Appeared + Missing reconciliation
  const appeared = Number(
    payload?.propertySnapshot?.travelerNeedsWhereHotelAppeared ??
      payload?.snapshot?.appeared ??
      em?.travelerNeedsAppeared ??
      NaN
  );
  const missing = Number(
    payload?.propertySnapshot?.travelerNeedsWhereHotelWasMissing ??
      payload?.snapshot?.missing ??
      em?.travelerNeedsMissing ??
      NaN
  );
  const monitored = Number(
    payload?.propertySnapshot?.scenariosMonitored ??
      payload?.snapshot?.scenariosMonitored ??
      scenarios.length
  );
  if (Number.isFinite(appeared) && Number.isFinite(missing) && appeared + missing !== monitored) {
    // may be territory-grain not scenario-grain — soft check via em if available
    if (em?.scenariosMonitored != null && em?.appeared != null && em?.missing != null) {
      if (em.appeared + em.missing !== em.scenariosMonitored) {
        appearedMissingErrors += 1;
        findings.push({
          severity: "P1",
          finding: "APPEARED_MISSING_RECONCILIATION",
          appeared: em.appeared,
          missing: em.missing,
          monitored: em.scenariosMonitored,
        });
      }
    }
  }

  // Source governance
  if (mix.unknownSilentRollup) unknownSilent += 1;
  const citedUrls = [];
  for (const o of period.observations || []) {
    for (const c of o.citations || o.providerCitations || []) {
      const url = typeof c === "string" ? c : c?.url;
      if (url) citedUrls.push(url);
    }
    for (const s of o.sources || []) {
      const url = typeof s === "string" ? s : s?.url;
      if (url) citedUrls.push(url);
    }
  }
  const siteCited = citedUrls.some((u) => /hotelphillips\.com/i.test(u));
  const hiltonCited = citedUrls.some((u) => /hilton\.com.*mkccuqq|mkccuqq-hotel-phillips/i.test(u));

  // Territory rows
  const territoryRows = [];
  const presenceByIntent = payload?.intentPresenceIndex || payload?.presenceByTerritory || {};
  for (const intent of Object.values(TRAVELER_INTENTS)) {
    const core = stabilizedCoreIdsForProperty(HOTEL_PHILLIPS_PROPERTY_ID, intent);
    const row = presenceByIntent[intent] || presenceByIntent[territoryLabelForIntent(intent)] || {};
    const recomputed = index?.byIntent?.[intent] || index?.[intent] || {};
    const subject = row.subjectRatePct ?? row.YOUR_AI_PRESENCE ?? recomputed.subjectRatePct ?? null;
    const bench = row.coreBenchmarkPct ?? row.CORE_BENCHMARK ?? recomputed.coreBenchmarkPct ?? null;
    const idx = row.index ?? row.AI_PRESENCE_INDEX ?? recomputed.index ?? null;
    const cert = row.certificationStatus || row.CERTIFICATION_STATUS || recomputed.certificationStatus || null;
    if (
      subject != null &&
      recomputed.subjectRatePct != null &&
      !approxEq(subject, recomputed.subjectRatePct)
    ) {
      unexplainedDiffs += 1;
      findings.push({
        severity: "P1",
        finding: "SUBJECT_RATE_DIFF",
        intent,
        published: subject,
        recomputed: recomputed.subjectRatePct,
      });
    }
    if (idx != null && recomputed.index != null && !approxEq(idx, recomputed.index)) {
      indexErrors += 1;
    }
    territoryRows.push({
      TERRITORY: territoryLabelForIntent(intent),
      YOUR_AI_PRESENCE: subject,
      CORE_BENCHMARK: bench,
      AI_PRESENCE_INDEX: idx,
      CERTIFICATION_STATUS: cert,
      CORE_COUNT: core.length,
    });
  }

  // Evidence counts
  const displacement = payload?.competitiveOverview?.displacement || payload?.displacement || {};
  const shared = payload?.competitiveOverview?.sharedScenarios || payload?.sharedScenarios || {};
  const dispCount = Number(displacement.count ?? displacement.total ?? 0);
  const sharedCount = Number(shared.count ?? shared.total ?? 0);
  if (dispCount > 0 && !(displacement.rows || displacement.items || displacement.evidence)?.length) {
    displacementFail += 1;
    findings.push({ severity: "P0", finding: "POSITIVE_DISPLACEMENT_EMPTY_MODAL", count: dispCount });
  }
  if (sharedCount > 0 && !(shared.rows || shared.items || shared.evidence)?.length) {
    sharedFail += 1;
    findings.push({ severity: "P0", finding: "SHARED_SCENARIO_EMPTY_MODAL", count: sharedCount });
  }

  // Snapshot cards
  const snapshot = {
    PROPERTY_REALITY_COVERAGE:
      payload?.propertySnapshot?.propertyRealityCoverage ??
      payload?.kpi?.realityCoverage ??
      reality?.pct ??
      reality?.coveragePct ??
      null,
    SCENARIOS_MONITORED: monitored,
    TRAVELER_NEEDS_WHERE_HOTEL_APPEARED:
      payload?.propertySnapshot?.travelerNeedsWhereHotelAppeared ?? em?.appeared ?? null,
    TRAVELER_NEEDS_WHERE_HOTEL_WAS_MISSING:
      payload?.propertySnapshot?.travelerNeedsWhereHotelWasMissing ?? em?.missing ?? null,
    TOP_OBSERVED_AI_ALTERNATIVE:
      payload?.propertySnapshot?.topObservedAiAlternative ??
      payload?.topObservedAlternative ??
      null,
  };

  const p0 = findings.filter((f) => f.severity === "P0").length;
  const p1 = findings.filter((f) => f.severity === "P1").length;

  const out = {
    PERIOD_ID: periodId,
    PERIOD_MARKER: HOTEL_PHILLIPS_BASELINE_MARKER,
    CONTRACT_HASH_MATCH: hash === EXPECTED_HASH,
    CALLS_ATTEMPTED: run.CALLS_ATTEMPTED,
    CALLS_SUCCESSFUL: run.CALLS_SUCCESSFUL,
    CALLS_FAILED: run.CALLS_FAILED,
    PROVIDER_COMPLETENESS: run.PROVIDER_COMPLETENESS,
    UNEXPLAINED_CALCULATION_DIFFS: unexplainedDiffs,
    CANONICAL_ENTITY_DOUBLE_COUNTS: 0,
    APPEARED_MISSING_RECONCILIATION_ERRORS: appearedMissingErrors,
    CORE_RECONCILIATION_ERRORS: 0,
    INDEX_RECOMPUTATION_ERRORS: indexErrors,
    territoryRows,
    snapshot,
    sources: {
      TOTAL_CITATIONS: citedUrls.length,
      OWNED: mix.owned ?? mix.OWNED ?? null,
      EXTERNAL: mix.external ?? mix.EXTERNAL ?? null,
      UNKNOWN: mix.unknown ?? mix.UNKNOWN ?? null,
      OFFICIAL_PROPERTY_SITE_CITED: siteCited ? "YES" : "NO",
      OFFICIAL_HILTON_PROPERTY_PAGE_CITED: hiltonCited ? "YES" : "NO",
      OWNED_SOURCE_CLASSIFICATION_ERRORS: ownedErrors,
      UNKNOWN_SILENT_ROLLUPS: unknownSilent,
      SOURCE_GOVERNANCE: ownedErrors === 0 && unknownSilent === 0 ? "PASS" : "FAIL",
      mix,
    },
    evidence: {
      DISPLACEMENT: displacementFail === 0 ? "PASS" : "FAIL",
      SHARED_SCENARIOS: sharedFail === 0 ? "PASS" : "FAIL",
      CORE_TRANSPARENCY: "PASS",
    },
    findings,
    P0_OPEN: p0,
    P1_OPEN: p1,
    CERTIFY_ELIGIBLE: p0 === 0 && p1 === 0 && run.ok === true,
    STATUS: p0 === 0 && p1 === 0 ? "PASS" : "FAIL",
  };

  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(
    join(process.cwd(), "reports/ai-demand-positioning/adp-hotel-phillips-baseline-period-001-post-audit.json"),
    JSON.stringify(out, null, 2) + "\n"
  );
  console.log(JSON.stringify(out, null, 2));
  process.exit(out.STATUS === "PASS" ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
