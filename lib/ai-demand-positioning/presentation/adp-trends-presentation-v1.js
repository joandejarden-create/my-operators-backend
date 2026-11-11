/**
 * ADP_TRENDS_PRESENTATION_V1
 *
 * Single Trends component for all ADP hotels (Bethesda structural master).
 * Never swaps to a divergent empty-only component — same KPI slots + chart shell.
 *
 * Current-period metrics are independent of historical trend availability:
 * - MULTI_PERIOD: ≥2 comparable trend points
 * - BASELINE_ONLY: one certified measurement (from trends[] OR executiveMetrics)
 * - NO_BASELINE: no qualifying measured period for this report context
 * - EVIDENCE_BLOCKED: measurement claimed but cannot be certified for display
 *
 * Empty trends[] must NEVER erase valid current-period executiveMetrics.
 */

import {
  ADP_TRENDS_KPI_SLOTS_V1,
  TRENDS_STATE,
} from "./adp-master-report-presentation-v1.js";
import { normalizeReportTrends } from "../normalize-report-trends-v1.js";

export const ADP_TRENDS_PRESENTATION_V1 = "ADP_TRENDS_PRESENTATION_V1";

export const TRENDS_SECTION_TITLE = "Trends";
export const TRENDS_SECTION_SUBTITLE =
  "How these results change from one comparable monitoring period to the next.";

function round1(n) {
  if (n == null || Number.isNaN(Number(n))) return null;
  return Math.round(Number(n) * 10) / 10;
}

function fmtPct(n) {
  const v = round1(n);
  return v == null ? "—" : `${v}%`;
}

function finiteRate(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Derive Reality Coverage from realityGap — never treat gapScore as coverage.
 * gapScore is the unrecognized share (e.g. 80); coverage is recognized/total (e.g. 20).
 */
export function derivePropertyRealityCoverage(realityGap = {}) {
  const rg = realityGap || {};
  if (Number.isFinite(Number(rg.coveragePercent))) {
    return Number(rg.coveragePercent);
  }
  // Prefer scored fully-recognized / testedEligible when available
  if (
    Number.isFinite(Number(rg.testedEligibleCount)) &&
    rg.testedEligibleCount > 0 &&
    rg.recognizedCountScored != null
  ) {
    return (
      Math.round(
        (Number(rg.recognizedCountScored) / Number(rg.testedEligibleCount)) * 1000
      ) / 10
    );
  }
  if (rg.totalAttributes > 0 && rg.recognizedCount != null) {
    return (
      Math.round(
        (Number(rg.recognizedCount) / Number(rg.totalAttributes)) * 1000
      ) / 10
    );
  }
  return null;
}

/**
 * Extract a single current-period trend point from executiveMetrics / period /
 * realityGap when trends[] is absent. Does not invent historical movement.
 *
 * @param {object} payload
 * @returns {object|null}
 */
export function extractCurrentPeriodTrendPointFromPayload(payload = {}) {
  if (!payload || typeof payload !== "object") return null;
  const em = payload.executiveMetrics || {};
  const considerationRate = finiteRate(em.considerationRate?.rate);
  const scenarioPresenceRate = finiteRate(em.scenarioPresence?.rate);
  const propertyRealityCoverage = derivePropertyRealityCoverage(
    payload.realityGap
  );

  const hasRate =
    considerationRate != null ||
    scenarioPresenceRate != null ||
    propertyRealityCoverage != null;
  if (!hasRate) return null;

  const period = payload.period || {};
  const baselineDate =
    period.baselineDate ||
    payload.baselineMonitoring?.baselineDate ||
    period.executionDate ||
    period.completedAt ||
    payload.publishedAt ||
    null;

  return {
    periodId: period.periodId || payload.periodId || null,
    date: baselineDate,
    baselineDate:
      period.baselineDate || payload.baselineMonitoring?.baselineDate || null,
    baselineId: period.baselineId || payload.baselineMonitoring?.baselineId || null,
    propertyRealityCoverage,
    scenarioPresenceRate,
    considerationRate,
    demandCaptureRate: payload.demandCapture?.overallRate ?? null,
    providerCount: null,
    observationCount: em.considerationRate?.comparableObservations ?? null,
    measurementContractVersion:
      payload.measurementContractVersion ||
      period.measurementContractVersion ||
      null,
    role: "current",
    isOfficialBaseline: true,
    monitoringStatus:
      payload.baselineMonitoring?.monitoringStatus || "ACTIVE",
    nextFormalRemeasurementLabel:
      payload.baselineMonitoring?.nextFormalRemeasurementLabel || null,
    _synthesizedFromCurrentPeriodMetrics: true,
  };
}

/**
 * Resolve evidence blockers when a period is claimed but rates cannot display.
 * @param {object} payload
 * @returns {string[]|null}
 */
export function resolveTrendsEvidenceBlockers(payload = {}) {
  const explicit =
    payload.trendsEvidenceBlockers ||
    payload._trendsEvidenceBlockers ||
    payload.evidenceBlockers?.trends ||
    null;
  if (Array.isArray(explicit) && explicit.length) {
    return explicit.map(String);
  }
  if (typeof explicit === "string" && explicit.trim()) {
    return [explicit.trim()];
  }

  const periodId = payload.period?.periodId || payload.periodId || null;
  const monitoringClaimed =
    !!periodId ||
    payload.baselineMonitoring?.monitoringStatus === "ACTIVE" ||
    payload.baselineMonitoring?.isOfficialBaseline === true;

  if (!monitoringClaimed) return null;

  const em = payload.executiveMetrics || {};
  const hasRate =
    finiteRate(em.considerationRate?.rate) != null ||
    finiteRate(em.scenarioPresence?.rate) != null ||
    derivePropertyRealityCoverage(payload.realityGap) != null;

  if (hasRate) return null;

  // Period / monitoring claimed but no displayable rates on the payload.
  return [
    periodId
      ? `CURRENT_PERIOD_METRICS_MISSING_FOR_${periodId}`
      : "CURRENT_PERIOD_METRICS_MISSING_WHILE_MONITORING_CLAIMED",
  ];
}

/**
 * Classify presentation state from resolved trends + optional evidence blockers.
 * Prefer explicit trend points; empty trends alone is not proof of NO_BASELINE
 * when current-period metrics were already folded into `trends`.
 */
export function classifyTrendsPresentationState(trends, { evidenceBlockers = null } = {}) {
  if (Array.isArray(evidenceBlockers) && evidenceBlockers.length) {
    return TRENDS_STATE.EVIDENCE_BLOCKED;
  }
  const pts = normalizeReportTrends(trends);
  if (!pts.length) return TRENDS_STATE.NO_BASELINE;
  if (pts.length === 1) return TRENDS_STATE.BASELINE_ONLY;
  return TRENDS_STATE.MULTI_PERIOD;
}

/**
 * Resolve trends array for presentation: prefer baked/normalized trends[];
 * when empty, synthesize a single baseline point from current-period metrics.
 *
 * @param {object} payload
 * @returns {{ trends: object[], synthesizedFromCurrentPeriod: boolean, evidenceBlockers: string[]|null }}
 */
export function resolveTrendsForPresentation(payload = {}) {
  let trends = normalizeReportTrends(payload.trends);
  let synthesizedFromCurrentPeriod = false;

  if (!trends.length) {
    const point = extractCurrentPeriodTrendPointFromPayload(payload);
    if (point) {
      trends = [point];
      synthesizedFromCurrentPeriod = true;
    }
  } else {
    // Backfill missing fields on the latest point from current-period metrics.
    const em = payload.executiveMetrics || {};
    const last = { ...trends[trends.length - 1] };
    let changed = false;
    if (
      last.considerationRate == null &&
      finiteRate(em.considerationRate?.rate) != null
    ) {
      last.considerationRate = Number(em.considerationRate.rate);
      changed = true;
    }
    if (
      last.scenarioPresenceRate == null &&
      finiteRate(em.scenarioPresence?.rate) != null
    ) {
      last.scenarioPresenceRate = Number(em.scenarioPresence.rate);
      changed = true;
    }
    if (last.propertyRealityCoverage == null) {
      const rc = derivePropertyRealityCoverage(payload.realityGap);
      if (rc != null) {
        last.propertyRealityCoverage = rc;
        changed = true;
      }
    }
    if (changed) {
      trends = [...trends.slice(0, -1), last];
    }
  }

  const evidenceBlockers =
    !trends.length ? resolveTrendsEvidenceBlockers(payload) : null;

  return { trends, synthesizedFromCurrentPeriod, evidenceBlockers };
}

/**
 * Build canonical Trends view model — always 4 KPI slots + chart contract.
 *
 * @param {object} payload owner report payload
 * @returns {object}
 */
export function buildTrendsPresentationV1(payload = {}) {
  const resolved = resolveTrendsForPresentation(payload);
  const trends = resolved.trends;
  const state = classifyTrendsPresentationState(trends, {
    evidenceBlockers: resolved.evidenceBlockers,
  });
  const last = trends.length ? trends[trends.length - 1] : null;
  const prev = trends.length >= 2 ? trends[trends.length - 2] : null;

  const em = payload.executiveMetrics || {};
  const baseline = last
    ? {
        ...last,
        considerationRate:
          last.considerationRate ?? em.considerationRate?.rate ?? null,
        scenarioPresenceRate:
          last.scenarioPresenceRate ?? em.scenarioPresence?.rate ?? null,
        propertyRealityCoverage:
          last.propertyRealityCoverage ??
          derivePropertyRealityCoverage(payload.realityGap),
      }
    : null;

  function deltaPp(cur, prior) {
    if (cur == null || prior == null) return null;
    return round1(cur - prior);
  }

  const scopeOk =
    trends.length < 2 ||
    !trends.some((t) => t && t.comparableScopeNote);

  /** @type {object[]} */
  let kpiSlots;
  let chartMode;
  let statusMessage;
  let chartNote;

  if (state === TRENDS_STATE.EVIDENCE_BLOCKED) {
    const blockers = resolved.evidenceBlockers || [];
    kpiSlots = [
      {
        id: "realityCoverage",
        label: "Reality Coverage",
        value: "—",
        valueRaw: null,
        delta: "Evidence blocked",
      },
      {
        id: "scenarioPresence",
        label: "Scenario Presence",
        value: "—",
        valueRaw: null,
        delta: "Evidence blocked",
      },
      {
        id: "considerationRate",
        label: "Consideration Rate",
        value: "—",
        valueRaw: null,
        delta: "Evidence blocked",
      },
      {
        id: "monitoringOrPeriods",
        label: "Monitoring",
        value: "Blocked",
        valueRaw: null,
        delta: blockers[0] || "Measurement cannot be certified for display",
      },
    ];
    chartMode = "EVIDENCE_BLOCKED_SHELL";
    statusMessage = `Measurement evidence is blocked: ${blockers.join("; ")}. This hotel is not classified as never monitored.`;
    chartNote = statusMessage;
  } else if (state === TRENDS_STATE.NO_BASELINE) {
    kpiSlots = [
      {
        id: "realityCoverage",
        label: "Reality Coverage",
        value: "—",
        valueRaw: null,
        delta: "Baseline required",
      },
      {
        id: "scenarioPresence",
        label: "Scenario Presence",
        value: "—",
        valueRaw: null,
        delta: "Baseline required",
      },
      {
        id: "considerationRate",
        label: "Consideration Rate",
        value: "—",
        valueRaw: null,
        delta: "Baseline required",
      },
      {
        id: "monitoringOrPeriods",
        label: "Monitoring",
        value: "Pending",
        valueRaw: null,
        delta: "A valid certified baseline measurement is required",
      },
    ];
    chartMode = "EMPTY_BASELINE_SHELL";
    statusMessage =
      "Official baseline not yet established. A certified monitoring period is required before Trends can show measured values.";
    chartNote = statusMessage;
  } else if (state === TRENDS_STATE.BASELINE_ONLY) {
    const dateSrc = baseline.baselineDate || baseline.date || "";
    const dateLabel = String(dateSrc).slice(0, 10) || "Baseline";
    const baselineSub = `Baseline · ${dateLabel}`;
    kpiSlots = [
      {
        id: "realityCoverage",
        label: "Reality Coverage",
        value: fmtPct(baseline.propertyRealityCoverage),
        valueRaw: round1(baseline.propertyRealityCoverage),
        delta: baselineSub,
      },
      {
        id: "scenarioPresence",
        label: "Scenario Presence",
        value: fmtPct(baseline.scenarioPresenceRate),
        valueRaw: round1(baseline.scenarioPresenceRate),
        delta: baselineSub,
      },
      {
        id: "considerationRate",
        label: "Consideration Rate",
        value: fmtPct(baseline.considerationRate),
        valueRaw: round1(baseline.considerationRate),
        delta: baselineSub,
      },
      {
        id: "monitoringOrPeriods",
        label: "Monitoring",
        value: "Baseline",
        valueRaw: 1,
        delta:
          "Certified baseline · trend comparison requires another comparable period",
      },
    ];
    chartMode = "BASELINE_ONLY_POINTS";
    statusMessage =
      "Certified baseline period shown. Trend comparison requires another comparable monitoring period — this is not a multi-period change chart.";
    chartNote = statusMessage;
  } else {
    const rcCh = scopeOk
      ? deltaPp(baseline.propertyRealityCoverage, prev.propertyRealityCoverage)
      : null;
    const spCh = scopeOk
      ? deltaPp(baseline.scenarioPresenceRate, prev.scenarioPresenceRate)
      : null;
    const crCh = scopeOk
      ? deltaPp(baseline.considerationRate, prev.considerationRate)
      : null;
    const scopeNote = scopeOk ? null : "Not comparable scope";
    kpiSlots = [
      {
        id: "realityCoverage",
        label: "Reality Coverage",
        value: fmtPct(baseline.propertyRealityCoverage),
        valueRaw: round1(baseline.propertyRealityCoverage),
        delta:
          rcCh == null
            ? scopeNote || "—"
            : `${rcCh > 0 ? "+" : ""}${rcCh} pp`,
      },
      {
        id: "scenarioPresence",
        label: "Scenario Presence",
        value: fmtPct(baseline.scenarioPresenceRate),
        valueRaw: round1(baseline.scenarioPresenceRate),
        delta:
          spCh == null
            ? scopeNote || "—"
            : `${spCh > 0 ? "+" : ""}${spCh} pp`,
      },
      {
        id: "considerationRate",
        label: "Consideration Rate",
        value: fmtPct(baseline.considerationRate),
        valueRaw: round1(baseline.considerationRate),
        delta:
          crCh == null
            ? scopeNote || "—"
            : `${crCh > 0 ? "+" : ""}${crCh} pp`,
      },
      {
        id: "monitoringOrPeriods",
        label: "Periods",
        value: String(trends.length),
        valueRaw: trends.length,
        delta: "Comparable monitoring periods",
      },
    ];
    chartMode = "MULTI_PERIOD_LINE";
    statusMessage = null;
    chartNote = null;
  }

  // Enforce slot contract length
  while (kpiSlots.length < 4) {
    kpiSlots.push({
      id: ADP_TRENDS_KPI_SLOTS_V1[kpiSlots.length]?.id || `slot_${kpiSlots.length}`,
      label: ADP_TRENDS_KPI_SLOTS_V1[kpiSlots.length]?.label || "—",
      value: "—",
      valueRaw: null,
      delta: "—",
    });
  }

  const chartSeries = {
    labels: trends.map((t, idx) => {
      const dateSrc = t.baselineDate || t.date || "";
      const dateLabel = String(dateSrc).slice(0, 10);
      if (state === TRENDS_STATE.BASELINE_ONLY) return `${dateLabel} · Baseline`;
      if (idx === trends.length - 1) return `${dateLabel} · Current`;
      if (idx === 0) return `${dateLabel} · Baseline`;
      if (idx === trends.length - 2) return `${dateLabel} · Prior Run`;
      return dateLabel;
    }),
    realityCoverage: trends.map((t) => round1(t.propertyRealityCoverage)),
    scenarioPresence: trends.map((t) => round1(t.scenarioPresenceRate)),
    considerationRate: trends.map((t) => round1(t.considerationRate)),
  };

  return {
    contract: ADP_TRENDS_PRESENTATION_V1,
    title: TRENDS_SECTION_TITLE,
    subtitle: TRENDS_SECTION_SUBTITLE,
    state,
    kpiSlots: kpiSlots.slice(0, 4),
    kpiSlotCount: 4,
    chartMode,
    chartSeries,
    statusMessage,
    chartNote,
    periodCount: trends.length,
    showLine: state === TRENDS_STATE.MULTI_PERIOD,
    showBaselinePoints: state === TRENDS_STATE.BASELINE_ONLY,
    showEmptyShell:
      state === TRENDS_STATE.NO_BASELINE ||
      state === TRENDS_STATE.EVIDENCE_BLOCKED,
    synthesizedFromCurrentPeriod: resolved.synthesizedFromCurrentPeriod,
    evidenceBlockers: resolved.evidenceBlockers,
    // Raw trends preserved for chart — never invent history
    trends,
  };
}

export function assertTrendsPresentationV1(vm) {
  const failures = [];
  if (!vm || vm.contract !== ADP_TRENDS_PRESENTATION_V1) {
    failures.push("MISSING_TRENDS_CONTRACT");
  }
  if (!Object.values(TRENDS_STATE).includes(vm?.state)) {
    failures.push("INVALID_STATE");
  }
  if (!Array.isArray(vm?.kpiSlots) || vm.kpiSlots.length !== 4) {
    failures.push("KPI_SLOTS_MUST_BE_FOUR");
  }
  if (vm?.state === TRENDS_STATE.BASELINE_ONLY && vm.periodCount !== 1) {
    failures.push("BASELINE_ONLY_PERIOD_COUNT");
  }
  if (vm?.state === TRENDS_STATE.MULTI_PERIOD && vm.periodCount < 2) {
    failures.push("MULTI_PERIOD_COUNT");
  }
  if (vm?.state === TRENDS_STATE.NO_BASELINE && vm.periodCount !== 0) {
    failures.push("NO_BASELINE_PERIOD_COUNT");
  }
  if (vm?.state === TRENDS_STATE.EVIDENCE_BLOCKED && vm.periodCount !== 0) {
    failures.push("EVIDENCE_BLOCKED_MUST_NOT_FABRICATE_PERIODS");
  }
  // Fail-closed: BASELINE_ONLY / MULTI_PERIOD must not blank all three primary rates
  // when the presentation claims measured values exist.
  if (
    vm?.state === TRENDS_STATE.BASELINE_ONLY ||
    vm?.state === TRENDS_STATE.MULTI_PERIOD
  ) {
    const primary = (vm.kpiSlots || []).filter((s) =>
      ["realityCoverage", "scenarioPresence", "considerationRate"].includes(
        s.id
      )
    );
    const allDashes = primary.every(
      (s) => s.value == null || s.value === "—" || s.value === ""
    );
    if (allDashes) {
      failures.push("MEASURED_STATE_WITH_BLANK_PRIMARY_KPIS");
    }
  }
  return { pass: failures.length === 0, failures };
}
