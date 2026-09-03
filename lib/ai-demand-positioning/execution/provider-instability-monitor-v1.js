/**
 * Provider instability operational monitoring — learn systematic failure patterns
 * (esp. Gemini 503 / timeout clusters) across ADP periods.
 *
 * Defect class linkage: UNRECOVERED_PROVIDER_COVERAGE_GAP
 */

import { classifyProviderFailure, FAILURE_CLASSES } from "../measurement-assurance/provider-coverage-recovery-v1.js";
import { isComparableObservation } from "../metrics/grain-governance.js";

export const PROVIDER_INSTABILITY_MONITOR_VERSION = "adp_provider_instability_monitor_v1";
export const DEFECT_UNRECOVERED_PROVIDER_COVERAGE_GAP = "UNRECOVERED_PROVIDER_COVERAGE_GAP";

/**
 * Summarize failure patterns for one period (or a list of observations).
 */
export function analyzeProviderInstability({ period, scenarios = [], focusProvider = null }) {
  const obs = period?.observations || [];
  const scenarioById = Object.fromEntries((scenarios || []).map((s) => [s.scenarioId, s]));
  const byProvider = Object.create(null);

  for (const o of obs) {
    const provider = o.provider || "unknown";
    if (focusProvider && provider !== focusProvider) continue;
    if (!byProvider[provider]) {
      byProvider[provider] = {
        provider,
        scheduled: 0,
        success: 0,
        failed: 0,
        byFailureClass: Object.create(null),
        byTerritory: Object.create(null),
        byHourUtc: Object.create(null),
        recoveryAttempted: 0,
        recovered: 0,
        residualMissing: 0,
      };
    }
    const b = byProvider[provider];
    b.scheduled += 1;
    if (isComparableObservation(o)) {
      b.success += 1;
      if (o.finalGovernedStatus === "RECOVERED_COMPARABLE" || o.recoveredAt) b.recovered += 1;
      continue;
    }
    b.failed += 1;
    const fc = classifyProviderFailure(o);
    b.byFailureClass[fc] = (b.byFailureClass[fc] || 0) + 1;
    const intent = scenarioById[o.scenarioId]?.intent || "unknown";
    b.byTerritory[intent] = (b.byTerritory[intent] || 0) + 1;
    const ts = o.timestamp || o.originalFailureSnapshot?.timestamp || period?.executionDate;
    if (ts) {
      const hour = new Date(ts).getUTCHours();
      b.byHourUtc[hour] = (b.byHourUtc[hour] || 0) + 1;
    }
    if (o.recoveryAttempted || o.recoveryAttempts?.length) {
      b.recoveryAttempted += 1;
      if (o.finalGovernedStatus === "RESIDUAL_MISSING_AFTER_RECOVERY") b.residualMissing += 1;
    } else {
      b.residualMissing += 1;
    }
  }

  const providers = Object.values(byProvider).map((b) => {
    const failRate = b.scheduled ? b.failed / b.scheduled : 0;
    const rate503 = b.scheduled ? (b.byFailureClass[FAILURE_CLASSES.PROVIDER_503] || 0) / b.scheduled : 0;
    const timeoutRate = b.scheduled ? (b.byFailureClass[FAILURE_CLASSES.TIMEOUT] || 0) / b.scheduled : 0;
    const flags = [];
    if (rate503 >= 0.1) flags.push("ELEVATED_503_RATE");
    if (timeoutRate >= 0.05) flags.push("REPEATED_TIMEOUT_CLUSTER");
    if (failRate >= 0.15) flags.push("ELEVATED_PROVIDER_FAILURE_RATE");
    const maxTerritory = Math.max(0, ...Object.values(b.byTerritory));
    if (maxTerritory >= 3) flags.push("TERRITORY_CLUSTERING");
    const hourEntries = Object.entries(b.byHourUtc);
    if (hourEntries.length) {
      const peak = hourEntries.sort((a, c) => c[1] - a[1])[0];
      if (peak && peak[1] >= 5) flags.push("TIME_OF_DAY_CLUSTERING");
    }
    return {
      ...b,
      failRate: Math.round(failRate * 1000) / 10,
      rate503: Math.round(rate503 * 1000) / 10,
      timeoutRate: Math.round(timeoutRate * 1000) / 10,
      operationalFlags: flags,
      learningHint:
        flags.includes("ELEVATED_503_RATE") || flags.includes("REPEATED_TIMEOUT_CLUSTER")
          ? "Consider lower concurrency / staggered Gemini batches / longer inter-call delay"
          : null,
    };
  });

  return {
    version: PROVIDER_INSTABILITY_MONITOR_VERSION,
    defectClass: DEFECT_UNRECOVERED_PROVIDER_COVERAGE_GAP,
    periodId: period?.periodId || null,
    propertyId: period?.propertyId || null,
    providers,
    portfolioFlags: [...new Set(providers.flatMap((p) => p.operationalFlags))],
  };
}
