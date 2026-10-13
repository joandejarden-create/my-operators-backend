/**
 * In-memory observability for GDI Jev calls (file report at end of run).
 */

const store = {
  calls: [],
  byDecisionType: {},
};

function ensureType(type) {
  if (!store.byDecisionType[type]) {
    store.byDecisionType[type] = {
      calls: 0,
      errors: 0,
      fallbacks: 0,
      technicalFallbacks: 0,
      policyFallbacks: 0,
      fallbackCauses: {},
      latencies: [],
      costUsd: 0,
      agreements: 0,
      disagreements: 0,
      highConfDisagreements: 0,
    };
  }
  return store.byDecisionType[type];
}

export function recordJevObservation(row) {
  store.calls.push(row);
  const bucket = ensureType(row.decisionType || "UNKNOWN");
  bucket.calls += 1;
  if (row.error) bucket.errors += 1;
  if (row.fallbackUsed) bucket.fallbacks += 1;
  if (row.technicalFallback) bucket.technicalFallbacks += 1;
  if (row.policyFallback) bucket.policyFallbacks += 1;
  if (row.fallbackCause) {
    bucket.fallbackCauses[row.fallbackCause] =
      (bucket.fallbackCauses[row.fallbackCause] || 0) + 1;
  }
  if (typeof row.latencyMs === "number") bucket.latencies.push(row.latencyMs);
  if (typeof row.costUsd === "number") bucket.costUsd += row.costUsd;
  if (row.matchExisting === true) bucket.agreements += 1;
  if (row.matchExisting === false) {
    bucket.disagreements += 1;
    if ((row.confidence || 0) >= 0.7) bucket.highConfDisagreements += 1;
  }
}

export function resetJevObservability() {
  store.calls = [];
  store.byDecisionType = {};
}

function percentile(sorted, p) {
  if (!sorted.length) return null;
  const i = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil((p / 100) * sorted.length) - 1)
  );
  return sorted[i];
}

export function summarizeJevObservability() {
  const latencies = store.calls
    .map((c) => c.latencyMs)
    .filter((n) => typeof n === "number")
    .sort((a, b) => a - b);
  const byType = {};
  for (const [k, v] of Object.entries(store.byDecisionType)) {
    const L = [...v.latencies].sort((a, b) => a - b);
    byType[k] = {
      calls: v.calls,
      errors: v.errors,
      fallbacks: v.fallbacks,
      technicalFallbacks: v.technicalFallbacks,
      policyFallbacks: v.policyFallbacks,
      fallbackCauses: v.fallbackCauses,
      agreements: v.agreements,
      disagreements: v.disagreements,
      highConfDisagreements: v.highConfDisagreements,
      costUsd: Math.round(v.costUsd * 1e6) / 1e6,
      p50: percentile(L, 50),
      p95: percentile(L, 95),
      p99: percentile(L, 99),
    };
  }
  const causeTotals = {};
  for (const c of store.calls) {
    if (c.fallbackCause) {
      causeTotals[c.fallbackCause] = (causeTotals[c.fallbackCause] || 0) + 1;
    }
  }
  return {
    totalCalls: store.calls.length,
    errors: store.calls.filter((c) => c.error).length,
    fallbacks: store.calls.filter((c) => c.fallbackUsed).length,
    technicalFallbacks: store.calls.filter((c) => c.technicalFallback).length,
    policyFallbacks: store.calls.filter((c) => c.policyFallback).length,
    fallbackCauses: causeTotals,
    agreements: store.calls.filter((c) => c.matchExisting === true).length,
    disagreements: store.calls.filter((c) => c.matchExisting === false).length,
    highConfDisagreements: store.calls.filter(
      (c) => c.matchExisting === false && (c.confidence || 0) >= 0.7
    ).length,
    costUsd:
      Math.round(
        store.calls.reduce((s, c) => s + (c.costUsd || 0), 0) * 1e6
      ) / 1e6,
    latency: {
      p50: percentile(latencies, 50),
      p95: percentile(latencies, 95),
      p99: percentile(latencies, 99),
    },
    byDecisionType: byType,
    samples: store.calls.slice(0, 200),
  };
}

export function getJevObservations() {
  return store.calls.slice();
}
