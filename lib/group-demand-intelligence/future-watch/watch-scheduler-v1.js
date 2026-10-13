/**
 * Future-watch scheduler — due query, bounded batch, idempotency, cost guards.
 * Generic — not hotel-specific cron.
 */

import { SCHEDULER_DEFAULTS, WATCH_STATUS } from "./constants.js";
import { isGdiFutureWatchReadyForResearch } from "./watch-readiness-v1.js";

/**
 * Query watches that are due for research.
 */
export function queryDueWatches(watches = [], opts = {}) {
  const now = opts.now || new Date();
  const due = [];
  for (const w of watches) {
    if (w.watchStatus === WATCH_STATUS.STOPPED || w.watchStatus === WATCH_STATUS.PROMOTED) {
      continue;
    }
    const check = isGdiFutureWatchReadyForResearch(w, {
      now,
      sourceMateriallyChanged: opts.sourceChanges?.[w.watchId] === true,
      triggerDetected: opts.triggersDetected?.[w.watchId] === true,
      manualOverride: opts.manualOverrides?.[w.watchId] === true,
    });
    if (check.ready) {
      due.push({ watch: w, readiness: check });
    }
  }
  // Stable order: nextResearchDate asc, then watchId
  due.sort((a, b) => {
    const da = String(a.watch.nextResearchDate || "");
    const db = String(b.watch.nextResearchDate || "");
    if (da !== db) return da < db ? -1 : 1;
    return String(a.watch.watchId).localeCompare(String(b.watch.watchId));
  });
  return due;
}

/**
 * Build a bounded scheduled batch with idempotency keys.
 */
export function buildWatchResearchBatch(dueItems = [], opts = {}) {
  const cfg = { ...SCHEDULER_DEFAULTS, ...opts.config };
  const alreadyProcessed = new Set(opts.processedIds || []);
  const batchId =
    opts.batchId || `gdi_fw_batch_${new Date().toISOString().replace(/[:.]/g, "-")}`;

  const selected = [];
  for (const item of dueItems) {
    if (selected.length >= cfg.maxWatchesPerBatch) break;
    const id = item.watch.watchId;
    const idempotencyKey = `${batchId}:${id}:${item.watch.nextResearchDate || "na"}`;
    if (alreadyProcessed.has(id) || alreadyProcessed.has(idempotencyKey)) continue;
    selected.push({
      ...item,
      batchId,
      idempotencyKey,
      maxJevActions: cfg.maxJevActionsPerWatch,
      maxQueries: cfg.maxQueriesPerAction,
      maxFetches: cfg.maxFetchesPerAction,
    });
  }

  return {
    batchId,
    config: cfg,
    selected,
    skippedAlreadyProcessed: dueItems.length - selected.length,
    estimatedMaxJevCalls: selected.length * cfg.maxJevActionsPerWatch,
    costGuardUsd: cfg.maxEstimatedCostUsd,
    maxProviderErrors: cfg.maxProviderErrors,
  };
}

/**
 * Cost ledger for a batch run.
 */
export function createBatchCostLedger(batchId) {
  return {
    batchId,
    jevCalls: 0,
    queries: 0,
    fetches: 0,
    estimatedCostUsd: 0,
    resolvedBlockers: 0,
    promotions: 0,
    rejections: 0,
    futureWatchesRetained: 0,
    providerErrors: 0,
    stopReason: null,
  };
}

export function updateBatchCost(ledger, delta = {}) {
  ledger.jevCalls += Number(delta.jevCalls) || 0;
  ledger.queries += Number(delta.queries) || 0;
  ledger.fetches += Number(delta.fetches) || 0;
  // Rough estimate: $0.02/query + $0.005/fetch + $0.01/jev
  ledger.estimatedCostUsd = Number(
    (
      ledger.queries * 0.02 +
      ledger.fetches * 0.005 +
      ledger.jevCalls * 0.01
    ).toFixed(4)
  );
  if (delta.resolved) ledger.resolvedBlockers += 1;
  if (delta.promoted) ledger.promotions += 1;
  if (delta.rejected) ledger.rejections += 1;
  if (delta.retainedWatch) ledger.futureWatchesRetained += 1;
  if (delta.providerError) ledger.providerErrors += 1;
  return ledger;
}

export function shouldStopBatch(ledger, config = SCHEDULER_DEFAULTS) {
  if (ledger.providerErrors >= (config.maxProviderErrors || 5)) {
    return { stop: true, reason: "PROVIDER_ERROR_THRESHOLD" };
  }
  if (ledger.estimatedCostUsd >= (config.maxEstimatedCostUsd || 25)) {
    return { stop: true, reason: "COST_GUARD" };
  }
  return { stop: false, reason: null };
}

export function costPerResolvedBlocker(ledger) {
  if (!ledger.resolvedBlockers) return null;
  return Number((ledger.estimatedCostUsd / ledger.resolvedBlockers).toFixed(4));
}

export function costPerReadyOpportunity(ledger) {
  if (!ledger.promotions) return null;
  return Number((ledger.estimatedCostUsd / ledger.promotions).toFixed(4));
}
