/**
 * Classify durable research-operation journal entries for measurement/reporting.
 *
 * Journal event kinds are lifecycle events (reservation, dispatch, result_persisted,
 * settlement, skip, completion) — NOT provider operation types.
 *
 * Provider operation type must come from:
 * - op.op / op.provider_operation / op.operation
 * - work_key prefix (context_dev:search:… / context_dev:scrape:…)
 */
export const JOURNAL_LIFECYCLE_KINDS = Object.freeze([
  "reservation",
  "dispatch",
  "result_persisted",
  "settlement",
  "skip",
  "completion",
]);

/**
 * @param {object} op
 * @returns {"search"|"scrape"|"other"|null}
 */
export function classifyJournalProviderOperation(op = {}) {
  const explicit = String(
    op.op || op.provider_operation || op.operation || op.operation_type || ""
  )
    .trim()
    .toLowerCase();
  if (explicit === "search" || explicit === "serp" || explicit === "web_search") return "search";
  if (
    explicit === "scrape" ||
    explicit === "fetch" ||
    explicit === "read" ||
    explicit === "scrape_markdown"
  ) {
    return "scrape";
  }

  const wk = String(op.work_key || op.workKey || "").toLowerCase();
  if (/:search:/.test(wk) || /^search:/.test(wk) || /context_dev:search/.test(wk)) return "search";
  if (/:scrape:/.test(wk) || /^scrape:/.test(wk) || /context_dev:scrape/.test(wk)) return "scrape";

  // Never infer from lifecycle kind — "search_skipped_resume" etc. must not count as search.
  return null;
}

/**
 * @param {object} op
 * @returns {boolean}
 */
export function isJournalProviderSearch(op) {
  return classifyJournalProviderOperation(op) === "search";
}

/**
 * @param {object} op
 * @returns {boolean}
 */
export function isJournalProviderScrape(op) {
  return classifyJournalProviderOperation(op) === "scrape";
}

/**
 * True when this journal row represents a newly dispatched provider call
 * (dispatch or settlement of a search/scrape), not a skip/replay marker alone.
 * @param {object} op
 */
export function isJournalProviderDispatchEvent(op = {}) {
  const providerOp = classifyJournalProviderOperation(op);
  if (!providerOp) return false;
  const kind = String(op.kind || op.operation_kind || op.type || "").toLowerCase();
  if (kind === "skip") return false;
  return kind === "dispatch" || kind === "settlement" || kind === "result_persisted";
}

/**
 * Summarize a journal slice for canary/measurement harnesses.
 * @param {object[]} ops
 */
export function summarizeJournalSliceForMeasurement(ops = []) {
  const searches = [];
  const scrapes = [];
  const dispatches = [];
  const settlements = [];
  const skips = [];
  let settledCredits = 0;
  const seenWorkKeys = new Set();

  for (const op of ops) {
    const kind = String(op.kind || "").toLowerCase();
    const providerOp = classifyJournalProviderOperation(op);
    const wk = op.work_key || op.workKey || null;
    const credits = Number(
      op.settled_credits ??
        op.provider_confirmed_credits ??
        op.credits ??
        op.key_metadata?.credits_consumed ??
        0
    );

    if (kind === "skip") {
      skips.push({ work_key: wk, url: op.url || null, provider_op: providerOp });
      continue;
    }
    if (kind === "dispatch" && providerOp) {
      dispatches.push({ work_key: wk, provider_op: providerOp, query: op.query || null });
    }
    if (kind === "settlement" && providerOp) {
      settlements.push({
        work_key: wk,
        provider_op: providerOp,
        credits,
        query: op.query || null,
      });
      if (wk && !seenWorkKeys.has(`settle:${wk}`)) {
        seenWorkKeys.add(`settle:${wk}`);
        settledCredits += credits;
      }
    }

    if (providerOp === "search" && (kind === "settlement" || kind === "result_persisted")) {
      if (wk && seenWorkKeys.has(`search:${wk}`)) continue;
      if (wk) seenWorkKeys.add(`search:${wk}`);
      const durable = op.durable_result || {};
      const data = durable.data || durable;
      const results = data.results || data.items || [];
      searches.push({
        work_key: wk,
        query: op.query || data.query || null,
        credits,
        result_count: Array.isArray(results) ? results.length : 0,
        empty_results: !Array.isArray(results) || results.length === 0,
        status: op.status || null,
      });
    }
    if (providerOp === "scrape" && (kind === "settlement" || kind === "result_persisted")) {
      if (wk && seenWorkKeys.has(`scrape:${wk}`)) continue;
      if (wk) seenWorkKeys.add(`scrape:${wk}`);
      scrapes.push({
        work_key: wk,
        url: op.url || null,
        credits,
        status: op.status || null,
      });
    }
  }

  return {
    new_searches: searches,
    new_scrapes: scrapes,
    new_dispatches: dispatches,
    new_settlements: settlements,
    skips,
    new_settled_credits: settledCredits,
    new_provider_dispatch_count: new Set(dispatches.map((d) => d.work_key).filter(Boolean)).size,
  };
}
