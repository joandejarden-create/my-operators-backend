/**
 * Normalize Apify run payloads + Dealality outcome counters into ledger rows.
 */

import {
  APIFY_AUTH_METHODS,
  APIFY_COST_SOURCE,
  APIFY_USAGE_VERSION,
  APIFY_USE_CASES,
} from "./constants.js";

function numOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function intOrNull(v) {
  const n = numOrNull(v);
  if (n == null) return null;
  return Math.trunc(n);
}

function ratio(num, den) {
  const a = numOrNull(num);
  const b = numOrNull(den);
  if (a == null || b == null || b === 0) return null;
  return Number((a / b).toFixed(8));
}

function assertUseCase(useCase) {
  const key = String(useCase || "").trim();
  if (!Object.prototype.hasOwnProperty.call(APIFY_USE_CASES, key)) {
    throw new Error(
      `Invalid Apify use case "${useCase}". Expected one of: ${Object.keys(APIFY_USE_CASES).join(", ")}`
    );
  }
  return key;
}

/**
 * Strip secrets from any accidental option bag.
 * @param {object} [obj]
 */
export function scrubSecrets(obj) {
  if (!obj || typeof obj !== "object") return obj;
  const out = { ...obj };
  for (const k of Object.keys(out)) {
    if (/token|password|authorization|api[_-]?key|secret/i.test(k)) {
      delete out[k];
    }
  }
  return out;
}

/**
 * Derive cost fields from an Apify GET /v2/actor-runs/{id} `data` object.
 * Prefer usageTotalUsd; never invent when unavailable.
 *
 * @param {object} runData
 */
export function extractApifyRunCost(runData) {
  const data = runData?.data && typeof runData.data === "object" ? runData.data : runData;
  if (!data || typeof data !== "object") {
    return {
      apify_run_cost_usd: null,
      cost_source: APIFY_COST_SOURCE.UNKNOWN,
      charged_event_counts: null,
      pricing_model: null,
      compute_units: null,
      event_usage: null,
    };
  }

  const usageTotal = numOrNull(data.usageTotalUsd);
  const charged = data.accountedChargedEventCounts || data.chargedEventCounts || null;
  const pricingModel = data.pricingInfo?.pricingModel || null;
  const computeUnits = numOrNull(data.stats?.computeUnits);

  return {
    apify_run_cost_usd: usageTotal,
    cost_source:
      usageTotal != null
        ? APIFY_COST_SOURCE.APIFY_USAGE_TOTAL_USD
        : APIFY_COST_SOURCE.UNKNOWN,
    charged_event_counts: charged,
    pricing_model: pricingModel,
    compute_units: computeUnits,
    event_usage: data.eventUsage || null,
  };
}

/**
 * Map Apify run API payload → partial ledger fields (identity + timing + cost).
 * @param {object} runData Apify run `data` or wrapped `{ data }`
 */
export function fromApifyRunPayload(runData) {
  const data = runData?.data && typeof runData.data === "object" ? runData.data : runData;
  if (!data || typeof data !== "object") {
    throw new Error("fromApifyRunPayload: missing run data");
  }
  const cost = extractApifyRunCost(data);
  const datasetId =
    data.defaultDatasetId ||
    data.storageIds?.datasets?.default ||
    null;
  const itemCount =
    intOrNull(data.datasetItemCount) ??
    intOrNull(data.stats?.datasetItemCount) ??
    null;

  return {
    actor_id: data.actId || data.actorId || null,
    actor_name: data.actorName || null,
    run_id: data.id || data.runId || null,
    started_at: data.startedAt || null,
    finished_at: data.finishedAt || null,
    status: data.status || null,
    dataset_id: datasetId,
    records_returned:
      itemCount ??
      (cost.charged_event_counts?.["result-scraped"] != null
        ? intOrNull(cost.charged_event_counts["result-scraped"])
        : null),
    apify_run_cost_usd: cost.apify_run_cost_usd,
    cost_source: cost.cost_source,
    charged_event_counts: cost.charged_event_counts,
    pricing_model: cost.pricing_model,
    compute_units: cost.compute_units,
    event_usage: cost.event_usage,
    console_url: data.consoleUrl || null,
    apify_meta_origin: data.meta?.origin || null,
  };
}

/**
 * Build a complete ledger row. Does not write.
 *
 * @param {object} input
 * @param {string} input.use_case
 * @param {string} [input.actor_id]
 * @param {string} [input.actor_name]
 * @param {string} [input.run_id]
 * @param {string} [input.started_at]
 * @param {string} [input.finished_at]
 * @param {number|null} [input.records_requested]
 * @param {number|null} [input.records_returned]
 * @param {number|null} [input.apify_run_cost_usd]
 * @param {number|null} [input.successful_matches]
 * @param {number|null} [input.successful_enrichments]
 * @param {number|null} [input.verified_enrichments]
 * @param {string} [input.auth_method]
 * @param {object} [input.apify_run] raw Apify run payload to merge
 */
export function buildApifyUsageRecord(input = {}) {
  const useCase = assertUseCase(input.use_case);
  const fromRun = input.apify_run ? fromApifyRunPayload(input.apify_run) : {};

  const recordsReturned =
    intOrNull(input.records_returned) ?? intOrNull(fromRun.records_returned);
  const costUsd =
    numOrNull(input.apify_run_cost_usd) ?? numOrNull(fromRun.apify_run_cost_usd);

  const successfulMatches = intOrNull(input.successful_matches);
  const successfulEnrichments = intOrNull(input.successful_enrichments);
  const verifiedEnrichments = intOrNull(input.verified_enrichments);

  const costSource =
    input.cost_source ||
    fromRun.cost_source ||
    (costUsd != null ? APIFY_COST_SOURCE.MANUAL : APIFY_COST_SOURCE.UNKNOWN);

  const row = {
    schema_version: APIFY_USAGE_VERSION,
    recorded_at: new Date().toISOString(),
    production_writes: false,
    authoritative_hotel_data: false,

    actor_id: input.actor_id || fromRun.actor_id || null,
    actor_name: input.actor_name || fromRun.actor_name || null,
    dealality_use_case: useCase,
    run_id: input.run_id || fromRun.run_id || null,
    started_at: input.started_at || fromRun.started_at || null,
    finished_at: input.finished_at || fromRun.finished_at || null,

    records_requested: intOrNull(input.records_requested),
    records_returned: recordsReturned,

    apify_run_cost_usd: costUsd,
    cost_source: costSource,

    successful_matches: successfulMatches,
    successful_enrichments: successfulEnrichments,
    verified_enrichments: verifiedEnrichments,

    cost_per_returned_record: ratio(costUsd, recordsReturned),
    cost_per_successful_enrichment: ratio(costUsd, successfulEnrichments),
    cost_per_verified_enrichment: ratio(costUsd, verifiedEnrichments),

    auth_method: input.auth_method || APIFY_AUTH_METHODS.UNKNOWN,
    status: input.status || fromRun.status || null,
    dataset_id: input.dataset_id || fromRun.dataset_id || null,
    charged_event_counts: fromRun.charged_event_counts || input.charged_event_counts || null,
    pricing_model: fromRun.pricing_model || input.pricing_model || null,
    compute_units: fromRun.compute_units ?? input.compute_units ?? null,
    notes: input.notes || null,
    label: input.label || null,
    console_url: fromRun.console_url || input.console_url || null,
  };

  return scrubSecrets(row);
}

/**
 * Summarize ledger rows for dashboards / reports.
 * @param {object[]} rows
 */
export function summarizeApifyUsage(rows = []) {
  const list = Array.isArray(rows) ? rows : [];
  const byUseCase = {};
  let totalCost = 0;
  let costKnown = 0;
  let runs = 0;
  let returned = 0;
  let enrichments = 0;
  let verified = 0;

  for (const r of list) {
    runs += 1;
    const uc = r.dealality_use_case || "UNKNOWN";
    if (!byUseCase[uc]) {
      byUseCase[uc] = { runs: 0, apify_run_cost_usd: 0, records_returned: 0 };
    }
    byUseCase[uc].runs += 1;
    const c = numOrNull(r.apify_run_cost_usd);
    if (c != null) {
      totalCost += c;
      costKnown += 1;
      byUseCase[uc].apify_run_cost_usd += c;
    }
    const ret = intOrNull(r.records_returned) || 0;
    returned += ret;
    byUseCase[uc].records_returned += ret;
    enrichments += intOrNull(r.successful_enrichments) || 0;
    verified += intOrNull(r.verified_enrichments) || 0;
  }

  return {
    schema_version: APIFY_USAGE_VERSION,
    runs,
    runs_with_known_cost: costKnown,
    total_apify_run_cost_usd: Number(totalCost.toFixed(6)),
    records_returned: returned,
    successful_enrichments: enrichments,
    verified_enrichments: verified,
    cost_per_returned_record: ratio(totalCost, returned),
    cost_per_successful_enrichment: ratio(totalCost, enrichments),
    cost_per_verified_enrichment: ratio(totalCost, verified),
    by_use_case: byUseCase,
  };
}
