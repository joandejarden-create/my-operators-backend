/**
 * Packet 2.8C-1 — Native research cost ledger (model + search + docs).
 * Never claim $0 if APIs incurred usage.
 */

export const NATIVE_COST_LEDGER_VERSION = "native-cost-ledger-v1";

/** Rough list prices — override via env when measured exactly. */
const DEFAULT_RATES = Object.freeze({
  serpapi_per_search_usd: Number(process.env.SERPAPI_USD_PER_SEARCH || 0.01),
  openai_input_per_1m_usd: Number(process.env.OPENAI_INPUT_USD_PER_1M || 0.15), // gpt-4o-mini-ish default
  openai_output_per_1m_usd: Number(process.env.OPENAI_OUTPUT_USD_PER_1M || 0.6),
  fetch_page_usd: 0,
  pdf_parse_usd: 0,
});

export function createNativeCostLedger(meta = {}) {
  return {
    version: NATIVE_COST_LEDGER_VERSION,
    meta,
    rates: { ...DEFAULT_RATES },
    entries: [],
    totals: {
      model_api_cost_usd: 0,
      search_api_cost_usd: 0,
      document_api_cost_usd: 0,
      other_api_cost_usd: 0,
      total_native_cost_usd: 0,
      model_calls: 0,
      search_calls: 0,
      document_retrievals: 0,
      input_tokens: 0,
      output_tokens: 0,
    },
  };
}

export function recordSearchCost(ledger, { queries = 1, note = null } = {}) {
  const n = Math.max(0, Number(queries) || 0);
  const usd = n * ledger.rates.serpapi_per_search_usd;
  ledger.entries.push({ kind: "search", queries: n, usd, note });
  ledger.totals.search_calls += n;
  ledger.totals.search_api_cost_usd += usd;
  recompute(ledger);
  return usd;
}

export function recordModelCost(ledger, { input_tokens = 0, output_tokens = 0, model = null, note = null } = {}) {
  const inT = Math.max(0, Number(input_tokens) || 0);
  const outT = Math.max(0, Number(output_tokens) || 0);
  const usd =
    (inT / 1e6) * ledger.rates.openai_input_per_1m_usd +
    (outT / 1e6) * ledger.rates.openai_output_per_1m_usd;
  ledger.entries.push({ kind: "model", input_tokens: inT, output_tokens: outT, usd, model, note });
  ledger.totals.model_calls += 1;
  ledger.totals.input_tokens += inT;
  ledger.totals.output_tokens += outT;
  ledger.totals.model_api_cost_usd += usd;
  recompute(ledger);
  return usd;
}

export function recordDocumentRetrieval(ledger, { count = 1, note = null } = {}) {
  const n = Math.max(0, Number(count) || 0);
  ledger.entries.push({ kind: "document", count: n, usd: 0, note });
  ledger.totals.document_retrievals += n;
  recompute(ledger);
}

function recompute(ledger) {
  ledger.totals.total_native_cost_usd =
    ledger.totals.model_api_cost_usd +
    ledger.totals.search_api_cost_usd +
    ledger.totals.document_api_cost_usd +
    ledger.totals.other_api_cost_usd;
}

export function summarizeLedger(ledger) {
  return {
    ...ledger.totals,
    entry_count: ledger.entries.length,
    version: ledger.version,
  };
}
