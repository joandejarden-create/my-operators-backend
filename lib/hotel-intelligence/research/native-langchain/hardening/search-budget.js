/**
 * Packet 2.8C-2 — Search budget + query economy classification.
 */

export const SEARCH_BUDGET_VERSION = "search-budget-v1";

const BUDGETS = Object.freeze({
  L3: { max_queries: 3, max_fetches: 2, max_upgrade_queries: 2 },
  L4: { max_queries: 4, max_fetches: 3, max_upgrade_queries: 3 },
  OWNERSHIP_MX_L4: { max_queries: 5, max_fetches: 4, max_upgrade_queries: 3 },
});

export function resolveSearchBudget({ level = 3, domain = "", jurisdiction = "" } = {}) {
  if (String(jurisdiction).toUpperCase() === "MX" && /OWNER|PROPCO|TRANSACT/i.test(domain) && level >= 4) {
    return { ...BUDGETS.OWNERSHIP_MX_L4, key: "OWNERSHIP_MX_L4" };
  }
  return level >= 4 ? { ...BUDGETS.L4, key: "L4" } : { ...BUDGETS.L3, key: "L3" };
}

export function classifyQueryOutcome({ query, results = [], accepted_claim_urls = [], fetched_urls = [] } = {}) {
  const urls = results.map((r) => r.url).filter(Boolean);
  const producedAccepted = urls.some((u) => accepted_claim_urls.includes(u));
  const producedFetch = urls.some((u) => fetched_urls.includes(u));
  if (producedAccepted) return { query, class: "PRODUCTIVE", reason: "led_to_accepted_claim" };
  if (producedFetch) return { query, class: "PRODUCTIVE", reason: "led_to_document_fetch" };
  if (!urls.length) return { query, class: "FAILED", reason: "no_results" };
  if (results.every((r) => /booking|tripadvisor|expedia|hotels\.com/i.test(r.url || ""))) {
    return { query, class: "LOW_VALUE", reason: "ota_only" };
  }
  return { query, class: "LOW_VALUE", reason: "no_downstream_use" };
}

export function summarizeQueryEconomy(rows = []) {
  const counts = { PRODUCTIVE: 0, DUPLICATIVE: 0, LOW_VALUE: 0, UNNECESSARY: 0, FAILED: 0 };
  for (const r of rows) {
    const c = r.class || "LOW_VALUE";
    counts[c] = (counts[c] || 0) + 1;
  }
  return { version: SEARCH_BUDGET_VERSION, total: rows.length, counts, rows };
}

export function dedupeQueries(queries = []) {
  const seen = new Set();
  const out = [];
  const duplicative = [];
  for (const q of queries) {
    const key = String(q || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    if (!key) continue;
    if (seen.has(key)) {
      duplicative.push(q);
      continue;
    }
    // Near-duplicate: same first 5 tokens
    const tokens = key.split(/\s+/).slice(0, 5).join(" ");
    if ([...seen].some((s) => s.split(/\s+/).slice(0, 5).join(" ") === tokens)) {
      duplicative.push(q);
      continue;
    }
    seen.add(key);
    out.push(q);
  }
  return { queries: out, duplicative };
}
