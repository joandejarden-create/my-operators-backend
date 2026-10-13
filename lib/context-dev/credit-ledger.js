/**
 * Context.dev credit ledger for bounded Contact Intelligence experiments.
 * Uses documented endpoint costs. Does not invent free credits.
 */

export const CONTEXT_DEV_CREDIT_COSTS = Object.freeze({
  search_per_10_results: 1,
  scrape_markdown: 1,
  scrape_markdown_with_actions: 2,
  extract: 10,
  brand_retrieve: 10,
});

export function createContextDevCreditLedger({
  budgetCredits = 200,
  alreadySpent = 0,
  label = "contact-person-email",
} = {}) {
  let spent = Number(alreadySpent) || 0;
  const entries = [];

  function remaining() {
    return Math.max(0, budgetCredits - spent);
  }

  function canAfford(cost) {
    return remaining() >= Number(cost);
  }

  function charge(kind, cost, meta = {}) {
    const c = Number(cost);
    if (!Number.isFinite(c)) {
      throw new Error(`invalid_credit_cost:${kind}`);
    }
    // Negative cost = refund (e.g. Context.dev validation errors bill 0).
    if (c < 0) {
      spent = Math.max(0, spent + c);
      const entry = {
        at: new Date().toISOString(),
        kind,
        cost: c,
        spent,
        remaining: remaining(),
        ...meta,
      };
      entries.push(entry);
      return { ok: true, blocked: false, entry, spent, remaining: remaining() };
    }
    if (!canAfford(c)) {
      return {
        ok: false,
        blocked: true,
        reason: "CONTEXT_DEV_CREDIT_BUDGET_EXCEEDED",
        spent,
        remaining: remaining(),
        budget: budgetCredits,
      };
    }
    spent += c;
    const entry = {
      at: new Date().toISOString(),
      kind,
      cost: c,
      spent,
      remaining: remaining(),
      ...meta,
    };
    entries.push(entry);
    return { ok: true, blocked: false, entry, spent, remaining: remaining() };
  }

  function estimateSearchCost(numResults = 10) {
    return Math.max(1, Math.ceil(Number(numResults || 10) / 10)) * CONTEXT_DEV_CREDIT_COSTS.search_per_10_results;
  }

  return {
    label,
    budgetCredits,
    get spent() {
      return spent;
    },
    remaining,
    canAfford,
    charge,
    estimateSearchCost,
    snapshot() {
      return {
        label,
        budget_credits: budgetCredits,
        spent_credits: spent,
        remaining_credits: remaining(),
        entries: entries.slice(),
        cost_table: { ...CONTEXT_DEV_CREDIT_COSTS },
      };
    },
  };
}
