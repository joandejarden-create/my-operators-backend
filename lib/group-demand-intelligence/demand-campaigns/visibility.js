/**
 * Demand Generator / Campaign visibility — SEPARATE from customer-ready.
 *
 * Generator visibility answers:
 *   "Is this a real, relevant demand campaign worth researching?"
 *
 * Opportunity visibility answers:
 *   "Is this specific account/group sufficiently qualified to pursue?"
 *
 * Do NOT call isGdiCustomerOpportunityReady for generator visibility.
 */

export const GENERATOR_VISIBILITY = Object.freeze({
  VISIBLE_CURRENT: "VISIBLE_CURRENT",
  HIDDEN_STALE: "HIDDEN_STALE",
  HIDDEN_WRONG_MARKET: "HIDDEN_WRONG_MARKET",
  HIDDEN_INACTIVE: "HIDDEN_INACTIVE",
  HIDDEN_ARCHIVED: "HIDDEN_ARCHIVED",
  REJECTED: "REJECTED",
});

/**
 * @param {object} generator — demand campaign / generator record
 * @param {object} [opts]
 * @param {string} [opts.nowDate] YYYY-MM-DD
 * @param {string[]} [opts.marketHints] destination tokens
 */
export function isGdiDemandGeneratorVisible(generator = {}, opts = {}) {
  const reasons = [];
  if (!generator || typeof generator !== "object") {
    return { ok: false, state: GENERATOR_VISIBILITY.REJECTED, reasons: ["missing_generator"] };
  }
  if (generator.archived === true || generator.status === "ARCHIVED") {
    return { ok: false, state: GENERATOR_VISIBILITY.HIDDEN_ARCHIVED, reasons: ["archived"] };
  }
  if (generator.superseded === true) {
    return { ok: false, state: GENERATOR_VISIBILITY.HIDDEN_STALE, reasons: ["superseded"] };
  }
  if (generator.currentCycle === false && generator.historical === true) {
    return { ok: false, state: GENERATOR_VISIBILITY.HIDDEN_STALE, reasons: ["historical_not_current"] };
  }

  const now = opts.nowDate || new Date().toISOString().slice(0, 10);
  const end = generator.eventEndDate || generator.cycleEndDate || generator.eventStartDate || null;
  if (end && String(end).slice(0, 10) < now) {
    return {
      ok: false,
      state: GENERATOR_VISIBILITY.HIDDEN_STALE,
      reasons: ["past_end_date"],
    };
  }

  const name = String(generator.name || generator.title || "").trim();
  if (name.length < 4) {
    return { ok: false, state: GENERATOR_VISIBILITY.REJECTED, reasons: ["unnamed"] };
  }

  const marketOk =
    generator.marketRelevant === true ||
    generator.marketFit === "STRONG" ||
    generator.marketFit === "PLAUSIBLE" ||
    (opts.marketHints || []).some((h) =>
      `${generator.market || ""} ${generator.venue || ""} ${generator.geography || ""}`
        .toLowerCase()
        .includes(String(h).toLowerCase())
    ) ||
    /geneva|genève|palexpo|suisse|switzerland|rome|roma|via veneto|fiera roma/i.test(
      `${generator.market || ""} ${generator.venue || ""} ${generator.geography || ""}`
    );

  if (!marketOk && generator.marketRelevant !== true) {
    return {
      ok: false,
      state: GENERATOR_VISIBILITY.HIDDEN_WRONG_MARKET,
      reasons: ["market_not_relevant"],
    };
  }

  const futureOk =
    generator.currentCycle === true ||
    generator.cycleStatus === "CURRENT_FUTURE" ||
    (generator.eventStartDate && String(generator.eventStartDate).slice(0, 10) >= now) ||
    (generator.eventYear && Number(generator.eventYear) >= Number(now.slice(0, 4)));

  if (!futureOk) {
    return {
      ok: false,
      state: GENERATOR_VISIBILITY.HIDDEN_STALE,
      reasons: ["no_future_or_current_cycle"],
    };
  }

  if (generator.active === false || generator.status === "INACTIVE") {
    return {
      ok: false,
      state: GENERATOR_VISIBILITY.HIDDEN_INACTIVE,
      reasons: ["inactive"],
    };
  }

  reasons.push("real_named_generator");
  reasons.push("future_or_current_cycle");
  reasons.push("market_relevant");
  return {
    ok: true,
    state: GENERATOR_VISIBILITY.VISIBLE_CURRENT,
    reasons,
  };
}

export function filterVisibleDemandGenerators(generators = [], opts = {}) {
  return (generators || []).filter((g) => isGdiDemandGeneratorVisible(g, opts).ok);
}
