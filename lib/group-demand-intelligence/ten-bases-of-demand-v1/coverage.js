/**
 * Hotel × Base coverage matrix — no fake percentage precision.
 */

import { BASE_OF_DEMAND_LIST, COVERAGE_STATE } from "./taxonomy.js";
import { getHotelBaseWeighting } from "./hotel-weighting.js";

/**
 * @param {object} hotel
 * @param {object} talliesByBase — { [base]: { signals, researchLeads, completeDemandPackets, customerReady, futureWatch, predicted, costUsd, sourceFamilies, languages, feederMarkets, gaps } }
 * @param {object} [opts]
 */
export function buildGdiBaseOfDemandCoverage(hotel = {}, talliesByBase = {}, opts = {}) {
  const weighting = getHotelBaseWeighting(hotel.hotelKey);
  const now = opts.nowDate || new Date().toISOString().slice(0, 10);
  const rows = [];

  for (const base of BASE_OF_DEMAND_LIST) {
    const t = talliesByBase[base] || {};
    const priority = weighting.priorityOf(base);
    const signals = Number(t.signals || 0);
    const leads = Number(t.researchLeads || 0);
    const packets = Number(t.completeDemandPackets || 0);
    const ready = Number(t.customerReady || 0);
    const watch = Number(t.futureWatch || 0);
    const predicted = Number(t.predicted || 0);
    const researched = Boolean(t.researched);

    let coverageState = COVERAGE_STATE.NOT_RESEARCHED;
    if (priority === "LOW" && !researched && signals === 0) {
      coverageState = COVERAGE_STATE.NOT_RESEARCHED;
    } else if (!researched && signals === 0) {
      coverageState = COVERAGE_STATE.NOT_RESEARCHED;
    } else if (packets >= 3 || ready >= 1) {
      coverageState = COVERAGE_STATE.DEEP;
    } else if (packets >= 1 || (leads >= 3 && signals >= 5)) {
      coverageState = COVERAGE_STATE.ADEQUATE;
    } else if (researched || signals > 0 || leads > 0) {
      coverageState = COVERAGE_STATE.LIGHT;
    }

    // Saturation: many signals, zero packets after deep attempt
    if (researched && signals >= 12 && packets === 0 && leads === 0 && priority === "HIGH") {
      coverageState = COVERAGE_STATE.SATURATED;
    }

    const yieldLabel =
      ready > 0
        ? "USEFUL_READY"
        : predicted > 0 || watch > 0
          ? "USEFUL_PREDICTED_OR_WATCH"
          : packets > 0
            ? "COMPLETE_PACKET_ONLY"
            : leads > 0
              ? "LEADS_ONLY"
              : signals > 0
                ? "SIGNALS_ONLY"
                : "NO_YIELD";

    rows.push({
      hotelId: hotel.hotelId || hotel.hotelKey,
      hotelKey: hotel.hotelKey,
      marketId: hotel.destinationMarket || hotel.market || "",
      baseOfDemand: base,
      priority,
      coverageState,
      lastResearchDate: researched || signals > 0 ? now : "",
      sourceFamiliesAttempted: (t.sourceFamilies || []).join("|"),
      languagesAttempted: (t.languages || []).join("|"),
      feederMarketsAttempted: (t.feederMarkets || []).join("|"),
      signals,
      researchLeads: leads,
      completeDemandPackets: packets,
      predictedOpportunities: predicted,
      customerReady: ready,
      futureWatch: watch,
      yield: yieldLabel,
      knownGaps: (t.gaps || []).join("|"),
      costUsd: Number(t.costUsd || 0),
    });
  }

  const highRows = rows.filter((r) => r.priority === "HIGH");
  const highAdequate = highRows.every(
    (r) =>
      r.coverageState === COVERAGE_STATE.ADEQUATE ||
      r.coverageState === COVERAGE_STATE.DEEP ||
      r.coverageState === COVERAGE_STATE.SATURATED
  );
  const noneNotResearchedHigh = highRows.every(
    (r) => r.coverageState !== COVERAGE_STATE.NOT_RESEARCHED
  );

  return {
    hotelKey: hotel.hotelKey,
    rows,
    highPriorityCoverageComplete: highAdequate && noneNotResearchedHigh,
    publicDataCeilingEligible: highAdequate && noneNotResearchedHigh,
  };
}
