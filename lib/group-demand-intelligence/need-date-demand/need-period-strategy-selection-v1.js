/**
 * Selective research strategy registry for Need-Date Demand Intelligence.
 * Recommends a subset of the 14 PowerShell workflows — never launches all blindly.
 */

import {
  NEED_DATE_DEMAND_INTELLIGENCE_V1,
  NEED_DATE_RESEARCH_STRATEGY,
  NEED_DATE_STRATEGY_META,
  NEED_REVENUE_TYPE,
  NEED_TIMING_OVERLAP,
} from "./need-date-demand-contract-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Score strategies for a need period given hotel context + inventory gaps.
 */
export function selectNeedDateResearchStrategies({
  needPeriod,
  hotel = {},
  inventoryMatch = null,
  maxStrategies = 5,
} = {}) {
  const need = new Set(needPeriod?.revenueNeed || []);
  const city = text(hotel.city || hotel.identity?.city);
  const isResort = /\bresort|all inclusive|canc[uú]n\b/i.test(
    `${hotel.name || ""} ${hotel.displayName || ""} ${city}`,
  );
  const isUrban = !isResort;

  const gapHints = new Set();
  for (const m of inventoryMatch?.matches || []) {
    for (const g of m.researchGapHints || []) gapHints.add(g);
  }

  const hasConfirmed =
    (inventoryMatch?.confirmedCount || 0) > 0 ||
    (inventoryMatch?.probableCount || 0) > 0;

  const revenueUnspecified = need.size === 0;
  const scored = [];
  for (const [strategyId, meta] of Object.entries(NEED_DATE_STRATEGY_META)) {
    let score = 0;
    const reasons = [];
    const fit = (meta.revenueFit || []).filter((r) => need.has(r));
    if (fit.length) {
      score += 40 + fit.length * 8;
      reasons.push(`revenue_fit:${fit.join(",")}`);
    } else if (revenueUnspecified) {
      // Hotel-reported need months with unspecified revenue: evaluate all 14
      // workflows with a baseline score so selective ranking still works.
      score += 28;
      reasons.push("revenue_unspecified_baseline");
    } else {
      continue; // no revenue fit → skip
    }

    if (
      strategyId === NEED_DATE_RESEARCH_STRATEGY.LOCAL_CATERING_DAY_MEETINGS &&
      (need.has(NEED_REVENUE_TYPE.LOCAL_CATERING) ||
        need.has(NEED_REVENUE_TYPE.PRIVATE_DINING))
    ) {
      score += 20;
      reasons.push("catering_priority");
    }
    if (
      strategyId === NEED_DATE_RESEARCH_STRATEGY.RELOCATION_EXTENDED_STAY &&
      need.has(NEED_REVENUE_TYPE.EXTENDED_STAY)
    ) {
      score += 25;
      reasons.push("extended_stay_priority");
    }
    if (
      strategyId === NEED_DATE_RESEARCH_STRATEGY.TRAVEL_TRADE &&
      isResort &&
      need.has(NEED_REVENUE_TYPE.RESORT_GROUP)
    ) {
      score += 18;
      reasons.push("resort_travel_trade");
    }
    if (
      strategyId === NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS &&
      need.has(NEED_REVENUE_TYPE.GROUP_GUESTROOMS)
    ) {
      score += 12;
      reasons.push("group_rooms_assoc");
    }
    if (
      strategyId === NEED_DATE_RESEARCH_STRATEGY.SCALING_COMPANIES &&
      isUrban &&
      (need.has(NEED_REVENUE_TYPE.GROUP_GUESTROOMS) ||
        need.has(NEED_REVENUE_TYPE.MEETING_SPACE))
    ) {
      score += 10;
      reasons.push("urban_corporate");
    }
    if (
      strategyId === NEED_DATE_RESEARCH_STRATEGY.PROFESSIONAL_EVENT_PLANNERS &&
      (need.has(NEED_REVENUE_TYPE.GROUP_GUESTROOMS) ||
        need.has(NEED_REVENUE_TYPE.MEETING_SPACE) ||
        need.has(NEED_REVENUE_TYPE.LOCAL_CATERING))
    ) {
      score += 8;
      reasons.push("planner_channel");
    }

    if (gapHints.has("VERIFY_NEED_PERIOD_DEMAND_DATES")) {
      score += 5;
      reasons.push("timing_gap_enrichment");
    }
    if (gapHints.has("NAMED_BUYER_OR_REFERRAL")) {
      score += 4;
      reasons.push("buyer_gap");
    }

    // If inventory already has strong confirmed matches, reduce discovery urgency
    if (hasConfirmed) {
      score -= 6;
      reasons.push("existing_matches_reduce_urgency");
    }

    scored.push({
      strategyId,
      powershell: meta.powershell,
      workflowNumber: meta.number,
      score,
      revenueFit: fit,
      reasons,
    });
  }

  scored.sort((a, b) => b.score - a.score);
  const selected = scored.slice(0, Math.max(1, maxStrategies));

  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    needPeriodId: needPeriod?.needPeriodId || null,
    hotelId: needPeriod?.hotelId || hotel.hotelId || null,
    selected,
    deferred: scored.slice(Math.max(1, maxStrategies)),
    estimatedDiscoveryQueries: selected.length * 3,
    estimatedDeepResearchCandidates: Math.min(8, selected.length * 2),
    note: "Strategies are recommendations only. Paid Exa execution requires separate founder authorization.",
    launchAllFourteen: false,
  };
}

/**
 * Aggregate strategy plan across multiple need periods (union, re-ranked).
 */
export function selectStrategiesForCampaign({
  needPeriods = [],
  hotel = {},
  inventoryMatch = null,
  maxStrategies = 6,
  commercialProfile = null,
} = {}) {
  const byId = new Map();
  for (const period of needPeriods) {
    const plan = selectNeedDateResearchStrategies({
      needPeriod: period,
      hotel,
      inventoryMatch,
      maxStrategies: 8,
    });
    for (const s of plan.selected) {
      const prev = byId.get(s.strategyId);
      if (!prev || s.score > prev.score) byId.set(s.strategyId, s);
      else {
        prev.score = Math.max(prev.score, s.score);
        prev.reasons = [...new Set([...(prev.reasons || []), ...s.reasons])];
      }
    }
  }

  // Soft boost from hotel commercial priorities (never hard-filters unless exclusion flagged).
  const profile = commercialProfile || {};
  const preferred = new Set(profile.prioritySegments || []);
  const excluded = new Set(profile.excludedSegments || []);
  for (const [strategyId, row] of byId.entries()) {
    if (excluded.has(strategyId)) {
      row.score = Math.max(0, row.score - 40);
      row.reasons = [...new Set([...(row.reasons || []), "hotel_segment_exclusion"])];
      continue;
    }
    if (preferred.has(strategyId)) {
      row.score += 18;
      row.reasons = [...new Set([...(row.reasons || []), "hotel_priority_segment"])];
    }
  }
  // Revenue priority → strategy soft boost via registry revenueFit already scored;
  // association / planners / catering / guestroom chips add preference weight.
  const softBoost = (strategyId, delta, reason) => {
    const row = byId.get(strategyId);
    if (!row) return;
    row.score += delta;
    row.reasons = [...new Set([...(row.reasons || []), reason])];
  };
  for (const rev of profile.revenuePriorities || []) {
    const id = String(rev?.id || rev || "");
    if (id === "ASSOCIATION_BUSINESS") {
      softBoost("association_events", 12, "hotel_revenue_priority:ASSOCIATION_BUSINESS");
    }
    if (id === "LOCAL_CATERING") {
      softBoost(
        "local_catering_day_meetings",
        12,
        "hotel_revenue_priority:LOCAL_CATERING",
      );
    }
    if (id === "BANQUETS_CELEBRATIONS") {
      softBoost(
        "corporate_celebrations",
        10,
        "hotel_revenue_priority:BANQUETS_CELEBRATIONS",
      );
    }
    if (id === "GROUP_ROOM_NIGHTS" || id === "CORPORATE_TRANSIENT") {
      softBoost("association_events", 12, `hotel_revenue_priority:${id}`);
      softBoost("scaling_companies", 12, `hotel_revenue_priority:${id}`);
      softBoost("preferred_lodging", 8, `hotel_revenue_priority:${id}`);
    }
    if (id === "MEETINGS_CONFERENCES") {
      softBoost("meetings_incentives", 12, "hotel_revenue_priority:MEETINGS_CONFERENCES");
      softBoost(
        "professional_event_planners",
        8,
        "hotel_revenue_priority:MEETINGS_CONFERENCES",
      );
    }
  }
  // Weekday guestroom preference is a soft ranking signal only.
  const dayPref = String(profile.dayPreference || "").toUpperCase();
  if (dayPref === "WEEKDAYS") {
    softBoost("scaling_companies", 6, "hotel_day_preference:WEEKDAYS");
    softBoost("association_events", 6, "hotel_day_preference:WEEKDAYS");
    softBoost("government_public_sector", 4, "hotel_day_preference:WEEKDAYS");
  } else if (dayPref === "WEEKENDS") {
    softBoost("social_special_events", 6, "hotel_day_preference:WEEKENDS");
    softBoost("sports_entertainment", 4, "hotel_day_preference:WEEKENDS");
  }

  const selected = [...byId.values()]
    .filter((s) => !excluded.has(s.strategyId) || (profile.segmentMode === "SELECT_SPECIFIC" && preferred.has(s.strategyId)))
    .sort((a, b) => b.score - a.score)
    .slice(0, maxStrategies);

  // Soft preferences must not zero out the plan — if exclusions wiped all, fall back.
  const finalSelected =
    selected.length > 0
      ? selected
      : [...byId.values()].sort((a, b) => b.score - a.score).slice(0, maxStrategies);

  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    selected: finalSelected,
    strategyIds: finalSelected.map((s) => s.strategyId),
    powershellEntryPoints: finalSelected.map((s) => s.powershell),
    commercialProfileApplied: Boolean(commercialProfile),
    estimatedSpendUsdHint: null,
    paidExecutionBlockedUntilFounderGo: true,
    note: "Commercial priorities influence ranking only; soft preferences do not hard-filter promising opportunities.",
  };
}

export function listAllRegisteredStrategies() {
  return Object.keys(NEED_DATE_RESEARCH_STRATEGY).map((k) => {
    const id = NEED_DATE_RESEARCH_STRATEGY[k];
    const meta = NEED_DATE_STRATEGY_META[id];
    return { key: k, strategyId: id, ...meta };
  });
}
