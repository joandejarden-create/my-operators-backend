/**
 * Evaluate all 14 GDI research workflows for a hotel need-date campaign.
 * Relevance only — does not authorize or execute paid research.
 */

import {
  NEED_DATE_RESEARCH_STRATEGY,
  NEED_DATE_STRATEGY_META,
} from "./need-date-demand-contract-v1.js";

export const STRATEGY_RELEVANCE = Object.freeze({
  HIGH_RELEVANCE: "HIGH_RELEVANCE",
  MEDIUM_RELEVANCE: "MEDIUM_RELEVANCE",
  LOW_RELEVANCE: "LOW_RELEVANCE",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

const WATERSTONE_DEFAULT = {
  hotelId: "recgMYovrrZDJMqzX",
  market: "Boca Raton / South Florida",
  facilities: {
    marinaResort: true,
    meetingSpace: true,
    catering: true,
    leisureSocial: true,
    extendedStayLikely: false,
  },
};

/**
 * @returns {{ strategies: Array, selectedForPursuit: string[], deferred: string[] }}
 */
export function evaluateFourteenStrategiesForNeedDateCampaign({
  hotel = WATERSTONE_DEFAULT,
  needPeriods = [],
  commercialProfile = null,
  inventoryCount = 0,
  priorSelected = [],
} = {}) {
  const months = needPeriods
    .map((p) => String(p.startDate || "").slice(0, 7))
    .filter(Boolean);
  const revenuePriorities = (commercialProfile?.revenuePriorities || []).map(
    (x) => (typeof x === "string" ? x : x.id),
  );
  const segments = new Set(commercialProfile?.prioritySegments || []);

  const rows = Object.values(NEED_DATE_RESEARCH_STRATEGY).map((strategyId) => {
    const meta = NEED_DATE_STRATEGY_META[strategyId] || {};
    const evalRow = scoreStrategy({
      strategyId,
      meta,
      hotel,
      months,
      revenuePriorities,
      segments,
      inventoryCount,
      priorSelected,
    });
    return {
      strategyId,
      workflowNumber: meta.number,
      powershell: meta.powershell,
      ...evalRow,
    };
  });

  rows.sort((a, b) => {
    const order = {
      HIGH_RELEVANCE: 0,
      MEDIUM_RELEVANCE: 1,
      LOW_RELEVANCE: 2,
      NOT_APPLICABLE: 3,
    };
    return (order[a.relevance] ?? 9) - (order[b.relevance] ?? 9) || a.workflowNumber - b.workflowNumber;
  });

  const selectedForPursuit = rows
    .filter((r) => r.relevance === STRATEGY_RELEVANCE.HIGH_RELEVANCE)
    .map((r) => r.strategyId);
  const deferred = rows
    .filter((r) => r.relevance !== STRATEGY_RELEVANCE.HIGH_RELEVANCE)
    .map((r) => r.strategyId);

  return {
    version: "gdi-need-date-fourteen-strategy-relevance-v1",
    hotelId: hotel.hotelId || null,
    needMonths: months,
    strategies: rows,
    selectedForPursuit,
    deferred,
    note: "Relevance evaluation only. Does not authorize paid Exa or launch workflows.",
  };
}

function scoreStrategy({
  strategyId,
  hotel,
  months,
  revenuePriorities,
  segments,
  priorSelected,
}) {
  const fac = hotel.facilities || {};
  const reasons = [];
  let relevance = STRATEGY_RELEVANCE.MEDIUM_RELEVANCE;

  const boost = (why) => {
    reasons.push(why);
  };

  switch (strategyId) {
    case NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS:
      relevance = STRATEGY_RELEVANCE.HIGH_RELEVANCE;
      boost(
        "Boca/Palm Beach corridor hosts association & medical programmes — prioritize editions without a competing host/block; do not treat speculative overflow as default demand.",
      );
      if (segments.has("association_events") || revenuePriorities.includes("ASSOCIATION_BUSINESS")) {
        boost("Hotel commercial priorities favor association business.");
      }
      break;
    case NEED_DATE_RESEARCH_STRATEGY.MEETINGS_INCENTIVES:
      relevance = STRATEGY_RELEVANCE.HIGH_RELEVANCE;
      boost("Resort + meeting space supports corporate meetings with peak-room potential.");
      break;
    case NEED_DATE_RESEARCH_STRATEGY.LOCAL_CATERING_DAY_MEETINGS:
      relevance = STRATEGY_RELEVANCE.HIGH_RELEVANCE;
      boost("Marina/resort F&B and day-meeting capacity are commercially accessible locally.");
      break;
    case NEED_DATE_RESEARCH_STRATEGY.CORPORATE_CELEBRATIONS:
      relevance = STRATEGY_RELEVANCE.HIGH_RELEVANCE;
      boost("Private dining / banquet capacity fits South Florida corporate celebrations.");
      break;
    case NEED_DATE_RESEARCH_STRATEGY.PROFESSIONAL_EVENT_PLANNERS:
      relevance = STRATEGY_RELEVANCE.HIGH_RELEVANCE;
      boost(
        "Planner intermediaries can source marina-resort groups, weddings and social programmes — prioritize open lodging requirements, not speculative overflow.",
      );
      break;
    case NEED_DATE_RESEARCH_STRATEGY.SCALING_COMPANIES:
      relevance = STRATEGY_RELEVANCE.HIGH_RELEVANCE;
      boost("South Florida corporate accounts can generate transient + meeting demand near need months.");
      break;
    case NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS:
      if (
        fac.marinaResort === true ||
        fac.leisureSocial === true ||
        revenuePriorities.includes("WEDDINGS_SOCIAL") ||
        segments.has("social_special_events") ||
        segments.has("weddings_social_private_events")
      ) {
        relevance = STRATEGY_RELEVANCE.HIGH_RELEVANCE;
        boost(
          "Marina/leisure resort product supports weddings, destination social groups and guestroom blocks — dedicated Social Special Events research is commercially material.",
        );
      } else {
        relevance = STRATEGY_RELEVANCE.MEDIUM_RELEVANCE;
      }
      boost(
        "Require future demand evidence and buyer relevance; avoid wedding-directory listings and venue marketing pages. Corporate Celebrations is a separate segment — not a substitute for wedding research.",
      );
      break;
    case NEED_DATE_RESEARCH_STRATEGY.SPORTS_ENTERTAINMENT:
      relevance = STRATEGY_RELEVANCE.MEDIUM_RELEVANCE;
      boost("Regional sports/entertainment travel exists; Aug–Oct fit depends on verified schedules.");
      break;
    case NEED_DATE_RESEARCH_STRATEGY.EDUCATION_SCIENTIFIC:
      relevance = STRATEGY_RELEVANCE.MEDIUM_RELEVANCE;
      boost("Academic/scientific groups present in region; many editions fall outside Aug–Oct.");
      break;
    case NEED_DATE_RESEARCH_STRATEGY.PREFERRED_LODGING:
      relevance = STRATEGY_RELEVANCE.MEDIUM_RELEVANCE;
      boost("Partnership pipeline is valuable but slower than event-dated demand for need months.");
      break;
    case NEED_DATE_RESEARCH_STRATEGY.GOVERNMENT_PUBLIC_SECTOR:
      relevance = STRATEGY_RELEVANCE.LOW_RELEVANCE;
      boost("Public-sector sourcing is slower and less marina-resort aligned for short need windows.");
      break;
    case NEED_DATE_RESEARCH_STRATEGY.TRAVEL_TRADE:
      relevance = STRATEGY_RELEVANCE.LOW_RELEVANCE;
      boost("Travel-trade groups possible but weaker incremental signal vs dated events for these months.");
      break;
    case NEED_DATE_RESEARCH_STRATEGY.PROJECTS_INFRASTRUCTURE:
      relevance = STRATEGY_RELEVANCE.LOW_RELEVANCE;
      boost("Project crews need verified deployment timing; not default for leisure/marina resort months.");
      break;
    case NEED_DATE_RESEARCH_STRATEGY.RELOCATION_EXTENDED_STAY:
      relevance =
        fac.extendedStayLikely === true
          ? STRATEGY_RELEVANCE.MEDIUM_RELEVANCE
          : STRATEGY_RELEVANCE.LOW_RELEVANCE;
      boost(
        fac.extendedStayLikely === true
          ? "Extended-stay inventory supports relocation programmes."
          : "Waterstone is not primarily extended-stay product; low expected yield for Aug–Oct need dates.",
      );
      break;
    default:
      relevance = STRATEGY_RELEVANCE.NOT_APPLICABLE;
      boost("Unknown strategy id.");
  }

  if (priorSelected.includes(strategyId)) {
    boost("Included in prior V4 selective set (verify execution depth separately).");
  }

  const pursueNow =
    relevance === STRATEGY_RELEVANCE.HIGH_RELEVANCE ||
    (relevance === STRATEGY_RELEVANCE.MEDIUM_RELEVANCE &&
      ["social_special_events", "sports_entertainment", "education_scientific"].includes(
        strategyId,
      ));

  return {
    relevance,
    pursueInContinuation: pursueNow,
    reasons,
    monthsConsidered: months,
  };
}
