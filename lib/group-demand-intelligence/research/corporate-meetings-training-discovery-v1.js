/**
 * GDI 2.0 Phase 3A — Corporate Meetings & Training discovery strategies.
 *
 * Verifiable indicators only. Hiring/growth/size alone ≠ hotel demand.
 * Composes Phase 2G buyer-discovery strategies; does not invent opportunities.
 */

import {
  CMT_DEMAND_CATEGORY,
  CMT_INSUFFICIENT_DEMAND_INDICATORS,
  CORPORATE_MEETINGS_TRAINING_V1,
} from "./corporate-meetings-training-constants-v1.js";
import {
  selectBuyerDiscoveryStrategies,
  getBuyerDiscoveryStrategy,
} from "../buyer-discovery-strategies-v1.js";

export const CMT_DISCOVERY_V1 = "gdi-corporate-meetings-training-discovery-v1";

const CATEGORY_STRATEGIES = Object.freeze({
  [CMT_DEMAND_CATEGORY.LEADERSHIP_EXECUTIVE_MEETINGS]: {
    verifiableIndicators: [
      "announced_leadership_meeting_dates",
      "published_executive_offsite_agenda",
      "confirmed_corporate_retreat",
    ],
    buyerStrategyIds: ["MEETINGS_CORPORATE_EVENTS", "REGIONAL_FACILITIES_OFFICE_OPS"],
    preferredSources: ["press_leadership_gatherings", "events_pages", "about_team"],
  },
  [CMT_DEMAND_CATEGORY.ANNUAL_SALES_KICKOFFS]: {
    verifiableIndicators: [
      "announced_sko_dates",
      "published_sales_kickoff_agenda",
      "hotel_or_venue_rfp_language",
    ],
    buyerStrategyIds: ["MEETINGS_CORPORATE_EVENTS", "COMPANY_TRAVEL_PROCUREMENT"],
    preferredSources: ["sales_kickoff_announcements", "events_pages", "partner_portals"],
  },
  [CMT_DEMAND_CATEGORY.CORPORATE_TRAINING_ACADEMIES]: {
    verifiableIndicators: [
      "corporate_training_calendar",
      "academy_program_pages",
      "multi_day_training_facility_requirements",
    ],
    buyerStrategyIds: ["LEARNING_DEVELOPMENT_TRAINING", "MEETINGS_CORPORATE_EVENTS"],
    preferredSources: ["academy_pages", "lnd_calendars", "training_partnerships"],
  },
  [CMT_DEMAND_CATEGORY.NEW_HIRE_ONBOARDING_COHORTS]: {
    verifiableIndicators: [
      "documented_onboarding_cohort_program",
      "training_calendar_with_travel",
      "large_hiring_WITH_documented_training_travel",
    ],
    insufficientAlone: ["large_hiring_without_training_program"],
    buyerStrategyIds: ["LEARNING_DEVELOPMENT_TRAINING", "COMPANY_TRAVEL_PROCUREMENT"],
    preferredSources: ["careers_onboarding_pages", "people_ops_training", "press_academy"],
  },
  [CMT_DEMAND_CATEGORY.REGIONAL_MANAGEMENT_CONFERENCES]: {
    verifiableIndicators: [
      "regional_management_conference_announcement",
      "recurring_company_meeting_agenda",
    ],
    buyerStrategyIds: ["MEETINGS_CORPORATE_EVENTS", "REGIONAL_FACILITIES_OFFICE_OPS"],
    preferredSources: ["regional_news", "events_pages", "office_location_pages"],
  },
  [CMT_DEMAND_CATEGORY.CORPORATE_RETREATS_OFFSITES]: {
    verifiableIndicators: [
      "confirmed_corporate_retreat",
      "offsite_announcement_with_dates_or_location",
    ],
    buyerStrategyIds: ["MEETINGS_CORPORATE_EVENTS", "COMPANY_TRAVEL_PROCUREMENT"],
    preferredSources: ["press_offsites", "events_pages", "leadership_comms"],
  },
  [CMT_DEMAND_CATEGORY.DISTRIBUTOR_PARTNER_TRAINING]: {
    verifiableIndicators: [
      "partner_training_program",
      "distributor_academy_calendar",
      "professional_training_partnership",
    ],
    buyerStrategyIds: ["LEARNING_DEVELOPMENT_TRAINING", "SUPPLIER_CONTRACTOR_IMPLEMENTATION"],
    preferredSources: ["partner_portals", "training_partnerships", "events_pages"],
  },
  [CMT_DEMAND_CATEGORY.MA_INTEGRATION_MEETINGS]: {
    verifiableIndicators: [
      "published_integration_meeting_agenda",
      "announced_post_merger_onsite_programs",
    ],
    buyerStrategyIds: ["MEETINGS_CORPORATE_EVENTS", "REGIONAL_FACILITIES_OFFICE_OPS"],
    preferredSources: ["press_ma_integration", "events_pages"],
  },
  [CMT_DEMAND_CATEGORY.PRODUCT_SERVICE_TRAINING]: {
    verifiableIndicators: [
      "product_training_calendar",
      "service_academy_pages",
      "training_facility_requirements",
    ],
    buyerStrategyIds: ["LEARNING_DEVELOPMENT_TRAINING", "MEETINGS_CORPORATE_EVENTS"],
    preferredSources: ["product_training_pages", "academy_pages"],
  },
  [CMT_DEMAND_CATEGORY.RECURRING_DEPARTMENTAL_MEETINGS]: {
    verifiableIndicators: [
      "recurring_company_meetings",
      "published_departmental_conference_cadence",
    ],
    buyerStrategyIds: ["MEETINGS_CORPORATE_EVENTS", "COMPANY_TRAVEL_PROCUREMENT"],
    preferredSources: ["internal_comms_public_summaries", "events_pages"],
  },
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Score whether a candidate signal is a verifiable CMT demand indicator
 * vs insufficient hiring/growth-alone noise.
 */
export function classifyCmtDiscoverySignal(signal = {}) {
  const blob = text(
    [
      signal.headline,
      signal.description,
      signal.evidenceQuote,
      signal.category,
      ...(signal.indicators || []),
    ].join(" "),
  );

  const hiringAlone =
    /\b(hiring|headcount|grew to|employees|workforce)\b/i.test(blob) &&
    !/\b(training|onboarding cohort|academy|offsite|kickoff|retreat|meeting|conference|workshop)\b/i.test(
      blob,
    );
  const virtualOnly = /\b(virtual.?only|fully online|no overnight|webinar.?only)\b/i.test(blob);
  const preContracted = /\b(exclusive venue|pre-?contracted (?:hotel|venue)|already booked at)\b/i.test(
    blob,
  );

  let status = "CANDIDATE";
  const reasons = [];
  if (hiringAlone) {
    status = "REJECT_INSUFFICIENT";
    reasons.push(...CMT_INSUFFICIENT_DEMAND_INDICATORS.filter((x) => /hiring|headcount|size/i.test(x)));
    reasons.push("hiring_or_growth_alone_does_not_establish_hotel_demand");
  }
  if (virtualOnly) {
    status = "REJECT_CONTRADICTORY";
    reasons.push("virtual_only_or_no_overnight");
  }
  if (preContracted) {
    status = "AMBIGUOUS_PRECONTRACTED_VENUE";
    reasons.push("event_may_have_precontracted_venue");
  }

  const verifiableHit = [
    /announced|scheduled|calendar|agenda|retreat|offsite|kickoff|academy|cohort|training program|conference|workshop/i.test(
      blob,
    ),
    /room.?block|hotel|lodging|meeting room|catering|overnight/i.test(blob),
  ].filter(Boolean).length;

  if (status === "CANDIDATE" && verifiableHit === 0) {
    status = "REJECT_INSUFFICIENT";
    reasons.push("no_verifiable_meeting_or_training_indicator");
  }

  return {
    version: CMT_DISCOVERY_V1,
    status,
    reasons: [...new Set(reasons)],
    verifiableIndicatorStrength: verifiableHit,
    synthetic: signal.synthetic === true,
  };
}

/**
 * Build discovery plan for a category — reuses Phase 2G buyer strategies.
 */
export function buildCmtDiscoveryPlan(categoryId, opts = {}) {
  const cat = CATEGORY_STRATEGIES[categoryId];
  if (!cat) {
    return {
      version: CMT_DISCOVERY_V1,
      ok: false,
      reason: "unknown_category",
      categoryId,
    };
  }
  const attempted = opts.attemptedStrategyIds || [];
  const buyer = selectBuyerDiscoveryStrategies({
    gapType: opts.gapType || "BUYER_IDENTITY_GAP",
    segmentFamily: "scaling_companies",
    attemptedIds: attempted,
    max: 3,
  });
  // Prefer category-specific buyer strategies first
  const preferred = (cat.buyerStrategyIds || [])
    .map((id) => getBuyerDiscoveryStrategy(id))
    .filter(Boolean)
    .filter((s) => !attempted.includes(s.id));

  return {
    version: CMT_DISCOVERY_V1,
    parentVersion: CORPORATE_MEETINGS_TRAINING_V1,
    ok: true,
    categoryId,
    verifiableIndicators: cat.verifiableIndicators,
    insufficientAlone: cat.insufficientAlone || [],
    preferredSources: cat.preferredSources,
    buyerStrategies: preferred.length ? preferred : buyer.strategies,
    note: "Hiring/growth/size alone must not establish hotel demand.",
    costAuthorization: { paid: false, authorization: "direct_public_or_cached_only", estimatedUsd: 0 },
  };
}

export function listCmtDemandCategories() {
  return Object.values(CMT_DEMAND_CATEGORY).map((id) => ({
    id,
    ...(CATEGORY_STRATEGIES[id] || {}),
  }));
}
