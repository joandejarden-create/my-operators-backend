/**
 * GDI 2.0 Phase 3B — Corporate Projects & Extended Stay discovery config.
 *
 * Awarded contracts / announced projects alone ≠ hotel demand.
 * Composes Phase 2G buyer strategies + shared independent discovery patterns.
 */

import {
  CPE_DEMAND_CATEGORY,
  CPE_INSUFFICIENT_DEMAND_INDICATORS,
  CORPORATE_PROJECTS_EXTENDED_STAY_V1,
  CPE_RESEARCH_SEGMENT,
} from "./corporate-projects-extended-stay-constants-v1.js";
import {
  selectBuyerDiscoveryStrategies,
  getBuyerDiscoveryStrategy,
} from "../buyer-discovery-strategies-v1.js";

export const CPE_DISCOVERY_V1 = "gdi-corporate-projects-extended-stay-discovery-v1";

const CATEGORY_STRATEGIES = Object.freeze({
  [CPE_DEMAND_CATEGORY.TECHNOLOGY_IMPLEMENTATIONS]: {
    verifiableIndicators: [
      "implementation_schedule_with_onsite_team",
      "vendor_deployment_announcement",
      "multi_week_cutover_with_travel",
    ],
    buyerStrategyIds: ["PROJECT_OPS_DEPLOYMENT", "SUPPLIER_CONTRACTOR_IMPLEMENTATION"],
    preferredSources: ["project_case_studies", "implementation_press", "careers_project_context"],
  },
  [CPE_DEMAND_CATEGORY.CONSULTING_ADVISORY_DEPLOYMENTS]: {
    verifiableIndicators: [
      "onsite_consulting_engagement",
      "advisory_team_mobilization",
    ],
    buyerStrategyIds: ["PROJECT_OPS_DEPLOYMENT", "COMPANY_TRAVEL_PROCUREMENT"],
    preferredSources: ["engagement_announcements", "case_studies"],
  },
  [CPE_DEMAND_CATEGORY.CONSTRUCTION_ENGINEERING_PROJECTS]: {
    verifiableIndicators: [
      "site_construction_mobilization",
      "engineering_field_team_deployment",
      "project_duration_with_nonlocal_trades",
    ],
    buyerStrategyIds: ["PROJECT_OPS_DEPLOYMENT", "REGIONAL_FACILITIES_OFFICE_OPS"],
    preferredSources: ["construction_press", "permit_notices", "contractor_announcements"],
  },
  [CPE_DEMAND_CATEGORY.FACILITIES_COMMISSIONING_MAINTENANCE]: {
    verifiableIndicators: [
      "commissioning_schedule",
      "shutdown_maintenance_with_visiting_crew",
    ],
    buyerStrategyIds: ["REGIONAL_FACILITIES_OFFICE_OPS", "PROJECT_OPS_DEPLOYMENT"],
    preferredSources: ["facilities_notices", "maintenance_turnaround_press"],
  },
  [CPE_DEMAND_CATEGORY.DATACENTER_INFRASTRUCTURE_PROJECTS]: {
    verifiableIndicators: [
      "datacenter_build_or_fitout",
      "infrastructure_mobilization",
    ],
    buyerStrategyIds: ["PROJECT_OPS_DEPLOYMENT", "SUPPLIER_CONTRACTOR_IMPLEMENTATION"],
    preferredSources: ["datacenter_press", "infrastructure_awards"],
  },
  [CPE_DEMAND_CATEGORY.NEW_OFFICE_FACILITY_OPENINGS]: {
    verifiableIndicators: [
      "new_office_opening_with_visiting_teams",
      "facility_launch_program",
    ],
    buyerStrategyIds: ["REGIONAL_FACILITIES_OFFICE_OPS", "COMPANY_TRAVEL_PROCUREMENT"],
    preferredSources: ["office_opening_press", "workplace_announcements"],
  },
  [CPE_DEMAND_CATEGORY.INDUSTRIAL_EQUIPMENT_INSTALLATIONS]: {
    verifiableIndicators: [
      "equipment_install_crew_travel",
      "factory_acceptance_or_site_install_schedule",
    ],
    buyerStrategyIds: ["SUPPLIER_CONTRACTOR_IMPLEMENTATION", "PROJECT_OPS_DEPLOYMENT"],
    preferredSources: ["vendor_install_press", "case_studies"],
  },
  [CPE_DEMAND_CATEGORY.GOVERNMENT_CONTRACT_MOBILIZATIONS]: {
    verifiableIndicators: [
      "contract_award_WITH_mobilization_or_onsite_performance",
      "performance_location_named",
    ],
    insufficientAlone: ["contract_award_without_mobilization_language"],
    buyerStrategyIds: ["PROJECT_OPS_DEPLOYMENT", "COMPANY_TRAVEL_PROCUREMENT"],
    preferredSources: ["award_notices", "mobilization_press"],
  },
  [CPE_DEMAND_CATEGORY.FIELD_SERVICE_TECHNICAL_TEAMS]: {
    verifiableIndicators: [
      "field_service_deployment",
      "technical_team_extended_stay_signal",
    ],
    buyerStrategyIds: ["PROJECT_OPS_DEPLOYMENT", "COMPANY_TRAVEL_PROCUREMENT"],
    preferredSources: ["field_service_press", "customer_deployment_pages"],
  },
  [CPE_DEMAND_CATEGORY.TEMPORARY_PROJECT_RELOCATIONS]: {
    verifiableIndicators: [
      "temporary_relocation_of_project_staff",
      "swing_space_or_surge_staffing_travel",
    ],
    buyerStrategyIds: ["COMPANY_TRAVEL_PROCUREMENT", "REGIONAL_FACILITIES_OFFICE_OPS"],
    preferredSources: ["relocation_notices", "project_surge_press"],
  },
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function classifyCpeDiscoverySignal(signal = {}) {
  const blob = text(
    [
      signal.headline,
      signal.description,
      signal.evidenceQuote,
      signal.category,
      ...(signal.indicators || []),
    ].join(" "),
  );

  const awardAlone =
    /\b(awarded|wins?\s+contract|contract\s+award|selected\s+as\s+contractor)\b/i.test(blob) &&
    !/\b(mobiliz|onsite|on-site|deployment|field team|extended stay|crew travel|lodging|hotel|overnight|visiting)\b/i.test(
      blob,
    );
  const remoteOnly = /\b(fully remote|remote.?only|no onsite|virtual delivery only)\b/i.test(blob);
  const localOnly =
    /\b(local (?:crew|workforce|staff) only|no travel required)\b/i.test(blob);

  let status = "CANDIDATE";
  const reasons = [];
  if (awardAlone) {
    status = "REJECT_INSUFFICIENT";
    reasons.push(...CPE_INSUFFICIENT_DEMAND_INDICATORS.filter((x) => /award|announced/.test(x)));
    reasons.push("awarded_contract_or_announced_project_alone_does_not_prove_hotel_demand");
  }
  if (remoteOnly || localOnly) {
    status = "REJECT_CONTRADICTORY";
    reasons.push(remoteOnly ? "remote_delivery_only" : "local_only_crew");
  }

  const verifiableHit = [
    /mobiliz|deployment|implementation|commissioning|install|onsite|on-site|field team|extended stay|visiting (?:crew|staff|engineers)/i.test(
      blob,
    ),
    /guestroom|lodging|hotel|overnight|per diem|travel|accommodation/i.test(blob),
  ].filter(Boolean).length;

  if (status === "CANDIDATE" && verifiableHit === 0) {
    status = "REJECT_INSUFFICIENT";
    reasons.push("no_verifiable_project_travel_or_lodging_indicator");
  }

  return {
    version: CPE_DISCOVERY_V1,
    status,
    reasons: [...new Set(reasons)],
    verifiableIndicatorStrength: verifiableHit,
    synthetic: signal.synthetic === true,
  };
}

export function buildCpeDiscoveryPlan(categoryId, opts = {}) {
  const cat = CATEGORY_STRATEGIES[categoryId];
  if (!cat) {
    return { version: CPE_DISCOVERY_V1, ok: false, reason: "unknown_category", categoryId };
  }
  const attempted = opts.attemptedStrategyIds || [];
  const buyer = selectBuyerDiscoveryStrategies({
    gapType: opts.gapType || "BUYER_IDENTITY_GAP",
    segmentFamily: "scaling_companies",
    attemptedIds: attempted,
    max: 3,
  });
  const preferred = (cat.buyerStrategyIds || [])
    .map((id) => getBuyerDiscoveryStrategy(id))
    .filter(Boolean)
    .filter((s) => !attempted.includes(s.id));

  return {
    version: CPE_DISCOVERY_V1,
    parentVersion: CORPORATE_PROJECTS_EXTENDED_STAY_V1,
    researchSegment: CPE_RESEARCH_SEGMENT,
    ok: true,
    categoryId,
    verifiableIndicators: cat.verifiableIndicators,
    insufficientAlone: cat.insufficientAlone || [],
    preferredSources: cat.preferredSources,
    buyerStrategies: preferred.length ? preferred : buyer.strategies,
    note: "Awarded contract or announced project alone must not establish hotel demand.",
    costAuthorization: { paid: false, authorization: "direct_public_or_cached_only", estimatedUsd: 0 },
  };
}

export function listCpeDemandCategories() {
  return Object.values(CPE_DEMAND_CATEGORY).map((id) => ({
    id,
    ...(CATEGORY_STRATEGIES[id] || {}),
  }));
}

/** Segment config for shared independent discovery engine. */
export function getCpeIndependentDiscoverySegmentConfig() {
  return {
    segmentId: CPE_RESEARCH_SEGMENT,
    sourceCategoryHints: [
      "construction_trade_press",
      "usaspending_awards",
      "corporate_ir_newsrooms",
      "municipal_business_news",
    ],
    insufficientAlonePatterns: [
      /\bawarded\b/i,
      /\bcontract award\b/i,
      /\bselected as (?:the )?contractor\b/i,
    ],
    demandStrengtheningPatterns: [
      /\bmobiliz/i,
      /\bdeployment\b/i,
      /\bonsite\b|\bon-site\b/i,
      /\bfield (?:team|crew|engineers)\b/i,
      /\bextended stay\b/i,
      /\blodging\b|\bhotel\b|\bovernight\b/i,
    ],
    extractionPatterns: [
      {
        id: "cpe_project_award_mobilization",
        label: "Project award with mobilization",
        categoryId: CPE_DEMAND_CATEGORY.CONSTRUCTION_ENGINEERING_PROJECTS,
        re: /\b(?<org>[A-Z][A-Za-z0-9&.\-']+(?:\s+[A-Z][A-Za-z0-9&.\-']+){0,6})\b[\s\S]{0,160}?\b(?:was awarded|awarded|wins?)\b[\s\S]{0,160}?\b(?<activity>mobiliz\w*|onsite field team|on-site field team|deployment|field (?:team|crew))\b/gi,
        defaultThesis:
          "Project mobilization language suggests visiting project personnel may need guestrooms near the worksite — demand still requires hotel-fit investigation.",
        defaultHotelUse: "Extended-stay / project crew guestrooms (inferred — not confirmed)",
      },
      {
        id: "cpe_implementation_onsite",
        label: "Technology implementation onsite",
        categoryId: CPE_DEMAND_CATEGORY.TECHNOLOGY_IMPLEMENTATIONS,
        re: /\b(?<org>[A-Z][A-Za-z0-9&.\-']+(?:\s+[A-Z][A-Za-z0-9&.\-']+){0,6})\b[\s\S]{0,140}?\b(?<activity>implementation|deploy(?:ment|ing)|cutover)\b[\s\S]{0,100}?\b(?:onsite|on-site|field team|extended stay)\b/gi,
        defaultThesis:
          "Onsite implementation activity may create transient guestroom demand for vendor/project teams.",
        defaultHotelUse: "Project team guestrooms during implementation window",
      },
      {
        id: "cpe_datacenter_fitout",
        label: "Data center / infrastructure project",
        categoryId: CPE_DEMAND_CATEGORY.DATACENTER_INFRASTRUCTURE_PROJECTS,
        re: /\b(?<org>[A-Z][A-Za-z0-9&.\-']+(?:\s+[A-Z][A-Za-z0-9&.\-']+){0,6})\b[\s\S]{0,120}?\b(?<activity>data\s*center|datacenter|infrastructure (?:project|build|fit-?out))\b[\s\S]{0,100}?\b(?:mobiliz\w*|crew|contractors?|engineers?)\b/gi,
        defaultThesis:
          "Infrastructure build with crew language may imply nonlocal personnel lodging near the site.",
        defaultHotelUse: "Crew / contractor guestrooms near project site",
      },
    ],
  };
}

/** CMT segment config for the same shared engine. */
export function getCmtIndependentDiscoverySegmentConfig() {
  return {
    segmentId: "corporate_meetings_training",
    sourceCategoryHints: [
      "meetings_industry_press",
      "training_industry_press",
      "corporate_ir_newsrooms",
      "chamber_events",
      "municipal_business_news",
    ],
    insufficientAlonePatterns: [/\bhiring\b/i, /\bheadcount\b/i, /\bgrew to\b/i],
    demandStrengtheningPatterns: [
      /\bkickoff\b/i,
      /\boffsite\b/i,
      /\bretreat\b/i,
      /\btraining (?:academy|cohort|program)\b/i,
      /\bmeeting rooms?\b/i,
      /\blodging\b|\bhotel\b|\bovernight\b/i,
    ],
    extractionPatterns: [
      {
        id: "cmt_sko_lodging",
        label: "Sales kickoff with lodging",
        categoryId: "ANNUAL_SALES_KICKOFFS",
        // Use [\s\S] (not [^.\\n]) — the latter accidentally excludes the letter "n".
        re: /\b(?<org>[A-Z][A-Za-z0-9&.\-']+(?:\s+[A-Z][A-Za-z0-9&.\-']+){0,5})\b[\s\S]{0,120}?\b(?<activity>sales kickoff|annual (?:sales )?meeting|leadership (?:offsite|retreat))\b[\s\S]{0,120}?\b(hotel|lodging|overnight|meeting rooms?)/gi,
        defaultThesis:
          "Announced kickoff/offsite with lodging or meeting-room language supports a reliable inferred guestroom/catering thesis pending buyer + hotel fit.",
        defaultHotelUse: "Guestrooms + meeting space for corporate program",
      },
      {
        id: "cmt_training_cohort",
        label: "Training cohort travel",
        categoryId: "CORPORATE_TRAINING_ACADEMIES",
        re: /\b(?<org>[A-Z][A-Za-z0-9&.\-']+(?:\s+[A-Z][A-Za-z0-9&.\-']+){0,5})\b[\s\S]{0,120}?\b(?<activity>training (?:academy|cohort|program)|onboarding academy)\b[\s\S]{0,120}?\b(travel|overnight|hotel|lodging|residential)/gi,
        defaultThesis:
          "Training program with travel/residential language may create guestroom demand near the training location.",
        defaultHotelUse: "Training cohort guestrooms + meeting/training space",
      },
    ],
  };
}
