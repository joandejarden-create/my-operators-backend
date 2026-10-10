/**
 * Canonical GDI Demand Engine Taxonomy V1 — hotel-agnostic.
 */

export const DEMAND_ENGINE = Object.freeze({
  CORPORATE: "CORPORATE",
  ASSOCIATION_NGO: "ASSOCIATION_NGO",
  PHARMA_LIFE_SCIENCES: "PHARMA_LIFE_SCIENCES",
  TECH_FINANCIAL_PROFESSIONAL_SERVICES: "TECH_FINANCIAL_PROFESSIONAL_SERVICES",
  GOVERNMENT_INTL_ORGANIZATIONS: "GOVERNMENT_INTL_ORGANIZATIONS",
  SPORTS_ENTERTAINMENT_SOCIAL: "SPORTS_ENTERTAINMENT_SOCIAL",
  PROJECT_CREW_EXTENDED_GROUP: "PROJECT_CREW_EXTENDED_GROUP",
  UNIVERSITY_EDUCATION: "UNIVERSITY_EDUCATION",
  TOUR_DMC_INCENTIVE: "TOUR_DMC_INCENTIVE",
  PROCUREMENT_RFP: "PROCUREMENT_RFP",
});

export const DEMAND_ENGINE_TAXONOMY = Object.freeze({
  [DEMAND_ENGINE.CORPORATE]: {
    label: "Corporate",
    subsegments: [
      "leadership_meeting",
      "board_meeting",
      "sales_kickoff",
      "training",
      "product_launch",
      "internal_conference",
      "ma_integration",
      "regional_meeting",
      "offsite",
      "client_event",
    ],
  },
  [DEMAND_ENGINE.ASSOCIATION_NGO]: {
    label: "Association / NGO",
    subsegments: [
      "congress",
      "annual_meeting",
      "committee_meeting",
      "board_meeting",
      "chapter_meeting",
      "training",
      "delegation",
      "overflow",
      "rotating_conference",
    ],
  },
  [DEMAND_ENGINE.PHARMA_LIFE_SCIENCES]: {
    label: "Pharma / Life Sciences",
    subsegments: [
      "investigator_meeting",
      "advisory_board",
      "clinical_kickoff",
      "medical_congress",
      "training",
      "launch_meeting",
      "research_consortium",
      "regional_sales_meeting",
    ],
  },
  [DEMAND_ENGINE.TECH_FINANCIAL_PROFESSIONAL_SERVICES]: {
    label: "Tech / Financial / Professional Services",
    subsegments: [
      "tech_summit",
      "partner_conference",
      "analyst_day",
      "banking_offsite",
      "consulting_retreat",
      "legal_conference",
      "fintech_meetup",
      "training",
    ],
  },
  [DEMAND_ENGINE.GOVERNMENT_INTL_ORGANIZATIONS]: {
    label: "Government / International Organizations",
    subsegments: [
      "diplomatic_meeting",
      "agency_conference",
      "un_ecosystem",
      "ngo_summit",
      "delegation",
      "working_group",
      "procurement_related",
    ],
  },
  [DEMAND_ENGINE.SPORTS_ENTERTAINMENT_SOCIAL]: {
    label: "Sports / Entertainment / Social",
    subsegments: [
      "team",
      "officials_referees",
      "production_crew",
      "broadcast_crew",
      "tournament",
      "training_camp",
      "festival_crew",
      "cultural_group",
      "wedding",
      "celebration",
    ],
  },
  [DEMAND_ENGINE.PROJECT_CREW_EXTENDED_GROUP]: {
    label: "Project Crew / Extended Group",
    subsegments: [
      "construction",
      "engineering",
      "commissioning",
      "infrastructure",
      "consultants",
      "aviation_crew",
      "relocation",
      "temporary_workforce",
    ],
  },
  [DEMAND_ENGINE.UNIVERSITY_EDUCATION]: {
    label: "University / Education",
    subsegments: [
      "academic_conference",
      "executive_education",
      "summer_program",
      "visiting_faculty",
      "research_consortium",
      "graduation",
      "international_cohort",
      "board_meeting",
      "faculty_retreat",
    ],
  },
  [DEMAND_ENGINE.TOUR_DMC_INCENTIVE]: {
    label: "Tour / DMC / Incentive",
    subsegments: [
      "incentive_group",
      "dmc_program",
      "luxury_tour_series",
      "pre_post_cruise",
      "wedding_travel",
      "specialist_operator",
    ],
  },
  [DEMAND_ENGINE.PROCUREMENT_RFP]: {
    label: "Procurement / RFP",
    subsegments: [
      "government_tender",
      "university_tender",
      "ngo_tender",
      "association_rfp",
      "sports_accommodation_bid",
      "lodging_tender",
      "crew_accommodation_contract",
      "conference_accommodation_tender",
    ],
  },
});

export const COVERAGE_STATE = Object.freeze({
  NOT_RESEARCHED: "NOT_RESEARCHED",
  LIGHT: "LIGHT",
  ADEQUATE: "ADEQUATE",
  DEEP: "DEEP",
  SATURATED: "SATURATED",
});

/** Default engines for a hotel archetype (advisory priority, not exclusion). */
export function defaultEnginesForHotel(hotelKey = "") {
  const k = String(hotelKey).toUpperCase();
  if (k === "YOTEL") {
    return [
      DEMAND_ENGINE.ASSOCIATION_NGO,
      DEMAND_ENGINE.GOVERNMENT_INTL_ORGANIZATIONS,
      DEMAND_ENGINE.PHARMA_LIFE_SCIENCES,
      DEMAND_ENGINE.CORPORATE,
      DEMAND_ENGINE.UNIVERSITY_EDUCATION,
      DEMAND_ENGINE.TECH_FINANCIAL_PROFESSIONAL_SERVICES,
      DEMAND_ENGINE.PROCUREMENT_RFP,
      DEMAND_ENGINE.SPORTS_ENTERTAINMENT_SOCIAL,
      DEMAND_ENGINE.PROJECT_CREW_EXTENDED_GROUP,
      DEMAND_ENGINE.TOUR_DMC_INCENTIVE,
    ];
  }
  if (k === "SPICE" || k === "CAMBRIDGE") {
    return [
      DEMAND_ENGINE.TOUR_DMC_INCENTIVE,
      DEMAND_ENGINE.CORPORATE,
      DEMAND_ENGINE.ASSOCIATION_NGO,
      DEMAND_ENGINE.SPORTS_ENTERTAINMENT_SOCIAL,
      DEMAND_ENGINE.PROCUREMENT_RFP,
      DEMAND_ENGINE.UNIVERSITY_EDUCATION,
      DEMAND_ENGINE.PHARMA_LIFE_SCIENCES,
      DEMAND_ENGINE.PROJECT_CREW_EXTENDED_GROUP,
      DEMAND_ENGINE.GOVERNMENT_INTL_ORGANIZATIONS,
      DEMAND_ENGINE.TECH_FINANCIAL_PROFESSIONAL_SERVICES,
    ];
  }
  if (k === "AC") {
    return [
      DEMAND_ENGINE.ASSOCIATION_NGO,
      DEMAND_ENGINE.CORPORATE,
      DEMAND_ENGINE.UNIVERSITY_EDUCATION,
      DEMAND_ENGINE.PROCUREMENT_RFP,
      DEMAND_ENGINE.SPORTS_ENTERTAINMENT_SOCIAL,
      DEMAND_ENGINE.GOVERNMENT_INTL_ORGANIZATIONS,
      DEMAND_ENGINE.PHARMA_LIFE_SCIENCES,
      DEMAND_ENGINE.TECH_FINANCIAL_PROFESSIONAL_SERVICES,
      DEMAND_ENGINE.PROJECT_CREW_EXTENDED_GROUP,
      DEMAND_ENGINE.TOUR_DMC_INCENTIVE,
    ];
  }
  // NOW NOW / urban boutique
  return [
    DEMAND_ENGINE.CORPORATE,
    DEMAND_ENGINE.TECH_FINANCIAL_PROFESSIONAL_SERVICES,
    DEMAND_ENGINE.ASSOCIATION_NGO,
    DEMAND_ENGINE.UNIVERSITY_EDUCATION,
    DEMAND_ENGINE.PHARMA_LIFE_SCIENCES,
    DEMAND_ENGINE.PROCUREMENT_RFP,
    DEMAND_ENGINE.SPORTS_ENTERTAINMENT_SOCIAL,
    DEMAND_ENGINE.GOVERNMENT_INTL_ORGANIZATIONS,
    DEMAND_ENGINE.PROJECT_CREW_EXTENDED_GROUP,
    DEMAND_ENGINE.TOUR_DMC_INCENTIVE,
  ];
}

export function allDemandEngines() {
  return Object.values(DEMAND_ENGINE);
}
