/**
 * Leaf RESEARCH_SEGMENT taxonomy — no imports.
 * Breaks circular TDZ between hotel-segment-research-plan ↔ research-constants /
 * event-to-demand consumers. Re-exported by hotel-segment-research-plan-v1.js.
 */

export const RESEARCH_SEGMENT = Object.freeze({
  ASSOCIATIONS_GOVERNMENT: "associations_government",
  SPORTS: "sports",
  CORPORATE_MEETINGS_TRAINING: "corporate_meetings_training",
  LOCAL_MEETINGS_CATERING: "local_meetings_catering",
  SCALING_COMPANIES: "scaling_companies",
  MEDICAL_ACADEMIC: "medical_academic",
  WEDDINGS_SOCIAL_PRIVATE_EVENTS: "weddings_social_private_events",
  ENTERTAINMENT_PRODUCTION: "entertainment_production",
  EXHIBITOR_SPONSOR_TEAMS: "exhibitor_sponsor_teams",
  TOURS_EDUCATIONAL_SPECIALIST: "tours_educational_specialist",
  PROFESSIONAL_PLANNERS_DMC: "professional_planners_dmc",
});

export const RESEARCH_SEGMENT_LABEL = Object.freeze({
  [RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT]: "Associations / government",
  [RESEARCH_SEGMENT.SPORTS]: "Sports",
  [RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING]:
    "Corporate meetings, training, and project teams",
  [RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING]:
    "Local Meetings & Catering (incl. no-guestroom)",
  [RESEARCH_SEGMENT.SCALING_COMPANIES]:
    "Scaling Companies / Emerging Corporate Accounts",
  [RESEARCH_SEGMENT.MEDICAL_ACADEMIC]: "Medical / academic",
  [RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS]:
    "Weddings, social groups, and nearby private-event venues",
  [RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION]: "Entertainment / production crews",
  [RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS]: "Exhibitor / sponsor teams",
  [RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST]:
    "Tours / educational / specialist groups",
  [RESEARCH_SEGMENT.PROFESSIONAL_PLANNERS_DMC]: "Professional planners / DMCs",
});
