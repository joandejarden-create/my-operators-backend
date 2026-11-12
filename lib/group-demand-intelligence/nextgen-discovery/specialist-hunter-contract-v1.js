/**
 * GDI Next-Generation Discovery — specialist hunter contract V1.
 *
 * Ten modular hunters are discovery *sources*, not a fifteenth demand segment
 * and not a parallel supervisor. Each hunter maps into existing 14-workflow /
 * RESEARCH_SEGMENT / ten-bases taxonomies and enters the same qualification
 * + publication lifecycle.
 *
 * Zero-paid by default. Apify / Exa live remain fail-closed behind existing gates.
 */

export const GDI_NEXTGEN_DISCOVERY_ARCHITECTURE_V1 =
  "gdi-nextgen-discovery-architecture-v1";

/** Independently executable specialist hunters (PowerShell entry points). */
export const SPECIALIST_HUNTER = Object.freeze({
  GOVERNMENT_PROCUREMENT: "government_procurement",
  ASSOCIATION_ROTATION: "association_rotation",
  CORPORATE_MEETING_TRIGGERS: "corporate_meeting_triggers",
  CLINICAL_TRIALS_MEDICAL: "clinical_trials_medical",
  PROJECT_WORKFORCE: "project_workforce",
  SPORTS_HOUSING: "sports_housing",
  UNIVERSITY_DEMAND: "university_demand",
  LEGAL_REGULATORY: "legal_regulatory",
  EMERGENCY_DISPLACEMENT: "emergency_displacement",
  CREW_PRODUCTION: "crew_production",
});

export const SPECIALIST_HUNTER_LIST = Object.freeze(
  Object.values(SPECIALIST_HUNTER)
);

export const HUNTER_WAVE = Object.freeze({
  FIRST: "FIRST",
  SECOND: "SECOND",
  THIRD: "THIRD",
});

/**
 * Capability status vs existing GDI surface (Phase 0 audit).
 * EXISTS = reusable module already covers intent.
 * PARTIAL = intent folded into adjacent workflow; needs extension.
 * NEW = genuinely missing specialist path.
 */
export const HUNTER_CAPABILITY = Object.freeze({
  EXISTS: "EXISTS",
  PARTIAL: "PARTIAL",
  NEW: "NEW",
});

export const SPECIALIST_HUNTER_META = Object.freeze({
  [SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT]: {
    label: "Government Procurement",
    wave: HUNTER_WAVE.FIRST,
    capability: HUNTER_CAPABILITY.PARTIAL,
    workflowIds: ["government_public_sector"],
    researchSegments: ["associations_government"],
    tenBases: [],
    primaryProviders: ["public_api", "exa"],
    apifyRole: "SUPPLEMENTAL",
    notes:
      "Extend existing government_public_sector with procurement solicitations / awards watch — not a new segment.",
  },
  [SPECIALIST_HUNTER.ASSOCIATION_ROTATION]: {
    label: "Association Rotation",
    wave: HUNTER_WAVE.SECOND,
    capability: HUNTER_CAPABILITY.EXISTS,
    workflowIds: ["association_events"],
    researchSegments: ["associations_government"],
    tenBases: ["HISTORIC_ROTATION_PREDICTION"],
    primaryProviders: ["exa", "apify"],
    apifyRole: "SUPPLEMENTAL",
    notes: "Reuse ten-bases rotation + association-events runners.",
  },
  [SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS]: {
    label: "Corporate Meeting Triggers",
    wave: HUNTER_WAVE.FIRST,
    capability: HUNTER_CAPABILITY.EXISTS,
    workflowIds: ["meetings_incentives", "scaling_companies"],
    researchSegments: ["corporate_meetings_incentives", "scaling_companies"],
    tenBases: ["CORPORATE_TRIGGER_DEMAND", "RECURRING_CORPORATE_MEETINGS"],
    primaryProviders: ["exa", "apify"],
    apifyRole: "SUPPLEMENTAL",
    notes: "Reuse CORPORATE_TRIGGER_DEMAND + meetings adapters.",
  },
  [SPECIALIST_HUNTER.CLINICAL_TRIALS_MEDICAL]: {
    label: "Clinical Trials / Medical Meetings",
    wave: HUNTER_WAVE.SECOND,
    capability: HUNTER_CAPABILITY.EXISTS,
    workflowIds: ["education_scientific"],
    researchSegments: ["medical_academic"],
    tenBases: ["PHARMA_MEDICAL_ECOSYSTEM"],
    primaryProviders: ["exa", "public_api"],
    apifyRole: "SUPPLEMENTAL",
    notes: "Pharma/medical ecosystem exists; CT.gov feed is an incremental extension.",
  },
  [SPECIALIST_HUNTER.PROJECT_WORKFORCE]: {
    label: "Construction / Project Workforce",
    wave: HUNTER_WAVE.FIRST,
    capability: HUNTER_CAPABILITY.EXISTS,
    workflowIds: ["projects_infrastructure", "relocation_extended_stay"],
    researchSegments: ["corporate_meetings_training"],
    tenBases: ["PROJECT_WORKFORCE_DEMAND"],
    primaryProviders: ["exa", "public_api"],
    apifyRole: "NONE",
    notes: "Reuse PROJECT_WORKFORCE_DEMAND + projects_infrastructure workflow.",
  },
  [SPECIALIST_HUNTER.SPORTS_HOUSING]: {
    label: "Sports Housing",
    wave: HUNTER_WAVE.FIRST,
    capability: HUNTER_CAPABILITY.PARTIAL,
    workflowIds: ["sports_entertainment"],
    researchSegments: ["sports"],
    tenBases: ["SPORTS_ENTERTAINMENT_PRODUCTION"],
    primaryProviders: ["apify", "exa"],
    apifyRole: "PRIMARY",
    notes: "Apify PRIMARY for schedules; deepen stay-to-play / housing agency thesis.",
  },
  [SPECIALIST_HUNTER.UNIVERSITY_DEMAND]: {
    label: "University Demand",
    wave: HUNTER_WAVE.FIRST,
    capability: HUNTER_CAPABILITY.PARTIAL,
    workflowIds: ["education_scientific"],
    researchSegments: ["tours_educational_specialist", "medical_academic"],
    tenBases: [],
    primaryProviders: ["exa", "apify"],
    apifyRole: "SUPPLEMENTAL",
    notes: "Split university weekend/homecoming demand from medical/scientific path.",
  },
  [SPECIALIST_HUNTER.LEGAL_REGULATORY]: {
    label: "Legal / Regulatory Events",
    wave: HUNTER_WAVE.THIRD,
    capability: HUNTER_CAPABILITY.PARTIAL,
    workflowIds: ["association_events", "government_public_sector"],
    researchSegments: ["associations_government"],
    tenBases: [],
    primaryProviders: ["exa"],
    apifyRole: "NONE",
    notes: "Today association/gov overflow — specialist filter, not new segment.",
  },
  [SPECIALIST_HUNTER.EMERGENCY_DISPLACEMENT]: {
    label: "Emergency / Displacement",
    wave: HUNTER_WAVE.THIRD,
    capability: HUNTER_CAPABILITY.NEW,
    workflowIds: ["relocation_extended_stay"],
    researchSegments: ["relocation_extended_stay"],
    tenBases: [],
    primaryProviders: ["public_api", "exa"],
    apifyRole: "NONE",
    notes:
      "Distinct from competitor-displacement pursuit tactics; catastrophe/FEMA housing demand.",
  },
  [SPECIALIST_HUNTER.CREW_PRODUCTION]: {
    label: "Crew / Production",
    wave: HUNTER_WAVE.SECOND,
    capability: HUNTER_CAPABILITY.EXISTS,
    workflowIds: ["sports_entertainment"],
    researchSegments: ["entertainment_production"],
    tenBases: ["SPORTS_ENTERTAINMENT_PRODUCTION"],
    primaryProviders: ["exa", "apify"],
    apifyRole: "SUPPLEMENTAL",
    notes: "Exists under entertainment_production; keep out of Sales Ready without lodging thesis.",
  },
});

export const FIRST_WAVE_HUNTERS = Object.freeze(
  SPECIALIST_HUNTER_LIST.filter(
    (id) => SPECIALIST_HUNTER_META[id].wave === HUNTER_WAVE.FIRST
  )
);

export const SECOND_WAVE_HUNTERS = Object.freeze(
  SPECIALIST_HUNTER_LIST.filter(
    (id) => SPECIALIST_HUNTER_META[id].wave === HUNTER_WAVE.SECOND
  )
);

export const THIRD_WAVE_HUNTERS = Object.freeze(
  SPECIALIST_HUNTER_LIST.filter(
    (id) => SPECIALIST_HUNTER_META[id].wave === HUNTER_WAVE.THIRD
  )
);

export function getSpecialistHunterMeta(hunterId) {
  return SPECIALIST_HUNTER_META[String(hunterId || "")] || null;
}

export function isSpecialistHunterId(hunterId) {
  return SPECIALIST_HUNTER_LIST.includes(String(hunterId || ""));
}

/** Default research horizon for specialist hunters (months). */
export const SPECIALIST_HUNTER_HORIZON_MONTHS = 18;

/**
 * Demand hypothesis maturity — never elevate to Sales Ready from hunter alone.
 */
export const HUNTER_HYPOTHESIS_STATUS = Object.freeze({
  ORGANIZATION_ONLY: "ORGANIZATION_ONLY",
  DEMAND_TRIGGER_INFERRED: "DEMAND_TRIGGER_INFERRED",
  ACTIVITY_EVIDENCED: "ACTIVITY_EVIDENCED",
  HOTEL_THESIS_FORMED: "HOTEL_THESIS_FORMED",
  READY_FOR_ORS: "READY_FOR_ORS",
  REJECTED: "REJECTED",
});
