/**
 * GDI 2.0 Phase 3B — Corporate Projects & Extended Stay constants.
 */

export const CORPORATE_PROJECTS_EXTENDED_STAY_V1 =
  "gdi-corporate-projects-extended-stay-v1";

export const CPE_RESEARCH_SEGMENT = "corporate_projects_extended_stay";

export const CPE_DEMAND_CATEGORY = Object.freeze({
  TECHNOLOGY_IMPLEMENTATIONS: "TECHNOLOGY_IMPLEMENTATIONS",
  CONSULTING_ADVISORY_DEPLOYMENTS: "CONSULTING_ADVISORY_DEPLOYMENTS",
  CONSTRUCTION_ENGINEERING_PROJECTS: "CONSTRUCTION_ENGINEERING_PROJECTS",
  FACILITIES_COMMISSIONING_MAINTENANCE: "FACILITIES_COMMISSIONING_MAINTENANCE",
  DATACENTER_INFRASTRUCTURE_PROJECTS: "DATACENTER_INFRASTRUCTURE_PROJECTS",
  NEW_OFFICE_FACILITY_OPENINGS: "NEW_OFFICE_FACILITY_OPENINGS",
  INDUSTRIAL_EQUIPMENT_INSTALLATIONS: "INDUSTRIAL_EQUIPMENT_INSTALLATIONS",
  GOVERNMENT_CONTRACT_MOBILIZATIONS: "GOVERNMENT_CONTRACT_MOBILIZATIONS",
  FIELD_SERVICE_TECHNICAL_TEAMS: "FIELD_SERVICE_TECHNICAL_TEAMS",
  TEMPORARY_PROJECT_RELOCATIONS: "TEMPORARY_PROJECT_RELOCATIONS",
});

export const CPE_FULL_E2E_STAGES = Object.freeze([
  "hotel_profile",
  "independent_segment_discovery",
  "candidate_identity_resolution",
  "demand_investigation",
  "buyer_research",
  "hotel_matching",
  "shared_qualification",
  "research_continuation_or_shadow_sales_ready",
  "evidence_backed_opportunity_brief",
  "checkpoint_persist_local_only",
]);

export const CPE_PILOT_LIMITS = Object.freeze({
  maxHotels: 5,
  maxCandidatesPerHotel: 10,
  maxExternalRetrievalsTotal: 60,
  authorizePaidResearch: false,
  authorizePublication: false,
  authorizeProductionWrites: false,
  allowRender: false,
});

/** Awarded contract / announced project alone ≠ accommodation demand. */
export const CPE_INSUFFICIENT_DEMAND_INDICATORS = Object.freeze([
  "awarded_contract_alone",
  "announced_project_alone_without_personnel_or_travel",
  "local_only_crew_without_nonlocal_evidence",
  "remote_delivery_only",
]);
