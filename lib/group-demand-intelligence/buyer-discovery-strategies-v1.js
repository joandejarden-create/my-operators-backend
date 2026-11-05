/**
 * GDI 2.0 Phase 2G — reusable buyer discovery strategy library.
 *
 * Evidence-grounded strategies for existing segments. Does not execute paid tools.
 * Title resemblance alone never classifies someone as a hotel buyer — validation
 * still requires hotel-commercial remit via buyer-evidence-gate.
 */

export const BUYER_DISCOVERY_STRATEGIES_V1 = "gdi-buyer-discovery-strategies-v1";

/**
 * @typedef {object} BuyerDiscoveryStrategy
 * @property {string} id
 * @property {string} label
 * @property {string} evidenceGapAddressed
 * @property {string[]} appropriateOrganizations
 * @property {string[]} appropriateRoles
 * @property {string[]} preferredSourceTypes
 * @property {string} researchApproach
 * @property {string[]} validationRequirements
 * @property {string} contactRouteStandard
 * @property {string} switchWhen
 * @property {number} expectedInformationGain
 * @property {{paid:boolean,authorization:string,estimatedUsd:number}} costAuthorization
 * @property {string[]} segmentFamilies
 */

/** @type {BuyerDiscoveryStrategy[]} */
export const BUYER_DISCOVERY_STRATEGY_LIBRARY = Object.freeze([
  {
    id: "COMPANY_TRAVEL_PROCUREMENT",
    label: "Company travel and procurement departments",
    evidenceGapAddressed: "BUYER_IDENTITY_GAP|BUYER_RESPONSIBILITY_GAP|CONTACT_ROUTING_GAP",
    appropriateOrganizations: ["scaling corporate accounts", "multi-site employers"],
    appropriateRoles: [
      "Travel Manager",
      "Director of Travel",
      "Procurement Manager",
      "Strategic Sourcing",
      "Vendor Management",
    ],
    preferredSourceTypes: [
      "official_about_team",
      "procurement_policy_pages",
      "press_office_moves",
      "careers_org_charts_deprioritized",
    ],
    researchApproach:
      "Start from verified company domain seeds → about/team/procurement/contact. Extract named travel/procurement roles with hotel-commercial remit validation.",
    validationRequirements: [
      "named_person",
      "org_affiliation",
      "hotel_commercial_remit_via_gate",
      "no_title_resemblance_alone",
    ],
    contactRouteStandard:
      "travel@ / procurement@ / sourcing@ org routes OK when labeled not person-direct; reject recruiting@ press@",
    switchWhen: "two low-info fetches or no travel/procurement language on official pages",
    expectedInformationGain: 16,
    costAuthorization: { paid: false, authorization: "direct_public_only", estimatedUsd: 0 },
    segmentFamilies: ["scaling_companies"],
  },
  {
    id: "MEETINGS_CORPORATE_EVENTS",
    label: "Meetings and corporate events leadership",
    evidenceGapAddressed: "BUYER_IDENTITY_GAP|BUYER_RESPONSIBILITY_GAP|HOTEL_DEMAND_GAP",
    appropriateOrganizations: ["corporates with offsites", "event-heavy employers"],
    appropriateRoles: [
      "Director of Meetings",
      "Events Manager",
      "Corporate Events",
      "Conference Planner",
    ],
    preferredSourceTypes: ["events_pages", "offsite_announcements", "team_directories", "contact_us"],
    researchApproach:
      "Prefer official events/meetings pages and leadership directories over careers. Validate meetings/events job-title shape.",
    validationRequirements: ["named_person", "meetings_events_role_shape", "hotel_commercial_remit_via_gate"],
    contactRouteStandard: "events@ / meetings@ / conference@ preferred",
    switchWhen: "pages lack meetings language; only product marketing staff found",
    expectedInformationGain: 17,
    costAuthorization: { paid: false, authorization: "direct_public_only", estimatedUsd: 0 },
    segmentFamilies: ["scaling_companies", "associations_events"],
  },
  {
    id: "LEARNING_DEVELOPMENT_TRAINING",
    label: "Learning and development / training organizers",
    evidenceGapAddressed: "BUYER_IDENTITY_GAP|HOTEL_DEMAND_GAP",
    appropriateOrganizations: ["employers running cohort training", "academies"],
    appropriateRoles: [
      "Head of Learning",
      "L&D Manager",
      "Training Director",
      "People Operations (training remit)",
    ],
    preferredSourceTypes: ["training_program_pages", "academy_pages", "people_team", "press_training_investments"],
    researchApproach:
      "Look for cohort/training/academy language tied to travel or multi-day programs; do not promote HR recruiters.",
    validationRequirements: ["named_person", "training_or_lnd_remit", "demand_mechanism_classified"],
    contactRouteStandard: "learning@ / training@ / people@ (non-recruiting) OK",
    switchWhen: "only recruiting/careers mailboxes or no cohort/travel signal",
    expectedInformationGain: 14,
    costAuthorization: { paid: false, authorization: "direct_public_only", estimatedUsd: 0 },
    segmentFamilies: ["scaling_companies"],
  },
  {
    id: "PROJECT_OPS_DEPLOYMENT",
    label: "Project operations and deployment managers",
    evidenceGapAddressed: "BUYER_IDENTITY_GAP|HOTEL_DEMAND_GAP",
    appropriateOrganizations: ["implementation firms", "field services", "construction/deployments"],
    appropriateRoles: [
      "Project Manager",
      "Program Manager",
      "Deployment Manager",
      "Site Lead",
      "Implementation Manager",
    ],
    preferredSourceTypes: ["project_case_studies", "office_expansion_press", "careers_project_roles_context_only"],
    researchApproach:
      "Use expansion/deployment announcements; identify ops leads with extended-stay implications. Title match alone insufficient.",
    validationRequirements: ["named_person", "deployment_or_project_remit", "demand_mechanism_B_or_better"],
    contactRouteStandard: "projects@ / operations@ / info@ ask-for-named",
    switchWhen: "no deployment/extended-stay evidence after two fetches",
    expectedInformationGain: 15,
    costAuthorization: { paid: false, authorization: "direct_public_only", estimatedUsd: 0 },
    segmentFamilies: ["scaling_companies"],
  },
  {
    id: "ASSOCIATION_CONFERENCE_PLANNERS",
    label: "Association conference and event planners",
    evidenceGapAddressed: "BUYER_IDENTITY_GAP|BUYER_RESPONSIBILITY_GAP|CONTACT_ROUTING_GAP",
    appropriateOrganizations: ["associations", "societies", "federations", "congress organizers"],
    appropriateRoles: [
      "Meetings Director",
      "Conference Manager",
      "Congress Coordinator",
      "Event Director",
      "Directora de Eventos",
    ],
    preferredSourceTypes: [
      "official_congress_pages",
      "housing_pages",
      "committee_rosters",
      "organizer_contact",
    ],
    researchApproach:
      "Prefer organizer/housing/contact pages in event language. Reject speakers/sponsors/registration-only staff.",
    validationRequirements: ["named_person", "meetings_events_role", "reject_speaker_only"],
    contactRouteStandard: "congreso@ / eventos@ / meetings@ / housing@",
    switchWhen: "only speakers or registration staff; switch to housing agency strategy",
    expectedInformationGain: 18,
    costAuthorization: { paid: false, authorization: "direct_public_only", estimatedUsd: 0 },
    segmentFamilies: ["associations_events"],
  },
  {
    id: "HOUSING_AGENCIES_PCO",
    label: "Event housing agencies and professional congress organizers",
    evidenceGapAddressed: "CONTACT_ROUTING_GAP|HOTEL_DEMAND_GAP|BUYER_IDENTITY_GAP",
    appropriateOrganizations: ["PCOs", "housing bureaus", "destination management", "CVBs"],
    appropriateRoles: ["Housing Manager", "PCO Account Manager", "Congress Coordinator"],
    preferredSourceTypes: ["housing_provider_pages", "official_housing_links", "pco_directories"],
    researchApproach:
      "Follow official housing/PCO links from event sites; treat agency as routing entity to planners.",
    validationRequirements: ["org_route_or_named_planner", "housing_language_present"],
    contactRouteStandard: "housing@ / reservas@ / rooms@ functional routes preferred",
    switchWhen: "no housing provider linked; return to association planner strategy",
    expectedInformationGain: 16,
    costAuthorization: { paid: false, authorization: "direct_public_only", estimatedUsd: 0 },
    segmentFamilies: ["associations_events"],
  },
  {
    id: "REGIONAL_FACILITIES_OFFICE_OPS",
    label: "Regional facilities and office operations",
    evidenceGapAddressed: "BUYER_IDENTITY_GAP|BUYER_RESPONSIBILITY_GAP",
    appropriateOrganizations: ["multi-office corporates", "regional HQs"],
    appropriateRoles: [
      "Facilities Manager",
      "Workplace Manager",
      "Office Manager",
      "Real Estate Manager",
      "Site Operations",
    ],
    preferredSourceTypes: ["office_location_pages", "workplace_team", "about_locations"],
    researchApproach:
      "Map local office to facilities/workplace owners; reject product/engineering VPs.",
    validationRequirements: ["named_person", "facilities_or_workplace_remit"],
    contactRouteStandard: "facilities@ / workplace@ / office@ / info@",
    switchWhen: "leadership-only titles without facilities remit",
    expectedInformationGain: 15,
    costAuthorization: { paid: false, authorization: "direct_public_only", estimatedUsd: 0 },
    segmentFamilies: ["scaling_companies"],
  },
  {
    id: "SUPPLIER_CONTRACTOR_IMPLEMENTATION",
    label: "Supplier, contractor and implementation-company relationships",
    evidenceGapAddressed: "BUYER_IDENTITY_GAP|HOTEL_DEMAND_GAP|ORG_ACTIVITY_GAP",
    appropriateOrganizations: ["vendors", "contractors", "implementation partners on-site"],
    appropriateRoles: [
      "Account Implementation Manager",
      "Customer Deployment Lead",
      "Field Operations",
    ],
    preferredSourceTypes: ["partner_announcements", "case_studies", "customer_success_deployment_pages"],
    researchApproach:
      "When demand is supplier-driven (install teams), research the implementation company—not the end customer's product VP.",
    validationRequirements: ["correct_org_affiliation", "deployment_remit", "no_wrong_employer"],
    contactRouteStandard: "implementation@ / partners@ / projects@",
    switchWhen: "affiliation mismatch or no on-site deployment evidence",
    expectedInformationGain: 13,
    costAuthorization: { paid: false, authorization: "direct_public_only", estimatedUsd: 0 },
    segmentFamilies: ["scaling_companies", "associations_events"],
  },
]);

const GAP_TO_STRATEGIES = Object.freeze({
  BUYER_IDENTITY_GAP: [
    "MEETINGS_CORPORATE_EVENTS",
    "ASSOCIATION_CONFERENCE_PLANNERS",
    "COMPANY_TRAVEL_PROCUREMENT",
    "REGIONAL_FACILITIES_OFFICE_OPS",
    "LEARNING_DEVELOPMENT_TRAINING",
    "PROJECT_OPS_DEPLOYMENT",
  ],
  BUYER_RESPONSIBILITY_GAP: [
    "REGIONAL_FACILITIES_OFFICE_OPS",
    "MEETINGS_CORPORATE_EVENTS",
    "COMPANY_TRAVEL_PROCUREMENT",
  ],
  CONTACT_ROUTING_GAP: [
    "HOUSING_AGENCIES_PCO",
    "ASSOCIATION_CONFERENCE_PLANNERS",
    "COMPANY_TRAVEL_PROCUREMENT",
    "MEETINGS_CORPORATE_EVENTS",
  ],
  HOTEL_DEMAND_GAP: [
    "HOUSING_AGENCIES_PCO",
    "MEETINGS_CORPORATE_EVENTS",
    "PROJECT_OPS_DEPLOYMENT",
    "LEARNING_DEVELOPMENT_TRAINING",
  ],
  ORG_ACTIVITY_GAP: ["SUPPLIER_CONTRACTOR_IMPLEMENTATION", "MEETINGS_CORPORATE_EVENTS"],
});

export function getBuyerDiscoveryStrategy(id) {
  return BUYER_DISCOVERY_STRATEGY_LIBRARY.find((s) => s.id === id) || null;
}

/**
 * Select strategies for an unresolved gap + segment, skipping already attempted ids.
 */
export function selectBuyerDiscoveryStrategies({
  gapType = "BUYER_IDENTITY_GAP",
  segmentFamily = "scaling_companies",
  attemptedIds = [],
  max = 3,
} = {}) {
  const attempted = new Set((attemptedIds || []).map(String));
  const preferred = GAP_TO_STRATEGIES[gapType] || GAP_TO_STRATEGIES.BUYER_IDENTITY_GAP;
  const out = [];
  for (const id of preferred) {
    const s = getBuyerDiscoveryStrategy(id);
    if (!s) continue;
    if (attempted.has(s.id)) continue;
    if (s.segmentFamilies && !s.segmentFamilies.includes(segmentFamily) && segmentFamily !== "other") {
      // Allow cross-segment when association/scaling overlap
      if (!(segmentFamily === "associations_events" && s.segmentFamilies.includes("associations_events"))) {
        if (!s.segmentFamilies.includes(segmentFamily)) continue;
      }
    }
    if (s.costAuthorization?.paid) continue;
    out.push(s);
    if (out.length >= max) break;
  }
  return {
    version: BUYER_DISCOVERY_STRATEGIES_V1,
    gapType,
    segmentFamily,
    strategies: out,
    note: "Strategies guide source selection; promotion still requires buyer-evidence-gate BUYER_ELIGIBLE.",
  };
}

/**
 * Map discovery strategy → planner-compatible approach object.
 */
export function discoveryStrategyToApproach(strategy) {
  if (!strategy) return null;
  return {
    id: strategy.id,
    discoveryStrategyId: strategy.id,
    label: strategy.label,
    queryHints: strategy.appropriateRoles.slice(0, 5),
    source: "buyer_discovery_strategies_v1",
    paid: false,
    expectedInformationGain: strategy.expectedInformationGain,
    lowInformationRisk: false,
    preferredSourceTypes: strategy.preferredSourceTypes,
    switchWhen: strategy.switchWhen,
    validationRequirements: strategy.validationRequirements,
  };
}
