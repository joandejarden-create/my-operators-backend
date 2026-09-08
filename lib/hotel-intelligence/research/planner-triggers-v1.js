/**
 * Native research planner triggers — versioned, case-derived (not hotel-hardcoded).
 * Packet 2.7-R5 · from KGPV + Cambridge learnings.
 */

export const PLANNER_TRIGGERS_VERSION = "planner-triggers-v1";

export const PLANNER_TRIGGERS = Object.freeze([
  {
    id: "PRIVATE_OPAQUE_OWNER_PLAYBOOK",
    version: 1,
    created_from_case: "cambridge_beaches_webhound_2",
    when: {
      ownership_visibility: "private_or_opaque",
      jurisdiction_hints: ["offshore", "island", "bermuda", "cayman", "cayman islands"],
    },
    playbook: "PRIVATE_CROSS_BORDER_RESORT_OWNERSHIP",
    query_patterns: [
      "tourism investment order {hotel}",
      "{hotel} holdings limited",
      "{hotel} acquired OR acquisition",
      "{sponsor} owner developer operator",
    ],
    expected_source_hierarchy: [
      "government_statute_order",
      "ministerial_statement",
      "first_party_hotel",
      "first_party_owner",
      "local_press",
      "lender_press",
      "operator_portfolio",
      "professional_profiles",
      "historical_corporate_databases",
    ],
    negative_screens: ["ORG_CAPABILITY_IS_NOT_PROPERTY_RELATIONSHIP", "SPONSOR_CONTROL_IS_NOT_DEED_UBO"],
    stop_conditions: ["registry_ubo_resolved", "deed_chain_resolved"],
    escalation: ["corporate_registry", "title_search"],
  },
  {
    id: "GOVERNMENT_DEVELOPMENT_ORDER_LANE",
    version: 1,
    created_from_case: "cambridge_beaches_webhound_2",
    when: { signals: ["tourism_investment_order", "ministerial_hotel_developer", "customs_relief_order"] },
    extraction_targets: ["developer_legal_name", "incorporation_date", "owner_language", "relief_windows"],
    negative_screens: ["ORDER_APPROVAL_IS_NOT_PHYSICAL_COMPLETION"],
  },
  {
    id: "SELLER_IDENTITY_DISCREPANCY_LANE",
    version: 1,
    created_from_case: "cambridge_beaches_webhound_2",
    when: { seller_name_conflicts_with_corporate_existence_dates: true },
    follow_up: [
      "successor_entity",
      "alternate_legal_entity",
      "title_owner",
      "trust",
      "holding_company",
      "reorganization",
      "revival",
      "press_shorthand",
    ],
    negative_screens: ["DO_NOT_TREAT_DISSOLVED_ENTITY_AS_EXACT_LEGAL_SELLER_WITHOUT_QUALIFICATION"],
  },
  {
    id: "TEMPORAL_OPERATOR_RESOLUTION_LANE",
    version: 1,
    created_from_cases: ["kgpv_webhound_1", "cambridge_beaches_webhound_2"],
    evidence_classes: [
      "CURRENT_FIRST_PARTY_HOTEL",
      "CURRENT_FIRST_PARTY_OWNER",
      "CURRENT_EMPLOYMENT_SIGNAL",
      "CURRENT_OPERATOR_DISCLOSURE",
      "HISTORICAL_OPERATOR_ANNOUNCEMENT",
      "RECENT_OPERATOR_PRESS",
      "RESIDUAL_PORTFOLIO_LISTING",
      "RESIDUAL_MEDIA_LIBRARY",
      "SEARCH_SNIPPET_ONLY",
      "CONTRACTUAL_OPERATOR_EVIDENCE",
      "TERMINATION_EVIDENCE",
    ],
    rules: [
      "NO_SIMPLE_VOTE_COUNTING",
      "NO_CURRENT_OPERATOR_DISCLOSURE_WEAKENS_ACTIVE_MANAGEMENT_BUT_IS_NOT_TERMINATION",
      "RESIDUAL_PORTFOLIO_IS_AMBIGUOUS_NOT_CURRENT_PROOF",
      "SEARCH_SNIPPET_IS_DISCOVERY_ONLY",
    ],
  },
  {
    id: "DYNAMIC_PORTFOLIO_DISCOVERY_LANE",
    version: 1,
    created_from_case: "cambridge_beaches_webhound_2",
    methods: ["page_html", "linked_js_data_endpoints", "search_engine_discovery", "sub_site_pages", "media_pages", "current_press"],
    rules: ["SEARCH_SNIPPET_EQUALS_DISCOVERY_EVIDENCE_ONLY"],
  },
  {
    id: "PROFESSIONAL_PROFILE_DISCOVERY_LANE",
    version: 1,
    created_from_cases: ["kgpv_webhound_1", "cambridge_beaches_webhound_2"],
    when: { person_resolved: true, verified_profile_missing: true },
    rules: ["PERSON_ONE_CANONICAL_MODEL", "ONLY_VERIFIED_PERSON_LINKEDIN_CUSTOMER_VISIBLE"],
  },
  {
    id: "PROPERTY_FUNDAMENTALS_RESEARCH_FALLBACK",
    version: 1,
    created_from_case: "cambridge_beaches_webhound_2",
    precedence: ["census_positive", "hotel_intelligence", "validated_research_observation", "missing"],
    rules: ["ZERO_NULL_DO_NOT_BLOCK_FALLBACK", "NO_PAID_RESEARCH_JUST_TO_FILL_ROOMS"],
  },
]);

/**
 * Select applicable triggers from a lightweight hotel/research context object.
 */
export function selectPlannerTriggers(ctx = {}) {
  const hits = [];
  const juris = String(ctx.jurisdiction || ctx.country || "").toLowerCase();
  const privateOwner = Boolean(ctx.private_owner || ctx.opaque_owner);
  if (privateOwner || /bermuda|cayman|cayman|offshore|island/.test(juris)) {
    hits.push("PRIVATE_OPAQUE_OWNER_PLAYBOOK");
  }
  if (ctx.tourism_investment_order || ctx.government_development_order) {
    hits.push("GOVERNMENT_DEVELOPMENT_ORDER_LANE");
  }
  if (ctx.seller_identity_discrepancy) hits.push("SELLER_IDENTITY_DISCREPANCY_LANE");
  if (ctx.operator_ambiguous) hits.push("TEMPORAL_OPERATOR_RESOLUTION_LANE");
  if (ctx.portfolio_js_only) hits.push("DYNAMIC_PORTFOLIO_DISCOVERY_LANE");
  if (ctx.person_resolved && !ctx.verified_profile) hits.push("PROFESSIONAL_PROFILE_DISCOVERY_LANE");
  if (ctx.rooms_missing) hits.push("PROPERTY_FUNDAMENTALS_RESEARCH_FALLBACK");
  return PLANNER_TRIGGERS.filter((t) => hits.includes(t.id));
}
