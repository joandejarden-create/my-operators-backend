/**
 * Contact Intelligence V1.1 — CI12 controlled cohort (complete 12 IDs).
 * Staging / benchmark only. No production writes.
 */

export const CONTACT_V1_12_VERSION = "contact-v1-12-case-plan-v1.1";

/**
 * Distinct archetypes. Prefer ownership-known or explicitly tagged OWNER_TRUTH_UNKNOWN.
 * Alliance siblings (05/06/10) prove owner-level reuse.
 */
export const CONTACT_V1_12_CASE_PLAN = Object.freeze([
  {
    case_id: "CI12-01",
    hotel_id: "recUNycnMwOVFX0hc",
    hotel_name: "Krystal Grand Puerto Vallarta",
    owner_entity_id: "dle_06G6AB1VK0BCCD94DNN7W8DRWZ",
    owner_hint: "Grupo Hotelero Santa Fe (GSF) — BMV public hotel company",
    archetype: "public_hotel_company",
    secondary_archetypes: ["multi-hotel_ownership", "mexico_private_operator_platform"],
    country: "Mexico",
    city: "Puerto Vallarta",
    language: "es",
    goals: ["owner_reuse", "person_email", "hotel_phone", "ir_vs_person"],
    seedHints: {
      hotel_website: "https://www.krystal-hotels.com/krystal-grand-puerto-vallarta/",
      owner_website: "https://www.gsfhotels.com/",
    },
  },
  {
    case_id: "CI12-02",
    hotel_id: "recIwaP1etgx2g9nA",
    hotel_name: "Cambridge Beaches Resort & Spa",
    owner_entity_id: "ent_dovetail-hospitality",
    owner_hint: "Dovetail Hospitality",
    archetype: "caribbean_owner",
    secondary_archetypes: ["single-property_focus", "private_capital"],
    country: "Bermuda",
    city: "Sandys Parish",
    language: "en",
    goals: ["org_domain", "decision_maker", "hotel_contact"],
    seedHints: {
      hotel_website: "https://www.cambridgebeaches.com/",
      owner_website: "https://www.dovetailandco.com/",
    },
  },
  {
    case_id: "CI12-03",
    hotel_id: "recsYJb2R1jarPpK3",
    hotel_name: "Sheraton Guadalajara Expo",
    owner_entity_id: "ent_inmobiliaria-hnf",
    owner_hint: "Inmobiliaria HNF / PropCo",
    archetype: "mexico_private_propco",
    secondary_archetypes: ["offshore_private_structure", "branded_hotel"],
    country: "Mexico",
    city: "Guadalajara",
    language: "es",
    goals: ["ownership_path", "org_contact", "no_false_person", "reject_former"],
    seedHints: {
      hotel_website:
        "https://www.marriott.com/en-us/hotels/gdlse-sheraton-guadalajara-expo/overview/",
    },
  },
  {
    case_id: "CI12-04",
    hotel_id: "recGZZCek9vDQGG1L",
    hotel_name: "voco Guadalajara Expo",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_hint: "Alliance Hotel Management (platform) — verify vs PropCo",
    archetype: "third_party_managed",
    secondary_archetypes: ["branded_hotel"],
    country: "Mexico",
    city: "Guadalajara",
    language: "es",
    owner_truth_status: "PARTIAL",
    goals: ["hotel_vs_owner_separation", "brand_person_reject"],
    seedHints: {},
  },
  {
    case_id: "CI12-05",
    hotel_id: "recTYaiA4S6fR6ixx",
    hotel_name: "Real Inn Cancún",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_hint: "Alliance Hotel Management",
    archetype: "multi_hotel_ownership",
    secondary_archetypes: ["us_owner_cala_asset"],
    country: "Mexico",
    city: "Cancún",
    language: "es",
    goals: ["owner_reuse_across_real_inn", "decision_maker"],
    seedHints: {},
  },
  {
    case_id: "CI12-06",
    hotel_id: "rec79Xs4mZkuiWnuN",
    hotel_name: "Real Inn Ciudad Juárez",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_hint: "Alliance Hotel Management",
    archetype: "portfolio_reuse_sibling",
    secondary_archetypes: ["multi_hotel_ownership"],
    country: "Mexico",
    city: "Ciudad Juárez",
    language: "es",
    owner_truth_status: "PARTIAL",
    goals: ["prove_owner_level_reuse_not_hotel_by_hotel"],
    seedHints: {},
  },
  {
    case_id: "CI12-07",
    hotel_id: "rec19X4tsCUM1A2q6",
    hotel_name: "Barceló México Reforma",
    owner_entity_id: "ent_barcelo-grupo",
    owner_hint: "Barceló Grupo (international hotel company) — economic owner PARTIAL",
    archetype: "institutional_owner",
    secondary_archetypes: ["public_hotel_company", "branded_hotel"],
    country: "Mexico",
    city: "Mexico City",
    language: "es",
    owner_truth_status: "PARTIAL",
    goals: ["ir_mailbox_vs_person", "official_filings", "brand_vs_owner"],
    seedHints: {
      hotel_website: "https://www.barcelo.com/es-mx/barcelo-mexico-reforma/",
      owner_website: "https://www.barcelogrupo.com/",
    },
  },
  {
    case_id: "CI12-08",
    hotel_id: "recL4PrLJpwXxyvV6",
    hotel_name: "Beach Palace All Inclusive",
    owner_entity_id: "ent_the-palace-company",
    owner_hint: "The Palace Company / Palace Resorts — family / private capital (PARTIAL)",
    archetype: "family_office_private_capital",
    secondary_archetypes: ["multi_hotel_ownership"],
    country: "Mexico",
    city: "Cancún",
    language: "es",
    owner_truth_status: "PARTIAL",
    goals: ["decision_maker_relevance", "reject_hr_marketing"],
    seedHints: {
      hotel_website: "https://beach.palaceresorts.com/en",
      owner_website: "https://www.palaceresorts.com/",
    },
  },
  {
    case_id: "CI12-09",
    hotel_id: "rec2ossLX1BBaaZuw",
    hotel_name: "Adhara Express",
    owner_entity_id: "ent_peninsular-de-hoteles",
    owner_hint: "Peninsular de Hoteles / Grupo Peninsular — owner-operated platform (PARTIAL)",
    archetype: "owner_operated",
    secondary_archetypes: ["independent_brand_platform"],
    country: "Mexico",
    city: null,
    language: "es",
    owner_truth_status: "PARTIAL",
    goals: ["hotel_equals_owner_path", "registry_mailbox_not_person"],
    seedHints: {},
  },
  {
    case_id: "CI12-10",
    hotel_id: "recFspIiglYxJp1N1",
    hotel_name: "Real Inn San Luis Potosí",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_hint: "Alliance Hotel Management — US-based owner platform with CALA assets",
    archetype: "us_owner_cala_asset",
    secondary_archetypes: ["multi_hotel_ownership", "portfolio_reuse_sibling"],
    country: "Mexico",
    city: "San Luis Potosí",
    language: "es",
    owner_truth_status: "PARTIAL",
    goals: ["cross_border_phone_e164", "owner_reuse", "domain_resolution"],
    seedHints: {},
  },
  {
    case_id: "CI12-11",
    hotel_id: "recxjhraXKAz0BaVH",
    hotel_name: "Amberes 64, an Ascend Collection Hotel",
    owner_entity_id: "ent_suites-amberes",
    owner_hint: "SUITES AMBERES — single-property / independent (PARTIAL registered business)",
    archetype: "single_hotel_independent",
    secondary_archetypes: ["branded_soft_brand", "owner_operated"],
    country: "Mexico",
    city: "Mexico City",
    language: "es",
    owner_truth_status: "PARTIAL",
    goals: ["no_over_enrich", "hotel_contact_first"],
    seedHints: {},
  },
  {
    case_id: "CI12-12",
    hotel_id: "recVTH9bA98rdzJ1A",
    hotel_name: "AVA Resort Cancún",
    owner_entity_id: null,
    owner_hint: "CORPORACION INMOBILIARIA KTRC registered — economic owner unresolved",
    archetype: "offshore_private_structure",
    secondary_archetypes: ["mexico_private_propco"],
    country: "Mexico",
    city: "Cancún",
    language: "es",
    owner_truth_status: "OWNER_TRUTH_UNKNOWN",
    registered_business_hint: "CORPORACION INMOBILIARIA KTRC",
    goals: ["abstain_when_uncertain", "no_false_verified_email", "no_false_person"],
    seedHints: {},
  },
]);

export function summarize12CasePlan() {
  return {
    version: CONTACT_V1_12_VERSION,
    n_cases: CONTACT_V1_12_CASE_PLAN.length,
    seeded_hotel_ids: CONTACT_V1_12_CASE_PLAN.filter((c) => c.hotel_id).length,
    tbd_slots: CONTACT_V1_12_CASE_PLAN.filter((c) => !c.hotel_id).length,
    owner_truth_unknown: CONTACT_V1_12_CASE_PLAN.filter(
      (c) => c.owner_truth_status === "OWNER_TRUTH_UNKNOWN"
    ).length,
    live_execution: "STAGING_ONLY_CI12",
    production_auto_writes: false,
    outreach: false,
    success_criteria: [
      "cohort_complete_12",
      "truth_frozen_before_live",
      "owner_resolver_single_path",
      "verification_fail_closed",
      "zero_wrong_person_critical",
      "zero_false_verified_email",
      "owner_reuse_proven",
    ],
  };
}

export function getCi12Case(caseId) {
  return CONTACT_V1_12_CASE_PLAN.find((c) => c.case_id === caseId) || null;
}

export function allianceReuseHotelIds() {
  return CONTACT_V1_12_CASE_PLAN.filter(
    (c) => c.owner_entity_id === "ent_alliance-hotel-management"
  ).map((c) => c.hotel_id);
}
