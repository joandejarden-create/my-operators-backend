/**
 * Packet 2.6C — versioned research template registry.
 * Customer product = Dealality Deep Research (not Webhound).
 */

export const TEMPLATE_REGISTRY_VERSION = "hotel-intelligence-research-templates-v1";

const SHARED_FUTURE_PRICING = Object.freeze({
  estimated_research_units: null,
  future_credit_cost: null,
  pricing_tier_key: null,
});

/** Future monetization slot — null until credits/pricing ship. */
const DEFAULT_CONFIRMATION_PRICING = null;

const DEFAULT_CONFIRMATION_DELIVERABLES = Object.freeze([
  "Research Addendum",
  "Evidence-backed findings",
  "Sources & open questions",
  "Downloadable PDF",
  "Archived research history",
]);

const DEFAULT_CONFIRMATION_INTRO =
  "Dealality will conduct a new evidence-backed investigation of this hotel and add the completed research to its Research Archive.";

function scopeFromLabels(labels) {
  return (labels || []).map((label) =>
    typeof label === "string"
      ? { title: label, detail: "" }
      : {
          title: String(label?.title || label?.label || "").trim(),
          detail: String(label?.detail || label?.description || "").trim(),
        }
  );
}

/**
 * Customer-facing confirmation modal fields (no provider/$ cost language).
 */
export function buildConfirmationFields(template) {
  if (!template) return null;
  const scope =
    Array.isArray(template.confirmation_scope) && template.confirmation_scope.length
      ? scopeFromLabels(template.confirmation_scope)
      : scopeFromLabels(template.scope_labels || template.research_lanes || []);
  return {
    confirmation_eyebrow: template.confirmation_eyebrow || "Deep Research",
    confirmation_title:
      template.confirmation_title || `Start ${template.display_name}?`,
    confirmation_summary: template.confirmation_summary || DEFAULT_CONFIRMATION_INTRO,
    confirmation_scope: scope,
    confirmation_deliverables:
      Array.isArray(template.confirmation_deliverables) &&
      template.confirmation_deliverables.length
        ? template.confirmation_deliverables.slice()
        : DEFAULT_CONFIRMATION_DELIVERABLES.slice(),
    // Future: { label, credits, balance, included } — keep slot, never expose now.
    confirmation_pricing:
      template.confirmation_pricing === undefined
        ? DEFAULT_CONFIRMATION_PRICING
        : template.confirmation_pricing,
  };
}

function baseTemplate(partial) {
  return {
    required_inputs: ["hotel_id", "hotel_name"],
    optional_inputs: ["known_facts", "known_gaps", "parent_report_id"],
    research_lanes: [],
    preferred_source_classes: [],
    query_patterns: [],
    document_terms: [],
    extraction_targets: [],
    validation_rules: [],
    negative_screens: [],
    stop_conditions: [],
    escalation_conditions: [],
    ...SHARED_FUTURE_PRICING,
    default_provider_strategy: "SIMULATION",
    default_provider_budget_usd: 5,
    max_provider_budget_usd: 5,
    estimated_research_units: 5,
    report_template: "RESEARCH_ADDENDUM",
    structured_output_hooks: [
      "ownership_change",
      "brand_change",
      "operator_change",
      "financing_event",
      "renovation_event",
      "portfolio_leverage",
      "decision_authority",
      "conversion_signal",
      "development_signal",
      "recent_change",
      "research_gap",
    ],
    claim_handoff: { auto_promote: false },
    confirmation_eyebrow: "Deep Research",
    confirmation_pricing: DEFAULT_CONFIRMATION_PRICING,
    ...partial,
  };
}

export const RESEARCH_TEMPLATES = Object.freeze({
  FULL_HOTEL_INTELLIGENCE: baseTemplate({
    template_id: "FULL_HOTEL_INTELLIGENCE",
    version: "1.0.0",
    display_name: "Full Hotel Intelligence Investigation",
    customer_question: "What do we know — and what remains unresolved — about this hotel?",
    description:
      "Conduct a comprehensive evidence-backed investigation covering ownership, corporate structure, operator and brand relationships, history, key people, portfolio connections, material changes and development implications.",
    research_objective:
      "Establish the baseline Full Hotel Intelligence Investigation for a hotel with source-backed findings and explicit open questions.",
    research_lanes: [
      "ownership",
      "corporate_structure",
      "brand_operator",
      "people",
      "development_intelligence",
      "portfolio",
      "material_changes",
    ],
    preferred_source_classes: [
      "corporate_registry",
      "official_filings",
      "brand_operator_disclosures",
      "press_announcements",
      "professional_profiles",
    ],
    extraction_targets: [
      "entities",
      "relationships",
      "people",
      "events",
      "findings",
      "open_questions",
      "sources",
    ],
    validation_rules: ["source_backed_findings", "explicit_open_questions", "no_title_equals_authority"],
    negative_screens: ["invented_people", "title_as_signing_authority", "unverified_ubo"],
    stop_conditions: ["budget_exhausted", "core_lanes_complete_or_blocked"],
    escalation_conditions: ["native_coverage_gap", "hard_case_authority_gap"],
    default_provider_strategy: "SIMULATION",
    default_provider_budget_usd: 5,
    report_template: "FULL_INVESTIGATION",
    research_unit_estimate: 10,
    estimated_research_units: 10,
    pricing_tier_key: "full_investigation_v1",
    icon: "dossier",
    is_baseline: true,
  }),

  DECISION_AUTHORITY: baseTemplate({
    template_id: "DECISION_AUTHORITY",
    version: "1.0.0",
    display_name: "Decision Authority & Contact Path",
    customer_question: "Who can actually make or influence the decision?",
    description:
      "Research who appears relevant to hotel ownership, brand, operator and development decisions; identify evidence of authority where available; locate verified professional profiles and public contact channels; and distinguish influence from legal signing authority.",
    research_objective:
      "Identify people relevant to ownership, brand, operator, development and capital decisions; determine what authority can actually be evidenced; find verified professional profiles and public contact routes.",
    research_lanes: [
      "ownership_principals",
      "board_control",
      "executive_leadership",
      "development_leadership",
      "brand_partnership_leadership",
      "asset_management",
      "legal_signing_authority",
      "property_leadership",
      "public_contacts",
      "verified_profiles",
    ],
    extraction_targets: ["people", "authority_evidence", "contacts", "open_questions"],
    validation_rules: ["title_is_not_authority", "profile_url_verified_or_blank"],
    negative_screens: ["inferred_signing_authority_from_title"],
    report_template: "RESEARCH_ADDENDUM",
    research_unit_estimate: 4,
    estimated_research_units: 4,
    pricing_tier_key: "followup_decision_authority_v1",
    icon: "people",
    is_baseline: false,
    scope_labels: [
      "Ownership principals",
      "Corporate leadership",
      "Development leadership",
      "Authority evidence",
      "Professional profiles",
      "Public contact routes",
    ],
  }),

  BRAND_OPERATOR_AGREEMENT: baseTemplate({
    template_id: "BRAND_OPERATOR_AGREEMENT",
    version: "1.0.0",
    display_name: "Brand, Franchise & Operator Agreement Investigation",
    customer_question: "What is the current brand / operator agreement situation?",
    description:
      "Investigate current brand, operator, management vs franchise structure, historical affiliation, announced conversion, agreement changes, public term evidence, and reflag constraints.",
    research_objective:
      "Clarify brand/operator agreement posture including conversion, transition signals, and public evidence of terms or termination.",
    research_lanes: [
      "current_brand",
      "operator",
      "management_vs_franchise",
      "historical_affiliation",
      "announced_conversion",
      "agreement_changes",
      "public_term_evidence",
      "termination_expiration",
      "transition_signals",
      "reflag_constraints",
    ],
    extraction_targets: ["brand_events", "operator_events", "conversion_signal", "open_questions"],
    report_template: "RESEARCH_ADDENDUM",
    research_unit_estimate: 4,
    estimated_research_units: 4,
    pricing_tier_key: "followup_brand_operator_v1",
    icon: "brand",
    is_baseline: false,
    scope_labels: [
      "Current brand & operator",
      "Management vs franchise",
      "Conversion / reflag",
      "Agreement evidence",
      "Transition signals",
    ],
  }),

  OWNERSHIP_CAPITAL_EVENTS: baseTemplate({
    template_id: "OWNERSHIP_CAPITAL_EVENTS",
    version: "1.0.0",
    display_name: "Ownership & Capital Events Investigation",
    customer_question: "Are there ownership, financing or transaction events that matter?",
    description:
      "Investigate ownership changes, share transactions, JV activity, financing, refinancing, debt, capital investment, sale processes and corporate restructuring.",
    research_objective:
      "Surface material ownership and capital events with documentary evidence where public.",
    research_lanes: [
      "ownership_changes",
      "share_transactions",
      "jv_activity",
      "financing",
      "refinancing",
      "debt_liens",
      "capital_investment",
      "sale_processes",
      "corporate_restructuring",
    ],
    extraction_targets: ["ownership_change", "financing_event", "events", "open_questions"],
    report_template: "RESEARCH_ADDENDUM",
    research_unit_estimate: 4,
    estimated_research_units: 4,
    pricing_tier_key: "followup_ownership_capital_v1",
    icon: "capital",
    is_baseline: false,
    scope_labels: [
      "Ownership changes",
      "Financing & debt",
      "JV / transactions",
      "Restructuring",
    ],
  }),

  REPOSITIONING_DEVELOPMENT: baseTemplate({
    template_id: "REPOSITIONING_DEVELOPMENT",
    version: "1.0.0",
    display_name: "Repositioning & Development Investigation",
    customer_question: "Is the property being repositioned or developed?",
    description:
      "Investigate renovations, expansion, redevelopment, conversion, residences, mixed-use, capex, permits, amenity and product changes.",
    research_objective:
      "Document repositioning and development signals with planning/permit and announcement evidence where appropriate.",
    research_lanes: [
      "renovations",
      "expansion",
      "redevelopment",
      "conversion",
      "residences",
      "mixed_use",
      "capex",
      "planning_permits",
      "amenity_product_changes",
    ],
    extraction_targets: ["renovation_event", "development_signal", "conversion_signal", "open_questions"],
    report_template: "RESEARCH_ADDENDUM",
    research_unit_estimate: 4,
    estimated_research_units: 4,
    pricing_tier_key: "followup_repositioning_v1",
    icon: "development",
    is_baseline: false,
    scope_labels: [
      "Renovations & capex",
      "Expansion / redevelopment",
      "Conversion",
      "Product & amenity changes",
    ],
  }),

  OWNER_PORTFOLIO: baseTemplate({
    template_id: "OWNER_PORTFOLIO",
    version: "1.0.0",
    display_name: "Owner Portfolio & Multi-Asset Opportunity",
    customer_question: "What else can this owner / organization influence?",
    description:
      "Investigate owned and controlled hotels, JVs, managed hotels, brands, operators, markets, acquisitions, disposals and pipeline.",
    research_objective:
      "Map portfolio leverage and multi-asset opportunity around the hotel's ownership/organization.",
    research_lanes: [
      "owned_hotels",
      "controlled_hotels",
      "jvs",
      "managed_hotels",
      "brands_operators",
      "markets",
      "acquisitions_disposals",
      "development_pipeline",
    ],
    extraction_targets: ["portfolio_leverage", "entities", "events", "open_questions"],
    report_template: "RESEARCH_ADDENDUM",
    research_unit_estimate: 5,
    estimated_research_units: 5,
    pricing_tier_key: "followup_owner_portfolio_v1",
    icon: "portfolio",
    is_baseline: false,
    scope_labels: [
      "Owned / controlled hotels",
      "JVs & management",
      "Brand relationships",
      "Pipeline & transactions",
    ],
  }),

  CHANGE_OPPORTUNITY: baseTemplate({
    template_id: "CHANGE_OPPORTUNITY",
    version: "1.1.0",
    display_name: "Change & Opportunity Investigation",
    customer_question:
      "Why might this hotel be worth pursuing now — and what risks could matter?",
    short_question: "Why now — and what risks could matter?",
    description:
      "Investigate recent material changes across ownership, financing, brand, operator, leadership, renovation and development, together with recurring signals about physical condition, product investment, operating quality and management stability.",
    research_objective:
      "Identify why the hotel may be actionable now; surface physical asset/product risk, deferred product investment, operating-quality patterns, and operator/management stability signals — then produce Deal Risk Flags and What to Verify Next. Does not invent Development Signals product surfaces.",
    research_lanes: [
      "change_triggers",
      "ownership",
      "financing",
      "brand",
      "operator",
      "leadership",
      "renovation",
      "development",
      "transaction_activity",
      "market_supply",
      "physical_asset_condition",
      "product_investment",
      "deferred_capex",
      "operating_quality",
      "management_response",
      "operator_stability",
      "operator_change",
      "deal_risk_flags",
    ],
    preferred_source_classes: [
      "first_party_hotel_owner",
      "brand_operator",
      "corporate_filings",
      "press_releases",
      "trade_press",
      "local_news",
      "planning_public_records",
      "renovation_announcements",
      "transactions",
      "public_review_evidence",
      "management_responses",
      "professional_property_descriptions",
    ],
    extraction_targets: [
      "OPPORTUNITY_TRIGGER",
      "ASSET_CONDITION",
      "PRODUCT_INVESTMENT",
      "DEFERRED_MAINTENANCE",
      "CAPEX",
      "RENOVATION",
      "INFRASTRUCTURE_CONCERN",
      "OPERATING_QUALITY",
      "SERVICE_CONSISTENCY",
      "MANAGEMENT_RESPONSE",
      "MANAGEMENT_CHANGE",
      "OPERATOR_STABILITY",
      "OPERATOR_CHANGE",
      "BRAND_CONVERSION",
      "DEAL_RISK",
      "open_questions",
    ],
    validation_rules: [
      "consistent_theme_not_single_anecdote",
      "temporal_relevance_required",
      "property_identity_guard",
      "physical_vs_operating_vs_operator_change_separate",
      "no_review_counting_theater",
      "no_dollar_capex_invention",
    ],
    negative_screens: [
      "wrong_property",
      "adjacent_hotel",
      "pre_renovation_as_current",
      "single_review_as_trend",
      "brand_level_as_property",
      "operator_level_as_hotel",
      "neighbor_construction",
      "announced_renovation_assumed_completed",
      "title_as_signing_authority",
    ],
    stop_conditions: ["budget_exhausted", "core_lanes_complete_or_blocked"],
    escalation_conditions: ["persistent_multi_source_gap", "hard_case_identity_conflict"],
    report_template: "RESEARCH_ADDENDUM",
    report_sections: [
      "executive_answer",
      "opportunity_thesis",
      "asset_product_risk",
      "operating_quality_management_risk",
      "operator_management_stability",
      "deal_risk_flags",
      "what_to_verify_next",
      "open_questions",
      "sources_evidence",
    ],
    default_provider_budget_usd: 5,
    max_provider_budget_usd: 5,
    research_unit_estimate: 7,
    estimated_research_units: 7,
    future_credit_cost: null,
    pricing_tier_key: "followup_change_opportunity_v1_1",
    icon: "opportunity",
    is_baseline: false,
    lookback_months_default: 24,
    scope_labels: [
      "Why Now",
      "Asset / Product Risk",
      "Operating Risk",
      "Operator Stability",
      "Deal Risk Flags",
    ],
    scope_groups: [
      {
        id: "change_triggers",
        label: "Change & Triggers",
        items: ["Ownership", "Financing", "Brand", "Operator", "Leadership", "Development"],
      },
      {
        id: "asset_product",
        label: "Asset & Product",
        items: ["Condition", "Renovation", "Product investment", "Maintenance", "Capex signals"],
      },
      {
        id: "operations",
        label: "Operations",
        items: [
          "Service consistency",
          "Management execution",
          "Maintenance response",
          "Leadership stability",
        ],
      },
      {
        id: "risk_opportunity",
        label: "Risk & Opportunity",
        items: ["Why Now", "Deal risk flags", "Operator-change signals", "What to verify next"],
      },
    ],
    assessments: {
      product_investment: [
        "NO_MATERIAL_CONCERN",
        "NORMAL_AGE_RELATED_INVESTMENT",
        "MODERATE_DEFERRED_INVESTMENT_RISK",
        "SIGNIFICANT_PRODUCT_CAPEX_RISK",
        "INSUFFICIENT_EVIDENCE",
      ],
      operating_quality: [
        "STRONG_OPERATING_SIGNAL",
        "GENERALLY_STABLE",
        "MIXED_INCONSISTENT",
        "MATERIAL_OPERATING_CONCERN",
        "INSUFFICIENT_EVIDENCE",
      ],
      operator_change: [
        "STABLE",
        "WATCH",
        "CHANGE_SIGNAL_PRESENT",
        "ACTIVE_TRANSITION",
        "INSUFFICIENT_EVIDENCE",
      ],
      risk_severity: ["HIGH", "MODERATE", "LOW", "WATCH", "UNKNOWN"],
      theme_frequency: ["ISOLATED", "REPEATED", "PERSISTENT", "MULTI_SOURCE"],
    },
    property_identity_guard: {
      note: "For KGPV: Krystal Grand Puerto Vallarta ≠ Krystal Resort Puerto Vallarta",
    },
    confirmation_title: "Start Change & Opportunity Investigation?",
    confirmation_summary:
      "Research why this hotel may be actionable now, what has materially changed, and what physical, operating or management risks could affect the opportunity.",
    confirmation_scope: [
      {
        title: "Recent Change & Triggers",
        detail: "Ownership, financing, brand, operator and leadership developments",
      },
      {
        title: "Asset & Product Risk",
        detail: "Physical condition, renovation history, investment and capex signals",
      },
      {
        title: "Operating Quality",
        detail: "Service consistency, maintenance response and management execution",
      },
      {
        title: "Operator Stability",
        detail: "Operating-model changes, management turnover and transition signals",
      },
      {
        title: "Deal Risk",
        detail: "What could undermine the opportunity and what requires further diligence",
      },
    ],
    confirmation_deliverables: [
      "Research Addendum",
      "Source-backed findings",
      "Risk and opportunity assessment",
      "Open questions",
      "What to verify next",
      "Downloadable PDF",
    ],
  }),
});

export const FOLLOW_UP_TEMPLATE_IDS = Object.freeze([
  "DECISION_AUTHORITY",
  "BRAND_OPERATOR_AGREEMENT",
  "OWNERSHIP_CAPITAL_EVENTS",
  "REPOSITIONING_DEVELOPMENT",
  "OWNER_PORTFOLIO",
  "CHANGE_OPPORTUNITY",
]);

export function listTemplates({ includeInternal = false } = {}) {
  return Object.values(RESEARCH_TEMPLATES).map((t) => customerTemplateView(t, includeInternal));
}

export function getTemplate(templateId) {
  const id = String(templateId || "").trim().toUpperCase();
  return RESEARCH_TEMPLATES[id] || null;
}

export function customerTemplateView(template, includeInternal = false) {
  if (!template) return null;
  const confirmation = buildConfirmationFields(template);
  const view = {
    template_id: template.template_id,
    version: template.version,
    display_name: template.display_name,
    customer_question: template.customer_question,
    short_question: template.short_question || template.customer_question,
    description: template.description,
    research_objective: template.research_objective,
    scope_labels: template.scope_labels || template.research_lanes || [],
    scope_groups: template.scope_groups || null,
    report_sections: template.report_sections || null,
    assessments: template.assessments || null,
    is_baseline: Boolean(template.is_baseline),
    icon: template.icon || "research",
    report_template: template.report_template,
    confirmation_eyebrow: confirmation.confirmation_eyebrow,
    confirmation_title: confirmation.confirmation_title,
    confirmation_summary: confirmation.confirmation_summary,
    confirmation_scope: confirmation.confirmation_scope,
    confirmation_deliverables: confirmation.confirmation_deliverables,
    confirmation_pricing: confirmation.confirmation_pricing,
  };
  if (includeInternal) {
    view.internal = {
      default_provider_strategy: template.default_provider_strategy,
      default_provider_budget_usd: template.default_provider_budget_usd,
      max_provider_budget_usd: template.max_provider_budget_usd,
      estimated_research_units: template.estimated_research_units,
      future_credit_cost: template.future_credit_cost,
      pricing_tier_key: template.pricing_tier_key,
      claim_handoff: template.claim_handoff,
      extraction_targets: template.extraction_targets,
      validation_rules: template.validation_rules,
      negative_screens: template.negative_screens,
    };
  }
  return view;
}
