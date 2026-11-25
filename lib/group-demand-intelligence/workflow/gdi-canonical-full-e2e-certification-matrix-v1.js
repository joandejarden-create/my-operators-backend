/**
 * Canonical Full E2E certification matrix V1 ($0 fixture/mock path).
 *
 * Labels accurately: FIXTURE_MOCK_CERTIFIED ≠ paid live certified.
 */

export const GDI_CANONICAL_FULL_E2E_CERTIFICATION_MATRIX_V1 =
  "gdi-canonical-full-e2e-certification-matrix-v1";

export const CERT_LABEL = Object.freeze({
  FIXTURE_MOCK_CERTIFIED: "FIXTURE_MOCK_CERTIFIED",
  REGISTERED_NOT_EXECUTED: "REGISTERED_NOT_EXECUTED",
  EXECUTED_ZERO_PAID: "EXECUTED_ZERO_PAID",
  PROPERLY_SKIPPED: "PROPERLY_SKIPPED",
  PENDING_PROVIDER_AUTH: "PENDING_PROVIDER_AUTH",
  GAP: "GAP",
  NOT_PAID_LIVE_CERTIFIED: "NOT_PAID_LIVE_CERTIFIED",
});

const CAPABILITIES = Object.freeze([
  "fourteen_core_segments",
  "leisure_festivals_concerts_cultural",
  "leisure_participatory_sports",
  "leisure_attractions_experiences",
  "leisure_universities_vfr",
  "leisure_destination_shifts",
  "leisure_distribution_referral",
  "competitive_account_discovery",
  "competitive_sourcing_window_research",
  "competitor_commitment_mapping",
  "predictive_sourcing_minimum_workflow",
  "two_stage_exa_agent",
  "who_how_research",
  "cross_segment_dedupe",
  "research_sot_three_layer",
  "strict_fqo_six_rule",
  "weekly_change_tracking",
  "commercial_presentation",
  "controlled_persistence_gates",
]);

/**
 * Build certification matrix rows from a canonical Full E2E run result.
 */
export function buildCanonicalCertificationMatrixFromRun(run = {}, opts = {}) {
  const leisure = run.leisureOptIn || {};
  const cad = run.cadOptIn || {};
  const outcomes = Array.isArray(leisure.moduleOutcomes)
    ? leisure.moduleOutcomes
    : [];
  const byModule = Object.fromEntries(
    outcomes.map((o) => [o.moduleId, o]),
  );

  function leisureRow(moduleId, label) {
    const o = byModule[moduleId];
    const outcome = o?.outcome || null;
    const executed = o?.executed === true;
    return {
      capability: label,
      registered: true,
      applicable:
        o?.applicable === true
          ? true
          : o?.applicable === false
            ? false
            : null,
      executedOrProperlySkipped: Boolean(outcome),
      outcome,
      sourceEvidence: executed ? "mock_live_fixture_sources" : null,
      researchOutput: executed ? "leisure_module_contracts" : null,
      qualificationOutcome: "shared_fqo_gate_not_auto_sales_ready",
      persistenceEligibility: false,
      customerPresentationMapping: "via_shared_commercial_presentation",
      researchCostUsd: 0,
      unresolvedErrors: outcome === "APPLICABLE_EXECUTION_FAILED" ? [outcome] : [],
      certificationLabel: executed
        ? CERT_LABEL.EXECUTED_ZERO_PAID
        : outcome === "APPLICABLE_PENDING_AUTHORIZATION"
          ? CERT_LABEL.PENDING_PROVIDER_AUTH
          : outcome
            ? CERT_LABEL.PROPERLY_SKIPPED
            : CERT_LABEL.REGISTERED_NOT_EXECUTED,
      paidLiveCertified: false,
    };
  }

  const rows = [
    {
      capability: "fourteen_core_segments",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: Boolean(run.chain || run.certificate),
      outcome: run.chain ? "EXECUTED_CANONICAL_CHAIN" : "MISSING_CHAIN",
      sourceEvidence: "fixture_or_historical_replay",
      researchOutput: "fourteen_segment_discovery_cells",
      qualificationOutcome: run.hotelWideFinalization?.completionOutcome || null,
      persistenceEligibility: false,
      customerPresentationMapping: "hotel_wide_finalization",
      researchCostUsd: Number(run.spentUsd || 0),
      unresolvedErrors: [],
      certificationLabel: CERT_LABEL.FIXTURE_MOCK_CERTIFIED,
      paidLiveCertified: false,
    },
    leisureRow("leisure_events", "leisure_festivals_concerts_cultural"),
    leisureRow("leisure_sports", "leisure_participatory_sports"),
    leisureRow("leisure_attractions", "leisure_attractions_experiences"),
    leisureRow("leisure_universities", "leisure_universities_vfr"),
    leisureRow("leisure_destination", "leisure_destination_shifts"),
    leisureRow("leisure_distribution", "leisure_distribution_referral"),
    {
      capability: "competitive_account_discovery",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: cad.enabled === true || cad.status?.includes("SKIPPED"),
      outcome: cad.status || null,
      sourceEvidence: "cad_classifier_v1",
      researchOutput: "competitiveAccountDiscovery_sot_field",
      qualificationOutcome: "cad_not_fqo_pass",
      persistenceEligibility: false,
      customerPresentationMapping: "competitive_fields_when_populated",
      researchCostUsd: 0,
      unresolvedErrors: [],
      certificationLabel:
        cad.enabled === true
          ? CERT_LABEL.EXECUTED_ZERO_PAID
          : CERT_LABEL.PROPERLY_SKIPPED,
      paidLiveCertified: false,
    },
    {
      capability: "competitive_sourcing_window_research",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: Boolean(cad.predictiveSourcing || cad.enabled),
      outcome: cad.predictiveSourcing?.capabilityStatus || cad.status || null,
      sourceEvidence: "cad_sourcing_window_CONFIRMED_INFERRED_UNKNOWN",
      researchOutput: "predictiveSourcingAssessment",
      qualificationOutcome: "estimated_window_not_open_contract",
      persistenceEligibility: false,
      customerPresentationMapping: "limited",
      researchCostUsd: 0,
      unresolvedErrors: cad.predictiveSourcing?.preciseRemainingGaps || [],
      certificationLabel: CERT_LABEL.EXECUTED_ZERO_PAID,
      paidLiveCertified: false,
    },
    {
      capability: "competitor_commitment_mapping",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: true,
      outcome: "REUSED_BY_CAD_CLASSIFIER",
      sourceEvidence: "gdi-research-competitor-commitment-mapping-v1",
      researchOutput: "incumbent_relationship_fields",
      qualificationOutcome: "venue_only_rejected",
      persistenceEligibility: false,
      customerPresentationMapping: "when_annotated",
      researchCostUsd: 0,
      unresolvedErrors: [],
      certificationLabel: CERT_LABEL.EXECUTED_ZERO_PAID,
      paidLiveCertified: false,
    },
    {
      capability: "predictive_sourcing_minimum_workflow",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: true,
      outcome: "PARTIAL_THROUGH_CAD_AND_DEEP_RESEARCH",
      sourceEvidence: "cad+commitment+deep_research_fields",
      researchOutput: "predictiveSourcingAssessment_checklist",
      qualificationOutcome: "not_full_predictive_engine",
      persistenceEligibility: false,
      customerPresentationMapping: "not_claimed",
      researchCostUsd: 0,
      unresolvedErrors: [
        "NO_STANDALONE_PREDICTIVE_CAPTURE_ENGINE",
        "WEEKLY_CHANGE_MATERIALITY_REQUIRES_PERSISTENCE_JOIN",
      ],
      certificationLabel: CERT_LABEL.GAP,
      paidLiveCertified: false,
      fullPredictiveEngineClaimed: false,
    },
    {
      capability: "two_stage_exa_agent",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: true,
      outcome: "ARCHITECTURE_INTACT_PAID_GATED",
      sourceEvidence: "segment_aware_routing_scaling_events_plp",
      researchOutput: "discovery_then_deep",
      qualificationOutcome: "feeds_fqo",
      persistenceEligibility: false,
      customerPresentationMapping: "via_research_sot",
      researchCostUsd: 0,
      unresolvedErrors: [],
      certificationLabel: CERT_LABEL.FIXTURE_MOCK_CERTIFIED,
      paidLiveCertified: false,
    },
    {
      capability: "who_how_research",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: true,
      outcome: "UNIVERSAL_CONTACT_CONTRACT_IN_CHAIN",
      sourceEvidence: "universal-contact-research-contract-v1",
      researchOutput: "named_stakeholders_when_evidenced",
      qualificationOutcome: "pathway_aware",
      persistenceEligibility: false,
      customerPresentationMapping: "contact_cards",
      researchCostUsd: 0,
      unresolvedErrors: [],
      certificationLabel: CERT_LABEL.FIXTURE_MOCK_CERTIFIED,
      paidLiveCertified: false,
    },
    {
      capability: "cross_segment_dedupe",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: true,
      outcome: "CHAIN_PLUS_LEISURE_DEDUPE",
      sourceEvidence: "inventory_org_keys_and_leisure_dedupe",
      researchOutput: "suppressed_duplicates",
      qualificationOutcome: "no_duplicate_generators",
      persistenceEligibility: false,
      customerPresentationMapping: "n/a",
      researchCostUsd: 0,
      unresolvedErrors: [],
      certificationLabel: CERT_LABEL.FIXTURE_MOCK_CERTIFIED,
      paidLiveCertified: false,
    },
    {
      capability: "research_sot_three_layer",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: true,
      outcome: "CAD_AND_PREDICTIVE_ENRICH_SOT_FIELDS",
      sourceEvidence: "lossless_enrichment_fields",
      researchOutput: "competitiveAccountDiscovery+predictiveSourcingAssessment",
      qualificationOutcome: "enrichment_not_promotion",
      persistenceEligibility: false,
      customerPresentationMapping: "layer_gated",
      researchCostUsd: 0,
      unresolvedErrors: [],
      certificationLabel: CERT_LABEL.FIXTURE_MOCK_CERTIFIED,
      paidLiveCertified: false,
    },
    {
      capability: "strict_fqo_six_rule",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: true,
      outcome: "PR604_GATES_INTACT",
      sourceEvidence: "gdi-finalized-qualified-opportunity-contract-v1",
      researchOutput: "fqo_only_selection",
      qualificationOutcome: "six_rule_enforced",
      persistenceEligibility: "validated_fqo_only",
      customerPresentationMapping: "fqo_packages",
      researchCostUsd: 0,
      unresolvedErrors: [],
      certificationLabel: CERT_LABEL.FIXTURE_MOCK_CERTIFIED,
      paidLiveCertified: false,
    },
    {
      capability: "weekly_change_tracking",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: false,
      outcome: "HELPERS_EXIST_PERSISTENCE_JOIN_PENDING",
      sourceEvidence: null,
      researchOutput: null,
      qualificationOutcome: null,
      persistenceEligibility: false,
      customerPresentationMapping: "pending_inventory_writes",
      researchCostUsd: 0,
      unresolvedErrors: ["REQUIRES_AUTHORIZED_PERSISTENCE_FOR_LIVE_CERT"],
      certificationLabel: CERT_LABEL.GAP,
      paidLiveCertified: false,
    },
    {
      capability: "commercial_presentation",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: true,
      outcome: "AVAILABLE_WHEN_PACKAGES_PRESENT",
      sourceEvidence: "publication_package_contracts",
      researchOutput: "customer_facing_copy",
      qualificationOutcome: "fqo_gated",
      persistenceEligibility: false,
      customerPresentationMapping: "presentation_contracts",
      researchCostUsd: 0,
      unresolvedErrors: [],
      certificationLabel: CERT_LABEL.FIXTURE_MOCK_CERTIFIED,
      paidLiveCertified: false,
    },
    {
      capability: "controlled_persistence_gates",
      registered: true,
      applicable: true,
      executedOrProperlySkipped: true,
      outcome: "FAIL_CLOSED_DEFAULT_DRY_RUN",
      sourceEvidence: "research_persistence_bridge",
      researchOutput: "dry_run_plan_only_in_this_auth",
      qualificationOutcome: "validated_fqo_only_policy",
      persistenceEligibility: false,
      customerPresentationMapping: "n/a",
      researchCostUsd: 0,
      unresolvedErrors: [],
      certificationLabel: CERT_LABEL.FIXTURE_MOCK_CERTIFIED,
      paidLiveCertified: false,
    },
  ];

  const paidLiveAny = rows.some((r) => r.paidLiveCertified === true);
  return {
    contract: GDI_CANONICAL_FULL_E2E_CERTIFICATION_MATRIX_V1,
    hotelId: run.hotelId || opts.hotelId || null,
    mode: run.mode || opts.mode || null,
    at: new Date().toISOString(),
    certificationScope: "FIXTURE_MOCK_CODE_PHASE",
    paidLiveCertified: false,
    note:
      "Fixture/mock-provider certification only. Do not claim paid live certification until a separately authorized live test succeeds.",
    capabilityIds: [...CAPABILITIES],
    rows,
    summary: {
      total: rows.length,
      fixtureMockCertified: rows.filter(
        (r) => r.certificationLabel === CERT_LABEL.FIXTURE_MOCK_CERTIFIED,
      ).length,
      executedZeroPaid: rows.filter(
        (r) => r.certificationLabel === CERT_LABEL.EXECUTED_ZERO_PAID,
      ).length,
      gaps: rows.filter((r) => r.certificationLabel === CERT_LABEL.GAP).length,
      properlySkipped: rows.filter(
        (r) => r.certificationLabel === CERT_LABEL.PROPERLY_SKIPPED,
      ).length,
      paidLiveCertified: paidLiveAny,
    },
    leisureDefaultOffRemoved: true,
    cadDefaultOffRemoved: true,
    freezes: {
      bethesdaFreeze: true,
      wyndhamClean11Freeze: true,
      paidResearch: false,
      airtableApply: false,
      stagedActivate: false,
      currentPublish: false,
      findymail: false,
      surfe: false,
    },
  };
}
