/**
 * ADP_FULL_E2E_V1 — versioned operating-system wrapper over existing ADP
 * measurement, fail-closed entity resolve, certification, and publish paths.
 *
 * Measurement completeness ≠ customer report certification.
 * Missing market registry → competitor analysis NOT_CERTIFIED (fail closed).
 */

import {
  ADP_PERIOD_PIPELINE_VERSION,
  ADP_PERIOD_PIPELINE_STAGES,
} from "../ai-demand-positioning/pipeline/existing-hotel-period-pipeline-v1.js";
import { ADP_COMPETITIVE_ENTITY_RESOLVE_VERSION } from "../ai-demand-positioning/metrics/adp-competitive-entity-resolve-v1.js";
import {
  PRODUCT,
  RUN_MODE,
  PUBLICATION_LIFECYCLE,
  OVERALL_RUN_STATUS,
} from "./execution-states.js";

export const ADP_FULL_E2E_WORKFLOW_ID = "ADP_FULL_E2E_V1";
export const ADP_FULL_E2E_WORKFLOW_VERSION = "adp-full-e2e-os-v1";
export const ADP_UNDERLYING_PIPELINE_VERSION = ADP_PERIOD_PIPELINE_VERSION;

export const ADP_CERTIFICATION_AXES = Object.freeze([
  "measurement_completeness",
  "observation_integrity",
  "market_identity_integrity",
  "competitive_analysis_integrity",
  "metric_integrity",
  "executive_narrative_integrity",
  "section_completeness",
  "customer_report_readiness",
]);

export const ADP_OS_STAGES = Object.freeze([
  {
    id: "hotel_identity_validation",
    required: true,
    pipelineStages: [],
    description: "Canonical hotel / ADP property identity + HPC link",
  },
  {
    id: "hpc_onboarding_enrichment",
    required: true,
    contractVersion: "HPC_FULL_ONBOARDING_CONTRACT_V1",
    description:
      "Shared Hotel Property Census resolve/create + full-field enrichment + field-semantic validation (hpc_field_semantics_cert) — identity alone is not complete; null-fill from evidence and governed overwrite of incorrect classifications via persistent HPC field-research ledger (resume-safe); fail-closed on critical identity/geo/semantic defects; IMPORTANT/ENRICHMENT backlog does not block Core monitoring",
    failClosed: true,
    module: "lib/hotel-census/hpc-full-onboarding-contract-v1.js",
    semanticModule: "lib/hotel-census/hpc-field-semantic-validation-v1.js",
    subGates: ["hpc_field_semantics_cert"],
  },
  {
    id: "market_location_validation",
    required: true,
    description: "Geography and market boundaries",
  },
  {
    id: "market_entity_registry",
    required: true,
    description:
      "Load or govern-create market entity registry — never inherit another hotel's",
    failClosed: true,
  },
  {
    id: "core_competitive_set",
    required: true,
    description: "Governed CORE peer validation",
  },
  {
    id: "scenario_universe_certification",
    required: true,
    failClosed: true,
    contractVersion: "ADP_SCENARIO_UNIVERSE_CERTIFICATION_V1",
    measurementFamily: "CORE_ADP",
    description:
      "Fail-closed Core ADP: approved methodology version, STANDARD density (60–65 / target 63) or disclosed EXPANDED market tier, unique scenarios, category distribution, manifest hash, expected provider observations — must pass before paid Core measurement",
    module: "lib/ai-demand-positioning/certification/adp-scenario-universe-certification-v1.js",
  },
  {
    id: "loyalty_scenario_universe_certification",
    required: true,
    failClosed: true,
    contractVersion: "LOYALTY_BPP_SCENARIO_STANDARD_V1",
    measurementFamily: "LOYALTY_BPP",
    description:
      "Fail-closed Loyalty BPP: STANDARD density target 28 (band 21–32) or disclosed EXPANDED 42–63, or LOYALTY_BPP_NOT_APPLICABLE for independents. Legacy 8–12 pulse does not satisfy STANDARD. Core completion does not satisfy this gate.",
    module:
      "lib/ai-demand-positioning/brand-portfolio/loyalty-bpp-scenario-standard-v1.js",
    dependsOn: "scenario_universe_certification",
  },
  {
    id: "monitoring_scenario_config",
    required: true,
    pipelineStages: ["RAW_OBSERVATIONS"],
    dependsOn: "scenario_universe_certification",
  },
  {
    id: "provider_measurement",
    required: true,
    pipelineStages: ["RAW_OBSERVATIONS", "PROVIDER_COVERAGE_RECOVERY"],
    authorization: "adp-authorization-ledger",
    measurementFamily: "CORE_ADP",
    dependsOn: "scenario_universe_certification",
  },
  {
    id: "loyalty_provider_measurement",
    required: true,
    authorization: "bpp-measurement-authorization",
    measurementFamily: "LOYALTY_BPP",
    description:
      "Paid Loyalty BPP provider measurement — separate authorization from Core; archives under LOYALTY_BPP family IDs",
    dependsOn: "loyalty_scenario_universe_certification",
    failClosed: true,
  },
  {
    id: "observation_completeness_integrity",
    required: true,
    pipelineStages: ["GOVERNED_INTERPRETATION", "MEASUREMENT_ASSURANCE"],
  },
  {
    id: "entity_resolution",
    required: true,
    description: "Fail-closed competitive entity resolve",
    resolveVersion: ADP_COMPETITIVE_ENTITY_RESOLVE_VERSION,
  },
  {
    id: "metric_calculation",
    required: true,
    pipelineStages: [
      "MISSING_EVIDENCE_INDEX",
      "POSITIVE_EVIDENCE_INDEX",
      "COMPETITIVE_RANKING",
    ],
  },
  {
    id: "competitive_analysis",
    required: true,
    requiresMarketRegistry: true,
  },
  {
    id: "citation_source_intelligence",
    required: true,
  },
  {
    id: "property_attribute_intelligence",
    required: true,
    contractVersion: "ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_V1",
    description:
      "Governed property attribute set with evidence — required before Reality Gap measurement; empty set is diagnosed, not silently treated as success",
    emptyAttributesPolicy: "STAGE_FAIL_OR_BLOCKED_NOT_SILENT_PASS",
  },
  {
    id: "property_reality_coverage",
    required: true,
    emptyAttributesPolicy: "NOT_MEASURED",
    dependsOn: "property_attribute_intelligence",
  },
  {
    id: "executive_summary",
    required: true,
    evidenceStrengthRequired: true,
  },
  {
    id: "bpp_portfolio_measurement",
    required: true,
    contractVersion: "ADP_UNIVERSAL_BPP_ENGINE_V1",
    description:
      "Governed Brand & Portfolio Position: lens → certified peer cohort → observation reuse / authorized BPP cycle → certified metrics → versioned customer pack → attach-at-read. Never invent ranks; never paid monitoring without authorization.",
    failClosed: true,
    module:
      "lib/ai-demand-positioning/brand-portfolio/universal-bpp-engine-v1.js",
    authorization: "bpp-measurement-authorization",
    subGates: ["bpp_measurement_comparability_certification"],
    comparabilityModule:
      "lib/ai-demand-positioning/brand-portfolio/bpp-measurement-comparability-v1.js",
    notes:
      "Loyalty-constrained pulse (~8–10 scenarios) and Core peer-ranking reuse (~63) are distinct protocols; cross-protocol silent comparison is prohibited. Core STANDARD density does not automatically govern dedicated BPP cycles.",
  },
  {
    id: "adp_metric_semantics_certification",
    required: true,
    contractVersion: "ADP_METRIC_SEMANTICS_CERTIFICATION_V1",
    description:
      "Fail-closed metric registry + grain/denominator + payload mapping + customer-facing displacement wording. Observation reuse only — never paid remonitor.",
    failClosed: true,
    module:
      "lib/ai-demand-positioning/certification/adp-metric-semantics-certification-v1.js",
  },
  {
    id: "section_completeness_certification",
    required: true,
    contractVersion: "ADP_SECTION_COMPLETENESS_V1",
    description:
      "Independent per-section product completeness — trends, BPP, attributes, competitive, citation, executive, evidence; material incomplete sections block publication readiness",
    failClosed: true,
    module: "lib/ai-demand-positioning/certification/adp-section-completeness-v1.js",
  },
  {
    id: "master_report_template_certification",
    required: true,
    contractVersion: "ADP_MASTER_REPORT_PRESENTATION_V1",
    description:
      "Single canonical ADP report template (Bethesda structural master): fixed section order, Trends four-slot shell, BPP four primary KPIs, SPA/share/PDF shell parity. Populated JSON alone is not sufficient — structural presentation must match.",
    failClosed: true,
    module:
      "lib/ai-demand-positioning/presentation/adp-master-template-certification-v1.js",
    structuralReference: "adp_bethesda_marriott",
    subGates: [
      "html_shell_section_parity",
      "trends_presentation_v1",
      "bpp_customer_presentation_v2",
      "canonical_view_model_v1",
    ],
  },
  {
    id: "report_integrity_certification",
    required: true,
    certificationAxes: ADP_CERTIFICATION_AXES,
    note: "Independent of measurement completeness alone; requires section_completeness axis",
  },
  {
    id: "airtable_persistence",
    required: true,
    publicationLifecycle: PUBLICATION_LIFECYCLE.RESEARCH_STAGED,
    note: "FS primary SoT + optional Airtable overlay; read-back mandatory",
  },
  {
    id: "controlled_publication",
    required: true,
    pipelineStages: ["PUBLICATION_INTEGRITY", "CUSTOMER_PUBLICATION"],
    authorization: "authorize-publish",
    publicationLifecycle: PUBLICATION_LIFECYCLE.PUBLISHED,
  },
  {
    id: "customer_delivery_verification",
    required: true,
    publicationLifecycle: PUBLICATION_LIFECYCLE.DELIVERY_VERIFIED,
  },
]);

export const ADP_WORKFLOW_MODES = Object.freeze({
  [RUN_MODE.NEW]: {
    description: "First complete ADP monitoring + report for a hotel",
    allowPaidMeasurement: true,
    allowPublication: true,
    requirePublicationAuth: true,
  },
  [RUN_MODE.REFRESH]: {
    description: "New monitoring period; preserve prior published until approved",
    allowPaidMeasurement: true,
    allowPublication: true,
    requirePublicationAuth: true,
    preservePublished: true,
  },
  [RUN_MODE.VALIDATE]: {
    description: "Recheck observations/metrics/report without new paid calls",
    allowPaidMeasurement: false,
    allowPublication: false,
  },
  [RUN_MODE.REPAIR]: {
    description:
      "Recompute competitor/metrics/report from preserved observations (Charlotte pattern)",
    allowPaidMeasurement: false,
    allowPublication: false,
    recomputeOnly: true,
    reuseObservations: true,
  },
  [RUN_MODE.STATUS]: {
    description: "Read-only status / certification axes",
    allowPaidMeasurement: false,
    allowPublication: false,
  },
  [RUN_MODE.DRY_RUN]: {
    description: "Plan-only; no paid measurement or publish overwrite",
    allowPaidMeasurement: false,
    allowPublication: false,
  },
});

export const ADP_REGRESSION_HOTELS = Object.freeze([
  {
    label: "Charlotte Marriott City Center",
    hpcId: "recbRIFhro6tLmSrO",
    adpPropertyId: "adp_charlotte_marriott_city_center",
    cases: [
      "252_observations_preserved",
      "no_waterstone_boca_fallback",
      "charlotte_market_registry",
      "no_self_displacement",
      "subject_alias_marriott_charlotte_city_center",
      "property_attribute_intelligence_governed_partial",
      "reality_gap_measured_or_explicit_blocker",
    ],
  },
  {
    label: "Waterstone Boca Raton",
    adpPropertyId: "adp_waterstone_boca_raton",
    cases: ["legitimate_south_florida_resolution_preserved"],
  },
  {
    label: "Tampa Airport Marriott",
    hpcId: "recSM9ARs8U9UZ3AZ",
    adpPropertyId: "adp_tampa_airport_marriott",
    cases: ["independent_tampa_market"],
  },
  {
    label: "Falls Church Marriott Fairview Park",
    hpcId: "recRkGGNKslJptFIL",
    adpPropertyId: "adp_falls_church_marriott_fairview_park",
    cases: ["northern_virginia_identity"],
  },
  {
    label: "Renaissance Arlington Capital View",
    hpcId: "recyZciNlXq5KulnD",
    adpPropertyId: "adp_renaissance_arlington_capital_view",
    cases: ["independent_of_fairview"],
  },
]);

export function getAdpWorkflowDefinition() {
  return Object.freeze({
    workflowId: ADP_FULL_E2E_WORKFLOW_ID,
    workflowVersion: ADP_FULL_E2E_WORKFLOW_VERSION,
    product: PRODUCT.ADP,
    underlyingPipelineVersion: ADP_UNDERLYING_PIPELINE_VERSION,
    underlyingPipelineStages: ADP_PERIOD_PIPELINE_STAGES,
    competitiveResolveVersion: ADP_COMPETITIVE_ENTITY_RESOLVE_VERSION,
    requiredInputs: Object.freeze(["hotelIdOrAdpPropertyId", "mode"]),
    optionalConfiguration: Object.freeze([
      "authorizationRef",
      "budgetUsd",
      "authorizePublish",
      "periodId",
      "dryRun",
      "repairFromObservations",
    ]),
    requiredStages: ADP_OS_STAGES,
    certificationAxes: ADP_CERTIFICATION_AXES,
    modes: ADP_WORKFLOW_MODES,
    regressionHotels: ADP_REGRESSION_HOTELS,
    terminalStates: Object.freeze([
      OVERALL_RUN_STATUS.FULL_E2E_COMPLETE,
      OVERALL_RUN_STATUS.FAILED,
      OVERALL_RUN_STATUS.BLOCKED,
      OVERALL_RUN_STATUS.WAITING_PUBLICATION_AUTHORIZATION,
      OVERALL_RUN_STATUS.REPORT_READY,
    ]),
    qualityGates: Object.freeze([
      "fail_closed_market_registry",
      "no_foreign_market_core_inheritance",
      "measurement_not_equal_report_certification",
      "section_completeness_independent_certification",
      "master_report_template_certification",
      "universal_core_loyalty_measurement_alignment",
      "bpp_portfolio_measurement_no_invented_ranks",
      "bpp_measurement_comparability_certification",
      "bpp_presentation_parity_certification",
      "loyalty_scenario_universe_certification",
      "core_and_loyalty_families_not_interchangeable",
      "adp_metric_semantics_certification_fail_closed",
      "empty_reality_gap_not_measured",
      "zero_denominator_no_fabricated_percent",
      "weak_displacement_qualified_or_omitted",
      "airtable_readback_when_overlay_enabled",
      "no_published_overwrite_without_auth",
      "ADP_REVIEW_AND_PDF_ARTIFACT_CERTIFICATION_V1",
      "ADP_OPERATIONAL_REGISTRY_HYGIENE_V1",
      "FOUNDER_REVIEW_READY_SEPARATE_FROM_PUBLISHED_READY_V1",
    ]),
    budgetPolicy: Object.freeze({
      productScoped: true,
      hotelScoped: true,
      noCrossHotelTransfer: true,
      reuseProviderJobs: true,
      repairReusesObservations: true,
      dryRunBlocksPaid: true,
    }),
    publicationRequirements: Object.freeze({
      filesystemPrimarySot: true,
      airtableOptionalOverlay: true,
      stagedCorrectionBeforeLiveOverwrite: true,
      versionedReplacementPreferred: true,
    }),
    cli: Object.freeze({
      canonical: "scripts/dealality-adp-full-e2e.mjs",
      underlyingSharedFirst:
        "lib/ai-demand-positioning/execution/shared-first-property-baseline-period-001-v1.js",
      integrityHardening:
        "scripts/adp-marriott-market-integrity-hardening-v1.mjs",
      npm: "dealality:adp-full-e2e",
    }),
    invariants: Object.freeze([
      "Never inherit another hotel's market registry or CORE as fallback",
      "Generic brand aliases must not auto-bind cross-market properties",
      "Ambiguous mentions abstain from property-level attribution",
      "Preserve raw observations even when competitor unresolved",
      "CERTIFIED measurement ≠ FULLY_CERTIFIED customer report",
      "Bethesda Marriott is ADP portfolio-eligible (31/31); paid remonitor and publication remain separately authorized",
    ]),
  });
}

/**
 * Collapse certification axes into customer-report readiness.
 * All axes must pass for FULLY_CERTIFIED; measurement alone is insufficient.
 * section_completeness may be supplied via assessAdpSectionCompleteness result:
 *   axes.section_completeness = { passed: materialIncompleteCount===0, assessment }
 */
export function assessAdpReportCertification(axes = {}) {
  const results = {};
  for (const axis of ADP_CERTIFICATION_AXES) {
    const v = axes[axis];
    results[axis] = {
      passed: v === true || v?.passed === true,
      status: v?.status || (v === true ? "PASS" : v === false ? "FAIL" : "UNKNOWN"),
      detail: v?.detail || v?.assessment || null,
    };
  }
  const allPassed = ADP_CERTIFICATION_AXES.every((a) => results[a].passed);
  const measurementOnly =
    results.measurement_completeness?.passed === true &&
    !results.customer_report_readiness?.passed;
  const sectionIncomplete =
    results.section_completeness && results.section_completeness.passed !== true;

  return {
    fullyCertified: allPassed,
    measurementCompleteNotReportReady: measurementOnly,
    sectionCompletenessBlocking: sectionIncomplete,
    axes: results,
    overallStatus: allPassed
      ? OVERALL_RUN_STATUS.REPORT_READY
      : measurementOnly
        ? OVERALL_RUN_STATUS.RESEARCH_COMPLETE
        : OVERALL_RUN_STATUS.PARTIAL,
  };
}
