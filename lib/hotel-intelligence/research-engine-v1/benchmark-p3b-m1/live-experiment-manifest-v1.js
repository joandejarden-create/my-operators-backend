/**
 * P3B-M1 first live ownership research experiment — no-spend manifest + protocol.
 * $5 envelope proposed; NOT AUTHORIZED until separate founder approval.
 */

import {
  P3B_M1_HARD_CAP_USD,
  P3B_M1_PER_HOTEL_CAP_USD,
  P3B_M1_FOUNDER_PAID_AUTH_ID,
  P3B_M1_CONTRACT_ID,
  resolveP3BM1FounderPaidAuthorization,
  writeGuaranteesP3BM1,
} from "./contract.js";
import { loadP3BM1PilotCohort, P3B_M1_HPC_IDS } from "./cohort.js";
import {
  buildP3BM1PlannedOperations,
} from "./planned-operations.js";
import {
  probeConfiguredProviderAvailability,
} from "./adapters.js";
import { probeAccountPricingPresence } from "./live-transports.js";
import { buildAdjudicationProtocol } from "./adjudication-protocol-v1.js";
import { QUALITY_WEIGHTS, QUALITY_SCORECARD_VERSION } from "./quality-scorecard-v1.js";
import { BENCHMARK_MODE } from "../benchmark-p3a/contract.js";
import {
  OWNERSHIP_RESEARCH_OBJECTIVE_ID,
  OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
  OWNERSHIP_DRAWER_QUESTIONS,
  PRIMARY_OWNERSHIP_GOALS,
} from "../ownership-research-objective.js";
import { CONSERVATIVE_ORCHESTRATION_HEADROOM_USD } from "./live-transports.js";

export const LIVE_EXPERIMENT_MANIFEST_ID =
  "HE_INTEL_001_P3B_M1_LIVE_OWNERSHIP_EXPERIMENT_V1";
export const LIVE_EXPERIMENT_MANIFEST_VERSION = "1.0.0-no-spend-readiness";

export const RESEARCH_METHODS = Object.freeze({
  A_EXA: "METHOD_A_EXA",
  B_PARALLEL_SEARCH: "METHOD_B_PARALLEL_SEARCH",
  C_PARALLEL_TASK: "METHOD_C_PARALLEL_TASK",
  D_DEALALITY_NATIVE: "METHOD_D_DEALALITY_NATIVE",
  E_HYBRID_WATERFALL: "METHOD_E_HYBRID_WATERFALL",
});

/** Conservative per-arm headroom on top of list-price unit ops (not a max guarantee). */
const ARM_ORCH_HEADROOM = CONSERVATIVE_ORCHESTRATION_HEADROOM_USD || 0.002;

export function buildHotelResearchInstructions(hotel) {
  return {
    hotel: {
      hpcId: hotel.hpcId,
      name: hotel.name,
      expectedName: hotel.expectedName || hotel.name,
    },
    drawer_questions: OWNERSHIP_DRAWER_QUESTIONS,
    primary_goals: PRIMARY_OWNERSHIP_GOALS,
    required_outputs: [
      "ultimate_owner_or_controlling_org_or_unresolved",
      "legal_entity_and_ownership_chain",
      "named_ownership_decision_makers",
      "verified_direct_contact_channels",
      "credible_indirect_ownership_contact_routes",
      "missing_conflicting_unverifiable_info",
    ],
    distinctions: [
      "legal_ownership",
      "economic_ownership",
      "hotel_operator",
      "asset_manager",
      "brand_franchise",
    ],
    never: [
      "invent_ownership_percentages",
      "invent_personal_names_roles_emails_phones",
      "infer_ownership_from_operator_or_brand_alone",
      "treat_url_alone_as_verification",
      "score_known_hotel_identity_as_new_intelligence",
    ],
    seed_mode_a: {
      propertyName: hotel.name,
      hpcId: hotel.hpcId,
      prior_intelligence: "LIMITED_HOTEL_SEED_ONLY",
      may_not_use_other_arms_discoveries: true,
    },
  };
}

export function buildMethodPlans() {
  const planned = buildP3BM1PlannedOperations();
  const ops = planned.hotelPlans[0]?.operations || {};

  return {
    [RESEARCH_METHODS.A_EXA]: {
      arm: RESEARCH_METHODS.A_EXA,
      provider: "EXA",
      executed_when_authorized: true,
      mode_a: true,
      operations: {
        search_lite: ops.EXA?.search_lite,
        contents: ops.EXA?.contents,
      },
      notes: [
        "Discovery candidates only until independent acceptance",
        "Contents as separate bounded op — no inline contents on search",
        `Reserve includes ~$${ARM_ORCH_HEADROOM} orchestration headroom per call where applied`,
      ],
      list_price_expected_usd_per_hotel:
        (ops.EXA?.search_lite?.maxUsd || 0) + (ops.EXA?.contents?.maxUsd || 0),
    },
    [RESEARCH_METHODS.B_PARALLEL_SEARCH]: {
      arm: RESEARCH_METHODS.B_PARALLEL_SEARCH,
      provider: "PARALLEL_SEARCH",
      executed_when_authorized: true,
      mode_a: true,
      operations: { search_fast: ops.PARALLEL_SEARCH?.search_fast },
      notes: [
        "Reserve for max_results requested, not returned count",
        "Hits are discovery candidates",
      ],
      list_price_expected_usd_per_hotel: ops.PARALLEL_SEARCH?.search_fast?.maxUsd || 0,
    },
    [RESEARCH_METHODS.C_PARALLEL_TASK]: {
      arm: RESEARCH_METHODS.C_PARALLEL_TASK,
      provider: "PARALLEL_TASK",
      executed_when_authorized: true,
      mode_a: true,
      operations: {
        task_core: ops.PARALLEL_TASK?.task_core,
        task_pro_optional: ops.PARALLEL_TASK?.task_pro_optional,
      },
      notes: [
        "Task creation ≠ research result",
        "Must poll/resume by run_id; no duplicate paid create",
        "Pro only if separately budgeted and core fails quality gate",
      ],
      list_price_expected_usd_per_hotel: ops.PARALLEL_TASK?.task_core?.maxUsd || 0,
    },
    [RESEARCH_METHODS.D_DEALALITY_NATIVE]: {
      arm: RESEARCH_METHODS.D_DEALALITY_NATIVE,
      provider: "DEALALITY_NATIVE",
      executed_when_authorized: false,
      mode_a: true,
      status_if_not_run: "NOT_EXECUTED_COST_UNKNOWN",
      operations: { research_loop: ops.DEALALITY_NATIVE?.research_loop },
      notes: [
        "Native USD UNKNOWN — must not execute as paid arm under this envelope",
        "Do not assign $0 cost or fabricated performance if not executed",
      ],
      list_price_expected_usd_per_hotel: null,
      usd_status: "UNKNOWN",
    },
    [RESEARCH_METHODS.E_HYBRID_WATERFALL]: {
      arm: RESEARCH_METHODS.E_HYBRID_WATERFALL,
      provider: "HYBRID_WATERFALL",
      executed_when_authorized: false,
      mode_a: true,
      execution_policy:
        "SEPARATE_ARM_WITH_OWN_BUDGET_OR_LABELED_NON_EXECUTED_SIMULATION_FROM_COMPLETED_RUNS",
      notes: [
        "Do not present retrospective combination of other arms as independent live result",
        "Hybrid live arm requires separate founder authorization + budget slice",
        "Default for this first pilot: LABELED_SIMULATION_AFTER_INDEPENDENT_ARMS (non-executed)",
      ],
      default_status: "NOT_EXECUTED_LABELED_SIMULATION_ONLY",
      waterfall_steps: [
        "search_existing_verified_intelligence",
        "inexpensive_discovery",
        "identify_material_gaps",
        "selective_deeper_escalation",
        "independent_validation",
        "named_ownership_contacts",
        "verify_contact_routes",
        "stop_when_actionable_objective_satisfied",
      ],
    },
  };
}

export function buildIndependentComparisonProtocol() {
  return {
    primary_mode: BENCHMARK_MODE.INDEPENDENT_DISCOVERY,
    mode_b: {
      id: BENCHMARK_MODE.INCREMENTAL_PRODUCTION,
      priority: "OPTIONAL_ONLY_IF_BUDGET_AND_FROZEN_PRIORS_ALLOW",
      do_not_sacrifice_mode_a: true,
    },
    fairness: [
      "Same hotel identity inputs",
      "Same research objectives",
      "Equivalent available prior evidence (Mode A: limited seed only)",
      "Comparable request and research-depth limits",
      "Consistent independent evidence acceptance standard",
      "Same ownership-success classifier HE_INTEL_001_OWNERSHIP_RESEARCH_OBJECTIVE_V1",
      "Isolated discovery bags — no cross-arm leakage during Mode A",
    ],
    arm_isolation: true,
    execution_order: {
      policy: "ROTATE_OR_RANDOMIZE_WHERE_PRACTICAL",
      planned_order_mode_a: [
        RESEARCH_METHODS.A_EXA,
        RESEARCH_METHODS.B_PARALLEL_SEARCH,
        RESEARCH_METHODS.C_PARALLEL_TASK,
        RESEARCH_METHODS.D_DEALALITY_NATIVE,
      ],
      note: "Report any ordering imbalance that could affect conclusions",
    },
    stopping: [
      "Record stop reason: objective_achieved | budget_exhausted | insufficient_evidence | technical_issue",
      "Early stop must not distort comparison without labeling",
      "Do not restart PENDING_RECONCILE / CLAIMED ops for more results",
    ],
  };
}

export function buildFinancialControls() {
  const planned = buildP3BM1PlannedOperations();
  return {
    paid_budget_authorized: false,
    proposed_hard_cap_usd: P3B_M1_HARD_CAP_USD,
    proposed_per_hotel_cap_usd: P3B_M1_PER_HOTEL_CAP_USD,
    retries_default: 0,
    forbidden: ["SURFE", "PDL", "unbounded_ai_research", "native_unknown_usd_paid"],
    expected_list_price_provider_usd: planned.totals.expectedMaxProviderUsd,
    expected_with_optional_pro_usd: planned.totals.expectedMaxWithOptionalProUsd,
    expected_cost_is_not_max_guarantee: true,
    binding_caps: planned.totals.bindingCaps,
    headroom_usd: planned.totals.headroomUsd,
    durable_ledger_required: true,
    claim_lease_required: true,
    pending_reconcile_no_auto_release: true,
    unknown_cost_fail_closed: true,
    never_report_unknown_as_zero: true,
    authorization: {
      required_auth_id: P3B_M1_FOUNDER_PAID_AUTH_ID,
      confirm_paid_execution: true,
      authorized_budget_usd_max: P3B_M1_HARD_CAP_USD,
      enable_live_network: true,
      this_prompt_is_not_spend_authorization: true,
      prior_pr_merges_are_not_spend_authorization: true,
    },
  };
}

export function buildExactPaidExecutionCommand() {
  return {
    phase1_no_spend:
      "npm run dry-run:he-intel-001-p3b-m1-live-experiment",
    phase2_paid_only_after_explicit_founder_approval:
      "npm run run:he-intel-001-p3b-m1-live-experiment -- --founder-auth FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1 --confirm-paid-execution --authorized-budget-usd 5 --enable-live-network --mode independent-discovery --founder-spend-authorized",
    refuse_without: [
      "FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1",
      "--confirm-paid-execution",
      "--authorized-budget-usd ≤ 5",
      "--enable-live-network or HE_INTEL_P3B_M1_LIVE_NETWORK=1",
      "--founder-spend-authorized (only after separate explicit founder spend approval)",
    ],
    note:
      "Entrypoint must refuse actual spend until founder explicitly authorizes the $5 envelope for this experiment run.",
  };
}

/**
 * Full no-spend readiness manifest for founder review.
 */
export function buildLiveExperimentManifest(opts = {}) {
  const cohort = loadP3BM1PilotCohort(opts);
  const availability = probeConfiguredProviderAvailability(opts.env || process.env);
  const pricing = probeAccountPricingPresence(opts.env || process.env);
  const planned = buildP3BM1PlannedOperations();
  const authGate = resolveP3BM1FounderPaidAuthorization(opts.founderAuth || {});
  const methods = buildMethodPlans();
  const hotels = (cohort.hotels || []).map((h) => buildHotelResearchInstructions(h));

  return {
    ok: cohort.ok === true,
    manifest_id: LIVE_EXPERIMENT_MANIFEST_ID,
    manifest_version: LIVE_EXPERIMENT_MANIFEST_VERSION,
    parent_contract: P3B_M1_CONTRACT_ID,
    merge_baseline_note: "Requires P3B-M1 live activation merged (PR #439)",
    paid_execution_authorized: false,
    proposed_budget_not_authorized: true,
    guarantees: writeGuaranteesP3BM1(),
    founder_auth_gate: authGate,
    objective: {
      id: OWNERSHIP_RESEARCH_OBJECTIVE_ID,
      version: OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
      questions: OWNERSHIP_DRAWER_QUESTIONS,
    },
    cohort: {
      ok: cohort.ok,
      hotels: cohort.hotels?.map((h) => ({
        hpcId: h.hpcId,
        name: h.name,
      })),
      hpcIds: [...P3B_M1_HPC_IDS],
      subsequent_six_hotel_cohort_retained: true,
      remaining_three_for_next_evaluation: true,
      no_duplicate_hotel_records: true,
      staging_only: true,
    },
    hotel_research_instructions: hotels,
    methods,
    comparison_protocol: buildIndependentComparisonProtocol(),
    adjudication: buildAdjudicationProtocol(),
    quality_scorecard: {
      version: QUALITY_SCORECARD_VERSION,
      weights: QUALITY_WEIGHTS,
      cost_evaluated_separately: true,
      numerical_pass_is_not_full_actionable: true,
    },
    cost_effectiveness_metrics: [
      "total_cost_per_method",
      "total_cost_per_hotel",
      "cost_per_independently_verified_useful_ownership_fact",
      "cost_per_verified_actual_owner",
      "cost_per_relevant_named_decision_maker",
      "cost_per_verified_usable_contact_pathway",
      "cost_per_full_actionable_hotel_outcome",
      "research_quality_per_dollar",
      "incremental_cost_to_close_material_gap",
    ],
    financial_controls: buildFinancialControls(),
    planned_operations: planned,
    provider_availability: availability,
    pricing_presence: pricing,
    exact_execution_command: buildExactPaidExecutionCommand(),
    deliverables_after_authorized_run: [
      "three_hotel_ownership_dossiers",
      "provider_comparison_table",
      "research_method_recommendation",
      "developer_operations_report_with_ledgers_and_run_ids",
    ],
    forbidden: [
      "canonical_graph_writes",
      "hpc_owner_operator_mutation",
      "radar_status_mutation",
      "customer_publication",
      "deployment",
      "surfe",
      "pdl",
    ],
  };
}
