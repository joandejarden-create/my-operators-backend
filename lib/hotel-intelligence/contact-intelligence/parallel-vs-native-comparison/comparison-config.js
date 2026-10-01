/**
 * Frozen Parallel vs Dealality-native ownership comparison config.
 * Preparation-only constants — no paid execution from this module.
 */

export const COMPARISON_ID = "parallel-vs-dealality-native-ownership-v1";
export const COMPARISON_LABEL = "REUSED_DEVELOPMENT_COHORT";
export const COHORT_FREEZE_PATH = "reports/astra-dev-comparison-10-freeze-proposed-v1.json";

/** Actual Parallel adapter path (user-facing docs sometimes mis-cite research-engine-v2). */
export const PARALLEL_ADAPTER_ROOT =
  "lib/hotel-intelligence/research/providers/parallel";

export const ARM_IDS = Object.freeze({
  DEALALITY_NATIVE: "DEALALITY_NATIVE",
  PARALLEL: "PARALLEL",
});

/**
 * Frozen Parallel Task API choice (verified from official docs 2026-09-16).
 * Endpoint: Task API POST /v1/tasks/runs — NOT FindAll.
 * Docs: https://docs.parallel.ai/task-api/guides/choose-a-processor
 * Pricing: https://docs.parallel.ai/getting-started/pricing
 */
export const PARALLEL_PROCESSOR_FREEZE = Object.freeze({
  api: "Task API",
  endpoint: "POST /v1/tasks/runs",
  not_used: ["FindAll", "Search API alone", "Extract API alone", "Responses API"],
  processor: "pro",
  processor_rationale:
    "Exploratory multi-step ownership + domain + decision-maker research; ~20 output fields; unsuitable to use lite/base/core merely to hit a dollar target.",
  cost_usd_per_1000_completed_runs: 100,
  cost_usd_per_completed_run: 0.1,
  latency_guide: "2min - 10min",
  max_fields_guide: "~20",
  docs_pricing_url: "https://docs.parallel.ai/getting-started/pricing",
  docs_processor_url: "https://docs.parallel.ai/task-api/guides/choose-a-processor",
  template_id: "FULL_HOTEL_INTELLIGENCE",
  include_contacts: true,
  blind_mode: true,
  inject_native_candidate_documents: false,
  automatic_retries: 0,
  frozen_at: "2026-09-16",
});

export const RELATIONSHIP_ENUM = Object.freeze([
  "PROPERTY_OWNER",
  "ECONOMIC_SPONSOR",
  "OPERATOR",
  "BRAND",
  "REGISTERED_BUSINESS",
  "HISTORICAL_OWNER",
  "UNRESOLVED",
]);

export const BUSINESS_OBJECTIVE = Object.freeze({
  title: "Hotel ownership and decision-maker research",
  questions: [
    "Identify the actual owner or evidenced economic sponsor of the named hotel asset (not brand membership or operator alone).",
    "Identify the owner organization’s official domain with evidence connecting domain to organization.",
    "Identify a relevant, currently affiliated decision-maker for ownership, development, acquisitions, or asset management.",
    "Preserve any public evidence-supported route toward obtaining that person’s contacts (source/attribution only — no enrichment providers).",
  ],
  evidence_requirements: [
    "Exact supporting passage and source URL for ownership/sponsor claims.",
    "Publication date and transaction/event date recorded separately.",
    "Currentness and contrary evidence recorded when present.",
    "Do not promote brand, management, registry listing, or generic portfolio language into ownership.",
    "A historical acquisition does not automatically establish current ownership.",
    "Absence of later-sale results is not proof of continued ownership.",
    "Do not require deed/UBO proof when an economic sponsor is clearly evidenced.",
    "Public contact details: preserve source/attribution/purpose; separate hotel, corporate, media, and named-person routes; do not claim deliverability verification.",
  ],
});

/**
 * Enforceable spending caps for the eventual live run (not executed in prep).
 * Combined target ≤ $10 when Parallel pro is appropriate.
 */
export const SPENDING_CAPS = Object.freeze({
  combined_target_usd_max: 10,
  parallel: {
    processor: PARALLEL_PROCESSOR_FREEZE.processor,
    runs_per_hotel_max: 1,
    hotels: 10,
    automatic_retries: 0,
    usd_per_run: PARALLEL_PROCESSOR_FREEZE.cost_usd_per_completed_run,
    arm_usd_max: 1.5,
    expected_max_usd_if_all_succeed: 1.0,
    note: "Failed Parallel runs are not billed per Parallel docs; still reserve arm_usd_max for ledger safety.",
  },
  dealality_native: {
    context_dev_credits_per_hotel_max: 8,
    context_dev_credits_arm_max: 80,
    serpapi_max: 0,
    openai_model_reader_usd_max: 0,
    usd_estimate_status: "CREDIT_BOUNDED_USD_NOT_VERIFIED_IN_REPO",
    usd_estimate_note:
      "Arm A is capped in Context.dev credits. Convert to USD from Context.dev dashboard rates before spend approval if a hard dollar gate is required.",
  },
  enrichment: {
    surfe: 0,
    fullenrich: 0,
    other_paid_contact_providers: 0,
  },
  forbidden: {
    purchases: true,
    overages: true,
    global_paid_enrichment_enablement: true,
    hpc_airtable_writes: true,
    canonical_ownership_mutations: true,
    customer_publication: true,
    outreach: true,
  },
});

export const COHORT_META = Object.freeze({
  label: COMPARISON_LABEL,
  languages: ["en", "pt"],
  prior_tuning_exposure: "YES",
  owner_group_separation: "UNCERTAIN",
  not_unseen: true,
  not_cala_representative: true,
  not_independent_holdout: true,
  spanish_cases_added: false,
  difficult_hotels_replaced: false,
  protected_held_out_untouched: true,
  denominator_hotels: 10,
});
