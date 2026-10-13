/**
 * Webhound-versus-native teacher diagnostic — config + caps.
 * Staging only. Webhound is an observable teacher; never production writer.
 */
export const WEBHOUND_NATIVE_TEACHER_DIAGNOSTIC_VERSION =
  "webhound-native-teacher-diagnostic-v1";

export const DIAGNOSTIC_LABEL = "WEBHOUND_NATIVE_TEACHER_DIAGNOSTIC";

export const COHORT_LABEL = "REUSED_DEVELOPMENT_DIAGNOSTIC";

export const EXPOSURE =
  "reused DEVELOPMENT diagnostic five (same hotel IDs as owner-disjoint native benchmark) — NOT held-out, NOT general CALA, NOT non-teacher, NOT owner-disjoint held-out; prior Parallel/native exposure possible";

/** Source freeze for hotel identity only. */
export const SOURCE_FREEZE_REL =
  "reports/astra-dev-comparison/native-flat-vs-iterative/REUSED_DEVELOPMENT_DIAGNOSTIC_FREEZE.json";

export const HOTEL_IDS = Object.freeze([
  "rec01a28DhirloEoM",
  "rec01uZcCtsRCW9tD",
  "rec03E9aDMxWgkqae",
  "rec1teYJI25SHMzvp",
  "recxDdnwAYADJYe6k",
]);

export const ARM_IDS = Object.freeze({
  NATIVE: "NATIVE",
  WEBHOUND: "WEBHOUND",
});

export const SPENDING_CAPS = Object.freeze({
  max_hotels: 5,
  /** Combined Context.dev across native arm only. */
  context_dev_max_run: 40,
  context_dev_max_per_hotel: 8,
  model_usd_max_run: 0.25,
  model_usd_max_per_hotel: 0.05,
  /** Webhound USD budget per hotel (live only; dry-plan documents). */
  webhound_usd_max_per_hotel: 5,
  webhound_usd_max_run: 25,
  serpapi_max: 0,
  retries: 0,
});

export const FORBIDDEN_INPUT_KEYS = Object.freeze([
  "expected_owner_name",
  "expected_owner",
  "owner_display_name",
  "owner_domain",
  "owner_domain_hints",
  "curated_ownership_urls",
  "known_url",
  "prior_surfe_people",
  "person_hypotheses",
  "domain_hypotheses",
  "owner_entity_id",
  "gold",
  "gold_owner",
  "candidate_documents",
  "known_ownership",
  "teacher_owner",
  "expected_person",
  "expected_email",
  "expected_domain",
  "ground_truth",
  "scoring_key",
  "curated_passage",
]);

export const BUSINESS_OBJECTIVE = Object.freeze({
  summary:
    "Discover hotel-specific property ownership or economic sponsor evidence with passages, dates, and currentness. Separate operator and brand. Do not invent parties.",
  enrichment_disabled: true,
  canonical_writes: false,
  customer_publication: "BLOCKED",
});

/** Approved adapter module path (do not invent a second client). */
export const APPROVED_WEBHOUND_ADAPTER =
  "lib/hotel-intelligence/research/webhound-mcp-client.js";
