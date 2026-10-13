/**
 * Score Webhound and native arms on ownership → contact chain dimensions.
 * Uses only observable envelopes + optional labeled test gold (never runner inputs).
 */
import { BEHAVIOR_DIFF_DIMENSIONS, SCORE_DIMENSIONS } from "./observable-schema.js";

const FAILURE_STAGES = Object.freeze([
  "hotel_identity",
  "query_generation",
  "source_selection",
  "page_fetching",
  "document_reading",
  "ownership_evidence",
  "currentness",
  "entity_resolution",
  "domain",
  "person",
  "affiliation",
  "email",
  "phone",
  "complete_chain",
  "none",
]);

function art(env, key) {
  return env?.artifacts?.[key] || { status: "UNAVAILABLE", value: null };
}

function hasPresent(env, key) {
  const a = art(env, key);
  return a.status === "PRESENT" && a.value != null && !(Array.isArray(a.value) && a.value.length === 0);
}

function scoreBool(ok) {
  return ok ? "PASS" : "FAIL";
}

/**
 * Score one arm envelope. Optional gold is for labeled test artifacts only.
 */
export function scoreArmEnvelope(env, gold = null) {
  const dims = {};
  const identity = hasPresent(env, "input_hotel_object");
  dims.hotel_identity = scoreBool(identity);

  const ownershipEvidence =
    hasPresent(env, "passages_or_snippets_used") || hasPresent(env, "owner_sponsor_candidates");
  dims.hotel_specific_ownership_evidence = scoreBool(ownershipEvidence);

  const current =
    hasPresent(env, "currentness_or_date") ||
    (hasPresent(env, "owner_sponsor_candidates") &&
      String(JSON.stringify(art(env, "owner_sponsor_candidates").value)).toLowerCase().includes("current"));
  dims.current_owner_or_sponsor_evidence = scoreBool(current && ownershipEvidence);

  const rel = art(env, "relationship_type");
  const sep =
    hasPresent(env, "relationship_type") ||
    (ownershipEvidence && String(JSON.stringify(rel.value || "")).match(/owner|operator|brand|sponsor/i));
  dims.owner_operator_brand_separation = scoreBool(Boolean(sep));

  const domain = hasPresent(env, "organization_or_domain");
  dims.confirmed_owner_organization_domain = scoreBool(domain);

  const person = hasPresent(env, "person_candidates");
  dims.relevant_person = scoreBool(person);

  const affiliation =
    person &&
    (JSON.stringify(art(env, "person_candidates").value || "").match(/affiliation|independently|confirmed/i) ||
      gold?.affiliation_supported === true);
  dims.independently_supported_affiliation = scoreBool(Boolean(affiliation));

  const email = hasPresent(env, "email_fields");
  const phone = hasPresent(env, "phone_fields");
  dims.attributable_email = scoreBool(email);
  dims.attributable_phone = scoreBool(phone);

  const chain =
    ownershipEvidence && domain && person && (email || phone);
  dims.complete_hotel_to_contact_chain = scoreBool(Boolean(chain));

  let incorrect = "NONE_DETECTED";
  if (gold?.forbidden_owner_names?.length && hasPresent(env, "owner_sponsor_candidates")) {
    const blob = JSON.stringify(art(env, "owner_sponsor_candidates").value || "").toLowerCase();
    const hit = gold.forbidden_owner_names.some((n) => blob.includes(String(n).toLowerCase()));
    incorrect = hit ? "INCORRECT_OWNER_SUSPECTED" : "NONE_DETECTED";
  }
  dims.incorrect_owner_assignments = incorrect;

  dims.earliest_failure_stage = earliestFailureStage(dims);

  return {
    arm: env?.arm || null,
    hotel_id: env?.hotel_id || null,
    dimensions: dims,
    score_dimension_order: [...SCORE_DIMENSIONS],
  };
}

function earliestFailureStage(dims) {
  const order = [
    ["hotel_identity", "hotel_identity"],
    ["hotel_specific_ownership_evidence", "ownership_evidence"],
    ["current_owner_or_sponsor_evidence", "currentness"],
    ["owner_operator_brand_separation", "entity_resolution"],
    ["confirmed_owner_organization_domain", "domain"],
    ["relevant_person", "person"],
    ["independently_supported_affiliation", "affiliation"],
    ["attributable_email", "email"],
    ["attributable_phone", "phone"],
    ["complete_hotel_to_contact_chain", "complete_chain"],
  ];
  for (const [dim, stage] of order) {
    if (dims[dim] === "FAIL") return stage;
  }
  return "none";
}

/**
 * Behavior difference: where Webhound succeeds and native fails (observable only).
 */
export function buildBehaviorDifferenceReport(nativeEnv, webhoundEnv, nativeScore, webhoundScore) {
  const rows = [];
  for (const dim of BEHAVIOR_DIFF_DIMENSIONS) {
    rows.push(diffBehaviorDimension(dim, nativeEnv, webhoundEnv, nativeScore, webhoundScore));
  }
  const webhoundWins = rows.filter((r) => r.pattern === "WEBHOUND_SUCCEEDS_NATIVE_FAILS");
  return {
    version: "webhound-native-behavior-diff-v1",
    hotel_id: nativeEnv?.hotel_id || webhoundEnv?.hotel_id || null,
    dimensions: rows,
    webhound_succeeds_native_fails: webhoundWins,
    summary: {
      webhound_advantage_count: webhoundWins.length,
      both_empty_count: rows.filter((r) => r.pattern === "BOTH_EMPTY_OR_UNAVAILABLE").length,
      native_advantage_count: rows.filter((r) => r.pattern === "NATIVE_SUCCEEDS_WEBHOUND_FAILS").length,
    },
  };
}

function statusOf(env, key) {
  return art(env, key).status;
}

function diffBehaviorDimension(dim, nativeEnv, webhoundEnv, nativeScore, webhoundScore) {
  const map = {
    query_generation: "generated_queries_or_research_questions",
    source_selection: "search_results_and_urls",
    source_ranking: "search_results_and_urls",
    page_fetching: "pages_fetched_or_read",
    document_reading: "passages_or_snippets_used",
    historical_currentness_follow_up: "follow_up_questions",
    entity_resolution: "owner_sponsor_candidates",
    person_discovery: "person_candidates",
    contact_enrichment: "email_fields",
  };
  const key = map[dim];
  const nSt = statusOf(nativeEnv, key);
  const wSt = statusOf(webhoundEnv, key);
  let pattern = "BOTH_PRESENT_OR_COMPARABLE";
  if (wSt === "PRESENT" && (nSt === "EMPTY" || nSt === "UNAVAILABLE" || nSt === "EMPTY")) {
    pattern = "WEBHOUND_SUCCEEDS_NATIVE_FAILS";
  } else if (nSt === "PRESENT" && (wSt === "EMPTY" || wSt === "UNAVAILABLE")) {
    pattern = "NATIVE_SUCCEEDS_WEBHOUND_FAILS";
  } else if (
    (nSt === "EMPTY" || nSt === "UNAVAILABLE") &&
    (wSt === "EMPTY" || wSt === "UNAVAILABLE")
  ) {
    pattern = "BOTH_EMPTY_OR_UNAVAILABLE";
  }

  // Score-linked overrides for ownership-related dims
  if (dim === "entity_resolution") {
    const nOk = nativeScore?.dimensions?.hotel_specific_ownership_evidence === "PASS";
    const wOk = webhoundScore?.dimensions?.hotel_specific_ownership_evidence === "PASS";
    if (wOk && !nOk) pattern = "WEBHOUND_SUCCEEDS_NATIVE_FAILS";
    else if (nOk && !wOk) pattern = "NATIVE_SUCCEEDS_WEBHOUND_FAILS";
  }
  if (dim === "contact_enrichment") {
    pattern = "DISABLED_BY_DIAGNOSTIC";
  }

  return {
    dimension: dim,
    native_status: nSt,
    webhound_status: wSt,
    pattern,
    native_note: art(nativeEnv, key).note,
    webhound_note: art(webhoundEnv, key).note,
  };
}

/**
 * Aggregate hotel scores + provisional native change hypotheses (post-comparison).
 */
export function buildComparisonScoreReport({ hotel_scores, behavior_diffs }) {
  const provisional_native_changes = [
    {
      priority: 1,
      change: "better_ownership_query_decomposition",
      rationale:
        "If Webhound generates ownership/sponsor/currentness questions and native remains identity-first, align native ownershipQueries to ownership-first decomposition (already partially patched — measure lift).",
    },
    {
      priority: 2,
      change: "better_source_ranking",
      rationale:
        "If Webhound selects registry/transaction/sponsor pages while native ranks brand-central/aggregators first, tighten demotions and hotel-link boosts without copying teacher URLs.",
    },
    {
      priority: 3,
      change: "more_useful_page_selection",
      rationale:
        "If Webhound fetches fewer but higher-yield pages, prefer ranked ownership URLs over quota-filling scrapes.",
    },
    {
      priority: 4,
      change: "follow_up_after_historical_or_ambiguous_claims",
      rationale:
        "If Webhound issues currentness follow-ups after sale/historical language and native stops, strengthen iterative follow-up goals.",
    },
    {
      priority: 5,
      change: "stronger_hotel_link_validation",
      rationale:
        "If Webhound rejects wrong-property entities and native accepts weak name matches, raise hotel-link evidence bar.",
    },
    {
      priority: 6,
      change: "owner_operator_brand_separation",
      rationale:
        "If Webhound labels relationship types and native conflates operator/brand as owner, keep classification explicit in extract/score.",
    },
    {
      priority: 7,
      change: "explicit_person_affiliation_research",
      rationale:
        "If Webhound surfaces affiliated people only after owner org is resolved and native invents or skips, gate person discovery on confirmed owner org.",
    },
  ];

  return {
    version: "webhound-native-teacher-score-v1",
    score_dimensions: [...SCORE_DIMENSIONS],
    failure_stages: [...FAILURE_STAGES],
    hotel_scores: hotel_scores || [],
    behavior_diffs: behavior_diffs || [],
    provisional_native_changes_after_comparison: provisional_native_changes,
    note:
      "Provisional change list is for post-live comparison prioritization. Do not implement a broad rewrite from dry-plan alone. Do not copy named owners/domains/people/emails/phones into extractors.",
  };
}

export { FAILURE_STAGES };
