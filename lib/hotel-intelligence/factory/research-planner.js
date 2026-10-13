/**
 * Packet 2.8 — research needs planner (native-first ladder).
 * Does not execute paid research.
 */

import { evaluateHotelIntelligenceCompleteness } from "./completeness-evaluator.js";

export const RESEARCH_PLANNER_VERSION = "hotel-research-planner-v1";

export const COST_METHOD_LADDER = Object.freeze({
  LEVEL_0: "Existing canonical Dealality data",
  LEVEL_1: "Existing research/evidence reuse",
  LEVEL_2: "Deterministic public-source / jurisdiction playbook",
  LEVEL_3: "Lightweight native search/retrieval",
  LEVEL_4: "Advanced native web/document research",
  LEVEL_5: "External research provider / Webhound (exception)",
});

const DOMAIN_TO_TEMPLATE = Object.freeze({
  OWNERSHIP: "OWNERSHIP_GAP",
  OPERATOR: "OPERATOR_GAP",
  BRAND: "BRAND_HISTORY_GAP",
  PEOPLE: "PERSON_PROFILE_GAP",
  PORTFOLIO: "PORTFOLIO_GAP",
  PROPERTY_FUNDAMENTALS: "PROPERTY_FUNDAMENTALS_GAP",
  DEVELOPMENT: "DEVELOPMENT_GAP",
  TRANSACTIONS: "TRANSACTION_GAP",
  ORGANIZATION: "OWNERSHIP_GAP",
  RELATIONSHIPS: "OWNERSHIP_GAP",
});

/**
 * @param {{ hotel_id: string, hotel_name?: string, domains?: object, completeness?: object }} input
 */
export function planHotelResearch(input = {}) {
  const completeness =
    input.completeness ||
    evaluateHotelIntelligenceCompleteness({
      hotel_id: input.hotel_id,
      hotel_name: input.hotel_name,
      domains: input.domains,
    });

  const tasks = [];
  for (const gap of completeness.actionable_gaps || []) {
    const methodLevel =
      gap.estimated_cost_class === "L5" || gap.escalation_required
        ? 5
        : gap.best_source === "census" || gap.best_source === "canonical"
          ? 0
          : gap.reusable_at && gap.reusable_at !== "PROPERTY"
            ? 1
            : 2;

    tasks.push({
      domain: gap.domain,
      question: gap.what_is_missing || `Resolve ${gap.domain}`,
      method: COST_METHOD_LADDER[`LEVEL_${Math.min(methodLevel, 5)}`] || COST_METHOD_LADDER.LEVEL_2,
      method_level: methodLevel,
      playbook: DOMAIN_TO_TEMPLATE[gap.domain] || "INTERNAL_DOMAIN_GAP",
      source_hierarchy: ["canonical", "existing_research", "jurisdiction_playbook", "native_web", "webhound"],
      expected_structured_output: ["claims", "entities", "relationships", "open_questions"],
      cost_class: gap.estimated_cost_class || `L${methodLevel}`,
      stop_condition: "required_evidence_threshold_or_blocked_private_records",
      escalation_rule:
        methodLevel >= 5
          ? "Webhound only after native L0–L4 fail on material gap + founder/user authorization"
          : "Escalate only if native methods fail and gap remains material",
      reusable_at: gap.reusable_at || "PROPERTY",
      why_it_matters: gap.why_it_matters,
    });
  }

  // Prefer reuse / lower cost first
  tasks.sort((a, b) => a.method_level - b.method_level);

  return {
    planner_version: RESEARCH_PLANNER_VERSION,
    hotel_id: input.hotel_id || completeness.hotel_id,
    hotel_name: input.hotel_name || completeness.hotel_name,
    planned_at: new Date().toISOString(),
    tasks,
    webhound_default: false,
    note: "Research only what is missing. Full HI must not fill a single-field gap.",
  };
}
