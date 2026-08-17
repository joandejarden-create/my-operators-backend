/**
 * Fastest-safe queue scoring for Census Autopilot.
 * Transparent, explainable priority — not over-engineered.
 */

import fs from "node:fs";
import path from "node:path";

import { QUEUE_ORDER } from "./census-autopilot-queue-router.js";

/** Business value weights (higher = process earlier when other factors equal). */
export const BUSINESS_VALUE_WEIGHTS = Object.freeze({
  source_discovery: 0.4,
  brand_normalization: 0.98,
  core_identity_quality: 1.0,
  city_state_normalization: 0.95,
  core_identity_source_lookup: 0.92,
  clean_core_classification: 0.91,
  market_geography_completion: 0.93,
  key_field_completion: 0.95,
  property_name_cleanup: 0.9,
  address_confirmation: 1.0,
  coordinate_completion: 1.0,
  phone_number_enrichment: 0.85,
  coordinate_resolution: 0.5,
  radar_public_readiness: 0.95,
  description_extraction: 0.85,
  amenities_extraction: 0.75,
  property_type_asset_context: 0.7,
  rooms_keys: 0.65,
  steward_webhound_hard_cases: 0.2,
});

export const STRATEGY_FASTEST_SAFE = "fastest-safe";
export const FUTURE_STRATEGIES = Object.freeze([
  "highest-yield",
  "highest-confidence",
  "geography-first",
  "descriptions-first",
]);

/**
 * Score a single queue for fastest-safe ordering.
 * @param {object} queue - QUEUE_ORDER entry (+ status/blockers)
 * @param {{
 *   expectedSafeWrites?: number,
 *   extractorReadiness?: number,
 *   sourceAccessSuccess?: number,
 *   runtimeRisk?: number,
 *   ambiguityRisk?: number,
 *   geocodeProviderReady?: boolean,
 *   schemaV114Ready?: boolean,
 *   fetchSuccessByFamily?: Record<string, number>,
 * }} [ctx]
 */
export function scoreQueueFastestSafe(queue, ctx = {}) {
  const id = queue.id;
  const business = BUSINESS_VALUE_WEIGHTS[id] ?? 0.5;
  let expected = ctx.expectedSafeWrites ?? estimateExpectedWrites(queue, ctx);
  let extractor = ctx.extractorReadiness ?? (queue.existing_module ? 0.85 : 0.35);
  let sourceAccess = ctx.sourceAccessSuccess ?? 0.7;
  let runtimeRisk = ctx.runtimeRisk ?? 1.0;
  let ambiguity = ctx.ambiguityRisk ?? (id === "rooms_keys" ? 1.25 : 1.0);
  let dependencyPenalty = 1.0;

  const reasons = [];

  if (queue.blocked_until === "provider_storage_decision" && !ctx.geocodeProviderReady) {
    dependencyPenalty = 8;
    expected = Math.min(expected, 0.1);
    reasons.push("geocode_provider_decision_missing → heavy penalty; soft-skip in apply");
  }
  if (queue.needs_schema === "v1.1.4" && !ctx.schemaV114Ready) {
    dependencyPenalty *= 1.15;
    reasons.push("v1.1.4 provenance incomplete → slight completeness penalty; Rooms/Keys still runnable");
  }
  if (id === "steward_webhound_hard_cases") {
    extractor = 0.2;
    expected = 0.05;
    reasons.push("learning-only queue; never production writes");
  }
  if (id === "description_extraction") {
    extractor = Math.max(extractor, 0.9);
    reasons.push("IHG description extractor proven in production");
  }
  if (id === "rooms_keys") {
    extractor = Math.max(extractor, 0.8);
    ambiguity = Math.max(ambiguity, 1.2);
    reasons.push("rooms extractor ready; mixed-use ambiguity elevates risk");
  }
  if (id === "property_name_cleanup") {
    extractor = Math.max(extractor, 0.75);
    ambiguity = Math.max(ambiguity, 1.1);
    reasons.push("official-source name cleanup; marketing/tagline detection");
  }

  const numerator = expected * business * extractor * sourceAccess;
  const denominator = Math.max(0.05, runtimeRisk * ambiguity * dependencyPenalty);
  const score = numerator / denominator;

  return {
    queue_id: id,
    letter: queue.letter,
    label: queue.label,
    score: Number(score.toFixed(4)),
    components: {
      expected_safe_writes: expected,
      business_value_weight: business,
      extractor_readiness: extractor,
      source_access_success: sourceAccess,
      runtime_risk: runtimeRisk,
      ambiguity_risk: ambiguity,
      dependency_penalty: dependencyPenalty,
    },
    reasons,
    status: queue.status,
    blockers: queue.blockers || [],
    soft_skip_in_apply:
      queue.blocked_until === "provider_storage_decision" && !ctx.geocodeProviderReady,
  };
}

function estimateExpectedWrites(queue, ctx) {
  // Heuristic from current Mexico Census fill rates when not injected
  const defaults = {
    source_discovery: 0.2,
    brand_normalization: 0.55,
    core_identity_quality: 0.5,
    city_state_normalization: 0.4,
    core_identity_source_lookup: 0.1,
    clean_core_classification: 0.1,
    key_field_completion: 0.35,
    property_name_cleanup: 0.35,
    address_confirmation: 0.3,
    coordinate_resolution: ctx.geocodeProviderReady ? 0.2 : 0.05,
    coordinate_completion: ctx.geocodeProviderReady ? 0.45 : 0.05,
    phone_number_enrichment: 0.25,
    radar_public_readiness: 0.35,
    description_extraction: 0.55,
    amenities_extraction: 0.45,
    property_type_asset_context: 0.4,
    rooms_keys: 0.5,
    steward_webhound_hard_cases: 0.05,
  };
  return defaults[queue.id] ?? 0.3;
}

/**
 * Build ordered priority plan.
 * @param {object[]} routedQueues - from routeAutopilotQueues().queues
 * @param {object} [ctx]
 */
export function buildFastestSafePriorityPlan(routedQueues = QUEUE_ORDER, ctx = {}) {
  const scored = routedQueues.map((q) => scoreQueueFastestSafe(q, ctx));
  const ordered = [...scored].sort((a, b) => {
    if (a.soft_skip_in_apply !== b.soft_skip_in_apply) return a.soft_skip_in_apply ? 1 : -1;
    return b.score - a.score;
  });

  return {
    version: "census-autopilot-fastest-safe-v1",
    strategy: STRATEGY_FASTEST_SAFE,
    generated_at: new Date().toISOString(),
    formula:
      "(expected_safe_writes * business_value_weight * extractor_readiness * source_access_success) / (runtime_risk * ambiguity_risk * dependency_penalty)",
    future_strategies: FUTURE_STRATEGIES,
    ordered_queue_ids: ordered.map((q) => q.queue_id),
    queues: ordered,
    why:
      "Prioritize geography/radar and proven extractors with high expected High-confidence writes; soft-defer geocode without provider; rooms early but after lower-ambiguity description when scores dictate; hard cases last.",
    geocode_soft_deferred: ordered.filter((q) => q.soft_skip_in_apply).map((q) => q.queue_id),
  };
}

export function renderQueuePriorityMarkdown(plan) {
  const lines = [
    `# Queue Priority Plan — ${plan.strategy}`,
    ``,
    `- **Formula:** ${plan.formula}`,
    `- **Why:** ${plan.why}`,
    ``,
    `## Order`,
    ``,
  ];
  plan.queues.forEach((q, i) => {
    lines.push(
      `${i + 1}. **${q.letter}. ${q.label}** (\`${q.queue_id}\`) — score ${q.score}${q.soft_skip_in_apply ? " — soft-skip (provider)" : ""}`
    );
    for (const r of q.reasons || []) lines.push(`   - ${r}`);
  });
  lines.push(``);
  return lines.join("\n");
}

export function writeQueuePriorityPlan(runDir, plan) {
  fs.mkdirSync(runDir, { recursive: true });
  fs.writeFileSync(path.join(runDir, "queue-priority-plan.json"), JSON.stringify(plan, null, 2), "utf8");
  fs.writeFileSync(path.join(runDir, "queue-priority-plan.md"), renderQueuePriorityMarkdown(plan), "utf8");
}
