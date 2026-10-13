/**
 * Build frozen Parallel Task instructions for the ownership comparison.
 * Reuses compileParallelTask — does not duplicate the Parallel client.
 */

import { compileParallelTask, validateCompiledParallelTask } from "../../research/providers/parallel/compile-parallel-task.js";
import {
  BUSINESS_OBJECTIVE,
  PARALLEL_PROCESSOR_FREEZE,
} from "./comparison-config.js";
import { applyLeanComparisonOutputSchema } from "./lean-output-schema.js";

export function buildComparisonResearchObjectiveText() {
  const q = BUSINESS_OBJECTIVE.questions.map((x, i) => `${i + 1}. ${x}`).join("\n");
  const e = BUSINESS_OBJECTIVE.evidence_requirements.map((x) => `- ${x}`).join("\n");
  return [
    "BUSINESS OBJECTIVE (identical to Dealality native arm):",
    BUSINESS_OBJECTIVE.title,
    "",
    "Questions to answer with evidence:",
    q,
    "",
    "Evidence requirements:",
    e,
    "",
    "Relationship vocabulary (use exactly when classifying):",
    "PROPERTY_OWNER | ECONOMIC_SPONSOR | OPERATOR | BRAND | REGISTERED_BUSINESS | HISTORICAL_OWNER | UNRESOLVED",
    "",
    "After subject identity is proven TRUE:",
    "- Prefer transaction announcements, owner disclosures, filings, and property histories.",
    "- Follow named parties; do not stop at brand membership or management.",
    "- Keep acquisition/event dates separate from publication dates.",
    "- Distinguish historical owner from current owner supported as of a stated date.",
    "- Public contacts: preserve source and attribution only; never mark inferred email VERIFIED or deliverability-verified.",
    "- Do not use paid contact enrichment services; discovery of publicly published routes is allowed.",
  ].join("\n");
}

/**
 * Compile a Parallel task for one hotel using the frozen processor and blind mode.
 * candidate_documents intentionally empty for fairness.
 * Output schema is lean (Parallel ≤100 properties); research objective unchanged.
 */
export function compileComparisonParallelTask(hotelSeed, opts = {}) {
  const unresolved = BUSINESS_OBJECTIVE.questions.map((q) => ({ question: q }));
  const compiled = compileParallelTask({
    template_id: PARALLEL_PROCESSOR_FREEZE.template_id,
    hotel_seed: hotelSeed,
    unresolved_questions: unresolved,
    known_facts: [],
    known_gaps: unresolved,
    blind_mode: true,
    benchmark_blind: true,
    parallel_processor: PARALLEL_PROCESSOR_FREEZE.processor,
    include_contacts: PARALLEL_PROCESSOR_FREEZE.include_contacts,
    candidate_documents: [],
    budget_usd: opts.budget_usd ?? PARALLEL_PROCESSOR_FREEZE.cost_usd_per_completed_run,
    investigation_id: opts.investigation_id || `pvn_${hotelSeed.hotel_id}`,
  });

  const extra = buildComparisonResearchObjectiveText();
  compiled.research_prompt = `${compiled.research_prompt}\n\n${extra}`;
  if (compiled.parallel_input) {
    compiled.parallel_input.research_objective = `${compiled.parallel_input.research_objective || ""}\n\n${extra}`;
  }
  compiled.comparison_processor_freeze = {
    processor: PARALLEL_PROCESSOR_FREEZE.processor,
    api: PARALLEL_PROCESSOR_FREEZE.api,
    endpoint: PARALLEL_PROCESSOR_FREEZE.endpoint,
  };

  applyLeanComparisonOutputSchema(compiled);

  const validation = validateCompiledParallelTask(compiled);
  return { compiled, validation, instructions_text: compiled.research_prompt };
}
