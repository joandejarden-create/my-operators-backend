/**
 * Research escalation ladder — Packet 2.4B.
 * External spend is never automatic.
 */

export const RESEARCH_ESCALATION_POLICY_VERSION = "research-escalation-policy-v1";

export const RESEARCH_LEVELS = Object.freeze({
  LEVEL_0: {
    id: 0,
    name: "EXISTING_DEALALITY_GRAPH",
    description: "Census, observations, ownership repository, prior validated relationships.",
  },
  LEVEL_1: {
    id: 1,
    name: "DETERMINISTIC_PLAYBOOKS",
    description: "Triggered research-method playbooks with bounded search/fetch.",
  },
  LEVEL_2: {
    id: 2,
    name: "BOUNDED_NATIVE_AI_EXTRACTION",
    description: "Bounded LLM assist for query expansion / document extraction only.",
  },
  LEVEL_3: {
    id: 3,
    name: "ADVANCED_NATIVE_DOCUMENT_BROWSER",
    description: "Deeper PDF/browser research still inside Dealality tooling.",
  },
  LEVEL_4: {
    id: 4,
    name: "EXTERNAL_RESEARCH_ESCALATION",
    description: "Webhound / paid deep research — founder/customer budget required.",
  },
  /** Routing V2 split — document discovery stays native; Parallel is interpretation. */
  LEVEL_4_DOCUMENT: {
    id: "4-DOCUMENT",
    name: "DECISIVE_DOCUMENT_DISCOVERY",
    description: "Native gap-targeted filing/PDF discovery + follow-through + extraction.",
  },
  LEVEL_4_PARALLEL: {
    id: "4-PARALLEL",
    name: "PARALLEL_INTERPRETATION",
    description: "Parallel deep interpretation / triangulation after native documents (not sole discovery).",
  },
  CONTACT: {
    id: "CONTACT",
    name: "CONTACT_INTELLIGENCE",
    description: "Separate Contact Intelligence engine — never bulk-route contacts to Parallel.",
  },
});

/**
 * External escalation only when ALL conditions hold.
 * @param {{ native_exhausted?: boolean, case_value_warrants?: boolean, teaches_reusable_method?: boolean, budget_allowed?: boolean }} c
 */
export function mayEscalateExternal(c = {}) {
  return Boolean(
    c.native_exhausted &&
      c.case_value_warrants &&
      c.teaches_reusable_method &&
      c.budget_allowed
  );
}

export function defaultEscalationRecommendation() {
  return {
    webhound_role: "SELECTIVE_ESCALATION_RD_ONLY",
    scale_readiness: "NO_SCALE",
    note: "Do not auto-spend. Graduate playbooks via regression + out-of-sample before limited Mexico pilot.",
  };
}
