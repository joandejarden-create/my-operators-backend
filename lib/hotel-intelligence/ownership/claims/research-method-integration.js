/**
 * Research-method integration note (Packet 2.5 / Part 41).
 *
 * Future playbook / native research stages should emit SOURCE documents into:
 *   extractClaimsFromCorpus → resolveClaimsEntities → reconcile → promote
 *
 * Methods must NOT each implement their own ownership semantics.
 * This module is the shared claim→entity→relationship seam.
 */

export const RESEARCH_METHOD_CLAIM_INTEGRATION =
  "playbook_or_native_stage → source_document → claim_pipeline_v1";
