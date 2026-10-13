/**
 * Packet 2.5 / 2.5A — Claim pipeline:
 * SOURCE → CLAIM CANDIDATE → ACCEPTANCE GATE → ENTITY RESOLUTION
 * → RELATIONSHIP CANDIDATE → RECONCILIATION → PROMOTION.
 *
 * Does not load evaluation-only gold fixtures.
 */

import { extractClaimsFromCorpus } from "./extractor.js";
import { acceptClaimsFromCandidates } from "./validate-claim.js";
import {
  createEntityResolver,
  resolveClaimsEntities,
  aliasesFromRegistry,
} from "./entity-resolve.js";
import {
  claimsToRelationshipCandidates,
  reconcileRelationshipCandidates,
} from "./reconcile.js";
import { promoteCandidates } from "./promote.js";
import { deriveOverallClaimConfidence } from "./schemas.js";

export const CLAIM_PIPELINE_VERSION = "claim-pipeline-v1.5a";

/**
 * @param {{
 *   documents: object[],
 *   entity_registry: object,
 *   hotel_context_id?: string,
 *   hotel_name?: string,
 *   research_run_id?: string,
 *   context_org?: string,
 * }} input
 */
export function runClaimPipeline(input) {
  const t0 = Date.now();
  const documents = input.documents || [];
  const registry = input.entity_registry || {
    hotels: [],
    organizations: [],
    people: [],
  };

  const candidates = extractClaimsFromCorpus(documents, {
    hotel_context_id: input.hotel_context_id,
    hotel_name: input.hotel_name,
    research_run_id: input.research_run_id,
    context_org: input.context_org,
  }).map((c) => ({
    ...c,
    acceptance_status: "EXTRACTED_CANDIDATE",
  }));

  const docsById = Object.fromEntries(
    documents.map((d) => [d.id || d.source_id, d])
  );
  const gated = acceptClaimsFromCandidates(candidates, {
    documentsById: docsById,
    hotel_name: input.hotel_name,
  });

  const resolver = createEntityResolver(registry);
  const resolved = resolveClaimsEntities(gated.accepted_claims, resolver, {
    hotel_context_id: input.hotel_context_id,
    hotel_name: input.hotel_name,
  }).map((c) => ({
    ...c,
    acceptance_status: "ACCEPTED_CLAIM",
    overall_claim_confidence: deriveOverallClaimConfidence({
      extraction_confidence: c.extraction_confidence,
      entity_resolution_confidence: c.entity_resolution_confidence ?? 0.4,
      semantic_confidence: c.semantic_confidence,
      temporal_confidence:
        c.temporal_status && c.temporal_status !== "UNKNOWN" ? 0.75 : 0.4,
      source_authority: c.source_authority,
      corroboration: 0.5,
    }),
  }));

  const aliases = aliasesFromRegistry(registry);
  const rawCandidates = claimsToRelationshipCandidates(resolved);
  const reconciled = reconcileRelationshipCandidates(rawCandidates);
  const promotion = promoteCandidates(reconciled, resolved);

  const elapsed = Date.now() - t0;
  const docCount = documents.length || 1;

  return {
    pipeline_version: CLAIM_PIPELINE_VERSION,
    research_run_id: input.research_run_id || null,
    hotel_context_id: input.hotel_context_id || null,
    documents_processed: documents.length,
    claim_candidates: candidates,
    claims: resolved,
    accepted_claims: resolved,
    observations: gated.observations,
    rejected_extractions: gated.rejected_extractions,
    gate_counts: gated.counts,
    aliases,
    relationship_candidates_raw: rawCandidates,
    relationship_candidates: reconciled,
    promoted_relationships: promotion.promoted,
    held_candidates: promotion.held,
    review_queue: promotion.reviewQueue,
    events: promotion.events,
    metrics: {
      documents_processed: documents.length,
      candidates_proposed: candidates.length,
      claims_accepted: resolved.length,
      observations: gated.observations.length,
      rejected: gated.rejected_extractions.length,
      claims_extracted: resolved.length,
      valid_claims: resolved.length,
      candidates: reconciled.length,
      promoted: promotion.promoted.length,
      review_queue: promotion.reviewQueue.length,
      events: promotion.events.length,
      duration_ms: elapsed,
      cost_usd_total: 0,
      median_cost_per_document: 0,
      cost_per_valid_claim: 0,
      cost_per_accepted_claim: 0,
      cost_per_hotel_corpus: 0,
      ms_per_document: Math.round(elapsed / docCount),
      candidates_per_document:
        Math.round((candidates.length / docCount) * 100) / 100,
      accepted_per_document:
        Math.round((resolved.length / docCount) * 100) / 100,
    },
  };
}
