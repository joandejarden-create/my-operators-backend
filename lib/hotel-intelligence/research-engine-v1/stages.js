/**
 * Deterministic stage graph for HOTEL_INTELLIGENCE_RESEARCH_ENGINE_V1.
 * Extends full-research-workflow-v2 RESEARCH_STAGE with ORGANIZATION + RELATIONSHIP
 * logical stages (mapped into OWNERSHIP/PERSON/CONTACT handlers in P1).
 */

import { RESEARCH_STAGE as WF_STAGE } from "../contact-intelligence/full-research-workflow.js";

export const ENGINE_STAGE = Object.freeze({
  INIT: "INIT",
  CANONICAL_HPC_LOOKUP: "CANONICAL_HPC_LOOKUP",
  IDENTITY_RESOLUTION: "IDENTITY_RESOLUTION",
  OWNERSHIP: "OWNERSHIP",
  ORGANIZATION: "ORGANIZATION",
  RELATIONSHIP: "RELATIONSHIP",
  PERSON: "PERSON",
  CONTACT: "CONTACT",
  EVIDENCE_VALIDATION: "EVIDENCE_VALIDATION",
  SYNTHESIS: "SYNTHESIS",
  COMPLETE: "COMPLETE",
  STOPPED: "STOPPED",
});

/** Ordered happy-path graph (same for every pilot hotel). */
export const ENGINE_STAGE_GRAPH = Object.freeze([
  ENGINE_STAGE.INIT,
  ENGINE_STAGE.CANONICAL_HPC_LOOKUP,
  ENGINE_STAGE.IDENTITY_RESOLUTION,
  ENGINE_STAGE.OWNERSHIP,
  ENGINE_STAGE.ORGANIZATION,
  ENGINE_STAGE.RELATIONSHIP,
  ENGINE_STAGE.PERSON,
  ENGINE_STAGE.CONTACT,
  ENGINE_STAGE.EVIDENCE_VALIDATION,
  ENGINE_STAGE.SYNTHESIS,
  ENGINE_STAGE.COMPLETE,
]);

/** Map engine stages onto existing workflow-v2 stage names where applicable. */
export const ENGINE_TO_WORKFLOW_STAGE = Object.freeze({
  [ENGINE_STAGE.INIT]: WF_STAGE.INIT,
  [ENGINE_STAGE.CANONICAL_HPC_LOOKUP]: WF_STAGE.IDENTITY,
  [ENGINE_STAGE.IDENTITY_RESOLUTION]: WF_STAGE.IDENTITY,
  [ENGINE_STAGE.OWNERSHIP]: WF_STAGE.OWNERSHIP,
  [ENGINE_STAGE.ORGANIZATION]: WF_STAGE.OWNERSHIP,
  [ENGINE_STAGE.RELATIONSHIP]: WF_STAGE.OWNERSHIP,
  [ENGINE_STAGE.PERSON]: WF_STAGE.PERSON,
  [ENGINE_STAGE.CONTACT]: WF_STAGE.CONTACT,
  [ENGINE_STAGE.EVIDENCE_VALIDATION]: WF_STAGE.SYNTHESIS,
  [ENGINE_STAGE.SYNTHESIS]: WF_STAGE.SYNTHESIS,
  [ENGINE_STAGE.COMPLETE]: WF_STAGE.COMPLETE,
  [ENGINE_STAGE.STOPPED]: WF_STAGE.STOPPED,
});

export const STAGE_IO_SCHEMAS = Object.freeze({
  [ENGINE_STAGE.INIT]: {
    in: ["hpcId", "objective", "mode", "runId"],
    out: ["caseId", "engineContract", "writeGuarantees"],
  },
  [ENGINE_STAGE.CANONICAL_HPC_LOOKUP]: {
    in: ["hpcId", "pilotManifest"],
    out: ["manifestEntry", "canonicalHpcId", "cohort"],
  },
  [ENGINE_STAGE.IDENTITY_RESOLUTION]: {
    in: ["canonicalHpcId", "hotelSeed"],
    out: ["identityStatus", "hotelIdentity", "failureClass?"],
  },
  [ENGINE_STAGE.OWNERSHIP]: {
    in: ["hotelIdentity"],
    out: ["ownershipClaims", "evidenceRefs"],
  },
  [ENGINE_STAGE.ORGANIZATION]: {
    in: ["ownershipClaims"],
    out: ["organizations", "orgDedupeKeys"],
  },
  [ENGINE_STAGE.RELATIONSHIP]: {
    in: ["canonicalHpcId", "organizations"],
    out: ["relationships", "relationshipDedupeKeys"],
  },
  [ENGINE_STAGE.PERSON]: {
    in: ["organizations"],
    out: ["persons", "personDedupeKeys"],
  },
  [ENGINE_STAGE.CONTACT]: {
    in: ["persons", "organizations"],
    out: ["channels", "channelDedupeKeys"],
  },
  [ENGINE_STAGE.EVIDENCE_VALIDATION]: {
    in: ["claims", "evidence"],
    out: ["validationOutcomes", "unsupportedClaims"],
  },
  [ENGINE_STAGE.SYNTHESIS]: {
    in: ["validatedPackage"],
    out: ["stagedPackage", "contactViewModel", "publicationState"],
  },
});

export function assertSameStageGraphForAllHotels(results) {
  const graphs = (results || []).map((r) => JSON.stringify(r.stage_graph || ENGINE_STAGE_GRAPH));
  if (!graphs.length) return { ok: false, reason: "NO_RESULTS" };
  const first = graphs[0];
  const mismatch = graphs.findIndex((g) => g !== first);
  if (mismatch >= 0) {
    return { ok: false, reason: "STAGE_GRAPH_MISMATCH", index: mismatch };
  }
  return { ok: true, stage_graph: ENGINE_STAGE_GRAPH };
}
