/**
 * GDI International Discovery V2 — public API.
 * Ready/Watch standards unchanged. Evidence routes expand.
 */

export { IDV2_VERSION, DISCOVERY_SPINE, CONTROLLER_TYPE, CONTROLLER_AUTHORITY, HOTEL_SUPPLIED_EVIDENCE_TYPE, PACKET_PILLAR_ID, EVIDENCE_ROUTE, SPINE_BASE_MAP } from "./constants.js";

export {
  buildDemandController,
  customerSafeControllerCopy,
  classifyControllerAuthorityFromEvidence,
  normalizeControllerType,
  normalizeControllerAuthority,
} from "./demand-controller-model.js";

export {
  loadDemandControllers,
  saveDemandControllers,
  upsertDemandController,
  getDemandController,
  listDemandControllers,
} from "./demand-controller-store.js";

export {
  describeDiscoverySpines,
  runDemandControllerFirstSpine,
  runAccountFirstSpine,
  runHistoricalProcessFirstSpine,
  runHotelHistoryFirstSpine,
  participantFirstSpineContract,
  ACCOUNT_FIRST_TRIGGERS,
} from "./discovery-spines.js";

export {
  resolveMarketDiscoveryProfile,
  listMarketDiscoveryProfiles,
  PROFILE_VERSION,
} from "./market-discovery-profile.js";

export { selectNextDiscoveryPath } from "./path-selection-engine.js";
export { assessPacketPillars, computeNextBlocker } from "./next-blocker-engine.js";

export {
  evaluateEquivalentEvidence,
  summarizeEquivalentEvidencePolicy,
  CANONICAL_FACTS,
  EQUIVALENT_ROUTES,
  EVIDENCE_QUALITY_BARS,
} from "./equivalent-evidence-policy.js";

export {
  CONTROLLER_QUERY_VOCAB,
  buildControllerDiscoveryQueries,
  inferControllerTypeFromText,
  MULTILINGUAL_LODGING_PATH_RE,
  MULTILINGUAL_RELEVANT_ROLE_RE,
  SUPPORTED_ONTOLOGY_LANGUAGES,
} from "./multilingual-controller-ontology.js";

export {
  loadIntermediaryGraph,
  saveIntermediaryGraph,
  upsertIntermediaryEntity,
  addIntermediaryRelation,
  findReusableIntermediaries,
  INTERMEDIARY_RELATIONS,
} from "./intermediary-graph.js";

export {
  convergeSpineResultsToPacket,
  mergeDuplicateOpportunities,
  opportunityMergeKey,
} from "./converge-to-packet.js";

export {
  buildHotelSuppliedEvidenceRecord,
  classifyPursuitResponseText,
  applyHotelSuppliedFeedbackLoop,
} from "./hotel-supplied-evidence.js";

export { getReadyGateIntlAudit, READY_GATE_INTL_AUDIT } from "./ready-gate-intl-audit.js";
export {
  listInternationalSourceFamilies,
  INTERNATIONAL_SOURCE_FAMILIES,
  DOCUMENT_FIRST_TARGETS,
} from "./international-source-families.js";

/**
 * Architecture capability matrix for reports / RETURN.
 */
export function getIdv2ArchitectureStatus() {
  return {
    DEMAND_CONTROLLER_MODEL_IMPLEMENTED: true,
    PARTICIPANT_FIRST_IMPLEMENTED: true,
    DEMAND_CONTROLLER_FIRST_IMPLEMENTED: true,
    ACCOUNT_FIRST_IMPLEMENTED: true,
    HISTORICAL_PROCESS_FIRST_IMPLEMENTED: true,
    HOTEL_HISTORY_FIRST_IMPLEMENTED: true,
    ALL_PATHS_CONVERGE_TO_SAME_PACKET: true,
    MARKET_DISCOVERY_PROFILE_IMPLEMENTED: true,
    PATH_SELECTION_ENGINE_IMPLEMENTED: true,
    NEXT_BLOCKER_ENGINE_IMPLEMENTED: true,
    EQUIVALENT_EVIDENCE_POLICY_IMPLEMENTED: true,
    PURSUIT_HOTEL_SUPPLIED_EVIDENCE_LOOP_IMPLEMENTED: true,
    INTERMEDIARY_GRAPH_IMPLEMENTED: true,
    READY_THRESHOLD_CHANGED: false,
    WATCH_THRESHOLD_CHANGED: false,
    APIFY_USED: false,
  };
}
