/**
 * Packet 2.8B-2 — Owner Control Graph + Owner Portfolio Factory public API.
 */

export {
  OWNER_CONTROL_VOCAB_VERSION,
  ENTITY_TYPES,
  RELATIONSHIP_TYPES,
  PORTFOLIO_BUCKETS,
  ASSOCIATION_LEVELS,
  OWNER_ROLES,
  COMPLETENESS_STATUSES,
} from "./vocabulary.js";

export {
  PORTFOLIO_ATTRIBUTION_POLICY_VERSION,
  classifyPortfolioBucket,
  mayImportHotelsFromRelatedEntity,
  assertNoJvPartnerPortfolioLeak,
  OWNED_EXCLUSIONS,
} from "./attribution-policy.js";

export {
  OWNER_GRAPH_TRAVERSAL_POLICY_VERSION,
  TRAVERSAL_BY_RELATIONSHIP,
  mayTraverse,
  createTraversalBudget,
} from "./traversal-policy.js";

export {
  OWNER_ANCHOR_RESOLVER_VERSION,
  resolveOwnerAnchor,
  slugify,
} from "./owner-anchor.js";

export {
  OWNER_CONTROL_GRAPH_VERSION,
  createEmptyOwnerControlGraph,
  upsertNode,
  upsertEdge,
  listRelatedEntities,
  explainEntityAssociation,
} from "./control-graph.js";

export {
  OWNER_PORTFOLIO_FACTORY_VERSION,
  buildOwnerPortfolioProfile,
} from "./portfolio-factory.js";

export {
  OWNER_PORTFOLIO_COMPLETENESS_VERSION,
  assessPortfolioCompleteness,
} from "./completeness.js";

export {
  getOwnerPortfolioProfile,
  getOwnerControlGraph,
  getOwnerPortfolioRecord,
  writeOwnerPortfolioProfile,
  ensureGoldenOwnerPortfoliosMaterialized,
  resolveOwnerEntityId,
  resolveOwnerForHotel,
  listMaterializedOwnerIds,
  HOTEL_TO_OWNER,
} from "./portfolio-store.js";

export {
  ORGANIZATION_EVIDENCE_CORPUS_VERSION,
  getOrganizationEvidenceCorpus,
} from "./organization-evidence-corpus.js";

export {
  compileGsfOwnerPortfolioFromEvidence,
  GSF_OWNER_ENTITY_ID,
  GSF_SLUG,
  KGPV_AIRTABLE_ID,
} from "./compilers/gsf-from-evidence.js";

export {
  compileCambridgeOwnerPortfolioFromEvidence,
  CAMBRIDGE_AIRTABLE_ID,
  DOVETAIL_ENTITY_ID,
} from "./compilers/cambridge-from-evidence.js";

export {
  compileSheratonHnfOwnerPortfolioFromEvidence,
  compileVocoAllianceOwnerPortfolioFromEvidence,
  SHERATON_GDL_ID,
  VOCO_CANCUN_ID,
  HNF_ENTITY_ID,
  ALLIANCE_ENTITY_ID,
} from "./compilers/mexico-from-evidence.js";
