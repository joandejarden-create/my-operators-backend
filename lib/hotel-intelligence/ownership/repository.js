/**
 * OwnershipRepository — persistence-agnostic interface.
 * MCP tools and domain services depend on this contract only.
 *
 * Implementations: createMemoryOwnershipRepository, createLocalOwnershipRepository
 */

export const OWNERSHIP_REPOSITORY_VERSION = "ownership-repository-v1";

/**
 * @typedef {import('./schemas.js').createOwnershipEntity extends Function ? ReturnType<typeof import('./schemas.js').createOwnershipEntity> : object} OwnershipEntity
 * @typedef {ReturnType<typeof import('./schemas.js').createEntityAlias>} EntityAlias
 * @typedef {ReturnType<typeof import('./schemas.js').createOwnershipRelationship>} OwnershipRelationship
 * @typedef {ReturnType<typeof import('./schemas.js').createRelationshipEvidence>} RelationshipEvidence
 * @typedef {ReturnType<typeof import('./schemas.js').createResearchRun>} OwnershipResearchRun
 */

/**
 * Method list every OwnershipRepository must implement.
 * Used by tests to assert adapter completeness.
 */
export const OWNERSHIP_REPOSITORY_METHODS = Object.freeze([
  "getEntity",
  "upsertEntity",
  "addAlias",
  "listAliases",
  "findByIdentifier",
  "findByNormalizedName",
  "getRelationship",
  "upsertRelationship",
  "listRelationshipsForHotel",
  "listRelationshipsForEntity",
  "addEvidence",
  "listEvidence",
  "createResearchRun",
  "updateResearchRun",
  "getResearchRun",
  "addObservation",
  "listObservationsForHotel",
  "listObservationsForSubject",
  "upsertResearchDossier",
  "getResearchDossier",
  "listResearchDossiersForHotel",
]);

/**
 * @param {object} repo
 * @returns {boolean}
 */
export function isOwnershipRepository(repo) {
  if (!repo || typeof repo !== "object") return false;
  return OWNERSHIP_REPOSITORY_METHODS.every((m) => typeof repo[m] === "function");
}
