/**
 * Ownership query helpers — empty-safe MCP payloads.
 */

import { rollupVerificationStatus } from "./confidence-map.js";
import { RELATIONSHIP_TYPE_TO_ROLE } from "./ontology.js";
import { isEntityId } from "./ids.js";
import { normalizeEntityName } from "./ids.js";
import { deriveOrganizationAggregates } from "./portfolio/organization-aggregates.js";

/**
 * @param {import('./memory-repository.js').createMemoryOwnershipRepository extends Function ? any : object} repo
 * @param {object} input
 */
export async function queryOwnerGet(repo, input = {}) {
  const entityId = String(input.entity_id || "").trim();
  const name = String(input.name || "").trim();
  const identifier = input.identifier || null;

  /** @type {object[]} */
  let entities = [];

  if (entityId) {
    if (!isEntityId(entityId)) {
      return {
        ok: true,
        status: "unknown",
        reason: "invalid_entity_id",
        entities: [],
      };
    }
    const one = await repo.getEntity(entityId);
    if (one) entities = [one];
  } else if (identifier?.kind && identifier?.value) {
    entities = await repo.findByIdentifier(identifier.kind, identifier.value);
  } else if (name) {
    entities = await repo.findByNormalizedName(name);
  } else {
    return {
      ok: true,
      status: "unknown",
      reason: "missing_query",
      entities: [],
    };
  }

  if (!entities.length) {
    return {
      ok: true,
      status: "unknown",
      reason: "not_found",
      entities: [],
    };
  }

  const enriched = [];
  for (const ent of entities) {
    const aliases = await repo.listAliases(ent.entity_id);
    const relationships = await repo.listRelationshipsForEntity(ent.entity_id, {
      currentOnly: true,
    });
    enriched.push({
      entity: ent,
      aliases,
      relationship_count: relationships.length,
      relationships: relationships.slice(0, 50),
    });
  }

  return {
    ok: true,
    status: "found",
    entities: enriched,
    organization: input.include_organization
      ? await deriveOrganizationAggregates(repo, enriched[0].entity.entity_id, {
          censusRecords: input.censusRecords,
          idRegistry: input.idRegistry,
        })
      : null,
  };
}

/**
 * @param {object} repo
 * @param {object} input
 * @param {{ hotelExists?: boolean | null }} [ctx]
 */
export async function queryHotelOwnership(repo, input = {}, ctx = {}) {
  const hotelId = String(input.hotel_id || "").trim();
  if (!hotelId) {
    return {
      ok: true,
      hotel_id: null,
      hotel_lookup: "not_found",
      ownership_status: "unknown",
      relationships: [],
      summary: emptySummary(),
      verification: emptyVerification(),
    open_review_items: [],
    observations: [],
    dossiers: [],
    reason: "hotel_id_required",
    };
  }

  const hotelExists =
    ctx.hotelExists === undefined || ctx.hotelExists === null
      ? true
      : Boolean(ctx.hotelExists);

  if (!hotelExists) {
    return {
      ok: true,
      hotel_id: hotelId,
      hotel_lookup: "not_found",
      ownership_status: "unknown",
      relationships: [],
      summary: emptySummary(),
      verification: emptyVerification(),
      open_review_items: [],
      observations: [],
      dossiers: [],
    };
  }

  const includeHistorical = Boolean(input.include_historical);
  const includeEvidence = Boolean(input.include_evidence);

  let relationships = await repo.listRelationshipsForHotel(hotelId, {
    currentOnly: !includeHistorical,
  });

  const outRels = [];
  for (const rel of relationships) {
    const objectEntity = await repo.getEntity(rel.object_entity_id);
    const row = {
      ...rel,
      role: rel.role || RELATIONSHIP_TYPE_TO_ROLE[rel.relationship_type] || null,
      object_entity: objectEntity,
    };
    if (includeEvidence) {
      row.evidence = await repo.listEvidence(rel.relationship_id);
    }
    outRels.push(row);
  }

  const summary = buildSummary(outRels);
  const verification = buildVerification(outRels);
  const ownershipStatus = deriveOwnershipStatus(outRels, verification);

  let observations = [];
  let dossiers = [];
  if (typeof repo.listObservationsForHotel === "function") {
    observations = await repo.listObservationsForHotel(hotelId);
  }
  if (typeof repo.listResearchDossiersForHotel === "function") {
    dossiers = await repo.listResearchDossiersForHotel(hotelId);
  }

  return {
    ok: true,
    hotel_id: hotelId,
    hotel_lookup: "found",
    ownership_status: ownershipStatus,
    relationships: outRels,
    observations,
    dossiers,
    summary,
    verification,
    open_review_items: [],
  };
}

function emptySummary() {
  return {
    propco: null,
    parent: null,
    ultimate_sponsor: null,
    operator: null,
    developer: null,
    brand: null,
    asset_manager: null,
  };
}

function emptyVerification() {
  return {
    propco: "unknown",
    parent: "unknown",
    ultimate_sponsor: "unknown",
    operator: "unknown",
    developer: "unknown",
    brand: "unknown",
    asset_manager: "unknown",
  };
}

function pickCurrent(rels, type) {
  return (
    rels.find((r) => r.relationship_type === type && r.is_current !== false) ||
    null
  );
}

function entityRef(rel) {
  if (!rel) return null;
  const e = rel.object_entity;
  return {
    entity_id: rel.object_entity_id,
    display_name: e?.display_name || e?.legal_name || null,
    legal_name: e?.legal_name || null,
    entity_type: e?.entity_type || null,
    relationship_id: rel.relationship_id,
    verification_status: rel.verification_status,
    confidence: rel.confidence,
  };
}

function buildSummary(rels) {
  return {
    propco: entityRef(pickCurrent(rels, "OWNED_BY")),
    parent: entityRef(pickCurrent(rels, "CONTROLLED_BY")),
    ultimate_sponsor: entityRef(pickCurrent(rels, "SPONSORED_BY")),
    operator: entityRef(pickCurrent(rels, "OPERATED_BY")),
    developer: entityRef(pickCurrent(rels, "DEVELOPED_BY")),
    brand: entityRef(pickCurrent(rels, "BRANDED_BY")),
    asset_manager: entityRef(pickCurrent(rels, "ASSET_MANAGED_BY")),
  };
}

function buildVerification(rels) {
  const keys = [
    ["propco", "OWNED_BY"],
    ["parent", "CONTROLLED_BY"],
    ["ultimate_sponsor", "SPONSORED_BY"],
    ["operator", "OPERATED_BY"],
    ["developer", "DEVELOPED_BY"],
    ["brand", "BRANDED_BY"],
    ["asset_manager", "ASSET_MANAGED_BY"],
  ];
  const out = emptyVerification();
  for (const [key, type] of keys) {
    const matches = rels.filter(
      (r) => r.relationship_type === type && r.is_current !== false
    );
    out[key] = rollupVerificationStatus(
      matches.map((m) => m.verification_status)
    );
  }
  return out;
}

function deriveOwnershipStatus(rels, verification) {
  if (!rels.length) return "unknown";
  if (Object.values(verification).includes("conflict")) return "conflict";
  if (verification.propco === "verified" || verification.propco === "high") {
    return "partial";
  }
  if (rels.some((r) => r.verification_status !== "unknown")) return "partial";
  return "unknown";
}

export { normalizeEntityName };
