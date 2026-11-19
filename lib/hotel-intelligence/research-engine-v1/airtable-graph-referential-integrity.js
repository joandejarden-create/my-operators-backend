/**
 * Referential integrity for HI Hotel–Org Relationships ↔ HPC + Organizations.
 * Cardinality: exactly one HPC hotel link per relationship (application-enforced).
 */

import {
  GRAPH_BASE_ID_CANONICAL,
  HPC_TABLE_ID,
  HI_GRAPH_TABLES,
  MAP_HI_HOTEL_ORG_RELATIONSHIP,
  assertCanonicalGraphBase,
} from "./airtable-graph-contract-v1.js";

export const REFERENTIAL_INTEGRITY_VERSION = "he-intel-hpc-link-integrity-v1";

/**
 * Validate a planned hotel–org relationship before create/update.
 *
 * @param {object} input
 * @param {string} input.hpcHotelId - canonical HPC record id
 * @param {string[]} [input.hpcHotelLinkIds] - native linked record ids
 * @param {string} [input.organizationId]
 * @param {string[]} [input.organizationLinkIds]
 * @param {object} [resolved]
 * @param {object|null} [resolved.hpcRecord]
 * @param {object|null} [resolved.orgRecord]
 * @param {boolean} [resolved.orgCreateApproved]
 */
export function validateHotelOrgRelationshipRefs(input = {}, resolved = {}) {
  const held = [];
  const hpcHotelId = String(input.hpcHotelId || "").trim();
  const hpcLinks = Array.isArray(input.hpcHotelLinkIds)
    ? input.hpcHotelLinkIds.map(String).filter(Boolean)
    : [];
  const orgId = String(input.organizationId || "").trim();
  const orgLinks = Array.isArray(input.organizationLinkIds)
    ? input.organizationLinkIds.map(String).filter(Boolean)
    : [];

  if (!hpcHotelId) {
    return {
      ok: false,
      error: "HPC_HOTEL_ID_REQUIRED",
      held_for_review: [{ reason: "MISSING_HPC_HOTEL_ID" }],
    };
  }
  if (!/^rec[a-zA-Z0-9]{14}$/.test(hpcHotelId)) {
    return {
      ok: false,
      error: "HPC_HOTEL_ID_INVALID",
      held_for_review: [{ reason: "INVALID_HPC_RECORD_ID", hpcHotelId }],
    };
  }

  // Cardinality: exactly one HPC link
  if (hpcLinks.length === 0) {
    held.push({ reason: "MISSING_HPC_NATIVE_LINK", hpcHotelId });
  } else if (hpcLinks.length > 1) {
    held.push({
      reason: "HPC_CARDINALITY_VIOLATION",
      expected: 1,
      actual: hpcLinks.length,
      links: hpcLinks,
    });
  } else if (hpcLinks[0] !== hpcHotelId) {
    held.push({
      reason: "HPC_ID_LINK_MISMATCH",
      hpcHotelId,
      linkedId: hpcLinks[0],
    });
  }

  if (!resolved.hpcRecord) {
    held.push({ reason: "HPC_RECORD_NOT_FOUND", hpcHotelId });
  } else if (resolved.hpcRecord.id && resolved.hpcRecord.id !== hpcHotelId) {
    held.push({
      reason: "HPC_RESOLVED_ID_MISMATCH",
      hpcHotelId,
      resolvedId: resolved.hpcRecord.id,
    });
  }

  if (!orgId && !resolved.orgCreateApproved) {
    held.push({ reason: "ORGANIZATION_UNRESOLVED" });
  }
  if (orgLinks.length > 1) {
    held.push({
      reason: "ORG_CARDINALITY_VIOLATION",
      expected: 1,
      actual: orgLinks.length,
    });
  }
  if (orgId && orgLinks.length === 1 && orgLinks[0] !== orgId && !resolved.orgAirtableRecId) {
    // orgId is dle_… while Airtable rec id differs — allow when resolved.orgAirtableRecId matches link
    held.push({
      reason: "ORG_LINK_AMBIGUOUS",
      organizationId: orgId,
      linkedId: orgLinks[0],
    });
  }
  if (
    orgLinks.length === 1 &&
    resolved.orgAirtableRecId &&
    orgLinks[0] !== resolved.orgAirtableRecId
  ) {
    held.push({
      reason: "ORG_LINK_MISMATCH",
      expected: resolved.orgAirtableRecId,
      linkedId: orgLinks[0],
    });
  }
  if (!resolved.orgRecord && !resolved.orgCreateApproved) {
    held.push({ reason: "ORGANIZATION_RECORD_NOT_FOUND", organizationId: orgId || null });
  }

  const blocking = held.filter((h) =>
    [
      "MISSING_HPC_NATIVE_LINK",
      "HPC_CARDINALITY_VIOLATION",
      "HPC_ID_LINK_MISMATCH",
      "HPC_RECORD_NOT_FOUND",
      "HPC_RESOLVED_ID_MISMATCH",
      "ORGANIZATION_UNRESOLVED",
      "ORGANIZATION_RECORD_NOT_FOUND",
      "ORG_CARDINALITY_VIOLATION",
      "ORG_LINK_MISMATCH",
    ].includes(h.reason)
  );

  return {
    ok: blocking.length === 0,
    version: REFERENTIAL_INTEGRITY_VERSION,
    hpcHotelId,
    hpcHotelLinkIds: hpcLinks,
    organizationId: orgId || null,
    organizationLinkIds: orgLinks,
    held_for_review: held,
    error: blocking[0]?.reason || null,
  };
}

/**
 * Build Airtable fields payload for a relationship with native links.
 */
export function buildRelationshipAirtableFields({
  relationshipId,
  hpcHotelId,
  organizationCanonicalId,
  organizationAirtableRecId,
  relationshipType,
  dedupeKey,
  confidence = "UNVERIFIED",
  notes = null,
  schemaVersion,
} = {}) {
  const M = MAP_HI_HOTEL_ORG_RELATIONSHIP;
  const gate = validateHotelOrgRelationshipRefs(
    {
      hpcHotelId,
      hpcHotelLinkIds: [hpcHotelId],
      organizationId: organizationCanonicalId,
      organizationLinkIds: organizationAirtableRecId ? [organizationAirtableRecId] : [],
    },
    {
      hpcRecord: { id: hpcHotelId },
      orgRecord: organizationAirtableRecId ? { id: organizationAirtableRecId } : null,
      orgAirtableRecId: organizationAirtableRecId || null,
      orgCreateApproved: false,
    }
  );
  if (!gate.ok) {
    return { ok: false, ...gate };
  }
  return {
    ok: true,
    table: HI_GRAPH_TABLES.hotelOrgRelationships,
    fields: {
      [M.relationshipId]: relationshipId,
      [M.hpcHotelId]: hpcHotelId,
      [M.hpcHotelLink]: [hpcHotelId],
      [M.organizationId]: organizationCanonicalId,
      [M.organizationLink]: organizationAirtableRecId ? [organizationAirtableRecId] : [],
      [M.relationshipType]: relationshipType,
      [M.current]: true,
      [M.confidence]: confidence,
      [M.dedupeKey]: dedupeKey,
      [M.publicationState]: "INTERNAL_ONLY",
      [M.schemaVersion]: schemaVersion || "he_intel_airtable_graph_v1",
      ...(notes ? { [M.notes]: notes } : {}),
    },
  };
}

export function assertWriteBase(baseId) {
  return assertCanonicalGraphBase(baseId || GRAPH_BASE_ID_CANONICAL);
}

export { HPC_TABLE_ID, GRAPH_BASE_ID_CANONICAL };
