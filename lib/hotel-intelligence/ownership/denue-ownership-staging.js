/**
 * Stage DENUE / research-supported ownership claims into Contact Intelligence store.
 * Evaluation staging only — no Census canonical ownership writes.
 */

import { slugify } from "./owner-control/owner-anchor.js";
import { STORE_VERSION } from "../contact-intelligence/store.js";

export const DENUE_OWNERSHIP_STAGING_VERSION = "denue-ownership-staging-v1";

export const RELATIONSHIP = Object.freeze({
  REGISTERED_BUSINESS: "REGISTERED_BUSINESS",
  OPERATOR: "OPERATOR",
  LESSEE: "LESSEE",
  PROPERTY_OWNER: "PROPERTY_OWNER",
  ECONOMIC_OWNER_OR_SPONSOR: "ECONOMIC_OWNER_OR_SPONSOR",
  UNRESOLVED: "UNRESOLVED",
});

/**
 * Stable entity id for a Mexican registered business razón social.
 * @param {string} razonSocial
 */
export function entityIdFromRazonSocial(razonSocial) {
  const slug = slugify(String(razonSocial || "").trim());
  return slug ? `ent_${slug}` : null;
}

/**
 * @param {object} store — createContactStore()
 * @param {object} claim
 */
export function stageOwnershipClaim(store, claim) {
  if (!store?.putOwnerRoute || !store?.putHotelPackage) {
    throw new Error("contact_store_required");
  }
  const hotelId = String(claim.hotel_id || claim.census_id || "").trim();
  if (!hotelId) throw new Error("hotel_id_required");

  const ownerEntityId =
    claim.owner_entity_id ||
    (claim.razon_social ? entityIdFromRazonSocial(claim.razon_social) : null) ||
    (claim.economic_owner_name ? `ent_${slugify(claim.economic_owner_name)}` : null);

  if (!ownerEntityId) throw new Error("owner_entity_id_or_razon_required");
  if (ownerEntityId.startsWith("staged_indie_")) {
    throw new Error("provisional_indie_owner_key_not_allowed");
  }

  const now = new Date().toISOString();
  const evidenceEntry = {
    claim_id: claim.claim_id || `own_${slugify(claim.hotel_name || hotelId).slice(0, 40)}`,
    relationship_type: claim.relationship_type || RELATIONSHIP.UNRESOLVED,
    entity_display_name: claim.entity_display_name || claim.razon_social || claim.economic_owner_name,
    legal_name: claim.razon_social || claim.legal_name || null,
    uncertainty: claim.uncertainty || null,
    evidence_excerpt: claim.evidence_excerpt || null,
    source_url: claim.source_url || null,
    source_date: claim.source_date || null,
    retrieved_at: claim.retrieved_at || now,
    do_not_advance_source_date: true,
    registry_access_note: claim.registry_access_note || null,
  };

  const existingOwner = store.getOwnerRoute(ownerEntityId) || {
    owner_entity_id: ownerEntityId,
    version: STORE_VERSION,
  };

  const hotelClaims = Array.isArray(existingOwner.staged_hotel_ownership)
    ? existingOwner.staged_hotel_ownership.slice()
    : [];
  const dup = hotelClaims.find((c) => c.hotel_id === hotelId && c.relationship_type === evidenceEntry.relationship_type);
  if (!dup) hotelClaims.push({ hotel_id: hotelId, ...evidenceEntry });

  const ownerRoute = store.putOwnerRoute({
    ...existingOwner,
    owner_entity_id: ownerEntityId,
    owner_display_name: existingOwner.owner_display_name || evidenceEntry.entity_display_name,
    legal_name: existingOwner.legal_name || evidenceEntry.legal_name,
    canonical_domain: claim.canonical_domain || existingOwner.canonical_domain || null,
    organization_contacts: claim.organization_contacts || existingOwner.organization_contacts || null,
    staged_hotel_ownership: hotelClaims,
    staged_ownership_evidence: [...(existingOwner.staged_ownership_evidence || []), evidenceEntry].slice(-30),
    evaluation_staging: true,
    airtable_write: false,
    census_ownership_write: false,
    updated_at: now,
  });

  const existingHotel = store.getHotelPackage(hotelId) || { hotel_id: hotelId };
  const stagedForHotel = Array.isArray(existingHotel.staged_ownership_claims)
    ? existingHotel.staged_ownership_claims.slice()
    : [];
  if (!stagedForHotel.some((c) => c.owner_entity_id === ownerEntityId && c.relationship_type === evidenceEntry.relationship_type)) {
    stagedForHotel.push({
      owner_entity_id: ownerEntityId,
      owner_display_name: evidenceEntry.entity_display_name,
      relationship_type: evidenceEntry.relationship_type,
      uncertainty: evidenceEntry.uncertainty,
      evidence: [evidenceEntry],
      staged_at: now,
    });
  }

  const hotelPkg = store.putHotelPackage({
    ...existingHotel,
    hotel_id: hotelId,
    hotel_name: claim.hotel_name || existingHotel.hotel_name,
    staged_ownership_claims: stagedForHotel,
    evaluation_staging: true,
    airtable_write: false,
    provenance: {
      summary: "denue_ownership_staging",
      version: DENUE_OWNERSHIP_STAGING_VERSION,
    },
  });

  return {
    ok: true,
    owner_entity_id: ownerEntityId,
    owner_route: ownerRoute,
    hotel_package: hotelPkg,
    evidence_entry: evidenceEntry,
  };
}
