/**
 * Apply audited ownership corrections to staged CI store records.
 * Preserves prior claims in correction_history — never silent replace.
 */

import { STORE_VERSION } from "../contact-intelligence/store.js";
import { RELATIONSHIP } from "./denue-ownership-staging.js";
import {
  classifyContactPurpose,
  isUsableOwnerBusinessRoute,
} from "./contact-purpose-classifier.js";

export const OWNERSHIP_CLAIM_CORRECTIONS_VERSION = "ownership-claim-corrections-v1";

function nowIso() {
  return new Date().toISOString();
}

/**
 * @param {object} store
 * @param {object} correction
 */
export function applyOwnershipClaimCorrection(store, correction) {
  const hotelId = String(correction.hotel_id || "").trim();
  const ownerEntityId = String(correction.owner_entity_id || "").trim();
  if (!hotelId || !ownerEntityId) throw new Error("hotel_id_and_owner_entity_id_required");

  const existingOwner = store.getOwnerRoute(ownerEntityId) || {
    owner_entity_id: ownerEntityId,
    version: STORE_VERSION,
  };

  const history = Array.isArray(existingOwner.correction_history)
    ? existingOwner.correction_history.slice()
    : [];

  const entry = {
    at: nowIso(),
    hotel_id: hotelId,
    prior_relationship: correction.prior_relationship,
    supported_relationship: correction.supported_relationship,
    audit: correction.audit || null,
    action: correction.action || "DOWNGRADE_OR_CONFIRM",
    note: correction.note || null,
  };
  history.push(entry);

  let hotelClaims = Array.isArray(existingOwner.staged_hotel_ownership)
    ? existingOwner.staged_hotel_ownership.slice()
    : [];

  if (correction.remove_relationship_types?.length) {
    for (const rel of correction.remove_relationship_types) {
      const removed = hotelClaims.filter(
        (c) => c.hotel_id === hotelId && c.relationship_type === rel
      );
      for (const r of removed) {
        history.push({
          at: nowIso(),
          hotel_id: hotelId,
          prior_relationship: r.relationship_type,
          supported_relationship: correction.downgrade_to || "UNSUPPORTED",
          action: "REMOVED_UNSUPPORTED",
          audit: correction.audit,
        });
      }
      hotelClaims = hotelClaims.filter(
        (c) => !(c.hotel_id === hotelId && correction.remove_relationship_types.includes(c.relationship_type))
      );
    }
  }

  if (correction.add_claim) {
    const dup = hotelClaims.find(
      (c) =>
        c.hotel_id === hotelId && c.relationship_type === correction.add_claim.relationship_type
    );
    if (!dup) {
      hotelClaims.push({ hotel_id: hotelId, ...correction.add_claim });
    }
  }

  const auditedEvidence = [...(existingOwner.audited_ownership_evidence || [])];
  if (correction.audit) {
    auditedEvidence.push({
      ...correction.audit,
      hotel_id: hotelId,
      corrected_at: nowIso(),
    });
  }

  let organization_contacts = existingOwner.organization_contacts || null;
  let detached_contact_sources = Array.isArray(existingOwner.detached_contact_sources)
    ? existingOwner.detached_contact_sources.slice()
    : [];

  if (correction.contact_routes?.length) {
    organization_contacts = organization_contacts || {};
    for (const route of correction.contact_routes) {
      const purpose = route.purpose || classifyContactPurpose(route.email, route);
      if (route.detached) {
        detached_contact_sources.push({
          email: route.email,
          source_url: route.source_url,
          source_class: route.source_class,
          purpose,
          detached_at: nowIso(),
          reason: route.detached_reason || "Insufficient org attribution for owner route",
          preserve_source_record: true,
        });
        continue;
      }
      if (route.usable_owner_route && isUsableOwnerBusinessRoute(purpose)) {
        if (!organization_contacts.general_email) organization_contacts.general_email = route.email;
        organization_contacts[`purpose_${purpose.toLowerCase()}`] = route.email;
      }
    }
  }

  const ownerRoute = store.putOwnerRoute({
    ...existingOwner,
    owner_entity_id: ownerEntityId,
    staged_hotel_ownership: hotelClaims,
    correction_history: history.slice(-50),
    audited_ownership_evidence: auditedEvidence.slice(-40),
    organization_contacts,
    detached_contact_sources,
    supported_relationships: correction.supported_relationships || existingOwner.supported_relationships,
    evaluation_staging: true,
    airtable_write: false,
    census_ownership_write: false,
    updated_at: nowIso(),
  });

  const existingHotel = store.getHotelPackage(hotelId) || { hotel_id: hotelId };
  let stagedClaims = Array.isArray(existingHotel.staged_ownership_claims)
    ? existingHotel.staged_ownership_claims.slice()
    : [];

  stagedClaims = stagedClaims
    .map((c) => {
      if (c.owner_entity_id !== ownerEntityId) return c;
      if (correction.remove_relationship_types?.includes(c.relationship_type)) {
        return {
          ...c,
          superseded: true,
          superseded_at: nowIso(),
          superseded_by: correction.downgrade_to || "UNSUPPORTED",
        };
      }
      return c;
    })
    .filter(Boolean);

  if (correction.supported_relationships) {
    const active = stagedClaims.filter((c) => !c.superseded && c.owner_entity_id === ownerEntityId);
    for (const rel of correction.supported_relationships) {
      if (!active.some((c) => c.relationship_type === rel)) {
        stagedClaims.push({
          owner_entity_id: ownerEntityId,
          relationship_type: rel,
          evidence: correction.audit ? [correction.audit] : [],
          staged_at: nowIso(),
          from_correction: true,
        });
      }
    }
  }

  const detachedOnHotel = [...(existingHotel.detached_contact_sources || [])];
  for (const route of correction.contact_routes || []) {
    if (route.detached) {
      detachedOnHotel.push({
        email: route.email,
        source_url: route.source_url,
        purpose: route.purpose,
        detached_at: nowIso(),
        reason: route.detached_reason,
      });
    }
  }

  const hotelPkg = store.putHotelPackage({
    ...existingHotel,
    hotel_id: hotelId,
    staged_ownership_claims: stagedClaims,
    detached_contact_sources: detachedOnHotel,
    contact_routes_by_purpose: buildPurposeMap(correction.contact_routes),
    evaluation_staging: true,
    provenance: {
      summary: "ownership_claim_correction",
      version: OWNERSHIP_CLAIM_CORRECTIONS_VERSION,
    },
  });

  return { ok: true, owner_route: ownerRoute, hotel_package: hotelPkg, correction: entry };
}

function buildPurposeMap(routes = []) {
  const out = {};
  for (const r of routes || []) {
    if (!r.email) continue;
    const p = r.purpose || classifyContactPurpose(r.email, r);
    out[p] = out[p] || [];
    out[p].push({ email: r.email, usable_owner_route: r.usable_owner_route, detached: r.detached });
  }
  return out;
}

export function buildCorrectionsFromAudit(caseDef) {
  const corrections = [];
  for (const audit of caseDef.ownership_audits || []) {
    if (audit.supported === false && audit.downgrade_to) {
      corrections.push({
        hotel_id: caseDef.census_id,
        owner_entity_id: audit.entity_id || entityForClaim(caseDef, audit.claimed),
        prior_relationship: audit.claimed,
        supported_relationship: audit.downgrade_to,
        remove_relationship_types: [audit.claimed],
        downgrade_to: audit.downgrade_to,
        audit,
        note: `Unsupported ${audit.claimed} — ${audit.establishes}`,
      });
    }
  }
  return corrections;
}

function entityForClaim(caseDef, relationship) {
  if (relationship === "ECONOMIC_OWNER_OR_SPONSOR") {
    if (caseDef.census_id === "recL4PrLJpwXxyvV6") return "ent_the-palace-company";
    if (caseDef.census_id === "rec19X4tsCUM1A2q6") return "ent_barcelo-grupo";
  }
  if (caseDef.denue_razon) {
    const slug = caseDef.denue_razon
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "");
    return `ent_${slug}`;
  }
  return null;
}

export { RELATIONSHIP };
