/**
 * Admin evaluation readback for staged ownership + contact classifications.
 * Reads CI store directly — proves persistence without Census writes.
 */

import { createContactStore } from "../contact-intelligence/store.js";
import { classifyContactPurpose, isUsableOwnerBusinessRoute } from "./contact-purpose-classifier.js";
import { getHotelCase } from "./nine-case-evidence-registry.js";

export const STAGED_EVALUATION_READBACK_VERSION = "staged-evaluation-readback-v1";

/**
 * @param {string} hotelId
 * @param {object} opts
 */
export function readStagedHotelEvaluation(hotelId, opts = {}) {
  const store = opts.store || createContactStore();
  const pkg = store.getHotelPackage(hotelId);
  const caseDef = getHotelCase(hotelId);

  const ownerIds = new Set();
  for (const c of pkg?.staged_ownership_claims || []) {
    if (!c.superseded) ownerIds.add(c.owner_entity_id);
  }
  for (const c of caseDef?.supported_relationships || []) {
    /* relationships keyed on case */
  }

  const owners = [];
  for (const oid of ownerIds) {
    const route = store.getOwnerRoute(oid);
    owners.push(readOwnerEvaluation(oid, { store, hotel_id: hotelId }));
  }

  const contactRoutes = (caseDef?.contact_routes || []).map((r) => ({
    ...r,
    purpose: r.purpose || classifyContactPurpose(r.email, r),
    usable_owner_route: r.usable_owner_route ?? isUsableOwnerBusinessRoute(r.purpose),
  }));

  return {
    version: STAGED_EVALUATION_READBACK_VERSION,
    hotel_id: hotelId,
    hotel_name: pkg?.hotel_name || caseDef?.hotel,
    staged_ownership_claims: pkg?.staged_ownership_claims || [],
    detached_contact_sources: pkg?.detached_contact_sources || [],
    contact_routes_by_purpose: pkg?.contact_routes_by_purpose || {},
    owners,
    ownership_audits: caseDef?.ownership_audits || [],
    supported_relationships: caseDef?.supported_relationships || [],
    readback_at: new Date().toISOString(),
  };
}

export function readOwnerEvaluation(ownerEntityId, opts = {}) {
  const store = opts.store || createContactStore();
  const route = store.getOwnerRoute(ownerEntityId);
  const hotelFilter = opts.hotel_id || null;

  const claims = (route?.staged_hotel_ownership || []).filter(
    (c) => !hotelFilter || c.hotel_id === hotelFilter
  );

  const v2 = route?.contact_resolution_v2 || null;
  const org = route?.organization_contacts || {};
  const orgRoutes = [];
  for (const [k, v] of Object.entries(org)) {
    if (!v || k === "website") continue;
    const purpose = classifyContactPurpose(v, { source_class: k.includes("denue") ? "DENUE_REGISTRY" : null });
    orgRoutes.push({
      field: k,
      email: v,
      purpose,
      usable_owner_route: isUsableOwnerBusinessRoute(purpose),
    });
  }

  return {
    owner_entity_id: ownerEntityId,
    owner_display_name: route?.owner_display_name,
    supported_relationships: route?.supported_relationships || null,
    active_claims: claims.filter((c) => !c.superseded),
    correction_history: route?.correction_history || [],
    audited_ownership_evidence: route?.audited_ownership_evidence || [],
    detached_contact_sources: route?.detached_contact_sources || [],
    organization_routes: orgRoutes,
    contact_resolution_v2: v2
      ? {
          resolution_status: v2.resolution_status,
          contacts: (v2.contacts || []).map((c) => ({
            full_name: c.full_name,
            title: c.title,
            email: c.email,
            email_status: c.email_status,
            recovery_source: c.recovery_source,
            provider: c.provider,
            provider_email_status: c.provider_email_status,
            usage_rights: c.usage_rights,
            email_publishable: c.email_publishable,
          })),
        }
      : null,
    people_from_legacy_route: route?.people || null,
  };
}

export function proveNineCaseReadback(opts = {}) {
  const store = opts.store || createContactStore();
  const ids = [
    "rec2ossLX1BBaaZuw",
    "recL4PrLJpwXxyvV6",
    "recVTH9bA98rdzJ1A",
    "recGL82SfUlRB2adK",
    "recxjhraXKAz0BaVH",
    "recPTwb8RtoFYAxmF",
    "rec19X4tsCUM1A2q6",
    "recrEDeuOcnTkMnTf",
    "recgg7Llf3EWpZqIQ",
  ];
  return ids.map((id) => readStagedHotelEvaluation(id, { store }));
}
