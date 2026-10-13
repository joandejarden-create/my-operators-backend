/**
 * Packet 2.8B-2 — OwnerAnchor resolver.
 * Prefer economic / control / property-owning org — never operator/brand/developer by default.
 */

import { OWNER_ROLES, TEMPORAL_STATUSES } from "./vocabulary.js";

export const OWNER_ANCHOR_RESOLVER_VERSION = "owner-anchor-resolver-v1";

const DISALLOWED_AUTO_ANCHOR_ROLES = new Set([
  "operator",
  "brand",
  "developer",
  "asset_manager",
  "manager",
]);

/**
 * @param {{
 *   hotel_id?: string,
 *   airtable_record_id?: string,
 *   hotel_name?: string,
 *   ownership_and_control?: object,
 *   ownership_chain?: Array<object>,
 *   economic_owner_verified?: boolean,
 * }} hotelOwnership
 */
export function resolveOwnerAnchor(hotelOwnership = {}) {
  const hotelId =
    hotelOwnership.hotel_id ||
    hotelOwnership.airtable_record_id ||
    hotelOwnership.hotel?.airtable_record_id ||
    null;
  const oac =
    hotelOwnership.ownership_and_control ||
    hotelOwnership.report?.ownership_and_control ||
    {};
  const chain = hotelOwnership.ownership_chain || hotelOwnership.report?.ownership_chain || [];

  const economic = oac.economic_owner_or_group || {};
  const propco = oac.legal_property_owner_propco || {};
  const operator = oac.operator || {};
  const brand = oac.brand || {};

  // Reject operator/brand auto-anchor
  if (economic.name && DISALLOWED_AUTO_ANCHOR_ROLES.has(String(economic.role || "").toLowerCase())) {
    return fail(hotelId, "economic_owner_role_disallowed");
  }

  // Prefer verified economic owner
  if (economic.known && economic.name && economic.status !== "NOT_YET_VERIFIED") {
    const fromChain = chain.find((n) => /economic_owner/i.test(String(n.role || "")));
    return ok({
      hotel_id: hotelId,
      primary_owner_entity_id: economic.entity_id || slugify(economic.name),
      owner_display_name: economic.name,
      owner_role: OWNER_ROLES[0], // ECONOMIC_OWNER
      confidence: mapConfidence(economic.status || economic.confidence || fromChain?.confidence),
      evidence: economic.note || fromChain?.note || null,
      temporal_status: TEMPORAL_STATUSES[0],
      propco: propco.known
        ? { name: propco.name, entity_id: propco.entity_id || slugify(propco.name) }
        : null,
      rejected_auto_anchors: {
        operator: operator.name || null,
        brand: brand.name || null,
      },
    });
  }

  // Chain fallback: economic_owner node
  const econNode = chain.find(
    (n) =>
      /economic_owner/i.test(String(n.role || "")) &&
      n.name &&
      String(n.status || "").toUpperCase() !== "NOT_VERIFIED"
  );
  if (econNode) {
    return ok({
      hotel_id: hotelId,
      primary_owner_entity_id: slugify(econNode.name),
      owner_display_name: econNode.name,
      owner_role: OWNER_ROLES[0],
      confidence: mapConfidence(econNode.confidence || econNode.status),
      evidence: econNode.note || null,
      temporal_status: TEMPORAL_STATUSES[0],
      propco: null,
      rejected_auto_anchors: {
        operator: operator.name || null,
        brand: brand.name || null,
      },
    });
  }

  // Property-owning org / PropCo as weaker anchor when economic unknown
  const propcoNode = chain.find((n) => /propco/i.test(String(n.role || "")) && n.name);
  if (propco.known && propco.name) {
    return ok({
      hotel_id: hotelId,
      primary_owner_entity_id: propco.entity_id || slugify(propco.name),
      owner_display_name: propco.name,
      owner_role: OWNER_ROLES[3], // PROPCO
      confidence: mapConfidence(propco.status || "PROBABLE"),
      evidence: propco.note || "PropCo anchor — economic owner unresolved",
      temporal_status: TEMPORAL_STATUSES[0],
      propco: { name: propco.name, entity_id: propco.entity_id || slugify(propco.name) },
      rejected_auto_anchors: {
        operator: operator.name || null,
        brand: brand.name || null,
      },
    });
  }
  if (propcoNode) {
    return ok({
      hotel_id: hotelId,
      primary_owner_entity_id: slugify(propcoNode.name),
      owner_display_name: propcoNode.name,
      owner_role: OWNER_ROLES[3],
      confidence: mapConfidence(propcoNode.confidence || "PROBABLE"),
      evidence: propcoNode.note || null,
      temporal_status: TEMPORAL_STATUSES[0],
      propco: { name: propcoNode.name, entity_id: slugify(propcoNode.name) },
      rejected_auto_anchors: {
        operator: operator.name || null,
        brand: brand.name || null,
      },
    });
  }

  return fail(hotelId, "no_economic_or_propco_anchor", {
    rejected_auto_anchors: {
      operator: operator.name || null,
      brand: brand.name || null,
    },
  });
}

function ok(payload) {
  return { ok: true, ...payload };
}

function fail(hotelId, reason, extra = {}) {
  return {
    ok: false,
    hotel_id: hotelId,
    primary_owner_entity_id: null,
    owner_role: OWNER_ROLES[5],
    confidence: "UNKNOWN",
    evidence: null,
    temporal_status: TEMPORAL_STATUSES[6],
    reason,
    ...extra,
  };
}

function mapConfidence(raw) {
  const s = String(raw || "").toUpperCase();
  if (/HIGH|VERIFIED/.test(s)) return "HIGH";
  if (/PROBABLE|PARTIAL/.test(s)) return "PROBABLE";
  if (/LOW|WEAK/.test(s)) return "LOW";
  return s || "UNKNOWN";
}

export function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 80);
}
