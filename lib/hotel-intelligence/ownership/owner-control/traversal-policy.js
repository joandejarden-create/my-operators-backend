/**
 * Packet 2.8B-2 — OwnerGraphTraversalPolicy.
 * Bounds graph expansion for control portfolio vs association display.
 */

export const OWNER_GRAPH_TRAVERSAL_POLICY_VERSION = "owner-graph-traversal-policy-v1";

/** @typedef {'YES'|'NO'|'CONDITIONAL'} TraverseFlag */

/**
 * @type {Record<string, { for_control_portfolio: TraverseFlag, for_related_entities: TraverseFlag, note: string }>}
 */
export const TRAVERSAL_BY_RELATIONSHIP = Object.freeze({
  CONTROLLED_BY: {
    for_control_portfolio: "YES",
    for_related_entities: "YES",
    note: "Follow control chains toward / from anchor.",
  },
  OWNED_BY: {
    for_control_portfolio: "YES",
    for_related_entities: "YES",
    note: "Ownership edges are portfolio-relevant.",
  },
  PARENT_OF: {
    for_control_portfolio: "CONDITIONAL",
    for_related_entities: "YES",
    note: "Only when evidence direction is parent→subsidiary control.",
  },
  SUBSIDIARY_OF: {
    for_control_portfolio: "YES",
    for_related_entities: "YES",
    note: "Subsidiary under control may carry owned hotels.",
  },
  JV_WITH: {
    for_control_portfolio: "CONDITIONAL",
    for_related_entities: "YES",
    note: "Related entity yes; hotel fan-out only with control evidence.",
  },
  SPONSORED_BY: {
    for_control_portfolio: "CONDITIONAL",
    for_related_entities: "YES",
    note: "Economic interest may classify separately; not auto-owned.",
  },
  OPERATED_BY: {
    for_control_portfolio: "NO",
    for_related_entities: "YES",
    note: "Operator edges never expand ownership portfolio.",
  },
  ASSET_MANAGED_BY: {
    for_control_portfolio: "NO",
    for_related_entities: "YES",
    note: "Management ≠ ownership.",
  },
  BRANDED_BY: {
    for_control_portfolio: "NO",
    for_related_entities: "NO",
    note: "Brand is not an owner-universe traversal edge.",
  },
  DEVELOPED_BY: {
    for_control_portfolio: "NO",
    for_related_entities: "YES",
    note: "Developer association only.",
  },
  LEASED_FROM: {
    for_control_portfolio: "NO",
    for_related_entities: "YES",
    note: "Lessor is related, not ownership portfolio fan-out.",
  },
  RELATED: {
    for_control_portfolio: "NO",
    for_related_entities: "CONDITIONAL",
    note: "Weak association — display only with evidence.",
  },
  UNKNOWN: {
    for_control_portfolio: "NO",
    for_related_entities: "NO",
    note: "Do not traverse unknown edges.",
  },
});

export const DEFAULT_MAX_HOPS_CONTROL = 3;
export const DEFAULT_MAX_HOPS_RELATED = 2;
export const DEFAULT_MAX_RELATED_ENTITIES = 40;
export const DEFAULT_MAX_PORTFOLIO_HOTELS = 200;

/**
 * @param {string} relationshipType
 * @param {'control_portfolio'|'related_entities'} purpose
 * @param {{ control_evidence?: boolean }} [opts]
 */
export function mayTraverse(relationshipType, purpose, opts = {}) {
  const rel = String(relationshipType || "UNKNOWN").toUpperCase();
  const rule = TRAVERSAL_BY_RELATIONSHIP[rel] || TRAVERSAL_BY_RELATIONSHIP.UNKNOWN;
  const flag =
    purpose === "control_portfolio" ? rule.for_control_portfolio : rule.for_related_entities;

  if (flag === "YES") return { ok: true, flag, note: rule.note };
  if (flag === "NO") return { ok: false, flag, note: rule.note };
  // CONDITIONAL
  if (purpose === "control_portfolio") {
    return {
      ok: Boolean(opts.control_evidence),
      flag,
      note: rule.note,
      requires: "control_evidence",
    };
  }
  return { ok: true, flag, note: rule.note };
}

/**
 * Bound BFS expansion.
 */
export function createTraversalBudget(overrides = {}) {
  return {
    max_hops_control: overrides.max_hops_control ?? DEFAULT_MAX_HOPS_CONTROL,
    max_hops_related: overrides.max_hops_related ?? DEFAULT_MAX_HOPS_RELATED,
    max_related_entities: overrides.max_related_entities ?? DEFAULT_MAX_RELATED_ENTITIES,
    max_portfolio_hotels: overrides.max_portfolio_hotels ?? DEFAULT_MAX_PORTFOLIO_HOTELS,
  };
}
