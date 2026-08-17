/**
 * Company-scoped entitlements for AI Visibility (read layer).
 *
 * Canonical sources only — no name/parent/domain/LLM inference:
 * - Brands: Company Profile `Brands You Operate / Support` + Users → Brand Basics link
 * - Operators: Users → Operator Setup - Master (resolveOperatorScope)
 * - Deals: deal-record-access / Company Profile on Deals
 *
 * Monitoring execution stays company-agnostic; entitlement applies at read time.
 */

import { extractLinkedRecordIds } from "../airtable-utils.js";
import { MAP_CP_BRANDS_AIRTABLE } from "../company-profile-brands-backfill.js";
import { dealRecordAllowedForUser } from "../dealality/deal-record-access.js";
import { MAP_OPERATOR_SCOPE } from "../dealality/resolve-operator-scope.js";

export const ENTITLEMENT_VERSION = "ai_visibility_entitlement_v1";

export const MAP_AI_VISIBILITY_ENTITLEMENT = Object.freeze({
  companyProfileTable: process.env.AIRTABLE_COMPANY_PROFILE_TABLE || "Company Profile",
  companyBrandLinkField: MAP_CP_BRANDS_AIRTABLE.brandLinkField,
  companyBrandLinkFieldAlt: MAP_CP_BRANDS_AIRTABLE.brandLinkFieldAlt,
  usersBrandBasicsLink:
    process.env.AIRTABLE_ME_USERS_BRAND_BASICS_LINK || "Brand Setup - Brand Basics",
  usersOperatorSetupLink: MAP_OPERATOR_SCOPE.usersOperatorSetupLink,
  dealsCompanyLink: process.env.AIRTABLE_DEALS_COMPANY_LINK_FIELD || "Company Profile",
});

/**
 * @typedef {object} EntitlementGraph
 * @property {string[]} entitledBrandIds
 * @property {string[]} entitledOperatorIds
 * @property {string[]} entitledDealIds
 * @property {Record<string, string[]>} [hotelToDealIds] hotelId → dealIds
 * @property {string[]} [peerBrandIds]
 * @property {string[]} [peerOperatorIds]
 * @property {string} [source]
 */

/**
 * Empty graph for safe defaults.
 * @returns {EntitlementGraph}
 */
export function emptyEntitlementGraph() {
  return {
    entitledBrandIds: [],
    entitledOperatorIds: [],
    entitledDealIds: [],
    hotelToDealIds: {},
    peerBrandIds: [],
    peerOperatorIds: [],
    source: "empty",
    textInferenceUsed: false,
  };
}

/**
 * Build graph from explicit IDs (unit tests / injected fixtures).
 * @param {Partial<EntitlementGraph>} partial
 * @returns {EntitlementGraph}
 */
export function buildFixtureEntitlementGraph(partial = {}) {
  return {
    ...emptyEntitlementGraph(),
    ...partial,
    entitledBrandIds: [...new Set(partial.entitledBrandIds || [])],
    entitledOperatorIds: [...new Set(partial.entitledOperatorIds || [])],
    entitledDealIds: [...new Set(partial.entitledDealIds || [])],
    peerBrandIds: [...new Set(partial.peerBrandIds || [])],
    peerOperatorIds: [...new Set(partial.peerOperatorIds || [])],
    hotelToDealIds: { ...(partial.hotelToDealIds || {}) },
    source: partial.source || "fixture",
    textInferenceUsed: false,
  };
}

function readCompanyBrandLinkIds(companyFields) {
  const ids = [];
  const f = companyFields || {};
  for (const key of [
    MAP_AI_VISIBILITY_ENTITLEMENT.companyBrandLinkField,
    MAP_AI_VISIBILITY_ENTITLEMENT.companyBrandLinkFieldAlt,
  ]) {
    if (f[key] != null) ids.push(...extractLinkedRecordIds(f[key]));
  }
  return [...new Set(ids)];
}

/**
 * Resolve entitled Brand Basics IDs from governed link fields only.
 *
 * @param {{ viewerContext: object, companyFields?: object|null, userFields?: object|null, entitlementGraph?: EntitlementGraph }} args
 */
export function resolveEntitledBrands(args) {
  const { viewerContext, companyFields, userFields, entitlementGraph } = args || {};
  if (entitlementGraph) {
    return {
      ok: true,
      brandIds: [...(entitlementGraph.entitledBrandIds || [])],
      source: entitlementGraph.source || "injected_graph",
      textInferenceUsed: false,
      mapping: MAP_AI_VISIBILITY_ENTITLEMENT,
    };
  }

  if (!viewerContext) {
    return { ok: false, brandIds: [], error: "viewer_required", textInferenceUsed: false };
  }

  const fromCompany = readCompanyBrandLinkIds(companyFields);
  const fromUser = extractLinkedRecordIds(
    (userFields || {})[MAP_AI_VISIBILITY_ENTITLEMENT.usersBrandBasicsLink]
  );
  const brandIds = [...new Set([...fromCompany, ...fromUser])];

  return {
    ok: true,
    brandIds,
    source: "company_profile_brands_link+users_brand_basics_link",
    textInferenceUsed: false,
    mapping: MAP_AI_VISIBILITY_ENTITLEMENT,
    viewerCompanyId: viewerContext.viewerCompanyId || null,
  };
}

/**
 * Resolve entitled Operator Master IDs from Users → Operator Setup - Master only.
 *
 * @param {{ viewerContext: object, userFields?: object|null, operatorScope?: object|null, entitlementGraph?: EntitlementGraph }} args
 */
export function resolveEntitledOperators(args) {
  const { viewerContext, userFields, operatorScope, entitlementGraph } = args || {};
  if (entitlementGraph) {
    return {
      ok: true,
      operatorIds: [...(entitlementGraph.entitledOperatorIds || [])],
      source: entitlementGraph.source || "injected_graph",
      textInferenceUsed: false,
      mapping: MAP_AI_VISIBILITY_ENTITLEMENT,
    };
  }

  if (!viewerContext) {
    return { ok: false, operatorIds: [], error: "viewer_required", textInferenceUsed: false };
  }

  if (operatorScope && Array.isArray(operatorScope.allowedOperatorSetupIds)) {
    return {
      ok: true,
      operatorIds: [...new Set(operatorScope.allowedOperatorSetupIds)],
      source: "resolveOperatorScope",
      textInferenceUsed: false,
      mapping: MAP_AI_VISIBILITY_ENTITLEMENT,
    };
  }

  const fromUser = extractLinkedRecordIds(
    (userFields || {})[MAP_AI_VISIBILITY_ENTITLEMENT.usersOperatorSetupLink]
  );

  return {
    ok: true,
    operatorIds: [...new Set(fromUser)],
    source: "users_operator_setup_master_link",
    textInferenceUsed: false,
    mapping: MAP_AI_VISIBILITY_ENTITLEMENT,
    viewerCompanyId: viewerContext.viewerCompanyId || null,
  };
}

/**
 * Resolve entitled deal IDs. Prefer injected graph; else evaluate deal fields via deal-record-access.
 *
 * @param {{ viewerContext: object, dealalityUser?: object, deals?: Array<{id:string,fields:object}>, entitlementGraph?: EntitlementGraph }} args
 */
export function resolveEntitledDeals(args) {
  const { viewerContext, dealalityUser, deals, entitlementGraph } = args || {};
  if (entitlementGraph) {
    return {
      ok: true,
      dealIds: [...(entitlementGraph.entitledDealIds || [])],
      source: entitlementGraph.source || "injected_graph",
      textInferenceUsed: false,
    };
  }

  if (!viewerContext) {
    return { ok: false, dealIds: [], error: "viewer_required" };
  }

  const user =
    dealalityUser ||
    ({
      isAdmin: viewerContext.isAdmin,
      isOwner: viewerContext.isOwner,
      companyId: viewerContext.viewerCompanyId,
      companyIds: viewerContext.viewerCompanyIds,
      userRecordId: viewerContext.viewerUserId,
    });

  const dealIds = [];
  for (const deal of deals || []) {
    if (!deal?.id) continue;
    if (dealRecordAllowedForUser(deal.fields || {}, user)) dealIds.push(deal.id);
  }

  return {
    ok: true,
    dealIds: [...new Set(dealIds)],
    source: "deal_record_access",
    textInferenceUsed: false,
  };
}

/**
 * Portfolio = derived grouping of entitled brands for the viewer's company (not a new SSOT entity).
 *
 * @param {{ viewerContext: object, brandIds?: string[], brandNamesById?: Record<string,string>, entitlementGraph?: EntitlementGraph, companyFields?: object }} args
 */
export function resolveBrandPortfolio(args) {
  const brands = resolveEntitledBrands(args);
  const brandIds = args.brandIds || brands.brandIds || [];
  const names = args.brandNamesById || {};
  return {
    ok: brands.ok !== false,
    subjectType: "brand_portfolio",
    portfolioCompanyId: args.viewerContext?.viewerCompanyId || null,
    portfolioCompanyName: args.viewerContext?.viewerCompanyName || null,
    brands: brandIds.map((brandId) => ({
      brandId,
      brandName: names[brandId] || null,
    })),
    textInferenceUsed: false,
    portfolioCompositeScore: null,
    source: brands.source,
  };
}

/**
 * Whether subject entity is a peer (comparative) given entitled + peer lists.
 * Peer list must intersect entitled set so random peers outside cohort are not exposed.
 */
export function isPeerComparativeEntity(subjectEntityId, entitledIds, peerIds) {
  const subject = String(subjectEntityId || "");
  if (!subject) return false;
  const entitled = new Set(entitledIds || []);
  const peers = new Set(peerIds || []);
  if (entitled.has(subject)) return false;
  if (!peers.has(subject)) return false;
  for (const id of entitled) {
    if (peers.has(id)) return true;
  }
  return false;
}
