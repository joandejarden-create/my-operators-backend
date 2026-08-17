/**
 * Central AI intelligence access resolver.
 * Viewer authorization + subject depth. Geography/provider are orthogonal query dims.
 *
 * Monitoring datasets remain centrally generated; this gate applies at read time only.
 */

import { ACCESS_DEPTH } from "./access-depth.js";
import { ACCESS_REASON } from "./access-reason-codes.js";
import {
  buildFixtureEntitlementGraph,
  emptyEntitlementGraph,
  isPeerComparativeEntity,
  resolveBrandPortfolio,
  resolveEntitledBrands,
  resolveEntitledDeals,
  resolveEntitledOperators,
} from "./entitlements.js";
import { SUBJECT_TYPES, normalizeAiVisibilitySubject } from "./subject-context.js";
import { normalizeAiVisibilityViewerContext } from "./viewer-context.js";

export const AUTHORIZATION_VERSION = "ai_visibility_authorization_v1";

/**
 * @param {{
 *   viewerContext?: object,
 *   dealalityUser?: object,
 *   subject: object,
 *   entitlementGraph?: object,
 *   companyFields?: object,
 *   userFields?: object,
 *   operatorScope?: object,
 *   deals?: Array<{id:string,fields:object}>,
 *   allowAdminOverride?: boolean,
 * }} args
 */
export function resolveAiIntelligenceAccess(args = {}) {
  const viewer =
    args.viewerContext ||
    (args.dealalityUser ? normalizeAiVisibilityViewerContext(args.dealalityUser) : null);

  if (!viewer) {
    return deny(ACCESS_REASON.VIEWER_REQUIRED, null, args.subject);
  }

  const subjectNorm = normalizeAiVisibilitySubject(args.subject);
  if (!subjectNorm.ok) {
    return deny(subjectNorm.reasonCode || ACCESS_REASON.INVALID_SUBJECT, viewer, args.subject);
  }
  const subject = subjectNorm.subject;
  const hasInjectedGraph = args.entitlementGraph != null;
  const graph = hasInjectedGraph ? args.entitlementGraph : emptyEntitlementGraph();
  const allowAdmin = args.allowAdminOverride !== false;

  if (allowAdmin && viewer.isAdmin) {
    // Founder/demo Brand Portfolio switch must stay authoritative — do not let
    // admin override deep-read Hilton brands while Choice (etc.) is selected.
    const demoPortfolioScoped =
      graph?.source === "demo_showcase_portfolio" &&
      (subject.subjectType === SUBJECT_TYPES.BRAND ||
        subject.subjectType === SUBJECT_TYPES.BRAND_PORTFOLIO);
    if (!demoPortfolioScoped) {
      return allow({
        accessDepth: ACCESS_DEPTH.DEEP,
        reasonCode: ACCESS_REASON.ADMIN_OVERRIDE,
        viewer,
        subject,
      });
    }
  }

  const shared = { ...args, viewer, subject, graph, hasInjectedGraph };
  if (subject.subjectType === SUBJECT_TYPES.BRAND) {
    return resolveBrandAccess(shared);
  }
  if (subject.subjectType === SUBJECT_TYPES.BRAND_PORTFOLIO) {
    return resolvePortfolioAccess(shared);
  }
  if (subject.subjectType === SUBJECT_TYPES.OPERATOR) {
    return resolveOperatorAccess(shared);
  }
  if (subject.subjectType === SUBJECT_TYPES.DEAL) {
    return resolveDealAccess(shared);
  }
  if (subject.subjectType === SUBJECT_TYPES.HOTEL_ASSET) {
    return resolveHotelAccess(shared);
  }

  return deny(ACCESS_REASON.SUBJECT_TYPE_UNSUPPORTED, viewer, subject);
}

function resolveBrandAccess({ viewer, subject, graph, companyFields, userFields, hasInjectedGraph }) {
  if (!viewer.isBrand && !viewer.isAdmin) {
    // Brand workspace required for brand AI Visibility (operators/owners use other subject types)
    if (viewer.isOperator || viewer.isOwner) {
      return deny(ACCESS_REASON.WORKSPACE_MISMATCH, viewer, subject);
    }
  }

  const brands = resolveEntitledBrands({
    viewerContext: viewer,
    companyFields,
    userFields,
    entitlementGraph: hasInjectedGraph ? graph : undefined,
  });
  const entitled = brands.brandIds || [];
  const peerIds = graph.peerBrandIds || [];

  if (entitled.includes(subject.subjectEntityId)) {
    return allow({
      accessDepth: ACCESS_DEPTH.DEEP,
      reasonCode: ACCESS_REASON.ENTITLED_BRAND,
      viewer,
      subject,
      entitledBrandIds: entitled,
    });
  }

  if (isPeerComparativeEntity(subject.subjectEntityId, entitled, peerIds)) {
    return allow({
      accessDepth: ACCESS_DEPTH.COMPARATIVE,
      reasonCode: ACCESS_REASON.PEER_BRAND_COMPARATIVE,
      viewer,
      subject,
      entitledBrandIds: entitled,
      peerBrandIds: peerIds,
    });
  }

  return deny(ACCESS_REASON.SUBJECT_NOT_ENTITLED, viewer, subject, { entitledBrandIds: entitled });
}

function resolvePortfolioAccess({
  viewer,
  subject,
  graph,
  companyFields,
  userFields,
  hasInjectedGraph,
}) {
  const companyIds = new Set(viewer.viewerCompanyIds || []);
  if (viewer.viewerCompanyId) companyIds.add(viewer.viewerCompanyId);

  if (!companyIds.has(subject.subjectCompanyId)) {
    return deny(ACCESS_REASON.WORKSPACE_MISMATCH, viewer, subject);
  }

  if (!viewer.isBrand && !viewer.isAdmin) {
    return deny(ACCESS_REASON.WORKSPACE_MISMATCH, viewer, subject);
  }

  const portfolio = resolveBrandPortfolio({
    viewerContext: viewer,
    companyFields,
    userFields,
    entitlementGraph: hasInjectedGraph ? graph : undefined,
  });

  return allow({
    accessDepth: ACCESS_DEPTH.DEEP,
    reasonCode: ACCESS_REASON.ENTITLED_BRAND_PORTFOLIO,
    viewer,
    subject,
    entitledBrandIds: (portfolio.brands || []).map((b) => b.brandId),
    portfolio,
  });
}

function resolveOperatorAccess({
  viewer,
  subject,
  graph,
  userFields,
  operatorScope,
  hasInjectedGraph,
}) {
  if (!viewer.isOperator && !viewer.isAdmin) {
    if (viewer.isBrand || viewer.isOwner) {
      return deny(ACCESS_REASON.WORKSPACE_MISMATCH, viewer, subject);
    }
  }

  const ops = resolveEntitledOperators({
    viewerContext: viewer,
    userFields,
    operatorScope,
    entitlementGraph: hasInjectedGraph ? graph : undefined,
  });
  const entitled = ops.operatorIds || [];
  const peerIds = graph.peerOperatorIds || [];

  if (entitled.includes(subject.subjectEntityId)) {
    return allow({
      accessDepth: ACCESS_DEPTH.DEEP,
      reasonCode: ACCESS_REASON.ENTITLED_OPERATOR,
      viewer,
      subject,
      entitledOperatorIds: entitled,
    });
  }

  if (isPeerComparativeEntity(subject.subjectEntityId, entitled, peerIds)) {
    return allow({
      accessDepth: ACCESS_DEPTH.COMPARATIVE,
      reasonCode: ACCESS_REASON.PEER_OPERATOR_COMPARATIVE,
      viewer,
      subject,
      entitledOperatorIds: entitled,
      peerOperatorIds: peerIds,
    });
  }

  return deny(ACCESS_REASON.SUBJECT_NOT_ENTITLED, viewer, subject, {
    entitledOperatorIds: entitled,
  });
}

function resolveDealAccess({ viewer, subject, graph, deals, dealalityUser, hasInjectedGraph }) {
  if (!viewer.isOwner && !viewer.isAdmin) {
    return deny(ACCESS_REASON.WORKSPACE_MISMATCH, viewer, subject);
  }

  const entitled = resolveEntitledDeals({
    viewerContext: viewer,
    dealalityUser,
    deals,
    entitlementGraph: hasInjectedGraph ? graph : undefined,
  });

  if ((entitled.dealIds || []).includes(subject.subjectDealId)) {
    return allow({
      accessDepth: ACCESS_DEPTH.DEEP,
      reasonCode: ACCESS_REASON.ENTITLED_DEAL,
      viewer,
      subject,
      entitledDealIds: entitled.dealIds,
    });
  }

  return deny(ACCESS_REASON.SUBJECT_NOT_ENTITLED, viewer, subject);
}

function resolveHotelAccess({ viewer, subject, graph, deals, dealalityUser, hasInjectedGraph }) {
  if (!viewer.isOwner && !viewer.isAdmin) {
    return deny(ACCESS_REASON.WORKSPACE_MISMATCH, viewer, subject);
  }

  const entitledDeals = resolveEntitledDeals({
    viewerContext: viewer,
    dealalityUser,
    deals,
    entitlementGraph: hasInjectedGraph ? graph : undefined,
  });
  const dealIds = new Set(entitledDeals.dealIds || []);

  if (subject.subjectDealId && dealIds.has(subject.subjectDealId)) {
    return allow({
      accessDepth: ACCESS_DEPTH.DEEP,
      reasonCode: ACCESS_REASON.ENTITLED_HOTEL_ASSET,
      viewer,
      subject,
      entitledDealIds: [...dealIds],
    });
  }

  if (subject.subjectHotelId) {
    const mapped = graph.hotelToDealIds?.[subject.subjectHotelId] || [];
    if (mapped.some((d) => dealIds.has(d))) {
      return allow({
        accessDepth: ACCESS_DEPTH.DEEP,
        reasonCode: ACCESS_REASON.ENTITLED_HOTEL_ASSET,
        viewer,
        subject,
        entitledDealIds: [...dealIds],
      });
    }
  }

  if (!subject.subjectDealId && subject.subjectHotelId && !(graph.hotelToDealIds || {})[subject.subjectHotelId]) {
    return deny(ACCESS_REASON.SUBJECT_NOT_FOUND, viewer, subject);
  }

  return deny(ACCESS_REASON.SUBJECT_NOT_ENTITLED, viewer, subject);
}

function allow(payload) {
  return {
    allowed: true,
    accessDepth: payload.accessDepth,
    reasonCode: payload.reasonCode,
    viewerCompanyId: payload.viewer?.viewerCompanyId || null,
    subjectType: payload.subject?.subjectType || null,
    subjectEntityId: payload.subject?.subjectEntityId || null,
    subjectCompanyId: payload.subject?.subjectCompanyId || null,
    subjectDealId: payload.subject?.subjectDealId || null,
    subjectHotelId: payload.subject?.subjectHotelId || null,
    authorizationVersion: AUTHORIZATION_VERSION,
    viewer: payload.viewer,
    subject: payload.subject,
    entitledBrandIds: payload.entitledBrandIds || undefined,
    entitledOperatorIds: payload.entitledOperatorIds || undefined,
    entitledDealIds: payload.entitledDealIds || undefined,
    peerBrandIds: payload.peerBrandIds || undefined,
    peerOperatorIds: payload.peerOperatorIds || undefined,
    portfolio: payload.portfolio || undefined,
  };
}

function deny(reasonCode, viewer, subject, extra = {}) {
  return {
    allowed: false,
    accessDepth: ACCESS_DEPTH.NONE,
    reasonCode,
    viewerCompanyId: viewer?.viewerCompanyId || null,
    subjectType: subject?.subjectType || null,
    subjectEntityId: subject?.subjectEntityId || null,
    subjectCompanyId: subject?.subjectCompanyId || null,
    subjectDealId: subject?.subjectDealId || null,
    subjectHotelId: subject?.subjectHotelId || null,
    authorizationVersion: AUTHORIZATION_VERSION,
    viewer: viewer || null,
    subject: subject || null,
    ...extra,
  };
}

/**
 * Build query context for future overview/trend/questions APIs.
 */
export function buildAiIntelligenceQueryContext(args = {}) {
  const access = resolveAiIntelligenceAccess(args);
  return {
    viewerContext: access.viewer,
    subject: access.subject,
    geographyScope: args.geographyScope || null,
    commercialRegion: args.region || args.commercialRegion || null,
    country: args.country || null,
    provider: args.provider || "openai",
    metricVersion: args.metricVersion || null,
    accessDepth: access.accessDepth,
    allowed: access.allowed,
    reasonCode: access.reasonCode,
    authorizationVersion: AUTHORIZATION_VERSION,
  };
}

export { buildFixtureEntitlementGraph };
