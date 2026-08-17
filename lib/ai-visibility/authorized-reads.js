/**
 * Authorized metric / overview read helpers over existing snapshots.
 * No provider calls. Access depth applied after entitlement resolve.
 */

import {
  ACCESS_DEPTH,
  COMPARATIVE_BLOCKED_FIELDS,
  COMPARATIVE_SAFE_METRIC_FIELDS,
  isDeepAccess,
  isComparativeAccess,
} from "./access-depth.js";
import { resolveAiIntelligenceAccess, buildAiIntelligenceQueryContext } from "./authorization.js";
import { SUBJECT_TYPES } from "./subject-context.js";
import { filterEvidenceByAccessDepth } from "./evidence-access.js";

export const AUTHORIZED_READ_VERSION = "ai_visibility_authorized_read_v1";

/**
 * Pick benchmark-safe fields for comparative competitor rows.
 * @param {object} row
 */
export function toBenchmarkSafeEntityView(row) {
  const out = {};
  for (const key of COMPARATIVE_SAFE_METRIC_FIELDS) {
    if (row[key] !== undefined) out[key] = row[key];
  }
  // Convenience aliases from snapshot shape
  if (out.aiPresenceRate == null && row.value != null && row.metric === "aiPresenceRate") {
    out.aiPresenceRate = row.value;
  }
  if (out.competitivePosition == null && row.metric === "competitivePosition") {
    out.competitivePosition = row.value ?? row.rank ?? null;
  }
  if (out.recommendationShare == null && row.metric === "recommendationShare") {
    out.recommendationShare = row.value;
  }
  if (!out.entityName && row.entityName) out.entityName = row.entityName;
  if (!out.entityId && row.entityId) out.entityId = row.entityId;
  for (const blocked of COMPARATIVE_BLOCKED_FIELDS) {
    delete out[blocked];
  }
  return out;
}

/**
 * Apply depth filter to a metric snapshot row / overview payload.
 */
export function applyAccessDepthToMetrics(payload, accessDepth) {
  if (!payload) return null;
  if (accessDepth === ACCESS_DEPTH.NONE) return null;
  if (isDeepAccess(accessDepth)) {
    return { ...payload, accessDepth, citationRateReadiness: payload.citationRateReadiness || "PARTIAL" };
  }
  if (isComparativeAccess(accessDepth)) {
    return {
      ...toBenchmarkSafeEntityView(payload),
      accessDepth,
      citationRateReadiness: "PARTIAL",
    };
  }
  return null;
}

/**
 * @param {{
 *   viewerContext?: object,
 *   dealalityUser?: object,
 *   subject: object,
 *   entitlementGraph?: object,
 *   geographyScope?: string,
 *   region?: string,
 *   commercialRegion?: string,
 *   country?: string,
 *   provider?: string,
 *   store: { listMetricSnapshots: Function },
 * }} args
 */
export async function getAuthorizedVisibilityOverview(args = {}) {
  const access = resolveAiIntelligenceAccess(args);
  const query = buildAiIntelligenceQueryContext({ ...args, ...access });

  if (!access.allowed) {
    return {
      ok: false,
      allowed: false,
      accessDepth: ACCESS_DEPTH.NONE,
      reasonCode: access.reasonCode,
      overview: null,
      authorizedReadVersion: AUTHORIZED_READ_VERSION,
    };
  }

  const provider = args.provider || "openai";
  const geographyScope = args.geographyScope || null;
  const region = args.region || args.commercialRegion || null;
  const store = args.store;
  if (!store || typeof store.listMetricSnapshots !== "function") {
    return {
      ok: false,
      allowed: true,
      accessDepth: access.accessDepth,
      reasonCode: access.reasonCode,
      error: "store_required",
      overview: null,
      authorizedReadVersion: AUTHORIZED_READ_VERSION,
    };
  }

  if (access.subject.subjectType === SUBJECT_TYPES.BRAND_PORTFOLIO) {
    return getAuthorizedBrandPortfolioOverview({
      access,
      query,
      store,
      provider,
      geographyScope,
      region,
      brandNamesById: args.brandNamesById || {},
    });
  }

  if (
    access.subject.subjectType === SUBJECT_TYPES.DEAL ||
    access.subject.subjectType === SUBJECT_TYPES.HOTEL_ASSET
  ) {
    // Owner AI Recommendation Intelligence — separate product surface; no brand visibility overview.
    return {
      ok: true,
      allowed: true,
      accessDepth: access.accessDepth,
      reasonCode: access.reasonCode,
      productSurface: "AI Recommendation Intelligence",
      overview: {
        subjectType: access.subject.subjectType,
        subjectDealId: access.subject.subjectDealId,
        subjectHotelId: access.subject.subjectHotelId,
        layers: {
          aiRecommendationPattern: null,
          dealalityAnalysis: null,
          ownerProcess: null,
        },
        note: "Owner deal/asset intelligence is not company AI Visibility. Pattern layer reads come in a later phase.",
      },
      query,
      authorizedReadVersion: AUTHORIZED_READ_VERSION,
    };
  }

  const entityId = access.subject.subjectEntityId;
  const snapshots = await store.listMetricSnapshots({
    entityId,
    geographyScope,
    region,
    provider,
  });

  const byMetric = {};
  for (const snap of snapshots) {
    const metric = snap.metric || "unknown";
    const shaped = applyAccessDepthToMetrics(
      {
        entityId: snap.entityId,
        entityName: snap.entityName,
        metric,
        value: snap.value,
        numerator: snap.numerator,
        denominator: snap.denominator,
        aiPresenceRate: metric === "aiPresenceRate" ? snap.value : snap.aiPresenceRate,
        competitivePosition:
          metric === "competitivePosition" ? snap.value ?? snap.rank : snap.competitivePosition,
        recommendationShare:
          metric === "recommendationShare" ? snap.value : snap.recommendationShare,
        questionsWon: metric === "questionsWon" ? snap.value : snap.questionsWon,
        questionsMissing: metric === "questionsMissing" ? snap.value : snap.questionsMissing,
        citationRate: metric === "citationRate" ? snap.value : snap.citationRate,
        citationRateReadiness: snap.citationRateReadiness || "PARTIAL",
        provider: snap.provider || provider,
        geographyScope: snap.geographyScope,
        commercialRegion: snap.commercialRegion || snap.region,
        batchId: snap.batchId,
        batchDate: snap.batchDate,
        metricVersion: snap.metricVersion,
        evidenceIds: snap.evidenceIds,
      },
      access.accessDepth
    );
    if (shaped) byMetric[metric] = shaped;
  }

  return {
    ok: true,
    allowed: true,
    accessDepth: access.accessDepth,
    reasonCode: access.reasonCode,
    productSurface: "AI Visibility",
    overview: {
      entityId,
      entityName: snapshots[0]?.entityName || null,
      geographyScope,
      commercialRegion: region,
      provider,
      metrics: byMetric,
      snapshotCount: snapshots.length,
    },
    query,
    authorizedReadVersion: AUTHORIZED_READ_VERSION,
  };
}

async function getAuthorizedBrandPortfolioOverview({
  access,
  query,
  store,
  provider,
  geographyScope,
  region,
  brandNamesById,
}) {
  const brandIds =
    access.entitledBrandIds ||
    (access.portfolio?.brands || []).map((b) => b.brandId) ||
    [];

  const brands = [];
  for (const brandId of brandIds) {
    const snaps = await store.listMetricSnapshots({
      entityId: brandId,
      geographyScope,
      region,
      provider,
    });
    const presence = snaps.find((s) => s.metric === "aiPresenceRate");
    const position = snaps.find((s) => s.metric === "competitivePosition");
    brands.push({
      brandId,
      brandName: brandNamesById[brandId] || presence?.entityName || position?.entityName || null,
      aiPresenceRate: presence?.value ?? null,
      competitivePosition: position?.value ?? position?.rank ?? null,
    });
  }

  return {
    ok: true,
    allowed: true,
    accessDepth: access.accessDepth,
    reasonCode: access.reasonCode,
    productSurface: "AI Visibility",
    overview: {
      portfolioCompany: access.viewer?.viewerCompanyName || null,
      portfolioCompanyId: access.subject?.subjectCompanyId || null,
      region: region || null,
      geographyScope,
      provider,
      brands,
      portfolioCompositeScore: null,
    },
    query,
    authorizedReadVersion: AUTHORIZED_READ_VERSION,
  };
}

/**
 * Authorized evidence retrieval with depth filtering.
 */
export async function getAuthorizedEvidence(args = {}) {
  const access = resolveAiIntelligenceAccess(args);
  if (!access.allowed) {
    return {
      ok: false,
      allowed: false,
      accessDepth: ACCESS_DEPTH.NONE,
      reasonCode: access.reasonCode,
      evidence: [],
      filterReason: "UNAUTHORIZED_EVIDENCE",
    };
  }

  const store = args.store;
  let records = args.evidenceRecords || [];
  if ((!records || !records.length) && store?.getEvidence && args.evidenceIds) {
    records = [];
    for (const id of args.evidenceIds) {
      const row = await store.getEvidence(id);
      if (row) records.push(row);
    }
  }
  if ((!records || !records.length) && Array.isArray(args.evidenceRecords)) {
    records = args.evidenceRecords;
  }

  const entitledEntityIds =
    access.entitledBrandIds || access.entitledOperatorIds || [];

  return {
    ...filterEvidenceByAccessDepth(records, {
      accessDepth: access.accessDepth,
      subjectEntityId: access.subject?.subjectEntityId,
      entitledEntityIds,
    }),
    allowed: true,
    reasonCode: access.reasonCode,
  };
}
