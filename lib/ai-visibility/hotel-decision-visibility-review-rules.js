/**
 * Hotel Decision Visibility — deterministic review items (v1).
 * Not Airtable Opportunities. No impact/confidence scores.
 * Competitive / regional gap pp reuses OPPORTUNITY_THRESHOLDS_V1.
 * Items are provider-scoped (no cross-provider / All AI claims).
 */

import { OPPORTUNITY_THRESHOLDS_V1 } from "./config.js";
import {
  DEFAULT_AI_VISIBILITY_PROVIDER,
  formatProviderLabel,
  resolveProviderId,
} from "./provider-dimension.js";

export const HDV_REVIEW_RULES_VERSION = "hotel_decision_visibility_review_rules_v1";

export const HDV_REVIEW_THRESHOLDS_V1 = Object.freeze({
  rulesVersion: HDV_REVIEW_RULES_VERSION,
  presenceGapPp:
    OPPORTUNITY_THRESHOLDS_V1.competitorDominance?.presenceRateGapPp ?? 15,
  questionsMissingShare: 0.5,
});

function toRate(v) {
  if (v == null || !Number.isFinite(v)) return null;
  return v > 1 ? v / 100 : v;
}

function ppDiff(a, b) {
  const ra = toRate(a);
  const rb = toRate(b);
  if (ra == null || rb == null) return null;
  return Math.round((ra - rb) * 1000) / 10;
}

/**
 * @param {object} args
 * @returns {Array<object>}
 */
export function buildHotelDecisionVisibilityReviewItems(args = {}) {
  const {
    geographyKey = "CALA",
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    language = null,
    entitledBrands = [],
    subjectBrandIds = [],
    leaderPresence = null,
    leaderBrandName = null,
    brandPresenceById = {},
    brandMissingShareById = {},
    brandEvidenceIdById = {},
    regionalPresenceByBrand = {},
    monitoredBrandIdsInGeo = [],
    gapPp = HDV_REVIEW_THRESHOLDS_V1.presenceGapPp,
    missingShareThreshold = HDV_REVIEW_THRESHOLDS_V1.questionsMissingShare,
  } = args;

  const provider = resolveProviderId(providerArg);
  const providerLabel = formatProviderLabel(provider);
  const items = [];
  const subjectSet = new Set(
    (subjectBrandIds.length ? subjectBrandIds : entitledBrands.map((b) => b.brandId)).filter(
      Boolean
    )
  );

  for (const brand of entitledBrands) {
    const brandId = brand.brandId;
    if (!subjectSet.has(brandId)) continue;
    const evidenceId = brandEvidenceIdById[brandId] || null;
    const name = brand.brandName || brandId;

    if (!monitoredBrandIdsInGeo.includes(brandId)) {
      if (evidenceId) {
        items.push({
          type: "monitoring_gap",
          title: "Monitoring Gap",
          description: `${name} has no completed ${providerLabel} monitoring batch in ${geographyKey}.`,
          geography: geographyKey,
          provider,
          providerLabel,
          language: language || null,
          brandId,
          evidenceId,
          rulesVersion: HDV_REVIEW_RULES_VERSION,
        });
      }
      continue;
    }

    const presence = brandPresenceById[brandId];
    if (
      evidenceId &&
      leaderPresence != null &&
      presence != null &&
      Number.isFinite(presence) &&
      Number.isFinite(leaderPresence)
    ) {
      const gap = ppDiff(leaderPresence, presence);
      if (gap != null && gap >= gapPp) {
        items.push({
          type: "competitive_gap",
          title: "Competitive Gap",
          description: `${name} trails ${leaderBrandName || "the regional leader"} by ${gap} pp AI Presence in ${geographyKey} (${providerLabel} monitoring).`,
          geography: geographyKey,
          provider,
          providerLabel,
          language: language || null,
          brandId,
          evidenceId,
          rulesVersion: HDV_REVIEW_RULES_VERSION,
        });
      }
    }

    const missingShare = brandMissingShareById[brandId];
    if (
      evidenceId &&
      missingShare != null &&
      Number.isFinite(missingShare) &&
      missingShare >= missingShareThreshold
    ) {
      const pct = Math.round(missingShare * 1000) / 10;
      items.push({
        type: "questions_missing",
        title: "Questions Missing",
        description: `${name} was absent on ${pct}% of successful owner questions in ${geographyKey} (${providerLabel} monitoring).`,
        geography: geographyKey,
        provider,
        providerLabel,
        language: language || null,
        brandId,
        evidenceId,
        rulesVersion: HDV_REVIEW_RULES_VERSION,
      });
    }

    const regional = regionalPresenceByBrand[brandId] || {};
    const geos = Object.keys(regional).filter((g) => regional[g] != null);
    for (let i = 0; i < geos.length; i += 1) {
      for (let j = i + 1; j < geos.length; j += 1) {
        const g1 = geos[i];
        const g2 = geos[j];
        const diff = Math.abs(ppDiff(regional[g1], regional[g2]) || 0);
        if (diff >= gapPp && evidenceId) {
          items.push({
            type: "regional_difference",
            title: "Regional Difference",
            description: `${name} AI Presence differs by ${diff} pp between ${g1} and ${g2} (${providerLabel} monitoring).`,
            geography: `${g1} / ${g2}`,
            provider,
            providerLabel,
            language: language || null,
            brandId,
            evidenceId,
            rulesVersion: HDV_REVIEW_RULES_VERSION,
          });
        }
      }
    }
  }

  // Dedupe by type+brandId+geography+provider
  const seen = new Set();
  return items.filter((item) => {
    if (!item.evidenceId) return false;
    if (!item.provider) return false;
    const key = `${item.type}|${item.brandId}|${item.geography}|${item.provider}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
