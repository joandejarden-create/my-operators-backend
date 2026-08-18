/**
 * P0E — Executive Intelligence Finding Engine (read-only assembly).
 * Combines P0C gaps, validated associations, Truth Layer, provider disagreement,
 * movement, and source/citation intelligence into 3–5 client-ready findings.
 * No provider calls. No certified metric changes. No numeric confidence scores.
 */

import { loadScenarioRegistry, buildScenarioRegistryIndex } from "./scenario-registry.js";
import { runCompetitiveGapEngineFromStore } from "./gaps/competitive-gap-engine.js";
import { isAssociationAttributeProductionEligible } from "./gaps/association-eligibility.js";
import { aggregateBrandAssociations } from "./associations/aggregation-research.js";
import {
  loadCachedTruthComparisons,
  filterTruthComparisonsForCohort,
} from "./truth-layer/truth-comparisons-loader.js";
import { PEER_SET_ID_V2 } from "./peer-sets.js";
import {
  auditGapInterpretations,
  buildCommercialGapHeadline,
  interpretProductionGap,
  ACTION_DISPOSITION,
  GAP_INTERPRETATION_RULE_VERSION,
} from "./gap-commercial-interpretation.js";
import { formatFindingSupportDescriptor } from "./stability-client.js";
import {
  buildPortfolioNarratives,
  loadAuthoritativeStabilityReport,
} from "./narrative-intelligence.js";
import { NARRATIVE_FAMILY_LABELS } from "./narrative-taxonomy.js";
import { applyExecutiveCopyGovernance } from "./executive-intelligence-copy-governance.js";

export const EXECUTIVE_FINDING_ENGINE_VERSION =
  "ai_visibility_executive_finding_engine_p0e_v1_1_commercial";

export const FINDING_TYPES = Object.freeze({
  LARGEST_COMPETITIVE_GAP: "LARGEST_COMPETITIVE_GAP",
  HIGHEST_PRIORITY_REVIEW: "HIGHEST_PRIORITY_REVIEW",
  STRONGEST_VALIDATED_ASSOCIATION: "STRONGEST_VALIDATED_ASSOCIATION",
  POTENTIAL_AI_PERCEPTION_GAP: "POTENTIAL_AI_PERCEPTION_GAP",
  PROVIDER_DISAGREEMENT: "PROVIDER_DISAGREEMENT",
  MATERIAL_MOVEMENT: "MATERIAL_MOVEMENT",
  SOURCE_CITATION_GAP: "SOURCE_CITATION_GAP",
  NARRATIVE_PATTERN: "NARRATIVE_PATTERN",
  SOURCE_PATTERN: "SOURCE_PATTERN",
});

export const ACTION_CATEGORIES = Object.freeze({
  DEVELOPMENT_WEBSITE: "DEVELOPMENT_WEBSITE",
  OWNER_FRANCHISE_EDUCATION: "OWNER_FRANCHISE_EDUCATION",
  BRAND_POSITIONING: "BRAND_POSITIONING",
  SOURCE_CITATION_COVERAGE: "SOURCE_CITATION_COVERAGE",
  THIRD_PARTY_INFORMATION_CORRECTION: "THIRD_PARTY_INFORMATION_CORRECTION",
  STRUCTURED_INFORMATION: "STRUCTURED_INFORMATION",
  DEVELOPMENT_COLLATERAL: "DEVELOPMENT_COLLATERAL",
  COMPETITIVE_COMPARISON: "COMPETITIVE_COMPARISON",
  MARKET_SPECIFIC_CONTENT: "MARKET_SPECIFIC_CONTENT",
  AI_PERCEPTION_REVIEW: "AI_PERCEPTION_REVIEW",
});

const FINDING_TYPE_ORDER = Object.freeze([
  FINDING_TYPES.LARGEST_COMPETITIVE_GAP,
  FINDING_TYPES.POTENTIAL_AI_PERCEPTION_GAP,
  FINDING_TYPES.HIGHEST_PRIORITY_REVIEW,
  FINDING_TYPES.NARRATIVE_PATTERN,
  FINDING_TYPES.PROVIDER_DISAGREEMENT,
  FINDING_TYPES.STRONGEST_VALIDATED_ASSOCIATION,
  FINDING_TYPES.SOURCE_PATTERN,
  FINDING_TYPES.SOURCE_CITATION_GAP,
  FINDING_TYPES.MATERIAL_MOVEMENT,
]);

const CLASSIFICATION_ORDER = Object.freeze({
  HIGH_PRIORITY: 4,
  PRIORITY: 3,
  REVIEW: 2,
  MONITOR: 1,
});

const MAX_FINDINGS = 5;
const MIN_FINDINGS = 0;

function rankedProductionGapsFromInterpreted(productionGaps = [], interpretations = []) {
  const byId = new Map(interpretations.map((r) => [r.gapId, r]));
  return [...productionGaps]
    .filter((g) => byId.get(g.gapId)?.executiveEligible)
    .sort((a, b) => {
      const ia = byId.get(a.gapId);
      const ib = byId.get(b.gapId);
      const pa = competitiveGapPriorityRank(a, ia);
      const pb = competitiveGapPriorityRank(b, ib);
      if (pb !== pa) return pb - pa;
      return (b.observationCount || 0) - (a.observationCount || 0);
    });
}

function pct(v) {
  if (v == null || !Number.isFinite(v)) return null;
  const n = v <= 1 ? v * 100 : v;
  return `${(Math.round(n * 10) / 10).toFixed(1)}%`;
}

function formatProvider(id) {
  if (!id) return "Provider";
  const s = String(id);
  if (s.toLowerCase() === "openai") return "OpenAI";
  if (s.toLowerCase() === "anthropic") return "Claude";
  if (s.toLowerCase() === "perplexity") return "Perplexity";
  if (s.toLowerCase() === "google") return "Google";
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function scenarioLabel(scenarioId, scenarioIndex = {}) {
  if (!scenarioId) return "monitored owner-decision scenario";
  const map = scenarioIndex.scenarioById || scenarioIndex;
  const row = map && typeof map.get === "function" ? map.get(scenarioId) : null;
  return row?.scenarioName || scenarioId;
}

function peerNames(peerIds = [], brandNamesById = {}, limit = 4) {
  return peerIds
    .map((id) => brandNamesById[id] || id)
    .filter(Boolean)
    .slice(0, limit);
}

function evidenceDescriptorFromGap(gap) {
  return gap.persistence || gap.evidenceStrength || "Repeated Across Providers";
}

function resolveReviewActionForGap(gap, interpretation = null) {
  if (interpretation?.reviewAction) {
    return {
      category: interpretation.reviewActionType || ACTION_CATEGORIES.COMPETITIVE_COMPARISON,
      text: interpretation.reviewAction,
      actionDisposition: interpretation.actionDisposition,
    };
  }
  const scenario = gap.scenarioId || "";
  if (scenario.includes("newbuild") || scenario.includes("conversion")) {
    return {
      category: ACTION_CATEGORIES.DEVELOPMENT_COLLATERAL,
      text: "Review development messaging and owner-facing brand materials for this scenario.",
    };
  }
  if (scenario.includes("soft_brand") || scenario.includes("flexibility")) {
    return {
      category: ACTION_CATEGORIES.BRAND_POSITIONING,
      text: "Review owner-facing positioning for flexibility and collection affiliation.",
    };
  }
  if (gap.gapClass === "VALIDATED_ASSOCIATION_GAP") {
    return {
      category: ACTION_CATEGORIES.STRUCTURED_INFORMATION,
      text: "Review structured brand information supporting this association dimension.",
    };
  }
  return {
    category: ACTION_CATEGORIES.COMPETITIVE_COMPARISON,
    text: "Review competitive differentiation where peers appear and the subject brand does not.",
  };
}

function competitiveGapPriorityRank(gap, interpretation) {
  if (interpretation?.actionDisposition === ACTION_DISPOSITION.ACTION_REQUIRED) {
    if (gap.classification === "HIGH_PRIORITY") return 110;
    if (gap.classification === "PRIORITY") return 75;
    return 70;
  }
  if (interpretation?.actionDisposition === ACTION_DISPOSITION.REVIEW_REQUIRED) {
    if (gap.classification === "HIGH_PRIORITY") return 85;
    if (gap.classification === "PRIORITY") return 65;
    return 55;
  }
  return 0;
}

function buildCompetitiveGapTile(gap, brandNamesById, scenarioIndex, opts = {}) {
  const interpretation =
    opts.interpretation || interpretProductionGap(gap, { brandNamesById, scenarioIndex: opts.scenarioIndex });
  const subject = brandNamesById[gap.subjectBrandId] || gap.subjectBrandId;
  const peers = peerNames(gap.peerBrandIds, brandNamesById);
  const scenario = scenarioLabel(gap.scenarioId, scenarioIndex);
  const geo = gap.geography || opts.geographyKey || "region";
  const obs = gap.observationCount || gap.questionsMissing || 1;
  const providerCount = (gap.providers || []).length;
  const review = resolveReviewActionForGap(gap, interpretation);

  const headline = buildCommercialGapHeadline(
    gap,
    interpretation,
    brandNamesById,
    scenarioIndex
  );
  const context = `${scenario} · ${geo}`;
  const persistenceLabel = gap.persistence
    ? gap.persistence
        .replace(/_/g, " ")
        .toLowerCase()
        .replace(/\b\w/g, (c) => c.toUpperCase())
    : null;
  const evidenceSummary = [
    providerCount ? `${providerCount} provider${providerCount === 1 ? "" : "s"}` : null,
    `${obs} observation${obs === 1 ? "" : "s"}`,
    persistenceLabel,
  ]
    .filter(Boolean)
    .join(" · ");

  return {
    findingType: FINDING_TYPES.LARGEST_COMPETITIVE_GAP,
    title:
      interpretation.actionDisposition === ACTION_DISPOSITION.ACTION_REQUIRED
        ? "Largest Competitive Gap"
        : "Competitive Gap — Review",
    headline,
    context,
    evidenceSummary,
    reviewAction: review.text,
    actionCategory: review.category,
    actionDisposition: interpretation.actionDisposition,
    commercialMeaning: interpretation.commercialMeaning,
    rootCause: interpretation.rootCause,
    scenarioEligibility: interpretation.eligibilityStatus,
    scenarioId: gap.scenarioId || null,
    scenarioName: scenario,
    brandName: subject,
    brandId: gap.subjectBrandId || null,
    peerBrandIds: (gap.peerBrandIds || []).slice(0, 4),
    leadPeerName: peers[0] || null,
    classification: gap.classification || null,
    persistence: gap.persistence || null,
    providerCount: providerCount || null,
    observationCount: obs,
    evidenceIds: (gap.evidenceIds || []).slice(0, 10),
    truthComparisonId: null,
    associationAttributeId: null,
    gapId: gap.gapId || null,
    dedupeKey: `gap:${gap.gapId || [gap.subjectBrandId, gap.scenarioId].join("|")}`,
    priorityRank: competitiveGapPriorityRank(gap, interpretation),
    WHY_INCLUDED: interpretation.executiveEligible
      ? `${interpretation.actionDisposition} on eligible scenario with ${evidenceDescriptorFromGap(gap)} evidence.`
      : null,
    executiveEligible: interpretation.executiveEligible,
    interpretation,
  };
}

function buildTruthTile(comparison, brandNamesById, scenarioIndex = {}) {
  const brand = brandNamesById[comparison.subjectBrandId] || comparison.subjectBrandName || "Brand";
  const explanation =
    comparison.clientSafeExplanation ||
    `AI responses occasionally differ from Dealality's governed facts for ${brand}.`;

  return {
    findingType: FINDING_TYPES.POTENTIAL_AI_PERCEPTION_GAP,
    title: "Potential AI Perception Gap",
    headline: explanation,
    context: scenarioLabel(comparison.scenarioId, scenarioIndex),
    evidenceSummary: [
      comparison.provider ? formatProvider(comparison.provider) : null,
      comparison.evidenceId ? "evidence-backed observation" : null,
      comparison.aiSemanticDimension
        ? comparison.aiSemanticDimension.replace(/_/g, " ").toLowerCase()
        : null,
    ]
      .filter(Boolean)
      .join(" · "),
    reviewAction: `Review how AI describes ${brand}'s Brand Architecture.`,
    actionCategory: ACTION_CATEGORIES.AI_PERCEPTION_REVIEW,
    scenarioId: comparison.scenarioId || null,
    scenarioName: scenarioLabel(comparison.scenarioId, scenarioIndex),
    brandName: brand,
    brandId: comparison.subjectBrandId || null,
    peerBrandIds: [],
    classification: null,
    persistence: null,
    providerCount: comparison.provider ? 1 : null,
    observationCount: 1,
    evidenceIds: comparison.evidenceId ? [comparison.evidenceId] : [],
    truthComparisonId: comparison.truthComparisonId || null,
    associationAttributeId: null,
    dedupeKey: `truth:${comparison.truthComparisonId || comparison.subjectBrandId}`,
    governedClassification:
      typeof comparison.dealalityFactValue === "string" &&
      /hard brand/i.test(comparison.dealalityFactValue)
        ? "Hard Brand"
        : null,
    priorityRank: 90,
    WHY_INCLUDED: "Executive-eligible Truth Layer perception gap with validated dimension.",
  };
}

function buildAssociationTile(row, brandNamesById) {
  if (!isAssociationAttributeProductionEligible(row.attributeId)) return null;
  const brand = brandNamesById[row.brandId] || row.brandId;
  const attr = row.attributeId.replace(/_/g, " ").toLowerCase();

  return {
    findingType: FINDING_TYPES.STRONGEST_VALIDATED_ASSOCIATION,
    title: "Observed AI Association",
    headline: `${brand} is consistently associated with ${attr} in monitored responses.`,
    context: (row.scenarios || []).slice(0, 2).join(", ") || "monitored scenarios",
    evidenceSummary: [
      `${row.observationCount || row.count} observation${(row.observationCount || row.count) === 1 ? "" : "s"}`,
      `${(row.providers || []).length} provider${(row.providers || []).length === 1 ? "" : "s"}`,
      row.descriptor
        ? row.descriptor
            .replace(/_/g, " ")
            .toLowerCase()
            .replace(/\b\w/g, (c) => c.toUpperCase())
        : null,
    ]
      .filter(Boolean)
      .join(" · "),
    reviewAction: `Confirm owner-facing materials reflect this ${attr} association.`,
    actionCategory: ACTION_CATEGORIES.STRUCTURED_INFORMATION,
    scenarioId: row.scenarios?.[0] || null,
    scenarioName: (row.scenarios || []).slice(0, 2).join(", "),
    brandName: brand,
    brandId: row.brandId || null,
    scenarioCount: Array.isArray(row.scenarios) ? row.scenarios.length : 0,
    peerBrandIds: [],
    classification: null,
    persistence: row.descriptor || null,
    providerCount: (row.providers || []).length || null,
    observationCount: row.observationCount || row.count || null,
    evidenceIds: [],
    truthComparisonId: null,
    associationAttributeId: row.attributeId,
    dedupeKey: `assoc:${row.brandId}:${row.attributeId}`,
    priorityRank: 50,
    WHY_INCLUDED: "Production-eligible association attribute with repeated evidence.",
  };
}

function buildProviderDisagreementTile(xp, geographyKey, brandName) {
  if (!xp || xp.NOT_COMPARABLE === true) return null;
  if (xp.PROVIDER_DISAGREEMENT?.status !== "DISAGREE") return null;

  const strong = xp.STRONGEST_PROVIDER_BY_PRESENCE;
  const weak = xp.WEAKEST_PROVIDER_BY_PRESENCE;
  if (!strong?.provider || !weak?.provider) return null;

  const strongLabel = formatProvider(strong.provider);
  const weakLabel = formatProvider(weak.provider);
  const strongPct = pct(strong.rate);
  const weakPct = pct(weak.rate);
  return {
    findingType: FINDING_TYPES.PROVIDER_DISAGREEMENT,
    title: "Provider Comparison",
    headline: `${strongLabel} ${strongPct} vs ${weakLabel} ${weakPct} portfolio Presence in ${geographyKey}.`,
    context: geographyKey,
    evidenceSummary: `Cross-provider · comparable cohort`,
    reviewAction: "Compare provider-level Presence before concluding overall visibility.",
    actionCategory: ACTION_CATEGORIES.COMPETITIVE_COMPARISON,
    scenarioId: null,
    brandId: null,
    peerBrandIds: [],
    classification: null,
    persistence: null,
    providerCount: (xp.PROVIDERS_MONITORED || []).length || null,
    providerStrongLabel: strongLabel,
    providerWeakLabel: weakLabel,
    providerStrongPct: strongPct,
    providerWeakPct: weakPct,
    observationCount: null,
    evidenceIds: [],
    truthComparisonId: null,
    associationAttributeId: null,
    dedupeKey: `provider:${geographyKey}:${strong.provider}:${weak.provider}`,
    priorityRank: 60,
    WHY_INCLUDED: "Material provider disagreement within comparable cohort.",
  };
}

function buildMovementTile(presenceChange, geographyKey) {
  if (!presenceChange?.comparable || presenceChange.comparable === false) return null;
  if (presenceChange.trendStatus === "INSUFFICIENT_HISTORY") return null;
  const delta = presenceChange.deltaPp;
  if (typeof delta !== "number" || !Number.isFinite(delta)) return null;
  if (Math.abs(delta) < 2) return null;

  const direction = delta > 0 ? "Improving" : "Declining";
  const brand = presenceChange.brandName || "Portfolio brand";

  return {
    findingType: FINDING_TYPES.MATERIAL_MOVEMENT,
    title: "Period Change",
    headline: `${brand} AI Presence ${direction.toLowerCase()} by ${Math.abs(delta)} pp in ${geographyKey}.`,
    context: geographyKey,
    evidenceSummary: "Comparable monitoring periods with measured Presence change.",
    reviewAction: "Confirm whether the change persists in the next comparable monitoring window.",
    actionCategory: ACTION_CATEGORIES.BRAND_POSITIONING,
    scenarioId: null,
    brandId: presenceChange.brandId || null,
    peerBrandIds: [],
    classification: null,
    persistence: null,
    providerCount: null,
    observationCount: null,
    evidenceIds: [],
    truthComparisonId: null,
    associationAttributeId: null,
    dedupeKey: `movement:${geographyKey}:${brand}`,
    priorityRank: 30,
    WHY_INCLUDED: "Material Presence movement across comparable periods.",
  };
}

function buildSourceTile(sourcePanel, geographyKey) {
  if (!sourcePanel || sourcePanel.CITATION_SUPPORT === "NOT_SUPPORTED") return null;

  const owned = sourcePanel.OWNED_SOURCE_CITATION_RATE;
  const external = sourcePanel.EXTERNAL_SOURCE_CITATION_RATE || sourcePanel.THIRD_PARTY_CITATION_RATE;

  if (owned?.value === 0 && owned?.denominator > 0) {
    return {
      findingType: FINDING_TYPES.SOURCE_CITATION_GAP,
      title: "Source / Citation Gap",
      headline: `Owned brand sources are underrepresented in AI citations for ${geographyKey}.`,
      context: geographyKey,
      evidenceSummary: `External sources dominate cited references in comparable responses.`,
      reviewAction: "Review source and citation representation for governed brand domains.",
      actionCategory: ACTION_CATEGORIES.SOURCE_CITATION_COVERAGE,
      scenarioId: null,
      brandId: null,
      peerBrandIds: [],
      classification: null,
      persistence: null,
      providerCount: null,
      observationCount: owned.denominator || null,
      evidenceIds: [],
      truthComparisonId: null,
      associationAttributeId: null,
      dedupeKey: `source:owned:${geographyKey}`,
      priorityRank: 40,
      WHY_INCLUDED: "Owned sources cited at zero rate in comparable cohort.",
    };
  }

  if (
    external?.value != null &&
    owned?.value != null &&
    external.value - owned.value >= 0.25 &&
    external.denominator > 0
  ) {
    return {
      findingType: FINDING_TYPES.SOURCE_CITATION_GAP,
      title: "Source / Citation Gap",
      headline: `External sources dominate AI citations compared with owned brand domains in ${geographyKey}.`,
      context: geographyKey,
      evidenceSummary: `Recurring external citations outweigh owned-domain citations in comparable responses.`,
      reviewAction: "Review source and citation coverage for owner-facing brand information.",
      actionCategory: ACTION_CATEGORIES.SOURCE_CITATION_COVERAGE,
      scenarioId: null,
      brandId: null,
      peerBrandIds: [],
      classification: null,
      persistence: null,
      providerCount: null,
      observationCount: external.denominator || null,
      evidenceIds: [],
      truthComparisonId: null,
      associationAttributeId: null,
      dedupeKey: `source:external:${geographyKey}`,
      priorityRank: 40,
      WHY_INCLUDED: "External citation share materially exceeds owned sources.",
    };
  }

  return null;
}

const NARRATIVE_PRODUCTION_ELIGIBLE_FAMILIES = Object.freeze([
  "CONVERSION_SUITABILITY",
  "DISTRIBUTION_LOYALTY",
]);

function buildNarrativeFindingTiles(narrativesByBrand, brandNamesById, existingCandidates) {
  const tiles = [];
  if (!narrativesByBrand) return tiles;

  const existingDistributionAssoc = existingCandidates.some(
    (c) =>
      c.findingType === FINDING_TYPES.STRONGEST_VALIDATED_ASSOCIATION &&
      c.associationAttributeId &&
      /distribution|loyalty/i.test(String(c.associationAttributeId))
  );

  for (const [brandName, narratives] of Object.entries(narrativesByBrand)) {
    for (const n of narratives) {
      if (!NARRATIVE_PRODUCTION_ELIGIBLE_FAMILIES.includes(n.narrativeFamily)) continue;
      if (n.productionState !== "PRODUCTION_ELIGIBLE") continue;
      if (n.observationCount < 2) continue;
      if (!n.relationshipToBrand || n.relationshipToBrand === "GENERAL_CONTEXT" || n.relationshipToBrand === "AMBIGUOUS") continue;

      // Non-redundancy: skip DISTRIBUTION_LOYALTY if distribution association already present for same brand
      if (n.narrativeFamily === "DISTRIBUTION_LOYALTY" && existingDistributionAssoc) {
        const assocForBrand = existingCandidates.find(
          (c) =>
            c.findingType === FINDING_TYPES.STRONGEST_VALIDATED_ASSOCIATION &&
            c.brandId === n.brandId &&
            /distribution|loyalty/i.test(String(c.associationAttributeId))
        );
        if (assocForBrand && n.providers.length <= (assocForBrand.providerCount || 1)) {
          continue;
        }
      }

      const providerStr = `${n.providers.length} provider${n.providers.length > 1 ? "s" : ""}`;
      const obsStr = `${n.observationCount} observations`;
      const recurrence = n.stabilityContext?.label || "Short-term recurrence";

      tiles.push({
        findingType: FINDING_TYPES.NARRATIVE_PATTERN,
        title: "Recurring Narrative",
        headline: `${brandName} is repeatedly associated with ${(NARRATIVE_FAMILY_LABELS[n.narrativeFamily] || n.narrativeFamily).toLowerCase()}`,
        context: n.geographies?.[0] || null,
        evidenceSummary: `${providerStr} · ${obsStr} · ${recurrence}`,
        reviewAction: `Review whether owned materials clearly communicate ${(NARRATIVE_FAMILY_LABELS[n.narrativeFamily] || n.narrativeFamily).toLowerCase()} positioning.`,
        actionCategory: ACTION_CATEGORIES.BRAND_POSITIONING,
        actionDisposition: n.reviewDisposition === "NO_ACTION_EXPECTED_POSITIONING"
          ? ACTION_DISPOSITION.NO_ACTION || "NO_ACTION_EXPECTED_POSITIONING"
          : n.reviewDisposition === "MONITOR_ONLY"
            ? ACTION_DISPOSITION.MONITOR || "MONITOR_ONLY"
            : ACTION_DISPOSITION.REVIEW || "REVIEW_REQUIRED",
        scenarioId: n.scenarioIds?.[0] || null,
        scenarioName: (n.scenarioIds || []).slice(0, 2).join(", "),
        brandId: n.brandId,
        brandName,
        peerBrandIds: [],
        classification: null,
        persistence: null,
        providerCount: n.providers.length,
        observationCount: n.observationCount,
        comparableResponseCount: n.comparableResponseCount || null,
        scenarioCount: Array.isArray(n.scenarioIds) ? n.scenarioIds.length : 0,
        evidenceIds: n.responseIds || [],
        truthComparisonId: null,
        associationAttributeId: null,
        narrativeFamily: n.narrativeFamily,
        dedupeKey: `narrative:${n.brandId}:${n.narrativeFamily}`,
        priorityRank: n.providers.length >= 3 ? 72 : n.providers.length >= 2 ? 62 : 48,
        WHY_INCLUDED: `Production-eligible narrative with ${providerStr} coverage.`,
      });
    }
  }

  return tiles.sort((a, b) => (b.priorityRank || 0) - (a.priorityRank || 0)).slice(0, 2);
}

function attachFindingObservationSupport(finding, evidence) {
  const ids = new Set((finding?.evidenceIds || []).filter(Boolean));
  const rows = Array.isArray(evidence)
    ? evidence.filter((e) => ids.has(e.evidenceId) || ids.has(e.observationId))
    : [];
  const observationCount = rows.length || Number(finding?.observationCount) || 0;
  const providers = new Set(
    rows.map((e) => String(e.provider?.name || e.provider || "").toLowerCase()).filter(Boolean)
  );
  const providerCount = providers.size || Number(finding?.providerCount) || 0;
  const descriptor = formatFindingSupportDescriptor({
    observationCount,
    providerCount,
    presenceCount: finding.observationCount != null ? finding.observationCount : observationCount,
    stabilityState: providerCount >= 2 && observationCount >= 2 ? null : finding.stabilityState,
    dateSpanLabel: null,
  });
  return {
    ...finding,
    observationSupport: descriptor,
  };
}

function dedupeFindings(findings = []) {
  const out = [];
  const seen = new Set();

  for (const f of findings) {
    if (!f || seen.has(f.dedupeKey)) continue;

    // Merge LARGEST + HIGHEST when same gap
    if (f.findingType === FINDING_TYPES.HIGHEST_PRIORITY_REVIEW) {
      const dupGap = out.find(
        (x) =>
          x.findingType === FINDING_TYPES.LARGEST_COMPETITIVE_GAP &&
          x.gapId &&
          x.gapId === f.gapId
      );
      if (dupGap) {
        dupGap.reviewAction = f.reviewAction || dupGap.reviewAction;
        dupGap.actionCategory = f.actionCategory || dupGap.actionCategory;
        continue;
      }
    }

    seen.add(f.dedupeKey);
    out.push(f);
  }

  return out;
}

function rankFindings(findings = []) {
  return [...findings].sort((a, b) => {
    if ((b.priorityRank || 0) !== (a.priorityRank || 0)) {
      return (b.priorityRank || 0) - (a.priorityRank || 0);
    }
    if ((b.providerCount || 0) !== (a.providerCount || 0)) {
      return (b.providerCount || 0) - (a.providerCount || 0);
    }
    if ((b.observationCount || 0) !== (a.observationCount || 0)) {
      return (b.observationCount || 0) - (a.observationCount || 0);
    }
    const ia = FINDING_TYPE_ORDER.indexOf(a.findingType);
    const ib = FINDING_TYPE_ORDER.indexOf(b.findingType);
    return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib);
  });
}

function collectValidatedAssociations(evidence = [], brandIds = [], opts = {}) {
  const rows = [];
  for (const brandId of brandIds) {
    const aggs = aggregateBrandAssociations(evidence, brandId, {
      language: opts.language || "en",
      requireMappedScenario: true,
    });
    for (const agg of aggs) {
      if (!isAssociationAttributeProductionEligible(agg.attributeId)) continue;
      if (agg.polarity !== "POSITIVE") continue;
      if ((agg.observationCount || 0) < 2) continue;
      rows.push({
        brandId,
        attributeId: agg.attributeId,
        observationCount: agg.observationCount,
        providers: agg.providers,
        scenarios: agg.scenarios,
        descriptor: agg.descriptor,
        count: agg.observationCount,
      });
    }
  }
  return rows.sort((a, b) => (b.observationCount || 0) - (a.observationCount || 0));
}

/**
 * Build portfolio or brand-scoped executive findings.
 * @param {object} input
 */
export async function buildExecutiveFindings(input = {}) {
  const {
    store,
    brandIds = [],
    brandNamesById = {},
    geographyKey = "CALA",
    language = "en",
    scope = "portfolio",
    subjectBrandId = null,
    crossProvider = null,
    presenceChange = null,
    sourceExecutivePanel = null,
    peerSetId = PEER_SET_ID_V2,
    gaps: preloadedGaps = null,
    preloadedGaps: preloadedGapsAlias = null,
    evidence: preloadedEvidence = null,
    preloadedEvidence: preloadedEvidenceAlias = null,
  } = input;

  const resolvedGaps = preloadedGaps ?? preloadedGapsAlias;
  const resolvedEvidence = preloadedEvidence ?? preloadedEvidenceAlias;

  if (!store && resolvedGaps == null) {
    return emptyFindings("no_store");
  }

  const registry = loadScenarioRegistry();
  const scenarioIndex = buildScenarioRegistryIndex(registry);

  let gaps = resolvedGaps;
  let evidence = resolvedEvidence;
  if (!gaps && store) {
    const engine = await runCompetitiveGapEngineFromStore(store, {
      geography: geographyKey,
      language,
      brandIds: scope === "brand" && subjectBrandId ? [subjectBrandId] : brandIds,
      brandNamesById,
      peerSetId,
    });
    gaps = engine.gaps || [];
    if (!evidence) {
      evidence = (await store.listEvidence({})) || [];
    }
  }

  const productionGaps = (gaps || []).filter(
    (g) =>
      g.classification &&
      g.lifecycleStatus !== "NOT_COMPARABLE" &&
      g.classification !== "MONITOR"
  );

  const scopeBrandIds =
    scope === "brand" && subjectBrandId ? [subjectBrandId] : brandIds;

  const { comparisons } = loadCachedTruthComparisons();
  const cohortTruth = filterTruthComparisonsForCohort(comparisons, {
    language,
    geography: geographyKey,
    brandIds: scopeBrandIds,
  }).filter(
    (c) =>
      c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP" && c.executiveEligible === true
  );

  const gapAudit = auditGapInterpretations(productionGaps, {
    brandNamesById,
    scenarioIndex,
  });
  const interpretationByGapId = new Map(
    gapAudit.interpretations.map((r) => [r.gapId, r])
  );

  const candidates = [];

  const executiveEligibleGaps = rankedProductionGapsFromInterpreted(
    productionGaps,
    gapAudit.interpretations
  );

  const gapTiles = [];
  const seenGapSubjects = new Set();
  const maxGapTiles = scope === "brand" ? 2 : 2;
  for (const gap of executiveEligibleGaps) {
    const interpretation = interpretationByGapId.get(gap.gapId);
    if (!interpretation?.executiveEligible) continue;
    const subjectKey = gap.subjectBrandId || "unknown";
    if (scope === "portfolio" && seenGapSubjects.has(subjectKey) && gapTiles.length >= 1) {
      continue;
    }
    if (scope === "brand" && gapTiles.length >= maxGapTiles) break;
    if (scope === "portfolio" && gapTiles.length >= maxGapTiles) break;
    seenGapSubjects.add(subjectKey);
    gapTiles.push(
      buildCompetitiveGapTile(gap, brandNamesById, scenarioIndex, {
        geographyKey,
        interpretation,
        scenarioIndex,
      })
    );
  }
  candidates.push(...gapTiles.filter((t) => t.executiveEligible !== false));

  const topEligibleGap = executiveEligibleGaps[0] || null;
  if (
    topEligibleGap &&
    interpretationByGapId.get(topEligibleGap.gapId)?.executiveEligible
  ) {
    const topInterp = interpretationByGapId.get(topEligibleGap.gapId);
    const review = resolveReviewActionForGap(topEligibleGap, topInterp);
    if (topInterp.actionDisposition === ACTION_DISPOSITION.ACTION_REQUIRED) {
      candidates.push({
        ...buildCompetitiveGapTile(topEligibleGap, brandNamesById, scenarioIndex, {
          geographyKey,
          interpretation: topInterp,
          scenarioIndex,
        }),
        findingType: FINDING_TYPES.HIGHEST_PRIORITY_REVIEW,
        title: "Highest Priority Review",
        reviewAction: review.text,
        actionCategory: review.category,
        actionDisposition: topInterp.actionDisposition,
        dedupeKey: `review:${topEligibleGap.gapId}`,
        priorityRank: topEligibleGap.classification === "HIGH_PRIORITY" ? 95 : 78,
        WHY_INCLUDED: "Priority review derived from eligible competitive gap.",
        gapId: topEligibleGap.gapId,
      });
    }
  }

  for (const cmp of cohortTruth) {
    candidates.push(buildTruthTile(cmp, brandNamesById, scenarioIndex));
  }

  const assocRows = evidence
    ? collectValidatedAssociations(evidence, scopeBrandIds, { language })
    : [];
  for (const row of assocRows.slice(0, scope === "portfolio" ? 1 : 2)) {
    const tile = buildAssociationTile(row, brandNamesById);
    if (tile) candidates.push(tile);
  }

  const providerTile = buildProviderDisagreementTile(
    crossProvider,
    geographyKey,
    scope === "brand" ? brandNamesById[subjectBrandId] : null
  );
  if (providerTile) candidates.push(providerTile);

  const sourceTile = buildSourceTile(sourceExecutivePanel, geographyKey);
  if (sourceTile) candidates.push(sourceTile);

  const movementTile = buildMovementTile(presenceChange, geographyKey);
  if (movementTile) candidates.push(movementTile);

  // Narrative Intelligence V1 — production-eligible families only
  let narrativesByBrand = null;
  try {
    if (evidence && evidence.length) {
      const stabilityReport = loadAuthoritativeStabilityReport();
      const portfolioNarratives = buildPortfolioNarratives({
        evidence,
        stabilityReport,
        portfolio: Object.fromEntries(
          scopeBrandIds.map((id) => [brandNamesById[id] || id, id])
        ),
        familyPrecision: { CONVERSION_SUITABILITY: 1.0, DISTRIBUTION_LOYALTY: 1.0 },
      });
      narrativesByBrand = portfolioNarratives.byBrand;
    }
  } catch (_) { /* narrative layer is non-blocking */ }
  const narrativeTiles = buildNarrativeFindingTiles(narrativesByBrand, brandNamesById, candidates);
  candidates.push(...narrativeTiles);

  const ranked = rankFindings(dedupeFindings(candidates));
  const selectedFindings = ranked.slice(0, MAX_FINDINGS).map((f) =>
    attachFindingObservationSupport(f, evidence)
  );
  const governance = applyExecutiveCopyGovernance(selectedFindings);
  const findings = governance.findings;

  const summary = {
    LARGEST_COMPETITIVE_GAP: findings.find(
      (f) => f.findingType === FINDING_TYPES.LARGEST_COMPETITIVE_GAP
    ) || null,
    HIGHEST_PRIORITY_REVIEW: findings.find(
      (f) => f.findingType === FINDING_TYPES.HIGHEST_PRIORITY_REVIEW
    ) || null,
    VALIDATED_ASSOCIATION: findings.find(
      (f) => f.findingType === FINDING_TYPES.STRONGEST_VALIDATED_ASSOCIATION
    ) || null,
    TRUTH_GAP: findings.find(
      (f) => f.findingType === FINDING_TYPES.POTENTIAL_AI_PERCEPTION_GAP
    ) || null,
    PROVIDER_DISAGREEMENT: findings.find(
      (f) => f.findingType === FINDING_TYPES.PROVIDER_DISAGREEMENT
    ) || null,
    MOVEMENT: findings.find((f) => f.findingType === FINDING_TYPES.MATERIAL_MOVEMENT) || null,
    SOURCE_INSIGHT:
      findings.find((f) => f.findingType === FINDING_TYPES.SOURCE_CITATION_GAP) || null,
    NARRATIVE_PATTERN:
      findings.find((f) => f.findingType === FINDING_TYPES.NARRATIVE_PATTERN) || null,
  };

  return {
    version: EXECUTIVE_FINDING_ENGINE_VERSION,
    IMPLEMENTED: true,
    scope,
    geographyKey,
    language,
    findings,
    totalFindings: findings.length,
    intelligenceInputs: {
      P0C_GAPS: productionGaps.length,
      VALIDATED_ASSOCIATION_ATTRIBUTES: assocRows.map((r) => r.attributeId),
      EXECUTIVE_TRUTH_GAPS: cohortTruth.length,
      ...gapAudit.counts,
    },
    commercialInterpretation: {
      ruleVersion: GAP_INTERPRETATION_RULE_VERSION,
      counts: gapAudit.counts,
      noActionGaps: gapAudit.noActionGaps.slice(0, 10),
    },
    summary,
    copyGovernance: {
      version: governance.version,
      checks: governance.checks,
    },
    safety: {
      RESEARCH_ASSOCIATIONS_CLIENT_VISIBLE: 0,
      NON_EXECUTIVE_TRUTH_GAPS_IN_EXEC: 0,
      CENSUS_FINDINGS: 0,
      NUMERIC_OPPORTUNITY_SCORES: 0,
    },
    emptyReason: findings.length >= MIN_FINDINGS ? null : "no_material_findings",
    NEW_PROVIDER_CALLS: 0,
    AIRTABLE_WRITES: 0,
  };
}

function emptyFindings(reason) {
  return {
    version: EXECUTIVE_FINDING_ENGINE_VERSION,
    IMPLEMENTED: true,
    findings: [],
    totalFindings: 0,
    intelligenceInputs: {
      P0C_GAPS: 0,
      VALIDATED_ASSOCIATION_ATTRIBUTES: [],
      EXECUTIVE_TRUTH_GAPS: 0,
    },
    summary: {},
    safety: {
      RESEARCH_ASSOCIATIONS_CLIENT_VISIBLE: 0,
      NON_EXECUTIVE_TRUTH_GAPS_IN_EXEC: 0,
      CENSUS_FINDINGS: 0,
      NUMERIC_OPPORTUNITY_SCORES: 0,
    },
    emptyReason: reason,
    NEW_PROVIDER_CALLS: 0,
    AIRTABLE_WRITES: 0,
  };
}

/**
 * Brand detail intelligence — scenario grouping, associations, truth, gaps.
 * @param {object} input
 */
export async function buildBrandDetailIntelligence(input = {}) {
  const {
    store,
    brandId,
    brandName = null,
    brandNamesById = {},
    geographyKey = "CALA",
    language = "en",
    provider = null,
    crossProvider = null,
    peerSetId = PEER_SET_ID_V2,
  } = input;

  if (!store || !brandId) {
    return { ok: false, reason: "missing_brand_or_store" };
  }

  const registry = loadScenarioRegistry();
  const scenarioIndex = buildScenarioRegistryIndex(registry);

  const engine = await runCompetitiveGapEngineFromStore(store, {
    geography: geographyKey,
    language,
    brandIds: [brandId],
    brandNamesById,
    peerSetId,
  });
  const brandResult = engine.brandResults?.[0] || null;
  const gaps = (brandResult?.gaps || []).filter((g) => g.classification);
  const gapAudit = auditGapInterpretations(
    gaps.filter((g) => g.lifecycleStatus !== "NOT_COMPARABLE"),
    { brandNamesById, scenarioIndex }
  );
  const interpretationByGapId = new Map(
    gapAudit.interpretations.map((r) => [r.gapId, r])
  );

  const evidence = (await store.listEvidence({})) || [];
  const associations = collectValidatedAssociations(evidence, [brandId], { language });

  const { comparisons } = loadCachedTruthComparisons();
  const truthComparisons = filterTruthComparisonsForCohort(comparisons, {
    language,
    geography: geographyKey,
    brandIds: [brandId],
  });

  const executiveTruth = truthComparisons.filter(
    (c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP" && c.executiveEligible === true
  );
  const detailTruth = truthComparisons.filter(
    (c) =>
      c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP" &&
      c.executiveEligible !== true &&
      c.comparisonStatus !== "NOT_EVALUATED"
  );

  const scenarioMap = new Map();
  for (const gap of gaps) {
    const sid = gap.scenarioId || "UNMAPPED";
    if (!scenarioMap.has(sid)) {
      scenarioMap.set(sid, {
        scenarioId: sid,
        scenarioName: scenarioLabel(sid, scenarioIndex),
        aiPresence: brandResult?.presence?.value ?? null,
        aiPresenceDisplay: brandResult?.presence?.display ?? null,
        questionsMissing: brandResult?.questionsMissing?.count ?? null,
        gaps: [],
        associations: [],
        truthSignals: [],
      });
    }
    scenarioMap.get(sid).gaps.push({
      classification: gap.classification,
      persistence: gap.persistence,
      peerBrandIds: (gap.peerBrandIds || []).slice(0, 4),
      peerNames: peerNames(gap.peerBrandIds, brandNamesById),
      providerCount: (gap.providers || []).length,
      observationCount: gap.observationCount,
      gapClass: gap.gapClass,
      interpretation: interpretationByGapId.get(gap.gapId) || null,
      executiveEligible: interpretationByGapId.get(gap.gapId)?.executiveEligible || false,
      actionDisposition: interpretationByGapId.get(gap.gapId)?.actionDisposition || null,
    });
  }

  for (const cmp of executiveTruth) {
    const sid = cmp.scenarioId || "UNMAPPED";
    if (!scenarioMap.has(sid)) {
      scenarioMap.set(sid, {
        scenarioId: sid,
        scenarioName: scenarioLabel(sid, scenarioIndex),
        gaps: [],
        associations: [],
        truthSignals: [],
      });
    }
    scenarioMap.get(sid).truthSignals.push({
      truthComparisonId: cmp.truthComparisonId,
      headline: cmp.clientSafeExplanation || cmp.comparisonReason,
      executiveEligible: true,
      dimension: cmp.aiSemanticDimension || cmp.aiClaimType,
    });
  }

  const recommendedReviews = gapAudit.interpretations
    .filter(
      (r) =>
        r.actionDisposition === ACTION_DISPOSITION.ACTION_REQUIRED ||
        r.actionDisposition === ACTION_DISPOSITION.REVIEW_REQUIRED
    )
    .sort((a, b) => {
      const order = {
        [ACTION_DISPOSITION.ACTION_REQUIRED]: 3,
        [ACTION_DISPOSITION.REVIEW_REQUIRED]: 2,
        [ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING]: 1,
      };
      return (order[b.actionDisposition] || 0) - (order[a.actionDisposition] || 0);
    })
    .slice(0, 5)
    .map((r) => ({
      scenarioId: r.scenarioId,
      scenarioName: scenarioLabel(r.scenarioId, scenarioIndex),
      actionCategory: r.reviewActionType,
      actionDisposition: r.actionDisposition,
      text: r.reviewAction,
      classification: r.rawClassification,
      eligibilityStatus: r.eligibilityStatus,
      executiveEligible: r.executiveEligible,
    }));

  return {
    ok: true,
    brandId,
    brandName,
    geographyKey,
    language,
    provider,
    topScenarios: [...scenarioMap.values()].slice(0, 8),
    competitiveGaps: gaps.slice(0, 12).map((g) => ({
      ...g,
      interpretation: interpretationByGapId.get(g.gapId) || null,
    })),
    gapInterpretationAudit: gapAudit.counts,
    noActionPositioningGaps: gapAudit.noActionGaps.slice(0, 8),
    detailReviewGaps: gapAudit.detailReviewGaps.slice(0, 8),
    validatedAssociations: associations.map((a) => ({
      attributeId: a.attributeId,
      observationCount: a.observationCount,
      providers: a.providers,
      scenarios: a.scenarios,
      descriptor: a.descriptor,
      clientVisible: isAssociationAttributeProductionEligible(a.attributeId),
    })),
    truthComparisons: {
      executiveEligible: executiveTruth.map((c) => ({
        truthComparisonId: c.truthComparisonId,
        headline: c.clientSafeExplanation,
        aiClaim: c.aiClaimValue,
        dealalityFact: c.dealalityFactValue,
        dimension: c.aiSemanticDimension,
        evidenceId: c.evidenceId,
        provider: c.provider,
      })),
      detailOnly: detailTruth.slice(0, 5).map((c) => ({
        truthComparisonId: c.truthComparisonId,
        headline: c.comparisonReason,
        detailOnly: true,
        dimension: c.aiSemanticDimension,
      })),
    },
    providerDisagreement: crossProvider?.PROVIDER_DISAGREEMENT?.status === "DISAGREE"
      ? {
          strongest: crossProvider.STRONGEST_PROVIDER_BY_PRESENCE,
          weakest: crossProvider.WEAKEST_PROVIDER_BY_PRESENCE,
          spread: crossProvider.PRESENCE_RANGE?.spread ?? null,
        }
      : null,
    presence: brandResult?.presence ?? null,
    questionsMissing: brandResult?.questionsMissing ?? null,
    peerPresentSubjectMissing: brandResult?.peerPresentSubjectMissing ?? null,
    recommendedReviews,
    safety: {
      RESEARCH_ASSOCIATIONS_CLIENT_VISIBLE: 0,
      CENSUS_FINDINGS: 0,
    },
  };
}

/**
 * Convert P0E findings to legacy insight box shape for UI backward compatibility.
 * @param {object} executiveFindings
 */
export function executiveFindingsToInsightBoxes(executiveFindings) {
  const findings = executiveFindings?.findings || [];
  if (!findings.length) {
    return {
      version: EXECUTIVE_FINDING_ENGINE_VERSION,
      title: "Executive Summary",
      IMPLEMENTED: true,
      boxes: [],
      insightTypes: [],
      p0ePrimary: true,
    };
  }

  const boxes = findings.map((f) => ({
    type: f.findingType,
    title: f.title,
    headline: f.governedHeadline || f.headline,
    finding: f.governedBody || f.headline,
    evidence: f.governedEvidence || f.evidenceSummary,
    evidenceConstruct: f.evidenceConstruct || null,
    observationSupport: f.observationSupport || null,
    soWhat: f.context || null,
    whatToWatch: null,
    takeaway: f.governedBody || f.headline || null,
    actionDisposition: f.actionDisposition || null,
    semanticValidationPass: f.semanticValidationPass !== false,
    evidenceRefs: f.evidenceIds?.length ? f.evidenceIds : [],
    dedupeKey: f.dedupeKey,
    p0eFinding: f,
    CAUSAL_LANGUAGE_USED: false,
  }));

  return {
    version: EXECUTIVE_FINDING_ENGINE_VERSION,
    title: "Executive Summary",
    IMPLEMENTED: true,
    boxes,
    insightTypes: boxes.map((b) => b.type),
    p0ePrimary: true,
  };
}
