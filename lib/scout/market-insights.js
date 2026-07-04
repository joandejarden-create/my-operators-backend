/**
 * Scout Phase 5B — Market Insight Engine (read-only).
 * Combines census coverage, signals, saved watchlist, demand overlays, and brand aliases.
 */

import {
  CENSUS_INDEPENDENT_AFFILIATION,
  STATUS_OPEN,
  STATUS_PIPELINE,
} from "../hotel-census/fields.js";
import {
  exactMatchKey,
  normalizeParentCompanyKey,
  loadActiveBrandAliasRows,
  resolveBrandAffiliationMatchers,
} from "../hotel-census/brand-alias-resolve.js";
import { buildMarketCoverageReport } from "./market-coverage.js";
import { buildOpportunitySignalsReport } from "./opportunity-signals.js";
import { annotateGeneratedSignalsWithSavedStatus, listSavedSignals } from "./scout-signal-watchlist.js";
import { buildDemandOverlaysReport } from "./demand-overlays.js";
import { calibrateInsights } from "./insight-calibration.js";

export const INSIGHT_TYPES = [
  "parent_company_underrepresentation",
  "brand_underrepresentation",
  "chain_scale_gap",
  "independent_conversion_potential",
  "operator_white_space",
  "pipeline_momentum",
  "all_inclusive_potential",
  "branded_residential_potential",
  "demand_driver_supported_opportunity",
];

export const OPPORTUNITY_TYPES = [
  "Brand White Space",
  "Operator White Space",
  "Conversion Opportunity",
  "Pipeline / Development Opportunity",
  "All-Inclusive Potential",
  "Branded Residential Potential",
  "Demand-Driver Supported Market",
];

/** Transparent opportunity scoring (cap 100). */
export const SCOUT_INSIGHT_SCORING = {
  existingBrandedSupply: 10,
  independentSupplyCluster: 15,
  largeIndependentAssets: 10,
  parentBrandGap: 20,
  chainScaleGap: 15,
  pipelineActivity: 10,
  demandDriversPresent: 15,
  airportTourismMiceMixedUseAnchor: 10,
  savedSignalHumanReview: 5,
};

const CHAIN_SCALES = [
  "Luxury",
  "Upper Upscale",
  "Upscale",
  "Upper Midscale",
  "Midscale",
  "Economy",
  "Extended Stay",
];

const BEACH_TOURISM_COUNTRIES = /mexico|dominican|puerto rico|jamaica|costa rica|panama|colombia|caribbean/i;

const TOURISM_ANCHOR_CATEGORIES = [
  "Tourist Attraction",
  "Beach / Waterfront",
  "Entertainment District",
  "Convention Center",
  "Mixed-Use Development",
  "Resort",
];

const DEFAULT_LIMIT = 100;

function capScore(n) {
  return Math.min(100, Math.max(0, Math.round(n)));
}

function slug(s) {
  return String(s || "geo")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 48);
}

function titleCasePriority(confidence) {
  const c = String(confidence || "medium").toLowerCase();
  if (c === "high") return { priority: "High", confidence: "High" };
  if (c === "low") return { priority: "Low", confidence: "Low" };
  return { priority: "Medium", confidence: "Medium" };
}

function geographyLabel(filters, metrics) {
  const parts = [];
  if (filters.submarket) parts.push(filters.submarket);
  else if (filters.market) parts.push(filters.market);
  else if (filters.city) parts.push(filters.city);
  else if (filters.country) parts.push(filters.country);
  if (parts.length) return parts.join(" · ");
  if (metrics?.strMarketCount === 1) return "Selected market";
  return "Selected geography";
}

function evidenceLevel(hasSignals, hasOverlays) {
  if (hasSignals && hasOverlays) return "census_plus_signals_plus_demand_overlays";
  if (hasSignals) return "census_plus_signals";
  if (hasOverlays) return "census_plus_demand_overlays";
  return "census_only";
}

function insightBase({
  insightType,
  title,
  geographyLabel: geo,
  priority,
  confidence,
  insightText,
  whyItMatters,
  supportingMetrics,
  relatedSignalIds,
  relatedDemandDrivers,
  recommendedNextStep,
  evidenceLevel: ev,
}) {
  return {
    insightId: `insight-${insightType}-${slug(geo)}-${slug(title).slice(0, 24)}`,
    insightType,
    title,
    geographyLabel: geo,
    priority,
    confidence,
    insightText,
    whyItMatters,
    supportingMetrics,
    relatedSignalIds: relatedSignalIds || [],
    relatedDemandDrivers: relatedDemandDrivers || [],
    recommendedNextStep,
    evidenceLevel: ev,
  };
}

function signalTypeForInsight(insightType) {
  const map = {
    parent_company_underrepresentation: "parent_company_market_gap",
    brand_underrepresentation: "brand_market_gap",
    independent_conversion_potential: "independent_conversion_cluster",
    operator_white_space: "operator_opportunity_market",
    pipeline_momentum: "pipeline_activity",
    chain_scale_gap: "brand_market_gap",
    demand_driver_supported_opportunity: "parent_company_market_gap",
  };
  return map[insightType] || null;
}

function findRelatedSignals(signals, insightType, filters) {
  const want = signalTypeForInsight(insightType);
  if (!want) return [];
  return (signals || [])
    .filter((s) => s.signalType === want)
    .filter((s) => {
      if (filters.submarket && s.submarket && exactMatchKey(s.submarket) !== exactMatchKey(filters.submarket))
        return false;
      if (filters.market && s.market && exactMatchKey(s.market) !== exactMatchKey(filters.market)) return false;
      return true;
    })
    .map((s) => s.signalId)
    .slice(0, 5);
}

function relatedDrivers(overlays, filters) {
  return (overlays || [])
    .filter((o) => {
      if (filters.submarket && o.submarket && exactMatchKey(o.submarket) !== exactMatchKey(filters.submarket))
        return false;
      if (filters.market && o.market && exactMatchKey(o.market) !== exactMatchKey(filters.market)) return false;
      if (filters.country && o.country && exactMatchKey(o.country) !== exactMatchKey(filters.country)) return false;
      return true;
    })
    .slice(0, 8)
    .map((o) => ({
      overlayId: o.overlayId,
      name: o.name,
      category: o.category,
      overlayType: o.overlayType,
    }));
}

export function parseInsightFilters(query = {}) {
  const limitRaw = parseInt(query.limit, 10);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 200) : DEFAULT_LIMIT;

  return {
    country: exactMatchKey(query.country),
    city: exactMatchKey(query.city),
    market: exactMatchKey(query.market),
    submarket: exactMatchKey(query.submarket),
    parentCompany: exactMatchKey(query.parentCompany || query.parent_company),
    brand: exactMatchKey(query.brand),
    chainScale: exactMatchKey(query.chainScale || query.chain_scale),
    locationType: exactMatchKey(query.locationType || query.location_type),
    includeDemandOverlays:
      query.includeDemandOverlays !== "0" && query.includeDemandOverlays !== "false",
    includeSavedSignals:
      query.includeSavedSignals !== "0" && query.includeSavedSignals !== "false",
    includePipeline: true,
    includeInsightReview:
      query.includeInsightReview === "1" || query.includeInsightReview === "true",
    includeSuppressed:
      query.includeSuppressed === "1" || query.includeSuppressed === "true",
    insightType: exactMatchKey(query.insightType),
    limit,
  };
}

function buildInsightsFromContext(ctx) {
  const { filters, coverage, signals, overlays, aliasRows, savedSignals, brandResolution } = ctx;
  const metrics = coverage.metrics;
  const breakdowns = coverage.breakdowns || {};
  const geo = geographyLabel(filters, metrics);
  const ev = evidenceLevel(signals.length > 0, overlays.length > 0);
  const drivers = relatedDrivers(overlays, filters);
  const insights = [];

  const openBranded = metrics.brandedHotels || 0;
  const parentCount = metrics.parentCompanyCount || 0;
  const independentOpen = metrics.independentHotels || 0;
  const pipelineHotels = metrics.pipelineHotels || 0;
  const pipelineRooms = metrics.pipelineRooms || 0;

  // A. Parent company underrepresentation
  if (filters.parentCompany && openBranded > 0) {
    const parentOpenFromBreakdown =
      (breakdowns.byParentCompany || []).find(
        (b) => normalizeParentCompanyKey(b.label) === normalizeParentCompanyKey(filters.parentCompany)
      )?.openHotels || 0;

    const selectedParentOpen = parentOpenFromBreakdown;
    if (selectedParentOpen === 0) {
      const sharePct =
        openBranded > 0 ? Math.round((selectedParentOpen / openBranded) * 1000) / 10 : 0;
      const { priority, confidence } = titleCasePriority(openBranded >= 20 ? "high" : "medium");
      insights.push(
        insightBase({
          insightType: "parent_company_underrepresentation",
          title: `${filters.parentCompany} appears underrepresented`,
          geographyLabel: geo,
          priority,
          confidence,
          insightText: `${filters.parentCompany} appears underrepresented in ${geo}. The geography has ${openBranded} branded open hotels across ${parentCount} parent companies, but no open ${filters.parentCompany} hotels in the current census (${sharePct}% branded share).`,
          whyItMatters:
            "A branded parent with no open supply where competitors are present may suggest white-space worth reviewing — not a recommendation to enter.",
          supportingMetrics: {
            selectedParentOpenHotels: selectedParentOpen,
            brandedOpenHotels: openBranded,
            parentCompaniesPresent: parentCount,
            brandedSharePct: sharePct,
            pipelineHotels,
            independentHotels: independentOpen,
          },
          relatedSignalIds: findRelatedSignals(signals, "parent_company_underrepresentation", filters),
          relatedDemandDrivers: drivers,
          recommendedNextStep:
            "Review parent-company gap signals and compare chain-scale mix before outreach.",
          evidenceLevel: ev,
        })
      );
    } else if (openBranded >= 10 && selectedParentOpen / openBranded < 0.05) {
      const sharePct = Math.round((selectedParentOpen / openBranded) * 1000) / 10;
      insights.push(
        insightBase({
          insightType: "parent_company_underrepresentation",
          title: `${filters.parentCompany} has limited branded share`,
          geographyLabel: geo,
          priority: "Medium",
          confidence: "Medium",
          insightText: `${filters.parentCompany} appears lightly represented in ${geo} with ${selectedParentOpen} open hotel(s) (~${sharePct}% of ${openBranded} branded open hotels).`,
          whyItMatters: "Low parent share where branded lodging exists may indicate room to expand or reposition — worth reviewing with local supply context.",
          supportingMetrics: {
            selectedParentOpenHotels: selectedParentOpen,
            brandedOpenHotels: openBranded,
            parentCompaniesPresent: parentCount,
            brandedSharePct: sharePct,
            pipelineHotels,
            independentHotels: independentOpen,
          },
          relatedSignalIds: findRelatedSignals(signals, "parent_company_underrepresentation", filters),
          relatedDemandDrivers: drivers,
          recommendedNextStep: "Compare submarket-level branded mix and pipeline activity.",
          evidenceLevel: ev,
        })
      );
    }
  } else if (!filters.parentCompany && openBranded >= 10) {
    const aliasParents = new Set(
      (aliasRows || []).map((r) => normalizeParentCompanyKey(r.parentCompany)).filter(Boolean)
    );
    const presentParents = new Set(
      (breakdowns.byParentCompany || [])
        .filter((b) => b.openHotels > 0 && b.label !== "Unknown")
        .map((b) => normalizeParentCompanyKey(b.label))
    );
    for (const ap of aliasParents) {
      if (presentParents.has(ap)) continue;
      const display =
        (aliasRows || []).find((r) => normalizeParentCompanyKey(r.parentCompany) === ap)
          ?.parentCompany || ap;
      if (insights.filter((i) => i.insightType === "parent_company_underrepresentation").length >= 3)
        break;
      insights.push(
        insightBase({
          insightType: "parent_company_underrepresentation",
          title: `${display} absent from branded supply`,
          geographyLabel: geo,
          priority: "Medium",
          confidence: "Low",
          insightText: `${display} does not appear to have open branded hotels in ${geo}, while ${openBranded} other branded open hotels are present across ${parentCount} parent companies.`,
          whyItMatters:
            "Alias-mapped parent companies with no census presence may suggest a gap worth reviewing where branded demand exists.",
          supportingMetrics: {
            brandedOpenHotels: openBranded,
            parentCompaniesPresent: parentCount,
            absentParentCompany: display,
            independentHotels: independentOpen,
          },
          relatedSignalIds: findRelatedSignals(signals, "parent_company_underrepresentation", filters),
          relatedDemandDrivers: drivers,
          recommendedNextStep: "Filter by parent company to validate gap and review signals.",
          evidenceLevel: ev,
        })
      );
    }
  }

  // B. Brand underrepresentation
  if (filters.brand && openBranded > 0) {
    let brandOpen = 0;
    if (brandResolution?.ok && brandResolution.affiliationMatchers?.length) {
      const matchers = new Set(brandResolution.affiliationMatchers);
      brandOpen = (breakdowns.byBrand || [])
        .filter((b) => matchers.has(b.label))
        .reduce((s, b) => s + (b.openHotels || 0), 0);
    }
    if (!brandOpen) {
      brandOpen =
        (breakdowns.byBrand || []).find(
          (b) => exactMatchKey(b.label).toLowerCase() === exactMatchKey(filters.brand).toLowerCase()
        )?.openHotels || 0;
    }

    if (brandOpen === 0) {
      const comparableScales = (breakdowns.byChainScale || [])
        .filter((b) => b.openHotels > 0 && b.label !== "Unknown" && b.label !== "Independent")
        .map((b) => b.label);
      insights.push(
        insightBase({
          insightType: "brand_underrepresentation",
          title: `${filters.brand} not present in open supply`,
          geographyLabel: geo,
          priority: comparableScales.length ? "High" : "Medium",
          confidence: comparableScales.length ? "High" : "Medium",
          insightText: `${filters.brand} does not appear to have open supply in ${geo}, while comparable chain-scale supply (${comparableScales.slice(0, 4).join(", ") || "other branded"}) is present.`,
          whyItMatters:
            "Absent brand flags where comparable branded supply exists may indicate white space — not a confirmed market opportunity.",
          supportingMetrics: {
            selectedBrandOpenHotels: brandOpen,
            brandedOpenHotels: openBranded,
            comparableChainScales: comparableScales,
            parentCompanyFilter: filters.parentCompany || null,
          },
          relatedSignalIds: findRelatedSignals(signals, "brand_underrepresentation", filters),
          relatedDemandDrivers: drivers,
          recommendedNextStep: "Review brand gap signals and parent-company presence in adjacent submarkets.",
          evidenceLevel: ev,
        })
      );
    }
  }

  // C. Chain scale gap
  const scaleMap = new Map((breakdowns.byChainScale || []).map((b) => [b.label, b.openHotels]));
  const totalOpen = metrics.openHotels || 0;
  if (totalOpen >= 5) {
    for (const scale of CHAIN_SCALES) {
      const open = scaleMap.get(scale) || 0;
      if (open === 0 && totalOpen >= 10) {
        const hasDemand = drivers.length > 0;
        insights.push(
          insightBase({
            insightType: "chain_scale_gap",
            title: `Thin ${scale} supply`,
            geographyLabel: geo,
            priority: hasDemand ? "High" : "Medium",
            confidence: hasDemand ? "Medium" : "Low",
            insightText: `${scale} chain scale appears absent or unreported in ${geo} (${totalOpen} total open hotels in scope).${hasDemand ? " Demand drivers are present, which may increase relevance." : ""}`,
            whyItMatters:
              "Missing chain-scale segments where other lodging exists may suggest positioning gaps worth reviewing.",
            supportingMetrics: {
              chainScale: scale,
              chainScaleOpenHotels: open,
              totalOpenHotels: totalOpen,
              demandDriversPresent: drivers.length,
            },
            relatedSignalIds: [],
            relatedDemandDrivers: drivers,
            recommendedNextStep: "Compare chain-scale mix in adjacent STR submarkets.",
            evidenceLevel: ev,
          })
        );
      }
    }
  }

  // D. Independent conversion potential
  const indThreshold = filters.submarket ? 5 : 15;
  if (independentOpen >= indThreshold) {
    const largeIndep = signals.filter((s) => s.signalType === "large_independent_asset").length;
    const hasBrandedNearby = openBranded > 0;
    const pri = independentOpen >= indThreshold * 2 || largeIndep ? "High" : "Medium";
    insights.push(
      insightBase({
        insightType: "independent_conversion_potential",
        title: "Independent conversion cluster may be worth reviewing",
        geographyLabel: geo,
        priority: pri,
        confidence: drivers.length ? "High" : "Medium",
        insightText: `${geo} has ${independentOpen} open independent hotels${largeIndep ? ` including ${largeIndep} large independent asset signal(s)` : ""}.${hasBrandedNearby ? " Branded supply is also present nearby." : ""}${drivers.length ? ` ${drivers.length} demand driver(s) may support conversion review.` : ""}`,
        whyItMatters:
          "Independent clusters can indicate franchise, brand, or operator conversion potential — requires deal-level diligence.",
        supportingMetrics: {
          independentOpenHotels: independentOpen,
          brandedOpenHotels: openBranded,
          largeIndependentSignals: largeIndep,
          demandDriversPresent: drivers.length,
        },
        relatedSignalIds: findRelatedSignals(signals, "independent_conversion_potential", filters),
        relatedDemandDrivers: drivers,
        recommendedNextStep: "Review independent conversion and large-asset signals; prioritize by rooms and location type.",
        evidenceLevel: ev,
      })
    );
  }

  // E. Operator white space
  const operatorSignals = signals.filter((s) => s.signalType === "operator_opportunity_market");
  if (operatorSignals.length || (independentOpen >= 8 && openBranded >= 3)) {
    insights.push(
      insightBase({
        insightType: "operator_white_space",
        title: "Operator white space may exist",
        geographyLabel: geo,
        priority: operatorSignals.length ? "High" : "Medium",
        confidence: operatorSignals.length ? "High" : "Medium",
        insightText: `${geo} shows ${independentOpen} independents and ${openBranded} branded open hotels.${operatorSignals.length ? ` ${operatorSignals.length} operator opportunity signal(s) suggest fragmented or franchise-heavy supply.` : " Supply mix may suggest operator engagement opportunities worth reviewing."}`,
        whyItMatters:
          "Fragmented ownership and franchise-heavy markets may benefit from third-party operator review — not a confirmed operator mandate.",
        supportingMetrics: {
          independentOpenHotels: independentOpen,
          brandedOpenHotels: openBranded,
          operatorSignals: operatorSignals.length,
          pipelineHotels,
        },
        relatedSignalIds: operatorSignals.map((s) => s.signalId).slice(0, 5),
        relatedDemandDrivers: drivers,
        recommendedNextStep: "Review operator opportunity signals and management company concentration.",
        evidenceLevel: ev,
      })
    );
  }

  // F. Pipeline momentum
  if (pipelineHotels > 0) {
    const gapAlso = insights.some((i) =>
      ["parent_company_underrepresentation", "brand_underrepresentation"].includes(i.insightType)
    );
    insights.push(
      insightBase({
        insightType: "pipeline_momentum",
        title: "Pipeline activity visible",
        geographyLabel: geo,
        priority: gapAlso ? "High" : pipelineHotels >= 3 ? "Medium" : "Low",
        confidence: pipelineHotels >= 3 ? "High" : "Medium",
        insightText: `${geo} has ${pipelineHotels} pipeline hotel(s) (~${pipelineRooms} rooms).${gapAlso ? " Pipeline overlaps with parent/brand gap context — may warrant coordinated review." : ""}`,
        whyItMatters: "Pipeline supply may shift competitive dynamics and timing for brand or operator conversations.",
        supportingMetrics: {
          pipelineHotels,
          pipelineRooms,
          openHotels: metrics.openHotels,
          parentBrandGapsPresent: gapAlso,
        },
        relatedSignalIds: findRelatedSignals(signals, "pipeline_momentum", filters),
        relatedDemandDrivers: drivers,
        recommendedNextStep: "Cross-reference pipeline hotels with white-space and conversion insights.",
        evidenceLevel: ev,
      })
    );
  }

  // G. All-inclusive potential
  const resortHotels = (breakdowns.byLocationType || []).filter(
    (b) => /resort|beach|waterfront/i.test(b.label) && b.openHotels > 0
  );
  const tourismAnchors = drivers.filter((d) =>
    TOURISM_ANCHOR_CATEGORIES.some((c) => (d.category || "").includes(c))
  );
  const beachCountry = BEACH_TOURISM_COUNTRIES.test(filters.country || "");
  if (
    (resortHotels.length || tourismAnchors.length || beachCountry) &&
    (independentOpen > 0 || openBranded > 0)
  ) {
    insights.push(
      insightBase({
        insightType: "all_inclusive_potential",
        title: "All-inclusive positioning may be worth reviewing",
        geographyLabel: geo,
        priority: tourismAnchors.length && resortHotels.length ? "Medium" : "Low",
        confidence: "Low",
        insightText: `${geo} shows tourism/resort indicators (${resortHotels.map((r) => r.label).join(", ") || "resort/beach context"}${tourismAnchors.length ? `; ${tourismAnchors.length} tourism demand anchor(s)` : ""}). Non-all-inclusive or independent supply exists — all-inclusive potential may be worth reviewing, not confirming suitability.`,
        whyItMatters:
          "Resort and tourism demand contexts sometimes support all-inclusive formats; census data alone cannot confirm fit.",
        supportingMetrics: {
          resortLocationTypes: resortHotels.map((r) => r.label),
          tourismAnchors: tourismAnchors.length,
          independentOpenHotels: independentOpen,
          brandedOpenHotels: openBranded,
        },
        relatedSignalIds: [],
        relatedDemandDrivers: tourismAnchors,
        recommendedNextStep: "Validate demand segment, comp set, and operator capabilities before pursuing all-inclusive scenarios.",
        evidenceLevel: ev,
      })
    );
  }

  // H. Branded residential potential
  const upscale = ["Luxury", "Upper Upscale", "Upscale"].some((s) => (scaleMap.get(s) || 0) > 0);
  const mixedUseAnchors = drivers.filter((d) =>
    /mixed-use|residential|growth/i.test(d.category || "")
  );
  if (upscale && (mixedUseAnchors.length || tourismAnchors.length || pipelineHotels > 0)) {
    insights.push(
      insightBase({
        insightType: "branded_residential_potential",
        title: "Branded residential potential may be worth exploring",
        geographyLabel: geo,
        priority: "Low",
        confidence: "Low",
        insightText: `${geo} has luxury/upscale hotel supply and tourism or mixed-use demand context.${pipelineHotels ? ` ${pipelineHotels} pipeline hotel(s) may suggest development activity.` : ""} This may suggest branded residential potential worth reviewing — not a confirmed product fit.`,
        whyItMatters:
          "Upscale lodging plus tourism/mixed-use anchors can coincide with residential branding conversations; requires separate feasibility work.",
        supportingMetrics: {
          upscaleSupplyPresent: upscale,
          mixedUseAnchors: mixedUseAnchors.length,
          tourismAnchors: tourismAnchors.length,
          pipelineHotels,
        },
        relatedSignalIds: [],
        relatedDemandDrivers: [...mixedUseAnchors, ...tourismAnchors].slice(0, 5),
        recommendedNextStep: "Separate branded residential diligence from hotel census signals.",
        evidenceLevel: ev,
      })
    );
  }

  // I. Demand driver supported opportunity
  if (
    drivers.length > 0 &&
    insights.some((i) =>
      [
        "parent_company_underrepresentation",
        "brand_underrepresentation",
        "chain_scale_gap",
        "independent_conversion_potential",
      ].includes(i.insightType)
    )
  ) {
    const gapInsights = insights.filter((i) =>
      ["parent_company_underrepresentation", "brand_underrepresentation", "chain_scale_gap"].includes(
        i.insightType
      )
    );
    insights.push(
      insightBase({
        insightType: "demand_driver_supported_opportunity",
        title: "Supply gaps align with demand drivers",
        geographyLabel: geo,
        priority: "High",
        confidence: "Medium",
        insightText: `${geo} has ${drivers.length} demand driver(s) (${drivers
          .slice(0, 3)
          .map((d) => d.name)
          .join(", ")}${drivers.length > 3 ? "…" : ""}) alongside ${gapInsights.length} supply-gap insight(s). This may indicate demand-supported white space worth reviewing.`,
        whyItMatters:
          "Linking infrastructure and demand anchors to supply gaps helps prioritize markets — still requires local validation.",
        supportingMetrics: {
          demandDrivers: drivers.length,
          supplyGapInsights: gapInsights.length,
          brandedOpenHotels: openBranded,
          independentOpenHotels: independentOpen,
        },
        relatedSignalIds: [
          ...new Set(gapInsights.flatMap((i) => i.relatedSignalIds)),
        ].slice(0, 5),
        relatedDemandDrivers: drivers,
        recommendedNextStep: "Review demand overlays on the map with white-space and conversion insights.",
        evidenceLevel: ev,
      })
    );
  }

  const savedBoost = (savedSignals || []).length > 0;
  if (savedBoost) {
    for (const ins of insights) {
      if (ins.relatedSignalIds?.length) {
        ins.supportingMetrics = {
          ...ins.supportingMetrics,
          savedSignalsInScope: savedSignals.length,
        };
      }
    }
  }

  return insights.slice(0, filters.limit);
}

function scoreOpportunity(type, insights, ctx) {
  let score = 0;
  const { metrics, overlays, savedSignals } = ctx;
  const related = insights.filter((i) => {
    const map = {
      "Brand White Space": ["parent_company_underrepresentation", "brand_underrepresentation", "chain_scale_gap"],
      "Operator White Space": ["operator_white_space"],
      "Conversion Opportunity": ["independent_conversion_potential"],
      "Pipeline / Development Opportunity": ["pipeline_momentum"],
      "All-Inclusive Potential": ["all_inclusive_potential"],
      "Branded Residential Potential": ["branded_residential_potential"],
      "Demand-Driver Supported Market": ["demand_driver_supported_opportunity"],
    };
    return (map[type] || []).includes(i.insightType);
  });
  if (!related.length) return null;

  if ((metrics.brandedHotels || 0) > 0) score += SCOUT_INSIGHT_SCORING.existingBrandedSupply;
  if ((metrics.independentHotels || 0) >= 15) score += SCOUT_INSIGHT_SCORING.independentSupplyCluster;
  if (related.some((i) => i.insightType === "independent_conversion_potential"))
    score += SCOUT_INSIGHT_SCORING.largeIndependentAssets;
  if (related.some((i) => /underrepresentation|chain_scale_gap/.test(i.insightType)))
    score += SCOUT_INSIGHT_SCORING.parentBrandGap;
  if (related.some((i) => i.insightType === "chain_scale_gap"))
    score += SCOUT_INSIGHT_SCORING.chainScaleGap;
  if ((metrics.pipelineHotels || 0) > 0) score += SCOUT_INSIGHT_SCORING.pipelineActivity;
  if ((overlays || []).length > 0) score += SCOUT_INSIGHT_SCORING.demandDriversPresent;
  if (
    (overlays || []).some((o) =>
      /airport|convention|mixed-use|tourist|beach|entertainment/i.test(o.category || "")
    )
  )
    score += SCOUT_INSIGHT_SCORING.airportTourismMiceMixedUseAnchor;
  if ((savedSignals || []).length > 0) score += SCOUT_INSIGHT_SCORING.savedSignalHumanReview;

  const highPri = related.filter((i) => i.priority === "High").length;
  score += highPri * 5;

  return capScore(score);
}

function buildRankedOpportunities(insights, ctx) {
  const geo = geographyLabel(ctx.filters, ctx.metrics);
  const ranked = [];

  for (const opportunityType of OPPORTUNITY_TYPES) {
    const score = scoreOpportunity(opportunityType, insights, ctx);
    if (score == null || score < 15) continue;

    const supportingInsights = insights
      .filter((i) => {
        const map = {
          "Brand White Space": ["parent_company_underrepresentation", "brand_underrepresentation", "chain_scale_gap"],
          "Operator White Space": ["operator_white_space"],
          "Conversion Opportunity": ["independent_conversion_potential"],
          "Pipeline / Development Opportunity": ["pipeline_momentum"],
          "All-Inclusive Potential": ["all_inclusive_potential"],
          "Branded Residential Potential": ["branded_residential_potential"],
          "Demand-Driver Supported Market": ["demand_driver_supported_opportunity"],
        };
        return (map[opportunityType] || []).includes(i.insightType);
      })
      .map((i) => i.insightId);

    const confidence =
      score >= 70 ? "High" : score >= 40 ? "Medium" : "Low";

    ranked.push({
      opportunityType,
      title: `${opportunityType} in ${geo}`,
      geographyLabel: geo,
      score,
      confidence,
      rationale: `Based on ${supportingInsights.length} supporting insight(s) and census/overlay context. Language is indicative — not a recommendation.`,
      supportingInsights,
      suggestedAction:
        opportunityType === "Conversion Opportunity"
          ? "Review independent conversion signals and prioritize by rooms and demand drivers."
          : opportunityType === "Brand White Space"
            ? "Validate parent/brand gaps with STR submarket detail and saved watchlist signals."
            : opportunityType === "Operator White Space"
              ? "Review operator opportunity signals and management fragmentation."
              : opportunityType === "Pipeline / Development Opportunity"
                ? "Map pipeline hotels against white-space insights."
                : opportunityType === "Demand-Driver Supported Market"
                  ? "Cross-reference demand overlays with supply-gap insights on the map."
                  : "Conduct additional market diligence before outreach.",
    });
  }

  return ranked.sort((a, b) => b.score - a.score).slice(0, 15);
}

/**
 * @param {Record<string, string|boolean>} [query]
 */
export async function buildMarketInsightsReport(query = {}) {
  const filters = parseInsightFilters(query);
  const warnings = [];

  const coverageQuery = {
    country: filters.country,
    city: filters.city,
    market: filters.market,
    submarket: filters.submarket,
    parentCompany: filters.parentCompany,
    brand: filters.brand,
    chainScale: filters.chainScale,
    locationType: filters.locationType,
    includePipeline: "1",
  };

  const scopeQuery = {
    country: filters.country,
    city: filters.city,
    market: filters.market,
    submarket: filters.submarket,
    chainScale: filters.chainScale,
    locationType: filters.locationType,
    includePipeline: "1",
  };

  const [coverage, scopeCoverage] = await Promise.all([
    buildMarketCoverageReport(coverageQuery),
    buildMarketCoverageReport(scopeQuery),
  ]);
  if (!coverage.ok) {
    return { ok: false, error: coverage.error || "market coverage failed" };
  }
  if (!scopeCoverage.ok) {
    return { ok: false, error: scopeCoverage.error || "scope coverage failed" };
  }
  warnings.push(...(coverage.warnings || []));

  const signalQuery = {
    ...coverageQuery,
    includePipeline: "1",
    limit: String(filters.limit),
  };

  const [signalReport, saved, overlayReport, aliasRows] = await Promise.all([
    buildOpportunitySignalsReport(signalQuery),
    filters.includeSavedSignals
      ? listSavedSignals({
          country: filters.country,
          market: filters.market,
          submarket: filters.submarket,
          parentCompany: filters.parentCompany,
          brand: filters.brand,
          limit: filters.limit,
        })
      : Promise.resolve({ ok: true, signals: [] }),
    filters.includeDemandOverlays
      ? buildDemandOverlaysReport({
          country: filters.country,
          city: filters.city,
          market: filters.market,
          submarket: filters.submarket,
          limit: String(filters.limit),
        })
      : Promise.resolve(null),
    loadActiveBrandAliasRows().catch((err) => {
      warnings.push(`BRAND_ALIAS: ${err?.message || String(err)}`);
      return [];
    }),
  ]);

  if (!signalReport.ok) {
    warnings.push(`SIGNALS: ${signalReport.error}`);
  }
  let signals = signalReport.signals || [];
  if (filters.includeSavedSignals) {
    signals = await annotateGeneratedSignalsWithSavedStatus(signals);
  }

  let savedSignals = [];
  if (filters.includeSavedSignals) {
    if (!saved.ok) warnings.push(`SAVED: ${saved.error}`);
    else savedSignals = saved.signals || [];
  }

  let overlays = [];
  if (filters.includeDemandOverlays && overlayReport) {
    if (!overlayReport.ok) warnings.push(`OVERLAYS: ${overlayReport.error}`);
    else {
      overlays = [
        ...(overlayReport.overlayMarkers || []),
        ...(overlayReport.overlayMarkersWithoutCoordinates || []),
      ];
    }
  }

  let brandResolution = null;
  if (filters.brand) {
    brandResolution = await resolveBrandAffiliationMatchers(
      filters.brand,
      filters.parentCompany || null
    );
    if (!brandResolution.ok) {
      warnings.push(`BRAND_RESOLUTION: ${brandResolution.error}`);
    }
  }

  const ctx = {
    filters,
    coverage: scopeCoverage,
    metrics: scopeCoverage.metrics,
    filteredCoverage: coverage,
    signals,
    overlays,
    aliasRows,
    savedSignals,
    brandResolution,
    recordsSample: scopeCoverage.recordsSample || [],
    warnings,
  };

  let insights = buildInsightsFromContext(ctx);

  if (filters.insightType) {
    insights = insights.filter((i) => exactMatchKey(i.insightType) === filters.insightType);
  }

  const runCalibration = filters.includeInsightReview || filters.includeSuppressed;
  let calibration = null;
  if (runCalibration) {
    calibration = calibrateInsights(insights, ctx, {
      includeSuppressed: filters.includeSuppressed,
    });
    insights = calibration.insights;
  }

  const rankedOpportunities = buildRankedOpportunities(insights, ctx);

  const m = scopeCoverage.metrics;
  const result = {
    ok: true,
    filters,
    summary: {
      insightsReturned: insights.length,
      market: filters.market || "",
      submarket: filters.submarket || "",
      openHotels: m.openHotels || 0,
      brandedHotels: m.brandedHotels || 0,
      independentHotels: m.independentHotels || 0,
      pipelineHotels: m.pipelineHotels || 0,
      parentCompaniesPresent: m.parentCompanyCount || 0,
      brandsPresent: m.brandCount || 0,
      demandDrivers: overlays.length,
      rankedOpportunities: rankedOpportunities.length,
    },
    insights,
    rankedOpportunities,
    warnings,
    source: {
      hotelSource: "Hotel Census",
      signalSource: "Scout Opportunity Signals",
      overlaySource: "Travel Infrastructure + Demand Anchors",
      readOnly: true,
      writes: false,
      aggregatedAt: new Date().toISOString(),
    },
  };

  if (calibration) {
    result.insightQualitySummary = calibration.insightQualitySummary;
    result.dataQualityNotes = calibration.dataQualityNotes;
    result.suppressedInsightCount = calibration.suppressedInsightCount;
    if (filters.includeInsightReview) {
      result.insightReviews = calibration.insightReviews;
      result.summary = {
        ...result.summary,
        ...calibration.summary,
      };
    }
    if (filters.includeSuppressed) {
      result.suppressedInsights = calibration.suppressedInsights;
    }
  }

  return result;
}

/**
 * Phase 5C — Insight review report (calibration + evidence).
 * @param {Record<string, string|boolean>} [query]
 */
export async function buildInsightReviewReport(query = {}) {
  const reviewQuery = {
    ...query,
    includeDemandOverlays: query.includeDemandOverlays ?? "1",
    includeSavedSignals: query.includeSavedSignals ?? "1",
    includeInsightReview: "1",
    includeSuppressed: query.includeSuppressed === "0" || query.includeSuppressed === "false" ? "0" : "1",
  };

  const report = await buildMarketInsightsReport(reviewQuery);
  if (!report.ok) {
    return { ok: false, error: report.error };
  }

  return {
    ok: true,
    filters: report.filters,
    summary: report.summary,
    insightReviews: report.insightReviews || [],
    suppressedInsights: report.suppressedInsights || [],
    dataQualityNotes: report.dataQualityNotes || [],
    insightQualitySummary: report.insightQualitySummary || null,
    warnings: report.warnings,
    source: report.source,
  };
}
