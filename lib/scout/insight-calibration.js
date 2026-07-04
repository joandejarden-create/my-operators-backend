/**
 * Scout Phase 5C — Insight calibration, evidence review, and quality classification.
 * Read-only; no Airtable writes.
 */

import { exactMatchKey } from "../hotel-census/brand-alias-resolve.js";

export const INSIGHT_QUALITY_LEVELS = ["Strong", "Directional", "Weak", "Suppressed"];

export const SUGGESTED_REVIEW_ACTIONS = ["Review", "Save", "Watch", "Dismiss"];

/** Review questions by insight type (commercial diligence prompts). */
export const INSIGHT_REVIEW_QUESTIONS = {
  parent_company_underrepresentation: [
    "Is this parent company actively targeting this geography?",
    "Is the market already saturated in the relevant chain scale?",
    "Are there independent assets that could support conversion?",
    "Is the parent absent because of brand strategy, owner economics, or lack of suitable assets?",
  ],
  brand_underrepresentation: [
    "Is the brand format appropriate for this market?",
    "Are comparable brands already present?",
    "Is the parent company present through other brands?",
    "Is the gap meaningful or intentional?",
  ],
  chain_scale_gap: [
    "Is the missing chain scale actually unmet demand or a data/reporting gap?",
    "Do comp sets in adjacent submarkets show the segment?",
    "Would a new entrant face owner or lender resistance at this scale?",
  ],
  independent_conversion_potential: [
    "Are independent hotels large enough for brand conversion?",
    "Are they in relevant locations?",
    "Are there demand anchors nearby?",
    "Are there visible quality or ownership signals available?",
  ],
  operator_white_space: [
    "Is management company coverage fragmented?",
    "Are independent hotels professionally managed?",
    "Is the opportunity operator-led, brand-led, or owner-led?",
  ],
  pipeline_momentum: [
    "Is pipeline supply concentrated in one submarket or chain scale?",
    "Does pipeline timing overlap with white-space or conversion opportunities?",
    "Are pipeline hotels affiliated or independent?",
  ],
  all_inclusive_potential: [
    "Is the market truly resort/leisure driven?",
    "Is there enough land or asset configuration for all-inclusive use?",
    "Are existing assets suitable or would this be development-led?",
  ],
  branded_residential_potential: [
    "Are there luxury/upper-upscale anchors?",
    "Is there mixed-use or residential-tourism context?",
    "Is this a resort or urban residential market?",
  ],
  demand_driver_supported_opportunity: [
    "Do demand drivers materially overlap with the supply gap geography?",
    "Are anchors seasonal or year-round?",
    "Does infrastructure support the proposed chain scale or format?",
  ],
};

const COMMERCIAL_INTERPRETATION = {
  parent_company_underrepresentation:
    "May indicate franchisor or brand-led white space where owners seek a recognizable flag — validate owner appetite and chain-scale fit before outreach.",
  brand_underrepresentation:
    "May indicate a format or positioning gap if comparable brands operate nearby — confirm whether absence reflects strategy or asset availability.",
  chain_scale_gap:
    "May suggest an underserved price/positioning tier — confirm with STR comp sets and owner economics, not census counts alone.",
  independent_conversion_potential:
    "May indicate franchise, soft-brand, or management conversion conversations — requires asset-level diligence on rooms, condition, and ownership.",
  operator_white_space:
    "May indicate third-party management or franchise assembly opportunities — validate management fragmentation and owner sophistication.",
  pipeline_momentum:
    "May shift competitive timing for brand or operator conversations — cross-reference pipeline affiliations with gap insights.",
  all_inclusive_potential:
    "May warrant resort-format exploration in leisure markets — census cannot confirm all-inclusive feasibility or operator capability.",
  branded_residential_potential:
    "May support separate branded residential diligence where upscale lodging and mixed-use demand coincide — not a hotel census conclusion.",
  demand_driver_supported_opportunity:
    "May prioritize markets where supply gaps align with visible demand infrastructure — still requires local validation of capture rates.",
};

function evidenceItem({ evidenceType, label, value, sourceTable, recordId, confidence }) {
  return {
    evidenceType,
    label,
    value: value == null ? "" : String(value),
    sourceTable: sourceTable || "",
    recordId: recordId || "",
    confidence: confidence || "Medium",
  };
}

function countDataGaps(gaps) {
  return (gaps || []).length;
}

function geographyCompleteness(filters) {
  const hasMarket = Boolean(filters.market);
  const hasSubmarket = Boolean(filters.submarket);
  const hasCountry = Boolean(filters.country);
  if (hasSubmarket && hasMarket && hasCountry) return "high";
  if (hasMarket && hasCountry) return "medium";
  if (hasCountry) return "low";
  return "minimal";
}

function assessGeographyDataGaps(filters) {
  const gaps = [];
  if (!filters.market) {
    gaps.push({
      gapType: "geography",
      label: "STR Market not specified",
      detail: "Insights are broader without an official STR Market filter — submarket precision may be limited.",
    });
  }
  if (!filters.submarket && filters.market) {
    gaps.push({
      gapType: "geography",
      label: "STR Submarket not specified",
      detail: "Submarket-level supply mix may be masked at market level.",
    });
  }
  if (!filters.country) {
    gaps.push({
      gapType: "geography",
      label: "Country not specified",
      detail: "Cross-country aggregation may dilute local competitive context.",
    });
  }
  return gaps;
}

function buildDataQualityNotes(filters, metrics, overlays, warnings) {
  const notes = [];
  const geo = geographyCompleteness(filters);
  if (geo === "low" || geo === "minimal") {
    notes.push("Geography filters are broad — calibrate confidence downward for submarket-specific claims.");
  }
  if ((metrics.openHotels || 0) < 5) {
    notes.push("Fewer than 5 open hotels in scope — insight quality may be Weak or Suppressed.");
  }
  if ((metrics.brandedHotels || 0) < 3 && filters.parentCompany) {
    notes.push("Fewer than 3 branded hotels — parent/brand gap insights may be suppressed or Low confidence.");
  }
  if (!overlays?.length) {
    notes.push("No demand overlays in scope — demand-supported confidence cannot increase.");
  }
  if ((warnings || []).some((w) => /OVERLAYS|DEMAND_ANCHORS/i.test(w))) {
    notes.push("Demand overlay warnings present — review overlay availability before relying on demand-linked insights.");
  }
  return notes;
}

function pickHotelExamples(recordsSample, insight, limit = 3) {
  const rows = recordsSample || [];
  if (!rows.length) return [];

  const type = insight.insightType;
  let pool = rows;

  if (type === "independent_conversion_potential") {
    pool = rows.filter((r) => /independent/i.test(r.affiliation || ""));
  } else if (type === "pipeline_momentum") {
    pool = rows.filter((r) => /pipeline/i.test(r.status || ""));
  } else if (type === "parent_company_underrepresentation" || type === "brand_underrepresentation") {
    pool = rows.filter((r) => r.parentCompany || r.affiliation);
  }

  return pool.slice(0, limit).map((r) => ({
    recordId: r.id,
    hotelName: r.name,
    affiliation: r.affiliation,
    parentCompany: r.parentCompany,
    status: r.status,
    rooms: r.rooms,
    market: r.market,
    submarket: r.submarket,
    chainScale: r.chainScale,
  }));
}

function pickSignalExamples(signals, insight, limit = 3) {
  const ids = new Set(insight.relatedSignalIds || []);
  return (signals || [])
    .filter((s) => ids.has(s.signalId))
    .slice(0, limit)
    .map((s) => ({
      signalId: s.signalId,
      signalType: s.signalType,
      title: s.title,
      reason: s.reason,
      reviewStatus: s.reviewStatus || s.savedReviewStatus || null,
    }));
}

function pickDemandDriverExamples(overlays, insight, limit = 4) {
  const related = insight.relatedDemandDrivers || [];
  const relatedIds = new Set(related.map((d) => d.overlayId).filter(Boolean));
  const pool = related.length
    ? related
    : (overlays || []).slice(0, limit).map((o) => ({
        overlayId: o.overlayId,
        name: o.name || o.popupTitle,
        category: o.category,
        overlayType: o.overlayType,
      }));

  return pool.slice(0, limit).map((d) => ({
    overlayId: d.overlayId || "",
    name: d.name,
    category: d.category,
    overlayType: d.overlayType,
  }));
}

function buildEvidenceItems(insight, ctx) {
  const { metrics, filters, signals, overlays, recordsSample } = ctx;
  const items = [];
  const sm = insight.supportingMetrics || {};

  items.push(
    evidenceItem({
      evidenceType: "census_metric",
      label: "Open hotels in scope",
      value: metrics.openHotels ?? sm.totalOpenHotels ?? "",
      sourceTable: "Hotel Census",
      confidence: (metrics.openHotels || 0) >= 10 ? "High" : (metrics.openHotels || 0) >= 3 ? "Medium" : "Low",
    })
  );

  if (sm.brandedOpenHotels != null || metrics.brandedHotels != null) {
    items.push(
      evidenceItem({
        evidenceType: "census_metric",
        label: "Branded open hotels",
        value: sm.brandedOpenHotels ?? metrics.brandedHotels,
        sourceTable: "Hotel Census",
        confidence: (sm.brandedOpenHotels ?? metrics.brandedHotels ?? 0) >= 10 ? "High" : "Medium",
      })
    );
  }

  if (sm.independentOpenHotels != null || metrics.independentHotels != null) {
    items.push(
      evidenceItem({
        evidenceType: "census_metric",
        label: "Independent open hotels",
        value: sm.independentOpenHotels ?? metrics.independentHotels,
        sourceTable: "Hotel Census",
        confidence: "Medium",
      })
    );
  }

  if (sm.selectedParentOpenHotels != null) {
    items.push(
      evidenceItem({
        evidenceType: "census_metric",
        label: "Selected parent open hotels",
        value: sm.selectedParentOpenHotels,
        sourceTable: "Hotel Census",
        confidence: "High",
      })
    );
  }

  if (sm.selectedBrandOpenHotels != null) {
    items.push(
      evidenceItem({
        evidenceType: "census_metric",
        label: "Selected brand open hotels",
        value: sm.selectedBrandOpenHotels,
        sourceTable: "Hotel Census",
        confidence: "High",
      })
    );
  }

  if (sm.pipelineHotels != null || metrics.pipelineHotels != null) {
    items.push(
      evidenceItem({
        evidenceType: "census_metric",
        label: "Pipeline hotels",
        value: sm.pipelineHotels ?? metrics.pipelineHotels,
        sourceTable: "Hotel Census",
        confidence: "High",
      })
    );
  }

  for (const sig of pickSignalExamples(signals, insight, 2)) {
    items.push(
      evidenceItem({
        evidenceType: "signal",
        label: sig.title,
        value: sig.reason,
        sourceTable: "Scout Opportunity Signals",
        recordId: sig.signalId,
        confidence: sig.reviewStatus === "Watchlist" ? "High" : "Medium",
      })
    );
  }

  for (const d of pickDemandDriverExamples(overlays, insight, 3)) {
    items.push(
      evidenceItem({
        evidenceType: "demand_overlay",
        label: d.name,
        value: d.category,
        sourceTable: d.overlayType === "travel_infrastructure" ? "Travel Infrastructure Data" : "Demand Anchors",
        recordId: d.overlayId,
        confidence: "Medium",
      })
    );
  }

  for (const h of pickHotelExamples(recordsSample, insight, 2)) {
    items.push(
      evidenceItem({
        evidenceType: "hotel_example",
        label: h.hotelName,
        value: [h.affiliation, h.chainScale, h.rooms ? `${h.rooms} rooms` : ""].filter(Boolean).join(" · "),
        sourceTable: "Hotel Census",
        recordId: h.recordId,
        confidence: "High",
      })
    );
  }

  return items;
}

function buildInsightDataGaps(insight, ctx) {
  const gaps = [...assessGeographyDataGaps(ctx.filters)];
  const { metrics, overlays, signals } = ctx;
  const sm = insight.supportingMetrics || {};

  if ((metrics.openHotels || 0) < 5) {
    gaps.push({
      gapType: "supply_depth",
      label: "Thin hotel sample",
      detail: `Only ${metrics.openHotels || 0} open hotel(s) in scope.`,
    });
  }

  if (
    ["parent_company_underrepresentation", "brand_underrepresentation", "demand_driver_supported_opportunity"].includes(
      insight.insightType
    ) &&
    !(insight.relatedSignalIds || []).length
  ) {
    gaps.push({
      gapType: "signals",
      label: "No linked Scout signals",
      detail: "Gap insight is census-only without a corroborating generated signal.",
    });
  }

  if (
    ["demand_driver_supported_opportunity", "all_inclusive_potential", "independent_conversion_potential"].includes(
      insight.insightType
    ) &&
    !(overlays || []).length
  ) {
    gaps.push({
      gapType: "demand_overlays",
      label: "No demand overlays in scope",
      detail: "Demand-supported interpretation is limited without travel infrastructure or demand anchors.",
    });
  }

  if (insight.insightType === "parent_company_underrepresentation" && (sm.brandedOpenHotels ?? 0) < 10) {
    gaps.push({
      gapType: "branded_depth",
      label: "Limited branded supply",
      detail: "Fewer than 10 branded hotels — parent gap may be less commercially meaningful.",
    });
  }

  if (insight.insightType === "chain_scale_gap" && !(sm.demandDriversPresent > 0)) {
    gaps.push({
      gapType: "demand_context",
      label: "Chain scale gap without demand drivers",
      detail: "Absent chain scale may reflect reporting or market structure, not unmet demand.",
    });
  }

  return gaps;
}

function classifyInsightQuality(insight, ctx) {
  const { metrics, filters, overlays, signals } = ctx;
  const sm = insight.supportingMetrics || {};
  const branded = sm.brandedOpenHotels ?? metrics.brandedHotels ?? 0;
  const open = metrics.openHotels ?? sm.totalOpenHotels ?? 0;
  const drivers = (insight.relatedDemandDrivers || []).length;
  const overlayCount = (overlays || []).length;
  const signalCount = (insight.relatedSignalIds || []).length;
  const geo = geographyCompleteness(filters);
  const suppressedReasons = [];

  const type = insight.insightType;

  if (type === "parent_company_underrepresentation") {
    const parentOpen = sm.selectedParentOpenHotels ?? 0;
    if (filters.parentCompany && branded < 3) {
      suppressedReasons.push(
        "Selected parent has 0 open hotels but fewer than 3 branded hotels in scope — insufficient competitive context."
      );
      return { quality: "Suppressed", suppressedReasons };
    }
    if (parentOpen === 0 && branded >= 10 && (drivers > 0 || signalCount > 0 || overlayCount > 0)) {
      return { quality: "Strong", suppressedReasons: [] };
    }
    if (parentOpen === 0 && branded >= 3) {
      return { quality: overlayCount > 0 || signalCount > 0 ? "Directional" : "Weak", suppressedReasons: [] };
    }
    if (parentOpen > 0 && branded >= 10) {
      return { quality: signalCount > 0 ? "Directional" : "Weak", suppressedReasons: [] };
    }
  }

  if (type === "brand_underrepresentation") {
    const brandOpen = sm.selectedBrandOpenHotels ?? 0;
    if (brandOpen === 0 && branded < 3) {
      suppressedReasons.push("Brand absent but fewer than 3 branded hotels — gap may be noise.");
      return { quality: "Suppressed", suppressedReasons };
    }
    if (brandOpen === 0 && branded >= 10 && (signalCount > 0 || drivers > 0)) {
      return { quality: "Strong", suppressedReasons: [] };
    }
    if (brandOpen === 0 && branded >= 3) {
      return { quality: "Directional", suppressedReasons: [] };
    }
  }

  if (type === "chain_scale_gap") {
    if (open < 10) {
      suppressedReasons.push("Total open hotels below 10 — chain scale absence may not be meaningful.");
      return { quality: "Suppressed", suppressedReasons };
    }
    if (drivers > 0) return { quality: "Directional", suppressedReasons: [] };
    return { quality: "Weak", suppressedReasons: [] };
  }

  if (type === "independent_conversion_potential") {
    const ind = sm.independentOpenHotels ?? metrics.independentHotels ?? 0;
    if (ind < 5) {
      suppressedReasons.push("Fewer than 5 independent hotels — conversion cluster threshold not met.");
      return { quality: "Suppressed", suppressedReasons };
    }
    if (ind >= 15 && (drivers > 0 || signalCount > 0)) return { quality: "Strong", suppressedReasons: [] };
    if (ind >= 8) return { quality: "Directional", suppressedReasons: [] };
    return { quality: "Weak", suppressedReasons: [] };
  }

  if (type === "operator_white_space") {
    if (signalCount > 0) return { quality: "Strong", suppressedReasons: [] };
    if ((sm.independentOpenHotels ?? 0) >= 8) return { quality: "Directional", suppressedReasons: [] };
    return { quality: "Weak", suppressedReasons: [] };
  }

  if (type === "pipeline_momentum") {
    if ((sm.pipelineHotels ?? 0) >= 3) return { quality: "Directional", suppressedReasons: [] };
    return { quality: "Weak", suppressedReasons: [] };
  }

  if (type === "all_inclusive_potential" || type === "branded_residential_potential") {
    if (geo === "minimal") {
      suppressedReasons.push("Geography too broad for format-specific potential insight.");
      return { quality: "Suppressed", suppressedReasons };
    }
    return { quality: drivers > 0 ? "Directional" : "Weak", suppressedReasons: [] };
  }

  if (type === "demand_driver_supported_opportunity") {
    if (drivers > 0 && signalCount > 0) return { quality: "Strong", suppressedReasons: [] };
    if (drivers > 0) return { quality: "Directional", suppressedReasons: [] };
    return { quality: "Weak", suppressedReasons: [] };
  }

  if (insight.confidence === "High" && insight.priority === "High") {
    return { quality: "Strong", suppressedReasons: [] };
  }
  if (insight.confidence === "Low" && open < 5) {
    suppressedReasons.push("Low confidence with thin supply base.");
    return { quality: "Suppressed", suppressedReasons };
  }
  if (insight.priority === "High") return { quality: "Directional", suppressedReasons: [] };
  return { quality: "Weak", suppressedReasons: [] };
}

function buildConfidenceExplanation(insight, quality, ctx) {
  const { metrics, overlays } = ctx;
  const parts = [];
  parts.push(`Classified as ${quality} based on census depth, overlay availability, and signal corroboration.`);
  parts.push(`Model confidence: ${insight.confidence}; priority: ${insight.priority}.`);
  if ((metrics.openHotels || 0) >= 10) parts.push("Adequate open-hotel sample in scope.");
  else parts.push("Limited open-hotel sample may constrain confidence.");
  if ((overlays || []).length > 0) parts.push("Demand overlays are available and may support interpretation.");
  else parts.push("No demand overlays loaded — interpretation relies primarily on census.");
  if ((insight.relatedSignalIds || []).length > 0) {
    parts.push(`${insight.relatedSignalIds.length} related Scout signal(s) corroborate the pattern.`);
  }
  if (quality === "Suppressed") {
    parts.push("Confidence is not justified for surfacing — see suppressedReasons.");
  }
  return parts.join(" ");
}

function buildPriorityExplanation(insight, quality) {
  if (quality === "Suppressed") {
    return "Suppressed from active review — priority ranking withheld until data depth improves.";
  }
  if (insight.priority === "High" && quality === "Strong") {
    return "High priority aligns with strong evidence mix (census depth + overlays and/or signals).";
  }
  if (insight.priority === "High") {
    return "High model priority — validate with local comp set before outreach.";
  }
  if (quality === "Weak") {
    return "Lower effective priority — treat as directional until additional evidence is gathered.";
  }
  return `${insight.priority} priority with ${quality.toLowerCase()} calibrated quality.`;
}

function buildEvidenceSummary(insight, evidenceItems, dataGaps) {
  const metrics = evidenceItems.filter((e) => e.evidenceType === "census_metric").length;
  const signals = evidenceItems.filter((e) => e.evidenceType === "signal").length;
  const overlays = evidenceItems.filter((e) => e.evidenceType === "demand_overlay").length;
  const hotels = evidenceItems.filter((e) => e.evidenceType === "hotel_example").length;
  const parts = [
    `${metrics} census metric(s)`,
    signals ? `${signals} Scout signal(s)` : null,
    overlays ? `${overlays} demand driver(s)` : null,
    hotels ? `${hotels} hotel example(s)` : null,
  ].filter(Boolean);
  const gapNote = dataGaps.length ? ` ${dataGaps.length} data gap(s) noted.` : "";
  return `Supported by ${parts.join(", ")}.${gapNote}`;
}

function suggestReviewAction(insight, quality) {
  if (quality === "Suppressed") return "Dismiss";
  if (quality === "Weak") return "Watch";
  if ((insight.relatedSignalIds || []).length > 0 && quality === "Strong") return "Save";
  if (quality === "Strong") return "Review";
  if (quality === "Directional") return "Review";
  return "Watch";
}

/**
 * Detect insights considered but not generated at source.
 */
export function detectSuppressedCandidates(ctx) {
  const { filters, metrics } = ctx;
  const candidates = [];
  const branded = metrics.brandedHotels || 0;

  if (filters.parentCompany && branded === 0) {
    candidates.push({
      insightId: `suppressed-parent-${exactMatchKey(filters.parentCompany)}`,
      insightType: "parent_company_underrepresentation",
      title: `${filters.parentCompany} gap not evaluated`,
      insightQuality: "Suppressed",
      suppressedReasons: [
        "No branded open hotels in scope — parent underrepresentation cannot be assessed meaningfully.",
      ],
      insightText: "Insight withheld: branded competitive set is empty in selected geography.",
      evidenceSummary: "No census evidence for parent share gap.",
      evidenceItems: [],
      dataGaps: [
        {
          gapType: "branded_supply",
          label: "No branded hotels",
          detail: "Parent/brand gap logic requires at least one branded open hotel.",
        },
      ],
      suggestedReviewAction: "Dismiss",
    });
  }

  if (filters.brand && branded === 0) {
    candidates.push({
      insightId: `suppressed-brand-${exactMatchKey(filters.brand)}`,
      insightType: "brand_underrepresentation",
      title: `${filters.brand} gap not evaluated`,
      insightQuality: "Suppressed",
      suppressedReasons: ["No branded open hotels in scope — brand gap cannot be benchmarked."],
      insightText: "Insight withheld: no branded comp set in selected geography.",
      evidenceSummary: "No branded census baseline.",
      evidenceItems: [],
      dataGaps: [
        {
          gapType: "branded_supply",
          label: "No branded hotels",
          detail: "Brand underrepresentation requires branded supply for comparison.",
        },
      ],
      suggestedReviewAction: "Dismiss",
    });
  }

  return candidates;
}

/**
 * Calibrate a single insight with evidence and review metadata.
 */
export function calibrateInsight(insight, ctx) {
  const { quality, suppressedReasons } = classifyInsightQuality(insight, ctx);
  const dataGaps = buildInsightDataGaps(insight, ctx);
  const evidenceItems = buildEvidenceItems(insight, ctx);
  const evidenceSummary = buildEvidenceSummary(insight, evidenceItems, dataGaps);

  return {
    ...insight,
    insightQuality: quality,
    evidenceSummary,
    evidenceItems,
    dataGaps,
    confidenceExplanation: buildConfidenceExplanation(insight, quality, ctx),
    priorityExplanation: buildPriorityExplanation(insight, quality),
    commercialInterpretation:
      COMMERCIAL_INTERPRETATION[insight.insightType] ||
      "May warrant commercial review with local market context — not a recommendation.",
    suggestedReviewQuestions: INSIGHT_REVIEW_QUESTIONS[insight.insightType] || [],
    relatedHotelExamples: pickHotelExamples(ctx.recordsSample, insight, 4),
    relatedDemandDriverExamples: pickDemandDriverExamples(ctx.overlays, insight, 5),
    relatedSignalExamples: pickSignalExamples(ctx.signals, insight, 5),
    suggestedReviewAction: suggestReviewAction(insight, quality),
    suppressedReasons: quality === "Suppressed" ? suppressedReasons : [],
  };
}

/**
 * Calibrate all insights; partition suppressed unless includeSuppressed.
 */
export function calibrateInsights(insights, ctx, options = {}) {
  const includeSuppressed = options.includeSuppressed !== false;
  const active = [];
  const suppressed = [];

  for (const ins of insights || []) {
    const calibrated = calibrateInsight(ins, ctx);
    if (calibrated.insightQuality === "Suppressed") {
      suppressed.push(calibrated);
    } else {
      active.push(calibrated);
    }
  }

  const extraSuppressed = detectSuppressedCandidates(ctx);
  for (const s of extraSuppressed) {
    suppressed.push(s);
  }

  const allReviewed = includeSuppressed ? [...active, ...suppressed] : active;

  const summary = {
    insightsReviewed: allReviewed.length,
    strongInsights: active.filter((i) => i.insightQuality === "Strong").length,
    directionalInsights: active.filter((i) => i.insightQuality === "Directional").length,
    weakInsights: active.filter((i) => i.insightQuality === "Weak").length,
    suppressedInsights: suppressed.length,
    dataGaps: allReviewed.reduce((n, i) => n + countDataGaps(i.dataGaps), 0),
  };

  const insightQualitySummary = {
    strong: summary.strongInsights,
    directional: summary.directionalInsights,
    weak: summary.weakInsights,
    suppressed: summary.suppressedInsights,
  };

  const dataQualityNotes = buildDataQualityNotes(ctx.filters, ctx.metrics, ctx.overlays, ctx.warnings);

  return {
    insights: active,
    suppressedInsights: includeSuppressed ? suppressed : [],
    insightReviews: allReviewed,
    summary,
    insightQualitySummary,
    dataQualityNotes,
    suppressedInsightCount: suppressed.length,
  };
}
