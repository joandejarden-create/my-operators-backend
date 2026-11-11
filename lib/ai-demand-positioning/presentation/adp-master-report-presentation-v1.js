/**
 * ADP_MASTER_REPORT_PRESENTATION_V1
 *
 * Single canonical ADP report component contract.
 * Structural master reference: Bethesda Marriott.
 *
 * Hotels differ only by certified data / measurement state — never by layout,
 * component selection, or section order.
 */

export const ADP_MASTER_REPORT_PRESENTATION_V1 = "ADP_MASTER_REPORT_PRESENTATION_V1";
export const ADP_MASTER_STRUCTURAL_REFERENCE = "adp_bethesda_marriott";

/**
 * Fixed customer-facing section order (owner-ai-demand.html data-adp-section).
 * Position is 1-indexed and binding for SPA / share / PDF parity.
 */
export const ADP_CANONICAL_SECTIONS_V1 = Object.freeze([
  {
    id: "executive-summary",
    componentId: "adp.section.executive_summary",
    position: 1,
    label: "Executive Summary",
    required: true,
    emptyState: "Executive summary is not available for this period.",
    pdfEquivalent: "executive-summary",
  },
  {
    id: "property-snapshot",
    componentId: "adp.section.property_snapshot",
    position: 2,
    label: "Property Snapshot",
    required: true,
    emptyState: "Headline KPIs unavailable for this period.",
    pdfEquivalent: "property-snapshot",
  },
  {
    id: "ai-demand-positioning-metrics",
    componentId: "adp.section.ai_demand_positioning_metrics",
    position: 3,
    label: "AI Demand Positioning Metrics",
    required: true,
    emptyState: "Metrics unavailable for this period.",
    pdfEquivalent: "ai-demand-positioning-metrics",
  },
  {
    id: "ai-presence-by-demand-territory",
    componentId: "adp.section.demand_territories",
    position: 4,
    label: "AI Presence by Demand Territory",
    required: true,
    emptyState: "Demand territory matrix unavailable.",
    pdfEquivalent: "ai-presence-by-demand-territory",
  },
  {
    id: "trends",
    componentId: "adp.section.trends",
    position: 5,
    label: "Trends",
    required: true,
    emptyState: null, // never collapse — use TRENDS_STATE_* slots
    pdfEquivalent: "trends",
    presentationContract: "ADP_TRENDS_PRESENTATION_V1",
  },
  {
    id: "provider-presence",
    componentId: "adp.section.provider_presence",
    position: 6,
    label: "Provider Presence",
    required: true,
    emptyState: "Provider presence unavailable.",
    pdfEquivalent: "provider-presence",
  },
  {
    id: "brand-portfolio-position",
    componentId: "adp.section.brand_portfolio_position",
    position: 7,
    label: "Brand & Portfolio Position",
    required: true,
    emptyState: null, // never collapse — use BPP presentation slots
    pdfEquivalent: "brand-portfolio-position",
    presentationContract: "BPP_CUSTOMER_PRESENTATION_V2",
  },
  {
    id: "ai-reality-gaps",
    componentId: "adp.section.ai_reality_gaps",
    position: 8,
    label: "AI Reality Gaps",
    required: true,
    emptyState: "Reality gap not measured for this period.",
    pdfEquivalent: "ai-reality-gaps",
  },
  {
    id: "ai-competitive-set",
    componentId: "adp.section.ai_competitive_set",
    position: 9,
    label: "AI Competitive Set",
    required: true,
    emptyState: "Competitive set unavailable.",
    pdfEquivalent: "ai-competitive-set",
  },
  {
    id: "competitive-context-priority-actions",
    componentId: "adp.section.competitive_context_priority_actions",
    position: 10,
    label: "Competitive Context & Priority Actions",
    required: true,
    emptyState: "Priority actions unavailable.",
    pdfEquivalent: "competitive-context-priority-actions",
  },
  {
    id: "evidence-sources-discovery",
    componentId: "adp.section.evidence",
    position: 11,
    label: "Evidence & Sources",
    required: true,
    emptyState: "Evidence unavailable.",
    pdfEquivalent: "evidence-sources-discovery",
  },
]);

export const ADP_CANONICAL_SECTION_IDS_V1 = Object.freeze(
  ADP_CANONICAL_SECTIONS_V1.map((s) => s.id)
);

/** BPP primary KPI contract (Bethesda). */
export const ADP_BPP_PRIMARY_KPI_SLOTS_V1 = Object.freeze([
  { id: "portfolioAiPresence", label: "Portfolio AI Presence", position: 1 },
  { id: "portfolioRank", label: "Portfolio Rank", position: 2 },
  { id: "numberOneAppearance", label: "#1 Appearance", position: 3 },
  { id: "top3Appearance", label: "Top-3 Appearance", position: 4 },
]);

/** Customer BPP must never render these (Bethesda master final). */
export const BPP_CUSTOMER_FORBIDDEN_UI_V1 = Object.freeze([
  "demandTerritorySelector",
  "additionalPortfolioAnalysis",
  "supplementalAccordion",
  "portfolioBenchmarkCard",
  "portfolioPresenceIndexCard",
  "bppProviderPresencePanel",
]);

/** Trends summary KPI contract (Bethesda). */
export const ADP_TRENDS_KPI_SLOTS_V1 = Object.freeze([
  { id: "realityCoverage", label: "Reality Coverage", position: 1 },
  { id: "scenarioPresence", label: "Scenario Presence", position: 2 },
  { id: "considerationRate", label: "Consideration Rate", position: 3 },
  { id: "monitoringOrPeriods", label: "Monitoring", position: 4 },
]);

export const TRENDS_STATE = Object.freeze({
  MULTI_PERIOD: "MULTI_PERIOD",
  BASELINE_ONLY: "BASELINE_ONLY",
  NO_BASELINE: "NO_BASELINE",
  /** Period/monitoring claimed but rates cannot be certified for display. */
  EVIDENCE_BLOCKED: "EVIDENCE_BLOCKED",
});

/**
 * Assert DOM / payload section order matches canonical contract.
 * @param {string[]} sectionIds
 */
export function assertCanonicalSectionOrder(sectionIds = []) {
  const failures = [];
  const expected = ADP_CANONICAL_SECTION_IDS_V1;
  if (sectionIds.length !== expected.length) {
    failures.push(
      `SECTION_COUNT_${sectionIds.length}_EXPECTED_${expected.length}`
    );
  }
  for (let i = 0; i < expected.length; i++) {
    if (sectionIds[i] !== expected[i]) {
      failures.push(
        `SECTION_ORDER_MISMATCH_AT_${i + 1}:got=${sectionIds[i] || "missing"}:expected=${expected[i]}`
      );
    }
  }
  return { pass: failures.length === 0, failures, expected, actual: sectionIds };
}

/**
 * Certify a payload against master presentation (structure, not hotel values).
 */
export function certifyMasterReportPresentation(payload, { sectionIdsFromDom = null } = {}) {
  const failures = [];
  if (!payload || typeof payload !== "object") {
    return { pass: false, failures: ["NO_PAYLOAD"], contract: ADP_MASTER_REPORT_PRESENTATION_V1 };
  }
  if (
    payload.masterPresentationContract &&
    payload.masterPresentationContract !== ADP_MASTER_REPORT_PRESENTATION_V1
  ) {
    failures.push("WRONG_MASTER_PRESENTATION_CONTRACT");
  }
  if (sectionIdsFromDom) {
    const ord = assertCanonicalSectionOrder(sectionIdsFromDom);
    failures.push(...ord.failures);
  }
  const trendsVm = payload.canonicalViewModel?.trends || payload.trendsPresentation;
  if (trendsVm && !Object.values(TRENDS_STATE).includes(trendsVm.state)) {
    failures.push(`INVALID_TRENDS_STATE:${trendsVm.state}`);
  }
  if (trendsVm && (!Array.isArray(trendsVm.kpiSlots) || trendsVm.kpiSlots.length !== 4)) {
    failures.push("TRENDS_KPI_SLOTS_NOT_FOUR");
  }
  const bpp = payload.brandPortfolioPosition;
  if (bpp?.presentationLayout?.primaryKpiCountTarget !== 4 && bpp?.status === "READY") {
    // READY packs must target Bethesda four-card primary row
    if (bpp.presentationContractVersion !== "BPP_CUSTOMER_PRESENTATION_V2") {
      failures.push("BPP_MISSING_PRESENTATION_V2");
    }
  }
  return {
    pass: failures.length === 0,
    failures,
    contract: ADP_MASTER_REPORT_PRESENTATION_V1,
    structuralReference: ADP_MASTER_STRUCTURAL_REFERENCE,
  };
}
