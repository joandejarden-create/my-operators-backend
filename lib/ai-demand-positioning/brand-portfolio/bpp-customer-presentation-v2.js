/**
 * BPP_CUSTOMER_PRESENTATION_V2
 *
 * Canonical customer-facing Brand & Portfolio Position layout (Bethesda reference).
 * Normalizes all supported BPP pack versions into one view model.
 *
 * Customer structure (binding):
 *   Header + lens → four primary KPIs → three evidence links →
 *   Portfolio Competitive Overview → END
 *
 * Forbidden in customer UI (do not render, even collapsed/hidden):
 *   - BPP Demand Territory selector / Overall dropdown
 *   - Additional Portfolio Analysis accordion
 *   - Supplemental Benchmark / Presence Index cards
 *   - Portfolio Read narrative (BPP section)
 *   - BPP-specific Provider Presence panel
 *
 * Does NOT change certified measurement values — presentation / slotting only.
 * Does NOT invent zeros for missing measured values.
 * Underlying secondary research remains in secondaryModules / kpisOriginal for
 * internal preservation — never customer-painted by the SPA.
 */

import { attachBppMethodologyDisclosure } from "./bpp-measurement-comparability-v1.js";

export const BPP_CUSTOMER_PRESENTATION_V2 = "BPP_CUSTOMER_PRESENTATION_V2";

export const BPP_PRESENTATION_SUBTITLE_V2 =
  "How this hotel performs within its brand, collection, portfolio, or loyalty ecosystem.";

/** Mandatory primary KPI order (Bethesda format). */
export const BPP_PRIMARY_KPI_IDS_V2 = Object.freeze([
  "portfolioAiPresence",
  "portfolioRank",
  "numberOneAppearance",
  "top3Appearance",
]);

/**
 * Secondary research KPI ids — retained in data stores, never customer-primary,
 * never painted under Additional Portfolio Analysis (component removed).
 */
export const BPP_SECONDARY_KPI_IDS_V2 = Object.freeze([
  "portfolioBenchmark",
  "portfolioPresenceIndex",
]);

export const BPP_PRIMARY_KPI_LABELS_V2 = Object.freeze({
  portfolioAiPresence: "Portfolio AI Presence",
  portfolioRank: "Portfolio Rank",
  numberOneAppearance: "#1 Appearance",
  top3Appearance: "Top-3 Appearance",
});

/** Explicit customer-facing prohibitions (Full E2E + DOM gates). */
export const BPP_CUSTOMER_FORBIDDEN_COMPONENTS_V2 = Object.freeze([
  "demandTerritorySelector",
  "overallDropdownInBpp",
  "additionalPortfolioAnalysis",
  "supplementalAccordion",
  "portfolioBenchmarkCard",
  "portfolioPresenceIndexCard",
  "portfolioReadNarrative",
  "bppProviderPresencePanel",
  "portfolioMeasurementCompleteStatusPanel",
  "peerRankingPendingStatusPanel",
]);

export const BPP_CUSTOMER_COMPONENT_ORDER_V2 = Object.freeze([
  "header",
  "lens",
  "primaryKpis",
  "evidenceLinks",
  "portfolioCompetitiveOverview",
]);

function kpiUsable(k) {
  if (!k) return false;
  // Rank-blocked placeholder stays in the Bethesda four-card shell.
  if (
    k.id === "portfolioRank" &&
    (k.blockedReason || k.available === false) &&
    (k.value === "—" || k.value === "\u2014" || k.value == null || k.value === "")
  ) {
    return true;
  }
  return (
    k.available !== false &&
    k.value != null &&
    k.value !== "" &&
    String(k.value).toLowerCase() !== "null"
  );
}

/**
 * Split pack KPIs into primary (canonical 4) vs secondary (benchmark/index).
 * Preserves original values and methodology metadata.
 */
export function splitBppKpisForPresentation(kpis = []) {
  const byId = new Map();
  for (const k of kpis || []) {
    if (k?.id) byId.set(k.id, k);
  }
  const primaryKpis = [];
  for (const id of BPP_PRIMARY_KPI_IDS_V2) {
    const k = byId.get(id);
    if (kpiUsable(k)) {
      primaryKpis.push({
        ...k,
        label: BPP_PRIMARY_KPI_LABELS_V2[id] || k.label,
        presentationSlot: "PRIMARY",
      });
    }
  }
  const secondaryKpis = [];
  for (const id of BPP_SECONDARY_KPI_IDS_V2) {
    const k = byId.get(id);
    if (kpiUsable(k)) {
      secondaryKpis.push({
        ...k,
        presentationSlot: "SECONDARY_INTERNAL_ONLY",
      });
    }
  }
  const unknown = (kpis || []).filter(
    (k) =>
      k?.id &&
      !BPP_PRIMARY_KPI_IDS_V2.includes(k.id) &&
      !BPP_SECONDARY_KPI_IDS_V2.includes(k.id) &&
      kpiUsable(k)
  );
  return { primaryKpis, secondaryKpis, unknownKpis: unknown, byId };
}

/**
 * Apply canonical presentation contract to any BPP payload (published, local, derived).
 *
 * @param {object|null} payload
 * @returns {object|null}
 */
export function applyBppCustomerPresentationV2(payload) {
  if (!payload || typeof payload !== "object") return payload;

  const originalKpis = Array.isArray(payload.kpis) ? payload.kpis : [];
  const { primaryKpis, secondaryKpis, unknownKpis } =
    splitBppKpisForPresentation(originalKpis);

  const peerCount =
    payload.peerSet?.peerCount ??
    payload.ranking?.denominator ??
    (Array.isArray(payload.ranking?.rows)
      ? Math.max(0, payload.ranking.rows.length)
      : null);

  const presented = {
    ...payload,
    presentationContractVersion: BPP_CUSTOMER_PRESENTATION_V2,
    title: payload.title || "Brand & Portfolio Position",
    subtitle: BPP_PRESENTATION_SUBTITLE_V2,
    // Primary row — Bethesda four-card contract (only measured/available cards)
    kpis: primaryKpis,
    kpisOriginal: originalKpis,
    // Retained for internal / historical preservation — SPA must not paint these
    secondaryKpis,
    secondaryModules: {
      portfolioBenchmarkIndex: secondaryKpis,
      portfolioRead: payload.narrative?.body ? payload.narrative : null,
      providerPresence:
        payload.providerPresence?.rows?.length > 0
          ? payload.providerPresence
          : null,
      territoryOptions:
        Array.isArray(payload.territoryOptions) &&
        payload.territoryOptions.length > 1
          ? payload.territoryOptions
          : [],
      benchmarkLimitation: payload.benchmarkLimitation || null,
      unknownKpis,
      customerFacing: false,
    },
    // Strip customer territory filter surface — keep raw on secondaryModules only
    territoryOptions: undefined,
    presentationLayout: {
      version: BPP_CUSTOMER_PRESENTATION_V2,
      primaryKpiOrder: [...BPP_PRIMARY_KPI_IDS_V2],
      primaryKpiCountTarget: 4,
      primaryKpiCountRendered: primaryKpis.length,
      componentOrder: [...BPP_CUSTOMER_COMPONENT_ORDER_V2],
      customerFacingEndsAfter: "portfolioCompetitiveOverview",
      secondaryBelowOverview: false,
      customerRenderSupplemental: false,
      customerRenderTerritoryFilter: false,
      forbidDemandTerritorySelector: true,
      forbidAdditionalPortfolioAnalysis: true,
      forbiddenComponents: [...BPP_CUSTOMER_FORBIDDEN_COMPONENTS_V2],
      hasSecondaryAnalysis: false,
      supplementalCollapsedByDefault: false,
      supplementalDisclosureLabel: null,
      evidenceOrder: ["positive", "missing", "displacement"],
      rankingColumns: [
        "rank",
        "hotel",
        "scenarioAppearance",
        "deltaVsPrior",
        "displacementVsYou",
        "scenariosShared",
      ],
      peerCount,
      referenceFormat: "BETHESDA_MARRIOTT",
    },
  };
  // Measurement protocol disclosure (attach-at-read) — does not change KPI values.
  return attachBppMethodologyDisclosure(presented);
}

/**
 * Assert presentation invariants for tests / certification.
 */
export function assertBppPresentationV2(payload) {
  const failures = [];
  if (!payload) {
    return { pass: false, failures: ["NO_PAYLOAD"] };
  }
  if (payload.presentationContractVersion !== BPP_CUSTOMER_PRESENTATION_V2) {
    failures.push("MISSING_PRESENTATION_CONTRACT_VERSION");
  }
  if (payload.subtitle !== BPP_PRESENTATION_SUBTITLE_V2) {
    failures.push("SUBTITLE_MISMATCH");
  }
  const primaryIds = (payload.kpis || []).map((k) => k.id);
  for (const id of primaryIds) {
    if (BPP_SECONDARY_KPI_IDS_V2.includes(id)) {
      failures.push(`SECONDARY_KPI_IN_PRIMARY_ROW:${id}`);
    }
  }
  for (let i = 0; i < primaryIds.length; i++) {
    const expectedOrder = BPP_PRIMARY_KPI_IDS_V2.filter((id) =>
      primaryIds.includes(id)
    );
    if (primaryIds[i] !== expectedOrder[i]) {
      failures.push("PRIMARY_KPI_ORDER_VIOLATION");
      break;
    }
  }
  if ((payload.kpis || []).length > 4) {
    failures.push("PRIMARY_KPI_COUNT_EXCEEDS_4");
  }
  const layout = payload.presentationLayout || {};
  if (layout.customerRenderSupplemental === true) {
    failures.push("CUSTOMER_SUPPLEMENTAL_MUST_BE_FALSE");
  }
  if (layout.customerRenderTerritoryFilter === true) {
    failures.push("CUSTOMER_TERRITORY_FILTER_MUST_BE_FALSE");
  }
  if (layout.hasSecondaryAnalysis === true) {
    failures.push("HAS_SECONDARY_ANALYSIS_MUST_BE_FALSE_FOR_CUSTOMER");
  }
  if (layout.supplementalDisclosureLabel) {
    failures.push("SUPPLEMENTAL_DISCLOSURE_LABEL_FORBIDDEN");
  }
  if (layout.forbidDemandTerritorySelector !== true) {
    failures.push("MISSING_FORBID_DEMAND_TERRITORY");
  }
  if (layout.forbidAdditionalPortfolioAnalysis !== true) {
    failures.push("MISSING_FORBID_ADDITIONAL_PORTFOLIO_ANALYSIS");
  }
  if (Array.isArray(payload.territoryOptions) && payload.territoryOptions.length > 1) {
    failures.push("TERRITORY_OPTIONS_MUST_NOT_BE_CUSTOMER_TOP_LEVEL");
  }
  return { pass: failures.length === 0, failures, primaryIds };
}

/**
 * DOM-string / HTML shell certification for forbidden BPP customer components.
 * Scope: only nodes inside #adpBrandPortfolioSection (or equivalent shell).
 */
export function assertBppCustomerDomForbiddenAbsent(htmlOrDomSlice = "") {
  // Strip HTML comments so documentation notes do not false-positive.
  const s = String(htmlOrDomSlice || "").replace(/<!--[\s\S]*?-->/g, "");
  const failures = [];
  if (/Additional Portfolio Analysis/i.test(s)) {
    failures.push("DOM_CONTAINS_ADDITIONAL_PORTFOLIO_ANALYSIS");
  }
  if (/id=["']adpBrandPortfolioSupplemental["']/.test(s)) {
    failures.push("DOM_CONTAINS_SUPPLEMENTAL_DETAILS");
  }
  if (/id=["']adpBppTerritorySelect["']/.test(s)) {
    failures.push("DOM_CONTAINS_BPP_TERRITORY_SELECT");
  }
  if (/id=["']adpBrandPortfolioTerritoryWrap["']/.test(s)) {
    failures.push("DOM_CONTAINS_BPP_TERRITORY_WRAP");
  }
  // Demand Territory label bound to BPP select only (not the main ADP section title)
  if (
    /for=["']adpBppTerritorySelect["']/.test(s) ||
    (/Demand Territory/i.test(s) && /adpBppTerritorySelect/.test(s))
  ) {
    failures.push("DOM_CONTAINS_BPP_DEMAND_TERRITORY_LABEL");
  }
  if (/id=["']adpBrandPortfolioSecondaryHost["']/.test(s)) {
    failures.push("DOM_CONTAINS_SECONDARY_HOST");
  }
  if (/id=["']adpBrandPortfolioProviderHost["']/.test(s)) {
    failures.push("DOM_CONTAINS_BPP_PROVIDER_HOST");
  }
  if (/PORTFOLIO MEASUREMENT COMPLETE\s*[—\-–]\s*PEER RANKING PENDING/i.test(s)) {
    failures.push("DOM_CONTAINS_UNAPPROVED_PEER_RANKING_PENDING_STATUS_PANEL");
  }
  if (/adp-bpp-status--rank-blocked/.test(s)) {
    failures.push("DOM_CONTAINS_RANK_BLOCKED_STATUS_PANEL");
  }
  return { pass: failures.length === 0, failures };
}
