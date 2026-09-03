/**
 * Build customer Brand & Portfolio Position section payload.
 * Analytically separate from Core NEUTRAL_DEMAND.
 * Does not invent ranking metrics when peer universe / cycle is incomplete.
 *
 * Principles:
 * - NO_EMPTY_ANALYTICAL_SCAFFOLDING_BEFORE_MEASUREMENT
 * - PORTFOLIO_TABLE_REQUIRES_READY_MEASUREMENT
 * - PORTFOLIO_METRIC_MISSING_IS_NOT_ZERO
 */

import fs from "fs";
import path from "path";
import {
  SECTION_COPY,
  BRAND_PORTFOLIO_SECTION_ID,
  BRAND_PORTFOLIO_CONTRACT_VERSION,
  PORTFOLIO_TYPES,
  PORTFOLIO_LENS_STATUS,
} from "./brand-portfolio-position-contract-v1.js";
import { getPortfolioMapping } from "./brand-portfolio-affiliation-mapping-v1.js";

export const CORE_PORTFOLIO_MEASUREMENT_ISOLATION = "CORE_PORTFOLIO_MEASUREMENT_ISOLATION";
export const PORTFOLIO_TABLE_REQUIRES_READY_MEASUREMENT = "PORTFOLIO_TABLE_REQUIRES_READY_MEASUREMENT";
export const PORTFOLIO_METRIC_MISSING_IS_NOT_ZERO = "PORTFOLIO_METRIC_MISSING_IS_NOT_ZERO";
export const NO_EMPTY_ANALYTICAL_SCAFFOLDING_BEFORE_MEASUREMENT =
  "NO_EMPTY_ANALYTICAL_SCAFFOLDING_BEFORE_MEASUREMENT";

/** Internal status codes — never show verbatim in customer UI. */
export const BRAND_PORTFOLIO_STATUS = Object.freeze({
  NOT_CONFIGURED: "NOT_CONFIGURED",
  METHODOLOGY_PENDING: "METHODOLOGY_PENDING",
  AWAITING_FIRST_MONITORING: "AWAITING_FIRST_MONITORING",
  PARTIAL_NOT_CERTIFIED: "PARTIAL_NOT_CERTIFIED",
  READY: "READY",
  ASSURANCE_REVIEW_REQUIRED: "ASSURANCE_REVIEW_REQUIRED",
});

/** @deprecated use BRAND_PORTFOLIO_STATUS */
export const BRAND_PORTFOLIO_RENDER_STATES = Object.freeze({
  AWAITING_GOVERNED_CYCLE: BRAND_PORTFOLIO_STATUS.AWAITING_FIRST_MONITORING,
  METHODOLOGY_PENDING: BRAND_PORTFOLIO_STATUS.METHODOLOGY_PENDING,
  READY: BRAND_PORTFOLIO_STATUS.READY,
});

const SUBTITLE_BRAND_PORTFOLIO =
  "How this hotel performs within its brand, collection, portfolio, or loyalty ecosystem.";
const SUBTITLE_INDEPENDENT =
  "How this hotel performs when travelers specifically consider independent hotels in its market.";

const CUSTOMER_COPY = Object.freeze({
  [BRAND_PORTFOLIO_STATUS.NOT_CONFIGURED]: Object.freeze({
    statusLabel: "Portfolio monitoring not yet available",
    headline: "Portfolio monitoring not yet available",
    body: "Brand & Portfolio insights will appear here once this hotel’s affiliation lens is configured.",
    secondary: null,
  }),
  [BRAND_PORTFOLIO_STATUS.METHODOLOGY_PENDING]: Object.freeze({
    statusLabel: "Independent positioning not yet available",
    headline: "Independent Positioning not yet available",
    body: "Independent Positioning will appear here after the first independent-hotel monitoring cycle.",
    secondary: "You’ll see AI Presence, relative rank, and the hotels most often competing in this set.",
  }),
  [BRAND_PORTFOLIO_STATUS.AWAITING_FIRST_MONITORING]: Object.freeze({
    statusLabel: "Portfolio monitoring not yet available",
    headline: "Portfolio monitoring not yet available",
    body: "Brand & Portfolio insights will appear here after the first monitoring cycle.",
    secondary: "You’ll see AI Presence, portfolio rank, and the properties most often competing with this hotel.",
  }),
  [BRAND_PORTFOLIO_STATUS.PARTIAL_NOT_CERTIFIED]: Object.freeze({
    statusLabel: "Portfolio monitoring not yet available",
    headline: "Portfolio monitoring not yet available",
    body: "Brand & Portfolio insights will appear here after the first certified monitoring cycle.",
    secondary: "You’ll see AI Presence, portfolio rank, and the properties most often competing with this hotel.",
  }),
  [BRAND_PORTFOLIO_STATUS.ASSURANCE_REVIEW_REQUIRED]: Object.freeze({
    statusLabel: "Portfolio monitoring under review",
    headline: "Portfolio monitoring under review",
    body: "Brand & Portfolio insights will appear here once this period’s results are cleared for display.",
    secondary: null,
  }),
});

function loadReclassSummary() {
  const p = path.join(
    process.cwd(),
    "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_OBSERVATION_RECLASS_V1.json"
  );
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function availabilityForProperty(propertyId, reclass) {
  const row = reclass?.byProperty?.[propertyId];
  const c = row?.counts || {};
  if (!row) {
    return {
      validObservations: 0,
      invalidObservations: 0,
      needsRerunObservations: 0,
      propertySpecificOtherObservations: 0,
      validScenarios: 0,
      sufficientForRanking: false,
      historicalCandidateReuseAvailable: false,
      source: "missing_reclass_report",
    };
  }
  const validObs = Number(c.validObservations || 0);
  return {
    validObservations: validObs,
    invalidObservations: Number(c.invalidObservations || 0),
    needsRerunObservations: Number(c.needsRerunObservations || 0),
    propertySpecificOtherObservations: Number(c.otherObservations || 0),
    validScenarios: Number(c.validForBrandPortfolio || 0),
    // Historical candidates must never auto-promote to READY
    sufficientForRanking: false,
    historicalCandidateReuseAvailable: validObs > 0,
    source: "ADP_BRAND_PORTFOLIO_OBSERVATION_RECLASS_V1",
  };
}

/**
 * Certified READY requires explicit status + certified content — never inferred from payload presence alone.
 * @param {object} bpp
 */
export function isBrandPortfolioCustomerReady(bpp) {
  if (!bpp || bpp.status !== BRAND_PORTFOLIO_STATUS.READY) return false;
  if (bpp.assuranceStatus && bpp.assuranceStatus !== "CUSTOMER_READY") return false;
  const hasKpis = Array.isArray(bpp.kpis) && bpp.kpis.length > 0;
  const hasRows = bpp.ranking && Array.isArray(bpp.ranking.rows) && bpp.ranking.rows.length > 0;
  return Boolean(hasKpis && hasRows);
}

function resolveStatus({ mapping, defaultLens, isIndependent, availability, options }) {
  if (options.forceStatus) return options.forceStatus;
  if (!mapping || !defaultLens) return BRAND_PORTFOLIO_STATUS.NOT_CONFIGURED;
  if (defaultLens.status === PORTFOLIO_LENS_STATUS.METHODOLOGY_PENDING) {
    return BRAND_PORTFOLIO_STATUS.METHODOLOGY_PENDING;
  }
  if (options.certifiedMeasurement === true && options.kpis && options.ranking?.rows?.length) {
    return BRAND_PORTFOLIO_STATUS.READY;
  }
  // Independents with frozen methodology still await first monitoring cycle
  if (isIndependent) {
    return BRAND_PORTFOLIO_STATUS.AWAITING_FIRST_MONITORING;
  }
  if (availability.historicalCandidateReuseAvailable) {
    return BRAND_PORTFOLIO_STATUS.AWAITING_FIRST_MONITORING;
  }
  return BRAND_PORTFOLIO_STATUS.AWAITING_FIRST_MONITORING;
}

/**
 * @param {object} propertyProfile
 * @param {object} [options]
 * @param {string} [options.forceStatus] test/override
 * @param {boolean} [options.certifiedMeasurement]
 * @param {Array} [options.kpis]
 * @param {object} [options.ranking]
 * @param {boolean} [options.hasPriorPeriod]
 */
export function buildBrandPortfolioPositionPayload(propertyProfile, options = {}) {
  const propertyId = propertyProfile?.propertyId;
  const mapping = getPortfolioMapping(propertyId) || null;
  const reclass = options.reclassSummary || loadReclassSummary();
  const availability = availabilityForProperty(propertyId, reclass);

  const defaultLens =
    mapping?.lenses?.find((l) => l.lensId === mapping.defaultLensId) || mapping?.lenses?.[0] || null;

  const isIndependent =
    mapping?.sectionMode === "INDEPENDENT_POSITIONING" ||
    defaultLens?.portfolioType === PORTFOLIO_TYPES.INDEPENDENT_POSITIONING;

  const forbiddenHyatt =
    propertyId === "adp_now_now_noho" ||
    /^independent$/i.test(String(propertyProfile?.brand || "")) ||
    /^independent$/i.test(String(propertyProfile?.affiliation || ""));

  if (forbiddenHyatt) {
    const brand = String(propertyProfile?.brand || "");
    const aff = String(propertyProfile?.affiliation || "");
    if (/hyatt/i.test(brand) || /hyatt/i.test(aff)) {
      const copy = CUSTOMER_COPY[BRAND_PORTFOLIO_STATUS.METHODOLOGY_PENDING];
      return {
        ok: false,
        sectionId: BRAND_PORTFOLIO_SECTION_ID,
        error: "HYATT_FORBIDDEN_ON_INDEPENDENT",
        sectionVisible: true,
        title: SECTION_COPY.title,
        subtitle: SUBTITLE_INDEPENDENT,
        status: BRAND_PORTFOLIO_STATUS.METHODOLOGY_PENDING,
        renderState: BRAND_PORTFOLIO_STATUS.METHODOLOGY_PENDING,
        customerState: copy,
        lens: {
          lensId: "independent_positioning",
          label: "Independent Positioning",
          portfolioType: PORTFOLIO_TYPES.INDEPENDENT_POSITIONING,
          status: PORTFOLIO_LENS_STATUS.METHODOLOGY_PENDING,
        },
        kpis: null,
        ranking: null,
        tableColumns: null,
        showAnalyticalScaffolding: false,
        dataAvailability: availability,
        coreIsolation: true,
        gate: CORE_PORTFOLIO_MEASUREMENT_ISOLATION,
        principles: [
          NO_EMPTY_ANALYTICAL_SCAFFOLDING_BEFORE_MEASUREMENT,
          PORTFOLIO_TABLE_REQUIRES_READY_MEASUREMENT,
          PORTFOLIO_METRIC_MISSING_IS_NOT_ZERO,
        ],
        doesNotAlterCoreMetrics: true,
      };
    }
  }

  const status = resolveStatus({ mapping, defaultLens, isIndependent, availability, options });
  const ready =
    status === BRAND_PORTFOLIO_STATUS.READY &&
    options.certifiedMeasurement === true &&
    Array.isArray(options.kpis) &&
    options.kpis.length > 0 &&
    options.ranking?.rows?.length > 0;

  let customerState = null;
  if (!ready) {
    const base =
      CUSTOMER_COPY[status] || CUSTOMER_COPY[BRAND_PORTFOLIO_STATUS.AWAITING_FIRST_MONITORING];
    customerState = { ...base };
    if (isIndependent) {
      customerState.statusLabel = "Independent positioning not yet available";
      customerState.headline = "Independent Positioning not yet available";
      customerState.body =
        "Independent Positioning will appear here after the first independent-hotel monitoring cycle.";
      customerState.secondary =
        "You’ll see AI Presence, relative rank, and the hotels most often competing in this set.";
    }
  }

  const payload = {
    ok: true,
    sectionId: BRAND_PORTFOLIO_SECTION_ID,
    contractVersion: BRAND_PORTFOLIO_CONTRACT_VERSION,
    sectionVisible: true,
    title: SECTION_COPY.title,
    subtitle: isIndependent ? SUBTITLE_INDEPENDENT : SUBTITLE_BRAND_PORTFOLIO,
    sectionMode: mapping?.sectionMode || "BRAND_PORTFOLIO_POSITION",
    status,
    /** Alias for older clients */
    renderState: status,
    customerState,
    /** Backward-compatible single-line message */
    emptyMessage: customerState?.body || null,
    lens: defaultLens
      ? {
          lensId: defaultLens.lensId,
          label: defaultLens.label,
          portfolioType: defaultLens.portfolioType,
          status: defaultLens.status,
        }
      : null,
    optionalFutureLenses: (mapping?.lenses || [])
      .filter((l) => l.status === PORTFOLIO_LENS_STATUS.OPTIONAL_FUTURE)
      .map((l) => ({ lensId: l.lensId, label: l.label, status: l.status })),
    assuranceStatus: ready ? "CUSTOMER_READY" : "NOT_READY",
    showAnalyticalScaffolding: ready,
    kpis: ready ? options.kpis : null,
    ranking: ready
      ? {
          ...options.ranking,
          hasPriorPeriod: Boolean(options.hasPriorPeriod),
          firstPeriodDeltaLabel: options.hasPriorPeriod ? null : "—",
        }
      : null,
    tableColumns: ready
      ? [
          "Rank",
          "Hotel",
          "AI Presence",
          "Δ vs Prior Run",
          "Displacement vs You",
          "Scenarios Shared",
        ]
      : null,
    dataAvailability: availability,
    affiliationSnapshot: mapping?.profileEvidence || {
      brand: propertyProfile?.brand || null,
      affiliation: propertyProfile?.affiliation || null,
      parentCompany: propertyProfile?.parentCompany || null,
      operatorCompany: propertyProfile?.operatorCompany || null,
    },
    coreIsolation: true,
    gate: CORE_PORTFOLIO_MEASUREMENT_ISOLATION,
    principles: [
      NO_EMPTY_ANALYTICAL_SCAFFOLDING_BEFORE_MEASUREMENT,
      PORTFOLIO_TABLE_REQUIRES_READY_MEASUREMENT,
      PORTFOLIO_METRIC_MISSING_IS_NOT_ZERO,
    ],
    doesNotAlterCoreMetrics: true,
  };

  return payload;
}
