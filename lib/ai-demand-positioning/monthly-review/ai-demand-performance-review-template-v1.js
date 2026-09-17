/**
 * Single canonical AI Demand Performance Review template contract.
 * Current generation must NEVER select legacy / fallback renderers.
 *
 * Gates:
 *   AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1
 *   SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_RENDERER
 *   AI_DEMAND_REVIEW_KPI_GRID_COMPONENT_REQUIRED
 *   AI_DEMAND_REVIEW_BASELINE_LAYOUT_PARITY
 */

export const AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1 =
  "AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1";

export const SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_RENDERER =
  "AdpMonthlyReviewReportPdfV1";

export const CANONICAL_PDF_RENDER_HTML = "/adp-monthly-review-pdf-render.html";
export const CANONICAL_PDF_RENDERER_JS =
  "/js/ai-demand-positioning/adp-monthly-review-report-pdf-v1.js";
export const CANONICAL_REPORT_SYSTEM_CSS =
  "/css/dealality-report-system-v1.css";
export const CANONICAL_MONTHLY_REVIEW_CSS =
  "/css/adp-monthly-review-report-v1.css";
export const CANONICAL_REPORT_FAMILY = "dealality-report-system-v1";

/** Required DOM components for current-template parity (Cambridge golden). */
export const REQUIRED_CANONICAL_COMPONENTS = Object.freeze([
  { section: "cover", selector: '[data-adp-mr-section="cover"]' },
  { section: "executive", selector: '[data-adp-mr-section="executive"]' },
  { section: "kpi_grid", selector: ".drs-kpi-band" },
  { section: "kpi_card", selector: ".drs-kpi" },
  { section: "assessment", selector: ".adp-mr-assessment-block" },
  { section: "callouts", selector: ".adp-mr-callout-stack .drs-callout" },
  { section: "movement", selector: '[data-adp-mr-section="movement"]' },
  { section: "evidence", selector: '[data-adp-mr-section="evidence"]' },
  { section: "actions", selector: '[data-adp-mr-section="actions"]' },
  {
    section: "accountability",
    selector: '[data-adp-mr-section="accountability"]',
  },
]);

/**
 * Forbidden markers in CURRENT generation HTML.
 * Live/meeting UI may still use .adp-mr-kpi; PDF must not.
 */
export const FORBIDDEN_CURRENT_PDF_MARKERS = Object.freeze([
  {
    id: "LEGACY_LIVE_KPI_ROW",
    pattern: /class=["'][^"']*adp-mr-kpi-row/,
    classify: "LEGACY",
  },
  {
    id: "LEGACY_LIVE_KPI_CARD",
    pattern: /class=["'][^"']*adp-mr-kpi(?:__|\s|["'])/,
    classify: "LEGACY",
  },
  {
    id: "LEGACY_MONTHLY_REVIEW_TEMPLATE_ID",
    pattern: /data-adp-mr-template=["']legacy/i,
    classify: "LEGACY",
  },
  {
    id: "FALLBACK_RAW_KPI_LIST",
    pattern: /data-adp-mr-kpi-fallback=["']1["']/,
    classify: "FALLBACK",
  },
  {
    id: "DEPRECATED_RENDERER_ATTR",
    pattern: /data-adp-mr-renderer=["'](?!AdpMonthlyReviewReportPdfV1)[^"']+["']/,
    classify: "FALLBACK",
  },
  {
    id: "LIVE_REPORT_PRINT_HOST",
    pattern: /adp-current-report-pdf-render|aiv-live-report-print/,
    classify: "LEGACY",
  },
  {
    id: "MISSING_REPORT_FAMILY",
    pattern: /data-report-family=["'](?!dealality-report-system-v1)[^"']*["']/,
    classify: "FALLBACK",
  },
]);

export function assertCanonicalPerformanceReviewHtml(html) {
  const text = String(html || "");
  const errors = [];

  if (!text.includes('data-report-family="dealality-report-system-v1"')) {
    errors.push("MISSING_CANONICAL_REPORT_FAMILY");
  }
  if (!text.includes("drs-kpi-band")) {
    errors.push("AI_DEMAND_REVIEW_KPI_GRID_COMPONENT_REQUIRED");
  }
  if (!text.includes("drs-kpi__label") || !text.includes("drs-kpi__value")) {
    errors.push("KPI_CARD_MARKUP_INCOMPLETE");
  }
  for (const marker of FORBIDDEN_CURRENT_PDF_MARKERS) {
    if (marker.pattern.test(text)) {
      errors.push(marker.id);
    }
  }
  return {
    ok: errors.length === 0,
    templateId: AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1,
    renderer: SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_RENDERER,
    errors,
  };
}
