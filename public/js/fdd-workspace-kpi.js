/**
 * Shared FDD Intelligence workspace KPI strip (same cards as brand economics profile).
 * Used by admin, economics profile, and brand comparison pages.
 *
 * Session keys: `fdd-intelligence-kpi-v1:` + context (`admin` | `economics` | `comparison`)
 * so KPI values survive tab navigation (full page loads). Keep in sync with
 * `public/js/fdd-workspace-kpi-cache-boot.js`.
 */

/** Session key prefix — must match `PREFIX` in `fdd-workspace-kpi-cache-boot.js`. */
export const FDD_KPI_SESSION_PREFIX = "fdd-intelligence-kpi-v1:";

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function emptyFddWorkspaceSummary() {
  return {
    approvedCount: 0,
    needsReviewCount: 0,
    possibleDuplicateCount: 0,
    legalReviewCount: 0,
    commercialReviewCount: 0,
  };
}

/**
 * @param {Array<Record<string, unknown>>|null|undefined} rows
 */
export function fddRowsToWorkspaceSummary(rows) {
  const out = emptyFddWorkspaceSummary();
  if (!Array.isArray(rows)) return out;
  for (const r of rows) {
    const rs = String(r.reviewStatus || "").trim();
    if (rs === "Approved") out.approvedCount++;
    if (rs === "Needs Review") out.needsReviewCount++;
    if (r.possibleDuplicate === true) out.possibleDuplicateCount++;
    if (r.needsLegalReview === true) out.legalReviewCount++;
    if (r.needsCommercialReview === true) out.commercialReviewCount++;
  }
  return out;
}

function normalizeWorkspaceSummary(s) {
  const z = emptyFddWorkspaceSummary();
  if (!s || typeof s !== "object") return z;
  return {
    approvedCount: Number(s.approvedCount) || 0,
    needsReviewCount: Number(s.needsReviewCount) || 0,
    possibleDuplicateCount: Number(s.possibleDuplicateCount) || 0,
    legalReviewCount: Number(s.legalReviewCount) || 0,
    commercialReviewCount: Number(s.commercialReviewCount) || 0,
  };
}

/**
 * @param {"admin" | "economics" | "comparison"} context
 * @returns {ReturnType<typeof emptyFddWorkspaceSummary> | null}
 */
export function readFddKpiSessionSummary(context) {
  if (!context) return null;
  try {
    const raw = sessionStorage.getItem(FDD_KPI_SESSION_PREFIX + context);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return normalizeWorkspaceSummary(parsed);
  } catch {
    return null;
  }
}

/**
 * @param {"admin" | "economics" | "comparison"} context
 * @param {ReturnType<typeof emptyFddWorkspaceSummary>|null|undefined} summary
 */
export function writeFddKpiSessionSummary(context, summary) {
  if (!context) return;
  const s = normalizeWorkspaceSummary(summary);
  try {
    sessionStorage.setItem(FDD_KPI_SESSION_PREFIX + context, JSON.stringify(s));
  } catch {
    /* quota / private mode */
  }
}

/**
 * @param {HTMLElement|null} containerEl host for innerHTML (e.g. #fdd-summary-cards)
 * @param {ReturnType<typeof emptyFddWorkspaceSummary>|null|undefined} summary
 * @param {"admin" | "economics" | "comparison" | null} [persistContext] when set, summary is stored for the next tab visit
 */
export function renderFddWorkspaceKpiStrip(containerEl, summary, persistContext) {
  if (!containerEl) return;
  const s = summary || emptyFddWorkspaceSummary();
  const items = [
    ["Approved rows", s.approvedCount],
    ["Needs Review rows", s.needsReviewCount],
    ["Possible duplicate rows", s.possibleDuplicateCount],
    ["Legal review (Yes)", s.legalReviewCount],
    ["Commercial review (Yes)", s.commercialReviewCount],
  ];
  const cards = items
    .map(
      ([label, n]) => `<div class="fdd-kpi-metric-card">
        <div class="fdd-kpi-metric-card__label-wrap">
          <span class="fdd-kpi-metric-card__label">${escapeHtml(label)}</span>
        </div>
        <div class="fdd-kpi-metric-card__value">${escapeHtml(String(n ?? 0))}</div>
        <div class="fdd-kpi-metric-card__footer"></div>
      </div>`
    )
    .join("");
  containerEl.innerHTML = `<div class="fdd-kpi-strip-inner">${cards}</div>`;
  if (persistContext) writeFddKpiSessionSummary(persistContext, s);
}
