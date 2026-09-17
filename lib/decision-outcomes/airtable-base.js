/**
 * Canonical intelligence Airtable base resolution (Decision/Outcome + GDI Opportunities).
 *
 * Prefer dedicated env vars — do NOT silently use Deal Capture MVP (AIRTABLE_BASE_ID)
 * for this layer after cutover.
 *
 * Target canonical base: appa2cE7FTRmIbB32 (shared with ADP governed base).
 * Legacy mistaken writes: appvtnDurnMSjINP6 (Deal Capture MVP).
 */

/** Known wrong base — Decision/Outcome must not write here after cutover. */
export const LEGACY_DEAL_CAPTURE_MVP_BASE_ID = "appvtnDurnMSjINP6";

/** Intended canonical intelligence / ADP base. */
export const CANONICAL_INTELLIGENCE_BASE_ID = "appa2cE7FTRmIbB32";

/**
 * Resolve Decision & Outcome Airtable base.
 * Order: AIRTABLE_INTELLIGENCE_BASE_ID → AIRTABLE_DECISION_OUTCOME_BASE_ID
 * → DECISION_OUTCOMES_AIRTABLE_BASE_ID → ADP_AIRTABLE_BASE_ID
 * → AIRTABLE_BASE_ID (legacy fallback; may trigger wrong-base guard)
 */
export function getDecisionOutcomesAirtableBaseId() {
  return (
    process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    process.env.AIRTABLE_DECISION_OUTCOME_BASE_ID ||
    process.env.DECISION_OUTCOMES_AIRTABLE_BASE_ID ||
    process.env.ADP_AIRTABLE_BASE_ID ||
    process.env.AIRTABLE_BASE_ID ||
    ""
  ).trim();
}

/**
 * Resolve GDI Opportunities Airtable base (same intelligence base by default).
 */
export function getGdiOpportunitiesAirtableBaseId() {
  return (
    process.env.AIRTABLE_GDI_BASE_ID ||
    process.env.GDI_OPPORTUNITIES_AIRTABLE_BASE_ID ||
    process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    process.env.AIRTABLE_DECISION_OUTCOME_BASE_ID ||
    process.env.DECISION_OUTCOMES_AIRTABLE_BASE_ID ||
    process.env.ADP_AIRTABLE_BASE_ID ||
    process.env.AIRTABLE_BASE_ID ||
    ""
  ).trim();
}

export function isLegacyMvpBase(baseId) {
  return String(baseId || "").trim() === LEGACY_DEAL_CAPTURE_MVP_BASE_ID;
}

/**
 * Reject writes to Deal Capture MVP for canonical Decision/Outcome / GDI Opportunities.
 * Default ON unless DECISION_OUTCOMES_ALLOW_MVP_BASE=1 (emergency only).
 */
export function assertNotLegacyMvpCanonicalBase(baseId, { surface = "decision_outcomes" } = {}) {
  const allow =
    String(process.env.DECISION_OUTCOMES_ALLOW_MVP_BASE || "").trim() === "1" ||
    String(process.env.DECISION_OUTCOMES_ALLOW_MVP_BASE || "")
      .trim()
      .toLowerCase() === "true";
  if (allow) return;
  if (!isLegacyMvpBase(baseId)) return;
  const err = new Error(
    `WRONG_CANONICAL_AIRTABLE_BASE: ${surface} resolved to Deal Capture MVP (${LEGACY_DEAL_CAPTURE_MVP_BASE_ID}). Set AIRTABLE_DECISION_OUTCOME_BASE_ID or AIRTABLE_INTELLIGENCE_BASE_ID to ${CANONICAL_INTELLIGENCE_BASE_ID}.`
  );
  err.code = "WRONG_CANONICAL_AIRTABLE_BASE";
  err.baseId = baseId;
  throw err;
}
