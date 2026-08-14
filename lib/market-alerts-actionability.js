/**
 * Audience-specific actionability for Market Alerts V1.3.
 * Deterministic — no scores, no LLM.
 */

const BRAND_ACTIONABLE_SIGNAL_TYPES = new Set([
  "Potential Development Opportunity",
  "Potential Conversion Opportunity",
  "Brand White-Space Signal",
  "Reflag Opportunity",
]);

const OPERATOR_ACTIONABLE_SIGNAL_TYPES = new Set([
  "Potential Management Opportunity",
  "New Development Opportunity",
  "Turnaround / Repositioning Opportunity",
]);

const CLOSED_SIGNAL_TIMING = new Set(["Decision Announced", "Post-Decision"]);

const CLOSED_DECISION_STAGES = new Set(["Likely Decided"]);

const BRAND_CLOSED_SIGNAL_TYPES = new Set([
  "Competitive Brand Move",
  "Owner Expansion Signal",
]);

const OPERATOR_CLOSED_SIGNAL_TYPES = new Set([
  "Management Agreement Announced",
  "Competitive Operator Move",
  "Operator Review Signal",
]);

/**
 * @param {{
 *   audience: 'owner'|'brand'|'operator',
 *   worthReviewing?: boolean,
 *   signalType?: string|null,
 *   decisionStage?: string|null,
 *   signalTiming?: string|null,
 *   projectDirection?: string|null,
 *   eventType?: string|null,
 *   treatment?: string|null,
 * }} input
 * @returns {boolean}
 */
export function isActionableForAudience(input = {}) {
  const {
    audience,
    worthReviewing = false,
    signalType = null,
    decisionStage = null,
    signalTiming = null,
    projectDirection = null,
    eventType = null,
    treatment = null,
  } = input;

  if (treatment === "IGNORE") return false;
  if (!worthReviewing) return false;
  if (projectDirection === "Rejected / Blocked") return false;

  if (audience === "brand") {
    if (!signalType || !BRAND_ACTIONABLE_SIGNAL_TYPES.has(signalType)) return false;
    if (BRAND_CLOSED_SIGNAL_TYPES.has(signalType)) return false;
    if (CLOSED_DECISION_STAGES.has(decisionStage || "")) return false;
    if (signalTiming && CLOSED_SIGNAL_TIMING.has(signalTiming)) return false;
    if (["Brand Signing", "Reflag", "Conversion"].includes(eventType || "")) return false;
    return true;
  }

  if (audience === "operator") {
    if (!signalType || !OPERATOR_ACTIONABLE_SIGNAL_TYPES.has(signalType)) return false;
    if (OPERATOR_CLOSED_SIGNAL_TYPES.has(signalType)) return false;
    if (CLOSED_DECISION_STAGES.has(decisionStage || "")) return false;
    if (signalTiming && CLOSED_SIGNAL_TIMING.has(signalTiming)) return false;
    if (
      ["Management Agreement", "Operator Appointment", "Operator Change"].includes(eventType || "")
    ) {
      return false;
    }
    return true;
  }

  if (audience === "owner") {
    if (eventType === "Hotel For Sale" && decisionStage === "Active") return true;
    if (
      signalType === "New Competitive Supply" &&
      ["Early", "Forming", "Active"].includes(decisionStage || "") &&
      (!signalTiming || !CLOSED_SIGNAL_TIMING.has(signalTiming))
    ) {
      return true;
    }
    if (
      signalType === "Capital / Transaction Signal" &&
      eventType === "Hotel For Sale" &&
      decisionStage === "Active"
    ) {
      return true;
    }
    if (["Sale", "Acquisition", "Portfolio Acquisition"].includes(eventType || "")) return false;
    if (signalType === "Strategic Market Change") return false;
    if (signalType === "Capital / Transaction Signal" && decisionStage === "Likely Decided") {
      return false;
    }
    return false;
  }

  return false;
}

/**
 * @param {{
 *   treatment?: string|null,
 *   owner?: { worthReviewing?: boolean, signalType?: string|null, decisionStage?: string|null },
 *   brand?: { worthReviewing?: boolean, signalType?: string|null, decisionStage?: string|null },
 *   operator?: { worthReviewing?: boolean, signalType?: string|null, decisionStage?: string|null },
 *   signalTiming?: string|null,
 *   projectDirection?: string|null,
 *   eventType?: string|null,
 * }} audienceBundle
 * @returns {{ owner: boolean, brand: boolean, operator: boolean }}
 */
export function computeActionableFlags(audienceBundle = {}) {
  const base = {
    treatment: audienceBundle.treatment || null,
    signalTiming: audienceBundle.signalTiming || null,
    projectDirection: audienceBundle.projectDirection || null,
    eventType: audienceBundle.eventType || null,
  };

  return {
    owner: isActionableForAudience({
      audience: "owner",
      worthReviewing: !!audienceBundle.owner?.worthReviewing,
      signalType: audienceBundle.owner?.signalType || null,
      decisionStage: audienceBundle.owner?.decisionStage || null,
      ...base,
    }),
    brand: isActionableForAudience({
      audience: "brand",
      worthReviewing: !!audienceBundle.brand?.worthReviewing,
      signalType: audienceBundle.brand?.signalType || null,
      decisionStage: audienceBundle.brand?.decisionStage || null,
      ...base,
    }),
    operator: isActionableForAudience({
      audience: "operator",
      worthReviewing: !!audienceBundle.operator?.worthReviewing,
      signalType: audienceBundle.operator?.signalType || null,
      decisionStage: audienceBundle.operator?.decisionStage || null,
      ...base,
    }),
  };
}

/**
 * Enforce V1.3 invariant: open opportunity types cannot coexist with decided timing/stage.
 * @param {'owner'|'brand'|'operator'} audience
 * @param {{ signalType?: string|null, decisionStage?: string|null }} slice
 * @param {string|null} signalTiming
 * @returns {boolean} true if invariant holds
 */
export function actionabilityInvariantHolds(audience, slice = {}, signalTiming = null) {
  const signalType = slice.signalType || null;
  const decisionStage = slice.decisionStage || null;

  if (audience === "brand" && signalType === "Potential Development Opportunity") {
    if (decisionStage === "Likely Decided") return false;
    if (signalTiming === "Decision Announced" || signalTiming === "Post-Decision") return false;
  }
  if (audience === "brand" && signalType === "Potential Conversion Opportunity") {
    if (decisionStage === "Likely Decided") return false;
    if (signalTiming === "Decision Announced" || signalTiming === "Post-Decision") return false;
  }
  if (audience === "operator" && signalType === "Potential Management Opportunity") {
    if (signalType === "Management Agreement Announced") return false;
    if (decisionStage === "Likely Decided") return false;
    if (signalTiming === "Decision Announced" || signalTiming === "Post-Decision") return false;
  }
  return true;
}

export function audienceActionableField(audience) {
  if (audience === "owner") return "Actionable — Owner";
  if (audience === "brand") return "Actionable — Brand";
  if (audience === "operator") return "Actionable — Operator";
  return null;
}
