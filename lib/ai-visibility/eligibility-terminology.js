/**
 * Phase 3A.9 — Eligibility terminology (not Suitability).
 * Suitability implies Dealality judging brand fit/recommendation — forbidden here.
 * Do not alter legitimate Sustainability / ESG wording.
 */

export const ELIGIBILITY_TERMINOLOGY_VERSION = "ai_visibility_eligibility_terminology_v1";

/** Methodological term for structural addressability. */
export const METHODOLOGICAL_TERM = "Eligibility";

const METHOD_SUITABILITY_RE =
  /\b(?:brand\s+)?(?:new[\s-]?build\s+)?(?:geographic(?:al)?\s+|geography\s+|decision\s+|intent\s+|conversion\s+)?suitabilit(?:y|ies)\b|\b(?:not\s+)?suitable\b/gi;

const TRUE_FIT_HINT_RE =
  /\b(?:strategic\s+fit|asset[\s-]?specific\s+match|recommend(?:ation|ed|ing)?\s+fit|fit\s+score)\b/i;

const SUSTAINABILITY_RE =
  /\bsustainabilit(?:y|ies)\b|\bsustainable\s+(?:development|building|program|tourism|hospitality)\b|\bESG\b/gi;

/**
 * Classify a suitability-like occurrence in text.
 * @param {string} text
 * @param {{ path?: string }} [meta]
 */
export function classifySuitabilityOccurrence(text, meta = {}) {
  const raw = String(text || "");
  const sustainability = [...raw.matchAll(SUSTAINABILITY_RE)].map((m) => m[0]);
  const suitability = [...raw.matchAll(METHOD_SUITABILITY_RE)].map((m) => m[0]);
  const trueFit = TRUE_FIT_HINT_RE.test(raw);

  return {
    path: meta.path || null,
    suitabilityMatches: suitability,
    sustainabilityMatches: sustainability,
    classification: suitability.length
      ? trueFit
        ? "TRUE_FIT_REVIEW"
        : "ELIGIBILITY_MEANING"
      : sustainability.length
        ? "SUSTAINABILITY_ONLY"
        : "NONE",
  };
}

/**
 * Replace methodological suitability wording with Eligibility equivalents.
 * Preserves Sustainability / ESG. Does not rewrite true-fit concepts.
 * @param {string} text
 */
export function replaceEligibilityTerminology(text) {
  let out = String(text || "");
  // Protect sustainability tokens
  const protectedTokens = [];
  out = out.replace(SUSTAINABILITY_RE, (m) => {
    const key = `__SUST_${protectedTokens.length}__`;
    protectedTokens.push(m);
    return key;
  });

  out = out.replace(/\bNew Build Suitability\b/gi, "New Build Eligibility");
  out = out.replace(/\bConversion Suitability\b/gi, "Conversion Eligibility");
  out = out.replace(/\bBrand Suitability\b/gi, "Brand Eligibility");
  out = out.replace(/\bGeographic(?:al)? Suitability\b/gi, "Geographic Eligibility");
  out = out.replace(/\bGeography Suitability\b/gi, "Geographic Eligibility");
  out = out.replace(/\bDecision Suitability\b/gi, "Decision Eligibility");
  out = out.replace(/\bIntent Suitability\b/gi, "Intent Eligibility");
  out = out.replace(/\bBranded Residences Suitability\b/gi, "Branded Residences Eligibility");
  out = out.replace(/\bnew-build suitability\b/gi, "new-build eligibility");
  out = out.replace(/\bsuitability field\b/gi, "eligibility field");
  out = out.replace(/\bsuitability scores?\b/gi, "eligibility scores");
  out = out.replace(/\bAI suitability\b/gi, "AI eligibility");
  out = out.replace(/\bNot Suitable\b/gi, "Not Eligible");
  out = out.replace(/\bnot suitable\b/g, "not eligible");
  out = out.replace(/\bSuitable\b/g, "Eligible");
  // Remaining bare "suitability" in methodological prose
  out = out.replace(/\bsuitability\b/gi, "eligibility");

  out = out.replace(/__SUST_(\d+)__/g, (_, i) => protectedTokens[Number(i)]);
  return out;
}

/**
 * Assert no methodological Suitability remains (tests).
 * @param {string} text
 */
export function hasMethodologicalSuitability(text) {
  const c = classifySuitabilityOccurrence(text);
  return c.classification === "ELIGIBILITY_MEANING" || c.classification === "TRUE_FIT_REVIEW";
}
