/**
 * Prompt neutrality / bias detection for ADP (token-boundary, no substring traps).
 */

import {
  BIAS_CLASSIFICATIONS,
  MAJOR_BRAND_TOKENS,
  MATERIALITY,
  SCENARIO_CLASSES,
} from "./prompt-integrity-contract-v1.js";

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Token-boundary match (case-insensitive). Avoids "now" inside "known". */
export function tokenPresent(haystack, needle) {
  const n = String(needle || "").trim();
  if (!n) return false;
  const h = String(haystack || "");
  const re = new RegExp(`(^|[^a-z0-9])${escapeRegExp(n)}([^a-z0-9]|$)`, "i");
  return re.test(h);
}

export function findMatchingTokens(haystack, tokens) {
  return (tokens || []).filter((t) => tokenPresent(haystack, t));
}

export function collectSubjectTokens(profile) {
  const out = new Set();
  for (const v of [
    profile?.name,
    profile?.displayName,
    profile?.legalName,
    ...(profile?.aliases || []),
  ]) {
    const s = String(v || "").trim();
    if (s.length >= 3) out.add(s);
  }
  // Multi-word hotel names only — skip ultra-short tokens like "NOW"
  return [...out].filter((t) => t.length >= 4 || /\s/.test(t));
}

export function collectSubjectBrandTokens(profile) {
  const out = new Set();
  for (const v of [
    profile?.brand,
    profile?.brandName,
    profile?.affiliation,
    profile?.parentCompany,
    profile?.parent,
  ]) {
    const s = String(v || "").trim();
    if (!s || /^independent$/i.test(s)) continue;
    out.add(s);
    // Also add major parent roots
    if (/hilton/i.test(s)) {
      out.add("Hilton");
      out.add("Hilton Honors");
      out.add("Curio Collection");
    }
    if (/marriott|renaissance|bonvoy/i.test(s)) {
      out.add("Marriott");
      out.add("Bonvoy");
      out.add("Renaissance");
    }
    if (/hyatt/i.test(s)) {
      out.add("Hyatt");
      out.add("World of Hyatt");
    }
  }
  return [...out];
}

export function hasUnresolvedPlaceholders(prompt) {
  return /\{\{[^}]+\}\}|\$\{[^}]+\}|%\w+%|<<[^>]+>>/.test(String(prompt || ""));
}

/** Loyalty / soft-brand tokens implied by a parent family. */
const LOYALTY_FAMILY = Object.freeze({
  hilton: ["hilton", "hilton honors", "honors points", "curio collection", "tapestry", "canopy", "waldorf"],
  marriott: [
    "marriott",
    "bonvoy",
    "renaissance",
    "renaissance hotels",
    "autograph collection",
    "tribute portfolio",
    "luxury collection",
    "westin",
    "sheraton",
    "w hotel",
    "st. regis",
  ],
  hyatt: [
    "hyatt",
    "world of hyatt",
    "park hyatt",
    "grand hyatt",
    "hyatt centric",
    "hyatt place",
    "hyatt house",
    "andaz",
    "thompson hotel",
  ],
  ihg: ["ihg", "one rewards", "intercontinental", "kimpton"],
});

function subjectCoversToken(subjectBrandTokens, token) {
  const subjectLow = subjectBrandTokens.map((t) => t.toLowerCase());
  const tok = String(token || "").toLowerCase();
  if (subjectLow.some((sb) => sb.includes(tok) || tok.includes(sb))) return true;
  for (const [family, members] of Object.entries(LOYALTY_FAMILY)) {
    const subjectInFamily = subjectLow.some(
      (sb) => sb.includes(family) || members.some((m) => sb.includes(m))
    );
    if (subjectInFamily && (tok === family || members.includes(tok))) return true;
  }
  return false;
}

/**
 * Detect brand tokens in prompt that are NOT the subject's governed brands.
 */
export function detectForeignBrandTokens(prompt, subjectBrandTokens) {
  const hits = [];
  for (const token of MAJOR_BRAND_TOKENS) {
    if (!tokenPresent(prompt, token)) continue;
    if (!subjectCoversToken(subjectBrandTokens, token)) hits.push(token);
  }
  return hits;
}

/**
 * Classify a single rendered prompt for a property.
 *
 * @param {object} args
 * @param {string} args.exactRenderedPrompt
 * @param {object} args.profile — property profile
 * @param {object} args.scenario — scenario row (source, intent, query)
 * @param {string[]} [args.competitorNames]
 * @param {string[]} [args.peerPropertyNames] — other cohort hotels for cross-property leak
 * @param {object} [args.groundTruthAffiliation] — optional override when profile affiliation is disputed
 */
export function classifyPromptIntegrity({
  exactRenderedPrompt,
  profile,
  scenario,
  competitorNames = [],
  peerPropertyNames = [],
  groundTruthAffiliation = null,
}) {
  const prompt = String(exactRenderedPrompt || "");
  const subjectTokens = collectSubjectTokens(profile);
  const profileBrandTokens = collectSubjectBrandTokens(profile);

  // Ground-truth affiliation (founder-corrected) when profile is wrong
  const effectiveBrandTokens = groundTruthAffiliation
    ? collectSubjectBrandTokens({
        ...profile,
        brand: groundTruthAffiliation.brand,
        affiliation: groundTruthAffiliation.affiliation,
        parentCompany: groundTruthAffiliation.parentCompany,
      })
    : profileBrandTokens;

  const subjectNameHits = subjectTokens.filter((t) => tokenPresent(prompt, t));
  const subjectBrandHits = effectiveBrandTokens.filter((t) => tokenPresent(prompt, t));
  const competitorHits = (competitorNames || []).filter((t) => tokenPresent(prompt, t));
  const peerHits = (peerPropertyNames || [])
    .filter((n) => n && n !== profile?.name)
    .filter((t) => tokenPresent(prompt, t));
  const foreignBrands = detectForeignBrandTokens(prompt, effectiveBrandTokens);
  const unresolved = hasUnresolvedPlaceholders(prompt);

  const source = scenario?.source || "unknown";
  let scenarioClass = SCENARIO_CLASSES.NEUTRAL_DEMAND;
  if (source === "property_specific") {
    scenarioClass =
      subjectBrandHits.length || foreignBrands.length
        ? SCENARIO_CLASSES.BRAND_SPECIFIC
        : SCENARIO_CLASSES.PROPERTY_SPECIFIC;
  } else if (foreignBrands.length || subjectBrandHits.length) {
    scenarioClass = SCENARIO_CLASSES.BRAND_SPECIFIC;
  }

  let biasClassification = BIAS_CLASSIFICATIONS.PASS_NEUTRAL;
  let defectReason = null;
  let caseType = null;
  let materiality = MATERIALITY.NON_MATERIAL;
  let measurementEligibleUnderContract = scenarioClass === SCENARIO_CLASSES.NEUTRAL_DEMAND;

  if (unresolved) {
    biasClassification = BIAS_CLASSIFICATIONS.UNRESOLVED_VARIABLE;
    defectReason = "Unresolved template placeholders in rendered prompt";
    materiality = MATERIALITY.MATERIAL_REQUIRES_PERIOD_REPROCESS;
    measurementEligibleUnderContract = false;
  } else if (peerHits.length) {
    biasClassification = BIAS_CLASSIFICATIONS.CROSS_PROPERTY_CONTAMINATION;
    defectReason = `Cross-property name(s) in prompt: ${peerHits.join(", ")}`;
    materiality = MATERIALITY.MATERIAL_REQUIRES_PERIOD_REPROCESS;
    measurementEligibleUnderContract = false;
    caseType = "C_CROSS_PROPERTY_TEMPLATE_CONTAMINATION";
  } else if (subjectNameHits.length && scenarioClass === SCENARIO_CLASSES.NEUTRAL_DEMAND) {
    biasClassification = BIAS_CLASSIFICATIONS.SUBJECT_NAME_LEAKAGE;
    defectReason = `Subject name leak in neutral prompt: ${subjectNameHits.join(", ")}`;
    materiality = MATERIALITY.MATERIAL_REQUIRES_PERIOD_REPROCESS;
    measurementEligibleUnderContract = false;
  } else if (competitorHits.length) {
    biasClassification = BIAS_CLASSIFICATIONS.COMPETITOR_PROMPT_LEAKAGE;
    defectReason = `Competitor name(s) in prompt: ${competitorHits.join(", ")}`;
    materiality = MATERIALITY.MATERIAL_REQUIRES_PERIOD_REPROCESS;
    measurementEligibleUnderContract = false;
  } else if (foreignBrands.length) {
    // Brand restriction not matching subject (under ground truth)
    biasClassification = BIAS_CLASSIFICATIONS.UNINTENDED_BRAND_BIAS;
    defectReason = `Foreign brand bias token(s): ${foreignBrands.join(", ")}`;
    materiality = MATERIALITY.MATERIAL_REQUIRES_PERIOD_REPROCESS;
    measurementEligibleUnderContract = false;
    caseType = "B_UNINTENDED_BRAND_BIAS";
    if (
      groundTruthAffiliation &&
      profileBrandTokens.some((t) => foreignBrands.some((f) => tokenPresent(t, f) || tokenPresent(f, t)))
    ) {
      biasClassification = BIAS_CLASSIFICATIONS.PROFILE_AFFILIATION_CONTAMINATION;
      defectReason = `${defectReason}; profile affiliation appears incorrect vs ground truth`;
    }
  } else if (subjectBrandHits.length) {
    // Subject's own brand / loyalty — governed brand-specific if intentional
    biasClassification = BIAS_CLASSIFICATIONS.GOVERNED_LEGITIMATE_BRAND_SPECIFIC;
    defectReason = `Subject-brand / loyalty restriction: ${subjectBrandHits.join(", ")}`;
    materiality = MATERIALITY.METHODOLOGY_REVIEW_REQUIRED;
    measurementEligibleUnderContract = false; // not eligible for NEUTRAL core KPIs
    caseType = "A_GOVERNED_LEGITIMATE_BRAND_SPECIFIC";
    scenarioClass = SCENARIO_CLASSES.BRAND_SPECIFIC;
  } else if (source === "property_specific") {
    biasClassification = BIAS_CLASSIFICATIONS.GOVERNED_LEGITIMATE_PROPERTY_SPECIFIC;
    materiality = MATERIALITY.METHODOLOGY_REVIEW_REQUIRED;
    measurementEligibleUnderContract = false;
    scenarioClass = SCENARIO_CLASSES.PROPERTY_SPECIFIC;
  }

  // Geography soft check — market keywords
  let geographyCheck = "PASS";
  const market = String(profile?.market || "").toLowerCase();
  const sub = String(profile?.submarket || "").toLowerCase();
  if (market.includes("bermuda") && /\b(boca|manhattan|kansas city|nyc|times square)\b/i.test(prompt)) {
    geographyCheck = "FAIL_WRONG_MARKET";
    biasClassification = BIAS_CLASSIFICATIONS.GEOGRAPHY_MISMATCH;
    materiality = MATERIALITY.MATERIAL_REQUIRES_PERIOD_REPROCESS;
    measurementEligibleUnderContract = false;
  } else if (
    (market.includes("kansas") || sub.includes("power")) &&
    /\b(boca|bermuda|manhattan|times square|noho|soho)\b/i.test(prompt) &&
    !/\bkansas\b/i.test(prompt)
  ) {
    geographyCheck = "REVIEW_GEO_TOKENS";
  } else if (
    (market.includes("boca") || sub.includes("boca")) &&
    /\b(bermuda|manhattan|kansas city|times square)\b/i.test(prompt)
  ) {
    geographyCheck = "FAIL_WRONG_MARKET";
    biasClassification = BIAS_CLASSIFICATIONS.GEOGRAPHY_MISMATCH;
    materiality = MATERIALITY.MATERIAL_REQUIRES_PERIOD_REPROCESS;
  }

  return {
    scenarioClass,
    biasClassification,
    caseType,
    materiality,
    measurementEligibleUnderContract,
    defectReason,
    subjectNamePresent: subjectNameHits.length > 0,
    subjectNameHits,
    subjectBrandPresent: subjectBrandHits.length > 0,
    subjectBrandHits,
    competitorNamePresent: competitorHits.length > 0,
    competitorHits,
    otherBrandPresent: foreignBrands.length > 0,
    otherBrandHits: foreignBrands,
    crossPropertyLeak: peerHits.length > 0,
    peerHits,
    geographyCheck,
    unresolvedVariable: unresolved,
    reviewStatus:
      materiality === MATERIALITY.NON_MATERIAL && biasClassification === BIAS_CLASSIFICATIONS.PASS_NEUTRAL
        ? "PASS"
        : materiality === MATERIALITY.METHODOLOGY_REVIEW_REQUIRED
          ? "METHODOLOGY_REVIEW"
          : "DEFECT_REVIEW",
  };
}
