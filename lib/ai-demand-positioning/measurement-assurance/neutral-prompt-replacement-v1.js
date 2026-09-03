/**
 * Neutral prompt replacement helpers + semantic equivalence checks.
 */

import { classifyPromptIntegrity } from "./prompt-bias-detection-v1.js";
import { SCENARIO_CLASSES, BIAS_CLASSIFICATIONS } from "./prompt-integrity-contract-v1.js";

export const NEUTRAL_REPLACEMENT_SEMANTIC_EQUIVALENCE = "NEUTRAL_REPLACEMENT_SEMANTIC_EQUIVALENCE";

const BRAND_LOYALTY_PATTERNS = [
  /\bwith\s+hilton\s+honors(?:\s+points)?\b/gi,
  /\bhilton\s+honors\b/gi,
  /\bhonors\s+points\b/gi,
  /\bwith\s+marriott\s+bonvoy(?:\s+points)?\b/gi,
  /\bmarriott\s+bonvoy(?:\s+points)?\b/gi,
  /\bbonvoy\s+points\b/gi,
  /\bwith\s+hyatt\s+world\s+of\s+hyatt\b/gi,
  /\bworld\s+of\s+hyatt\b/gi,
  /\bhyatt\s+world\s+of\s+hyatt\b/gi,
  /\bcurio\s+collection\s+by\s+hilton\b/gi,
  /\bsoft-brand\s+hilton\b/gi,
  /\bmarriott-affiliated\b/gi,
  /\boutique-style\s+hilton\s+property\b/gi,
  /\bhilton\s+property\b/gi,
  /\brenaissance\s+hotel\b/gi,
  /\ba\s+hyatt\s+hotel\b/gi,
  /\bhyatt\s+hotel\b/gi,
  /\bhilton\s+hotel\b/gi,
];

/**
 * Produce strongest neutral formulation preserving traveler need + geography + decision context.
 */
export function neutralizePromptText(originalPrompt) {
  let q = String(originalPrompt || "").trim();
  const before = q;

  // Ordered brand/loyalty strip
  q = q.replace(/\bRecommend a Hyatt hotel\b/gi, "Recommend a hotel");
  q = q.replace(/\bBest hotel in downtown NYC with Hyatt World of Hyatt for business\b/gi, "Best hotel in downtown NYC for business");
  q = q.replace(/\bBest hotel in Midtown Manhattan with Marriott Bonvoy points for business\b/gi, "Best hotel in Midtown Manhattan for business");
  q = q.replace(/\bBest hotel in Boca Raton with Hilton Honors points for business travel\b/gi, "Best hotel in Boca Raton for business travel");
  q = q.replace(/\bBest Hilton Honors hotel in downtown Kansas City for business travelers\b/gi, "Best hotel in downtown Kansas City for business travelers");
  q = q.replace(/\bBoutique-style Hilton property in Boca Raton for a business trip with Honors points\b/gi, "Boutique-style hotel in Boca Raton for a business trip");
  q = q.replace(/\bBest Curio Collection by Hilton property in Florida\b/gi, "Best boutique soft-brand hotel in Florida");
  q = q.replace(/\bRenaissance hotel in New York City with Marriott Bonvoy for a business trip\b/gi, "Upscale full-service hotel in New York City for a business trip");
  q = q.replace(/\bBest Marriott-affiliated hotel in Times Square for business travelers\b/gi, "Best upscale hotel in Times Square for business travelers");
  q = q.replace(/\bCurio Collection by Hilton boutique hotel in downtown Kansas City with Honors points\b/gi, "Boutique hotel in downtown Kansas City for a business trip");
  q = q.replace(/\bSoft-brand Hilton hotel in Kansas City that feels independent and boutique\b/gi, "Soft-brand hotel in Kansas City that feels independent and boutique");

  for (const re of BRAND_LOYALTY_PATTERNS) {
    q = q.replace(re, "");
  }

  // Cleanup whitespace / articles
  q = q
    .replace(/\s{2,}/g, " ")
    .replace(/\s+for\s+for\b/gi, " for ")
    .replace(/\bin\s+in\b/gi, " in ")
    .replace(/\s+,/g, ",")
    .replace(/\s+\?/g, "?")
    .replace(/\s+$/g, "")
    .replace(/^\s+/g, "")
    .replace(/\bwith\s+for\b/gi, "for")
    .trim();

  // If stripping emptied meaning, fall back to generic
  if (q.length < 12) {
    q = before;
  }

  return q;
}

export function extractSemanticAxes(prompt) {
  const p = String(prompt || "").toLowerCase();
  return {
    business: /\bbusiness|corporate|executive|meetings|client\b/.test(p),
    leisure: /\bleisure|vacation|getaway|weekend|sightseeing|tourist\b/.test(p),
    couples: /\bcouple|romantic|anniversary|honeymoon|partner\b/.test(p),
    family: /\bfamily|kids|teen|multigenerational\b/.test(p),
    group: /\bgroup|meeting|retreat|offsite|board|event|wedding|celebration\b/.test(p),
    wellness: /\bspa|wellness|fitness\b/.test(p),
    adventure: /\badventure|nightlife|explore|kayak|snorkel|dive\b/.test(p),
    geoNycDt: /\bnoho|soho|downtown|lower manhattan|greenwich|village|tribeca|lafayette\b/.test(p),
    geoNycMid: /\btimes square|midtown|broadway|theater\b/.test(p),
    geoBoca: /\bboca|palm beach|intracoastal|mizner|south florida\b/.test(p),
    geoBermuda: /\bbermuda|somerset|west end\b/.test(p),
    geoKc: /\bkansas city|power and light|power & light|bartle|kauffman\b/.test(p),
    boutique: /\bboutique|design|lifestyle|soft-brand|independent\b/.test(p),
    luxury: /\bluxury|upscale|full-service\b/.test(p),
  };
}

export function checkNeutralReplacementSemanticEquivalence({ originalPrompt, replacementPrompt, ownerIntent }) {
  const a = extractSemanticAxes(originalPrompt);
  const b = extractSemanticAxes(replacementPrompt);
  const defects = [];

  const intentKey = {
    business: "business",
    leisure: "leisure",
    couples: "couples",
    family: "family",
    group_meeting: "group",
    celebration: "group",
    wellness: "wellness",
    adventure: "adventure",
  }[ownerIntent];

  if (intentKey && a[intentKey] && !b[intentKey]) {
    defects.push(`intent_drift:${ownerIntent}`);
  }

  for (const geo of ["geoNycDt", "geoNycMid", "geoBoca", "geoBermuda", "geoKc"]) {
    if (a[geo] && !b[geo]) defects.push(`geo_drift:${geo}`);
  }

  // Replacement must not introduce a different primary geo
  const geos = ["geoNycDt", "geoNycMid", "geoBoca", "geoBermuda", "geoKc"];
  const aGeo = geos.filter((g) => a[g]);
  const bGeo = geos.filter((g) => b[g]);
  if (aGeo.length && bGeo.length && !bGeo.some((g) => aGeo.includes(g))) {
    defects.push("geo_switched");
  }

  return {
    gate: NEUTRAL_REPLACEMENT_SEMANTIC_EQUIVALENCE,
    pass: defects.length === 0,
    defects,
    originalAxes: a,
    replacementAxes: b,
  };
}

export function classifyScenarioForCore({ scenario, profile, exactPrompt }) {
  const integrity = classifyPromptIntegrity({
    exactRenderedPrompt: exactPrompt || scenario?.query || "",
    profile,
    scenario,
  });
  // Source property_specific always non-core under founder decision
  if (scenario?.source === "property_specific") {
    const cls =
      integrity.biasClassification === BIAS_CLASSIFICATIONS.GOVERNED_LEGITIMATE_BRAND_SPECIFIC ||
      integrity.otherBrandPresent ||
      integrity.subjectBrandPresent
        ? SCENARIO_CLASSES.BRAND_SPECIFIC
        : SCENARIO_CLASSES.PROPERTY_SPECIFIC;
    return { ...integrity, scenarioClass: cls, coreEligible: false };
  }
  if (
    integrity.biasClassification === BIAS_CLASSIFICATIONS.GOVERNED_LEGITIMATE_BRAND_SPECIFIC ||
    integrity.otherBrandPresent ||
    integrity.subjectBrandPresent ||
    integrity.biasClassification === BIAS_CLASSIFICATIONS.UNINTENDED_BRAND_BIAS ||
    integrity.biasClassification === BIAS_CLASSIFICATIONS.PROFILE_AFFILIATION_CONTAMINATION
  ) {
    return { ...integrity, scenarioClass: SCENARIO_CLASSES.BRAND_SPECIFIC, coreEligible: false };
  }
  return {
    ...integrity,
    scenarioClass: SCENARIO_CLASSES.NEUTRAL_DEMAND,
    coreEligible: integrity.biasClassification === BIAS_CLASSIFICATIONS.PASS_NEUTRAL,
  };
}

export function buildReplacementScenarioId(originalScenarioId) {
  return `neutral_repl_${originalScenarioId}`;
}
