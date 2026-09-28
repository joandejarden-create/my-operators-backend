/**
 * Phase 2 — Origin / travel class for lodging prior.
 */

import { TRAVEL_CLASS } from "./v3-constants.js";
import { cleanEntityDisplayName } from "./v3-entity-clean.js";

const NYC_LOCAL_RE =
  /\b(new\s*york|nyc|manhattan|brooklyn|queens|bronx|staten\s*island|long\s*island|jersey\s*city|hoboken|newark|nyc metro)\b/i;

const DRIVE_RE =
  /\b(philadelphia|philly|boston|hartford|new\s*haven|albany|syracuse|princeton|new\s*brunswick|stamford|bridgeport|white\s*plains|westchester|fairfield|new\s*jersey|nj\b|ct\b|pa\b)\b/i;

const SHORT_HAUL_RE =
  /\b(washington|d\.?c\.?|baltimore|richmond|pittsburgh|cleveland|detroit|toronto|ottawa|montreal|chicago|atlanta|charlotte|raleigh|columbus|buffalo)\b/i;

const LONG_HAUL_US_RE =
  /\b(california|los\s*angeles|san\s*francisco|seattle|portland|denver|dallas|houston|austin|phoenix|las\s*vegas|miami|orlando|tampa|minneapolis|salt\s*lake|san\s*diego)\b/i;

const INTL_RE =
  /\b(uk|united\s*kingdom|london|germany|france|japan|china|india|brazil|mexico|australia|korea|italy|spain|canada|ireland|netherlands|singapore|hong\s*kong|uae|israel|sweden|switzerland)\b/i;

/**
 * @returns {{ originSummary, travelClass, originConfidence, originBasis, hqHints }}
 */
export function classifyOriginTravel(entity = {}, deepenText = "") {
  const name = cleanEntityDisplayName(entity.entityName);
  const blob = [
    entity.country,
    entity.city,
    entity.state,
    entity.evidenceSnippet,
    deepenText,
    name,
  ]
    .filter(Boolean)
    .join(" ");

  const hqHints = extractHqHints(blob);
  let travelClass = TRAVEL_CLASS.UNKNOWN;
  let originSummary = hqHints[0] || entity.country || entity.city || null;
  let confidence = "LOW";
  let basis = [];

  if (entity.country && !/united\s*states|usa|u\.s\.?/i.test(entity.country)) {
    travelClass = TRAVEL_CLASS.INTERNATIONAL;
    originSummary = entity.country;
    confidence = "MEDIUM";
    basis.push("entity.country");
  } else if (INTL_RE.test(blob) && !NYC_LOCAL_RE.test(blob.slice(0, 80))) {
    travelClass = TRAVEL_CLASS.INTERNATIONAL;
    confidence = "MEDIUM";
    basis.push("international_keyword");
  } else if (NYC_LOCAL_RE.test(blob) && !LONG_HAUL_US_RE.test(blob) && !INTL_RE.test(blob)) {
    travelClass = TRAVEL_CLASS.LOCAL;
    confidence = "MEDIUM";
    basis.push("nyc_local_keyword");
  } else if (DRIVE_RE.test(blob)) {
    travelClass = TRAVEL_CLASS.DRIVE_MARKET;
    confidence = "MEDIUM";
    basis.push("drive_market_keyword");
  } else if (LONG_HAUL_US_RE.test(blob)) {
    travelClass = TRAVEL_CLASS.LONG_HAUL_AIR;
    confidence = "MEDIUM";
    basis.push("long_haul_us_keyword");
  } else if (SHORT_HAUL_RE.test(blob)) {
    travelClass = TRAVEL_CLASS.SHORT_HAUL_AIR;
    confidence = "MEDIUM";
    basis.push("short_haul_keyword");
  } else if (/headquarters|hq\b|based\s+in|located\s+in/i.test(blob)) {
    const loc = blob.match(/(?:headquarters|hq|based\s+in|located\s+in)[:\s]+([^.;|]{4,60})/i);
    if (loc) {
      originSummary = loc[1].trim();
      travelClass = classifyLocationPhrase(originSummary);
      confidence = "MEDIUM";
      basis.push("hq_phrase");
    }
  }

  // Name-embedded geography (e.g. "Montreal – Fall") already filtered as noise;
  // residual: company with city in deepen text
  return {
    originSummary,
    travelClass,
    originConfidence: confidence,
    originBasis: basis,
    hqHints,
  };
}

function classifyLocationPhrase(phrase = "") {
  if (NYC_LOCAL_RE.test(phrase)) return TRAVEL_CLASS.LOCAL;
  if (DRIVE_RE.test(phrase)) return TRAVEL_CLASS.DRIVE_MARKET;
  if (INTL_RE.test(phrase)) return TRAVEL_CLASS.INTERNATIONAL;
  if (LONG_HAUL_US_RE.test(phrase)) return TRAVEL_CLASS.LONG_HAUL_AIR;
  if (SHORT_HAUL_RE.test(phrase)) return TRAVEL_CLASS.SHORT_HAUL_AIR;
  return TRAVEL_CLASS.UNKNOWN;
}

function extractHqHints(blob) {
  const out = [];
  const m = String(blob).match(
    /(?:headquarters|hq|based\s+in|located\s+in|offices?\s+in)[:\s]+([A-Za-z][A-Za-z\s,.-]{3,50})/gi
  );
  if (m) {
    for (const x of m.slice(0, 3)) {
      const cleaned = x.replace(/^[^:]+:\s*/i, "").trim();
      if (cleaned) out.push(cleaned);
    }
  }
  return out;
}

export function travelIncreasesLodging(travelClass) {
  return (
    travelClass === TRAVEL_CLASS.SHORT_HAUL_AIR ||
    travelClass === TRAVEL_CLASS.LONG_HAUL_AIR ||
    travelClass === TRAVEL_CLASS.INTERNATIONAL
  );
}
