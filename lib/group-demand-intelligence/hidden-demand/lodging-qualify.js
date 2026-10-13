/**
 * Lodging qualification for extracted entities — PRIOR + evidence, not invented room counts.
 */

import { LODGING_SIGNAL_STRENGTH } from "./constants.js";

const NYC_LOCAL_RE =
  /\b(new\s*york|nyc|manhattan|brooklyn|queens|bronx|staten\s*island|long\s*island|jersey\s*city|hoboken|newark)\b/i;

/**
 * Origin prior only — never proof of lodging.
 * @returns {'HIGH'|'MEDIUM'|'LOW'|'UNKNOWN'}
 */
export function lodgingOriginPrior({ country = null, city = null, state = null, evidenceSnippet = "" } = {}) {
  const blob = `${country || ""} ${city || ""} ${state || ""} ${evidenceSnippet || ""}`;
  if (/international|overseas|abroad|canada|uk|united\s*kingdom|germany|france|japan|china|india|brazil|mexico|australia|korea|italy|spain/i.test(blob)) {
    if (!NYC_LOCAL_RE.test(country || "")) return "HIGH";
  }
  if (/\b(NY|New\s*York)\b/i.test(state || "") || NYC_LOCAL_RE.test(city || "")) return "LOW";
  if (/\b(CA|TX|FL|IL|MA|PA|NJ|CT|WA|CO|GA|NC|OH|MI)\b/.test(state || "") || /out[- ]of[- ]state|from\s+[A-Z]{2}\b/i.test(blob)) {
    return "MEDIUM";
  }
  if (country && !/united\s*states|usa|u\.s\./i.test(country)) return "HIGH";
  return "UNKNOWN";
}

/**
 * Qualify lodging signal from entity + optional housing doc signals + follow-up text.
 */
export function qualifyEntityLodging(entity = {}, housingSignals = null, followUpText = "") {
  const blob = `${entity.evidenceSnippet || ""} ${entity.entityName || ""} ${followUpText || ""}`;
  const prior = lodgingOriginPrior({
    country: entity.country,
    city: entity.city,
    state: entity.state,
    evidenceSnippet: blob,
  });

  const signals = {
    originPrior: prior,
    outOfMarket: prior === "MEDIUM" || prior === "HIGH",
    international: prior === "HIGH",
    multiDay: /multi[- ]day|three[- ]day|four[- ]day|\d+\s*days?\b|setup|breakdown|load[- ]in|load[- ]out/i.test(blob),
    teamCohort: /\b(team|crew|cohort|delegation|staff|booth\s*staff|project\s*team)\b/i.test(blob),
    travelInstruction: /travel|itinerary|arrive|flight|hotel\s*recommend/i.test(blob),
    hotelBooking: /hotel\s*block|room\s*block|book\s*hotel|housing|accommodation|stay[- ]to[- ]play/i.test(blob),
    housingLink: Boolean(housingSignals?.housingPageFound || housingSignals?.roomBlockMentioned),
    overnight: /overnight|room\s*night|stay\s*overnight/i.test(blob),
    intensity:
      Boolean(entity.boothNumber) ||
      /sponsor|keynote|product\s*launch|multiple\s*sessions|large\s*booth/i.test(blob),
  };

  let strength = LODGING_SIGNAL_STRENGTH.UNKNOWN;
  let score = 0;
  if (signals.hotelBooking || signals.housingLink) score += 40;
  if (signals.international) score += 20;
  else if (signals.outOfMarket) score += 12;
  if (signals.multiDay) score += 12;
  if (signals.teamCohort) score += 10;
  if (signals.travelInstruction) score += 8;
  if (signals.overnight) score += 10;
  if (signals.intensity) score += 6;
  if (housingSignals?.overflowMentioned) score += 8;
  if (housingSignals?.internationalDelegationMentioned) score += 10;

  if (score >= 55) strength = LODGING_SIGNAL_STRENGTH.STRONG;
  else if (score >= 30) strength = LODGING_SIGNAL_STRENGTH.MEDIUM;
  else if (score >= 15) strength = LODGING_SIGNAL_STRENGTH.WEAK;
  else strength = LODGING_SIGNAL_STRENGTH.UNKNOWN;

  return {
    lodgingSignalStrength: strength,
    lodgingScore: score,
    lodgingSignals: signals,
    housingEvidence: housingSignals || null,
  };
}
