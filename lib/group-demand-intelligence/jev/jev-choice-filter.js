/**
 * Bound Jev choice sets to relevant options for the target/context.
 * Avoids asking sports targets about PRIVATE_EVENT_SIGNAL, etc.
 */

import { CHOICES } from "./jev-types.js";

const PLAYBOOK_BY_TARGET = Object.freeze({
  DEMAND_GENERATOR: [
    "DEMAND_GENERATOR",
    "EVENT_FUTURE_CYCLE",
    "LODGING_HOUSING",
    "SOURCE_AUTHORITY",
    "TRAINING_PROGRAM",
    "GOVERNMENT_PROJECT",
    "CORPORATE_MOBILIZATION",
    "SPORTS_HOUSING",
    "GENERAL_FOLLOWUP",
    "STOP",
  ],
  PROGRAM: [
    "EVENT_FUTURE_CYCLE",
    "LODGING_HOUSING",
    "SOURCE_AUTHORITY",
    "TRAINING_PROGRAM",
    "GOVERNMENT_PROJECT",
    "SPORTS_HOUSING",
    "DEMAND_GENERATOR",
    "GENERAL_FOLLOWUP",
    "STOP",
  ],
  PRIVATE_EVENT_VENUE: [
    "PRIVATE_EVENT_SIGNAL",
    "VENUE_PARTNERSHIP",
    "LODGING_HOUSING",
    "SOURCE_AUTHORITY",
    "GENERAL_FOLLOWUP",
    "STOP",
  ],
  OPPORTUNITY_CANDIDATE: [
    "EVENT_FUTURE_CYCLE",
    "LODGING_HOUSING",
    "GEO_VERIFICATION",
    "SOURCE_AUTHORITY",
    "DEMAND_GENERATOR",
    "GENERAL_FOLLOWUP",
    "STOP",
  ],
});

/**
 * @param {string} decisionType
 * @param {object} context
 * @returns {string[]}
 */
export function filterChoicesForContext(decisionType, context = {}) {
  const all = CHOICES[decisionType] || [];
  if (!all.length) return all;

  if (decisionType === "RESEARCH_PLAYBOOK") {
    const targetType = String(context.targetType || context.entityType || "");
    const programType = String(context.programType || "").toUpperCase();
    let allowed = PLAYBOOK_BY_TARGET[targetType] || null;
    if (!allowed && /SPORT|ATHLETIC|TOURNAMENT/.test(programType)) {
      allowed = [
        "SPORTS_HOUSING",
        "LODGING_HOUSING",
        "EVENT_FUTURE_CYCLE",
        "SOURCE_AUTHORITY",
        "GENERAL_FOLLOWUP",
        "STOP",
      ];
    }
    if (!allowed && /GOVERNMENT|FEDERAL|MUNICIPAL/.test(programType)) {
      allowed = [
        "GOVERNMENT_PROJECT",
        "EVENT_FUTURE_CYCLE",
        "LODGING_HOUSING",
        "SOURCE_AUTHORITY",
        "GENERAL_FOLLOWUP",
        "STOP",
      ];
    }
    if (allowed) {
      return all.filter((c) => allowed.includes(c));
    }
    // Default: drop PRIVATE_EVENT_SIGNAL unless venue-like
    return all.filter((c) => c !== "PRIVATE_EVENT_SIGNAL" || /VENUE|PRIVATE|EVENT/.test(targetType));
  }

  if (decisionType === "FOLLOWUP_TYPE") {
    const missing = new Set(
      (Array.isArray(context.missingFields) ? context.missingFields : []).map(String)
    );
    if (!missing.size) return all;
    const prefer = [];
    if (missing.has("lodgingEvidence") || missing.has("roomDemand")) prefer.push("LODGING", "ROOM_DEMAND");
    if (missing.has("futureCycle") || missing.has("eventDate")) prefer.push("FUTURE_CYCLE");
    if (missing.has("geo") || missing.has("geoEvidence")) prefer.push("GEO");
    if (missing.has("venue")) prefer.push("VENUE");
    if (missing.has("source") || missing.has("sourceAuthority")) prefer.push("SOURCE");
    if (missing.has("program")) prefer.push("PROGRAM");
    if (missing.has("partnership") || missing.has("partnerStatus")) prefer.push("PARTNERSHIP");
    prefer.push("NONE");
    const filtered = all.filter((c) => prefer.includes(c));
    return filtered.length >= 2 ? filtered : all;
  }

  return all;
}
