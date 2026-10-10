/**
 * Deterministic completion-potential scoring (not customer-facing).
 */

export const RESEARCH_PRIORITY = Object.freeze({
  P0_HIGH: "P0_HIGH",
  P1_MEDIUM: "P1_MEDIUM",
  P2_LOW: "P2_LOW",
});

const NOISE_RE =
  /\b(booking\.com|expedia|tripadvisor|trivago|indeed\.|jobs?\.|airbnb|vrbo|tempslibre\.ch)\b/i;
const LODGING_RE =
  /\b(room block|host hotel|housing|accommodation|hébergement|alojamiento|hotel block|preferred hotel|overflow|prestations hôtelières)\b/i;
const PROC_RE = /\b(rfp|tender|licitación|appel d['']offres|procurement|marché public)\b/i;
const FUTURE_RE = /\b(202[6-9]|203[0-2]|upcoming|annual|édition|biennial)\b/i;
const CONTACT_RE = /\/(contact|secretariat|procurement|about)/i;

export function scoreCompletionPotential(item = {}) {
  const title = String(item.title || item.eventProgram || "");
  const org = String(item.organization || "");
  const url = String(item.source || "");
  const blob = `${title} ${org} ${url} ${item.signalType || ""}`;
  let score = 0;
  const signals = [];

  if (NOISE_RE.test(blob)) {
    return {
      completionPotentialScore: -10,
      signals: ["noise"],
      researchPriority: RESEARCH_PRIORITY.P2_LOW,
    };
  }

  // Entity strength
  if (org && org.length > 4 && org.split(/\s+/).length >= 2 && !/^https?/i.test(org)) {
    score += 2;
    signals.push("entity_strength");
  }

  if (item.timingState && /CONFIRMED_|RECURRING|ROTATION/i.test(item.timingState)) {
    score += 3;
    signals.push("future_timing_hint");
  } else if (FUTURE_RE.test(blob) || item.existingEvidence?.nextKnownCycle) {
    score += 2;
    signals.push("future_timing_hint");
  }

  const lodgingHint =
    item.lodgingState === "HINT" ||
    LODGING_RE.test(blob) ||
    /\/(accommodation|housing)/i.test(url);
  if (lodgingHint) {
    score += 3;
    signals.push("lodging_hint");
  }

  if (item.signalType === "PRE_RFP" || PROC_RE.test(blob)) {
    score += 2;
    signals.push("procurement_signal");
  }

  if (item.signalType === "ROTATION_SERIES" || item.rotates) {
    score += 3;
    signals.push("repeat_rotation_evidence");
  }

  if (item.preRfp) {
    score += 2;
    signals.push("pre_rfp");
  }

  if (item.signalType === "COMPETITIVE_PATTERN") {
    score += 2;
    signals.push("competitive_pattern");
  }

  if (Number(item.defaultFitScore || 0) >= 50) {
    score += 1;
    signals.push("hotel_fit");
  }

  if (CONTACT_RE.test(url)) {
    score += 2;
    signals.push("public_contact_route");
  }

  // Source authority heuristic
  if (/\.(gov|int|edu|org)\b|un\.org|coe\.int|europa\.eu|palexpo/i.test(url)) {
    score += 2;
    signals.push("source_authority");
  } else if (url) {
    score += 1;
    signals.push("has_source");
  }

  // Market relevance — signal already hotel-scoped
  if (item.marketId) {
    score += 1;
    signals.push("market_relevance");
  }

  if (item.signalType === "PRE_RFP" || item.signalType === "ROTATION_SERIES") {
    score += 1;
    signals.push("named_buyer_or_series");
  }

  let researchPriority = RESEARCH_PRIORITY.P2_LOW;
  const strongTiming = /CONFIRMED_|RECURRING|ROTATION_PREDICTED/i.test(String(item.timingState || ""));
  if (
    (lodgingHint && score >= 6) ||
    (item.preRfp && score >= 7) ||
    (item.signalType === "ROTATION_SERIES" && strongTiming && score >= 7) ||
    (item.signalType === "COMPETITIVE_PATTERN" && score >= 6) ||
    score >= 10
  ) {
    researchPriority = RESEARCH_PRIORITY.P0_HIGH;
  } else if (score >= 5 || item.signalType === "ROTATION_SERIES" || item.preRfp) {
    researchPriority = RESEARCH_PRIORITY.P1_MEDIUM;
  }

  return { completionPotentialScore: score, signals, researchPriority };
}

export function applyBaselineScoring(items = []) {
  const scored = items.map((it) => {
    const s = scoreCompletionPotential(it);
    return {
      ...it,
      completionPotentialScore: s.completionPotentialScore,
      completionSignals: s.signals,
      researchPriority: s.researchPriority,
    };
  });
  scored.sort((a, b) => {
    const order = {
      [RESEARCH_PRIORITY.P0_HIGH]: 0,
      [RESEARCH_PRIORITY.P1_MEDIUM]: 1,
      [RESEARCH_PRIORITY.P2_LOW]: 2,
    };
    const d = order[a.researchPriority] - order[b.researchPriority];
    if (d !== 0) return d;
    // Prefer rotation / pre-RFP within band
    const boost = (x) =>
      (x.signalType === "PRE_RFP" ? 3 : 0) +
      (x.signalType === "ROTATION_SERIES" ? 2 : 0) +
      (x.signalType === "COMPETITIVE_PATTERN" ? 2 : 0);
    return b.completionPotentialScore + boost(b) - (a.completionPotentialScore + boost(a));
  });
  return {
    scored,
    tallies: {
      P0: scored.filter((x) => x.researchPriority === RESEARCH_PRIORITY.P0_HIGH).length,
      P1: scored.filter((x) => x.researchPriority === RESEARCH_PRIORITY.P1_MEDIUM).length,
      P2: scored.filter((x) => x.researchPriority === RESEARCH_PRIORITY.P2_LOW).length,
    },
  };
}
