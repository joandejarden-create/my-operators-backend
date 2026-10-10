/**
 * Discovery admission gate — NOT customer readiness.
 * SIGNAL → CANDIDATE (worth completion research) → OPPORTUNITY (canonical gates).
 */

export const ADMISSION_CLASS = Object.freeze({
  SIGNAL_ONLY: "SIGNAL_ONLY",
  ADMITTED_CANDIDATE: "ADMITTED_CANDIDATE",
  VALID_FUTURE_WATCH_CANDIDATE: "VALID_FUTURE_WATCH_CANDIDATE",
  REJECTED_EARLY: "REJECTED_EARLY",
  DUPLICATE: "DUPLICATE",
});

export const ADMISSION_REASON = Object.freeze({
  VALID_ENTITY: "VALID_ENTITY",
  PLAUSIBLE_FUTURE_DEMAND: "PLAUSIBLE_FUTURE_DEMAND",
  TARGET_MARKET_RELEVANCE: "TARGET_MARKET_RELEVANCE",
  LODGING_HINT: "LODGING_HINT",
  BUYER_ORGANIZER_HINT: "BUYER_ORGANIZER_HINT",
  REPEAT_ROTATION_SIGNAL: "REPEAT_ROTATION_SIGNAL",
  PROCUREMENT_SIGNAL: "PROCUREMENT_SIGNAL",
  HOUSING_SIGNAL: "HOUSING_SIGNAL",
  KNOWN_GROUP_MOTION: "KNOWN_GROUP_MOTION",
  COMPETITOR_USE: "COMPETITOR_USE",
  EVENT_SERIES_TRAVEL: "EVENT_SERIES_TRAVEL",
  ENTITY_WEAK: "ENTITY_WEAK",
  TIMING_WEAK: "TIMING_WEAK",
  TOO_GENERIC: "TOO_GENERIC",
  NOT_GROUP_DEMAND: "NOT_ACTUALLY_GROUP_DEMAND",
  NO_BUYER: "NO_BUYER",
  NO_FUTURE_DECISION_POINT: "NO_FUTURE_DECISION_POINT",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  NOISE_DIRECTORY: "NOISE_DIRECTORY",
  HQ_ONLY: "ORGANIZATION_HQ_ONLY",
  GENERIC_CALENDAR: "GENERIC_EVENT_CALENDAR",
  GENERIC_CORPORATE_NEWS: "GENERIC_CORPORATE_NEWS",
  GENERIC_TOURISM: "GENERIC_TOURISM",
});

const NOISE_RE =
  /\b(booking\.com|expedia|tripadvisor|trivago|indeed\.|jobs?\.|airbnb|vrbo|tempslibre\.ch|vol pas cher|jettours)\b/i;
const LODGING_RE =
  /\b(room block|host hotel|housing|accommodation|hébergement|alojamiento|hotel block|preferred hotel|overflow|stay[- ]to[- ]play|prestations hôtelières)\b/i;
const HOUSING_URL_RE = /\/(accommodation|housing|hotels?|hébergement|alojamiento)\b/i;
const PROC_RE =
  /\b(rfp|tender|licitación|appel d['']offres|procurement|marché public|solicitation)\b/i;
const FUTURE_RE = /\b(202[6-9]|203[0-2]|upcoming|next (year|cycle)|édition|annual|biennial)\b/i;
const GROUP_RE =
  /\b(congress|congrès|conference|summit|meeting|kickoff|retreat|delegation|tournament|symposium|convention|expo|incentive|training)\b/i;
const SERIES_RE = /\b(annual|series|édition|rotat|biennial|recurring|congress)\b/i;
const HQ_RE = /\b(headquarters|corporate hq|about us|our offices|contact us only)\b/i;
const TOURISM_RE = /\b(things to do|tourist|visit (geneva|bermuda|grenada)|sightseeing)\b/i;
const CORP_NEWS_RE =
  /\b(raises funding|announces earnings|appoints ceo|stock price|press release)\b/i;

function blobOf(c = {}) {
  return [
    c.title,
    c.organizationName || c.organization,
    c.summaryWhat,
    c.officialSource || c.source || c.url,
    c.signalType,
    c.discoveryMeta?.scoutFamily,
    c.hotelOpportunityThesis,
  ]
    .map((x) => String(x || ""))
    .join(" ");
}

function hasValidEntity(c = {}) {
  const org = String(c.organizationName || c.organization || "").trim();
  const title = String(c.title || "").trim();
  if (org.length >= 4 && org.split(/\s+/).length >= 2 && !/^https?/i.test(org)) return true;
  if (title.length >= 8 && !NOISE_RE.test(title) && GROUP_RE.test(title)) return true;
  if (c.signalType === "ROTATION_SERIES" && (c.eventSeriesId || c.existingEvidence?.seriesId)) {
    return true;
  }
  return false;
}

function marketRelevant(c = {}, geoTokens = []) {
  if (!geoTokens?.length) return true; // hotel-scoped discovery assumed
  const b = blobOf(c).toLowerCase();
  if (geoTokens.some((t) => b.includes(String(t).toLowerCase()))) return true;
  if (c.eventLocationSummary || c.destinationStatus || c.discoveryMeta?.marketPlace) return true;
  if (c.signalType === "ROTATION_SERIES" || c.signalType === "PRE_RFP") return true;
  return false;
}

/**
 * @returns {{ ok, class, reasons, blockers, motionSignals }}
 */
export function isGdiDiscoveryCandidateWorthCompleting(candidate = {}, opts = {}) {
  const geoTokens = opts.geoTokens || [];
  const blob = blobOf(candidate);
  const reasons = [];
  const motionSignals = [];

  if (candidate.duplicateCanonical === true || opts.isDuplicate === true) {
    return {
      ok: false,
      class: ADMISSION_CLASS.DUPLICATE,
      reasons: ["DUPLICATE"],
      blockers: ["DUPLICATE"],
      motionSignals,
    };
  }

  if (NOISE_RE.test(blob)) {
    return {
      ok: false,
      class: ADMISSION_CLASS.REJECTED_EARLY,
      reasons: [ADMISSION_REASON.NOISE_DIRECTORY],
      blockers: ["NOISE"],
      motionSignals,
    };
  }

  if (TOURISM_RE.test(blob) && !LODGING_RE.test(blob)) {
    return {
      ok: false,
      class: ADMISSION_CLASS.REJECTED_EARLY,
      reasons: [ADMISSION_REASON.GENERIC_TOURISM],
      blockers: ["NOT_GROUP_DEMAND"],
      motionSignals,
    };
  }

  if (CORP_NEWS_RE.test(blob) && !GROUP_RE.test(blob)) {
    return {
      ok: false,
      class: ADMISSION_CLASS.REJECTED_EARLY,
      reasons: [ADMISSION_REASON.GENERIC_CORPORATE_NEWS],
      blockers: ["NOT_GROUP_DEMAND"],
      motionSignals,
    };
  }

  if (/HISTORICAL_ONLY/i.test(String(candidate.timingState || ""))) {
    return {
      ok: false,
      class: ADMISSION_CLASS.REJECTED_EARLY,
      reasons: [ADMISSION_REASON.HISTORICAL_ONLY],
      blockers: ["TIMING"],
      motionSignals,
    };
  }

  const entityOk = hasValidEntity(candidate);
  if (!entityOk) {
    return {
      ok: false,
      class: ADMISSION_CLASS.REJECTED_EARLY,
      reasons: [ADMISSION_REASON.ENTITY_WEAK],
      blockers: ["ENTITY"],
      motionSignals,
    };
  }
  reasons.push(ADMISSION_REASON.VALID_ENTITY);

  const futureOk =
    FUTURE_RE.test(blob) ||
    Boolean(candidate.eventStartDate || candidate.eventYear) ||
    /CONFIRMED_|RECURRING|ROTATION|FUTURE_UNCONFIRMED|PRE_RFP/i.test(
      String(candidate.timingState || candidate.signalType || "")
    ) ||
    candidate.signalType === "ROTATION_SERIES" ||
    candidate.preRfp === true;

  if (!futureOk) {
    return {
      ok: false,
      class: ADMISSION_CLASS.SIGNAL_ONLY,
      reasons: [ADMISSION_REASON.TIMING_WEAK, ADMISSION_REASON.NO_FUTURE_DECISION_POINT],
      blockers: ["TIMING"],
      motionSignals,
    };
  }
  reasons.push(ADMISSION_REASON.PLAUSIBLE_FUTURE_DEMAND);

  if (!marketRelevant(candidate, geoTokens)) {
    return {
      ok: false,
      class: ADMISSION_CLASS.SIGNAL_ONLY,
      reasons: ["MARKET_RELEVANCE_WEAK"],
      blockers: ["GEOGRAPHY"],
      motionSignals,
    };
  }
  reasons.push(ADMISSION_REASON.TARGET_MARKET_RELEVANCE);

  // At least one motion signal
  const url = String(candidate.officialSource || candidate.source || candidate.url || "");
  if (
    LODGING_RE.test(blob) ||
    candidate.lodgingEvidence ||
    candidate.lodgingState === "HINT" ||
    HOUSING_URL_RE.test(url)
  ) {
    motionSignals.push(ADMISSION_REASON.LODGING_HINT);
    if (HOUSING_URL_RE.test(url) || /housing|accommodation/i.test(blob)) {
      motionSignals.push(ADMISSION_REASON.HOUSING_SIGNAL);
    }
  }
  if (
    (candidate.organizationName || candidate.organization) &&
    String(candidate.organizationName || candidate.organization).split(/\s+/).length >= 2
  ) {
    motionSignals.push(ADMISSION_REASON.BUYER_ORGANIZER_HINT);
  }
  if (
    candidate.signalType === "ROTATION_SERIES" ||
    candidate.rotates ||
    SERIES_RE.test(blob) ||
    candidate.existingEvidence?.seriesId
  ) {
    motionSignals.push(ADMISSION_REASON.REPEAT_ROTATION_SIGNAL);
  }
  if (PROC_RE.test(blob) || candidate.signalType === "PRE_RFP" || /Procurement/i.test(String(candidate.discoveryMeta?.scoutFamily || ""))) {
    motionSignals.push(ADMISSION_REASON.PROCUREMENT_SIGNAL);
  }
  if (GROUP_RE.test(blob)) motionSignals.push(ADMISSION_REASON.KNOWN_GROUP_MOTION);
  if (candidate.signalType === "COMPETITIVE_PATTERN" || candidate.existingEvidence?.historicHotels) {
    motionSignals.push(ADMISSION_REASON.COMPETITOR_USE);
  }
  if (SERIES_RE.test(blob) && GROUP_RE.test(blob)) {
    motionSignals.push(ADMISSION_REASON.EVENT_SERIES_TRAVEL);
  }

  const uniqueMotion = [...new Set(motionSignals)];
  if (!uniqueMotion.length) {
    if (HQ_RE.test(blob)) {
      return {
        ok: false,
        class: ADMISSION_CLASS.REJECTED_EARLY,
        reasons: [ADMISSION_REASON.HQ_ONLY],
        blockers: ["NO_MOTION"],
        motionSignals: [],
      };
    }
    if (/calendar|agenda|manifestations/i.test(blob) && !LODGING_RE.test(blob)) {
      return {
        ok: false,
        class: ADMISSION_CLASS.SIGNAL_ONLY,
        reasons: [ADMISSION_REASON.GENERIC_CALENDAR, ADMISSION_REASON.TOO_GENERIC],
        blockers: ["NO_MOTION"],
        motionSignals: [],
      };
    }
    return {
      ok: false,
      class: ADMISSION_CLASS.SIGNAL_ONLY,
      reasons: [ADMISSION_REASON.TOO_GENERIC, ADMISSION_REASON.NOT_GROUP_DEMAND],
      blockers: ["NO_MOTION"],
      motionSignals: [],
    };
  }

  reasons.push(...uniqueMotion);

  // Watch-shaped: series/pre-RFP with future but weak lodging
  const watchShaped =
    (candidate.signalType === "ROTATION_SERIES" || candidate.preRfp) &&
    !uniqueMotion.includes(ADMISSION_REASON.LODGING_HINT) &&
    !uniqueMotion.includes(ADMISSION_REASON.HOUSING_SIGNAL);

  return {
    ok: true,
    class: watchShaped
      ? ADMISSION_CLASS.VALID_FUTURE_WATCH_CANDIDATE
      : ADMISSION_CLASS.ADMITTED_CANDIDATE,
    reasons,
    blockers: [],
    motionSignals: uniqueMotion,
  };
}
