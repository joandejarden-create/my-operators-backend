/**
 * CompleteDemandPacket — six pillars. SIGNAL ≠ RESEARCH LEAD ≠ PACKET ≠ READY.
 */

export const PACKET_PILLAR = Object.freeze({
  A_NAMED_DEMAND_ENTITY: "A_NAMED_DEMAND_ENTITY",
  B_DEFINED_GROUP_MOTION: "B_DEFINED_GROUP_MOTION",
  C_BUYER_ORGANIZER_PATH: "C_BUYER_ORGANIZER_PATH",
  D_FUTURE_DECISION_POINT: "D_FUTURE_DECISION_POINT",
  E_HOTEL_LODGING_EVIDENCE: "E_HOTEL_LODGING_EVIDENCE",
  F_TARGET_HOTEL_FIT: "F_TARGET_HOTEL_FIT",
});

export const PACKET_QUALITY = Object.freeze({
  COMPLETE_STRONG: "COMPLETE_STRONG",
  COMPLETE_PLAUSIBLE: "COMPLETE_PLAUSIBLE",
  PARTIAL_PACKET: "PARTIAL_PACKET",
  SIGNAL_ONLY: "SIGNAL_ONLY",
  REJECTED: "REJECTED",
  DUPLICATE: "DUPLICATE",
});

export const PILLAR_STRENGTH = Object.freeze({
  STRONG: "STRONG",
  PRESENT: "PRESENT",
  WEAK: "WEAK",
  MISSING: "MISSING",
});

const LISTICLE_RE =
  /\b(best (luxury )?hotels|top hotels|hotels?:|wedding wednesday|things to do|where to stay|photo gallery|deals? & reviews)\b/i;

const MOTION_RE =
  /\b(conference|congress|congrès|congreso|delegation|executive meeting|sales kickoff|training|investigator|advisory board|association|overflow|sports? team|tournament|production crew|workforce|government|tour|incentive|hotel block|room block|housing|kickoff|retreat|symposium)\b/i;

function blob(r = {}) {
  return [
    r.title,
    r.organizationName || r.organization,
    r.eventProgram,
    r.summaryWhat,
    r.hotelOpportunityThesis,
    r.fact,
    r.officialSource || r.source || r.url,
    r.signalType,
  ]
    .map((x) => String(x || ""))
    .join(" ");
}

function namedEntity(r = {}) {
  const org = String(r.organizationName || r.organization || "").trim();
  if (org.length < 6) return { strength: PILLAR_STRENGTH.MISSING, detail: "NO_ORG" };
  if (LISTICLE_RE.test(org)) return { strength: PILLAR_STRENGTH.MISSING, detail: "LISTICLE_ORG" };
  if (/^https?:/i.test(org)) return { strength: PILLAR_STRENGTH.MISSING, detail: "URL_AS_ORG" };
  if (
    /^(the\s+)?[\w\s&'-]+hotel\b/i.test(org) &&
    !/\b(association|society|congress|federation|foundation|agency|university)\b/i.test(org)
  ) {
    return { strength: PILLAR_STRENGTH.WEAK, detail: "HOTEL_NAME_AS_ORG" };
  }
  const tokens = org.split(/\s+/).filter(Boolean);
  if (tokens.length >= 3 || /\b(Association|Society|Federation|Foundation|University|Agency)\b/i.test(org)) {
    return { strength: PILLAR_STRENGTH.STRONG, detail: org };
  }
  if (tokens.length >= 2) return { strength: PILLAR_STRENGTH.PRESENT, detail: org };
  return { strength: PILLAR_STRENGTH.WEAK, detail: org };
}

function groupMotion(r = {}, b = "") {
  // Prefer structured traveling-entity / group-motion stamps over opportunityType labels
  const structuredType =
    r.groupMotionType || r.travelingEntityType || r.groupMotion || r.groupType || "";
  const structuredEvidence =
    r.groupMotionEvidence || r.travelingEntityEvidence || "";
  if (r.travelingEntityProven === true && (structuredType || structuredEvidence)) {
    return {
      strength: /DELEGATION|EXHIBITOR|SPONSOR|PRODUCTION|VENDOR/i.test(String(structuredType))
        ? PILLAR_STRENGTH.STRONG
        : PILLAR_STRENGTH.PRESENT,
      detail: structuredType || structuredEvidence || "TRAVELING_ENTITY_PROVEN",
    };
  }
  if (structuredType && structuredEvidence) {
    return {
      strength: PILLAR_STRENGTH.PRESENT,
      detail: structuredType,
    };
  }
  // Do not treat lifecycle labels (FUTURE_CYCLE / FUTURE_WATCH) as group motion
  const motion = /^(FUTURE_CYCLE|FUTURE_WATCH|PRIMARY_PURSUIT|WATCH)$/i.test(
    String(r.opportunityType || "")
  )
    ? structuredType
    : r.groupMotion || r.groupType || r.opportunityType || "";
  if (MOTION_RE.test(`${motion} ${b}`)) {
    const strong =
      /\b(hotel block|room block|overflow|delegation|investigator|sales kickoff|housing|official hotel)\b/i.test(
        b
      ) || /OVERFLOW|PRIMARY|HOUSING/i.test(String(motion));
    return {
      strength: strong ? PILLAR_STRENGTH.STRONG : PILLAR_STRENGTH.PRESENT,
      detail: motion || "GROUP_MOTION_INFERRED",
    };
  }
  if (/ROTATION_SERIES|PRE_RFP/i.test(String(r.signalType || ""))) {
    return { strength: PILLAR_STRENGTH.WEAK, detail: "SERIES_WITHOUT_MOTION" };
  }
  return { strength: PILLAR_STRENGTH.MISSING, detail: "NO_GROUP_MOTION" };
}

function buyerPath(r = {}, b = "") {
  if (r.buyerEntity || r.organizer || r.organizationContactUrl || r.publicContactPath || r.functionalContactEmail) {
    return {
      strength: r.publicContactPath || r.organizationContactUrl || r.functionalContactEmail
        ? PILLAR_STRENGTH.STRONG
        : PILLAR_STRENGTH.PRESENT,
      detail: r.buyerEntity || r.organizer || r.organizationName || "BUYER_PATH",
    };
  }
  if (/\b(organizer|organiser|secretariat|housing bureau|DMC|procurement|agency)\b/i.test(b)) {
    return { strength: PILLAR_STRENGTH.PRESENT, detail: "BUYER_LANGUAGE" };
  }
  if (namedEntity(r).strength === PILLAR_STRENGTH.STRONG) {
    return { strength: PILLAR_STRENGTH.WEAK, detail: "ORG_ONLY_NO_BUYER_PATH" };
  }
  return { strength: PILLAR_STRENGTH.MISSING, detail: "NO_BUYER" };
}

function futureDecision(r = {}, b = "") {
  if (r.eventStartDate && String(r.eventStartDate).slice(0, 4) >= "2026") {
    return { strength: PILLAR_STRENGTH.STRONG, detail: r.eventStartDate };
  }
  if (
    r.nextKnownCycle ||
    r.nextDecisionWindow ||
    r.futureCycleEvidenceState === "CURRENT_FUTURE_CYCLE_CONFIRMED"
  ) {
    return {
      strength: PILLAR_STRENGTH.STRONG,
      detail: r.nextKnownCycle || r.nextDecisionWindow || r.futureCycleEvidenceState,
    };
  }
  if (
    r.eventYear &&
    Number(r.eventYear) >= 2026 ||
    r.nextExpectedCycle ||
    /\b(202[6-9]|203[0-2]|site selection|RFP|housing opening|registration opening|host proposal|bidding)\b/i.test(
      b
    )
  ) {
    return {
      strength: PILLAR_STRENGTH.PRESENT,
      detail: r.eventYear || r.nextExpectedCycle || "FUTURE_WINDOW_HINT",
    };
  }
  if (/ROTATION|RECURRING|PRE_RFP/i.test(`${r.signalType} ${r.timingState}`)) {
    return { strength: PILLAR_STRENGTH.WEAK, detail: "RECURRENCE_WITHOUT_DECISION_POINT" };
  }
  return { strength: PILLAR_STRENGTH.MISSING, detail: "NO_FUTURE_DECISION" };
}

function lodgingEvidence(r = {}, b = "") {
  const evidence = String(r.evidenceClass || "");
  if (evidence === "DIRECT_CONFIRMED") {
    return { strength: PILLAR_STRENGTH.STRONG, detail: "DIRECT_COMP_USE" };
  }
  if (
    r.lodgingEvidence?.roomBlockMentioned ||
    /\b(official hotel|host hotel|hotel block|room block|housing program|hébergement officiel)\b/i.test(b)
  ) {
    return { strength: PILLAR_STRENGTH.STRONG, detail: "BLOCK_OR_OFFICIAL_HOTEL" };
  }
  if (
    evidence === "STRONG_ASSOCIATION" ||
    r.lodgingEvidence ||
    r.competitorHotel ||
    /\b(accommodation|housing|alojamiento|hébergement|crew hotel|workforce lodging)\b/i.test(b)
  ) {
    return { strength: PILLAR_STRENGTH.PRESENT, detail: "LODGING_HINT_OR_COMP" };
  }
  if (evidence === "WEAK_ASSOCIATION" || evidence === "DISCOVERY_ONLY") {
    return { strength: PILLAR_STRENGTH.WEAK, detail: evidence };
  }
  return { strength: PILLAR_STRENGTH.MISSING, detail: "NO_LODGING" };
}

function hotelFit(r = {}, opts = {}) {
  const fit = Number(r.hotelFitScore ?? r.defaultFitScore ?? opts.defaultFitScore ?? 0);
  const fitClass = r.fitClass || r.fitState || "";
  if (/STRONG_FIT/i.test(fitClass) || fit >= 55) {
    return { strength: PILLAR_STRENGTH.STRONG, detail: fitClass || String(fit) };
  }
  if (/PLAUSIBLE_FIT/i.test(fitClass) || fit >= 45) {
    return { strength: PILLAR_STRENGTH.PRESENT, detail: fitClass || String(fit) };
  }
  if (/WEAK_FIT|NO_FIT/i.test(fitClass) || (fit > 0 && fit < 45)) {
    return { strength: PILLAR_STRENGTH.WEAK, detail: fitClass || String(fit) };
  }
  // Default market hotels in pilot get PRESENT if geo matches
  if (opts.geoOk || r.market || r.eventLocationSummary) {
    return { strength: PILLAR_STRENGTH.PRESENT, detail: "MARKET_DEFAULT_FIT" };
  }
  return { strength: PILLAR_STRENGTH.MISSING, detail: "NO_FIT" };
}

function strengthRank(s) {
  return { STRONG: 3, PRESENT: 2, WEAK: 1, MISSING: 0 }[s] ?? 0;
}

/**
 * Evaluate six pillars and assign packet quality.
 */
export function evaluateCompleteDemandPacket(record = {}, opts = {}) {
  if (opts.isDuplicate) {
    return {
      quality: PACKET_QUALITY.DUPLICATE,
      pillars: {},
      strongCount: 0,
      presentOrStrongCount: 0,
      missingPillars: Object.values(PACKET_PILLAR),
      packet: null,
    };
  }

  const b = blob(record);
  const pillars = {
    [PACKET_PILLAR.A_NAMED_DEMAND_ENTITY]: namedEntity(record),
    [PACKET_PILLAR.B_DEFINED_GROUP_MOTION]: groupMotion(record, b),
    [PACKET_PILLAR.C_BUYER_ORGANIZER_PATH]: buyerPath(record, b),
    [PACKET_PILLAR.D_FUTURE_DECISION_POINT]: futureDecision(record, b),
    [PACKET_PILLAR.E_HOTEL_LODGING_EVIDENCE]: lodgingEvidence(record, b),
    [PACKET_PILLAR.F_TARGET_HOTEL_FIT]: hotelFit(record, opts),
  };

  const ranks = Object.values(pillars).map((p) => strengthRank(p.strength));
  const strongCount = ranks.filter((x) => x >= 3).length;
  const presentOrStrongCount = ranks.filter((x) => x >= 2).length;
  const missingPillars = Object.entries(pillars)
    .filter(([, p]) => p.strength === PILLAR_STRENGTH.MISSING || p.strength === PILLAR_STRENGTH.WEAK)
    .map(([k]) => k);

  // Reject garbage
  if (
    pillars[PACKET_PILLAR.A_NAMED_DEMAND_ENTITY].strength === PILLAR_STRENGTH.MISSING &&
    pillars[PACKET_PILLAR.B_DEFINED_GROUP_MOTION].strength === PILLAR_STRENGTH.MISSING
  ) {
    return {
      quality: PACKET_QUALITY.REJECTED,
      pillars,
      strongCount,
      presentOrStrongCount,
      missingPillars,
      packet: null,
      reason: "NO_ENTITY_NO_MOTION",
    };
  }

  let quality = PACKET_QUALITY.SIGNAL_ONLY;
  // COMPLETE_STRONG: ≥5 present/strong including A,B,D,E at least PRESENT, ≥3 STRONG
  const aOk = strengthRank(pillars[PACKET_PILLAR.A_NAMED_DEMAND_ENTITY].strength) >= 2;
  const bOk = strengthRank(pillars[PACKET_PILLAR.B_DEFINED_GROUP_MOTION].strength) >= 2;
  const cOk = strengthRank(pillars[PACKET_PILLAR.C_BUYER_ORGANIZER_PATH].strength) >= 2;
  const dOk = strengthRank(pillars[PACKET_PILLAR.D_FUTURE_DECISION_POINT].strength) >= 2;
  const eOk = strengthRank(pillars[PACKET_PILLAR.E_HOTEL_LODGING_EVIDENCE].strength) >= 2;
  const fOk = strengthRank(pillars[PACKET_PILLAR.F_TARGET_HOTEL_FIT].strength) >= 2;

  const coreComplete = aOk && bOk && dOk && eOk && fOk && cOk;
  const corePlausible = aOk && bOk && dOk && eOk && fOk; // buyer can be WEAK→PRESENT borderline

  if (coreComplete && strongCount >= 3 && presentOrStrongCount >= 6) {
    quality = PACKET_QUALITY.COMPLETE_STRONG;
  } else if (
    (coreComplete || (corePlausible && cOk)) &&
    presentOrStrongCount >= 5 &&
    strongCount >= 2
  ) {
    quality = PACKET_QUALITY.COMPLETE_PLAUSIBLE;
  } else if (corePlausible && presentOrStrongCount >= 5) {
    // buyer weak but other 5 present
    quality = PACKET_QUALITY.COMPLETE_PLAUSIBLE;
  } else if (presentOrStrongCount >= 3 || (aOk && (bOk || dOk || eOk))) {
    quality = PACKET_QUALITY.PARTIAL_PACKET;
  } else if (aOk || bOk || eOk) {
    quality = PACKET_QUALITY.SIGNAL_ONLY;
  } else {
    quality = PACKET_QUALITY.SIGNAL_ONLY;
  }

  // Rotation alone cannot be complete
  if (
    /ROTATION_SERIES/i.test(String(record.signalType || "")) &&
    !(cOk && eOk && dOk)
  ) {
    if (
      quality === PACKET_QUALITY.COMPLETE_STRONG ||
      quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE
    ) {
      quality = PACKET_QUALITY.PARTIAL_PACKET;
    }
  }

  const packet = {
    packetId: record.packetId || record.id || record.researchId || record.traceId,
    hotelKey: record.hotelKey,
    organization: record.organizationName || record.organization,
    groupMotion: pillars[PACKET_PILLAR.B_DEFINED_GROUP_MOTION].detail,
    buyerEntity: record.buyerEntity || record.organizer || null,
    futureDecision: pillars[PACKET_PILLAR.D_FUTURE_DECISION_POINT].detail,
    lodgingEvidence: pillars[PACKET_PILLAR.E_HOTEL_LODGING_EVIDENCE].detail,
    hotelFit: pillars[PACKET_PILLAR.F_TARGET_HOTEL_FIT].detail,
    quality,
    pillars,
    strongCount,
    presentOrStrongCount,
    missingPillars,
    source: record.officialSource || record.source || record.url,
    sourceFamily: record.sourceFamily || record.signalType || record.engine || "UNKNOWN",
    demandEngine: record.demandEngine || null,
    language: record.language || record.queryLanguage || "en",
    feederMarket: record.feederMarket || record.originMarket || null,
    buyerMarket: record.buyerMarket || record.originMarket || null,
    eventMarket: record.eventMarket || record.market || null,
    lodgingMarket: record.lodgingMarket || record.market || null,
    competitorHotel: record.competitorHotel || null,
    evidenceClass: record.evidenceClass || null,
  };

  return { quality, pillars, strongCount, presentOrStrongCount, missingPillars, packet };
}

export function isQualifiedForExpensiveCompletion(quality) {
  return (
    quality === PACKET_QUALITY.COMPLETE_STRONG ||
    quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE
  );
}

export function highPotentialPartial(evalResult = {}) {
  return (
    evalResult.quality === PACKET_QUALITY.PARTIAL_PACKET &&
    (evalResult.presentOrStrongCount || 0) >= 4
  );
}
