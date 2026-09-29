/**
 * GDI Evidence Gap Model V1 — deterministic unresolved dimensions + primary blocker.
 * Jev does not invent gaps; it only routes among permitted next actions.
 */

export const EVIDENCE_DIMENSION = Object.freeze({
  MARKET_VALIDATION: "MARKET_VALIDATION",
  FUTURE_CYCLE_VALIDATION: "FUTURE_CYCLE_VALIDATION",
  DATE_VALIDATION: "DATE_VALIDATION",
  ORGANIZER_VALIDATION: "ORGANIZER_VALIDATION",
  LODGING_CONTROL: "LODGING_CONTROL",
  HOUSING_STATUS: "HOUSING_STATUS",
  HOTEL_SELECTION_STATUS: "HOTEL_SELECTION_STATUS",
  COMMERCIAL_OPENNESS: "COMMERCIAL_OPENNESS",
  OVERFLOW_EVIDENCE: "OVERFLOW_EVIDENCE",
  ROOM_DEMAND: "ROOM_DEMAND",
  EVENT_SCALE: "EVENT_SCALE",
  HOTEL_FIT: "HOTEL_FIT",
  WHO_IDENTITY: "WHO_IDENTITY",
  WHO_ROLE: "WHO_ROLE",
  ACTION_PATH: "ACTION_PATH",
  SOURCE_AUTHORITY: "SOURCE_AUTHORITY",
  SOURCE_RECENCY: "SOURCE_RECENCY",
});

export const JEV_NEXT_ACTION = Object.freeze({
  VERIFY_MARKET: "VERIFY_MARKET",
  VERIFY_FUTURE_CYCLE: "VERIFY_FUTURE_CYCLE",
  VERIFY_DATES: "VERIFY_DATES",
  FIND_OFFICIAL_EVENT_PAGE: "FIND_OFFICIAL_EVENT_PAGE",
  FIND_OFFICIAL_HOUSING_PAGE: "FIND_OFFICIAL_HOUSING_PAGE",
  FIND_REGISTRATION_PAGE: "FIND_REGISTRATION_PAGE",
  FIND_TRAVEL_ACCOMMODATION_PAGE: "FIND_TRAVEL_ACCOMMODATION_PAGE",
  FIND_EVENT_MANUAL: "FIND_EVENT_MANUAL",
  FIND_TEAM_MANUAL: "FIND_TEAM_MANUAL",
  FIND_HOSTING_BID: "FIND_HOSTING_BID",
  FIND_FUTURE_HOST_PAGE: "FIND_FUTURE_HOST_PAGE",
  VERIFY_HOUSING_STATUS: "VERIFY_HOUSING_STATUS",
  VERIFY_HOTEL_SELECTION_STATUS: "VERIFY_HOTEL_SELECTION_STATUS",
  VERIFY_OVERFLOW: "VERIFY_OVERFLOW",
  VERIFY_ORGANIZER_CONTROL: "VERIFY_ORGANIZER_CONTROL",
  VERIFY_ROOM_DEMAND: "VERIFY_ROOM_DEMAND",
  VERIFY_EVENT_SCALE: "VERIFY_EVENT_SCALE",
  VERIFY_WHO: "VERIFY_WHO",
  VERIFY_WHO_ROLE: "VERIFY_WHO_ROLE",
  VERIFY_ACTION_PATH: "VERIFY_ACTION_PATH",
  WAIT_FOR_TRIGGER: "WAIT_FOR_TRIGGER",
  STOP_NO_PUBLIC_PATH: "STOP_NO_PUBLIC_PATH",
});

export const ACTION_OUTCOME = Object.freeze({
  BLOCKER_RESOLVED_POSITIVE: "BLOCKER_RESOLVED_POSITIVE",
  BLOCKER_RESOLVED_NEGATIVE: "BLOCKER_RESOLVED_NEGATIVE",
  BLOCKER_PARTIALLY_RESOLVED: "BLOCKER_PARTIALLY_RESOLVED",
  NO_NEW_EVIDENCE: "NO_NEW_EVIDENCE",
  WRONG_MARKET: "WRONG_MARKET",
  CURRENT_CYCLE_CLOSED: "CURRENT_CYCLE_CLOSED",
  FULLY_PLACED: "FULLY_PLACED",
  WAIT_TRIGGER: "WAIT_TRIGGER",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
});

export const JEV_VALUE_CLASS = Object.freeze({
  DECISIVE_POSITIVE: "DECISIVE_POSITIVE",
  DECISIVE_NEGATIVE: "DECISIVE_NEGATIVE",
  HELPFUL: "HELPFUL",
  SAME_AS_DEFAULT: "SAME_AS_DEFAULT",
  UNHELPFUL: "UNHELPFUL",
  WRONG_ROUTE: "WRONG_ROUTE",
});

export const STATE_AFTER = Object.freeze({
  CUSTOMER_READY: "CUSTOMER_READY",
  HIGH_QUALITY_WATCH: "HIGH_QUALITY_WATCH",
  FUTURE_WATCH: "FUTURE_WATCH",
  REJECTED: "REJECTED",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
});

const MARKET_TOKENS = {
  AC: ["a coruña", "coruña", "galicia", "santiago", "palexco", "expocoruña", "udc", "ferrol"],
  SPICE: ["grenada", "grand anse", "st george", "st. george", "carriacou", "spice island", "windward"],
};

/** Hard wrong-market URL/host patterns for Spice (from prior forensics). */
const SPICE_WRONG_HOST_RE =
  /digimarcon(losangeles|newyork|south)|travelprnews\.com|ispe\.org\/conferences\/2027-apac|torre.?melina|barcelona|los.?angeles|new.?york|apac|asia.?pacific|gainesville|vouvray|touraine/i;

const AC_WRONG_HOST_RE = /barcelona|madrid.?only|valencia|sevilla/i; // weak; prefer text tokens

export function validateMarket(candidate = {}, hotelShort = "") {
  const url = String(candidate.sourceUrl || "");
  const blob = `${candidate.event || ""} ${candidate.eventResolved || ""} ${url}`.toLowerCase();
  const tokens = MARKET_TOKENS[hotelShort] || [];

  if (hotelShort === "SPICE" && SPICE_WRONG_HOST_RE.test(url + " " + blob)) {
    return {
      ok: false,
      reason: "WRONG_MARKET",
      detail: "URL/event destination outside Grenada market",
      confidence: "HIGH",
    };
  }
  if (hotelShort === "AC" && /digimarcon|apac|asia.?pacific|new.?york|los.?angeles|grenada|barbados/i.test(blob)) {
    return {
      ok: false,
      reason: "WRONG_MARKET",
      detail: "non-Galicia destination signal",
      confidence: "HIGH",
    };
  }

  const hit = tokens.some((t) => blob.includes(t));
  if (hit) {
    return { ok: true, reason: "MARKET_OK", detail: "market token matched", confidence: "HIGH" };
  }
  // Organizer housing pages on .gal / coruna.gal for AC
  if (hotelShort === "AC" && /\.(gal)(\/|$)/i.test(url)) {
    return { ok: true, reason: "MARKET_OK", detail: "coruna.gal regional host", confidence: "HIGH" };
  }
  if (hotelShort === "AC" && /udc\.es|coruna|galicia|santiago.?2027|congresoend/i.test(blob + url)) {
    return { ok: true, reason: "MARKET_OK", detail: "Galicia event signal", confidence: "MEDIUM" };
  }
  // Uncertain — do not hard-reject; leave MARKET unresolved for VERIFY_MARKET
  if (hotelShort === "SPICE") {
    return {
      ok: false,
      reason: "WRONG_MARKET",
      detail: "no Grenada destination token on event/URL",
      confidence: "MEDIUM",
    };
  }
  return {
    ok: true,
    reason: "MARKET_UNCERTAIN",
    detail: "no hard wrong-market; verify via research",
    confidence: "LOW",
    needsVerify: true,
  };
}

/**
 * Deterministic evidence gap assessment from HQ watch row + evaluation.
 */
export function assessEvidenceGaps(candidate = {}, hotelShort = "") {
  const ev = candidate.evaluation || {};
  const resolved = [];
  const unresolved = [];

  const market = validateMarket(candidate, hotelShort);
  if (market.ok && !market.needsVerify) resolved.push(EVIDENCE_DIMENSION.MARKET_VALIDATION);
  else unresolved.push(EVIDENCE_DIMENSION.MARKET_VALIDATION);

  const eventName = candidate.eventResolved || candidate.event || "";
  if (eventName && eventName.length > 5 && !/^(aloxamento|convocatorias|chain:)/i.test(eventName)) {
    resolved.push(EVIDENCE_DIMENSION.FUTURE_CYCLE_VALIDATION);
  } else {
    unresolved.push(EVIDENCE_DIMENSION.FUTURE_CYCLE_VALIDATION);
  }

  if (/\b202[6-9]\b/.test(`${eventName} ${candidate.sourceUrl || ""} ${candidate.futureCycle || ""}`)) {
    resolved.push(EVIDENCE_DIMENSION.DATE_VALIDATION);
  } else {
    unresolved.push(EVIDENCE_DIMENSION.DATE_VALIDATION);
  }

  if (candidate.organizerResolved || ev.organizerControl === "ORGANIZER_CONTROLLED") {
    resolved.push(EVIDENCE_DIMENSION.ORGANIZER_VALIDATION);
  } else {
    unresolved.push(EVIDENCE_DIMENSION.ORGANIZER_VALIDATION);
  }

  if (["A", "B"].includes(ev.lodgingGrade) && ev.organizerControl === "ORGANIZER_CONTROLLED") {
    resolved.push(EVIDENCE_DIMENSION.LODGING_CONTROL);
  } else {
    unresolved.push(EVIDENCE_DIMENSION.LODGING_CONTROL);
  }

  // Housing status / hotel selection / commercial openness — usually the open gap for HQ watch
  const status = String(ev.commercialStatus || "");
  if (/TBD|FUTURE CYCLE NOT|RFP|OVERFLOW|OPEN \/ UNRESOLVED/i.test(status) && status !== "UNKNOWN") {
    unresolved.push(EVIDENCE_DIMENSION.HOUSING_STATUS);
    unresolved.push(EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS);
  } else if (status === "UNKNOWN") {
    unresolved.push(EVIDENCE_DIMENSION.HOUSING_STATUS);
    unresolved.push(EVIDENCE_DIMENSION.HOTEL_SELECTION_STATUS);
    unresolved.push(EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS);
  } else if (/PRIMARY HOTEL SELECTED \/ NO OVERFLOW|FULLY PLACED|CURRENT CYCLE CLOSED/i.test(status)) {
    resolved.push(EVIDENCE_DIMENSION.HOUSING_STATUS);
    resolved.push(EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS);
  } else {
    unresolved.push(EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS);
  }

  if (!/OVERFLOW/i.test(String(ev.lodgingRelationship || ""))) {
    unresolved.push(EVIDENCE_DIMENSION.OVERFLOW_EVIDENCE);
  } else {
    resolved.push(EVIDENCE_DIMENSION.OVERFLOW_EVIDENCE);
  }

  unresolved.push(EVIDENCE_DIMENSION.ROOM_DEMAND);
  unresolved.push(EVIDENCE_DIMENSION.EVENT_SCALE);
  unresolved.push(EVIDENCE_DIMENSION.WHO_IDENTITY);
  unresolved.push(EVIDENCE_DIMENSION.WHO_ROLE);
  unresolved.push(EVIDENCE_DIMENSION.ACTION_PATH);

  if (ev.surface && !/NOISE|OTA|DIRECTORY|NEARBY|BLOG/i.test(ev.surface)) {
    resolved.push(EVIDENCE_DIMENSION.SOURCE_AUTHORITY);
  } else {
    unresolved.push(EVIDENCE_DIMENSION.SOURCE_AUTHORITY);
  }

  // Dedupe
  const res = [...new Set(resolved)];
  const unr = [...new Set(unresolved)].filter((d) => !res.includes(d));

  return { resolvedDimensions: res, unresolvedDimensions: unr, market };
}

/**
 * One highest-value primary blocker.
 */
export function identifyPrimaryBlocker(gaps = {}, hotelShort = "") {
  const unr = gaps.unresolvedDimensions || [];
  if (unr.includes(EVIDENCE_DIMENSION.MARKET_VALIDATION)) {
    return {
      primary: EVIDENCE_DIMENSION.MARKET_VALIDATION,
      secondary: unr.find((d) => d !== EVIDENCE_DIMENSION.MARKET_VALIDATION) || null,
    };
  }
  if (unr.includes(EVIDENCE_DIMENSION.FUTURE_CYCLE_VALIDATION)) {
    return {
      primary: EVIDENCE_DIMENSION.FUTURE_CYCLE_VALIDATION,
      secondary: EVIDENCE_DIMENSION.DATE_VALIDATION,
    };
  }
  if (unr.includes(EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS)) {
    return {
      primary: EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS,
      secondary: unr.includes(EVIDENCE_DIMENSION.HOUSING_STATUS)
        ? EVIDENCE_DIMENSION.HOUSING_STATUS
        : EVIDENCE_DIMENSION.HOTEL_SELECTION_STATUS,
    };
  }
  if (unr.includes(EVIDENCE_DIMENSION.HOUSING_STATUS)) {
    return {
      primary: EVIDENCE_DIMENSION.HOUSING_STATUS,
      secondary: EVIDENCE_DIMENSION.LODGING_CONTROL,
    };
  }
  if (unr.includes(EVIDENCE_DIMENSION.LODGING_CONTROL)) {
    return {
      primary: EVIDENCE_DIMENSION.LODGING_CONTROL,
      secondary: EVIDENCE_DIMENSION.HOTEL_SELECTION_STATUS,
    };
  }
  if (unr.includes(EVIDENCE_DIMENSION.WHO_IDENTITY)) {
    return {
      primary: EVIDENCE_DIMENSION.WHO_IDENTITY,
      secondary: EVIDENCE_DIMENSION.ACTION_PATH,
    };
  }
  return {
    primary: unr[0] || EVIDENCE_DIMENSION.SOURCE_RECENCY,
    secondary: unr[1] || null,
  };
}

/**
 * Deterministic default action if Jev absent — maps primary blocker → action.
 */
export function defaultActionForBlocker(primaryBlocker, hotelShort = "") {
  const map = {
    [EVIDENCE_DIMENSION.MARKET_VALIDATION]: JEV_NEXT_ACTION.VERIFY_MARKET,
    [EVIDENCE_DIMENSION.FUTURE_CYCLE_VALIDATION]: JEV_NEXT_ACTION.VERIFY_FUTURE_CYCLE,
    [EVIDENCE_DIMENSION.DATE_VALIDATION]: JEV_NEXT_ACTION.VERIFY_DATES,
    [EVIDENCE_DIMENSION.ORGANIZER_VALIDATION]: JEV_NEXT_ACTION.FIND_OFFICIAL_EVENT_PAGE,
    [EVIDENCE_DIMENSION.LODGING_CONTROL]: JEV_NEXT_ACTION.VERIFY_ORGANIZER_CONTROL,
    [EVIDENCE_DIMENSION.HOUSING_STATUS]: JEV_NEXT_ACTION.FIND_OFFICIAL_HOUSING_PAGE,
    [EVIDENCE_DIMENSION.HOTEL_SELECTION_STATUS]: JEV_NEXT_ACTION.VERIFY_HOTEL_SELECTION_STATUS,
    [EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS]: JEV_NEXT_ACTION.VERIFY_HOUSING_STATUS,
    [EVIDENCE_DIMENSION.OVERFLOW_EVIDENCE]: JEV_NEXT_ACTION.VERIFY_OVERFLOW,
    [EVIDENCE_DIMENSION.ROOM_DEMAND]: JEV_NEXT_ACTION.VERIFY_ROOM_DEMAND,
    [EVIDENCE_DIMENSION.EVENT_SCALE]: JEV_NEXT_ACTION.VERIFY_EVENT_SCALE,
    [EVIDENCE_DIMENSION.WHO_IDENTITY]: JEV_NEXT_ACTION.VERIFY_WHO,
    [EVIDENCE_DIMENSION.WHO_ROLE]: JEV_NEXT_ACTION.VERIFY_WHO_ROLE,
    [EVIDENCE_DIMENSION.ACTION_PATH]: JEV_NEXT_ACTION.VERIFY_ACTION_PATH,
    [EVIDENCE_DIMENSION.SOURCE_AUTHORITY]: JEV_NEXT_ACTION.FIND_OFFICIAL_EVENT_PAGE,
    [EVIDENCE_DIMENSION.SOURCE_RECENCY]: JEV_NEXT_ACTION.VERIFY_FUTURE_CYCLE,
  };
  // Market-aware tweak: Spice prefers travel/accommodation page before generic housing
  if (
    hotelShort === "SPICE" &&
    (primaryBlocker === EVIDENCE_DIMENSION.HOUSING_STATUS ||
      primaryBlocker === EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS)
  ) {
    return JEV_NEXT_ACTION.FIND_TRAVEL_ACCOMMODATION_PAGE;
  }
  return map[primaryBlocker] || JEV_NEXT_ACTION.STOP_NO_PUBLIC_PATH;
}

export function isAllowedJevAction(action) {
  return Object.values(JEV_NEXT_ACTION).includes(action);
}
