/**
 * GDI Location Status Ontology V2 — Santo Domingo geography resolution.
 * Absence of venue ≠ VENUE_TBD. Affirmative evidence required for TBD states.
 */

export const LOCATION_STATUS = Object.freeze({
  SUBMARKET_KNOWN: "SUBMARKET_KNOWN",
  METRO_WIDE: "METRO_WIDE",
  VENUE_TBD: "VENUE_TBD",
  HOTEL_TBD: "HOTEL_TBD",
  LOCATION_UNKNOWN_RESEARCH_INCOMPLETE: "LOCATION_UNKNOWN_RESEARCH_INCOMPLETE",
  LOCATION_UNKNOWN_PUBLIC_DATA_CEILING: "LOCATION_UNKNOWN_PUBLIC_DATA_CEILING",
  WRONG_MARKET: "WRONG_MARKET",
});

export const V1_HOLD_REASON = Object.freeze({
  TRUE_GEO_NONFIT: "TRUE_GEO_NONFIT",
  SUBMARKET_NOT_FOUND: "SUBMARKET_NOT_FOUND",
  GENUINE_VENUE_TBD: "GENUINE_VENUE_TBD",
  GENUINE_HOTEL_TBD: "GENUINE_HOTEL_TBD",
  LOCATION_RESEARCH_INCOMPLETE: "LOCATION_RESEARCH_INCOMPLETE",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
});

/** Affirmative TBD lexicon (EN + ES) — absence alone is insufficient. */
export const AFFIRMATIVE_TBD_PATTERNS = Object.freeze({
  venueTbd: [
    /venue\s*(to\s*be\s*)?(announced|confirmed|determined|tba|tbd)/i,
    /sede\s*(por\s*determinar|a\s*confirmar|pendiente|ser[aá]\s*anunciada)/i,
    /location\s*(tba|tbd|to\s*be\s*announced|forthcoming)/i,
    /lugar\s*(por\s*definir|a\s*anunciar|pendiente)/i,
    /hosting\s*(city|venue)\s*(pending|tbd|selection)/i,
    /future\s*host\s*(selection|pending|tbd)/i,
  ],
  hotelTbd: [
    /hotel\s*(to\s*be\s*)?(announced|confirmed|tba|tbd|pending)/i,
    /hotel\s*(oficial\s*)?(por\s*determinar|a\s*confirmar|pendiente)/i,
    /host\s*hotel\s*(pending|tba|tbd|forthcoming|not\s*yet)/i,
    /housing\s*(not\s*yet\s*open|forthcoming|coming\s*soon|to\s*follow|pending)/i,
    /alojamiento\s*(por\s*anunciar|pr[oó]ximamente|pendiente|a\s*confirmar)/i,
    /accommodation\s*(details\s*)?(to\s*follow|forthcoming|coming\s*soon)/i,
    /room\s*block\s*(not\s*yet|pending|tba)/i,
    /\brfp\b.*hotel|hotel.*\brfp\b/i,
    /site\s*selection\s*(underway|in\s*progress|open)/i,
  ],
  metroWide: [
    /citywide/i,
    /across\s*(the\s*)?(city|metro|capital)/i,
    /multiple\s*(venues|hotels|locations)\s*(across|throughout)/i,
    /varios\s*(hoteles|sedes|lugares)\s*(en|a\s*trav[eé]s)/i,
    /en\s*toda\s*(la\s*)?(ciudad|capital)/i,
    /metro[- ]?wide/i,
    /distributed\s*(across|throughout)\s*santo\s*domingo/i,
  ],
});

const WRONG_MARKET_STRONG = [
  /\bpuerto rico\b/i,
  /\bpuertorrique[nñ]o\b/i,
  /\bsan juan\b/i,
  /\bm[eé]xico\b(?!.*santo domingo)/i,
  /\bgermany\b|\balemania\b/i,
  /\baustralia\b/i,
  /\bvenezuela\b(?!.*santo domingo)/i,
  /\bkarol\s*g\b/i,
  /\bbad\s*bunny\b/i,
  /\bempleos\s*de\s*alojamiento\b/i,
  /\b\d+\s*empleos\b/i,
  /\blinkedin\.com\/jobs\b/i,
  /alojamiento\s*de\s*portales\s*gubernamentales/i,
  /tipos\s*de\s*planes\s*de\s*alojamiento/i,
  /\bcataloniahotels\.com\b/i,
  /\btabla\s*de\s*posiciones\b/i,
  /\bsophia-anphis\.es\b/i,
  /\bphoto\s+by\s+humanidades\b/i,
  /\bpartidos\s+amistosos\b/i,
  /\bviajando\s+por\s+el\s+mundo\b/i,
];

const SD_LIVE_RE =
  /\bsanto domingo\b|\bdistrito nacional\b|\bpiantini\b|\bnaco\b|\btiradentes\b|\bblue mall\b/i;

const EVENT_LANGUAGE_RE =
  /congreso|conference|symposium|foro|meeting|jornadas|summit|convention|asamblea|reunion\s+anual|reunión\s+anual|turismo\s+de\s+salud|hotel\s+oficial|host\s+hotel|room\s+block|alojamiento\s+oficial/i;

const SUBMARKET_PATTERNS = [
  { label: "Piantini / Blue Mall", re: /\bpiantini\b|\bblue mall\b|\bwinston churchill\b|\bac[oó]polis\b|\bcidac\b/i },
  { label: "Naco / Tiradentes", re: /\bnaco\b|\btiradentes\b|\bpresidente gonz/i },
  { label: "Zona Colonial", re: /\bzona colonial\b|\bciudad colonial\b/i },
  { label: "Malecón", re: /\bmalec[oó]n\b/i },
  { label: "Bella Vista", re: /\bbella vista\b/i },
  { label: "Santo Domingo Este", re: /\bsanto domingo este\b/i },
];

export function hasAffirmativeVenueTbd(text = "") {
  return AFFIRMATIVE_TBD_PATTERNS.venueTbd.some((p) => p.test(text));
}

export function hasAffirmativeHotelTbd(text = "") {
  return AFFIRMATIVE_TBD_PATTERNS.hotelTbd.some((p) => p.test(text));
}

export function hasAffirmativeMetroWide(text = "") {
  return AFFIRMATIVE_TBD_PATTERNS.metroWide.some((p) => p.test(text));
}

export function detectSubmarketFromText(text = "") {
  for (const s of SUBMARKET_PATTERNS) {
    if (s.re.test(text)) return s.label;
  }
  return null;
}

export function isWrongMarketEvidence(text = "", url = "") {
  const blob = `${text} ${url}`;
  return WRONG_MARKET_STRONG.some((p) => p.test(blob));
}

/**
 * Classify location status from evidence.
 * @param {object} opts
 * @param {string} opts.text - research / page text (do NOT inject geography stamp here)
 * @param {string} [opts.seedText] - title + organization only (trusted over fetch noise)
 * @param {string} [opts.url]
 * @param {string} [opts.geography] - prior destination label (stamp; never overrides seed wrong-market)
 * @param {boolean} [opts.santoDomingoConfirmed]
 * @param {boolean} [opts.researchAttempted]
 * @param {boolean} [opts.researchExhausted]
 */
export function classifyLocationStatusV2(opts = {}) {
  const text = String(opts.text || "");
  const seedText = String(opts.seedText || "");
  const url = String(opts.url || "");
  const geography = String(opts.geography || "");
  // Live research blob must not include geography stamp — stamp is separate.
  const liveBlob = `${text} ${url}`;
  const seedBlob = seedText || text;
  const blob = `${text} ${geography} ${url}`;
  const evidence = [];

  // Seed title/org wrong-market cannot be rescued by geography stamp or fetch noise
  if (
    seedText &&
    isWrongMarketEvidence(seedText, "") &&
    !SD_LIVE_RE.test(seedText)
  ) {
    return {
      locationStatus: LOCATION_STATUS.WRONG_MARKET,
      locationConfidence: "HIGH",
      locationEvidence: ["wrong_market_seed_tokens"],
      submarket: null,
      santoDomingoConfirmed: false,
    };
  }

  // Strong wrong-market tokens in live text win unless live SD is also present
  if (isWrongMarketEvidence(liveBlob, url)) {
    const liveMentionsSd = SD_LIVE_RE.test(liveBlob);
    if (!liveMentionsSd) {
      return {
        locationStatus: LOCATION_STATUS.WRONG_MARKET,
        locationConfidence: "HIGH",
        locationEvidence: ["wrong_market_tokens"],
        submarket: null,
        santoDomingoConfirmed: false,
      };
    }
    // Fetch noise mentioned SD alongside wrong-market seed → still wrong market
    if (seedText && isWrongMarketEvidence(seedText, "") && !SD_LIVE_RE.test(seedText)) {
      return {
        locationStatus: LOCATION_STATUS.WRONG_MARKET,
        locationConfidence: "HIGH",
        locationEvidence: ["wrong_market_seed_over_fetch_noise"],
        submarket: null,
        santoDomingoConfirmed: false,
      };
    }
  }

  const sub = detectSubmarketFromText(liveBlob) || detectSubmarketFromText(blob);
  const sdConfirmedLive = SD_LIVE_RE.test(liveBlob);
  const sdConfirmed =
    opts.santoDomingoConfirmed === true ||
    sdConfirmedLive ||
    // Prior geography stamp only when seed looks like a real SD-eligible event
    (/\bsanto domingo\b/i.test(geography) &&
      sdConfirmedLive === false &&
      !isWrongMarketEvidence(seedBlob, "") &&
      EVENT_LANGUAGE_RE.test(seedBlob));

  if (sub && (sdConfirmedLive || sdConfirmed)) {
    evidence.push(`submarket:${sub}`);
    if (hasAffirmativeHotelTbd(blob) && !hasAffirmativeVenueTbd(blob)) {
      evidence.push("affirmative_hotel_tbd");
      return {
        locationStatus: LOCATION_STATUS.HOTEL_TBD,
        locationConfidence: "HIGH",
        locationEvidence: evidence,
        submarket: sub,
        santoDomingoConfirmed: true,
      };
    }
    return {
      locationStatus: LOCATION_STATUS.SUBMARKET_KNOWN,
      locationConfidence: "HIGH",
      locationEvidence: evidence,
      submarket: sub,
      santoDomingoConfirmed: true,
    };
  }

  if (!sdConfirmed) {
    if (opts.researchExhausted || isWrongMarketEvidence(liveBlob, url)) {
      return {
        locationStatus: LOCATION_STATUS.WRONG_MARKET,
        locationConfidence: "MEDIUM",
        locationEvidence: isWrongMarketEvidence(liveBlob, url)
          ? ["wrong_market_tokens"]
          : ["santo_domingo_not_confirmed_after_research"],
        submarket: null,
        santoDomingoConfirmed: false,
      };
    }
    return {
      locationStatus: LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE,
      locationConfidence: "LOW",
      locationEvidence: ["santo_domingo_unconfirmed"],
      submarket: null,
      santoDomingoConfirmed: false,
    };
  }

  if (hasAffirmativeVenueTbd(blob)) {
    evidence.push("affirmative_venue_tbd");
    return {
      locationStatus: LOCATION_STATUS.VENUE_TBD,
      locationConfidence: "HIGH",
      locationEvidence: evidence,
      submarket: null,
      santoDomingoConfirmed: true,
    };
  }

  if (hasAffirmativeHotelTbd(blob)) {
    evidence.push("affirmative_hotel_tbd");
    return {
      locationStatus: LOCATION_STATUS.HOTEL_TBD,
      locationConfidence: "MEDIUM",
      locationEvidence: evidence,
      submarket: geography.includes("Piantini")
        ? "Piantini / Blue Mall"
        : geography.includes("Naco")
          ? "Naco / Tiradentes"
          : null,
      santoDomingoConfirmed: true,
    };
  }

  if (hasAffirmativeMetroWide(blob)) {
    evidence.push("affirmative_metro_wide");
    return {
      locationStatus: LOCATION_STATUS.METRO_WIDE,
      locationConfidence: "MEDIUM",
      locationEvidence: evidence,
      submarket: null,
      santoDomingoConfirmed: true,
    };
  }

  // Santo Domingo confirmed, no precise submarket, no affirmative TBD
  if (opts.researchExhausted) {
    return {
      locationStatus: LOCATION_STATUS.LOCATION_UNKNOWN_PUBLIC_DATA_CEILING,
      locationConfidence: "MEDIUM",
      locationEvidence: ["bounded_research_exhausted_no_submarket_no_affirmative_tbd"],
      submarket: null,
      santoDomingoConfirmed: true,
    };
  }

  if (opts.researchAttempted) {
    return {
      locationStatus: LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE,
      locationConfidence: "LOW",
      locationEvidence: ["sd_confirmed_submarket_still_missing"],
      submarket: null,
      santoDomingoConfirmed: true,
    };
  }

  return {
    locationStatus: LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE,
    locationConfidence: "LOW",
    locationEvidence: ["not_yet_researched_for_location"],
    submarket: null,
    santoDomingoConfirmed: true,
  };
}

/**
 * Whether location status may enter hotel-fit evaluation (geo gate V2).
 */
export function locationStatusAllowsHotelFit(locationStatus, opts = {}) {
  const s = locationStatus;
  if (s === LOCATION_STATUS.SUBMARKET_KNOWN) return true;
  if (s === LOCATION_STATUS.METRO_WIDE) return true;
  if (s === LOCATION_STATUS.VENUE_TBD) return true;
  if (s === LOCATION_STATUS.HOTEL_TBD) return true;
  if (s === LOCATION_STATUS.LOCATION_UNKNOWN_PUBLIC_DATA_CEILING) {
    // Cautious: only if SD confirmed + commercial open + no incompatible location
    return (
      opts.santoDomingoConfirmed === true &&
      opts.commercialOpen === true &&
      opts.incompatibleLocation !== true
    );
  }
  if (s === LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE) return false;
  if (s === LOCATION_STATUS.WRONG_MARKET) return false;
  return false;
}

/**
 * Map location status → default geographic applicability when precise geo unavailable.
 */
export function defaultGeoApplicabilityForLocationStatus(locationStatus) {
  switch (locationStatus) {
    case LOCATION_STATUS.VENUE_TBD:
    case LOCATION_STATUS.METRO_WIDE:
    case LOCATION_STATUS.HOTEL_TBD:
    case LOCATION_STATUS.LOCATION_UNKNOWN_PUBLIC_DATA_CEILING:
      return "PLAUSIBLE";
    case LOCATION_STATUS.SUBMARKET_KNOWN:
      return null; // use precise matching
    default:
      return "UNKNOWN";
  }
}

export function mapV1HoldReason(locationStatus, v1Final = "NOT_APPLICABLE") {
  if (v1Final !== "NOT_APPLICABLE" && v1Final !== "UNKNOWN") {
    return null;
  }
  switch (locationStatus) {
    case LOCATION_STATUS.SUBMARKET_KNOWN:
      return V1_HOLD_REASON.SUBMARKET_NOT_FOUND; // was missing in V1, now found
    case LOCATION_STATUS.VENUE_TBD:
      return V1_HOLD_REASON.GENUINE_VENUE_TBD;
    case LOCATION_STATUS.HOTEL_TBD:
      return V1_HOLD_REASON.GENUINE_HOTEL_TBD;
    case LOCATION_STATUS.METRO_WIDE:
      return V1_HOLD_REASON.SUBMARKET_NOT_FOUND;
    case LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE:
      return V1_HOLD_REASON.LOCATION_RESEARCH_INCOMPLETE;
    case LOCATION_STATUS.LOCATION_UNKNOWN_PUBLIC_DATA_CEILING:
      return V1_HOLD_REASON.PUBLIC_DATA_CEILING;
    case LOCATION_STATUS.WRONG_MARKET:
      return V1_HOLD_REASON.TRUE_GEO_NONFIT;
    default:
      return V1_HOLD_REASON.SUBMARKET_NOT_FOUND;
  }
}

export function decideLocationJevAction(locationStatus, opts = {}) {
  if (locationStatus === LOCATION_STATUS.WRONG_MARKET) {
    return { action: "STOP_NO_PUBLIC_PATH", rationale: "Wrong market" };
  }
  if (
    locationStatus === LOCATION_STATUS.SUBMARKET_KNOWN ||
    locationStatus === LOCATION_STATUS.VENUE_TBD ||
    locationStatus === LOCATION_STATUS.HOTEL_TBD ||
    locationStatus === LOCATION_STATUS.METRO_WIDE
  ) {
    return { action: "STOP_NO_PUBLIC_PATH", rationale: "Location state resolved" };
  }
  if (locationStatus === LOCATION_STATUS.LOCATION_UNKNOWN_PUBLIC_DATA_CEILING) {
    return { action: "STOP_NO_PUBLIC_PATH", rationale: "Public data ceiling" };
  }
  // RESEARCH_INCOMPLETE — pick highest-value action
  if (opts.missingOfficialPage) {
    return {
      action: "FIND_OFFICIAL_EVENT_PAGE",
      rationale: "Need official page for venue/location",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  if (opts.ambiguousTbd) {
    return {
      action: "VERIFY_VENUE",
      rationale: "Distinguish TBD vs missing research",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  return {
    action: "VERIFY_EVENT_LOCATION",
    rationale: "Resolve submarket or affirmative TBD",
    maxQueries: 3,
    maxFetches: 5,
  };
}
