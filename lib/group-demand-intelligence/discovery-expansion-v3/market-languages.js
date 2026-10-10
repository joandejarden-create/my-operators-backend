/**
 * GDI Discovery Expansion V3 — market language routing.
 * Hotel-agnostic resolver with curated profiles for pilot hotels.
 */

export const MARKET_LANGUAGE_PROFILES = Object.freeze({
  recrPQcZg7SFARRb2: {
    hotelKey: "YOTEL",
    label: "YOTEL Geneva Lake",
    market: "Geneva / Lake Geneva / La Côte",
    primaryLanguage: "fr",
    secondaryLanguages: ["en"],
    selectiveLanguages: ["de", "it"],
    feederMarkets: ["Geneva", "Vaud", "Lausanne", "France border", "European association HQ"],
    yieldPolicy: "PRIMARY_THEN_SECONDARY; selective only when primary yield supports or HQ evidence",
    rationale: "Local Swiss/French demand is primary; EN for intl associations; DE/IT selective",
  },
  rec2PVBDavppGpenm: {
    hotelKey: "AC",
    label: "AC Hotel A Coruña",
    market: "A Coruña / Galicia",
    primaryLanguage: "es",
    secondaryLanguages: ["gl", "en"],
    selectiveLanguages: [],
    feederMarkets: ["Madrid", "Barcelona", "Galicia", "Portugal", "European associations"],
    yieldPolicy: "PRIMARY es+gl local discovery; SECONDARY en control / intl congress",
    rationale: "Spanish + Galician are primary local demand languages; English is secondary control",
  },
  recUOyzOXn2Zdp98I: {
    hotelKey: "RADISSON",
    label: "Radisson Hotel Santo Domingo",
    market: "Santo Domingo / Naco–Tiradentes",
    primaryLanguage: "es",
    secondaryLanguages: ["en"],
    selectiveLanguages: [],
    feederMarkets: [
      "Distrito Nacional",
      "Piantini",
      "Caribbean regional",
      "US Hispanic corporate",
    ],
    yieldPolicy: "PRIMARY es Dominican/local institutional; SECONDARY en control / intl org",
    rationale: "Spanish-first Dominican demand sources; English secondary for intl associations",
  },
  recFaxTEFF9ILHWC9: {
    hotelKey: "WESTIN_MUC",
    label: "The Westin Grand München",
    market: "Munich / Arabellapark / Bogenhausen",
    primaryLanguage: "de",
    secondaryLanguages: ["en"],
    selectiveLanguages: [],
    feederMarkets: [
      "Munich",
      "Bavaria",
      "DACH corporate",
      "Messe München",
      "European associations",
    ],
    yieldPolicy: "PRIMARY de local/DACH discovery; SECONDARY en control / intl congress",
    rationale: "German is primary local demand language; English secondary control for international programs",
  },
  recKRJjcPnb4tVDDS: {
    hotelKey: "SPICE",
    label: "Spice Island Beach Resort",
    market: "Grenada / Grand Anse",
    primaryLanguage: "en",
    secondaryLanguages: [],
    selectiveLanguages: ["fr", "es"],
    feederMarkets: ["US East Coast", "Canada", "UK", "Caribbean regional"],
    yieldPolicy: "EN only unless regional FR/ES feeder evidence warrants",
    rationale: "English Caribbean luxury; FR/ES selective for regional demand",
  },
  recIwaP1etgx2g9nA: {
    hotelKey: "CAMBRIDGE",
    label: "Cambridge Beaches Resort & Spa",
    market: "Bermuda",
    primaryLanguage: "en",
    secondaryLanguages: [],
    selectiveLanguages: [],
    feederMarkets: ["New York", "Boston", "Toronto", "London", "US", "Canada", "UK"],
    yieldPolicy: "EN + feeder-source emphasis; no language for coverage alone",
    rationale: "Destination leisure/incentive with US/CA/UK feeders",
  },
  recGkME49yYuxQl0u: {
    hotelKey: "NOW_NOW",
    label: "NOW NOW NOHO",
    market: "New York / NoHo",
    primaryLanguage: "en",
    secondaryLanguages: [],
    selectiveLanguages: [],
    feederMarkets: ["US corporate", "international business", "association feeders"],
    yieldPolicy: "EN primary; multilingual only when source-market logic supports",
    rationale: "NYC urban boutique; intl discovery via EN sources first",
  },
});

/**
 * @param {{ hotelId?: string, hotelKey?: string, market?: string }} hotel
 * @param {string} [market]
 */
export function resolveGdiMarketLanguages(hotel = {}, market = null) {
  const hotelId = String(hotel.hotelId || hotel.id || "").trim();
  const hotelKey = String(hotel.hotelKey || hotel.key || "").trim().toUpperCase();
  let profile =
    MARKET_LANGUAGE_PROFILES[hotelId] ||
    Object.values(MARKET_LANGUAGE_PROFILES).find((p) => p.hotelKey === hotelKey) ||
    null;

  if (!profile) {
    profile = {
      hotelKey: hotelKey || "UNKNOWN",
      label: hotel.label || hotel.name || hotelId || "unknown",
      market: market || hotel.market || "unknown",
      primaryLanguage: "en",
      secondaryLanguages: [],
      selectiveLanguages: [],
      feederMarkets: [],
      yieldPolicy: "DEFAULT_EN_ONLY",
      rationale: "No curated profile — English default, no speculative multilingual",
    };
  }

  return {
    hotelId: hotelId || null,
    hotelKey: profile.hotelKey,
    market: market || profile.market,
    primaryLanguage: profile.primaryLanguage,
    secondaryLanguages: [...(profile.secondaryLanguages || [])],
    selectiveLanguages: [...(profile.selectiveLanguages || [])],
    feederMarkets: [...(profile.feederMarkets || [])],
    yieldPolicy: profile.yieldPolicy,
    rationale: profile.rationale,
  };
}

/**
 * Languages to actually query for a scout pass (adaptive).
 * Selective languages only when opts.allowSelective === true.
 */
export function languagesForScoutPass(langProfile, opts = {}) {
  const out = [langProfile.primaryLanguage];
  for (const l of langProfile.secondaryLanguages || []) {
    if (!out.includes(l)) out.push(l);
  }
  if (opts.allowSelective === true) {
    for (const l of langProfile.selectiveLanguages || []) {
      if (!out.includes(l)) out.push(l);
    }
  }
  return out;
}
