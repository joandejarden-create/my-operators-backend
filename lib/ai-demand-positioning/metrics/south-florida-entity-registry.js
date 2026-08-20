/**
 * Governed South Florida hotel identity registry for ACI research.
 * RESEARCH ONLY — not a customer competitive set.
 * Census is not required. Generic phrases never enter benchmark research.
 */

export const ENTITY_RESOLUTION_VERSION = "adp_sf_entity_resolution_v2";

export const ENTITY_CLASSES = Object.freeze({
  CANONICAL_HOTEL: "CANONICAL_HOTEL",
  DUPLICATE_ALIAS: "DUPLICATE_ALIAS",
  NON_HOTEL_ENTITY: "NON_HOTEL_ENTITY",
  GENERIC_PHRASE: "GENERIC_PHRASE",
  VENUE_ONLY: "VENUE_ONLY",
  LOCATION: "LOCATION",
  BRAND_NOT_PROPERTY: "BRAND_NOT_PROPERTY",
  AMBIGUOUS: "AMBIGUOUS",
  UNRESOLVED: "UNRESOLVED",
});

const GENERIC_EXACT = [
  "best hotel",
  "top hotel",
  "upscale hotel",
  "luxury hotel",
  "luxury resort",
  "upscale resort",
  "family resort",
  "friendly hotel",
  "friendly resort",
  "waterfront hotel",
  "waterfront resort",
  "south florida hotel",
  "south florida resort",
  "top meeting hotel",
  "top upscale hotel",
  "top resort",
  "top waterfront hotel",
  "best family resort",
  "best romantic hotel",
  "best hilton hotel",
  "this hotel",
  "this resort",
  "the resort",
  "boutique hotel",
  "boutique resort",
  "conference hotel",
  "meeting hotel",
  "recommended hotel",
  "romantic hotel",
  "beach resort",
  "ocean resort",
  "oceanfront resort",
  "golf resort",
  "iconic resort",
  "standard resort",
  "suite hotel",
  "service hotel",
  "service resort",
  "feel hotel",
  "house hotel",
  "six hotel",
  "tree hotel",
  "premiere hotel",
  "popular family resort",
  "small upscale resort",
  "boutique waterfront hotel",
  "boutique beach resort",
  "romantic waterfront hotel",
  "waterfront dining resort",
  "luxury iconic resort",
  "friendly upscale hotel",
  "service business hotel",
  "important note hotel",
];

const VENUE_OR_CLUB = [
  "yacht club",
  "the yacht club",
  "country club",
  "harborside pool club",
  "the harborside pool club",
  "banyan bunch kids club",
  "the banyan bunch kids club",
  "kids club",
  "supper club",
  "racquet club",
  "presidents club",
  "private club",
  "harbor club",
  "harbor beach club",
  "local yacht club",
  "river yacht club",
  "shark river yacht club",
  "its club",
  "mizner country club",
  "boca greens country club",
  "broken sound club",
  "andrews country club",
  "addison reserve country club",
  "addison",
  "top private oceanfront club",
];

const LOCATION_ONLY = ["royal palm", "mizner park"];

const BRAND_ONLY = [
  "waldorf astoria",
  "residence inn",
  "fairfield inn",
  "hampton inn",
  "holiday inn",
  "hilton garden inn",
  "homewood suites",
  "embassy suites",
  "kimpton hotel",
  "four seasons hotel",
  "four seasons resort",
  "marriott inn",
  "hilton suites",
  "mandarin oriental",
];

/**
 * Canonical hotels with stable research IDs.
 * geography: boca_local | palm_beach_county | south_florida_coast | distant_florida
 */
export const SOUTH_FLORIDA_CANONICAL_HOTELS = Object.freeze([
  {
    entityId: "the_boca_raton",
    canonical: "The Boca Raton",
    market: "Boca Raton",
    geography: "boca_local",
    chainScale: "Luxury",
    propertyType: "destination_resort",
    identityConfidence: "HIGH",
    meetings: true,
    spa: true,
    oceanfront: false,
    aliases: [
      "the boca raton",
      "the boca raton (waldorf astoria)",
      "boca raton resort",
      "boca raton resort & club",
      "the boca raton resort",
      "the boca raton resort & club",
      "boca raton resort & club, a waldorf astoria resort",
      "boca raton resort & club (waldorf astoria)",
      "boca raton resort & club (now the boca raton)",
      "boca raton resort & club (the boca)",
      "the boca raton (formerly boca raton resort & club)",
      "the boca raton (formerly the boca raton resort & club)",
      "the boca raton (formerly boca raton resort)",
      "the boca raton (formerly the boca raton resort & club)",
      "formerly boca raton resort",
      "this waldorf astoria resort",
      "waldorf astoria resort",
      "boca resort",
      "the boca resort",
      "the boca raton yacht club",
    ],
  },
  {
    entityId: "boca_beach_club",
    canonical: "Boca Beach Club",
    market: "Boca Raton",
    geography: "boca_local",
    chainScale: "Luxury",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: false,
    spa: true,
    oceanfront: true,
    aliases: [
      "boca beach club",
      "the boca raton beach club",
      "boca beach resort",
      "boca beach club, a waldorf astoria resort",
      "the boca beach club, a waldorf astoria resort",
    ],
  },
  {
    entityId: "renaissance_boca_raton",
    canonical: "Renaissance Boca Raton Hotel",
    market: "Boca Raton",
    geography: "boca_local",
    chainScale: "Upper Upscale",
    propertyType: "urban_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    oceanfront: false,
    aliases: ["renaissance boca raton"],
  },
  {
    entityId: "marriott_boca_raton",
    canonical: "Boca Raton Marriott at Boca Center",
    market: "Boca Raton",
    geography: "boca_local",
    chainScale: "Upper Upscale",
    propertyType: "urban_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    oceanfront: false,
    aliases: ["marriott boca raton"],
  },
  {
    entityId: "wyndham_boca_raton",
    canonical: "Wyndham Boca Raton",
    market: "Boca Raton",
    geography: "boca_local",
    chainScale: "Upscale",
    propertyType: "urban_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    oceanfront: false,
    aliases: ["wyndham boca raton", "wyndham boca raton hotel"],
  },
  {
    entityId: "hilton_boca_raton_suites",
    canonical: "Hilton Boca Raton Suites",
    market: "Boca Raton",
    geography: "boca_local",
    chainScale: "Upscale",
    propertyType: "all_suites",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    oceanfront: false,
    aliases: ["hilton boca raton suites", "choose hilton boca raton suites"],
  },
  {
    entityId: "embassy_suites_boca",
    canonical: "Embassy Suites by Hilton Boca Raton",
    market: "Boca Raton",
    geography: "boca_local",
    chainScale: "Upscale",
    propertyType: "all_suites",
    identityConfidence: "MEDIUM",
    meetings: true,
    spa: false,
    oceanfront: false,
    aliases: ["choose embassy suites"],
  },
  {
    entityId: "seagate_hotel_delray",
    canonical: "The Seagate Hotel & Spa",
    market: "Delray Beach",
    geography: "palm_beach_county",
    chainScale: "Upper Upscale",
    propertyType: "boutique_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: true,
    oceanfront: false,
    aliases: ["seagate hotel", "the seagate hotel", "the seagate beach club", "seagate beach club", "the seagate hotel & spa"],
  },
  {
    entityId: "delray_sands_resort",
    canonical: "Delray Sands Resort",
    market: "Delray Beach",
    geography: "palm_beach_county",
    chainScale: "Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: false,
    spa: false,
    oceanfront: true,
    aliases: ["delray sands resort", "choose delray sands resort"],
  },
  {
    entityId: "colony_hotel_delray",
    canonical: "The Colony Hotel Delray Beach",
    market: "Delray Beach",
    geography: "palm_beach_county",
    chainScale: "Upper Upscale",
    propertyType: "boutique_hotel",
    identityConfidence: "HIGH",
    meetings: false,
    spa: false,
    oceanfront: false,
    aliases: ["the colony hotel"],
  },
  {
    entityId: "the_ray_hotel",
    canonical: "The Ray Hotel",
    market: "Delray Beach",
    geography: "palm_beach_county",
    chainScale: "Upper Upscale",
    propertyType: "boutique_hotel",
    identityConfidence: "HIGH",
    meetings: false,
    spa: false,
    oceanfront: false,
    aliases: ["the ray hotel"],
  },
  {
    entityId: "opal_grand_delray",
    canonical: "Opal Grand Oceanfront Resort",
    market: "Delray Beach",
    geography: "palm_beach_county",
    chainScale: "Upper Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: true,
    spa: true,
    oceanfront: true,
    aliases: ["opal grand", "opal grand oceanfront resort", "opal grand oceanfront resort & spa", "opal grand resort"],
  },
  {
    entityId: "eau_palm_beach",
    canonical: "Eau Palm Beach Resort & Spa",
    market: "Manalapan",
    geography: "palm_beach_county",
    chainScale: "Luxury",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: true,
    spa: true,
    oceanfront: true,
    aliases: ["eau palm beach resort", "the eau palm beach resort"],
  },
  {
    entityId: "four_seasons_palm_beach",
    canonical: "Four Seasons Resort Palm Beach",
    market: "Palm Beach",
    geography: "palm_beach_county",
    chainScale: "Luxury",
    propertyType: "beach_resort",
    identityConfidence: "MEDIUM",
    meetings: true,
    spa: true,
    oceanfront: true,
    aliases: ["the four seasons resort"],
  },
  {
    entityId: "tideline_ocean_resort",
    canonical: "Tideline Ocean Resort & Spa",
    market: "Palm Beach",
    geography: "palm_beach_county",
    chainScale: "Luxury",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: false,
    spa: true,
    oceanfront: true,
    aliases: ["tideline ocean resort", "the tideline palm beach ocean resort"],
  },
  {
    entityId: "brazilian_court",
    canonical: "The Brazilian Court Hotel",
    market: "Palm Beach",
    geography: "palm_beach_county",
    chainScale: "Luxury",
    propertyType: "boutique_hotel",
    identityConfidence: "HIGH",
    meetings: false,
    spa: false,
    oceanfront: false,
    aliases: ["the brazilian court hotel", "brazilian court hotel"],
  },
  {
    entityId: "diplomat_beach_resort",
    canonical: "The Diplomat Beach Resort Hollywood",
    market: "Hollywood",
    geography: "south_florida_coast",
    chainScale: "Upper Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: true,
    spa: true,
    oceanfront: true,
    aliases: [
      "the diplomat beach resort",
      "diplomat beach resort",
      "hilton diplomat beach resort",
      "the diplomat beach resort, hollywood",
      "the diplomat beach resort hollywood",
    ],
  },
  {
    entityId: "pelican_grand",
    canonical: "Pelican Grand Beach Resort",
    market: "Fort Lauderdale",
    geography: "south_florida_coast",
    chainScale: "Upper Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: false,
    spa: false,
    oceanfront: true,
    aliases: ["pelican grand beach resort"],
  },
  {
    entityId: "harbor_beach_marriott",
    canonical: "Fort Lauderdale Marriott Harbor Beach Resort & Spa",
    market: "Fort Lauderdale",
    geography: "south_florida_coast",
    chainScale: "Upper Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: true,
    spa: true,
    oceanfront: true,
    aliases: [
      "marriott harbor beach resort",
      "fort lauderdale marriott harbor beach resort",
      "marriott harbor beach resort & spa",
      "harbor beach marriott",
    ],
  },
  {
    entityId: "hilton_ftl_beach",
    canonical: "Hilton Fort Lauderdale Beach Resort",
    market: "Fort Lauderdale",
    geography: "south_florida_coast",
    chainScale: "Upper Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    oceanfront: true,
    aliases: ["hilton fort lauderdale beach resort"],
  },
  {
    entityId: "lago_mar",
    canonical: "Lago Mar Beach Resort & Club",
    market: "Fort Lauderdale",
    geography: "south_florida_coast",
    chainScale: "Upper Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: false,
    spa: false,
    oceanfront: true,
    aliases: ["lago mar beach resort"],
  },
  {
    entityId: "acqualina",
    canonical: "Acqualina Resort & Residences",
    market: "Sunny Isles",
    geography: "south_florida_coast",
    chainScale: "Luxury",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: true,
    spa: true,
    oceanfront: true,
    aliases: ["acqualina resort", "the acqualina resort"],
  },
  {
    entityId: "loews_miami_beach",
    canonical: "Loews Miami Beach Hotel",
    market: "Miami Beach",
    geography: "south_florida_coast",
    chainScale: "Upper Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    oceanfront: true,
    aliases: ["loews miami beach hotel"],
  },
  {
    entityId: "margaritaville_hollywood",
    canonical: "Margaritaville Hollywood Beach Resort",
    market: "Hollywood",
    geography: "south_florida_coast",
    chainScale: "Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    oceanfront: true,
    aliases: ["margaritaville hollywood beach resort", "margaritaville beach resort"],
  },
  {
    entityId: "carillon_miami",
    canonical: "Carillon Miami Wellness Resort",
    market: "Miami Beach",
    geography: "south_florida_coast",
    chainScale: "Luxury",
    propertyType: "wellness_resort",
    identityConfidence: "HIGH",
    meetings: false,
    spa: true,
    oceanfront: true,
    aliases: ["carillon miami wellness resort"],
  },
  {
    entityId: "amrit_ocean",
    canonical: "Amrit Ocean Resort & Residences",
    market: "Singer Island",
    geography: "palm_beach_county",
    chainScale: "Luxury",
    propertyType: "beach_resort",
    identityConfidence: "MEDIUM",
    meetings: false,
    spa: true,
    oceanfront: true,
    aliases: ["amrit ocean resort"],
  },
  {
    entityId: "jupiter_beach_resort",
    canonical: "Jupiter Beach Resort & Spa",
    market: "Jupiter",
    geography: "palm_beach_county",
    chainScale: "Upper Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: false,
    spa: true,
    oceanfront: true,
    aliases: ["jupiter beach resort"],
  },
  {
    entityId: "hawks_cay",
    canonical: "Hawks Cay Resort",
    market: "Duck Key",
    geography: "distant_florida",
    chainScale: "Upper Upscale",
    propertyType: "destination_resort",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    oceanfront: true,
    aliases: ["hawks cay resort", "cay resort"],
  },
  {
    entityId: "cheeca_lodge",
    canonical: "Cheeca Lodge & Spa",
    market: "Islamorada",
    geography: "distant_florida",
    chainScale: "Luxury",
    propertyType: "destination_resort",
    identityConfidence: "HIGH",
    meetings: false,
    spa: true,
    oceanfront: true,
    aliases: ["cheeca lodge"],
  },
  {
    entityId: "riverside_hotel",
    canonical: "Riverside Hotel Fort Lauderdale",
    market: "Fort Lauderdale",
    geography: "south_florida_coast",
    chainScale: "Upscale",
    propertyType: "urban_hotel",
    identityConfidence: "MEDIUM",
    meetings: false,
    spa: false,
    oceanfront: false,
    aliases: ["riverside hotel", "the riverside hotel"],
  },
  {
    entityId: "wyndham_deerfield",
    canonical: "Wyndham Deerfield Beach Resort",
    market: "Deerfield Beach",
    geography: "palm_beach_county",
    chainScale: "Upscale",
    propertyType: "beach_resort",
    identityConfidence: "HIGH",
    meetings: false,
    spa: false,
    oceanfront: true,
    aliases: ["wyndham deerfield beach resort"],
  },
  {
    entityId: "hilton_deerfield",
    canonical: "Hilton Deerfield Beach Resort",
    market: "Deerfield Beach",
    geography: "palm_beach_county",
    chainScale: "Upscale",
    propertyType: "beach_resort",
    identityConfidence: "MEDIUM",
    meetings: false,
    spa: false,
    oceanfront: true,
    aliases: ["hilton deerfield beach resort"],
  },
  {
    entityId: "faena_miami",
    canonical: "Faena Hotel Miami Beach",
    market: "Miami Beach",
    geography: "south_florida_coast",
    chainScale: "Luxury",
    propertyType: "boutique_hotel",
    identityConfidence: "HIGH",
    meetings: false,
    spa: false,
    oceanfront: true,
    aliases: ["faena hotel"],
  },
  {
    entityId: "palm_house",
    canonical: "Palm House Hotel Palm Beach",
    market: "Palm Beach",
    geography: "palm_beach_county",
    chainScale: "Luxury",
    propertyType: "boutique_hotel",
    identityConfidence: "MEDIUM",
    meetings: false,
    spa: false,
    oceanfront: false,
    aliases: ["palm house hotel"],
  },
  {
    entityId: "sailfish_marina",
    canonical: "Sailfish Marina Resort",
    market: "Palm Beach Shores",
    geography: "palm_beach_county",
    chainScale: "Upscale",
    propertyType: "marina_resort",
    identityConfidence: "MEDIUM",
    meetings: false,
    spa: false,
    oceanfront: false,
    aliases: ["sailfish marina resort"],
  },
]);

function norm(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/\*\*/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function hotelById(id) {
  return SOUTH_FLORIDA_CANONICAL_HOTELS.find((h) => h.entityId === id) || null;
}

export function classifyObservedEntity(rawName) {
  const n = norm(rawName);
  if (!n || n.length < 4) {
    return { class: ENTITY_CLASSES.GENERIC_PHRASE, canonical: null, entityId: null, identityOk: false };
  }
  if (GENERIC_EXACT.includes(n)) {
    return { class: ENTITY_CLASSES.GENERIC_PHRASE, canonical: null, entityId: null, identityOk: false, raw: rawName };
  }
  if (n === "beach club" || n === "the beach club") {
    return { class: ENTITY_CLASSES.AMBIGUOUS, canonical: null, entityId: null, identityOk: false, raw: rawName, note: "Could be Boca Beach Club or a venue" };
  }
  if (VENUE_OR_CLUB.includes(n) || (/\bclub$/i.test(n) && !/\b(hotel|resort)\b/i.test(n))) {
    return { class: ENTITY_CLASSES.VENUE_ONLY, canonical: null, entityId: null, identityOk: false, raw: rawName };
  }
  if (LOCATION_ONLY.includes(n)) {
    return { class: ENTITY_CLASSES.LOCATION, canonical: null, entityId: null, identityOk: false, raw: rawName };
  }

  for (const hotel of SOUTH_FLORIDA_CANONICAL_HOTELS) {
    const canon = hotel.canonical.toLowerCase();
    const exact = hotel.aliases.includes(n) || n === canon;
    const aliasContained = hotel.aliases.some((a) => a.length >= 12 && n.includes(a));
    const shortFormOfAlias = hotel.aliases.some((a) => a.length >= 12 && n.length >= 12 && a.includes(n));
    const canonContained = canon.length >= 12 && n.includes(canon);
    if (exact || aliasContained || shortFormOfAlias || canonContained) {
      return {
        class: exact ? ENTITY_CLASSES.CANONICAL_HOTEL : ENTITY_CLASSES.DUPLICATE_ALIAS,
        canonical: hotel.canonical,
        entityId: hotel.entityId,
        identityOk: true,
        hotel,
        raw: rawName,
        duplicateOf: n !== canon ? hotel.entityId : null,
      };
    }
  }

  if (n === "beach club" || n === "the beach club") {
    return { class: ENTITY_CLASSES.AMBIGUOUS, canonical: null, entityId: null, identityOk: false, raw: rawName, note: "Could be Boca Beach Club or a venue" };
  }
  if (n === "waldorf astoria" || n === "waldorf astoria resort" || BRAND_ONLY.includes(n)) {
    if (n === "waldorf astoria" || n === "waldorf astoria resort") {
      return {
        class: ENTITY_CLASSES.DUPLICATE_ALIAS,
        canonical: "The Boca Raton",
        entityId: "the_boca_raton",
        identityOk: true,
        hotel: hotelById("the_boca_raton"),
        raw: rawName,
        note: "Brand fragment mapped to The Boca Raton in this market only",
      };
    }
    return { class: ENTITY_CLASSES.BRAND_NOT_PROPERTY, canonical: null, entityId: null, identityOk: false, raw: rawName };
  }

  if (/\b(hotel|resort|inn|suites?|lodge)\b/i.test(n)) {
    return { class: ENTITY_CLASSES.UNRESOLVED, canonical: null, entityId: null, identityOk: false, raw: rawName };
  }
  return { class: ENTITY_CLASSES.UNRESOLVED, canonical: null, entityId: null, identityOk: false, raw: rawName };
}

export function canonicalizeToEntityId(rawName) {
  const c = classifyObservedEntity(rawName);
  return c.identityOk ? c.entityId : null;
}

export function entityEligibleForBenchmark(classification) {
  if (!classification?.identityOk || !classification.hotel) return false;
  const h = classification.hotel;
  return Boolean(h.canonical && h.entityId && h.market && h.identityConfidence !== "LOW");
}

export function classifyEntityUniverse(rawNames) {
  const rows = [];
  const byClass = {};
  const canonicalIds = new Set();
  let duplicatesMerged = 0;
  for (const name of rawNames || []) {
    const c = classifyObservedEntity(name);
    rows.push({ name, ...c });
    byClass[c.class] = (byClass[c.class] || 0) + 1;
    if (c.entityId) {
      if (canonicalIds.has(c.entityId)) duplicatesMerged += 1;
      canonicalIds.add(c.entityId);
    }
  }
  const artifacts = (rawNames || []).filter((n) => {
    const c = classifyObservedEntity(n);
    return [ENTITY_CLASSES.GENERIC_PHRASE, ENTITY_CLASSES.VENUE_ONLY, ENTITY_CLASSES.LOCATION, ENTITY_CLASSES.NON_HOTEL_ENTITY].includes(c.class);
  });
  return {
    version: ENTITY_RESOLUTION_VERSION,
    rawEntities: (rawNames || []).length,
    canonicalHotels: canonicalIds.size,
    duplicatesMerged,
    artifactsRemoved: artifacts.length,
    ambiguous: byClass[ENTITY_CLASSES.AMBIGUOUS] || 0,
    unresolved: byClass[ENTITY_CLASSES.UNRESOLVED] || 0,
    byClass,
    rows,
    canonicalIds: [...canonicalIds],
  };
}
