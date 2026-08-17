/**
 * Wave 15 Stage 5 — supplemental Americas / CALA openings and property pages
 * for Hilton brands whose Stage 4 tab-factory openings alone are fewer than 3.
 *
 * All URLs below are verified official hilton.com/en/hotels/{code}/ property
 * pages discovered via public search (2026). CALA-first: when a CALA Hilton
 * property exists for the brand it is used before U.S. International Reference
 * fallbacks. Sibling brand imagery / URLs are never used across brands.
 *
 * No slug in the Wave 15 eight is pre-designated as held (unlike Wave 14 Four
 * Points Flex by Sheraton) — property openings coverage should be complete
 * once tab-factory openings + this supplemental list are combined.
 */
export const WAVE15_IMAGE_SUPPLEMENTAL_VERSION = "wave15-image-supplemental-v1";

/** No Wave 15 slug is held pending Stage 5. */
export const WAVE15_PROPERTY_HOLD_SLUGS = Object.freeze([]);

export const WAVE15_SUPPLEMENTAL_OPENINGS_BY_SLUG = Object.freeze({
  // hilton-hotels-and-resorts: Stage 4 tab-factory already supplies 3 verified
  // CALA openings (Panama, Cancun Mar Caribe, Bogota). No supplemental needed.

  "homewood-suites-by-hilton": Object.freeze([
    {
      propertyName: "Homewood Suites by Hilton Orlando at Flamingo Crossings Town Center",
      url: "https://www.hilton.com/en/hotels/mcofchw-homewood-suites-orlando/",
      market: "Winter Garden / Orlando, USA",
      marketCity: "Winter Garden",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "Homewood Suites by Hilton Orlando Airport",
      url: "https://www.hilton.com/en/hotels/mcogthw-homewood-suites-orlando-airport/",
      market: "Orlando, USA",
      marketCity: "Orlando",
      geographyLabel: "International Reference",
    },
  ]),

  "home2-suites-by-hilton": Object.freeze([
    {
      propertyName: "Home2 Suites by Hilton Miami Doral West Airport",
      url: "https://www.hilton.com/en/hotels/miawaht-home2-suites-miami-doral-west-airport/",
      market: "Doral / Miami, USA",
      marketCity: "Doral",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "Home2 Suites by Hilton Ft. Lauderdale Airport-Cruise Port",
      url: "https://www.hilton.com/en/hotels/fllacht-home2-suites-ft-lauderdale-airport-cruise-port/",
      market: "Dania Beach / Ft. Lauderdale, USA",
      marketCity: "Dania Beach",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "Home2 Suites by Hilton San Antonio Airport",
      url: "https://www.hilton.com/en/hotels/sataiht-home2-suites-san-antonio-airport-tx/",
      market: "San Antonio, USA",
      marketCity: "San Antonio",
      geographyLabel: "International Reference",
    },
  ]),

  "tru-by-hilton": Object.freeze([
    {
      propertyName: "Tru by Hilton Atlanta Galleria Ballpark",
      url: "https://www.hilton.com/en/hotels/atlgbru-tru-atlanta-galleria-ballpark/",
      market: "Atlanta, USA",
      marketCity: "Atlanta",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "Tru by Hilton Atlanta Airport College Park",
      url: "https://www.hilton.com/en/hotels/atlazru-tru-atlanta-airport-college-park/",
      market: "College Park / Atlanta, USA",
      marketCity: "College Park",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "Tru by Hilton Miami Airport South Blue Lagoon",
      url: "https://www.hilton.com/en/hotels/miabnru-tru-miami-airport-south-blue-lagoon/",
      market: "Miami, USA",
      marketCity: "Miami",
      geographyLabel: "International Reference",
    },
  ]),

  "doubletree-by-hilton": Object.freeze([
    {
      propertyName: "DoubleTree by Hilton Mexico City Santa Fe",
      url: "https://www.hilton.com/en/hotels/mexstdt-doubletree-mexico-city-santa-fe/",
      market: "Mexico City, Mexico",
      marketCity: "Mexico City",
      geographyLabel: "CALA",
    },
  ]),

  "hampton-by-hilton": Object.freeze([
    {
      propertyName: "Hampton by Hilton San Jose Airport",
      url: "https://www.hilton.com/en/hotels/sjcaphx-hampton-san-jose-airport/",
      market: "Alajuela / San José, Costa Rica",
      marketCity: "Alajuela",
      geographyLabel: "CALA",
    },
    {
      propertyName: "Hampton by Hilton Guanacaste Airport",
      url: "https://www.hilton.com/en/hotels/lirrahx-hampton-guanacaste-airport/",
      market: "Liberia / Guanacaste, Costa Rica",
      marketCity: "Liberia",
      geographyLabel: "CALA",
    },
    {
      propertyName: "Hampton by Hilton Bogotá - Usaquén",
      url: "https://www.hilton.com/en/hotels/bogushx-hampton-bogota-usaquen/",
      market: "Bogotá, Colombia",
      marketCity: "Bogotá",
      geographyLabel: "CALA",
    },
  ]),

  "hilton-garden-inn": Object.freeze([
    {
      propertyName: "Hilton Garden Inn Bogota Airport",
      url: "https://www.hilton.com/en/hotels/bogatgi-hilton-garden-inn-bogota-airport/",
      market: "Bogotá, Colombia",
      marketCity: "Bogotá",
      geographyLabel: "CALA",
    },
    {
      propertyName: "Hilton Garden Inn Panama City Downtown",
      url: "https://www.hilton.com/en/hotels/ptydngi-hilton-garden-inn-panama-city-downtown/",
      market: "Panama City, Panama",
      marketCity: "Panama City",
      geographyLabel: "CALA",
    },
    {
      propertyName: "Hilton Garden Inn Cancun Airport",
      url: "https://www.hilton.com/en/hotels/cunrogi-hilton-garden-inn-cancun-airport/",
      market: "Cancún, Mexico",
      marketCity: "Cancún",
      geographyLabel: "CALA",
    },
  ]),

  "spark-by-hilton": Object.freeze([
    {
      propertyName: "Spark by Hilton Atlanta Cumberland Ballpark",
      url: "https://www.hilton.com/en/hotels/atlgape-spark-atlanta-cumberland-ballpark/",
      market: "Atlanta, USA",
      marketCity: "Atlanta",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "Spark by Hilton Duluth",
      url: "https://www.hilton.com/en/hotels/atlaope-spark-duluth/",
      market: "Duluth / Atlanta, USA",
      marketCity: "Duluth",
      geographyLabel: "International Reference",
    },
  ]),
});

export function getWave15SupplementalOpenings(slug) {
  const s = String(slug || "").trim().toLowerCase();
  return WAVE15_SUPPLEMENTAL_OPENINGS_BY_SLUG[s] || [];
}

export function isWave15PropertyHoldSlug(slug) {
  return WAVE15_PROPERTY_HOLD_SLUGS.includes(String(slug || "").trim().toLowerCase());
}

/**
 * Curated official image seeds — used when live harvest cannot discover
 * enough official imagery (Akamai bot-block on hilton.com property pages
 * returns 403 for scripted UAs). URLs below are official
 * hilton.com/im/en/{PROPERTY_CODE}/{ASSET_ID}/{name}.jpg entries surfaced in
 * public search-engine snippets of the real property pages and confirmed as
 * canonical property photography for the named hotel. Roles are best-effort
 * from filename tokens — the runtime role-match may still flag diversity if
 * seeded assets are heavily concentrated in one category.
 *
 * Homewood + Home2 in particular were fully 403-blocked during harvest, so
 * these seeds are the initial live pool for those brands.
 */
export const WAVE15_CURATED_POOL_SEED_BY_SLUG = Object.freeze({
  "homewood-suites-by-hilton": Object.freeze([
    {
      propertyKey: "homewood-suites-orlando-airport",
      propertyName: "Homewood Suites by Hilton Orlando Airport",
      marketCity: "Orlando",
      geographyLabel: "International Reference",
      sourcePageUrl: "https://www.hilton.com/en/hotels/mcogthw-homewood-suites-orlando-airport/",
      imageUrl:
        "https://www.hilton.com/im/en/MCOGTHW/22826686/mcogthw-fl-orlando-hws-nqut-418-twobedsuite-wide.jpg",
      label: "property",
      role: "guest_room_suite",
      caption: "Homewood Suites Orlando Airport two-bed suite (official Hilton property photography).",
    },
    {
      propertyKey: "homewood-suites-lake-mary-orlando-north",
      propertyName: "Homewood Suites by Hilton Lake Mary Orlando North",
      marketCity: "Lake Mary",
      geographyLabel: "International Reference",
      sourcePageUrl: "https://www.hilton.com/en/hotels/lkmflhw-homewood-suites-lake-mary-orlando-north/",
      imageUrl:
        "https://www.hilton.com/im/en/LKMFLHW/19583848/lkmflhw-nkqt-nkqt-1.jpg",
      label: "property",
      role: "guest_room_suite",
      caption: "Homewood Suites Lake Mary Orlando North suite (official Hilton property photography).",
    },
    {
      propertyKey: "homewood-suites-miami-dolphin-mall",
      propertyName: "Homewood Suites by Hilton Miami Dolphin Mall",
      marketCity: "Miami",
      geographyLabel: "International Reference",
      sourcePageUrl: "https://www.hilton.com/en/hotels/miadmhw-homewood-suites-miami-dolphin-mall/",
      imageUrl:
        "https://www.hilton.com/im/en/MIADMHW/1218120/homewood-suites-miami-bedroom-suite-2-bed.jpg",
      label: "property",
      role: "guest_room_suite",
      caption: "Homewood Suites Miami Dolphin Mall two-bed suite (official Hilton property photography).",
    },
  ]),

  "home2-suites-by-hilton": Object.freeze([
    {
      propertyKey: "home2-suites-phoenix-downtown",
      propertyName: "Home2 Suites by Hilton Phoenix Downtown",
      marketCity: "Phoenix",
      geographyLabel: "International Reference",
      sourcePageUrl: "https://www.hilton.com/en/hotels/phxpxht-home2-suites-phoenix-downtown/",
      imageUrl:
        "https://www.hilton.com/im/en/PHXPXHT/22988533/phxpx-bedroom-nkj.jpg",
      label: "property",
      role: "guest_room_suite",
      caption: "Home2 Suites Phoenix Downtown king bedroom (official Hilton property photography).",
    },
    {
      propertyKey: "home2-suites-miami-doral-west-airport",
      propertyName: "Home2 Suites by Hilton Miami Doral West Airport",
      marketCity: "Doral",
      geographyLabel: "International Reference",
      sourcePageUrl:
        "https://www.hilton.com/en/hotels/miawaht-home2-suites-miami-doral-west-airport/",
      imageUrl:
        "https://www.hilton.com/im/en/MIAWAHT/16230446/baywood-fl-doral-hm2-nks-one-bed-wide-625.jpg",
      label: "property",
      role: "guest_room_suite",
      caption: "Home2 Suites Miami Doral one-bedroom suite with kitchen (official Hilton property photography).",
    },
    {
      propertyKey: "home2-suites-ft-lauderdale-airport-cruise-port",
      propertyName: "Home2 Suites by Hilton Ft. Lauderdale Airport-Cruise Port",
      marketCity: "Dania Beach",
      geographyLabel: "International Reference",
      sourcePageUrl:
        "https://www.hilton.com/en/hotels/fllacht-home2-suites-ft-lauderdale-airport-cruise-port/",
      imageUrl:
        "https://www.hilton.com/im/en/FLLACHT/6581886/103-ada-1-bed-suite-secondary-angle-low-res.jpg",
      label: "property",
      role: "guest_room_suite",
      caption: "Home2 Suites Ft. Lauderdale Airport ADA one-bed suite (official Hilton property photography).",
    },
    {
      propertyKey: "home2-suites-san-antonio-at-the-rim",
      propertyName: "Home2 Suites by Hilton San Antonio at the Rim",
      marketCity: "San Antonio",
      geographyLabel: "International Reference",
      sourcePageUrl:
        "https://www.hilton.com/en/hotels/satrmht-home2-suites-san-antonio-at-the-rim/",
      imageUrl:
        "https://www.hilton.com/im/en/SATRMHT/9261165/san-antonio-home-2-suites-double-queen-overall.jpg",
      label: "property",
      role: "guest_room_suite",
      caption: "Home2 Suites San Antonio at the Rim double-queen studio (official Hilton property photography).",
    },
  ]),
});

export function getWave15CuratedPoolSeed(slug) {
  const s = String(slug || "").trim().toLowerCase();
  return WAVE15_CURATED_POOL_SEED_BY_SLUG[s] || [];
}
