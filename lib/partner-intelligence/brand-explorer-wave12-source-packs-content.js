/**
 * Wave 12 Stage 3 — curated official source pack content (read-only).
 * No Airtable writes. Target Guest Segments are recommendations only.
 *
 * Match property URLs by property name (never array index).
 * CALA examples first; otherwise label International Reference.
 */
export const WAVE12_SOURCE_PACKS_VERSION = "wave12-source-packs-v1";

/** Multi-select options known to render without golden generic_audience_prose adjacency. */
export const SAFE_TGS_OPTIONS = Object.freeze([
  "Experience-Oriented",
  "Leisure",
  "Bleisure",
  "International Inbound",
]);

export const TGS_AVOID_NOTE =
  "Do not combine Luxury / Discerning with Leisure (or Experience-Oriented adjacency that renders as generic audience prose). Prefer brand-specific Bleisure / Experience-Oriented / Leisure / International Inbound only when source-supported.";

/**
 * @typedef {object} Wave12SourceRef
 * @property {string} url
 * @property {string} label
 * @property {"brand_page"|"development_page"|"property_page"|"parent_platform"|"announcement"|"image_source"} role
 * @property {"highest"|"high"|"medium"} trust
 * @property {string} [note]
 */

/**
 * @typedef {object} Wave12PropertyExample
 * @property {string} propertyName
 * @property {string} url
 * @property {"CALA"|"International Reference"} geographyLabel
 * @property {string} market
 * @property {string} matchKey
 */

/**
 * @typedef {object} Wave12MomentumCandidate
 * @property {string} dateLine
 * @property {string} title
 * @property {string} summary
 * @property {string} announcementUrl
 * @property {string} linkLabel
 * @property {"CALA"|"International Reference"} geographyLabel
 */

function pack(partial) {
  return Object.freeze({
    writeAirtable: false,
    writeBrandStatus: false,
    writeReleaseFields: false,
    writeTargetGuestSegments: false,
    ...partial,
  });
}

export const WAVE12_SOURCE_PACKS_BY_SLUG = Object.freeze({
  "even-hotels": pack({
    slug: "even-hotels",
    name: "EVEN Hotels",
    recordId: "recvvmiyReHhiKdoK",
    parentPlatform: "IHG",
    family: "ihg",
    lens:
      "Wellness-oriented upscale IHG brand for travelers who want to keep fitness, rest, nutrition, and productivity routines on the road. Distinguish from Holiday Inn Express, avid, voco, and Hotel Indigo.",
    calaAvailability: "thin",
    officialBrandPage: {
      url: "https://www.ihg.com/evenhotels/content/us/en/home",
      label: "EVEN Hotels consumer brand page (IHG)",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://development.ihg.com/hotel-brands/even-hotels",
      label: "EVEN Hotels — IHG Development",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.ihgplc.com/en/our-brands",
        label: "IHG brand portfolio (parent platform context)",
        role: "parent_platform",
        trust: "high",
        note: "Parent/platform context only — not brand-specific proof.",
      },
      {
        url: "https://www.ihgplc.com/en/news-and-media/news-releases/2022/ihg-hotels-and-resorts-evolves-upscale-even-hotels-brand",
        label: "IHG newsroom — EVEN prototype evolution (2022)",
        role: "announcement",
        trust: "high",
        note: "Parent newsroom announcement supporting brand evolution; use as momentum/development evidence, not fee language.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "EVEN Hotel New York - Times Square South",
        url: "https://www.ihg.com/evenhotels/hotels/us/en/new-york/nycep/hoteldetail",
        geographyLabel: "International Reference",
        market: "New York, USA",
        matchKey: "EVEN Hotel New York - Times Square South",
      },
      {
        propertyName: "EVEN Hotel Miami - Airport",
        url: "https://www.ihg.com/evenhotels/hotels/us/en/miami/miaeh/hoteldetail",
        geographyLabel: "International Reference",
        market: "Miami, USA",
        matchKey: "EVEN Hotel Miami - Airport",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "September 2022",
        title: "IHG evolves EVEN Hotels prototype for broader owner markets",
        summary:
          "IHG announced an updated EVEN Hotels design and wellness experience intended to improve build/operate efficiency while keeping wellness-led positioning.",
        announcementUrl:
          "https://www.ihgplc.com/en/news-and-media/news-releases/2022/ihg-hotels-and-resorts-evolves-upscale-even-hotels-brand",
        linkLabel: "IHG newsroom — EVEN Hotels evolution",
        geographyLabel: "International Reference",
      },
    ],
    imageSourceHints: [
      {
        url: "https://development.ihg.com/hotel-brands/even-hotels",
        label: "IHG Development EVEN brand imagery / resource library",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "Bleisure"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "EVEN is wellness/productivity-led, not a luxury collection. Bleisure + Experience-Oriented reflects business travelers keeping wellness routines without overstating luxury.",
    },
    distinguishFrom: ["holiday-inn-express", "avid-hotels", "voco-hotels", "hotel-indigo"],
    notes: [
      "CALA open inventory appears thin; prefer International Reference property examples until CALA openings are confirmed on official pages.",
      "Do not treat IHG parent portfolio copy as EVEN-specific proof.",
    ],
  }),

  "voco-hotels": pack({
    slug: "voco-hotels",
    name: "voco",
    recordId: "recwONQTqGU1jHCsM",
    parentPlatform: "IHG",
    family: "ihg",
    lens:
      "Premium conversion-oriented IHG soft brand with flexible design and welcoming service. Distinguish from Hotel Indigo, Kimpton, Vignette, and Holiday Inn.",
    calaAvailability: "partial",
    officialBrandPage: {
      url: "https://www.ihg.com/voco/hotels/us/en/reservation",
      label: "voco hotels consumer brand page (IHG)",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://development.ihg.com/hotel-brands/voco-hotels",
      label: "voco hotels — IHG Development",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.ihgplc.com/en/our-brands",
        label: "IHG brand portfolio (parent platform context)",
        role: "parent_platform",
        trust: "high",
        note: "Parent/platform context only.",
      },
      {
        url: "https://www.ihgplc.com/en/news-and-media/news-releases/2025/ihg-hotels-and-resorts-accelerates-growth-in-mexico-with-6-signed-voco-hotels",
        label: "IHG newsroom — six Mexico voco signings (Sep 2025)",
        role: "announcement",
        trust: "high",
        note: "CALA pipeline momentum; openings scheduled 2027 — label as signed pipeline, not open hotels.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "voco Ciudad de Mexico Reforma",
        url: "https://www.ihg.com/voco/hotels/us/en/ciudad-de-mexico/mexvc/hoteldetail",
        geographyLabel: "CALA",
        market: "Mexico City, Mexico",
        matchKey: "voco Ciudad de Mexico Reforma",
      },
      {
        propertyName: "voco Times Square South New York",
        url: "https://www.ihg.com/voco/hotels/us/en/new-york/nycvc/hoteldetail",
        geographyLabel: "International Reference",
        market: "New York, USA",
        matchKey: "voco Times Square South New York",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "September 9, 2025",
        title: "IHG signs six Mexico voco hotels with Alliance Hotels",
        summary:
          "IHG announced six signed Mexico conversions for voco (Cancun, Guadalajara, Ciudad Juarez, San Luis Potosi, Torreon, Nuevo Laredo), expanding the brand’s CALA pipeline with openings planned for 2027.",
        announcementUrl:
          "https://www.ihgplc.com/en/news-and-media/news-releases/2025/ihg-hotels-and-resorts-accelerates-growth-in-mexico-with-6-signed-voco-hotels",
        linkLabel: "IHG newsroom — Mexico voco signings",
        geographyLabel: "CALA",
      },
    ],
    imageSourceHints: [
      {
        url: "https://development.ihg.com/hotel-brands/voco-hotels",
        label: "IHG Development voco brand imagery",
        role: "image_source",
        trust: "highest",
      },
      {
        url: "https://www.ihg.com/voco/hotels/us/en/ciudad-de-mexico/mexvc/hoteldetail",
        label: "Official property page — voco Ciudad de Mexico Reforma",
        role: "image_source",
        trust: "highest",
        note: "Match images to this property name only.",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "Leisure", "International Inbound"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "voco is premium/conversion soft brand — experience-led and leisure-friendly without luxury-collection claims.",
    },
    distinguishFrom: ["hotel-indigo", "kimpton", "vignette-collection", "holiday-inn"],
    notes: [
      "Prioritize open CALA property (Mexico City Reforma) for examples; use Mexico 2025 signings as pipeline momentum with dates.",
      "Parent IHG pages are platform context only.",
    ],
  }),

  "avid-hotels": pack({
    slug: "avid-hotels",
    name: "avid hotels",
    recordId: "recoEarnE8T6sDjZq",
    parentPlatform: "IHG",
    family: "ihg",
    lens:
      "Essentials-focused midscale / select-service IHG brand emphasizing value, sleep quality, and efficient owner economics. Distinguish from Holiday Inn Express and other midscale brands.",
    calaAvailability: "thin",
    officialBrandPage: {
      url: "https://www.ihg.com/avidhotels/content/us/en/home",
      label: "avid hotels consumer brand page (IHG)",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://development.ihg.com/hotel-brands/avid-hotels",
      label: "avid hotels — IHG Development",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.ihgplc.com/en/our-brands",
        label: "IHG brand portfolio (parent platform context)",
        role: "parent_platform",
        trust: "high",
        note: "Parent/platform context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "avid hotel Austin - Downtown / Domain Area",
        url: "https://www.ihg.com/avidhotels/hotels/us/en/austin/ausav/hoteldetail",
        geographyLabel: "International Reference",
        market: "Austin, USA",
        matchKey: "avid hotel Austin - Downtown / Domain Area",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (development page)",
        title: "avid hotels continues midscale expansion via IHG Development",
        summary:
          "IHG Development positions avid as a fast-growing midscale brand with low-cost-to-build prototypes and essentials-led guest guarantees — use development page as primary owner-facing source.",
        announcementUrl: "https://development.ihg.com/hotel-brands/avid-hotels",
        linkLabel: "IHG Development — avid hotels",
        geographyLabel: "International Reference",
      },
    ],
    imageSourceHints: [
      {
        url: "https://development.ihg.com/hotel-brands/avid-hotels",
        label: "IHG Development avid brand imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "Leisure"],
      avoid: ["Luxury / Discerning", "Luxury / Discerning + Leisure adjacency"],
      rationale:
        "avid is midscale essentials — value and consistency. Do not overstate luxury/discerning audiences.",
    },
    distinguishFrom: ["holiday-inn-express", "even-hotels", "garner-hotels"],
    notes: [
      "CALA examples thin in current open inventory research — label US properties International Reference until CALA opens are verified on official pages.",
    ],
  }),

  "holiday-inn-express": pack({
    slug: "holiday-inn-express",
    name: "Holiday Inn Express",
    recordId: "recmGmiIqDtAsm01f",
    parentPlatform: "IHG",
    family: "ihg",
    lens:
      "Scaled upper-midscale / limited-service IHG powerhouse brand. Distinguish from Holiday Inn full-service and avid essentials.",
    calaAvailability: "strong",
    officialBrandPage: {
      url: "https://www.ihg.com/holidayinnexpress/content/us/en/home",
      label: "Holiday Inn Express consumer brand page (IHG)",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://development.ihg.com/hotel-brands/holiday-inn-express",
      label: "Holiday Inn Express — IHG Development",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.ihgplc.com/en/our-brands",
        label: "IHG brand portfolio (parent platform context)",
        role: "parent_platform",
        trust: "high",
        note: "Parent/platform context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Holiday Inn Express & Suites Bogota DC",
        url: "https://www.ihg.com/holidayinnexpress/hotels/us/en/bogota/bogbo/hoteldetail",
        geographyLabel: "CALA",
        market: "Bogotá, Colombia",
        matchKey: "Holiday Inn Express & Suites Bogota DC",
      },
      {
        propertyName: "Holiday Inn Express Mexico Aeropuerto",
        url: "https://www.ihg.com/holidayinnexpress/hotels/us/en/mexico-city/mexae/hoteldetail",
        geographyLabel: "CALA",
        market: "Mexico City, Mexico",
        matchKey: "Holiday Inn Express Mexico Aeropuerto",
      },
      {
        propertyName: "Holiday Inn Express Mexico City Satelite",
        url: "https://www.ihg.com/holidayinnexpress/hotels/us/en/naucalpan-de-juarez/mexsa/hoteldetail",
        geographyLabel: "CALA",
        market: "Mexico City metro, Mexico",
        matchKey: "Holiday Inn Express Mexico City Satelite",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (development page)",
        title: "Holiday Inn Express remains IHG’s largest essentials growth engine",
        summary:
          "IHG Development cites global scale (3,000+ hotels / 50+ countries) and conversion/new-build flexibility — use as owner-growth context supported by the development page, not unsupported metrics inventing.",
        announcementUrl: "https://development.ihg.com/hotel-brands/holiday-inn-express",
        linkLabel: "IHG Development — Holiday Inn Express",
        geographyLabel: "International Reference",
      },
    ],
    imageSourceHints: [
      {
        url: "https://www.ihg.com/holidayinnexpress/hotels/us/en/bogota/bogbo/hoteldetail",
        label: "Official property page — Holiday Inn Express & Suites Bogota DC",
        role: "image_source",
        trust: "highest",
        note: "Match by property name Holiday Inn Express & Suites Bogota DC.",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "Leisure"],
      avoid: ["Luxury / Discerning", "Luxury / Discerning + Leisure adjacency"],
      rationale:
        "HIE is value/consistency-led upper midscale. Bleisure + Leisure matches official guest mix language without luxury overclaim.",
    },
    distinguishFrom: ["holiday-inn", "avid-hotels", "even-hotels"],
    notes: ["Strong CALA property inventory — prioritize Bogotá and Mexico City official pages."],
  }),

  "courtyard-by-marriott": pack({
    slug: "courtyard-by-marriott",
    name: "Courtyard by Marriott",
    recordId: "rec6hye5H8zJmAGv3",
    parentPlatform: "Marriott",
    family: "marriott",
    lens:
      "Major Marriott select-service / business-transient brand with flexible spaces. Distinguish from AC, Moxy, City Express, and Fairfield.",
    calaAvailability: "strong",
    officialBrandPage: {
      url: "https://www.marriott.com/en-us/hotels/travel/courtyard/",
      label: "Courtyard by Marriott consumer brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/select-brands",
      label: "Marriott Hotel Development — Select Service brands (includes Courtyard)",
      role: "development_page",
      trust: "highest",
      note: "Select-service hub — cite Courtyard specifically; do not treat sibling brand copy as Courtyard proof.",
    },
    parentPlatformContext: [
      {
        url: "https://www.hotel-development.marriott.com/brands",
        label: "Marriott brand portfolio (parent platform context)",
        role: "parent_platform",
        trust: "high",
        note: "Parent/platform context only.",
      },
      {
        url: "https://www.marriott.com/brands.mi",
        label: "Marriott Bonvoy brands directory (parent context)",
        role: "parent_platform",
        trust: "high",
        note: "Directory context — not Courtyard-specific proof.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Courtyard by Marriott Cancun Airport",
        url: "https://www.marriott.com/en-us/hotels/cuncy-courtyard-cancun-airport/overview/",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "Courtyard by Marriott Cancun Airport",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (official property)",
        title: "Courtyard Cancun Airport remains a CALA select-service reference",
        summary:
          "Official Marriott property page for Courtyard by Marriott Cancun Airport supports CALA select-service presence near airport demand generators.",
        announcementUrl:
          "https://www.marriott.com/en-us/hotels/cuncy-courtyard-cancun-airport/overview/",
        linkLabel: "Courtyard Cancun Airport — official property page",
        geographyLabel: "CALA",
      },
    ],
    imageSourceHints: [
      {
        url: "https://www.marriott.com/en-us/hotels/cuncy-courtyard-cancun-airport/overview/",
        label: "Official property imagery — Courtyard Cancun Airport",
        role: "image_source",
        trust: "highest",
        note: "Match by property name Courtyard by Marriott Cancun Airport.",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "Leisure"],
      avoid: ["Luxury / Discerning", "Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Courtyard is select-service business + leisure. Avoid luxury-collection audience language.",
    },
    distinguishFrom: [
      "ac-hotels-by-marriott",
      "moxy-hotels",
      "city-express-by-marriott",
      "fairfield",
    ],
    notes: [
      "Do not cite FDD PDFs in owner-facing copy; development hub is for steward reference only.",
    ],
  }),

  "ac-hotels-by-marriott": pack({
    slug: "ac-hotels-by-marriott",
    name: "AC Hotels by Marriott",
    recordId: "rec9aZp7GHtzUEg0c",
    parentPlatform: "Marriott",
    family: "marriott",
    lens:
      "Design-led lifestyle-select Marriott with European / modern business roots. Distinguish from Moxy, Courtyard, Autograph, and Tribute.",
    calaAvailability: "partial",
    officialBrandPage: {
      url: "https://www.marriott.com/en-us/brands/ac-hotels",
      label: "AC Hotels by Marriott consumer brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/select-brands",
      label: "Marriott Hotel Development — Select Service (AC Hotels listed)",
      role: "development_page",
      trust: "highest",
      note: "Use AC-specific sections only; sibling brand blurbs are not AC proof.",
    },
    parentPlatformContext: [
      {
        url: "https://www.hotel-development.marriott.com/brands",
        label: "Marriott brand portfolio (parent platform context)",
        role: "parent_platform",
        trust: "high",
        note: "Parent/platform context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "AC Hotel Guadalajara, Mexico",
        url: "https://www.marriott.com/en-us/hotels/gdlac-ac-hotel-guadalajara-mexico/overview/",
        geographyLabel: "CALA",
        market: "Guadalajara, Mexico",
        matchKey: "AC Hotel Guadalajara, Mexico",
      },
      {
        propertyName: "AC Hotel Miami Brickell",
        url: "https://www.marriott.com/en-us/hotels/miaac-ac-hotel-miami-brickell/overview/",
        geographyLabel: "International Reference",
        market: "Miami, USA",
        matchKey: "AC Hotel Miami Brickell",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (official brand page)",
        title: "AC Hotels design-led select positioning for modern business travel",
        summary:
          "Official AC Hotels brand page frames modern design for modern business — use as positioning source for owner relevance around design-led select affiliation.",
        announcementUrl: "https://www.marriott.com/en-us/brands/ac-hotels",
        linkLabel: "AC Hotels by Marriott — official brand page",
        geographyLabel: "International Reference",
      },
    ],
    imageSourceHints: [
      {
        url: "https://www.marriott.com/en-us/hotels/gdlac-ac-hotel-guadalajara-mexico/overview/",
        label: "Official property imagery — AC Hotel Guadalajara, Mexico",
        role: "image_source",
        trust: "highest",
        note: "Match by property name AC Hotel Guadalajara, Mexico.",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "Bleisure"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "AC is design-led lifestyle-select for modern business travelers — Experience-Oriented + Bleisure without luxury-collection overclaim.",
    },
    distinguishFrom: ["moxy-hotels", "courtyard-by-marriott", "autograph-collection", "tribute-portfolio"],
    notes: [
      "Verify each Marriott property URL still resolves before tab-factory build; swap if Marriott redirects change codes.",
    ],
  }),

  "city-express-by-marriott": pack({
    slug: "city-express-by-marriott",
    name: "City Express by Marriott",
    recordId: "recucEzAS6724tOYA",
    parentPlatform: "Marriott",
    family: "marriott",
    lens:
      "Latin America / CALA-relevant midscale select-service Marriott family (City Express / Plus / Junior / Centro). Distinguish from Courtyard, Fairfield, and Four Points.",
    calaAvailability: "strong",
    officialBrandPage: {
      url: "https://www.marriott.com/brands/city-express.mi",
      label: "City Express by Marriott consumer brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/pt/brands/cityexpressbymarriott",
      label: "Marriott Hotel Development — City Express by Marriott",
      role: "development_page",
      trust: "highest",
      note: "Owner/development framing for Americas midscale; do not surface FDD/fee language in owner-facing UI.",
    },
    parentPlatformContext: [
      {
        url: "https://www.hotel-development.marriott.com/brands",
        label: "Marriott brand portfolio (parent platform context)",
        role: "parent_platform",
        trust: "high",
        note: "Parent/platform context only.",
      },
      {
        url: "https://www.marriott.com/brands.mi",
        label: "Marriott Bonvoy brands directory (parent context)",
        role: "parent_platform",
        trust: "high",
        note: "Directory context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "City Express by Marriott Cancun Aeropuerto",
        url: "https://www.marriott.com/en-us/hotels/cunxa-city-express-cancun-aeropuerto/overview/",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "City Express by Marriott Cancun Aeropuerto",
      },
      {
        propertyName: "City Express by Marriott Cancun",
        url: "https://www.marriott.com/en-us/hotels/cunxc-city-express-cancun/overview/",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "City Express by Marriott Cancun",
      },
      {
        propertyName: "City Centro by Marriott Oaxaca",
        url: "https://www.marriott.com/en-us/hotels/oaxco-city-centro-oaxaca/overview/",
        geographyLabel: "CALA",
        market: "Oaxaca, Mexico",
        matchKey: "City Centro by Marriott Oaxaca",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (official brand page)",
        title: "City Express footprint spans Mexico and broader Americas midscale markets",
        summary:
          "Official City Express brand page highlights central-city stays across Mexico, Colombia, Chile, Brazil, and the US — strong CALA relevance for owners comparing midscale affiliation.",
        announcementUrl: "https://www.marriott.com/brands/city-express.mi",
        linkLabel: "City Express by Marriott — official brand page",
        geographyLabel: "CALA",
      },
    ],
    imageSourceHints: [
      {
        url: "https://www.marriott.com/en-us/hotels/cunxa-city-express-cancun-aeropuerto/overview/",
        label: "Official property imagery — City Express Cancun Aeropuerto",
        role: "image_source",
        trust: "highest",
        note: "Match by property name City Express by Marriott Cancun Aeropuerto.",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "Leisure", "International Inbound"],
      avoid: ["Luxury / Discerning", "Luxury / Discerning + Leisure adjacency"],
      rationale:
        "City Express is midscale / efficient select-service for value-consistent travelers across CALA — not luxury.",
    },
    distinguishFrom: ["courtyard-by-marriott", "fairfield", "four-points"],
    notes: [
      "Strongest CALA Wave 12 brand — prioritize Mexico property pages matched by name.",
      "Clarify City Express vs City Express Plus / Junior / Centro when selecting examples.",
    ],
  }),

  "moxy-hotels": pack({
    slug: "moxy-hotels",
    name: "Moxy Hotels",
    recordId: "recahVIW4aCx0Ao84",
    parentPlatform: "Marriott",
    family: "marriott",
    lens:
      "Playful / social lifestyle-select Marriott with compact rooms and activated public space. Distinguish from AC, Aloft, and Autograph.",
    calaAvailability: "strong",
    officialBrandPage: {
      url: "https://www.marriott.com/brands/moxy-hotels.mi",
      label: "Moxy Hotels consumer brand page (Marriott Bonvoy)",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/select-brands",
      label: "Marriott Hotel Development — Select Service (Moxy listed)",
      role: "development_page",
      trust: "highest",
      note: "Use Moxy-specific framing only.",
    },
    parentPlatformContext: [
      {
        url: "https://www.marriott.com/brands.mi",
        label: "Marriott Bonvoy brands directory (parent context)",
        role: "parent_platform",
        trust: "high",
        note: "Parent/platform context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Moxy Tulum",
        url: "https://www.marriott.com/en-us/hotels/tqoox-moxy-tulum/overview/",
        geographyLabel: "CALA",
        market: "Tulum, Mexico",
        matchKey: "Moxy Tulum",
      },
      {
        propertyName: "Moxy Atlanta Downtown",
        url: "https://www.marriott.com/en-us/hotels/atldx-moxy-atlanta-downtown/overview/",
        geographyLabel: "International Reference",
        market: "Atlanta, USA",
        matchKey: "Moxy Atlanta Downtown",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "2024 (CALA debut)",
        title: "Moxy Tulum opens as first Moxy in Caribbean & Latin America",
        summary:
          "Trade and brand coverage mark Moxy Tulum as the brand’s CALA debut near Jaguar Park / Tulum ruins — pair with the official Marriott property page matched by name Moxy Tulum.",
        announcementUrl: "https://www.marriott.com/en-us/hotels/tqoox-moxy-tulum/overview/",
        linkLabel: "Moxy Tulum — official property page",
        geographyLabel: "CALA",
      },
    ],
    imageSourceHints: [
      {
        url: "https://www.marriott.com/en-us/hotels/tqoox-moxy-tulum/overview/",
        label: "Official property imagery — Moxy Tulum",
        role: "image_source",
        trust: "highest",
        note: "Match by property name Moxy Tulum.",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "Leisure"],
      avoid: ["Luxury / Discerning", "Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Moxy is playful lifestyle-select — experience and leisure oriented, not luxury collection.",
    },
    distinguishFrom: ["ac-hotels-by-marriott", "aloft", "autograph-collection"],
    notes: [
      "CALA anchor example is Moxy Tulum — always match URLs by property name.",
      "Also keep moxy-hotels.marriott.com as secondary brand storytelling surface.",
    ],
  }),

  "canopy-by-hilton": pack({
    slug: "canopy-by-hilton",
    name: "Canopy by Hilton",
    recordId: "recsggfbKlJbjeRP9",
    parentPlatform: "Hilton",
    family: "hilton",
    lens:
      "Hilton lifestyle / neighborhood-oriented boutique brand. Distinguish from Curio, Tapestry, Tempo, and Motto.",
    calaAvailability: "thin",
    officialBrandPage: {
      url: "https://www.hilton.com/en/brands/canopy-by-hilton/",
      label: "Canopy by Hilton consumer brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://stories.hilton.com/canopy-by-hilton-fact-sheet",
      label: "Canopy Hotels fact sheet — Stories from Hilton",
      role: "development_page",
      trust: "high",
      note: "Official Hilton stories/fact sheet; prefer brand page + property pages for owner copy.",
    },
    parentPlatformContext: [
      {
        url: "https://stories.hilton.com/releases/qa-with-phil-cordell-lifestyle-category-head",
        label: "Hilton Lifestyle category context (Canopy / Motto / Tempo)",
        role: "parent_platform",
        trust: "high",
        note: "Parent lifestyle-category context — label clearly; do not use as Canopy-only proof.",
      },
      {
        url: "https://www.hilton.com/en/",
        label: "Hilton.com (parent platform context)",
        role: "parent_platform",
        trust: "high",
        note: "Parent platform only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Canopy by Hilton Reykjavik City Centre",
        url: "https://www.hilton.com/en/hotels/rekcpcp-canopy-reykjavik-city-centre/",
        geographyLabel: "International Reference",
        market: "Reykjavik, Iceland",
        matchKey: "Canopy by Hilton Reykjavik City Centre",
      },
      {
        propertyName: "Canopy by Hilton Washington DC The Wharf",
        url: "https://www.hilton.com/en/hotels/waswhcp-canopy-washington-dc-the-wharf/",
        geographyLabel: "International Reference",
        market: "Washington, DC, USA",
        matchKey: "Canopy by Hilton Washington DC The Wharf",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (Hilton fact sheet)",
        title: "Canopy expands as Hilton’s neighborhood lifestyle boutique brand",
        summary:
          "Hilton’s Canopy fact sheet frames layered design and local F&B for neighborhood destinations — use as an International Reference positioning signal until verified regional opens are confirmed for the target market.",
        announcementUrl: "https://stories.hilton.com/canopy-by-hilton-fact-sheet",
        linkLabel: "Canopy Hotels fact sheet",
        geographyLabel: "International Reference",
      },
    ],
    imageSourceHints: [
      {
        url: "https://www.hilton.com/en/brands/canopy-by-hilton/",
        label: "Official Canopy brand page imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "Leisure", "International Inbound"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Canopy is lifestyle boutique / neighborhood — experience and leisure led. Avoid soft-luxury filler language.",
    },
    distinguishFrom: [
      "curio-collection",
      "tapestry-collection-by-hilton",
      "tempo-by-hilton",
      "motto-by-hilton",
    ],
    notes: [
      "CALA open examples appear thin vs Motto — keep International Reference until CALA Canopy properties are confirmed on Hilton.com.",
    ],
  }),

  "motto-by-hilton": pack({
    slug: "motto-by-hilton",
    name: "Motto by Hilton",
    recordId: "reclt44apoi8co0e6",
    parentPlatform: "Hilton",
    family: "hilton",
    lens:
      "Compact urban lifestyle / micro-hotel Hilton brand with flexible connecting rooms and social commons. Distinguish from Canopy and Tempo.",
    calaAvailability: "strong",
    officialBrandPage: {
      url: "https://www.hilton.com/en/brands/motto-by-hilton/",
      label: "Motto by Hilton consumer brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://stories.hilton.com/aboutmotto",
      label: "About Motto — Stories from Hilton",
      role: "development_page",
      trust: "high",
    },
    parentPlatformContext: [
      {
        url: "https://stories.hilton.com/releases/qa-with-phil-cordell-lifestyle-category-head",
        label: "Hilton Lifestyle category context (parent)",
        role: "parent_platform",
        trust: "high",
        note: "Parent lifestyle-category context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Motto by Hilton Tulum",
        url: "https://www.hilton.com/en/hotels/czmtuua-motto-tulum/",
        geographyLabel: "CALA",
        market: "Tulum, Mexico",
        matchKey: "Motto by Hilton Tulum",
      },
      {
        propertyName: "Motto by Hilton Cusco",
        url: "https://www.hilton.com/en/hotels/cuzinua-motto-cusco/",
        geographyLabel: "CALA",
        market: "Cusco, Peru",
        matchKey: "Motto by Hilton Cusco",
      },
      {
        propertyName: "Motto by Hilton Washington DC City Center",
        url: "https://www.hilton.com/en/hotels/dcamtmt-motto-washington-dc-city-center/",
        geographyLabel: "International Reference",
        market: "Washington, DC, USA",
        matchKey: "Motto by Hilton Washington DC City Center",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "November 2022",
        title: "Motto by Hilton debuts in Mexico with Motto by Hilton Tulum",
        summary:
          "Hilton announced Motto by Hilton Tulum as the brand’s Caribbean & Latin America debut inside Hunab Lifestyle Center — dated CALA opening with official stories + property page.",
        announcementUrl:
          "https://stories.hilton.com/releases/motto-by-hilton-debuts-in-mexico-this-fall-with-the-opening-of-motto-by-hilton-tulum",
        linkLabel: "Hilton stories — Motto Tulum opening",
        geographyLabel: "CALA",
      },
      {
        dateLine: "December 2022",
        title: "Motto by Hilton marks international debut in Mexico and the Netherlands",
        summary:
          "Hilton newsroom framed Motto Tulum and Motto Rotterdam Blaak as the brand’s international debut milestone.",
        announcementUrl:
          "https://stories.hilton.com/releases/motto-by-hilton-makes-international-debut-mexico-netherlands",
        linkLabel: "Hilton stories — Motto international debut",
        geographyLabel: "CALA",
      },
    ],
    imageSourceHints: [
      {
        url: "https://www.hilton.com/en/hotels/czmtuua-motto-tulum/",
        label: "Official property imagery — Motto by Hilton Tulum",
        role: "image_source",
        trust: "highest",
        note: "Match by property name Motto by Hilton Tulum.",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "Leisure"],
      avoid: ["Luxury / Discerning", "Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Motto is compact urban lifestyle / micro-hotel — experience and leisure, not luxury collection.",
    },
    distinguishFrom: ["canopy-by-hilton", "tempo-by-hilton", "tapestry-collection-by-hilton"],
    notes: [
      "Strong CALA examples (Tulum, Cusco, Recife pipeline mentions) — match property URLs by name.",
      "Verify Motto Cusco Hilton hotel code before image stage.",
    ],
  }),

  "tempo-by-hilton": pack({
    slug: "tempo-by-hilton",
    name: "Tempo by Hilton",
    recordId: "recqiHq3GHKMj8Meo",
    parentPlatform: "Hilton",
    family: "hilton",
    lens:
      "Modern lifestyle / wellness-productivity Hilton brand for ambitious travelers. Distinguish from Canopy, Motto, and Hilton Garden Inn.",
    calaAvailability: "none",
    officialBrandPage: {
      url: "https://www.hilton.com/en/brands/tempo-by-hilton/",
      label: "Tempo by Hilton consumer brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://stories.hilton.com/releases/hilton-launches-elevated-approachable-lifestyle-brand-with-tempo-by-hilton",
      label: "Hilton launches Tempo by Hilton (official stories)",
      role: "development_page",
      trust: "high",
    },
    parentPlatformContext: [
      {
        url: "https://stories.hilton.com/releases/qa-with-phil-cordell-lifestyle-category-head",
        label: "Hilton Lifestyle category context (parent)",
        role: "parent_platform",
        trust: "high",
        note: "Parent lifestyle-category context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Tempo by Hilton Nashville Downtown",
        url: "https://www.hilton.com/en/hotels/bnapopo-tempo-nashville-downtown/",
        geographyLabel: "International Reference",
        market: "Nashville, USA",
        matchKey: "Tempo by Hilton Nashville Downtown",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "2024 (Nashville opening)",
        title: "Tempo by Hilton Nashville Downtown opens in Music City",
        summary:
          "Hilton stories announced Tempo by Hilton Nashville Downtown as a lifestyle opening near Broadway — dated International Reference momentum with official property page matched by name.",
        announcementUrl:
          "https://stories.hilton.com/releases/hilton-lifestyle-brand-lands-in-music-city-tempo-by-hilton-nashville-downtown-is-now-open",
        linkLabel: "Hilton stories — Tempo Nashville opening",
        geographyLabel: "International Reference",
      },
      {
        dateLine: "October 2021",
        title: "Hilton launches Tempo by Hilton lifestyle brand",
        summary:
          "Hilton announced Tempo as an approachable lifestyle brand with wellness- and productivity-oriented spaces and an efficient service model for owners.",
        announcementUrl:
          "https://stories.hilton.com/releases/hilton-launches-elevated-approachable-lifestyle-brand-with-tempo-by-hilton",
        linkLabel: "Hilton stories — Tempo brand launch",
        geographyLabel: "International Reference",
      },
    ],
    imageSourceHints: [
      {
        url: "https://www.hilton.com/en/brands/tempo-by-hilton/",
        label: "Official Tempo brand page imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "Bleisure"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Tempo targets ambitious / modern-achiever travelers with wellness-productivity cues — Experience-Oriented + Bleisure without luxury-collection overclaim.",
    },
    distinguishFrom: ["canopy-by-hilton", "motto-by-hilton", "hilton-garden-inn"],
    notes: [
      "No confirmed CALA open examples in this pack — all property examples International Reference until CALA Tempo opens appear on Hilton.com.",
      "Match Tempo Nashville by property name Tempo by Hilton Nashville Downtown (hotel code bnapopo).",
    ],
  }),

  "bunkhouse-hotels": pack({
    slug: "bunkhouse-hotels",
    name: "Bunkhouse Hotels",
    recordId: "recGv268Wda31PlSZ",
    parentPlatform: "Hyatt",
    family: "lifestyle",
    lens:
      "Design-, music-, and community-led boutique hotel platform (now part of Hyatt lifestyle / World of Hyatt). Verify affiliation model per property; distinguish from soft-brand collections.",
    calaAvailability: "strong",
    officialBrandPage: {
      url: "https://bunkhousehotels.com/",
      label: "Bunkhouse Hotels official site",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://newsroom.hyatt.com/bunkhouse",
      label: "Hyatt newsroom — Bunkhouse",
      role: "development_page",
      trust: "high",
      note: "Parent newsroom integration / World of Hyatt context — label parent-platform where appropriate.",
    },
    parentPlatformContext: [
      {
        url: "https://newsroom.hyatt.com/bunkhouse",
        label: "Hyatt newsroom Bunkhouse hub (parent platform context)",
        role: "parent_platform",
        trust: "high",
        note: "Hyatt ownership / World of Hyatt integration context — not a substitute for brand-specific property proof.",
      },
      {
        url: "https://www.hyatt.com/",
        label: "Hyatt.com (parent platform context)",
        role: "parent_platform",
        trust: "high",
        note: "Parent platform only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Hotel San Cristóbal",
        url: "https://bunkhousehotels.com/hotels/hotel-san-cristobal/",
        geographyLabel: "CALA",
        market: "Todos Santos, Mexico",
        matchKey: "Hotel San Cristóbal",
      },
      {
        propertyName: "Hotel San Fernando",
        url: "https://bunkhousehotels.com/hotels/hotel-san-fernando/",
        geographyLabel: "CALA",
        market: "Mexico City, Mexico",
        matchKey: "Hotel San Fernando",
      },
      {
        propertyName: "Hotel Saint Cecilia",
        url: "https://bunkhousehotels.com/hotels/hotel-saint-cecilia/",
        geographyLabel: "International Reference",
        market: "Austin, USA",
        matchKey: "Hotel Saint Cecilia",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "2024–2025 (Hyatt integration)",
        title: "Bunkhouse joins Hyatt lifestyle group / World of Hyatt in phases",
        summary:
          "Hyatt newsroom materials document Bunkhouse’s phased World of Hyatt integration across Austin, Houston, and Mexico properties — use for platform affiliation momentum with clear parent-context labeling.",
        announcementUrl: "https://newsroom.hyatt.com/bunkhouse",
        linkLabel: "Hyatt newsroom — Bunkhouse",
        geographyLabel: "International Reference",
      },
    ],
    imageSourceHints: [
      {
        url: "https://bunkhousehotels.com/hotels/hotel-san-cristobal/",
        label: "Official property imagery — Hotel San Cristóbal",
        role: "image_source",
        trust: "highest",
        note: "Match by property name Hotel San Cristóbal.",
      },
      {
        url: "https://bunkhousehotels.com/hotels/hotel-san-fernando/",
        label: "Official property imagery — Hotel San Fernando",
        role: "image_source",
        trust: "highest",
        note: "Match by property name Hotel San Fernando.",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "Leisure"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Bunkhouse is boutique lifestyle / community design-led — experience and leisure specific, not generic luxury collection.",
    },
    distinguishFrom: [
      "autograph-collection",
      "tribute-portfolio",
      "tapestry-collection-by-hilton",
      "design-hotels",
    ],
    notes: [
      "Parent platform is Hyatt (post-acquisition) — always label Hyatt materials as parent context.",
      "Do not invent franchise/fee economics; confirm commercial model in later Brand Basics stage.",
      "CALA examples: Hotel San Cristóbal (Todos Santos) and Hotel San Fernando (Mexico City).",
    ],
  }),
});

export function getWave12SourcePack(slug) {
  return WAVE12_SOURCE_PACKS_BY_SLUG[String(slug || "").trim().toLowerCase()] || null;
}
