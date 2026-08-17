/**
 * Wave 17 Batch A — curated owner-facing content (LOW-risk Hyatt cohort).
 * Hyatt Regency · Hyatt Centric · Thompson Hotels
 *
 * Grounded in Wave 17 Hyatt foundation / property candidates.
 * No Recent Momentum cards. No Dream Hotels language for Thompson.
 */
export const WAVE17_BATCH_A_TAB_FACTORY_CONTENT_VERSION =
  "wave17-batch-a-tab-factory-content-v1";

export const WAVE17_SAFE_TGS = Object.freeze([
  "Corporate / Business",
  "Leisure",
  "Bleisure",
  "Family",
  "Solo Traveler",
  "Wellness Seeker",
  "Group / MICE",
  "Contract / Extended Stay",
  "Government / Military",
  "International Inbound",
  "Staycation / Local",
  "Digital Nomad",
  "Luxury / Discerning",
  "Experience-Oriented",
]);

export const FORBIDDEN_SCENARIO_TITLES = Object.freeze([
  "Property Fit",
  "Support Across Lifecycle",
  "Where This Brand Creates the Most Value",
  "Brand Snapshot",
  "Owner Fit",
  "Brand Positioning",
  "Proof Points",
  "Portfolio Context",
  "Geographic Footprint",
  "Growth Priorities",
  "Operating Model",
  "Standards Philosophy",
  "Flexibility Indicators",
  "Third-Party Operator Compatibility",
  "Compliance & Oversight",
  "Opening / Conversion Path",
  "Recent Momentum",
  "Portfolio Mix",
  "Similar Brands",
  "Owner Considerations",
  "Questions Owners Should Ask",
]);

export const WAVE17_BATCH_A_PORTFOLIO_MIX = Object.freeze({
  "hyatt-regency": [
    "Urban / suburban meetings hotels: primary",
    "Airport / convention-adjacent full-service: strong secondary",
    "Resort-adjacent meetings resorts: selective",
    "Select-service or lifestyle-light boxes: weak fit",
  ],
  "hyatt-centric": [
    "Urban lifestyle corridors: primary",
    "Walkable mixed-use / destination neighborhoods: strong secondary",
    "Secondary city centers with explorer demand: selective",
    "Meetings-led Regency boxes or select-service Place: wrong lane",
  ],
  "thompson-hotels": [
    "Design-led urban lifestyle hotels: primary",
    "Cultural / entertainment districts with F&B intensity: strong secondary",
    "Destination urban resorts with social programming: selective",
    "Dream nightlife-led or Centric explorer-light boxes: wrong lane",
  ],
});

function freezeCard(title, body) {
  return Object.freeze({ title, body });
}

function freezeRegion(slotKey, title, body, tags) {
  return Object.freeze({ slotKey, title, body, tags });
}

function pack(partial) {
  if (partial.valueOwnersScenarios.length !== 4) {
    throw new Error(`${partial.slug}: need 4 valueOwners scenarios`);
  }
  if (partial.overviewScenarios.length !== 3) {
    throw new Error(`${partial.slug}: need 3 overview scenarios`);
  }
  if (partial.regions.length < 3) {
    throw new Error(`${partial.slug}: need >=3 geo regions`);
  }
  for (const t of partial.tgs || []) {
    if (!WAVE17_SAFE_TGS.includes(t)) {
      throw new Error(`${partial.slug}: invalid TGS ${t}`);
    }
  }
  for (const c of [...partial.valueOwnersScenarios, ...partial.overviewScenarios]) {
    if (FORBIDDEN_SCENARIO_TITLES.includes(c.title)) {
      throw new Error(`${partial.slug}: forbidden scenario title ${c.title}`);
    }
  }
  return Object.freeze({
    ...partial,
    momentum: Object.freeze([]),
    sourcePack: Object.freeze(partial.sourcePack),
  });
}

function prop(name, geographyLabel, market, note = "") {
  return Object.freeze({
    propertyName: name,
    url: "",
    geographyLabel,
    market,
    matchKey: String(name)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, ""),
    note,
  });
}

export const WAVE17_BATCH_A_BRAND_CONTENT = Object.freeze({
  "hyatt-regency": pack({
    slug: "hyatt-regency",
    displayName: "Hyatt Regency",
    model: "core upper-upscale full-service brand with meetings and group depth",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    peers:
      "Grand Hyatt, Hyatt Centric, Hyatt Place, Marriott Hotels, Sheraton, and Westin",
    peerPrimary: "Grand Hyatt",
    calaAvailability: "supported",
    tgs: ["Corporate / Business", "Group / MICE", "Leisure"],
    ownerLens: [
      "Underwrite Hyatt Regency as Hyatt’s core upper-upscale full-service meetings brand—never as Grand Hyatt prestige intensity, Centric lifestyle exploration, or Hyatt Place select-service economics.",
      "Favor urban, suburban, airport, and convention-adjacent assets where meeting inventory, full-service F&B, and World of Hyatt distribution justify operating intensity.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Meetings-Led Upper-Upscale Full-Service Hotels",
        "Hyatt Regency fits owners with credible meeting inventory and full-service public space who need World of Hyatt reach without Grand Hyatt prestige capital or Centric lifestyle design intensity."
      ),
      freezeCard(
        "Convention Airport And Corporate Corridor Depth",
        "Convention-adjacent, airport, and corporate corridor sites create Regency value when group and transient demand share a productive full-service plant. Do not underwrite Hyatt Place select-service economics as if they were Regency."
      ),
      freezeCard(
        "Full-Service Conversion Without Lifestyle Drift",
        "Conversions work when an existing full-service box can deliver Regency’s meetings-capable promise. Value erodes if sponsors import Centric explorer language, Grand Hyatt luxury ritual, or Marriott Hotels / Sheraton generic full-service copy."
      ),
      freezeCard(
        "Operating Intensity Between Select Service And Grand",
        "Regency’s owner case sits above Hyatt Place and below Grand Hyatt. Staff F&B and meetings honestly so affiliation matches real demand rather than a select-service box or a prestige flagship."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Core Full-Service Meetings Assets For World Of Hyatt",
        "Hyatt Regency creates owner value on upper-upscale full-service hotels where meeting inventory, banquet capacity, and multi-outlet F&B support corporate, group, and leisure demand. Affiliation strengthens commercial reach when the plant can host those programs without Grand Hyatt prestige intensity. Owner value weakens when sponsors treat Regency as interchangeable with Marriott Hotels, Sheraton, or Westin generic full-service language, or collapse it into Hyatt Centric lifestyle exploration."
      ),
      freezeCard(
        "Urban Airport And Convention Corridor Productivity",
        "Owner value rises on urban, airport, suburban commercial, and convention-adjacent hotels with enough lobby, F&B, and meeting capacity to carry Regency’s core full-service promise. Underwrite service intensity above Hyatt Place select-service and below Grand Hyatt. Capital returns hold when the competitive set is productive upper-upscale meetings hotels—not soft-brand individuality or lifestyle nightlife."
      ),
      freezeCard(
        "Hard Separation From Grand Centric And Place",
        "Hyatt Regency conversions and new builds create confidence when owners keep Grand Hyatt prestige, Hyatt Centric urban lifestyle, and Hyatt Place select-service as separate lanes. Never reuse Centric property examples, Place operating economics, or Grand ritual assumptions as Regency proof. Compare those siblings explicitly and reject any paragraph that could describe Marriott Hotels or Sheraton unchanged."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA diligence for Hyatt Regency can use named examples such as Hyatt Regency Mexico City when listings verify. Test meetings-capable full-service product and World of Hyatt guest delivery—not Centric lifestyle proof or Grand Hyatt prestige framing.",
        "CALA, Full-service, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America remains a core Hyatt Regency theater for urban, airport, and convention-adjacent full-service hotels such as Hyatt Regency Miami, Chicago, and Orlando. Treat US examples as International Reference relative to CALA-first posture when needed, and keep Regency distinct from Centric and Place.",
        "International Reference, North America, Full-service"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe / other international",
        "Outside verified CALA and core Americas references, label Hyatt Regency geography International Reference until a named official property matches the market. Confirm meetings-capable full-service standards rather than lifestyle or select-service sibling assumptions.",
        "International Reference, Full-service"
      ),
    ],
    openings: [
      {
        propertyName: "Hyatt Regency Mexico City",
        geographyLabel: "CALA",
        market: "Mexico City, Mexico",
        matchKey: "hyatt-regency-mexico-city",
      },
      {
        propertyName: "Hyatt Regency Orlando",
        geographyLabel: "International Reference",
        market: "Orlando, USA",
        matchKey: "hyatt-regency-orlando",
      },
      {
        propertyName: "Hyatt Regency Miami",
        geographyLabel: "International Reference",
        market: "Miami, USA",
        matchKey: "hyatt-regency-miami",
      },
    ],
    sourcePack: {
      recordId: "recP9SqDootMrzaU1",
      officialBrandName: "Hyatt Regency",
      parentPlatform: "Hyatt Hotels Corporation",
      lens: "core upper-upscale full-service / meetings",
      calaAvailability: "supported",
      calaFirstPosture:
        "Use named CALA operating examples first for Hyatt Regency; label other geographies International Reference.",
      officialBrandPage: Object.freeze({
        label: "Hyatt Regency brand page",
        url: "https://www.hyatt.com/hyatt-regency",
      }),
      developmentPage: Object.freeze({
        label: "Hyatt Hotels development",
        url: "https://about.hyatt.com/",
      }),
      ownerFacingPositioningNotes: Object.freeze([
        "Core upper-upscale full-service meetings brand within World of Hyatt.",
        "Stronger than Hyatt Place on meetings/F&B intensity; leaner than Grand Hyatt prestige.",
      ]),
      siblingBrandDistinctionNotes: Object.freeze([
        "Grand Hyatt: prestige and ritual intensity above Regency’s productive full-service lane.",
        "Hyatt Centric: urban lifestyle exploration—not meetings-led Regency.",
        "Hyatt Place: select-service economics—not Regency full-service staffing.",
        "Marriott Hotels / Sheraton / Westin: competing upper-upscale full-service—keep Regency Hyatt-specific.",
      ]),
      distinguishFrom: Object.freeze([
        "Grand Hyatt",
        "Hyatt Centric",
        "Hyatt Place",
        "Marriott Hotels",
        "Sheraton",
        "Westin",
      ]),
      manualReviewRisks: Object.freeze([
        "Do not import Grand Hyatt prestige language into Hyatt Regency owner copy.",
        "Do not import Hyatt Centric lifestyle explorer language into Hyatt Regency owner copy.",
        "Do not paste generic Marriott Hotels or Sheraton full-service copy onto Regency.",
        "Dated momentum not locked for this Batch A build — Recent Momentum left untouched.",
      ]),
      sourceGaps: Object.freeze([
        "Dated momentum not locked for this Batch A build — Recent Momentum left untouched.",
        "Image materialization deferred to a later stage.",
      ]),
      propertyExamples: Object.freeze([
        prop(
          "Hyatt Regency Mexico City",
          "CALA",
          "Mexico City, Mexico",
          "CALA meetings-capable full-service reference."
        ),
        prop(
          "Hyatt Regency Orlando",
          "International Reference",
          "Orlando, USA",
          "International Reference meetings-capable Regency operating plant."
        ),
        prop(
          "Hyatt Regency Miami",
          "International Reference",
          "Miami, USA",
          "International Reference urban/convention Regency plant."
        ),
      ]),
    },
  }),

  "hyatt-centric": pack({
    slug: "hyatt-centric",
    displayName: "Hyatt Centric",
    model: "urban lifestyle full-service brand driven by location and local exploration",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    peers:
      "Hyatt Regency, Caption by Hyatt, Hyatt Place, Thompson Hotels, Hotel Indigo, Canopy by Hilton, and MGallery Collection",
    peerPrimary: "Hyatt Regency",
    calaAvailability: "supported",
    tgs: ["Leisure", "Bleisure", "Experience-Oriented"],
    ownerLens: [
      "Underwrite Hyatt Centric as location-led urban lifestyle full-service—never as Regency meetings depth, Caption select-service commons, or Thompson design/nightlife intensity.",
      "Favor walkable urban corridors where the hotel’s design, local connection, and social F&B support explorer demand without collapsing into generic lifestyle language.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Location-Led Urban Lifestyle Full-Service Hotels",
        "Hyatt Centric fits owners delivering a modern urban stay where neighborhood access, design, and social public space matter more than Regency ballroom depth or Caption select-service commons."
      ),
      freezeCard(
        "Walkable Mixed-Use And Destination Neighborhoods",
        "Walkable mixed-use corridors create Centric value when guests want to explore the city from a lifestyle full-service base. Do not underwrite Hyatt Place select-service economics or Regency meetings capital as substitutes."
      ),
      freezeCard(
        "Full-Service Lifestyle Without Thompson Or Dream Drift",
        "Conversions work when the asset can carry Centric’s explorer promise and local connection. Value erodes if sponsors import Thompson design-led social intensity, Dream nightlife language, or Indigo / Canopy generic boutique copy."
      ),
      freezeCard(
        "Service Model Between Place And Traditional Full Service",
        "Centric’s owner case is lifestyle full-service—not Hyatt Place select-service and not meetings-led Regency. Staff F&B and public space for social urban energy without inventing Thompson nightlife as the brand thesis."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Urban Explorer Full-Service Assets With Local Connection",
        "Hyatt Centric creates owner value on urban lifestyle full-service hotels where location, design, and neighborhood access drive guest choice. Affiliation helps when the asset can deliver an explorer-forward stay with credible F&B and public space. Owner value fails if diligence substitutes Regency meetings assumptions, Caption select-service commons, or generic Hotel Indigo / Canopy lifestyle copy that could describe any boutique brand unchanged."
      ),
      freezeCard(
        "Walkable Corridor Fit Versus Meetings Or Select Service",
        "Owner value rises on walkable urban and mixed-use sites matched to Centric’s local-exploration promise. Underwrite amenities and operating intensity above Hyatt Place and away from Regency group programming as the primary thesis. Capital returns hold when the competitive set is urban lifestyle full-service—not Thompson design-led social hotels or Dream nightlife-led assets."
      ),
      freezeCard(
        "Hard Separation From Regency Caption And Thompson",
        "Hyatt Centric conversions and new builds create confidence when owners keep Regency, Caption by Hyatt, and Thompson Hotels as separate decision pathways. Never reuse Regency meeting plants, Caption commons language, or Thompson/Dream social-nightlife proof as Centric evidence. Reject any paragraph that could describe MGallery or Indigo unchanged."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA diligence for Hyatt Centric can use named examples such as Hyatt Centric Guatemala City and Hyatt Centric San Isidro Lima when listings verify. Test location-led lifestyle full-service product—not Regency meetings proof or Thompson design-led nightlife framing.",
        "CALA, Lifestyle, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is a core Hyatt Centric theater for urban lifestyle hotels such as Hyatt Centric Brickell Miami. Treat US examples as International Reference relative to CALA-first posture when needed, and keep Centric distinct from Regency and Place.",
        "International Reference, North America, Lifestyle"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe / other international",
        "Outside verified CALA and core Americas references, label Hyatt Centric geography International Reference until a named official property matches the market. Confirm explorer-forward lifestyle standards rather than meetings or select-service sibling assumptions.",
        "International Reference, Lifestyle"
      ),
    ],
    openings: [
      {
        propertyName: "Hyatt Centric Guatemala City",
        geographyLabel: "CALA",
        market: "Guatemala City, Guatemala",
        matchKey: "hyatt-centric-guatemala-city",
      },
      {
        propertyName: "Hyatt Centric San Isidro Lima",
        geographyLabel: "CALA",
        market: "Lima, Peru",
        matchKey: "hyatt-centric-san-isidro-lima",
      },
      {
        propertyName: "Hyatt Centric Brickell Miami",
        geographyLabel: "International Reference",
        market: "Miami, USA",
        matchKey: "hyatt-centric-brickell-miami",
      },
    ],
    sourcePack: {
      recordId: "recNy2efMm4N1JtgC",
      officialBrandName: "Hyatt Centric",
      parentPlatform: "Hyatt Hotels Corporation",
      lens: "urban lifestyle full-service / location-led exploration",
      calaAvailability: "supported",
      calaFirstPosture:
        "Use named CALA operating examples first for Hyatt Centric; label other geographies International Reference.",
      officialBrandPage: Object.freeze({
        label: "Hyatt Centric brand page",
        url: "https://www.hyatt.com/hyatt-centric",
      }),
      developmentPage: Object.freeze({
        label: "Hyatt Hotels development",
        url: "https://about.hyatt.com/",
      }),
      ownerFacingPositioningNotes: Object.freeze([
        "Urban lifestyle full-service with location and local connection as the primary product thesis.",
        "Distinct from Regency meetings depth and Caption select-service commons.",
      ]),
      siblingBrandDistinctionNotes: Object.freeze([
        "Hyatt Regency: meetings-led full-service—not Centric explorer lifestyle.",
        "Caption by Hyatt: select-service lifestyle commons—not Centric full-service.",
        "Thompson Hotels: design-led social intensity above Centric’s explorer posture.",
        "Hotel Indigo / Canopy / MGallery: competing lifestyle—keep Centric Hyatt-specific.",
      ]),
      distinguishFrom: Object.freeze([
        "Hyatt Regency",
        "Caption by Hyatt",
        "Hyatt Place",
        "Thompson Hotels",
        "Hotel Indigo",
        "Canopy by Hilton",
        "MGallery Collection",
      ]),
      manualReviewRisks: Object.freeze([
        "Do not import Hyatt Regency meetings language into Hyatt Centric owner copy.",
        "Do not import Caption by Hyatt select-service commons language into Centric.",
        "Do not import Thompson or Dream nightlife language into Centric.",
        "Dated momentum not locked for this Batch A build — Recent Momentum left untouched.",
      ]),
      sourceGaps: Object.freeze([
        "Dated momentum not locked for this Batch A build — Recent Momentum left untouched.",
        "Image materialization deferred to a later stage.",
      ]),
      propertyExamples: Object.freeze([
        prop(
          "Hyatt Centric Guatemala City",
          "CALA",
          "Guatemala City, Guatemala",
          "CALA urban lifestyle Centric reference."
        ),
        prop(
          "Hyatt Centric San Isidro Lima",
          "CALA",
          "Lima, Peru",
          "CALA neighborhood Centric operating reference."
        ),
        prop(
          "Hyatt Centric Brickell Miami",
          "International Reference",
          "Miami, USA",
          "International Reference urban explorer Centric plant."
        ),
      ]),
    },
  }),

  "thompson-hotels": pack({
    slug: "thompson-hotels",
    displayName: "Thompson Hotels",
    model: "design-led lifestyle brand with elevated F&B and cultural social energy",
    parent: "Hyatt Hotels Corporation",
    loyalty: "World of Hyatt",
    peers:
      "Dream Hotels, Hyatt Centric, EDITION, W Hotels, Kimpton Hotels, and Hotel Indigo",
    peerPrimary: "Dream Hotels",
    calaAvailability: "supported",
    tgs: ["Experience-Oriented", "Leisure", "Luxury / Discerning"],
    ownerLens: [
      "Underwrite Thompson Hotels as design-led lifestyle with elevated F&B and cultural social energy—never as Dream nightlife-led identity, Centric explorer-light full-service, or EDITION/W ritual luxury-lifestyle copy.",
      "Favor urban cultural and entertainment districts where design, destination appeal, and F&B intensity justify operating complexity.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Design-Led Urban Lifestyle With Elevated FAndB",
        "Thompson fits owners delivering a design-forward lifestyle hotel where F&B, public space, and cultural energy are central to the guest stay—not Centric explorer-light or Dream nightlife-first framing."
      ),
      freezeCard(
        "Cultural Entertainment District Destination Hotels",
        "Cultural and entertainment districts create Thompson value when destination appeal and social programming support premium lifestyle demand. Do not underwrite Indigo boutique generics or Centric location-only theses as substitutes."
      ),
      freezeCard(
        "Lifestyle Conversion Without Dream Or Edition Drift",
        "Conversions work when the asset can carry Thompson’s design and F&B intensity. Value erodes if sponsors import Dream nightlife language, EDITION or W Hotels ritual luxury-lifestyle copy, or Kimpton interchangeable lifestyle paragraphs."
      ),
      freezeCard(
        "Operating Complexity For Experience-Led Assets",
        "Thompson’s owner case assumes higher F&B and experience operating complexity than Centric. Staff and capital must match that intensity; do not underwrite as if it were a soft select-service lifestyle box."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Design-Led Lifestyle Assets With Cultural Social Energy",
        "Thompson Hotels creates owner value on design-led lifestyle hotels where elevated F&B, cultural relevance, and destination appeal shape guest choice. Affiliation helps when the asset can deliver that experience with World of Hyatt reach. Owner value fails if diligence substitutes Dream Hotels nightlife-led language, Hyatt Centric explorer-light copy, or EDITION / W Hotels paragraphs that could describe those brands unchanged."
      ),
      freezeCard(
        "Urban Cultural District Fit Versus Centric Or Dream",
        "Owner value rises on urban cultural and entertainment sites matched to Thompson’s design and social programming. Underwrite F&B intensity above Centric and keep Dream as a separate nightlife-leaning pathway. Capital returns hold when the competitive set is design-led lifestyle—not Indigo boutique generics or Kimpton interchangeable soft lifestyle."
      ),
      freezeCard(
        "Hard Separation From Dream Centric And Luxury Lifestyle Peers",
        "Thompson conversions and new builds create confidence when owners keep Dream Hotels, Hyatt Centric, EDITION, and W Hotels as separate decision pathways. Never reuse Dream properties, Dream nightlife proof, Centric explorer examples, or EDITION/W ritual language as Thompson evidence. Reject any paragraph that could describe those peers unchanged."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA diligence for Thompson Hotels can use named examples such as The Cape, A Thompson Hotel when listings verify. Test design-led lifestyle product and F&B intensity—not Dream nightlife proof or Centric explorer-only framing.",
        "CALA, Lifestyle, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is a core Thompson theater for design-led urban lifestyle hotels such as Thompson Chicago, Thompson Nashville, and Thompson Dallas. Treat US examples as International Reference relative to CALA-first posture when needed, and keep Thompson distinct from Dream and Centric.",
        "International Reference, North America, Lifestyle"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe / other international",
        "Outside verified CALA and core Americas references, label Thompson geography International Reference until a named official property matches the market—for example Thompson Madrid. Confirm design-led lifestyle standards rather than Dream or Centric sibling assumptions.",
        "International Reference, Lifestyle"
      ),
    ],
    openings: [
      {
        propertyName: "The Cape, A Thompson Hotel",
        geographyLabel: "CALA",
        market: "Cabo San Lucas, Mexico",
        matchKey: "the-cape-thompson",
      },
      {
        propertyName: "Thompson Chicago",
        geographyLabel: "International Reference",
        market: "Chicago, USA",
        matchKey: "thompson-chicago",
      },
      {
        propertyName: "Thompson Nashville",
        geographyLabel: "International Reference",
        market: "Nashville, USA",
        matchKey: "thompson-nashville",
      },
    ],
    sourcePack: {
      recordId: "rec4Mga6ejz3L1M3P",
      officialBrandName: "Thompson Hotels",
      parentPlatform: "Hyatt Hotels Corporation",
      lens: "design-led lifestyle / elevated F&B and cultural social energy",
      calaAvailability: "supported",
      calaFirstPosture:
        "Use named CALA operating examples first for Thompson Hotels; label other geographies International Reference.",
      officialBrandPage: Object.freeze({
        label: "Thompson Hotels brand page",
        url: "https://www.hyatt.com/thompson-hotels",
      }),
      developmentPage: Object.freeze({
        label: "Hyatt Hotels development",
        url: "https://about.hyatt.com/",
      }),
      ownerFacingPositioningNotes: Object.freeze([
        "Design-led lifestyle with elevated F&B and cultural social energy.",
        "Must remain distinct from Dream Hotels nightlife-led identity.",
      ]),
      siblingBrandDistinctionNotes: Object.freeze([
        "Dream Hotels: nightlife/social intensity above Thompson’s design-led cultural posture.",
        "Hyatt Centric: explorer-forward lifestyle full-service—not Thompson design/F&B intensity.",
        "EDITION / W Hotels: luxury lifestyle ritual peers—keep Thompson Hyatt-specific.",
        "Kimpton / Hotel Indigo: competing lifestyle—reject interchangeable boutique copy.",
      ]),
      distinguishFrom: Object.freeze([
        "Dream Hotels",
        "Hyatt Centric",
        "EDITION",
        "W Hotels",
        "Kimpton Hotels",
        "Hotel Indigo",
      ]),
      manualReviewRisks: Object.freeze([
        "Do not import Dream Hotels nightlife language into Thompson Hotels owner copy.",
        "Do not import Hyatt Centric explorer-light language into Thompson.",
        "Do not paste EDITION or W Hotels luxury-lifestyle copy onto Thompson.",
        "Dated momentum not locked for this Batch A build — Recent Momentum left untouched.",
      ]),
      sourceGaps: Object.freeze([
        "Dated momentum not locked for this Batch A build — Recent Momentum left untouched.",
        "Image materialization deferred to a later stage.",
      ]),
      propertyExamples: Object.freeze([
        prop(
          "The Cape, A Thompson Hotel",
          "CALA",
          "Cabo San Lucas, Mexico",
          "CALA design-led Thompson lifestyle reference."
        ),
        prop(
          "Thompson Chicago",
          "International Reference",
          "Chicago, USA",
          "International Reference urban Thompson plant."
        ),
        prop(
          "Thompson Nashville",
          "International Reference",
          "Nashville, USA",
          "International Reference cultural-district Thompson plant."
        ),
      ]),
    },
  }),
});

export function getWave17BatchABrandContent(slug) {
  const content = WAVE17_BATCH_A_BRAND_CONTENT[slug];
  if (!content) {
    throw new Error(`No Wave 17 Batch A content for slug: ${slug}`);
  }
  return content;
}
