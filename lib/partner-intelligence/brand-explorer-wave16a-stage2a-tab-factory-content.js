/**
 * Wave 16A Stage 2A — curated owner-facing content (LOW-risk cohort only).
 * Fairfield by Marriott · Four Points by Sheraton · Delta Hotels by Marriott
 *
 * Grounded in Stage 1 source foundation. No Recent Momentum cards.
 * Do not borrow Active sibling factual copy.
 */
import { getWave16aStage1Pack } from "./brand-explorer-wave16a-stage1-source-content.js";

export const WAVE16A_STAGE2A_TAB_FACTORY_CONTENT_VERSION =
  "wave16a-stage2a-tab-factory-content-v1";

export const WAVE16A_SAFE_TGS = Object.freeze([
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

/** Qualitative mix only — illustrative posture, not portfolio census. */
export const WAVE16A_STAGE2A_PORTFOLIO_MIX = Object.freeze({
  "fairfield-by-marriott": [
    "Suburban / employment corridors: primary",
    "Airport / highway nodes: strong secondary",
    "Secondary urban commercial: selective",
    "Resort / meetings-led assets: weak fit",
  ],
  "four-points-by-sheraton": [
    "Airport / suburban midscale: primary",
    "Secondary urban practical traveler: strong secondary",
    "Highway commercial: selective",
    "Full-service Sheraton or Flex-light conversion: wrong lane",
  ],
  "delta-hotels-by-marriott": [
    "Urban / airport full-service with meetings: primary",
    "Suburban commercial full-service: strong secondary",
    "Gateway business hotels: selective",
    "Luxury ritual or select-service boxes: weak fit",
  ],
});

function freezeCard(title, body) {
  return Object.freeze({ title, body });
}

function freezeRegion(slotKey, title, body, tags) {
  return Object.freeze({ slotKey, title, body, tags });
}

function stage1ToSourcePack(s1) {
  const brandPage = (s1.officialReferences || []).find((r) => r.type === "brand_page") || {};
  const props = (s1.propertyCandidates || []).map((p) => ({
    propertyName: p.name,
    url: p.officialUrl || "",
    geographyLabel: p.region === "CALA" ? "CALA" : "International Reference",
    market: [p.city, p.country].filter(Boolean).join(", "),
    matchKey: String(p.name || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, ""),
    note: p.whyUseful || "",
  }));
  const calaProps = props.filter((p) => p.geographyLabel === "CALA");
  return Object.freeze({
    recordId: null,
    officialBrandName: s1.name,
    parentPlatform: "Marriott International",
    lens: s1.differentiation?.coreIdentity || s1.strategicSegment,
    calaAvailability: calaProps.length ? "supported" : "international_reference",
    calaFirstPosture: calaProps.length
      ? `Use named CALA operating examples first for ${s1.name}; label other geographies International Reference.`
      : `CALA operating examples remain limited for ${s1.name}; keep International Reference labels until named CALA proof is verified.`,
    officialBrandPage: Object.freeze({
      label: brandPage.title || `${s1.name} brand page`,
      url: brandPage.url || "",
    }),
    developmentPage: Object.freeze({
      label: "Marriott Hotel Development",
      url: "https://hotel-development.marriott.com/",
    }),
    ownerFacingPositioningNotes: Object.freeze([
      s1.differentiation?.ownerProposition,
      s1.positioning?.ownerProposition,
      s1.positioning?.operatingImplications,
    ].filter(Boolean)),
    siblingBrandDistinctionNotes: Object.freeze(
      (s1.differentiation?.siblingRules || []).map(
        (r) => `${r.sibling}: ${r.mustRemainDistinctBecause}`
      )
    ),
    distinguishFrom: Object.freeze([...(s1.differentiation?.contaminationSiblings || [])]),
    manualReviewRisks: Object.freeze([
      ...(s1.notes || []),
      ...(s1.differentiation?.mustNotMigrateLanguageFrom || []).map(
        (x) => `Do not import ${x} into ${s1.name} owner copy.`
      ),
    ]),
    sourceGaps: Object.freeze([
      s1.fieldSupport?.recentMomentumCandidates === "CLEANLY_UNAVAILABLE"
        ? "Dated momentum not locked for this Stage 2A build — Recent Momentum left untouched."
        : null,
      s1.fieldSupport?.meetings === "NOT_APPLICABLE"
        ? "Meetings programming is not a primary Fairfield diligence pillar — keep meetings claims limited."
        : null,
      s1.fieldSupport?.technology === "NEEDS_RESEARCH"
        ? "Technology detail stays high-level until brand-specific systems evidence is confirmed."
        : null,
    ].filter(Boolean)),
    propertyExamples: Object.freeze(props),
  });
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
    if (!WAVE16A_SAFE_TGS.includes(t)) {
      throw new Error(`${partial.slug}: invalid TGS ${t}`);
    }
  }
  for (const c of [...partial.valueOwnersScenarios, ...partial.overviewScenarios]) {
    if (FORBIDDEN_SCENARIO_TITLES.includes(c.title)) {
      throw new Error(`${partial.slug}: forbidden scenario title ${c.title}`);
    }
  }
  const s1 = getWave16aStage1Pack(partial.slug);
  return Object.freeze({
    ...partial,
    momentum: Object.freeze([]),
    sourcePack: stage1ToSourcePack(s1),
  });
}

export const WAVE16A_STAGE2A_BRAND_CONTENT = Object.freeze({
  "fairfield-by-marriott": pack({
    slug: "fairfield-by-marriott",
    displayName: "Fairfield by Marriott",
    model: "efficient upper-midscale Marriott select-service rooms brand",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers:
      "Courtyard by Marriott, Four Points by Sheraton, SpringHill Suites by Marriott, and Residence Inn by Marriott",
    peerPrimary: "Courtyard by Marriott",
    calaAvailability: "supported",
    tgs: ["Corporate / Business", "Leisure", "Family"],
    ownerLens: [
      "Underwrite Fairfield as a lean Bonvoy select-service rooms product—never as Courtyard F&B/meetings intensity or an all-suite SpringHill / Residence Inn stay.",
      "Favor prototype-ready suburban, airport, highway, and secondary-urban sites where operating cost discipline and consistent rooms product carry the case.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Prototype-Led Select-Service New Build",
        "Fairfield fits owners building a rooms-focused Marriott select-service hotel where site, prototype, and lean public space can deliver Bonvoy reliability without Courtyard bistro or meeting-room capital."
      ),
      freezeCard(
        "Airport And Highway Transient Capture",
        "Airport and highway nodes create Fairfield value when transient demand needs a consistent rooms product and owners underwrite limited F&B rather than fuller select-service or suite living programs."
      ),
      freezeCard(
        "Suburban Employment Corridor Efficiency",
        "Suburban employment corridors suit Fairfield when sponsors want Bonvoy reach with disciplined operating cost. Value weakens if the thesis needs SpringHill suite product or Residence Inn extended-stay kitchens."
      ),
      freezeCard(
        "Lean Conversion Only When Prototype Fits",
        "Conversion is Fairfield-relevant only when the existing box can meet Fairfield product standards without inventing Courtyard meetings depth. Otherwise keep the asset in a different Marriott lane."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Rooms-First Bonvoy Select-Service Assets",
        "Fairfield by Marriott creates owner value on rooms-first select-service assets where guests need reliable transient lodging and Bonvoy distribution without Courtyard-level F&B or meeting intensity. Affiliation helps when the prototype, site, and operator can deliver consistent rooms product day one. Owner value is weaker when sponsors underwrite bistro, ballroom, or all-suite living as if they were Fairfield requirements."
      ),
      freezeCard(
        "Airport Suburban And Highway Efficiency",
        "Owner value rises on airport, suburban, and highway sites where Fairfield’s lean operating model matches demand. Underwrite public space and limited F&B to Fairfield’s select-service lane rather than Four Points Sheraton-family framing or SpringHill suite expectations. Capital returns hold when the asset stays a practical rooms product, not a fuller-service Courtyard box."
      ),
      freezeCard(
        "Prototype Discipline Versus Sibling Drift",
        "Fairfield conversions and new builds create confidence when owners keep prototype and brand standards Fairfield-specific. Value erodes if diligence borrows Residence Inn extended-stay proof, SpringHill all-suite language, or Courtyard meetings assumptions. Compare against those siblings on product scope before locking affiliation."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA diligence for Fairfield by Marriott can use named operating examples such as Fairfield by Marriott Cancun Airport and Fairfield by Marriott Santo Domingo. Owners should test lean select-service product, airport or capital-city demand, and Bonvoy guest delivery—not Courtyard meetings capacity.",
        "CALA, Select-service, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America remains a core Fairfield theater for suburban, airport, and secondary-urban rooms product. Treat US examples as International Reference relative to CALA-first posture unless the asset under review is in the same corridor, and keep Fairfield distinct from SpringHill and Residence Inn.",
        "International Reference, North America, Select-service"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe / other international",
        "Outside CALA and core Americas corridors, treat Fairfield placements as International Reference until a named property matches the market under review. Confirm the asset can carry Fairfield’s lean select-service promise rather than a fuller Marriott sibling lane.",
        "International Reference, Select-service"
      ),
    ],
    openings: [
      {
        propertyName: "Fairfield by Marriott Cancun Airport",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "fairfield-cancun-airport",
      },
      {
        propertyName: "Fairfield by Marriott Santo Domingo",
        geographyLabel: "CALA",
        market: "Santo Domingo, Dominican Republic",
        matchKey: "fairfield-santo-domingo",
      },
      {
        propertyName: "Fairfield Inn & Suites New York Manhattan/Times Square South",
        geographyLabel: "International Reference",
        market: "New York, USA",
        matchKey: "fairfield-nyc-times-square-south",
      },
    ],
  }),

  "four-points-by-sheraton": pack({
    slug: "four-points-by-sheraton",
    displayName: "Four Points by Sheraton",
    model: "Sheraton-family midscale select-service brand for practical travelers",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers:
      "Four Points Flex by Sheraton, Courtyard by Marriott, Fairfield by Marriott, Aloft Hotels, and Sheraton",
    peerPrimary: "Four Points Flex by Sheraton",
    calaAvailability: "supported",
    tgs: ["Corporate / Business", "Leisure", "Bleisure"],
    ownerLens: [
      "Underwrite classic Four Points by Sheraton product and traveler promise—never Four Points Flex conversion-light scope, Flex prototypes, or Flex economics.",
      "Keep Sheraton-family midscale distinct from Fairfield prototype language, Aloft lifestyle design, and full-service Sheraton ballroom assumptions.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Classic Four Points Midscale Reliability",
        "Four Points fits owners delivering a practical Sheraton-family midscale stay with Bonvoy reach when the asset can meet classic Four Points standards—not Flex-light conversion assumptions."
      ),
      freezeCard(
        "Airport And Secondary Urban Traveler Nodes",
        "Airport and secondary urban sites create Four Points value when business and bleisure demand needs straightforward comfort. Do not underwrite Aloft lifestyle design or Fairfield prototype-only economics as substitutes."
      ),
      freezeCard(
        "Conversion Only At Classic Four Points Scope",
        "Conversions work when PIP and product reach classic Four Points standards. Value collapses if sponsors import Four Points Flex lighter conversion logic or treat Flex hotels as Four Points proof."
      ),
      freezeCard(
        "Sheraton-Family Midscale Versus Full Service",
        "Owners should choose Four Points when select-service midscale is the right lane—not full-service Sheraton meetings depth. Keep public-space and F&B capital honest to the Four Points promise."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Practical Sheraton-Family Midscale Assets",
        "Four Points by Sheraton creates owner value on practical midscale select-service hotels where guests want reliable comfort and Bonvoy access without Aloft lifestyle design or full-service Sheraton complexity. Affiliation helps when the asset can carry classic Four Points product standards. Owner value fails if diligence substitutes Four Points Flex conversion-light scope or Flex property examples."
      ),
      freezeCard(
        "Airport Suburban And Secondary Urban Fit",
        "Owner value rises on airport, suburban, and secondary-urban sites matched to the Four Points traveler promise. Underwrite amenities and operating intensity to classic Four Points—not Courtyard meetings posture or Fairfield rooms-only prototype language. Capital returns hold when the competitive set is midscale practical travel, not lifestyle or full-service siblings."
      ),
      freezeCard(
        "Hard Separation From Four Points Flex",
        "Four Points conversions and new builds create confidence only when owners keep Flex as a separate brand. Never reuse Flex hotels, Flex PIP assumptions, Flex owner economics, or Flex imagery as Four Points evidence. Compare Flex explicitly and reject any copy that could describe Flex unchanged."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA diligence for Four Points by Sheraton can use named examples such as Four Points by Sheraton Cancun Centro and Four Points by Sheraton Bogota when listings verify. Use these to test classic midscale product—never Four Points Flex properties as substitutes.",
        "CALA, Midscale, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is a core Four Points operating theater for airport and urban midscale hotels. Treat US examples as International Reference relative to CALA-first posture when needed, and keep classic Four Points distinct from Four Points Flex and Aloft.",
        "International Reference, North America, Midscale"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe / other international",
        "Outside verified CALA and core Americas references, label Four Points geography International Reference until a named official property matches the market. Confirm classic Four Points standards—not Flex conversion framing.",
        "International Reference, Midscale"
      ),
    ],
    openings: [
      {
        propertyName: "Four Points by Sheraton Cancun Centro",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "four-points-cancun-centro",
      },
      {
        propertyName: "Four Points by Sheraton Bogota",
        geographyLabel: "CALA",
        market: "Bogotá, Colombia",
        matchKey: "four-points-bogota",
      },
      {
        propertyName: "Four Points by Sheraton Miami Airport",
        geographyLabel: "International Reference",
        market: "Miami, USA",
        matchKey: "four-points-miami-airport",
      },
    ],
  }),

  "delta-hotels-by-marriott": pack({
    slug: "delta-hotels-by-marriott",
    displayName: "Delta Hotels by Marriott",
    model: "productive upper-upscale full-service Marriott brand",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers:
      "Marriott Hotels, Renaissance Hotels, Courtyard by Marriott, Four Points by Sheraton, and Sheraton",
    peerPrimary: "Marriott Hotels",
    calaAvailability: "supported",
    tgs: ["Corporate / Business", "Group / MICE", "Bleisure"],
    ownerLens: [
      "Underwrite Delta as productive full-service Bonvoy—meetings and contemporary public space without Marriott Hotels flagship ritual or Renaissance discovery/lifestyle framing.",
      "Favor urban, airport, and meetings-oriented assets where full-service staffing is justified but luxury service intensity is not the thesis.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Productive Full-Service Meetings Hotels",
        "Delta fits owners with credible meeting inventory and contemporary full-service public space who need Bonvoy reach without Marriott Hotels flagship or luxury complexity."
      ),
      freezeCard(
        "Airport And Urban Business Gateways",
        "Airport and urban gateways create Delta value when business and group demand share a productive full-service plant. Do not underwrite Courtyard or Four Points select-service economics as if they were Delta."
      ),
      freezeCard(
        "Full-Service Conversion Without Lifestyle Drift",
        "Conversions work when an existing full-service box can deliver Delta’s productive promise. Value erodes if sponsors import Renaissance design/discovery language or Marriott Hotels flagship assumptions."
      ),
      freezeCard(
        "Operating Intensity Between Select Service And Luxury",
        "Delta’s owner case sits between select-service and luxury lanes. Staff F&B and meetings honestly so affiliation matches real demand rather than a Courtyard box or a luxury ritual hotel."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Meetings-Capable Productive Full-Service Assets",
        "Delta Hotels by Marriott creates owner value on full-service assets where meeting inventory, contemporary public space, and moderate F&B can support business and group demand. Affiliation strengthens commercial reach when the plant can host those programs without luxury staffing intensity. Owner value is weaker when sponsors treat Delta as interchangeable with Marriott Hotels flagship or Renaissance lifestyle discovery."
      ),
      freezeCard(
        "Urban Airport And Commercial Corridor Depth",
        "Owner value rises on urban, airport, and suburban commercial hotels with enough lobby, F&B, and meeting capacity to carry Delta’s productive promise. Underwrite service intensity above Courtyard or Four Points select-service and below luxury ritual brands. Capital returns hold when the competitive set is upper-upscale productivity, not soft-brand individuality."
      ),
      freezeCard(
        "Full-Service Conversion With Clear Peer Separation",
        "Conversion and repositioning assets fit Delta when owners need productive full-service Bonvoy without Marriott Hotels flagship framing or Renaissance design-led storytelling. Affiliation helps when PIP and staffing can deliver a credible Delta stay. Value weakens if the capital plan collapses into select-service scope or drifts into Marriott Hotels / Renaissance sibling copy."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA diligence for Delta Hotels by Marriott can use named examples such as Delta Hotels Cancun Inn and Delta Hotels by Marriott Mexico City Metropolitan when listings verify. Test productive full-service product and meetings readiness—not Marriott Hotels flagship or Renaissance lifestyle proof.",
        "CALA, Full-service, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America—especially Canadian urban and airport conference hotels such as Delta Hotels by Marriott Toronto Airport & Conference Centre—is a core Delta reference theater. Treat these as International Reference relative to CALA-first posture when the asset under review is outside that corridor, and keep Delta distinct from Marriott Hotels.",
        "International Reference, North America, Full-service"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe / other international",
        "Outside verified CALA and core Americas references, label Delta geography International Reference until a named official property matches the market. Confirm productive full-service standards rather than luxury or select-service sibling assumptions.",
        "International Reference, Full-service"
      ),
    ],
    openings: [
      {
        propertyName: "Delta Hotels Cancun Inn",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "delta-cancun-inn",
      },
      {
        propertyName: "Delta Hotels by Marriott Mexico City Metropolitan",
        geographyLabel: "CALA",
        market: "Mexico City, Mexico",
        matchKey: "delta-mexico-city-metropolitan",
      },
      {
        propertyName: "Delta Hotels by Marriott Toronto Airport & Conference Centre",
        geographyLabel: "International Reference",
        market: "Toronto, Canada",
        matchKey: "delta-toronto-airport-conference",
      },
    ],
  }),
});

export function getWave16aStage2aBrandContent(slug) {
  const content = WAVE16A_STAGE2A_BRAND_CONTENT[slug];
  if (!content) {
    throw new Error(`No Wave 16A Stage 2A content for slug: ${slug}`);
  }
  return content;
}
