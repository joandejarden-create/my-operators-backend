/**
 * Wave 15 Stage 4 — curated owner-facing content packages (Hilton cohort).
 * valueOwners.scenario.1–4: ~26–45 words, real owner-use titles
 * overview.scenario.1–3: ~45–75 words, investment/value titles (never section labels)
 * Hilton Hotels & Resorts ≠ Hilton Worldwide corporate. Homewood ≠ Home2.
 * Tru ≠ Spark ≠ Hampton. DoubleTree ≠ Hilton Hotels & Resorts. HGI ≠ Hampton.
 */
import { getWave15SourcePack } from "./brand-explorer-wave15-source-packs-content.js";

export const WAVE15_TAB_FACTORY_CONTENT_VERSION = "wave15-tab-factory-content-v1";

/** Canonical Brand Basics Target Guest Segments KEEP options. */
export const WAVE15_SAFE_TGS = Object.freeze([
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

export const WAVE15_PORTFOLIO_MIX = Object.freeze({
  "hilton-hotels-and-resorts": [
    "Urban / Gateway: 40%",
    "Resort / Leisure: 30%",
    "Airport / Convention: 20%",
    "Suburban / Secondary: 10%",
  ],
  "homewood-suites-by-hilton": [
    "Suburban / Employment: 45%",
    "Urban / Mixed-Use: 25%",
    "Airport / Highway: 20%",
    "Destination / Leisure: 10%",
  ],
  "home2-suites-by-hilton": [
    "Suburban / Employment: 50%",
    "Highway / Infrastructure: 25%",
    "Urban / Mixed-Use: 15%",
    "Airport / Highway: 10%",
  ],
  "tru-by-hilton": [
    "Suburban / Highway: 40%",
    "Urban / Secondary: 30%",
    "Airport / Mixed-Use: 20%",
    "Destination / Leisure: 10%",
  ],
  "doubletree-by-hilton": [
    "Urban / Gateway: 40%",
    "Resort / Destination: 25%",
    "Airport / Convention: 20%",
    "Suburban / Secondary: 15%",
  ],
  "hampton-by-hilton": [
    "Highway / Airport: 35%",
    "Suburban / Secondary: 35%",
    "Urban / Secondary: 20%",
    "Destination / Leisure: 10%",
  ],
  "hilton-garden-inn": [
    "Suburban / Commercial: 40%",
    "Urban / Mixed-Use: 30%",
    "Airport / Convention: 20%",
    "Destination / Leisure: 10%",
  ],
  "spark-by-hilton": [
    "Highway / Conversion: 40%",
    "Suburban / Secondary: 30%",
    "Urban / Value: 20%",
    "Destination / Leisure: 10%",
  ],
});

function freezeCard(title, body) {
  return Object.freeze({ title, body });
}

function freezeRegion(slotKey, title, body, tags) {
  return Object.freeze({ slotKey, title, body, tags });
}

function freezeMomentum(title, dateLine, geography, summary, sourceLabel, sourceUrl = "") {
  return Object.freeze({ title, dateLine, geography, summary, sourceLabel, sourceUrl });
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
    if (!WAVE15_SAFE_TGS.includes(t)) {
      throw new Error(`${partial.slug}: invalid TGS ${t}`);
    }
  }
  for (const c of [...partial.valueOwnersScenarios, ...partial.overviewScenarios]) {
    if (FORBIDDEN_SCENARIO_TITLES.includes(c.title)) {
      throw new Error(`${partial.slug}: forbidden scenario title ${c.title}`);
    }
  }
  return Object.freeze(partial);
}

export const WAVE15_BRAND_CONTENT = Object.freeze({
  "hilton-hotels-and-resorts": pack({
    slug: "hilton-hotels-and-resorts",
    displayName: "Hilton Hotels & Resorts",
    model: "flagship full-service Hilton Hotels & Resorts brand",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    peers: "Waldorf Astoria, Conrad, Signia by Hilton, Curio Collection, Tapestry Collection, and DoubleTree by Hilton",
    peerPrimary: "DoubleTree by Hilton",
    calaAvailability: "supported",
    tgs: ["Bleisure", "Group / MICE", "International Inbound"],
    ownerLens: [
      "Underwrite Hilton Hotels & Resorts as the namesake full-service flag—never as Hilton Worldwide corporate or a Waldorf Astoria / Conrad luxury adjacency.",
      "Prioritize CALA sites where business, leisure, and inbound demand can share a capable full-service plant with credible meetings capacity.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Full-Service CALA Gateway Anchor",
        "Hilton Hotels & Resorts fits owners anchoring CALA gateways where rooms product, meetings capacity, and service depth can carry the namesake flag without drifting into Conrad luxury or Curio Collection soft-brand individuality."
      ),
      freezeCard(
        "Meetings And Group Demand Capture",
        "Assets with credible meeting and banquet capacity gain Hilton value when owners underwrite public-space, staffing, and F&B honestly so Honors distribution matches the property's true group and bleisure demand profile."
      ),
      freezeCard(
        "International Inbound Positioning",
        "Gateway and resort corridors with strong international arrivals suit Hilton Hotels & Resorts when the physical plant supports full-service delivery for inbound leisure and business guests seeking a globally recognized flag."
      ),
      freezeCard(
        "Standards-Led Flagship Reflag",
        "Independent or legacy full-service hotels gain Hilton owner confidence when brand PIP, systems cutover, and Honors readiness are sequenced clearly and the asset stays brand-legible as Hilton—not DoubleTree or Curio—after conversion."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Meetings-Capable Full-Service Assets",
        "Hilton Hotels & Resorts creates owner value on full-service assets where meeting inventory, polished public space, and banquet capacity can support group and bleisure demand day one. Affiliation strengthens commercial reach when the plant can host those programs consistently. Owner value is weaker when the asset lacks meetings depth or when sponsors underwrite as if Waldorf Astoria or Conrad luxury were the same product."
      ),
      freezeCard(
        "Urban Gateway And Resort Depth",
        "Owner value rises on urban gateway and resort hotels with enough lobby, F&B, and guestroom quality to carry the namesake Hilton Hotels & Resorts promise. Underwrite public-space and food-and-beverage capital so rate can reflect a full-service experience rather than a limited-service or lifestyle box. Capital returns hold when service intensity matches the flagship lane—not Conrad luxury or Signia meetings-led positioning."
      ),
      freezeCard(
        "CALA Flagship Conversion Confidence",
        "Conversion and repositioning assets in CALA fit Hilton Hotels & Resorts when owners need a globally recognized full-service flag without moving into luxury or soft-brand sibling territory. Affiliation helps when PIP, staffing, and product standards can deliver a credible Hilton stay. Value weakens if the capital plan cannot sustain full-service depth or if the thesis drifts toward DoubleTree, Curio Collection, or Tapestry Collection positioning."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA is a primary Hilton Hotels & Resorts diligence region with named operating examples including Hilton Panama, Hilton Cancun Mar Caribe All-Inclusive Resort, and Hilton Bogota. Owners should use these hotels to test full-service product, meetings readiness, and inbound-guest experience delivery in regional demand corridors.",
        "CALA, Full-service, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is a core Hilton Hotels & Resorts operating theater for business gateways and mixed-demand markets. Owners evaluating US or Canada assets should underwrite meetings depth and service intensity against local competitive sets while keeping the flagship distinct from Waldorf Astoria luxury and Curio Collection soft-brand siblings.",
        "International Reference, North America, Full-service"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European Hilton Hotels & Resorts placements suit owners needing flagship full-service recognition in capital and gateway cities. Treat Europe as International Reference evidence unless a specific CALA-comparable corridor is under review, and confirm the asset can carry namesake standards.",
        "International Reference, Europe, Full-service"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Hilton Cancun, an All-Inclusive Resort opens as CALA flagship expansion",
        "Directory",
        "CALA",
        "As of 2026, Hilton Cancun, an All-Inclusive Resort (Mar Caribe) expands the Hilton Hotels & Resorts footprint in Mexico's Caribbean corridor and provides CALA property-level proof of full-service resort product and guest-experience delivery for owners evaluating the flagship brand in leisure gateways.",
        "Stories From Hilton — Hilton Cancun opening",
        "https://stories.hilton.com/releases/hilton-cancun-opening-expands-hiltons-footprint"
      ),
      freezeMomentum(
        "Hilton Panama — CALA property proof",
        "Directory",
        "CALA · Panama City, Panama",
        "As of 2026, Hilton Panama provides CALA property-level proof of urban full-service Hilton Hotels & Resorts product, meetings capacity, and inbound-guest delivery. Labeled property proof for owner review—not a dated opening announcement.",
        "Hilton Panama property page",
        "https://www.hilton.com/en/hotels/ptyhfhh-hilton-panama/"
      ),
      freezeMomentum(
        "Hilton Bogota — CALA property proof",
        "Directory",
        "CALA · Bogotá, Colombia",
        "As of 2026, Hilton Bogota provides CALA property-level proof of urban full-service Hilton Hotels & Resorts product for owners evaluating the flagship brand in Andean gateway markets. Labeled property proof—not a dated opening announcement.",
        "Hilton Bogota property page",
        "https://www.hilton.com/en/hotels/bogbchh-hilton-bogota/"
      ),
    ],
    openings: [
      {
        propertyName: "Hilton Panama",
        url: "https://www.hilton.com/en/hotels/ptyhfhh-hilton-panama/",
        geographyLabel: "CALA",
        market: "Panama City, Panama",
        matchKey: "hilton-panama",
      },
      {
        propertyName: "Hilton Cancun, an All-Inclusive Resort (Mar Caribe)",
        url: "https://www.hilton.com/en/hotels/cunmchh-hilton-cancun-mar-caribe-all-inclusive-resort/",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "hilton-cancun-mar-caribe",
      },
      {
        propertyName: "Hilton Bogota",
        url: "https://www.hilton.com/en/hotels/bogbchh-hilton-bogota/",
        geographyLabel: "CALA",
        market: "Bogotá, Colombia",
        matchKey: "hilton-bogota",
      },
    ],
  }),

  "homewood-suites-by-hilton": pack({
    slug: "homewood-suites-by-hilton",
    displayName: "Homewood Suites by Hilton",
    model: "upscale extended-stay Homewood Suites by Hilton brand",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    peers: "Home2 Suites by Hilton, Embassy Suites by Hilton, Hilton Garden Inn, and Hampton by Hilton",
    peerPrimary: "Home2 Suites by Hilton",
    calaAvailability: "limited_or_unconfirmed",
    tgs: ["Contract / Extended Stay", "Bleisure", "Leisure"],
    ownerLens: [
      "Underwrite upscale extended-stay economics—fully equipped kitchens, hot breakfast, evening social—not transient select-service logic.",
      "Keep Homewood clearly separate from Home2 Suites (midscale extended-stay) and Hilton Garden Inn (not extended-stay). Verified CALA inventory is limited today; label International Reference until a named CALA hotel is confirmed.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Upscale Extended-Stay Suite Capture",
        "Homewood Suites fits owners targeting longer-stay and relocation demand when residential suite layouts, kitchens, and social breakfast/evening programming match upscale extended-stay expectations rather than short-stay select-service or Home2 midscale prototypes."
      ),
      freezeCard(
        "Suburban Employment Node Suites",
        "Suburban employment, medical, and education corridors create Homewood value when sponsors underwrite weekly housekeeping cadence, parking, and suite mix against real length-of-stay patterns for corporate and project-based guests."
      ),
      freezeCard(
        "Conversion Into Upscale Extended Stay",
        "Capable suite or apartment-style shells can reflag to Homewood when the PIP delivers residential product quality, breakfast/social space, and kitchen build-outs. Value weakens if sponsors force a Home2 midscale prototype or Hilton Garden Inn focused-service thesis onto the asset."
      ),
      freezeCard(
        "Honors Reach for Longer Stays",
        "Hilton Honors reach helps Homewood when commercial systems support recurring extended-stay booking patterns. Owners should compare Home2 Suites only when the product tier truly matches cost-conscious midscale extended-stay demand."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Longer-Stay Demand Near Anchors",
        "Homewood Suites creates owner value near employment, medical, education, project, and relocation anchors that generate multi-night stays. Affiliation supports recurring demand when the site sits inside those trip generators rather than relying on transient weekend leisure alone. Owner value is weaker when longer-stay demand is thin or when the competitive set already saturates upscale suite supply with Homewood or comparable branded product."
      ),
      freezeCard(
        "Kitchen And Breakfast Length-Of-Stay Economics",
        "Suite-and-kitchen product plus hot breakfast and evening social create owner value when length-of-stay economics—not short-stay select-service rate math—drive the underwrite. Owners should capitalize residential suite mix, F&B rhythms, and housekeeping cadence for multi-night guests. Affiliation helps when the asset can deliver a residential stay guests will book for weeks, not a transient all-suite night."
      ),
      freezeCard(
        "Upscale Extended-Stay Coverage Depth",
        "Suburban and urban extended-stay assets fit Homewood when Hilton distribution supports recurring corporate, medical, and relocation demand at upscale suite quality. Underwrite staffing, kitchen product, and social programming for residential stays rather than SpringHill short-stay or Hilton Garden Inn focused-service logic. Owner value is weaker when the thesis drifts into Home2 midscale kitchens or Hampton focused-service breakfast prototypes."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is the primary Homewood Suites theater for upscale extended-stay corridors, with verified International Reference examples including Homewood Suites by Hilton Miami Downtown/Brickell and Homewood Suites by Hilton Nashville-Downtown. Underwrite suite economics and competitive set carefully while keeping Home2 Suites and Hilton Garden Inn as separate owner choices.",
        "International Reference, North America, Extended-stay"
      ),
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA inventory for Homewood Suites is not yet confirmed in official Hilton brand materials. Keep CALA cleanly unavailable and use International Reference Homewood hotels for brand-fit diligence until a named CALA property is published on an official Hilton URL.",
        "International Reference required, CALA unconfirmed"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European Homewood Suites placements, where present, provide International Reference evidence for upscale extended-stay delivery. Confirm local longer-stay demand and product parity before transferring North American Homewood operating assumptions.",
        "International Reference, Europe, Extended-stay"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Homewood Suites by Hilton Miami Downtown/Brickell — property proof",
        "Directory",
        "International Reference · Miami, Florida, USA",
        "As of 2026, Homewood Suites by Hilton Miami Downtown/Brickell serves as an Americas property-level reference for upscale extended-stay product, kitchens, and social breakfast/evening programming. Labeled property proof for owner review of Homewood Suites positioning—not a dated opening announcement or CALA claim.",
        "Homewood Suites by Hilton Miami Downtown/Brickell property page",
        "https://www.hilton.com/en/hotels/miadbhw-homewood-suites-miami-downtown-brickell/"
      ),
      freezeMomentum(
        "Homewood Suites by Hilton Nashville-Downtown — property proof",
        "Directory",
        "International Reference · Nashville, Tennessee, USA",
        "As of 2026, Homewood Suites by Hilton Nashville-Downtown serves as an urban Americas reference for upscale extended-stay delivery. Labeled property proof for owner review of Homewood residential suite product and Hilton Honors reach for longer stays.",
        "Homewood Suites by Hilton Nashville-Downtown property page",
        "https://www.hilton.com/en/hotels/bnadwhw-homewood-suites-nashville-downtown/"
      ),
    ],
    openings: [
      {
        propertyName: "Homewood Suites by Hilton Miami Downtown/Brickell",
        url: "https://www.hilton.com/en/hotels/miadbhw-homewood-suites-miami-downtown-brickell/",
        geographyLabel: "International Reference",
        market: "Miami, Florida, USA",
        matchKey: "homewood-miami-downtown-brickell",
      },
      {
        propertyName: "Homewood Suites by Hilton Nashville-Downtown",
        url: "https://www.hilton.com/en/hotels/bnadwhw-homewood-suites-nashville-downtown/",
        geographyLabel: "International Reference",
        market: "Nashville, Tennessee, USA",
        matchKey: "homewood-nashville-downtown",
      },
    ],
  }),

  "home2-suites-by-hilton": pack({
    slug: "home2-suites-by-hilton",
    displayName: "Home2 Suites by Hilton",
    model: "midscale extended-stay Home2 Suites by Hilton brand",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    peers: "Homewood Suites by Hilton, Hampton by Hilton, Tru by Hilton, and Spark by Hilton",
    peerPrimary: "Homewood Suites by Hilton",
    calaAvailability: "limited_or_unconfirmed",
    tgs: ["Contract / Extended Stay", "Leisure", "Bleisure"],
    ownerLens: [
      "Underwrite Home2 as midscale, cost-conscious extended-stay with flexible suite configurations and efficient operations—never as upscale Homewood.",
      "Dual-brand development with Tru or Hampton can improve site economics; verified CALA inventory is limited today so label International Reference until a named CALA hotel is confirmed.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Midscale Extended-Stay Capture",
        "Home2 Suites fits owners chasing longer stays with midscale efficiency when flexible suite configurations, simplified public space, and cost-conscious operations match demand without Homewood upscale residential intensity or Hampton focused-service positioning."
      ),
      freezeCard(
        "Suburban Employment Corridor Suites",
        "Suburban employment and highway corridors create Home2 value when length-of-stay patterns support flexible-suite product. Owners should underwrite parking, guest-laundry, and simplified housekeeping cadence against real project and relocation demand."
      ),
      freezeCard(
        "Dual-Brand Development Efficiency",
        "Sites suited to Tru + Home2 dual-brand pairings gain Home2 value when shared land, back-of-house, and construction efficiencies improve returns without compromising the extended-stay guest experience or Hampton focused-service standards."
      ),
      freezeCard(
        "Secondary Market Extended-Stay Play",
        "Secondary and tertiary markets can favor Home2 when competitive midscale suite supply is thin. Compare Homewood Suites only when the product tier truly warrants upscale kitchens, hot breakfast, and evening social programming."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Cost-Conscious Longer-Stay Demand",
        "Home2 Suites creates owner value where guests want extended-stay suite space at midscale rates—without full Homewood upscale kitchens, hot breakfast, and evening social overhead. Affiliation fits suburban and highway corridors where employment, relocation, and project demand can fill flexible suite mixes. Owner value weakens when the site is pure transient leisure or when capital assumes upscale extended-stay amenities Home2 was never meant to carry."
      ),
      freezeCard(
        "Flexible Suite Configuration Efficiency",
        "Flexible-configuration suites and simplified public space create Home2 value for cost-conscious multi-night guests. Underwrite housekeeping cadence, guest-laundry, and back-of-house for longer stays rather than transient select-service assumptions. Capital cases are stronger when the asset serves those guests efficiently—not when the thesis drifts into Homewood residential upscale or Tru new-build focused-service prototypes."
      ),
      freezeCard(
        "Efficient Dual-Brand Development Pattern",
        "Dual-brand development sites—typically Home2 paired with Tru or Hampton—create owner value when shared land, construction, and operating efficiencies lift returns while keeping each brand's guest promise intact. Underwrite the pairing honestly so extended-stay guests still get the Home2 suite experience. Owner value is weaker when the pairing forces compromises Home2 or its sibling brand cannot sustain."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is the primary Home2 Suites theater for midscale extended-stay corridors and dual-brand development. Verified International Reference property proof includes Home2 Suites by Hilton Phoenix Downtown; dual-brand storytelling also cites the Phoenix Midtown Home2 × Tru pairing in official Hilton materials.",
        "International Reference, North America, Extended-stay"
      ),
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA inventory for Home2 Suites is not yet confirmed in official Hilton brand materials. Keep CALA cleanly unavailable and use International Reference Home2 hotels for brand-fit diligence until a named CALA property is published on an official Hilton URL.",
        "International Reference required, CALA unconfirmed"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European Home2 Suites placements, where present, provide International Reference evidence for midscale extended-stay delivery. Confirm local longer-stay demand and product parity before transferring North American Home2 operating assumptions.",
        "International Reference, Europe, Extended-stay"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Home2 Suites by Hilton Phoenix Midtown — dual-brand International Reference",
        "Directory",
        "International Reference · Phoenix, Arizona, USA",
        "As of 2026, Home2 Suites by Hilton Phoenix Midtown appears in official Hilton dual-brand storytelling as part of a Home2 × Tru pairing. Serves as Americas dual-brand development pattern reference for Home2 midscale extended-stay—not a named hilton.com openings property and not a CALA claim.",
        "Stories From Hilton — Double the Brand (dual-brand release)",
        "https://stories.hilton.com/releases/double-the-brand-the-rise-of-dual-brand-hotels"
      ),
      freezeMomentum(
        "Home2 Suites by Hilton Phoenix Downtown — property proof",
        "Directory",
        "International Reference · Phoenix, Arizona, USA",
        "As of 2026, Home2 Suites by Hilton Phoenix Downtown serves as a verified Americas property-level reference for Home2 midscale extended-stay product and flexible suite configurations. Labeled property proof for owner review of Home2 positioning versus Homewood Suites and Hampton—not a dated opening announcement.",
        "Home2 Suites by Hilton Phoenix Downtown property page",
        "https://www.hilton.com/en/hotels/phxpxht-home2-suites-phoenix-downtown/"
      ),
    ],
    openings: [
      {
        propertyName: "Home2 Suites by Hilton Phoenix Downtown",
        url: "https://www.hilton.com/en/hotels/phxpxht-home2-suites-phoenix-downtown/",
        geographyLabel: "International Reference",
        market: "Phoenix, Arizona, USA",
        matchKey: "home2-phoenix-downtown",
      },
    ],
  }),

  "tru-by-hilton": pack({
    slug: "tru-by-hilton",
    displayName: "Tru by Hilton",
    model: "new-build spirited focused-service Tru by Hilton brand",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    peers: "Spark by Hilton, Hampton by Hilton, Home2 Suites by Hilton, and Hilton Garden Inn",
    peerPrimary: "Hampton by Hilton",
    calaAvailability: "unconfirmed_verify_with_steward",
    tgs: ["Leisure", "Bleisure", "Experience-Oriented"],
    ownerLens: [
      "Underwrite Tru as new-build, spirited focused-service with cross-generational appeal—never as conversion-only Spark or classic Hampton focused-service.",
      "Americas footprint is confirmed at brand level; a specific CALA (Caribbean/Latin America) Tru property has not been verified, so label International Reference until a named CALA hotel is confirmed with the brand.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Spirited New-Build Focused-Service",
        "Tru fits owners building focused-service assets in suburban, highway, and secondary corridors where a spirited, design-forward product and social lobby can lift demand without Hampton classic focused-service or Spark conversion-only economics."
      ),
      freezeCard(
        "Cross-Generational Leisure Capture",
        "Leisure and cross-generational travel corridors create Tru value when design cues, public-space energy, and rooms product resonate with younger and family travelers who would otherwise select an alternative midscale flag."
      ),
      freezeCard(
        "Dual-Brand New-Build Pattern",
        "Sites suited to Tru + Home2 dual-brand pairings gain Tru value when shared land and construction efficiencies improve returns while both brand promises stay intact—new-build focused-service for Tru, midscale extended-stay for Home2."
      ),
      freezeCard(
        "Efficient Focused-Service Operations",
        "Owner value holds when staffing and F&B stay focused-service while design and social lobby carry the Tru brand. Value fades if the PIP produces a generic midscale box that guests cannot distinguish from Hampton or Home2."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Spirited New-Build Value Product",
        "Tru by Hilton creates owner value on new-build focused-service assets where spirited design, social lobby energy, and efficient rooms product can lift midscale demand. Affiliation fits when the site can carry the Tru guest promise—cross-generational, playful, value-first—rather than a conventional Hampton focused-service or Spark conversion-only box. Owner value weakens when the design intent is diluted by cost cuts that erase the brand story."
      ),
      freezeCard(
        "Cross-Generational Leisure And Bleisure Capture",
        "Cross-generational leisure and bleisure demand corridors create Tru value when guests want an energetic, design-led midscale stay. Underwrite rooms design, lobby programming, and F&B cues so the Tru experience is visible in product—not only in brand copy. Capital cases are stronger when the local competitive set is dominated by tired midscale supply that Tru's cross-generational positioning can displace."
      ),
      freezeCard(
        "New-Build Growth-Pipeline Discipline",
        "New-build development creates Tru value when construction cost, prototype discipline, and operating simplicity keep the asset repeatable across corridors. Underwrite the pipeline honestly so Tru stays a distinct new-build focused-service choice—not a conversion Spark alternative or a Hampton substitute by another name. Value holds when Tru retains its spirited design story after opening and through ongoing standards refreshes."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is the primary Tru by Hilton theater for new-build focused-service, with a verified International Reference property at Tru by Hilton Scottsdale Salt River. Official Hilton dual-brand storytelling also cites Tru by Hilton Phoenix Midtown as part of a Tru × Home2 pairing. Underwrite spirited design and rooms product against local midscale competitors while keeping Spark and Hampton in separate owner lanes.",
        "International Reference, North America, Focused-service"
      ),
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "A specific CALA Tru by Hilton operating example is not yet verified in official brand materials at this review pass. Hilton's brand bio references the 'Americas' broadly; do not imply CALA presence until a property-name-matched official Hilton URL is confirmed with the brand.",
        "International Reference required, CALA unconfirmed"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European Tru placements, where present, provide International Reference evidence for new-build focused-service delivery. Confirm local design-forward midscale demand before transferring North American Tru operating assumptions.",
        "International Reference, Europe, Focused-service"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Tru by Hilton Phoenix Midtown — dual-brand International Reference",
        "Directory",
        "International Reference · Phoenix, Arizona, USA",
        "As of 2026, Tru by Hilton Phoenix Midtown appears in official Hilton dual-brand storytelling as part of a Tru × Home2 pairing. Serves as Americas dual-brand development pattern reference for Tru new-build focused-service—not a named hilton.com openings property and not a CALA claim.",
        "Stories From Hilton — Double the Brand (dual-brand release)",
        "https://stories.hilton.com/releases/double-the-brand-the-rise-of-dual-brand-hotels"
      ),
      freezeMomentum(
        "Tru by Hilton Scottsdale Salt River — property proof",
        "Directory",
        "International Reference · Scottsdale, Arizona, USA",
        "As of 2026, Tru by Hilton Scottsdale Salt River serves as a verified Americas property-level reference for Tru new-build focused-service design and cross-generational product. Labeled property proof for owner review of Tru positioning versus Spark by Hilton and Hampton by Hilton—not a dated opening announcement.",
        "Tru by Hilton Scottsdale Salt River property page",
        "https://www.hilton.com/en/hotels/phxscru-tru-scottsdale-salt-river/"
      ),
    ],
    openings: [
      {
        propertyName: "Tru by Hilton Scottsdale Salt River",
        url: "https://www.hilton.com/en/hotels/phxscru-tru-scottsdale-salt-river/",
        geographyLabel: "International Reference",
        market: "Scottsdale, Arizona, USA",
        matchKey: "tru-scottsdale-salt-river",
      },
    ],
  }),

  "doubletree-by-hilton": pack({
    slug: "doubletree-by-hilton",
    displayName: "DoubleTree by Hilton",
    model: "upscale full-service DoubleTree by Hilton brand",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    peers: "Hilton Hotels & Resorts, Curio Collection by Hilton, Tapestry Collection by Hilton, and Embassy Suites by Hilton",
    peerPrimary: "Hilton Hotels & Resorts",
    calaAvailability: "supported",
    tgs: ["Bleisure", "Group / MICE", "Leisure"],
    ownerLens: [
      "Underwrite DoubleTree as upscale full-service with the signature warm-cookie welcome—never as flagship Hilton Hotels & Resorts or a Curio/Tapestry soft-brand collection asset.",
      "CALA growth is confirmed with verified hilton.com property pages for DoubleTree by Hilton Buenos Aires and DoubleTree by Hilton Lima San Isidro—use those named hotels for openings diligence, not brand-page URLs.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Upscale Full-Service CALA Expansion",
        "DoubleTree fits owners bringing upscale full-service into CALA gateways—Buenos Aires debut, Lima San Isidro—when meetings, rooms, and signature arrival experience can carry the brand without drifting into Hilton flagship or Curio Collection positioning."
      ),
      freezeCard(
        "Meetings And Group Demand Capture",
        "Assets with credible meeting and banquet inventory create DoubleTree value when owners underwrite public-space, staffing, and F&B honestly so Hilton Honors distribution matches the property's real group and bleisure demand profile."
      ),
      freezeCard(
        "Signature Guest-Arrival Repositioning",
        "Legacy upscale hotels gain DoubleTree value when the reflag delivers a brand-legible arrival experience, warm cookie welcome, and refreshed public space—rather than a generic upscale box that lenders and guests cannot distinguish from Hilton siblings."
      ),
      freezeCard(
        "Bleisure Urban And Resort Coverage",
        "Urban and resort corridors with mixed business and leisure demand suit DoubleTree when the plant can carry full-service delivery. Owners should compare Hilton Hotels & Resorts flagship only when the asset can genuinely support namesake standards."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Signature Warm-Welcome Full-Service Product",
        "DoubleTree by Hilton creates owner value on upscale full-service assets where the signature warm-cookie welcome, meetings capacity, and refreshed public space can distinguish the property from generic upscale supply. Affiliation strengthens commercial reach when the plant can carry full-service standards day one. Owner value is weaker when the asset lacks meetings depth or when sponsors underwrite as if flagship Hilton Hotels & Resorts were the same product."
      ),
      freezeCard(
        "CALA Urban Full-Service Expansion",
        "CALA growth—brand debut in Argentina (Buenos Aires) and continued expansion in Peru (Lima San Isidro)—creates DoubleTree value in gateway markets where owners need an upscale full-service flag with room for both business and leisure. Underwrite public-space, meetings, and F&B capital honestly so the asset can meet DoubleTree standards. Capital cases weaken when trade-press coverage is used as a substitute for asset-level readiness."
      ),
      freezeCard(
        "Legacy Full-Service Repositioning With Signature Cue",
        "Legacy or independent upscale full-service hotels fit DoubleTree when owners need a globally recognized upscale flag with a recognizable arrival cue after PIP. Affiliation helps when standards, staffing, and Honors readiness can deliver a credible DoubleTree stay. Value weakens if the plan drifts into Curio Collection soft-brand individuality or attempts to imitate flagship Hilton Hotels & Resorts without the underlying full-service depth."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA is an active DoubleTree diligence region with verified hilton.com property pages for DoubleTree by Hilton Buenos Aires (Argentina) and DoubleTree by Hilton Lima San Isidro (Peru). Owners should use these named hotels to benchmark upscale full-service product and meetings readiness in regional gateways.",
        "CALA, Full-service, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is a core DoubleTree operating theater for upscale full-service repositioning and gateway coverage. Owners should underwrite meetings depth and service intensity against local competitive sets while keeping the brand distinct from flagship Hilton Hotels & Resorts and Curio Collection.",
        "International Reference, North America, Full-service"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European DoubleTree placements provide International Reference evidence for upscale full-service delivery in gateway and secondary cities. Confirm the asset can carry DoubleTree standards depth rather than midscale focused-service assumptions before transferring US operating logic.",
        "International Reference, Europe, Full-service"
      ),
    ],
    momentum: [
      freezeMomentum(
        "DoubleTree by Hilton Buenos Aires — CALA property proof",
        "Directory",
        "CALA · Buenos Aires, Argentina",
        "As of 2026, DoubleTree by Hilton Buenos Aires serves as a CALA property-level reference for upscale full-service delivery in an Argentine gateway market. Property proof for owner review of DoubleTree positioning in CALA—not a dated opening announcement.",
        "DoubleTree by Hilton Buenos Aires property page",
        "https://www.hilton.com/en/hotels/buesidt-doubletree-buenos-aires/"
      ),
      freezeMomentum(
        "DoubleTree by Hilton Lima San Isidro — CALA property proof",
        "Directory",
        "CALA · Lima, Peru",
        "As of 2026, DoubleTree by Hilton Lima San Isidro serves as a CALA property-level reference for upscale full-service delivery in an Andean gateway market. Property proof for owner review of DoubleTree positioning in CALA—not a dated opening announcement.",
        "DoubleTree by Hilton Lima San Isidro property page",
        "https://www.hilton.com/en/hotels/limaldt-doubletree-lima-san-isidro/"
      ),
    ],
    openings: [
      {
        propertyName: "DoubleTree by Hilton Buenos Aires",
        url: "https://www.hilton.com/en/hotels/buesidt-doubletree-buenos-aires/",
        geographyLabel: "CALA",
        market: "Buenos Aires, Argentina",
        matchKey: "doubletree-buenos-aires",
      },
      {
        propertyName: "DoubleTree by Hilton Lima San Isidro",
        url: "https://www.hilton.com/en/hotels/limaldt-doubletree-lima-san-isidro/",
        geographyLabel: "CALA",
        market: "Lima, Peru",
        matchKey: "doubletree-lima-san-isidro",
      },
    ],
  }),

  "hampton-by-hilton": pack({
    slug: "hampton-by-hilton",
    displayName: "Hampton by Hilton",
    model: "focused-service Hampton by Hilton brand",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    peers: "Tru by Hilton, Spark by Hilton, Hilton Garden Inn, and Home2 Suites by Hilton",
    peerPrimary: "Tru by Hilton",
    calaAvailability: "supported",
    tgs: ["Leisure", "Bleisure", "Experience-Oriented"],
    ownerLens: [
      "Underwrite Hampton as classic focused-service with 'Hamptonality' service style, free hot breakfast, and the 100% Hampton Guarantee—never as new-build Tru or conversion-only Spark.",
      "CALA openings diligence uses verified named hotels (e.g. Hampton by Hilton Panama). Ecuador brand premiere remains a trade-press momentum signal until a property-name-matched hilton.com URL is confirmed. Legacy Brand Basics data may still say 'Hampton Inn'—treat as same brand.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Focused-Service CALA Market Entry",
        "Hampton fits owners entering new CALA markets with classic focused-service product when free hot breakfast, Hamptonality service style, and Hilton Honors distribution can carry demand without Tru new-build design cost or Spark conversion-only positioning."
      ),
      freezeCard(
        "Highway And Airport Focused-Service Capture",
        "Highway and airport corridors create Hampton value when owners underwrite rooms product, breakfast delivery, and simplified public space against reliable transient bleisure and leisure demand rather than group or extended-stay assumptions."
      ),
      freezeCard(
        "Hamptonality Guest Consistency Advantage",
        "Assets in competitive focused-service sets gain Hampton value when the 100% Hampton Guarantee, service culture, and consistent product delivery differentiate the property from lower-tier midscale supply and value-only conversion flags."
      ),
      freezeCard(
        "Bleisure Suburban And Secondary Coverage",
        "Suburban and secondary markets suit Hampton when reliable focused-service delivery matches guest expectations. Owners should compare Hilton Garden Inn only when the asset can support the higher upscale focused-service tier and its associated capital and staffing."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Reliable Focused-Service Consistency",
        "Hampton by Hilton creates owner value on focused-service assets where consistent product, service culture, and the 100% Hampton Guarantee can lift demand across highway, airport, suburban, and secondary corridors. Affiliation fits when the site can deliver the Hamptonality guest promise reliably day after day. Owner value weakens when the asset drifts into lifestyle-design territory better suited to Tru or into conversion-only economics that belong to Spark."
      ),
      freezeCard(
        "Hamptonality And Breakfast Guarantee Coverage",
        "Hamptonality service style plus free hot breakfast and the 100% Hampton Guarantee create Hampton value when guests want a dependable, brand-consistent stay across trip types. Underwrite breakfast delivery, staffing, and rooms consistency so the guest experience matches the promise. Capital cases are stronger when the local competitive set is dominated by inconsistent midscale supply that Hampton's discipline can displace."
      ),
      freezeCard(
        "CALA Market-Entry Momentum Advantage",
        "CALA growth—Hampton's brand premiere in Ecuador as part of Hilton's 2023 regional expansion—creates owner value in gateway and secondary markets where a globally recognized focused-service flag can lift demand. Underwrite asset-level readiness rather than treating trade coverage as certainty. Capital returns hold when the property can deliver Hamptonality reliably in a market still building brand awareness for the focused-service tier."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA is an active Hampton diligence region with a verified hilton.com property page for Hampton by Hilton Panama (Panama City). Trade coverage also notes Hampton's brand premiere in Ecuador as part of Hilton's 2023 Caribbean and Latin America expansion—treat Ecuador as a dated growth signal until a named property URL is confirmed.",
        "CALA, Focused-service, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America remains a core Hampton theater for classic focused-service across highway, airport, suburban, and secondary corridors. Owners should underwrite rooms and breakfast delivery against local midscale competitors while keeping Tru, Spark, and Hilton Garden Inn as separate owner choices.",
        "International Reference, North America, Focused-service"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European Hampton placements provide International Reference evidence for focused-service delivery in gateway, secondary, and airport corridors. Confirm local demand and product parity before transferring North American Hampton operating assumptions to European or CALA underwriting.",
        "International Reference, Europe, Focused-service"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Hampton by Hilton premieres in Ecuador as part of 2023 CALA expansion",
        "2023",
        "CALA · Ecuador",
        "In 2023, Hampton by Hilton premiered in Ecuador as part of Hilton's Caribbean and Latin America expansion. Trade coverage confirms the brand entry; exact Ecuador property name and Hilton hotel URL remain unverified, so owners should treat this as a dated regional growth signal—not a named openings property.",
        "Hotel Online — Hilton CALA expansion 2023",
        "https://www.hotel-online.com/index.php/news/hilton-accelerates-expansion-in-the-caribbean-and-latin-america-with-record-room-growth-in-2023-and-a-pipeline-of-nearly-110-hotels"
      ),
      freezeMomentum(
        "Hampton by Hilton Panama — CALA property proof",
        "Directory",
        "CALA · Panama City, Panama",
        "As of 2026, Hampton by Hilton Panama serves as a CALA property-level reference for classic focused-service delivery with Hamptonality service, free hot breakfast, and the 100% Hampton Guarantee. Labeled property proof for owner review of Hampton positioning versus Tru by Hilton, Spark by Hilton, and Hilton Garden Inn—not a dated opening announcement.",
        "Hampton by Hilton Panama property page",
        "https://www.hilton.com/en/hotels/ptyhxhx-hampton-panama/"
      ),
    ],
    openings: [
      {
        propertyName: "Hampton by Hilton Panama",
        url: "https://www.hilton.com/en/hotels/ptyhxhx-hampton-panama/",
        geographyLabel: "CALA",
        market: "Panama City, Panama",
        matchKey: "hampton-panama",
      },
    ],
  }),

  "hilton-garden-inn": pack({
    slug: "hilton-garden-inn",
    displayName: "Hilton Garden Inn",
    model: "upscale focused-service Hilton Garden Inn brand",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    peers: "Hampton by Hilton, DoubleTree by Hilton, Embassy Suites by Hilton, and Homewood Suites by Hilton",
    peerPrimary: "Hampton by Hilton",
    calaAvailability: "supported",
    tgs: ["Bleisure", "Group / MICE", "Leisure"],
    ownerLens: [
      "Underwrite Hilton Garden Inn as upscale focused-service with 'laid-back sophisticated' positioning—never as classic Hampton focused-service or DoubleTree upscale full-service.",
      "CALA openings diligence uses verified hilton.com properties (San Jose Airport City Mall; Guanacaste Airport). Paraguay market entry (Asunción) remains a Stories From Hilton momentum signal until a property-name-matched hilton.com URL is confirmed.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Upscale Focused-Service CALA Expansion",
        "Hilton Garden Inn fits owners driving upscale focused-service growth in CALA gateways—Costa Rica, Paraguay, Dominican Republic, Panama, Puerto Rico—when rooms, meetings, and F&B can carry the brand without drifting into Hampton classic focused-service or DoubleTree full-service."
      ),
      freezeCard(
        "Bleisure And Small-Group Meetings Capture",
        "Assets with modest meeting inventory and reliable F&B create HGI value when owners underwrite public-space, breakfast/restaurant, and staffing honestly so Hilton Honors distribution matches real bleisure and small-group demand."
      ),
      freezeCard(
        "Suburban Commercial Corridor Coverage",
        "Suburban commercial and mixed-use corridors suit HGI when travelers want upscale focused-service polish—not classic Hampton midscale or extended-stay Homewood positioning—and the site's demand profile supports the higher tier."
      ),
      freezeCard(
        "Milestone-Driven Market-Entry Play",
        "HGI's CALA momentum—including the brand's 1,000th hotel opening in Costa Rica and its Paraguay market entry—creates owner value in growth markets when new-market recognition and Honors reach support asset-level ramp-up."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Upscale Focused-Service Balance",
        "Hilton Garden Inn creates owner value on upscale focused-service assets where rooms product, meetings capability, and F&B can lift demand above classic Hampton focused-service without full DoubleTree upscale complexity. Affiliation fits when the site can deliver the 'laid-back sophisticated' guest promise consistently. Owner value weakens when the asset drifts into full-service meetings intensity or when sponsors underwrite as if flagship Hilton Hotels & Resorts were the comparable product."
      ),
      freezeCard(
        "Bleisure And Meetings Cross-Coverage",
        "Bleisure and small-group meetings demand corridors create HGI value when owners can offer upscale focused-service rooms plus modest meeting and F&B inventory. Underwrite public-space, breakfast/restaurant, and staffing so the guest experience matches the upscale focused-service promise. Capital cases are stronger when the local competitive set lacks reliable upscale focused-service supply that HGI can capture."
      ),
      freezeCard(
        "Milestone-Driven CALA Expansion Corridor",
        "CALA growth—HGI's 1,000th-hotel milestone opening in Costa Rica plus new-market entries in Dominican Republic, Panama, Puerto Rico, and Paraguay—creates owner value in gateway and secondary markets where a globally recognized upscale focused-service flag can lift demand. Underwrite asset-level readiness rather than treating trade coverage or milestone announcements as substitutes for property-level diligence and asset-specific ramp-up assumptions."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA is a high-priority Hilton Garden Inn diligence region with verified hilton.com property pages for Hilton Garden Inn San Jose Airport City Mall and Hilton Garden Inn Guanacaste Airport (Costa Rica). Paraguay market entry (Asunción) remains a Stories From Hilton 2024 momentum signal until a property-name-matched hilton.com URL is confirmed.",
        "CALA, Focused-service, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America remains a core Hilton Garden Inn theater for upscale focused-service coverage across suburban commercial, urban mixed-use, and airport corridors. Underwrite rooms, breakfast, and modest meetings inventory against local competitive sets while keeping Hampton and DoubleTree in separate owner lanes.",
        "International Reference, North America, Focused-service"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European Hilton Garden Inn placements provide International Reference evidence for upscale focused-service delivery. Confirm local bleisure and small-group meetings demand before transferring US operating assumptions to European or CALA underwriting.",
        "International Reference, Europe, Focused-service"
      ),
    ],
    momentum: [
      freezeMomentum(
        "First Hilton hotel to open in Paraguay is a Hilton Garden Inn (Asunción)",
        "2024",
        "CALA · Asunción, Paraguay",
        "In 2024, Hilton Garden Inn Asuncion Aviadores del Chaco opened as the first Hilton-branded hotel in Paraguay, per the official Stories From Hilton global-footprint release. Serves as a dated CALA market-entry momentum signal for Hilton Garden Inn—not a named openings property until a verified hilton.com hotel URL is confirmed.",
        "Stories From Hilton — Global footprint expanded 2024",
        "https://stories.hilton.com/growth-development/global-footprint-expanded-2024"
      ),
      freezeMomentum(
        "Hilton Garden Inn San Jose Airport City Mall — CALA property proof",
        "Directory",
        "CALA · San José, Costa Rica",
        "As of 2026, Hilton Garden Inn San Jose Airport City Mall serves as a CALA property-level reference for upscale focused-service delivery and is associated with the brand's 1,000th-hotel milestone in Costa Rica. Labeled property proof for owner review—not a dated opening announcement.",
        "Hilton Garden Inn San Jose Airport City Mall property page",
        "https://www.hilton.com/en/hotels/sjoacgi-hilton-garden-inn-san-jose-airport-city-mall/"
      ),
    ],
    openings: [
      {
        propertyName: "Hilton Garden Inn San Jose Airport City Mall",
        url: "https://www.hilton.com/en/hotels/sjoacgi-hilton-garden-inn-san-jose-airport-city-mall/",
        geographyLabel: "CALA",
        market: "San José, Costa Rica",
        matchKey: "hgi-san-jose-airport-city-mall",
      },
      {
        propertyName: "Hilton Garden Inn Guanacaste Airport",
        url: "https://www.hilton.com/en/hotels/sjolagi-hilton-garden-inn-guanacaste-airport/",
        geographyLabel: "CALA",
        market: "Guanacaste, Costa Rica",
        matchKey: "hgi-guanacaste-airport",
      },
    ],
  }),

  "spark-by-hilton": pack({
    slug: "spark-by-hilton",
    displayName: "Spark by Hilton",
    model: "premium economy conversion-only Spark by Hilton brand",
    parent: "Hilton Worldwide",
    loyalty: "Hilton Honors",
    peers: "Tru by Hilton, Hampton by Hilton, Home2 Suites by Hilton, and Hilton Garden Inn",
    peerPrimary: "Tru by Hilton",
    calaAvailability: "not_yet_launched_in_cala",
    tgs: ["Leisure", "Experience-Oriented", "Bleisure"],
    ownerLens: [
      "Underwrite Spark as premium economy, conversion-only—never as new-build Tru or higher-tier Hampton with its free hot breakfast guarantee.",
      "Spark launched in January 2023; confirmed geographic footprint spans the U.S., Canada, and a 2025-era licensing agreement in India. No CALA property has been identified—label International Reference until a named CALA hotel is confirmed with the brand.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Conversion-Only Premium Economy Play",
        "Spark fits owners converting aging independent or under-branded hotels into a premium economy Hilton flag when a light PIP, value-first product, and Hilton Honors reach can lift demand without Tru new-build costs or Hampton focused-service capital."
      ),
      freezeCard(
        "Value-Ladder Portfolio Bottom Fill",
        "Multi-asset owners gain Spark value when several older midscale or economy hotels can join Hilton Honors quickly at a value tier, filling the bottom of the portfolio rate ladder without stealing demand from Hampton or Hilton Garden Inn siblings."
      ),
      freezeCard(
        "Highway And Suburban Conversion Coverage",
        "Highway, suburban, and secondary corridors suit Spark when the asset already has functional bones and the market will pay a Hilton-affiliated premium economy rate—rather than requiring Hampton-tier product or Tru-level design intent."
      ),
      freezeCard(
        "Fast-Ramp Honors Distribution",
        "Affiliation helps Spark when Hilton Honors distribution can lift occupancy quickly on a converted asset. Owners should compare Hampton or Hilton Garden Inn only when the asset can genuinely support those higher-tier product and service standards."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Conversion-Only Premium Economy Discipline",
        "Spark by Hilton creates owner value on conversion-only opportunities where a Hilton-affiliated premium economy flag can lift demand at value-tier economics. Affiliation fits sponsors who want quick Honors distribution and a disciplined conversion path—rather than Tru new-build design cost or Hampton focused-service capital. Owner value is weaker when the asset needs deep re-engineering or when the sponsor wants to market Spark as a Hampton or Hilton Garden Inn substitute to lenders."
      ),
      freezeCard(
        "Value-Ladder Portfolio Bottom Fill",
        "Owner value concentrates for portfolios that need a value-tier Hilton flag to complete the rate ladder—Hilton Hotels & Resorts, DoubleTree, Hilton Garden Inn, Hampton, and Spark. Underwrite Spark honestly as premium economy rather than borrowing Hampton free-breakfast assumptions. Capital cases hold when the asset serves rate-sensitive leisure and bleisure demand that a heavier midscale flag could not efficiently cover."
      ),
      freezeCard(
        "Efficient Reflag for Aging Independents",
        "Aging independent or under-branded hotels fit Spark when the conversion path can deliver a Hilton-affiliated stay at a controlled capital envelope. Underwrite PIP scope, systems cutover, and Honors readiness so the property can open credibly as Spark—not as a stretched Hampton or a design-diluted Tru. Value weakens if sponsors overspend on amenities the premium economy positioning was never meant to carry."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America (U.S. and Canada) is the primary Spark by Hilton theater today, with a named reference at Spark by Hilton Nashville at Opryland and continued growth into Atlanta, San Antonio, Toronto/Markham, and Oshawa per the 2024 Stories From Hilton global-footprint release.",
        "International Reference, North America, Premium economy conversion"
      ),
      freezeRegion(
        "footprint.region.apac",
        "Asia Pacific",
        "Asia Pacific appears in official Spark by Hilton growth messaging via a licensing agreement to open up to 150 Spark hotels across India (Olive by Embassy). Treat APAC as International Reference only; do not imply CALA presence from APAC activity.",
        "International Reference, Asia Pacific, Premium economy conversion"
      ),
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "Spark by Hilton has not launched in CALA based on current official sourcing. Do not imply regional presence—leave CALA cleanly unavailable until a property-name-matched official Hilton URL exists.",
        "International Reference required, CALA unavailable"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Hilton unveils new brand, Spark by Hilton",
        "January 2023",
        "International Reference · United States",
        "In January 2023, Hilton officially unveiled Spark by Hilton, a conversion-only premium economy brand created to meet value-conscious guest and owner demand. Serves as foundational dated momentum for owners evaluating the newest Hilton Worldwide brand and comparing Spark's positioning against Tru by Hilton and Hampton by Hilton.",
        "Stories From Hilton — Spark by Hilton announced",
        "https://stories.hilton.com/releases/spark-by-hilton-announced"
      ),
      freezeMomentum(
        "Spark by Hilton accelerates U.S. expansion and enters Canada",
        "2024",
        "International Reference · United States and Canada",
        "In 2024, Spark by Hilton continued its U.S. growth (Nashville, Atlanta, San Antonio) and planned entry into Canada (Toronto/Markham, Oshawa) per the official Stories From Hilton global-footprint release. Dated momentum signal for Spark's premium economy conversion pipeline outside CALA; owners should verify individual asset-level readiness before underwriting.",
        "Stories From Hilton — Global footprint expanded 2024",
        "https://stories.hilton.com/growth-development/global-footprint-expanded-2024"
      ),
    ],
    openings: [
      {
        propertyName: "Spark by Hilton Nashville at Opryland",
        url: "https://www.hilton.com/en/hotels/bnamepe-spark-nashville-at-opryland/",
        geographyLabel: "International Reference",
        market: "Nashville, Tennessee, USA",
        matchKey: "spark-nashville-at-opryland",
      },
    ],
  }),
});

export function getWave15BrandContent(slug) {
  const content = WAVE15_BRAND_CONTENT[slug];
  if (!content) throw new Error(`Missing Wave 15 content package for ${slug}`);
  let pack = null;
  try {
    pack = getWave15SourcePack(slug);
  } catch {
    pack = null;
  }

  const cands = pack?.recentMomentumCandidates || [];
  const props = (pack?.propertyExamples || []).filter((p) => p.sourcePageUrl || p.url);
  const brandPage = pack?.officialBrandPage?.url || "";
  const devPage = pack?.developmentPage?.url || "";

  function normalizeDate(dateLine, title = "") {
    let d = String(dateLine || "").trim();
    d = d.replace(/\s*\(.*?\)\s*/g, "").trim();
    if (/^\d{4}-\d{2}$/.test(d)) {
      const [y, mo] = d.split("-");
      const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
      return `${months[Number(mo) - 1]} ${y}`;
    }
    if (/^January\s+2023$/i.test(d)) return "January 2023";
    if (/spark by hilton/i.test(title) && /2023/.test(d) && !/2024/.test(d)) return "January 2023";
    if (/^(Directory|Collection|Editorial|Affiliation|Pipeline)$/i.test(d)) return d;
    if (/^Ongoing\b/i.test(d)) return "Directory";
    if (/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{4}$/i.test(d)) return d;
    if (/^Q[1-4]\s+\d{4}$/i.test(d)) return d;
    if (/^\d{4}$/.test(d)) return d;
    if (/^\d{4}[–-]\d{4}$/.test(d)) return d;
    return "Directory";
  }

  let momentum = (content.momentum || []).map((m, index) => {
    const geo = m.geography || "International Reference";
    let sourceUrl = String(m.sourceUrl || "").trim();
    const title = m.title || "";
    const dateLine = normalizeDate(m.dateLine, title);
    const isCalaProp = /\bCALA\b/i.test(title) || /\bCALA\b/i.test(geo);

    if (!sourceUrl) {
      const propUrl = (i) => props[i]?.sourcePageUrl || props[i]?.url || "";
      const storiesCand = cands.find((c) =>
        /stories\.hilton\.com/i.test(c.announcementUrl || "")
      )?.announcementUrl;
      const tradeCand = cands.find((c) =>
        /hotel-online\.com/i.test(c.announcementUrl || "")
      )?.announcementUrl;

      if (isCalaProp && propUrl(0)) sourceUrl = propUrl(0);
      if (!sourceUrl && isCalaProp && tradeCand) sourceUrl = tradeCand;
      if (!sourceUrl && storiesCand) sourceUrl = storiesCand;
      if (!sourceUrl && cands[index]?.announcementUrl) sourceUrl = cands[index].announcementUrl;
      if (!sourceUrl && cands[0]?.announcementUrl) sourceUrl = cands[0].announcementUrl;
      if (!sourceUrl && brandPage) sourceUrl = brandPage;
      if (!sourceUrl && devPage) sourceUrl = devPage;
    }

    let summary = String(m.summary || "")
      .replace(/\bowner diligence\b/gi, "owner review")
      .trim();
    if (!new RegExp(`\\b${geo.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}\\b`, "i").test(summary)) {
      summary = `${geo}. ${summary}`;
    }
    const pad =
      ` Owners evaluating ${content.displayName || slug} should treat this as a dated momentum signal for ${geo} and verify the same brand story still fits the asset being reviewed.`;
    if (summary.split(/\s+/).filter(Boolean).length < 35) summary = `${summary}${pad}`;

    return Object.freeze({
      ...m,
      geography: geo,
      dateLine,
      sourceUrl,
      summary,
    });
  });

  const hasCalaMomentum = momentum.some(
    (m) => /\bCALA\b/i.test(m.geography || "") || /\bCALA\b/i.test(m.title || "")
  );
  if (hasCalaMomentum) {
    momentum = [
      ...momentum.filter(
        (m) => /\bCALA\b/i.test(m.geography || "") || /\bCALA\b/i.test(m.title || "")
      ),
      ...momentum.filter(
        (m) => !(/\bCALA\b/i.test(m.geography || "") || /\bCALA\b/i.test(m.title || ""))
      ),
    ];
  }

  if (momentum.length < 2) {
    const prop = props[0];
    if (prop?.propertyName) {
      const brand = content.displayName || slug;
      const propUrl = prop.sourcePageUrl || prop.url || "";
      momentum = [
        ...momentum,
        Object.freeze({
          title: `${prop.propertyName} — property proof`,
          dateLine: "Directory",
          geography: prop.geographyLabel === "CALA" ? "CALA" : "International Reference",
          summary: `Official property reference for ${prop.propertyName}. Labeled property proof for ${brand} product and operating-model review—not a dated opening announcement.`,
          sourceLabel: `${prop.propertyName} property reference`,
          sourceUrl: propUrl,
        }),
      ];
    } else if (brandPage) {
      const brand = content.displayName || slug;
      momentum = [
        ...momentum,
        Object.freeze({
          title: `${brand} — brand-page property proof`,
          dateLine: "Directory",
          geography: "International Reference",
          summary: `Official ${brand} brand page reference. Labeled property proof for ${brand} product and operating-model review—not a dated opening announcement.`,
          sourceLabel: `${brand} brand page`,
          sourceUrl: brandPage,
        }),
      ];
    }
  }

  return Object.freeze({
    ...content,
    momentum,
    sourcePack: pack,
  });
}
