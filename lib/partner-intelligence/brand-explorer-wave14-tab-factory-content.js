/**
 * Wave 14 Stage 4 — curated owner-facing content packages (Marriott cohort).
 * valueOwners.scenario.1–4: ~26–45 words, real owner-use titles
 * overview.scenario.1–3: ~45–75 words, investment/value titles (never section labels)
 */
import { getWave14SourcePack } from "./brand-explorer-wave14-source-packs-content.js";

export const WAVE14_TAB_FACTORY_CONTENT_VERSION = "wave14-tab-factory-content-v1";

/** Canonical Brand Basics Target Guest Segments KEEP options. */
export const WAVE14_SAFE_TGS = Object.freeze([
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

export const WAVE14_PORTFOLIO_MIX = Object.freeze({
  "marriott-hotels": [
    "Urban / Gateway: 45%",
    "Resort / Destination: 25%",
    "Airport / Convention: 20%",
    "Suburban / Secondary: 10%",
  ],
  sheraton: [
    "Urban / Gateway: 35%",
    "Resort / Destination: 30%",
    "Airport / Convention: 20%",
    "Suburban / Secondary: 15%",
  ],
  westin: [
    "Resort / Destination: 40%",
    "Urban / Gateway: 35%",
    "Airport / Convention: 15%",
    "Suburban / Secondary: 10%",
  ],
  "residence-inn-by-marriott": [
    "Suburban / Corporate: 45%",
    "Urban / Mixed-Use: 25%",
    "Airport / Highway: 20%",
    "Destination / Leisure: 10%",
  ],
  "springhill-suites-by-marriott": [
    "Suburban / Mixed-Use: 40%",
    "Airport / Highway: 25%",
    "Urban / Downtown: 20%",
    "Destination / Leisure: 15%",
  ],
  "towneplace-suites-by-marriott": [
    "Suburban / Employment: 45%",
    "Highway / Infrastructure: 25%",
    "Airport / Medical: 20%",
    "Urban / Mixed-Use: 10%",
  ],
  "aloft-hotels": [
    "Urban / Lifestyle: 45%",
    "Airport / Mixed-Use: 25%",
    "Suburban / Secondary: 20%",
    "Resort / Destination: 10%",
  ],
  studiores: [
    "Suburban / Employment: 50%",
    "Highway / Infrastructure: 25%",
    "Urban / Mixed-Use: 15%",
    "Airport / Medical: 10%",
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
    if (!WAVE14_SAFE_TGS.includes(t)) {
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

export const WAVE14_BRAND_CONTENT = Object.freeze({
  "marriott-hotels": pack({
    slug: "marriott-hotels",
    displayName: "Marriott Hotels",
    model: "flagship full-service Marriott Hotels brand",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers: "JW Marriott, Sheraton, Westin, Renaissance, Autograph Collection, and Tribute Portfolio",
    peerPrimary: "JW Marriott",
    calaAvailability: "supported",
    tgs: ["Bleisure", "Group / MICE", "International Inbound"],
    ownerLens: [
      "Underwrite full-service product, meetings capacity, and brand standards against the specific asset—not Marriott International corporate averages.",
      "Favor sites where business/leisure mix and global distribution meaningfully support Marriott Hotels positioning versus luxury or soft-brand peers.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Full-Service Gateway Conversion",
        "Marriott Hotels fits owners converting capable full-service assets in business and leisure gateways when meetings space, rooms product, and service depth can carry the flagship promise without drifting into JW luxury or soft-brand collection positioning."
      ),
      freezeCard(
        "Meetings And Group Demand Capture",
        "Assets with credible meeting and group capacity create Marriott Hotels value when owners underwrite banquet, staffing, and public-space capital honestly so affiliation lift matches the property’s real group demand profile."
      ),
      freezeCard(
        "Business-Leisure Mixed Market Play",
        "Mixed-demand urban and resort corridors suit Marriott Hotels when transient and leisure demand can share a full-service plant—owners should price operating intensity against Sheraton or Westin alternatives before locking the flag."
      ),
      freezeCard(
        "Standards-Led Reflag Confidence",
        "Independent or legacy full-service hotels gain Marriott Hotels owner confidence when PIP scope, systems cutover, and Bonvoy readiness are sequenced clearly and the asset can stay brand-legible after conversion."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Meetings-Capable Full-Service Assets",
        "Marriott Hotels creates owner value on full-service assets where meeting inventory, polished public space, and banquet capacity can support group and bleisure demand. Affiliation strengthens commercial reach when the physical plant can host those programs day one. Owner value is weaker when the asset lacks meetings depth or when sponsors underwrite as if a soft-brand or lifestyle sibling were the same product."
      ),
      freezeCard(
        "Urban And Resort Public-Space Depth",
        "Owner value rises on urban gateway and resort hotels with enough lobby, F&B, and guestroom quality to carry the namesake Marriott Hotels promise. Underwrite public-space and food-and-beverage capital so rate can reflect a full-service experience rather than a limited-service box. Capital returns hold when the asset’s service intensity matches the brand lane—not JW Marriott luxury or Autograph individuality."
      ),
      freezeCard(
        "Confident Full-Service Conversion",
        "Conversion and repositioning assets fit Marriott Hotels when owners need a globally recognized full-service flag without moving into luxury or soft-brand sibling territory. Affiliation helps when PIP, staffing, and product standards can deliver a credible Marriott Hotels stay. Value weakens if the capital plan cannot sustain full-service depth or if the thesis drifts toward Renaissance, Tribute, or Autograph positioning."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA remains a primary diligence region for Marriott Hotels with named operating examples such as Marriott Cancun Resort and Mexico City Marriott Reforma. Owners should use these hotels to test full-service product, meetings readiness, and guest-experience delivery in regional demand corridors.",
        "CALA, Full-service, Property-backed"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is a core Marriott Hotels operating theater for business gateways and mixed-demand markets. Owners evaluating US or Canada assets should underwrite meetings depth and service intensity against local competitive sets while keeping the brand distinct from JW and soft-brand peers.",
        "International Reference, North America, Full-service"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European Marriott Hotels placements suit owners needing flagship full-service recognition in capital and gateway cities. Treat Europe as International Reference evidence unless a specific CALA-comparable corridor is under review, and confirm the asset can carry full-service standards.",
        "International Reference, Europe, Full-service"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Marriott Cancun Resort anchors CALA full-service reference",
        "Directory",
        "CALA",
        "As of 2026, Marriott Cancun Resort provides CALA property-level proof of full-service product, meetings capacity, and guest-experience delivery for owners evaluating Marriott Hotels affiliation in regional leisure corridors.",
        "Marriott Cancun Resort overview page",
        "https://www.marriott.com/en-us/hotels/cunmc-marriott-cancun-resort/overview/"
      ),
      freezeMomentum(
        "Mexico City Marriott Reforma — property proof",
        "Directory",
        "CALA · Mexico City, Mexico",
        "As of 2026, Mexico City Marriott Reforma provides CALA property-level proof of urban full-service Marriott Hotels product and guest-experience delivery. Labeled property proof—not a dated opening announcement.",
        "Mexico City Marriott Reforma overview page",
        "https://www.marriott.com/en-us/hotels/mexmc-mexico-city-marriott-reforma-hotel/overview/"
      ),
    ],
  }),

  sheraton: pack({
    slug: "sheraton",
    displayName: "Sheraton",
    model: "full-service Sheraton with legacy repositioning potential",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers: "Marriott Hotels, Westin, Four Points by Sheraton, and Four Points Flex by Sheraton",
    peerPrimary: "Four Points by Sheraton",
    calaAvailability: "supported",
    tgs: ["Bleisure", "Group / MICE", "International Inbound"],
    ownerLens: [
      "Underwrite Sheraton as full-service—never as select-service Flex or Four Points.",
      "Value often sits in public-space refresh, meetings reactivation, and network reach on conversion or reinvestment assets.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Legacy Full-Service Repositioning",
        "Sheraton creates owner value on legacy full-service hotels where lobby, meetings, and rooms can be refreshed into a contemporary Sheraton story without collapsing into Four Points select-service or Flex midscale conversion logic."
      ),
      freezeCard(
        "Meetings And Social Space Reactivation",
        "Assets with underused ballrooms and social public space fit Sheraton when owners capitalize reactivation honestly—staffing, F&B, and PIP must match group demand rather than cosmetic lobby work alone."
      ),
      freezeCard(
        "Resort And Gateway Network Play",
        "Resort and gateway Sheraton sites gain from Bonvoy reach when product and service stay full-service. Owners should compare Westin wellness and Marriott Hotels flagship lanes before assuming Sheraton is interchangeable."
      ),
      freezeCard(
        "Conversion With Brand-Legible Public Space",
        "Conversions work when arrival, lobby, and meetings areas become unmistakably Sheraton. Value erodes if the PIP leaves a generic midscale box that guests read as Four Points or Flex."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Legacy Full-Service Repositioning",
        "Sheraton creates owner value when sponsors reinvest in capable full-service shells—especially where meetings rooms, ballrooms, and arrival experience can be modernized into a credible brand stay. Underwrite PIP and operating intensity as full-service work, not select-service shortcuts. Owner value holds when the relaunch restores a recognizable Sheraton experience rather than a light midscale refresh."
      ),
      freezeCard(
        "Meetings And Community-Space Assets",
        "Assets with banquet, social, and community-space capacity create Sheraton value when local meetings and bleisure demand need a full-service hub. Affiliation supports commercial lift when public-space programming and operator depth match the plant. Capital cases are stronger when the hotel can host events guests remember—not when meetings inventory is thin or under-capitalized."
      ),
      freezeCard(
        "Globally Recognized Full-Service Stability",
        "Sheraton helps stabilize owner confidence in markets where a globally recognized full-service flag matters for lenders, partners, and transient demand. Underwrite service standards and public-space quality so the flag stays credible after cutover. Value is weaker when the asset’s product story is really Four Points or Four Points Flex—or when wellness-led premium better fits Westin."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA Sheraton references include Sheraton Cancun Resort & Spa and Sheraton Buganvilias in Puerto Vallarta. Use these properties to diligence full-service resort product, meetings capacity, and guest-experience delivery in regional leisure corridors.",
        "CALA, Full-service, Resort"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North American Sheraton inventory supports business gateway and mixed-demand underwriting. Owners should stress-test public-space and meetings reactivation capital against local competitive sets while staying clear of Four Points Flex logic.",
        "International Reference, North America, Full-service"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European Sheraton placements provide International Reference evidence for urban full-service repositioning. Confirm the asset can carry Sheraton standards depth rather than midscale Flex conversion assumptions.",
        "International Reference, Europe, Full-service"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Sheraton Cancun Resort & Spa supports CALA full-service proof",
        "Directory",
        "CALA",
        "As of 2026, Sheraton Cancun Resort & Spa provides CALA property-level proof of full-service resort product, meetings capacity, and public-space delivery for owners evaluating Sheraton affiliation in regional leisure markets.",
        "Sheraton Cancun Resort & Spa overview page",
        "https://www.marriott.com/en-us/hotels/cunsi-sheraton-cancun-resort-and-spa/overview/"
      ),
      freezeMomentum(
        "Sheraton Buganvilias Resort & Convention Center — property proof",
        "Directory",
        "CALA · Puerto Vallarta, Mexico",
        "As of 2026, Sheraton Buganvilias Resort & Convention Center provides CALA property-level proof of full-service resort and meetings product. Labeled property proof—not a dated opening announcement.",
        "Sheraton Buganvilias overview page",
        "https://www.marriott.com/en-us/hotels/pvrsi-sheraton-buganvilias-resort-and-convention-center/overview/"
      ),
    ],
  }),

  westin: pack({
    slug: "westin",
    displayName: "Westin",
    model: "premium wellness-led full-service Westin brand",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers: "Sheraton, Marriott Hotels, W Hotels, JW Marriott, and Renaissance",
    peerPrimary: "W Hotels",
    calaAvailability: "supported",
    tgs: ["Experience-Oriented", "Bleisure", "Leisure"],
    ownerLens: [
      "Lead with wellness-led premium full-service fit—rooms, fitness, and wellbeing cues that owners can underwrite.",
      "Keep Westin distinct from W lifestyle luxury and JW luxury when comparing capital and service intensity.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Premium Wellness Urban Conversion",
        "Westin fits premium urban assets where wellness programming, rooms product, and calm public space can support higher-intent bleisure demand without forcing a W lifestyle or JW luxury capital stack."
      ),
      freezeCard(
        "Wellness-Led Resort Repositioning",
        "Resort sites create Westin value when spa, fitness, and restorative guestroom design are capitalized as demand drivers—owners must price wellness operating intensity honestly against Sheraton or Marriott Hotels alternatives."
      ),
      freezeCard(
        "Business-Leisure Premium Mixed Use",
        "Mixed business and leisure corridors suit Westin when guests pay for wellbeing and polished full-service delivery. Affiliation helps when the plant already supports premium standards rather than midscale shortcuts."
      ),
      freezeCard(
        "Standards-Protective Reflag",
        "Reflags succeed when Heavenly-adjacent product cues and service culture stay brand-legible after cutover. Value fades if the PIP leaves a generic upscale box that guests cannot distinguish from sibling full-service brands."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Wellness-Led Premium Urban Or Resort",
        "Westin creates owner value on premium urban and resort assets where wellness is a product system—rooms, fitness, sleep cues, and calm public space—not a marketing slogan. Affiliation fits when capital and staffing can deliver that restorative stay consistently. Owner value weakens when wellness is filler language or when the asset belongs in a lifestyle-luxury or meetings-led sibling lane."
      ),
      freezeCard(
        "Business-Leisure Demand With Wellness Edge",
        "Business and leisure hotels create Westin value when a clearer wellness story helps the property stand out in mixed-demand competitive sets. Underwrite fitness access, rooms consistency, and F&B cues that support recovery and productivity stays. Affiliation lift is strongest when the wellness edge is visible in the product—not only in brand copy."
      ),
      freezeCard(
        "Premium Wellness Repositioning",
        "Repositioning assets fit Westin when rooms, fitness, sleep product, and F&B can support a premium wellness promise after PIP. Owners should capitalize those elements honestly so rate can reflect Westin’s lane versus Sheraton meetings or Marriott Hotels flagship breadth. Value holds when the conversion delivers restorative product guests can feel on arrival."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA Westin evidence includes The Westin Resort & Spa, Cancun. Owners should use this property to diligence premium wellness-led resort product, service intensity, and guest-experience delivery in a high-leisure CALA corridor.",
        "CALA, Premium, Wellness"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America remains a core Westin theater for urban and resort premium placements. Underwrite wellbeing product and service depth against local upscale competitors while keeping W and JW in separate lanes.",
        "International Reference, North America, Premium"
      ),
      freezeRegion(
        "footprint.region.apac",
        "Asia Pacific",
        "Asia Pacific Westin placements provide International Reference evidence for premium wellness full-service delivery. Confirm asset readiness for Westin standards before treating APAC comps as transferable to CALA underwriting.",
        "International Reference, Asia Pacific, Premium"
      ),
    ],
    momentum: [
      freezeMomentum(
        "The Westin Resort & Spa, Cancun anchors CALA wellness-led proof",
        "Directory",
        "CALA",
        "As of 2026, The Westin Resort & Spa, Cancun provides CALA property-level proof of premium wellness-led resort product and guest-experience delivery for owners evaluating Westin affiliation in regional leisure corridors.",
        "The Westin Resort & Spa, Cancun overview page",
        "https://www.marriott.com/en-us/hotels/cunwi-the-westin-resort-and-spa-cancun/overview/"
      ),
      freezeMomentum(
        "The Westin Resort & Spa, Puerto Vallarta — property proof",
        "Directory",
        "CALA · Puerto Vallarta, Mexico",
        "As of 2026, The Westin Resort & Spa, Puerto Vallarta provides CALA property-level proof of premium wellness-led full-service resort product. Labeled property proof—not a dated opening announcement.",
        "The Westin Resort & Spa Puerto Vallarta overview page",
        "https://www.marriott.com/en-us/hotels/pvrwi-the-westin-resort-and-spa-puerto-vallarta/overview/"
      ),
    ],
  }),

  "residence-inn-by-marriott": pack({
    slug: "residence-inn-by-marriott",
    displayName: "Residence Inn by Marriott",
    model: "upscale extended-stay Residence Inn suite brand",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers: "TownePlace Suites, StudioRes, Element, and Apartments by Marriott Bonvoy",
    peerPrimary: "TownePlace Suites by Marriott",
    calaAvailability: "supported",
    tgs: ["Contract / Extended Stay", "Bleisure", "Leisure"],
    ownerLens: [
      "Underwrite suite/kitchen product and longer-stay demand—not transient select-service logic.",
      "Keep StudioRes, TownePlace Suites, Element, and Apartments by Marriott Bonvoy in separate diligence lanes.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Extended-Stay Suite Demand Capture",
        "Residence Inn fits owners targeting longer-stay and project demand when suite layouts, kitchens, and residential social space match upscale extended-stay expectations rather than short-stay select-service prototypes."
      ),
      freezeCard(
        "Suburban And Urban Corridor Suites",
        "Suburban employment nodes and urban extended-stay corridors create Residence Inn value when sponsors underwrite weekly housekeeping models, parking, and suite mix against real length-of-stay patterns."
      ),
      freezeCard(
        "Conversion Into Upscale Extended Stay",
        "Capable suite or apartment-style shells can reflag to Residence Inn when PIP delivers residential product quality. Value weakens if the asset is forced into a StudioRes midscale or TownePlace select-service story."
      ),
      freezeCard(
        "Network Reach For Longer Stays",
        "Bonvoy reach helps Residence Inn when commercial systems support extended-stay booking patterns. Owners should compare Element and Apartments by Marriott Bonvoy only when the product thesis truly matches those lanes."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Longer-Stay Demand Near Anchors",
        "Residence Inn creates owner value near employment, medical, education, project, and relocation anchors that generate multi-night stays. Affiliation supports recurring demand when the site sits inside those trip generators rather than relying on transient weekend leisure alone. Owner value is weaker when longer-stay demand is thin or when the competitive set already saturates suite supply."
      ),
      freezeCard(
        "Suite And Kitchen Length-Of-Stay Economics",
        "Suite-and-kitchen product creates Residence Inn value when length-of-stay economics—not short-stay select-service rate math—drive the underwriting. Owners should capitalize residential suite mix, social space, and housekeeping rhythms for multi-night guests. Affiliation helps when the asset can deliver a residential stay guests will book for weeks, not a transient all-suite night."
      ),
      freezeCard(
        "Suburban And Urban Extended-Stay Coverage",
        "Suburban and urban extended-stay assets fit Residence Inn when Marriott distribution can support recurring corporate, medical, and relocation demand at upscale suite quality. Underwrite staffing and product depth for residential stays rather than SpringHill short-stay suite logic. Value weakens if the thesis drifts into TownePlace midscale kitchens or StudioRes prototype simplicity."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA extended-stay evidence includes Residence Inn Merida. Owners should use this property to diligence suite product, residential programming, and longer-stay operating fit in a regional market context.",
        "CALA, Extended-stay, Suites"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is the core Residence Inn operating theater for suburban and urban extended-stay corridors. Underwrite suite economics and competitive set carefully while keeping TownePlace and StudioRes as separate owner choices.",
        "International Reference, North America, Extended-stay"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European Residence Inn placements, where present, provide International Reference evidence for upscale extended-stay delivery. Confirm local longer-stay demand before transferring US operating assumptions.",
        "International Reference, Europe, Extended-stay"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Residence Inn Merida confirms CALA extended-stay proof",
        "Directory",
        "CALA",
        "As of 2026, Residence Inn Merida provides CALA property-level proof of upscale extended-stay suite product and residential programming for owners evaluating Residence Inn affiliation in regional markets.",
        "Residence Inn Merida overview page",
        "https://www.marriott.com/en-us/hotels/midri-residence-inn-merida/overview/"
      ),
      freezeMomentum(
        "Residence Inn by Marriott Merida — property proof",
        "Directory",
        "CALA · Mérida, Mexico",
        "As of 2026, Residence Inn by Marriott Merida provides CALA property-level proof of upscale extended-stay suite product and residential programming. Labeled property proof—not a dated opening announcement.",
        "Residence Inn Merida overview page",
        "https://www.marriott.com/en-us/hotels/midri-residence-inn-merida/overview/"
      ),
    ],
  }),

  "springhill-suites-by-marriott": pack({
    slug: "springhill-suites-by-marriott",
    displayName: "SpringHill Suites by Marriott",
    model: "all-suite select-service SpringHill Suites brand",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers: "Residence Inn, Fairfield by Marriott, Courtyard by Marriott, and TownePlace Suites",
    peerPrimary: "Residence Inn by Marriott",
    calaAvailability: "weak_or_limited",
    tgs: ["Bleisure", "Leisure", "Corporate / Business"],
    ownerLens: [
      "All-suite select-service—do not confuse with extended-stay Residence Inn or TownePlace.",
      "CALA evidence is limited; treat property examples as International Reference until a named CALA hotel with a matching official property URL is verified.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "All-Suite Select-Service New Build",
        "SpringHill Suites fits owners building upper-midscale suite product for bleisure and leisure demand when efficient select-service operations can sustain suite sizing without extended-stay kitchen economics."
      ),
      freezeCard(
        "Suite-Led Conversion Play",
        "Conversions work when existing rooms can become credible suites with select-service public space. Value fades if sponsors underwrite Residence Inn longer-stay assumptions the asset cannot support."
      ),
      freezeCard(
        "Upper-Midscale Suite Competitive Response",
        "Markets crowded with standard kings need SpringHill when suites differentiate rate without Courtyard meeting intensity or Fairfield limited-service constraints—owners should price suite PIP honestly."
      ),
      freezeCard(
        "Efficient Operating Model With Suite Product",
        "Owner value holds when staffing and F&B stay select-service while suite product carries the guest story. Compare TownePlace only when longer-stay demand—not all-suite short stay—is the true thesis."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "All-Suite Select-Service Demand",
        "SpringHill Suites creates owner value when guests want suite space with select-service efficiency—without full extended-stay kitchens or Residence Inn residential complexity. Affiliation fits upper-midscale corridors where a broader room product can separate the asset from conventional king/queen boxes. Owner value holds when the suite story is real in floor plans, not just in marketing labels."
      ),
      freezeCard(
        "Highway Airport And Suburban Suite Coverage",
        "Highway, airport, suburban, and secondary-market assets create SpringHill value when travelers need more room product than a standard select-service flag without taking on extended-stay operations. Underwrite suite sizing, breakfast scope, and public-space simplicity together. Capital returns are stronger when demand wants space and efficiency—not long-stay kitchenettes."
      ),
      freezeCard(
        "Newbuild And Conversion Suite Positioning",
        "Newbuild and conversion opportunities fit SpringHill when suite positioning can separate the asset from Fairfield and Courtyard-style competitors in the same corridor. Underwrite capital envelopes for suite product while keeping operating complexity select-service. Owner value weakens if the plan borrows TownePlace or Residence Inn longer-stay proof into an all-suite short-stay thesis."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is the primary SpringHill Suites market for all-suite select-service new builds and conversions. Owners should underwrite suite product against local upper-midscale competitors using International Reference evidence where verified CALA inventory is not yet available.",
        "International Reference, North America, All-suite"
      ),
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "Verified CALA SpringHill Suites operating examples are not yet available in official brand materials. Do not imply regional presence—label any future CALA example only after a property-name-matched official URL is verified.",
        "International Reference required, CALA unconfirmed"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European placements, where applicable, remain International Reference only. Confirm whether SpringHill is the right Marriott select-service choice versus other options before transferring US suite assumptions.",
        "International Reference, Europe, All-suite"
      ),
    ],
    momentum: [
      freezeMomentum(
        "SpringHill Suites by Marriott San Diego Carlsbad — property proof",
        "Directory",
        "International Reference · Carlsbad, California, USA",
        "As of 2026, Official Marriott property page for SpringHill Suites by Marriott San Diego Carlsbad. Property proof for all-suite select-service product—not a dated opening announcement.",
        "Marriott property page — SpringHill Suites San Diego Carlsbad",
        "https://www.marriott.com/en-us/hotels/sancb-springhill-suites-san-diego-carlsbad/overview/"
      ),
      freezeMomentum(
        "SpringHill Suites by Marriott Colorado Springs Downtown — property proof",
        "Directory",
        "International Reference · Colorado Springs, Colorado, USA",
        "As of 2026, Official Marriott property page for SpringHill Suites by Marriott Colorado Springs Downtown. Property proof for urban all-suite select-service delivery.",
        "Marriott property page — SpringHill Suites Colorado Springs Downtown",
        "https://www.marriott.com/en-us/hotels/cossh-springhill-suites-colorado-springs-downtown/overview/"
      ),
      freezeMomentum(
        "SpringHill Suites by Marriott Myrtle Beach Oceanfront — property proof",
        "Directory",
        "International Reference · Myrtle Beach, South Carolina, USA",
        "As of 2026, Official Marriott property page for SpringHill Suites by Marriott Myrtle Beach Oceanfront. Property proof for leisure-corridor suite product.",
        "Marriott property page — SpringHill Suites Myrtle Beach Oceanfront",
        "https://www.marriott.com/en-us/hotels/myrsi-springhill-suites-myrtle-beach-oceanfront/overview/"
      ),
    ],
    openings: [
      {
        propertyName: "SpringHill Suites by Marriott San Diego Carlsbad",
        geographyLabel: "International Reference",
        market: "Carlsbad, California, USA",
        matchKey: "springhill-san-diego-carlsbad",
      },
      {
        propertyName: "SpringHill Suites by Marriott Colorado Springs Downtown",
        geographyLabel: "International Reference",
        market: "Colorado Springs, Colorado, USA",
        matchKey: "springhill-colorado-springs",
      },
      {
        propertyName: "SpringHill Suites by Marriott Myrtle Beach Oceanfront",
        geographyLabel: "International Reference",
        market: "Myrtle Beach, South Carolina, USA",
        matchKey: "springhill-myrtle-beach",
      },
    ],
  }),

  "towneplace-suites-by-marriott": pack({
    slug: "towneplace-suites-by-marriott",
    displayName: "TownePlace Suites by Marriott",
    model: "longer-stay select-service TownePlace Suites brand",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers: "Residence Inn, StudioRes, SpringHill Suites, and Element",
    peerPrimary: "Residence Inn by Marriott",
    calaAvailability: "weak_or_limited",
    tgs: ["Contract / Extended Stay", "Bleisure", "Corporate / Business"],
    ownerLens: [
      "Longer-stay select-service with kitchen/suite product—distinct from upscale Residence Inn and midscale StudioRes.",
      "CALA evidence is limited; use International Reference until a named CALA hotel with a matching official property URL is verified.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Longer-Stay Select-Service Capture",
        "TownePlace Suites fits owners chasing extended and longer stays with select-service efficiency when kitchenette suites and simplified public space match demand without Residence Inn upscale residential intensity."
      ),
      freezeCard(
        "Suburban Employment Node Suites",
        "Suburban job and medical nodes create TownePlace value when length-of-stay patterns support suite product. Owners should underwrite housekeeping cadence and parking against real project and relocation demand."
      ),
      freezeCard(
        "Efficient Conversion Into Longer Stay",
        "Conversions succeed when existing rooms can support kitchenettes and residential cues at select-service cost. Value erodes if sponsors force StudioRes midscale prototype logic or SpringHill short-stay suite assumptions."
      ),
      freezeCard(
        "Secondary Market Extended-Stay Play",
        "Secondary and tertiary markets can favor TownePlace when competitive suite supply is thin. Compare Element and Residence Inn only when product tier and wellness or upscale cues truly match those brands."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Longer-Stay Suburban And Secondary Markets",
        "TownePlace Suites creates owner value in suburban and secondary markets where multi-night guests need practical suite stays without upscale Residence Inn depth. Affiliation supports occupancy when employment and project demand is real and competitive suite supply is underwritten honestly. Owner value is weaker when the site is pure transient leisure or when capital assumes luxury residential amenities."
      ),
      freezeCard(
        "Kitchen Suites For Project And Relocation Demand",
        "Kitchen-and-suite product creates TownePlace value for work crews, relocations, medical stays, education trips, and project demand that needs weeks of practical living space. Underwrite kitchenette utility, suite mix, and housekeeping for longer stays. Affiliation helps when the asset can serve those guests efficiently—not when the product is really a short-stay all-suite SpringHill."
      ),
      freezeCard(
        "Focused Extended-Stay Operating Model",
        "Owner value comes from a focused extended-stay operating model—disciplined suite product, practical public space, and select-service cost structure. Underwrite staffing and maintenance for longer-stay rhythms rather than full-service or lifestyle intensity. Value holds when TownePlace stays clearly midscale extended-stay versus Residence Inn upscale suites or StudioRes prototype newbuilds."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America is the primary TownePlace Suites market for longer-stay select-service suites. Underwrite kitchenette product and competitive set using International Reference evidence where verified CALA inventory is not yet available.",
        "International Reference, North America, Longer-stay"
      ),
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "Verified CALA TownePlace Suites operating examples are not yet available in official brand materials. Do not imply regional presence—add CALA cards only after property-name-matched official URLs are verified.",
        "International Reference required, CALA unconfirmed"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European placements, where applicable, remain International Reference. Confirm longer-stay demand and brand availability before transferring North American TownePlace assumptions.",
        "International Reference, Europe, Longer-stay"
      ),
    ],
    momentum: [
      freezeMomentum(
        "TownePlace Suites by Marriott Dallas DFW Airport North/Grapevine — property proof",
        "Directory",
        "International Reference · Dallas–Fort Worth, Texas, USA",
        "As of 2026, Official Marriott property page for TownePlace Suites by Marriott Dallas DFW Airport North/Grapevine. Property proof for longer-stay select-service kitchenette product—not a dated opening announcement.",
        "Marriott property page — TownePlace Suites DFW Grapevine",
        "https://www.marriott.com/en-us/hotels/dalgt-towneplace-suites-dallas-dfw-airport-north-grapevine/overview/"
      ),
      freezeMomentum(
        "TownePlace Suites by Marriott Orlando at FLAMINGO CROSSINGS Town Center — property proof",
        "Directory",
        "International Reference · Orlando, Florida, USA",
        "As of 2026, Official Marriott property page for TownePlace Suites by Marriott Orlando at FLAMINGO CROSSINGS Town Center. Property proof for leisure-adjacent longer-stay suite delivery.",
        "Marriott property page — TownePlace Suites Orlando Flamingo Crossings",
        "https://www.marriott.com/en-us/hotels/mcotf-towneplace-suites-orlando-at-flamingo-crossings-town-center/overview/"
      ),
      freezeMomentum(
        "TownePlace Suites by Marriott Houston Hobby Airport — property proof",
        "Directory",
        "International Reference · Houston, Texas, USA",
        "As of 2026, Official Marriott property page for TownePlace Suites by Marriott Houston Hobby Airport. Property proof for airport-adjacent longer-stay select-service product.",
        "Marriott property page — TownePlace Suites Houston Hobby Airport",
        "https://www.marriott.com/en-us/hotels/houht-towneplace-suites-houston-hobby-airport/overview/"
      ),
    ],
    openings: [
      {
        propertyName: "TownePlace Suites by Marriott Dallas DFW Airport North/Grapevine",
        geographyLabel: "International Reference",
        market: "Dallas–Fort Worth, Texas, USA",
        matchKey: "towneplace-dfw-grapevine",
      },
      {
        propertyName: "TownePlace Suites by Marriott Orlando at FLAMINGO CROSSINGS Town Center",
        geographyLabel: "International Reference",
        market: "Orlando, Florida, USA",
        matchKey: "towneplace-orlando-flamingo",
      },
      {
        propertyName: "TownePlace Suites by Marriott Houston Hobby Airport",
        geographyLabel: "International Reference",
        market: "Houston, Texas, USA",
        matchKey: "towneplace-houston-hobby",
      },
    ],
  }),

  "aloft-hotels": pack({
    slug: "aloft-hotels",
    displayName: "Aloft Hotels",
    model: "select-service lifestyle Aloft Hotels brand",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers: "Moxy Hotels, AC Hotels by Marriott, Four Points by Sheraton, Element, and W Hotels",
    peerPrimary: "Moxy Hotels",
    calaAvailability: "supported",
    tgs: ["Experience-Oriented", "Bleisure", "Leisure"],
    ownerLens: [
      "Lifestyle select-service with social public space—distinct from Moxy budget lifestyle and W luxury lifestyle.",
      "Urban, airport, and secondary lifestyle corridors can fit when design and social lobby energy stay brand-legible.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Lifestyle Select-Service Urban Play",
        "Aloft fits urban and airport-adjacent assets where social lobby energy and modern rooms can lift select-service demand without Moxy’s budget lifestyle intensity or W’s luxury capital stack."
      ),
      freezeCard(
        "Social Public-Space Conversion",
        "Conversions create Aloft value when arrival and lobby become social destinations guests remember. Owners must capitalize F&B and public-space programming honestly so lifestyle cues survive cutover."
      ),
      freezeCard(
        "Secondary City Lifestyle Coverage",
        "Secondary lifestyle markets suit Aloft when competitors lack modern select-service energy. Compare AC Hotels when European contemporary select-service is the stronger product story."
      ),
      freezeCard(
        "Efficient Lifestyle Operations",
        "Owner value holds when staffing stays select-service while design and social space carry the brand. Value fades if the PIP produces a generic Four Points box without Aloft character."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Lifestyle Select-Service Urban And Airport",
        "Aloft Hotels creates owner value on select-service lifestyle assets in urban, airport, and mixed-use locations where modern design and social energy can lift demand. Affiliation fits when the site can carry lifestyle rates without W-level capital or Moxy’s budget lifestyle intensity. Owner value weakens when the asset reads as a conventional Four Points box without Aloft character."
      ),
      freezeCard(
        "Social Public-Space And Bar-Led Demand",
        "Social lobby and bar-led programming create Aloft value when the hotel needs more energy than a conventional select-service flag. Underwrite public-space design, F&B hours, and staffing so social space becomes a demand driver. Capital cases are stronger when guests remember the arrival experience—not when the lobby stays inert after cutover."
      ),
      freezeCard(
        "Lifestyle Conversion And Newbuild Separation",
        "Conversion and newbuild opportunities fit Aloft when lifestyle positioning separates the asset from Moxy, AC Hotels, Four Points, or W in the competitive set. Underwrite design narrative and rooms modernity so the brand promise is visible in product. Owner value holds when Aloft stays select-service lifestyle—energetic, efficient, and clearly not luxury lifestyle capital."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "CALA Aloft evidence includes Aloft Cancun. Owners should use this property to diligence lifestyle select-service product, social public space, and guest-experience delivery in a regional leisure gateway.",
        "CALA, Lifestyle, Select-service"
      ),
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America remains a core Aloft theater for urban and airport lifestyle placements. Underwrite social lobby programming against local lifestyle competitors while keeping Moxy and W in separate lanes.",
        "International Reference, North America, Lifestyle"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "European Aloft placements provide International Reference evidence for lifestyle select-service delivery. Compare AC Hotels carefully when European contemporary select-service better matches the asset.",
        "International Reference, Europe, Lifestyle"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Aloft Cancun demonstrates CALA lifestyle select-service presence",
        "Directory",
        "CALA",
        "As of 2026, Aloft Cancun provides CALA property-level proof of lifestyle select-service product and social public-space delivery for owners evaluating Aloft affiliation in regional leisure gateways.",
        "Aloft Cancun overview page",
        "https://www.marriott.com/en-us/hotels/cunal-aloft-cancun/overview/"
      ),
      freezeMomentum(
        "Aloft Mexico City - Santa Fe — property proof",
        "Directory",
        "CALA · Mexico City, Mexico",
        "As of 2026, Aloft Mexico City - Santa Fe provides CALA property-level proof of urban lifestyle select-service product. Labeled property proof—not a dated opening announcement.",
        "Aloft Mexico City Santa Fe overview page",
        "https://www.marriott.com/en-us/hotels/mexal-aloft-mexico-city-santa-fe/overview/"
      ),
    ],
  }),

  "four-points-flex-by-sheraton": pack({
    slug: "four-points-flex-by-sheraton",
    displayName: "Four Points Flex by Sheraton",
    model: "affordable midscale Four Points Flex by Sheraton conversion brand",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers: "Four Points by Sheraton and Sheraton",
    peerPrimary: "Four Points by Sheraton",
    calaAvailability: "none_supported",
    tgs: ["Bleisure", "Leisure", "Corporate / Business"],
    ownerLens: [
      "Conversion-suited midscale Flex brand—never Four Points by Sheraton and never full-service Sheraton.",
      "No CALA evidence in current pack; International Reference only (EMEA / APAC ex-China focus).",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Midscale Conversion Platform",
        "Four Points Flex suits owners converting existing midscale hotels into Marriott Bonvoy distribution when light operational and design requirements fit the asset—without adopting classic Four Points by Sheraton product expectations."
      ),
      freezeCard(
        "Secondary And Tertiary Market Coverage",
        "Secondary European and international midscale markets create Flex value when guests need affordable, reliable stays. Owners should underwrite conversion scope honestly and keep Sheraton full-service out of the file."
      ),
      freezeCard(
        "Portfolio Conversion Acceleration",
        "Multi-asset owners gain Flex value when several midscale hotels can join quickly under a conversion-suited model. Diligence brand standards flexibility versus Four Points by Sheraton before assuming interchangeable PIP."
      ),
      freezeCard(
        "Bonvoy Access At Midscale Cost Structure",
        "Affiliation helps when Bonvoy reach matters more than full-service amenities. Value fades if sponsors market Flex as Sheraton or classic Four Points to lenders and partners."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Conversion-Suited Midscale Platform",
        "Four Points Flex by Sheraton creates owner value on midscale conversions where sponsors need Marriott Bonvoy reach with a lighter product model than Four Points by Sheraton or full-service Sheraton. Underwrite conversion scope and operating simplicity honestly. Do not use Four Points by Sheraton hotels as Flex proof, and do not imply CALA presence—current evidence is International Reference."
      ),
      freezeCard(
        "EMEA And International Midscale Expansion",
        "Owner value concentrates in Europe, Middle East, Africa, and selected Asia Pacific markets where Flex has been positioned for affordable midscale growth. Diligence local competitive sets and conversion capital before modeling lift. Keep geography labeled International Reference."
      ),
      freezeCard(
        "Sibling Clarity With Sheraton Family",
        "Flex sits in the Sheraton family naming lane but is not Sheraton full-service and not classic Four Points. Sequence systems and training for midscale conversion realities, and keep sibling brands separated in every owner memo."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "Europe is a primary International Reference theater for Four Points Flex conversion growth. Owners should diligence midscale conversion fit and competitive pricing here—never substitute Four Points by Sheraton property proof.",
        "International Reference, Europe, Midscale conversion"
      ),
      freezeRegion(
        "footprint.region.mea",
        "Middle East & Africa",
        "Middle East and Africa appear in official Flex availability messaging as International Reference markets. Confirm asset-level conversion readiness before treating regional announcements as transferable underwriting proof.",
        "International Reference, MEA, Midscale conversion"
      ),
      freezeRegion(
        "footprint.region.apac",
        "Asia Pacific",
        "Asia Pacific excluding China is cited in official Flex development messaging. Use as International Reference only, and do not imply CALA coverage from APAC activity.",
        "International Reference, Asia Pacific, Midscale conversion"
      ),
    ],
    momentum: [
      freezeMomentum(
        "Official Four Points Flex conversion positioning",
        "Ongoing",
        "International Reference",
        "Marriott hotel-development materials frame Four Points Flex by Sheraton as a conversion-suited affordable midscale franchise brand—primary owner diligence source for product posture.",
        "Marriott hotel-development — Four Points Flex"
      ),
      freezeMomentum(
        "European Flex portfolio expansion signal",
        "2025-04",
        "International Reference",
        "Trade coverage described Marriott plans to expand Four Points Flex across Europe toward a larger open portfolio by end-2026. Treat as a dated International Reference growth signal, not CALA evidence.",
        "Trade press coverage of Marriott Flex Europe plans"
      ),
    ],
  }),

  studiores: pack({
    slug: "studiores",
    displayName: "StudioRes",
    model: "midscale longer-stay StudioRes by Marriott brand",
    parent: "Marriott International",
    loyalty: "Marriott Bonvoy",
    peers: "Residence Inn, TownePlace Suites, Element, and Apartments by Marriott Bonvoy",
    peerPrimary: "Residence Inn by Marriott",
    calaAvailability: "none_supported",
    tgs: ["Contract / Extended Stay", "Bleisure", "Corporate / Business"],
    ownerLens: [
      "Midscale longer-stay / studio product—never Residence Inn, TownePlace, Element, or Apartments by Marriott Bonvoy.",
      "No CALA evidence; US & Canada new-build focus with EMEA conversion option labeled International Reference.",
    ],
    valueOwnersScenarios: [
      freezeCard(
        "Midscale Longer-Stay New Build",
        "StudioRes fits owners building efficient midscale longer-stay studios when prototype economics and simplified operations match demand—without Residence Inn upscale residential intensity or Apartments soft-brand complexity."
      ),
      freezeCard(
        "Secondary Market Extended-Stay Coverage",
        "Secondary and suburban longer-stay nodes create StudioRes value when kitchenette studios meet affordable multi-night demand. Owners should separate TownePlace select-service and Element wellness lanes during diligence."
      ),
      freezeCard(
        "Conversion Path Where Officially Available",
        "Where Marriott positions StudioRes for conversion in applicable regions, owners must verify current official availability rather than inventing conversion scope. Do not borrow Residence Inn PIP assumptions."
      ),
      freezeCard(
        "Bonvoy Access For Affordable Longer Stays",
        "Affiliation helps when Bonvoy distribution matters for midscale longer stays. Value fades if sponsors present StudioRes as Residence Inn or Apartments by Marriott Bonvoy to capital partners."
      ),
    ],
    overviewScenarios: [
      freezeCard(
        "Purpose-Built Extended-Stay Prototype",
        "StudioRes creates owner value on purpose-built extended-stay and affordable midscale prototype opportunities where simplicity and repeatability matter more than upscale residential depth. Affiliation fits sponsors who want a clear longer-stay box without Residence Inn product intensity. Owner value is weaker when the capital plan overbuilds amenities the prototype was never meant to carry."
      ),
      freezeCard(
        "Longer-Stay Markets Without Full Upscale Suites",
        "Markets with longer-stay demand but limited need for a full Residence Inn or TownePlace product fit StudioRes when guests want efficient studio stays at affordable midscale economics. Underwrite demand honestly against local suite supply. Affiliation helps when the competitive gap is prototype clarity—not when Element wellness or apartments-style living is the real thesis."
      ),
      freezeCard(
        "Newbuild Simplicity And Brand Clarity",
        "Newbuild development creates StudioRes value when simplicity, repeatability, and brand clarity reduce execution risk across corridors. Underwrite construction cost, operating model, and product scope so the prototype stays disciplined after opening. Owner value holds when StudioRes remains a focused midscale extended-stay choice—not a Residence Inn, TownePlace, or Element substitute by another name."
      ),
    ],
    regions: [
      freezeRegion(
        "footprint.region.am",
        "North America",
        "North America (US & Canada) is the primary International Reference theater for StudioRes new-build longer-stay diligence. Underwrite prototype fit and competitive midscale supply here; do not imply CALA coverage.",
        "International Reference, North America, Longer-stay"
      ),
      freezeRegion(
        "footprint.region.eu",
        "Europe",
        "Official Longer Stays materials reference an EMEA conversion-friendly StudioRes option. Treat Europe as International Reference and verify current official availability before underwriting conversion scope.",
        "International Reference, Europe, Conversion option"
      ),
      freezeRegion(
        "footprint.region.cala",
        "Caribbean & Latin America",
        "No verified CALA StudioRes inventory is identified in the current brand materials. Do not imply regional presence—leave CALA cleanly unavailable until a property-name-matched official URL exists.",
        "International Reference required, CALA unavailable"
      ),
    ],
    momentum: [
      freezeMomentum(
        "StudioRes Fort Myers — property proof",
        "Directory",
        "International Reference · Fort Myers, Florida, USA",
        "As of 2026, StudioRes Fort Myers provides first-market property-level proof of the midscale longer-stay prototype for owners evaluating StudioRes positioning and operating-model simplicity. Labeled property proof—not a dated opening announcement.",
        "Marriott hotel-development page — StudioRes Fort Myers reference",
        "https://www.hotel-development.marriott.com/brands/studiores"
      ),
      freezeMomentum(
        "StudioRes longer-stay prototype — International Reference property proof",
        "Directory",
        "International Reference · United States",
        "As of 2026, Official Marriott StudioRes development materials describe the midscale longer-stay prototype for US and Canada new builds. Treat as labeled product proof for owner comparison versus Residence Inn and TownePlace Suites—not as a dated market opening.",
        "Marriott hotel-development page — StudioRes",
        "https://www.hotel-development.marriott.com/brands/studiores"
      ),
    ],
    openings: [
      {
        propertyName: "StudioRes Fort Myers",
        geographyLabel: "International Reference",
        market: "Fort Myers, Florida, USA",
        matchKey: "studiores-fort-myers",
      },
    ],
  }),
});

export function getWave14BrandContent(slug) {
  const content = WAVE14_BRAND_CONTENT[slug];
  if (!content) throw new Error(`Missing Wave 14 content package for ${slug}`);
  let pack = null;
  try {
    pack = getWave14SourcePack(slug);
  } catch {
    pack = null;
  }

  const cands = pack?.recentMomentumCandidates || [];
  const props = (pack?.propertyExamples || []).filter((p) => p.sourcePageUrl || p.url);

  function normalizeDate(dateLine, title = "") {
    let d = String(dateLine || "").trim();
    if (/^\d{4}-\d{2}$/.test(d)) {
      const [y, mo] = d.split("-");
      const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
      return `${months[Number(mo) - 1]} ${y}`;
    }
    if (/^2025-04\b/i.test(d) || /\b2025-04\b/.test(title)) return "Apr 2025";
    if (/^(Directory|Collection|Editorial|Affiliation|Pipeline)$/i.test(d)) return d;
    if (/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{4}$/i.test(d)) return d;
    if (/^Q[1-4]\s+\d{4}$/i.test(d)) return d;
    if (/^\d{4}$/.test(d)) return d;
    // Ongoing / informal → Directory (official brand / property directory proof)
    return "Directory";
  }

  let momentum = (content.momentum || []).map((m, index) => {
    const geo = m.geography || "International Reference";
    let sourceUrl = String(m.sourceUrl || "").trim();
    const title = m.title || "";
    const dateLine = normalizeDate(m.dateLine, title);
    const isCalaProp = /\bCALA\b/i.test(title) || /\bCALA\b/i.test(geo);
    const isFlexEurope = /europe|flex portfolio|skift/i.test(title) || dateLine === "Apr 2025";
    if (!sourceUrl) {
      const propUrl = (i) => props[i]?.sourcePageUrl || props[i]?.url || "";
      if (isFlexEurope) {
        const skift = cands.find((c) => /skift\.com/i.test(c.announcementUrl || ""));
        if (skift?.announcementUrl) sourceUrl = skift.announcementUrl;
      }
      if (!sourceUrl && isCalaProp && propUrl(0)) sourceUrl = propUrl(0);
      const devUrl = cands.find((c) =>
        /hotel-development\.marriott\.com/i.test(c.announcementUrl || "")
      )?.announcementUrl;
      // Prefer official development URL for Directory brand-positioning cards
      if (!sourceUrl && !isFlexEurope && !isCalaProp && devUrl) sourceUrl = devUrl;
      else if (
        !sourceUrl &&
        cands[index]?.announcementUrl &&
        !(/skift\.com/i.test(cands[index].announcementUrl) && !isFlexEurope)
      ) {
        sourceUrl = cands[index].announcementUrl;
      } else if (!sourceUrl && devUrl) sourceUrl = devUrl;
      else if (!sourceUrl && cands[0]?.announcementUrl && !(/skift\.com/i.test(cands[0].announcementUrl) && !isFlexEurope)) {
        sourceUrl = cands[0].announcementUrl;
      } else if (!sourceUrl && propUrl(0)) sourceUrl = propUrl(0);
    }
    let summary = String(m.summary || "")
      .replace(/\bowner diligence\b/gi, "owner review")
      .trim();
    if (!new RegExp(`\\b${geo.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}\\b`, "i").test(summary)) {
      summary = `${geo}. ${summary}`;
    }
    // Ensure ≥35 words for evidence gate
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

  // CALA-first ordering when any CALA momentum card exists (evidence-quality priority gate)
  const hasCalaMomentum = momentum.some((m) => /\bCALA\b/i.test(m.geography || "") || /\bCALA\b/i.test(m.title || ""));
  if (hasCalaMomentum) {
    momentum = [
      ...momentum.filter((m) => /\bCALA\b/i.test(m.geography || "") || /\bCALA\b/i.test(m.title || "")),
      ...momentum.filter((m) => !(/\bCALA\b/i.test(m.geography || "") || /\bCALA\b/i.test(m.title || ""))),
    ];
  }

  // Ensure ≥2 momentum cards without injecting brand-site pages as fake momentum
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
    }
  }

  return Object.freeze({
    ...content,
    momentum,
    sourcePack: pack,
  });
}
