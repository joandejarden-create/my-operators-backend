/**
 * Brand Setup — Project Fit form payloads for Choice Hotels International brands.
 * Informed by tier-1 explorer profiles, FDD Item 19, CHI architecture (Oct 2025), and choicehotelsdevelopment.com positioning.
 */

/** @typedef {'economy'|'midscale'|'upperMidscale'|'upscale'|'extendedStay'|'softCollection'|'luxuryCollection'} Segment */

/** Brands requested for full Project Fit population */
export const TARGET_BRANDS = [
  "Ascend Hotel Collection",
  "Cambria Hotels",
  "Clarion",
  "Comfort Inn & Suites",
  "MainStay Suites",
  "Quality Inn",
  "Sleep Inn",
  "Econo Lodge",
  "Rodeway Inn",
  "Suburban Studios",
  "WoodSpring Suites",
  "Radisson (Choice)",
  "Radisson RED  (Choice)",
  "Radisson Collection  (Choice)",
  "Park Plaza (Choice)",
  "Park Inn by Radisson (Choice)",
  "Country Inn & Suites by Radisson (Choice)",
  "Radisson Blu (Choice)",
  "Radisson Individual (Choice)",
  "Clarion Pointe",
  "Radisson Inn & Suites",
  "Everhome Suites",
];

const RTG = "Choice Hotels' Room to be Green®";

const US_CALA_MARKETS = [
  "United States (Broad)",
  "Canada",
  "Mexico",
  "Central America",
  "Caribbean",
  "South America",
  "Latin America (Broad)",
];

const US_ONLY_MARKETS = ["United States (Broad)", "Canada"];

/** @param {Segment} segment */
function segmentDefaults(segment) {
  /** @type {Record<string, unknown>} */
  const base = {
    idealProjectTypes: [
      "New Build",
      "Conversion / Reflag",
      "Renovation / Repositioning",
      "Expansion / Add-on",
    ],
    idealAgreementTypes: ["Franchise Only", "Flexible/Open", "Brand + Third-Party Mgmt. (Combined)"],
    projectStage: [
      "Land Under Control Only",
      "Entitlements in Process",
      "Fully Entitled",
      "Under Construction",
      "Stabilized Operating Asset",
    ],
    ownerInvolvementLevel: ["Silent Investor", "High-Level Oversight Only", "Hands-On in Operations"],
    ownerNonNegotiableTypes: [
      "Key Vendors / Contracts",
      "ADR / Positioning Philosophy",
      "Minimum Services / Amenities",
    ],
    capitalStatus: [
      "Equity and Debt Fully Committed",
      "Equity Committed, Debt in Process",
      "Equity in Process, Debt Not Started",
    ],
    brandStatus: ["Brand Already Selected", "Shortlisted Brands", "Open to Operator Recommendation Only"],
    feeExpectationVsMarket: ["In Line with Market Fees", "Open to Incentive Structures / Performance Fees"],
    exitHorizon: ["Hold 5–10 Years", "Hold 10+ Years", "Sell or Recap Within 3–5 Years"],
    dateFlexibility: "Targets with Some Flexibility",
    preferredOwnerType: "Developer / Builder",
    coBrandingAllowed: "Case-by-case",
    brandedResidencesAllowed: "No",
    mixedUseAllowed: "Case-by-case",
    whoPaysForPIP: "Owner",
    milestoneOperatorSelectionMinMonths: 3,
    milestoneConstructionStartMinMonths: 4,
    milestoneSoftOpeningMinMonths: 6,
    milestoneGrandOpeningMinMonths: 2,
    ownerNonNegotiables:
      "Franchise agreement governs pricing, brand standards, and capital plans. Owners retain asset control; brand approval required for material design, F&B, and meeting-product changes.",
    capexSupport:
      "Owners fund franchise-required PIP, FF&E reserves, and opening working capital per the disclosure document. Limited brand key money—confirm regional development incentives in LOI.",
    esgExpectations: `Projects should meet ${RTG} and local code. Upscale and full-service hotels should track energy, water, and waste programs aligned with Choice corporate ESG reporting where required.`,
    marketsToAvoid: [],
  };

  if (segment === "economy") {
    return {
      ...base,
      segment,
      idealBuildingTypes: ["Low-Rise", "Mid-Rise", "Historic / Renovated"],
      idealRoomCountMin: 40,
      idealRoomCountMax: 120,
      idealProjectSizeMin: 50,
      idealProjectSizeMax: 90,
      minReqOperatorExperienceYears: 1,
      minLeadTimeMonths: 6,
      ownerHotelExperience: "Some Prior Hotel Experience",
      priorityMarkets: US_ONLY_MARKETS,
      pipRepositioningDetails:
        "Light to moderate PIPs for conversion—fresh guestrooms, casegoods, soft goods, and exterior/logo compliance. Heavy structural or full public-space gut typically re-scoped to midscale flags.",
      knownRedFlags:
        "Declining interstate traffic counts without replacement demand; unresolved environmental or ADA violations; persistent crime perception; franchise defaults on sister assets without cure plan.",
      idealProjectsAdditionalNotes:
        "Economy operators must accept lean staffing, minimal F&B, and lower loyalty contribution versus midscale—underwrite on net fees and OTA mix, not headline occupancy alone.",
    };
  }

  if (segment === "midscale") {
    return {
      ...base,
      segment,
      idealBuildingTypes: ["Low-Rise", "Mid-Rise", "Historic / Renovated"],
      idealRoomCountMin: 60,
      idealRoomCountMax: 200,
      idealProjectSizeMin: 80,
      idealProjectSizeMax: 150,
      minReqOperatorExperienceYears: 2,
      minLeadTimeMonths: 12,
      ownerHotelExperience: "Some Prior Hotel Experience",
      priorityMarkets: US_CALA_MARKETS,
      pipRepositioningDetails:
        "Conversion PIPs typically include guestroom refresh, breakfast area compliance, signage, and technology. Moderate repositioning acceptable when meeting space or F&B scope matches brand tier.",
      knownRedFlags:
        "Breakfast infrastructure that cannot meet brand standards without full rebuild; chronic underperformance vs. STR comp set; unresolved franchise litigation; cap stack not sized for PIP + ramp.",
    };
  }

  if (segment === "upperMidscale") {
    return {
      ...base,
      segment,
      idealBuildingTypes: ["Low-Rise", "Mid-Rise", "Mixed-Use", "Podium / Tower"],
      idealRoomCountMin: 80,
      idealRoomCountMax: 250,
      idealProjectSizeMin: 100,
      idealProjectSizeMax: 200,
      minReqOperatorExperienceYears: 3,
      minLeadTimeMonths: 14,
      ownerHotelExperience: "Seasoned Hotel Investor",
      priorityMarkets: US_CALA_MARKETS,
      mixedUseAllowed: "Case-by-case",
      pipRepositioningDetails:
        "Moderate to heavy PIPs acceptable for conversion when lobby, breakfast, and guestroom programs can meet prototype. Confirm smoke-free, breakfast, and loyalty technology requirements early.",
      knownRedFlags:
        "Inadequate breakfast or lobby footprint for upper-midscale positioning; owner resistance to smoke-free conversion; meeting-space promises without staffing plan; unrealistic ramp vs. comp set.",
    };
  }

  if (segment === "upscale") {
    return {
      ...base,
      segment,
      idealBuildingTypes: [
        "Mid-Rise",
        "High-Rise",
        "Mixed-Use",
        "Podium / Tower",
        "Historic / Renovated",
        "Resort-Style Compound",
      ],
      idealRoomCountMin: 120,
      idealRoomCountMax: 450,
      idealProjectSizeMin: 150,
      idealProjectSizeMax: 300,
      minReqOperatorExperienceYears: 4,
      minLeadTimeMonths: 18,
      ownerHotelExperience: "Seasoned Hotel Investor",
      preferredOwnerType: "Private Equity / Fund",
      priorityMarkets: [...US_CALA_MARKETS, "Western Europe", "United Kingdom"],
      brandedResidencesAllowed: "Case-by-case",
      mixedUseAllowed: "Yes",
      coBrandingAllowed: "Case-by-case",
      pipRepositioningDetails:
        "Upscale conversions require F&B, meetings, and guestroom programs aligned to brand standards—budget full-service or select-service PIPs with design review milestones.",
      knownRedFlags:
        "Under-capitalized F&B or meeting product; operator without upscale service depth; union or labor constraints not modeled; design that cannot achieve brand QA without down-tiering.",
      idealProjectsAdditionalNotes:
        "Confirm bar/restaurant, meeting, and spa-inspired bath requirements in disclosure and prototype documents before LOI—upscale economics fail when public-space scope is under-built.",
    };
  }

  if (segment === "extendedStay") {
    return {
      ...base,
      segment,
      idealBuildingTypes: ["Low-Rise", "Mid-Rise"],
      idealRoomCountMin: 60,
      idealRoomCountMax: 140,
      idealProjectSizeMin: 80,
      idealProjectSizeMax: 120,
      minReqOperatorExperienceYears: 2,
      minLeadTimeMonths: 12,
      ownerHotelExperience: "Some Prior Hotel Experience",
      priorityMarkets: US_ONLY_MARKETS,
      pipRepositioningDetails:
        "Extended-stay conversions require in-suite kitchen or kitchenette infrastructure, laundry planning, and weekly housekeeping model—light cosmetic-only PIPs rarely sufficient.",
      knownRedFlags:
        "Studio layout without viable kitchen FF&E; weekly rate mix under 30% in comp set; insufficient parking for extended-stay guests; operator inexperienced with kitchen wear and longer stay housekeeping.",
      idealProjectsAdditionalNotes:
        "Underwrite weekly/monthly mix, utility cost per occupied suite, and housekeeping cadence—not transient ADR alone.",
    };
  }

  if (segment === "softCollection") {
    return {
      ...base,
      segment,
      idealProjectTypes: ["Conversion / Reflag", "Renovation / Repositioning", "Expansion / Add-on", "New Build"],
      idealBuildingTypes: ["Low-Rise", "Mid-Rise", "Historic / Renovated", "Mixed-Use", "Resort-Style Compound"],
      idealRoomCountMin: 50,
      idealRoomCountMax: 300,
      idealProjectSizeMin: 80,
      idealProjectSizeMax: 200,
      minReqOperatorExperienceYears: 3,
      minLeadTimeMonths: 12,
      ownerHotelExperience: "Seasoned Hotel Investor",
      coBrandingAllowed: "Case-by-case",
      brandedResidencesAllowed: "Case-by-case",
      priorityMarkets: US_CALA_MARKETS,
      pipRepositioningDetails:
        "Collection conversions preserve local character within membership standards—expect design review on public spaces, F&B identity, and guestroom touchpoints while meeting Choice systems requirements.",
      knownRedFlags:
        "Owner unwilling to adopt required technology or loyalty programs; design that cannot meet life-safety and brand QA; unrealistic expectation of zero PIP for distressed independent assets.",
    };
  }

  // luxuryCollection
  return {
    ...base,
    segment,
    idealBuildingTypes: [
      "Mid-Rise",
      "High-Rise",
      "Mixed-Use",
      "Historic / Renovated",
      "Resort-Style Compound",
    ],
    idealRoomCountMin: 100,
    idealRoomCountMax: 500,
    idealProjectSizeMin: 140,
    idealProjectSizeMax: 350,
    minReqOperatorExperienceYears: 5,
    minLeadTimeMonths: 24,
    ownerHotelExperience: "Established Hotel Operating Company",
    preferredOwnerType: "Institutional",
    brandedResidencesAllowed: "Case-by-case",
    mixedUseAllowed: "Yes",
    priorityMarkets: [...US_CALA_MARKETS, "Western Europe", "United Kingdom", "Middle East"],
    pipRepositioningDetails:
      "Iconic and luxury-collection assets require bespoke repositioning plans—heavy PIPs common; wellness, F&B, and meeting programs must match collection positioning.",
    knownRedFlags:
      "Heritage restrictions blocking required life-safety upgrades; insufficient reserves for collection-level FF&E; operator without luxury service culture; unresolved union or staffing constraints.",
  };
}

/** @type {Record<string, Partial<ReturnType<typeof segmentDefaults>> & { segment: Segment }>} */
const BRAND_SPECS = {
  "Ascend Hotel Collection": {
    segment: "softCollection",
    idealProjectsAdditionalNotes:
      "Soft collection for unique independents and boutiques—flexibility on design within membership standards; not a single rigid limited-service prototype.",
  },
  "Cambria Hotels": {
    segment: "upscale",
    idealProjectsAdditionalNotes:
      "Site criteria favor corporate, convention, and attraction adjacency—budget restaurant, bar, rooftop activation, and meetings; J.D. Power–level guest experience expectations.",
  },
  Clarion: {
    segment: "midscale",
    idealBuildingTypes: ["Low-Rise", "Mid-Rise", "Mixed-Use", "Historic / Renovated"],
    idealProjectsAdditionalNotes:
      "Meetings-capable midscale—viable event space and modest F&B required; shares Item 19 sample with Clarion Pointe (confirm counts in disclosure).",
  },
  "Clarion Pointe": {
    segment: "midscale",
    idealRoomCountMin: 60,
    idealRoomCountMax: 160,
    idealProjectSizeMin: 70,
    idealProjectSizeMax: 120,
    idealProjectsAdditionalNotes:
      "Select-service Clarion family—lighter meetings/F&B than full Clarion; conversion-friendly prototype.",
  },
  "Comfort Inn & Suites": {
    segment: "upperMidscale",
    coBrandingAllowed: "Case-by-case",
    idealProjectsAdditionalNotes:
      "Breakfast-led upper-midscale; smoke-free portfolio; Rise & Shine prototype for new construction; strong enterprise/loyalty mix in system averages.",
  },
  "Country Inn & Suites by Radisson (Choice)": {
    segment: "upperMidscale",
    idealProjectsAdditionalNotes:
      "Radisson-family warmth and breakfast-led model—suburban and highway upper-midscale; high enterprise participation in Item 19 sample.",
  },
  "Econo Lodge": { segment: "economy" },
  "Everhome Suites": {
    segment: "extendedStay",
    idealProjectsAdditionalNotes:
      "Newer extended-stay system—no Item 19 performance table in current disclosure; diligence on prototype, fees, and market study versus MainStay/Suburban.",
    minLeadTimeMonths: 14,
  },
  "MainStay Suites": {
    segment: "extendedStay",
    coBrandingAllowed: "Yes",
    idealProjectsAdditionalNotes:
      "Pair with Sleep Inn on dual-brand pads when market supports blended transient + extended-stay—shared site costs and parking.",
  },
  "Park Inn by Radisson (Choice)": {
    segment: "upperMidscale",
    idealProjectsAdditionalNotes:
      "Radisson-family functional upper-midscale—airport and suburban conversions; small Item 19 sample—treat metrics as directional.",
  },
  "Park Plaza (Choice)": {
    segment: "upscale",
    idealProjectsAdditionalNotes:
      "Upscale full-service city and resort hotels in Americas under Choice—meetings, leisure, and F&B depth required.",
  },
  "Quality Inn": {
    segment: "midscale",
    idealProjectsAdditionalNotes:
      "Core Choice midscale for conversions and NC—Value Qs framework; broad U.S. footprint and conversion economics.",
  },
  "Radisson (Choice)": {
    segment: "upscale",
    idealProjectsAdditionalNotes:
      "Core Radisson upscale full-service in Americas—balanced calm positioning; confirm RHG relationship outside Americas.",
  },
  "Radisson Blu (Choice)": {
    segment: "upscale",
    idealRoomCountMin: 150,
    idealProjectSizeMin: 180,
    idealProjectsAdditionalNotes:
      "Design-forward upscale—Nordic-inspired guest experience; small Item 19 sample; urban gateway and resort gateways.",
  },
  "Radisson Collection  (Choice)": {
    segment: "luxuryCollection",
    idealProjectsAdditionalNotes:
      "Hand-selected iconic hotels—Explorers Welcome positioning; wellness and responsible business integrated in guest journey.",
  },
  "Radisson Individual (Choice)": {
    segment: "softCollection",
    idealRoomCountMin: 80,
    idealRoomCountMax: 250,
    idealProjectsAdditionalNotes:
      "Upper-upscale soft brand relaunched 2024—boutique personality with Choice distribution; no Item 19 performance table in current disclosure.",
  },
  "Radisson Inn & Suites": {
    segment: "upperMidscale",
    idealProjectsAdditionalNotes:
      "Launched 2022 upper-midscale select-service—café lobby, elevated breakfast, metro/airport/mixed-use; naturally grounded design program.",
  },
  "Radisson RED  (Choice)": {
    segment: "upscale",
    idealRoomCountMin: 100,
    idealRoomCountMax: 300,
    idealProjectSizeMin: 120,
    idealProjectSizeMax: 220,
    idealBuildingTypes: ["Mid-Rise", "High-Rise", "Mixed-Use", "Historic / Renovated"],
    idealProjectsAdditionalNotes:
      "Upscale select-service urban social brand—flex deli-bar F&B and local personality; Enjoy It! positioning.",
  },
  "Rodeway Inn": { segment: "economy" },
  "Sleep Inn": {
    segment: "midscale",
    idealRoomCountMin: 60,
    idealRoomCountMax: 130,
    idealProjectSizeMin: 70,
    idealProjectSizeMax: 110,
    coBrandingAllowed: "Yes",
    idealProjectsAdditionalNotes:
      "Lowest-in-segment build cost narrative—Scenic Dreams NC prototype; dual-brand with MainStay on same pad in select markets.",
  },
  "Suburban Studios": {
    segment: "extendedStay",
    idealRoomCountMin: 50,
    idealRoomCountMax: 120,
    idealProjectSizeMin: 60,
    idealProjectSizeMax: 100,
    idealProjectsAdditionalNotes:
      "Economy extended-stay studios—weekly rate mix and kitchenette opex control; bridge between economy and MainStay.",
  },
  "WoodSpring Suites": {
    segment: "extendedStay",
    idealRoomCountMin: 80,
    idealRoomCountMax: 140,
    idealProjectSizeMin: 100,
    idealProjectSizeMax: 130,
    idealProjectsAdditionalNotes:
      "Efficient extended-stay prototype—typical pads ~100–130 rooms; lean staffing model; confirm Item 19 operating metrics format in disclosure.",
  },
};

/**
 * @param {string} brandName
 * @returns {Record<string, unknown>|null}
 */
export function buildProjectFitFormForBrand(brandName) {
  const spec = BRAND_SPECS[brandName];
  if (!spec) return null;
  const { segment, ...overrides } = spec;
  return { ...segmentDefaults(segment), ...overrides };
}
