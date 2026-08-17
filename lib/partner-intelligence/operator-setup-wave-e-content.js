/**
 * Wave E Operator Setup content — OxoHotel, Grupo Marta Hospitality, Grupo Iberostar.
 * Source-backed from official sites (fetched 2026-07-24). Select options from observed schema only.
 */
const FIGURES = "July 2026 (website-sourced Setup)";
const NM = "Not Measured / N/A";

function j(arr) {
  return JSON.stringify(arr);
}

/** @type {Record<string, import('./operator-setup-website-content-packs.js').SetupContentPack>} */
export const WAVE_E_WEBSITE_CONTENT_PACKS = Object.freeze({
  oxohotel: Object.freeze({
    sources: [
      { title: "OxoHotel — Who We Are", url: "https://www.oxohotel.com/en/who-we-are/" },
      { title: "OxoHotel — Hotels & Destinations", url: "https://www.oxohotel.com/en/hotels-destinations/" },
      { title: "OxoHotel home", url: "https://www.oxohotel.com/en/" },
    ],
    profile: {
      website: "https://www.oxohotel.com/en/",
      headquarters: "Colombia",
      primaryServiceModel: "Mixed",
      yearEstablished: 2009,
      yearsInBusiness: 17,
      totalProperties: "30+",
      companyDescription:
        "OxoHotel is a Colombia hospitality management company founded in 2009. Official materials describe evolution from hotel operator to Hospitality Manager across 30+ properties, using a multi-brand, multi-product, and multi-segment model spanning conceptualization, development, and operation of lodging services. Public portfolio pages list branded and proprietary hotels across destinations including Bogotá, Cartagena, Santa Marta, Medellín, Barranquilla, San Andrés, La Guajira, Bucaramanga, Yopal, and Chocó.",
      companyHistory:
        "Founded 2009 in Colombia. Official Who We Are page positions OxoHotel among the larger players in Colombia’s hospitality industry, with a stated vision (through 2026) to remain a key hospitality developer in Colombia with presence in strategic Latin America destinations. Public hotel list includes Marriott, Hilton, and IHG flags alongside proprietary/lifestyle assets (e.g. Tribute Portfolio, Curio Collection, Autograph Collection, AC Hotels, Holiday Inn Express, Courtyard, Residence Inn, and independent/resort properties).",
      missionStatement:
        "To improve the lives of our team members, guests, and investors by creating value, development opportunities, and progress, while positively impacting society (oxohotel.com).",
      differentiators:
        "Colombia multi-brand hospitality manager (30+ properties cited)\nMulti-product / multi-segment model (city, beach, resort, lifestyle)\nPublic brand mix spanning Marriott, Hilton, IHG soft/collection and select-service flags plus proprietary assets\nPriority Owners investor program described on official site",
    },
    platformMarkets: {
      specificMarkets:
        "Colombia — Bogotá; Cartagena; Santa Marta; Medellín; Barranquilla; San Andrés; La Guajira; Bucaramanga; Yopal; Chocó — per oxohotel.com hotels & destinations. Vision cites strategic Latin America destinations (confirm active non-Colombia assets in diligence).",
      totalProperties: "30+",
    },
    commercial: {
      bf_operating_situations:
        "Owners and developers of branded or independent hotels in Colombia seeking a multi-brand hospitality manager with city and leisure capability; assets that fit Marriott/Hilton/IHG or proprietary lifestyle/resort positioning in OxoHotel’s published destinations.",
      bf_not_ideal_for:
        "Mandates outside Colombia/LATAM without an OxoHotel expansion plan; owners needing a U.S.-only remote management platform without in-market Colombia ops",
    },
    governance: {
      risk_programs_narrative:
        "Brand-standard QA and franchise compliance by flag (Marriott, Hilton, IHG where applicable). Confirm safety, insurance, and certifications per asset. Source: oxohotel.com/en/who-we-are/.",
    },
  }),

  "grupo-marta-hospitality": Object.freeze({
    sources: [
      { title: "Grupo Marta Hospitality — About", url: "https://www.grupomarta.com/en/sobre-nosotros" },
      { title: "Grupo Marta Hospitality home", url: "https://grupomarta.com/" },
    ],
    profile: {
      website: "https://www.grupomarta.com/",
      headquarters: "San José, Costa Rica",
      primaryServiceModel: "Mixed",
      yearEstablished: 1960,
      yearsInBusiness: 65,
      companyDescription:
        "Grupo Marta Hospitality is a Costa Rican family hospitality company founded in 1960. Official materials describe pioneering hotel, restaurant, and tourism projects in Costa Rica, including opening the country’s first airport hotel, and operating hotels, restaurants, and vacation rentals under recognized international franchises. Public site metrics cite 65+ years of experience, 475+ collaborators, and 9 international brands introduced to the country.",
      companyHistory:
        "Founded 1960. Official About page positions Grupo Marta as a pioneering family business in Costa Rica tourism development and operations. Published operating divisions include Hotels, Restaurants, and Vacation Rentals. Public portfolio highlights include Holiday Inn La Sabana, Holiday Inn Express San José Airport, Best Western Jacó Beach All Inclusive Resort, Irazú Hotel & Studios, Denny’s Costa Rica, and Casago Vacation Rentals.",
      missionStatement:
        "Vision, operational excellence, and commitment to sustainable tourism development of Costa Rica — people-centered culture with professional corporate structure (grupomarta.com).",
      differentiators:
        "65+ years Costa Rica hospitality operating history\nMulti-division platform: hotels + restaurants + vacation rentals\nInternational franchise experience (Holiday Inn / Holiday Inn Express; Best Western Jacó Beach AI cited)\nPublished scale signals: 475+ collaborators; 9 international brands introduced to Costa Rica",
    },
    platformMarkets: {
      specificMarkets:
        "Costa Rica — San José / La Sabana; San José Airport; Jacó Beach; Irazú / highland lodging contexts — per grupomarta.com portfolio highlights. Confirm additional markets in diligence.",
    },
    commercial: {
      bf_operating_situations:
        "Costa Rica hotel owners and developers seeking a long-tenured local operator with IHG/Best Western franchise operating experience and adjacent F&B / vacation-rental capability.",
      bf_not_ideal_for:
        "Assets outside Costa Rica without a Grupo Marta expansion mandate; pure U.S. third-party management briefs without Costa Rica operating relevance",
    },
    governance: {
      risk_programs_narrative:
        "Franchise brand standards (IHG / Best Western where applicable) and local Costa Rica operating compliance. Confirm certifications and insurance per asset. Source: grupomarta.com.",
    },
  }),

  "grupo-iberostar": Object.freeze({
    sources: [
      { title: "Iberostar Group home", url: "https://grupoiberostar.com/en/" },
      { title: "Iberostar Group — Expansion", url: "https://grupoiberostar.com/en/expansion/" },
    ],
    profile: {
      website: "https://grupoiberostar.com/en/",
      headquarters: "Palma de Mallorca, Spain",
      primaryServiceModel: "Full-service focus",
      yearEstablished: 1956,
      yearsInBusiness: 70,
      totalProperties: "100",
      companyDescription:
        "Grupo Iberostar (Iberostar Group) is a family-owned Spanish tourism group. Official corporate materials (2025/2026) cite on the order of 100 hotels, ~40,000 people, 95 nationalities, 14 operating countries, 8.1 million clients, and €5.1B 2025 revenue. Business areas include Beachfront Resorts (Iberostar Hotels & Resorts), Vacation Club (Iberostar The Club), and W2M travel division. The hotel division focuses on 4- and 5-star beachfront resorts across Europe, the Americas, and Africa, with brand lanes including upscale beachfront, upper-upscale, and JOIA by Iberostar.",
      companyHistory:
        "Public corporate narrative emphasizes ~70 years of tourism experience as a family-owned company (Fluxà family leadership; D. Miguel Fluxà cited as President). Hotel division Iberostar Hotels & Resorts is the core business; expansion materials describe beachfront resort growth with owner partnerships and centralized brand/marketing support. CALA relevance: Americas beachfront destinations within the 14-country operating footprint — confirm country-level inventory in diligence (public destinations historically include Mexico, Dominican Republic, Cuba, Jamaica, Aruba, Brazil, Peru among Americas markets).",
      missionStatement:
        "Champion a positive change through positive tourism — responsible tourism model via Iberostar Wave of Change (grupoiberostar.com).",
      differentiators:
        "Family-owned global beachfront resort platform (~100 hotels / 14 countries cited)\nIntegrated group: hotels + vacation club + travel (W2M)\nUpscale / upper-upscale / JOIA luxury beachfront segmentation\nWave of Change sustainability program embedded across the group\nOwner-partnership expansion model described on corporate Expansion page",
    },
    platformMarkets: {
      specificMarkets:
        "Europe, the Americas, and Africa beachfront destinations — 14 operating countries and 35 destinations cited on grupoiberostar.com expansion materials. Label enterprise global footprint vs active CALA managed/owned inventory in diligence.",
      totalProperties: "100",
    },
    commercial: {
      bf_operating_situations:
        "Owners and partners of beachfront 4–5★ resorts in Iberostar’s Americas/Europe/Africa leisure corridors seeking Iberostar Hotels & Resorts brand/operating partnership; assets that fit family, couples, or JOIA luxury beachfront positioning.",
      bf_not_ideal_for:
        "Urban select-service or non-beachfront mandates; assets outside Iberostar’s vacation beachfront model without a clear expansion fit",
    },
    governance: {
      risk_programs_narrative:
        "Corporate Wave of Change sustainability program; brand and owner-collaboration standards described on Expansion. Confirm property-level certifications, safety, and insurance in diligence. HQ: Palma de Mallorca, Spain. Sources: grupoiberostar.com.",
    },
  }),
});

/** @type {Record<string, Record<string, unknown>>} */
export const WAVE_E_PROFILE_DEEP_PACKS = Object.freeze({
  oxohotel: Object.freeze({
    company_name: "OxoHotel",
    website: "https://www.oxohotel.com/en/",
    companyTagline:
      "Colombia hospitality managers — multi-brand, multi-product, multi-segment lodging.",
    primaryServiceModel: "Mixed",
    headquarters: "Colombia",
    companySize: "Medium (10-50 properties)",
    figuresAsOf: FIGURES,
    yearEstablished: 2009,
    yearsInBusiness: 17,
    totalProperties: "30+",
    companyDescription: WAVE_E_WEBSITE_CONTENT_PACKS.oxohotel.profile.companyDescription,
    companyHistory: WAVE_E_WEBSITE_CONTENT_PACKS.oxohotel.profile.companyHistory,
    missionStatement: WAVE_E_WEBSITE_CONTENT_PACKS.oxohotel.profile.missionStatement,
    differentiators: WAVE_E_WEBSITE_CONTENT_PACKS.oxohotel.profile.differentiators,
    managementPhilosophy:
      "Hospitality management across conceptualization, development, and operations — multi-brand and multi-segment execution for owners and guests in Colombia destinations.",
    "Service Models Supported": ["Full-service", "Select-service", "Resort", "Lifestyle", "Boutique"],
    "Brand Families Operated": ["Marriott", "Hilton", "IHG", "Independent", "Soft brands / collections"],
    chainScalesSupported: ["Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    propertyTypes: ["Full Service", "Select Service", "Resort", "Lifestyle", "Boutique"],
    additionalExperience: ["Urban", "Resort"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    numberOfBrands: 4,
    brand_soft_independent_narrative:
      "Public portfolio mixes major-brand select-service and soft/collection hotels (Tribute, Curio, Autograph, AC) with proprietary and independent resort/lifestyle assets.",
    brand_narrative_relationship:
      "OxoHotel positions as hospitality manager for branded and proprietary assets in Colombia. Confirm franchise agreements and third-party management scope per asset.",
    brand_narrative_compliance:
      "Brand-standard QA by flag (Marriott, Hilton, IHG) where applicable — confirm audit cadence and PIP processes in diligence.",
    brand_portfolio_mix_json: j([
      {
        brandFlagType: "Marriott International",
        portfolioMix: "Branded / soft brands",
        assetContext: "AC, Courtyard, Residence Inn, Tribute, Autograph examples on public hotel list",
        relationshipStatus: "Active (public portfolio)",
      },
      {
        brandFlagType: "Hilton",
        portfolioMix: "Soft brands / collections",
        assetContext: "Curio Collection (Nácar Cartagena) on public hotel list",
        relationshipStatus: "Active (public portfolio)",
      },
      {
        brandFlagType: "IHG Hotels & Resorts",
        portfolioMix: "Select-service",
        assetContext: "Holiday Inn Express examples on public hotel list",
        relationshipStatus: "Active (public portfolio)",
      },
      {
        brandFlagType: "Independent / proprietary",
        portfolioMix: "Independent & lifestyle",
        assetContext: "Resort and urban lifestyle assets on public hotel list",
        relationshipStatus: "Active (public portfolio)",
      },
    ]),
    brand_relationship_depth_json: j([
      {
        title: "Multi-brand Colombia operator",
        description: "Public destinations and hotel list show concurrent Marriott, Hilton, and IHG family assets plus independents.",
      },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Hospitality management", description: "Conceptualization, development, and operation of lodging services (official positioning)." },
      { title: "Multi-segment ops", description: "City, beach, and lifestyle/resort products across Colombia destinations." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Brand QA", description: "Franchise compliance by flag — confirm per property in diligence." },
    ]),
    overview_why_1_headline: "Colombia multi-brand depth",
    overview_why_1_story:
      "30+ properties cited across major Colombia leisure and urban destinations under one hospitality management platform.",
    overview_why_2_headline: "Brand + proprietary mix",
    overview_why_2_story:
      "Public portfolio spans Marriott/Hilton/IHG flags and soft brands alongside proprietary lifestyle and resort assets.",
    overview_why_3_headline: "Owner-aligned hospitality manager",
    overview_why_3_story:
      "Official materials emphasize evolution to Hospitality Manager with Priority Owners benefits for property investors.",
    overview_bestat_1_headline: "Colombia urban hotels",
    overview_bestat_1_story: "Bogotá and Medellín select-service and lifestyle assets on public hotel list.",
    overview_bestat_2_headline: "Caribbean Colombia leisure",
    overview_bestat_2_story: "Cartagena, Santa Marta, San Andrés, and La Guajira properties cited publicly.",
    overview_bestat_3_headline: "Soft-brand / collection hotels",
    overview_bestat_3_story: "Tribute, Curio, and Autograph examples appear on OxoHotel’s public hotel list.",
    overview_signal_1_value: "Founded 2009 · 30+ properties (oxohotel.com)",
    overview_signal_2_value: "Multi-brand · multi-product · multi-segment model",
    overview_signal_3_value: "Colombia destinations + LATAM vision (confirm non-CO assets)",
  }),

  "grupo-marta-hospitality": Object.freeze({
    company_name: "Grupo Marta Hospitality",
    website: "https://www.grupomarta.com/",
    companyTagline:
      "Costa Rica family hospitality — hotels, restaurants, and vacation rentals since 1960.",
    primaryServiceModel: "Mixed",
    headquarters: "San José, Costa Rica",
    companySize: "Small (1-10 properties)",
    figuresAsOf: FIGURES,
    yearEstablished: 1960,
    yearsInBusiness: 65,
    companyDescription: WAVE_E_WEBSITE_CONTENT_PACKS["grupo-marta-hospitality"].profile.companyDescription,
    companyHistory: WAVE_E_WEBSITE_CONTENT_PACKS["grupo-marta-hospitality"].profile.companyHistory,
    missionStatement: WAVE_E_WEBSITE_CONTENT_PACKS["grupo-marta-hospitality"].profile.missionStatement,
    differentiators: WAVE_E_WEBSITE_CONTENT_PACKS["grupo-marta-hospitality"].profile.differentiators,
    managementPhilosophy:
      "Family-owned Costa Rica operator with professional corporate structure, franchise hotel operations, and adjacent F&B / vacation-rental divisions.",
    "Service Models Supported": ["Full-service", "Select-service", "All-inclusive", "Resort"],
    "Brand Families Operated": ["IHG", "Independent"],
    chainScalesSupported: ["Upper Midscale", "Midscale", "Upscale"],
    propertyTypes: ["Full Service", "Select Service", "Resort"],
    additionalExperience: ["Urban", "Resort", "Airport"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    numberOfBrands: 3,
    brand_soft_independent_narrative:
      "Public portfolio includes IHG flags (Holiday Inn / Holiday Inn Express), Best Western Jacó Beach All Inclusive, and proprietary Irazú Hotel & Studios, plus Denny’s and Casago vacation rentals.",
    brand_narrative_relationship:
      "Grupo Marta operates hotels under international franchises in Costa Rica and runs adjacent restaurant and vacation-rental divisions. Confirm franchise agreements per asset.",
    brand_narrative_compliance:
      "Franchise brand standards (IHG / Best Western where applicable) and Costa Rica operating compliance — confirm in diligence.",
    brand_portfolio_mix_json: j([
      {
        brandFlagType: "IHG Hotels & Resorts",
        portfolioMix: "Franchise hotels",
        assetContext: "Holiday Inn La Sabana; Holiday Inn Express San José Airport (public portfolio)",
        relationshipStatus: "Active (public portfolio)",
      },
      {
        brandFlagType: "Best Western",
        portfolioMix: "Franchise resort",
        assetContext: "Best Western Jacó Beach All Inclusive Resort (public portfolio)",
        relationshipStatus: "Active (public portfolio)",
      },
      {
        brandFlagType: "Independent / proprietary",
        portfolioMix: "Hotels + F&B + vacation rentals",
        assetContext: "Irazú Hotel & Studios; Denny’s Costa Rica; Casago Vacation Rentals",
        relationshipStatus: "Active (public portfolio)",
      },
    ]),
    brand_relationship_depth_json: j([
      {
        title: "Costa Rica franchise operator",
        description: "Long-tenured local operator introducing and operating international hospitality brands in Costa Rica.",
      },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Hotel operations", description: "Full- and select-service plus all-inclusive beach resort cited on public site." },
      { title: "Multi-division platform", description: "Hotels, restaurants, and vacation rentals under one group." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Franchise QA", description: "Confirm IHG / Best Western standards and local compliance per asset." },
    ]),
    overview_why_1_headline: "65+ years in Costa Rica",
    overview_why_1_story:
      "Family pioneer that opened the country’s first airport hotel and continues as a multi-division hospitality platform.",
    overview_why_2_headline: "Franchise + local brands",
    overview_why_2_story:
      "Holiday Inn / Holiday Inn Express and Best Western Jacó Beach AI alongside proprietary lodging and F&B.",
    overview_why_3_headline: "People-scale Costa Rica platform",
    overview_why_3_story:
      "Official metrics cite 475+ collaborators and 9 international brands introduced to the country.",
    overview_bestat_1_headline: "San José gateway hotels",
    overview_bestat_1_story: "Holiday Inn La Sabana and Holiday Inn Express San José Airport on public portfolio.",
    overview_bestat_2_headline: "Pacific beach all-inclusive",
    overview_bestat_2_story: "Best Western Jacó Beach All Inclusive Resort cited publicly.",
    overview_bestat_3_headline: "Adjacent F&B and vacation rentals",
    overview_bestat_3_story: "Denny’s Costa Rica and Casago Vacation Rentals expand beyond pure hotel ops.",
    overview_signal_1_value: "Founded 1960 · 65+ years (grupomarta.com)",
    overview_signal_2_value: "475+ collaborators · 9 international brands introduced",
    overview_signal_3_value: "Hotels + restaurants + vacation rentals",
  }),

  "grupo-iberostar": Object.freeze({
    company_name: "Grupo Iberostar",
    website: "https://grupoiberostar.com/en/",
    companyTagline:
      "Family-owned beachfront resorts — responsible tourism via Wave of Change.",
    primaryServiceModel: "Full-service focus",
    headquarters: "Palma de Mallorca, Spain",
    companySize: "Large (50+ properties)",
    figuresAsOf: FIGURES,
    yearEstablished: 1956,
    yearsInBusiness: 70,
    totalProperties: "100",
    companyDescription: WAVE_E_WEBSITE_CONTENT_PACKS["grupo-iberostar"].profile.companyDescription,
    companyHistory: WAVE_E_WEBSITE_CONTENT_PACKS["grupo-iberostar"].profile.companyHistory,
    missionStatement: WAVE_E_WEBSITE_CONTENT_PACKS["grupo-iberostar"].profile.missionStatement,
    differentiators: WAVE_E_WEBSITE_CONTENT_PACKS["grupo-iberostar"].profile.differentiators,
    managementPhilosophy:
      "Family-owned beachfront resort operating and brand-portfolio model with owner partnerships, centralized brand/marketing support, and Wave of Change sustainability strategy.",
    "Service Models Supported": ["Resort", "All-inclusive", "Full-service"],
    "Brand Families Operated": ["Independent", "Soft brands / collections"],
    chainScalesSupported: ["Luxury", "Upper Upscale", "Upscale"],
    propertyTypes: ["Resort", "Full Service"],
    additionalExperience: ["Resort"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    numberOfBrands: 3,
    brand_soft_independent_narrative:
      "Iberostar Beachfront Resorts brand family including upscale, upper-upscale, and JOIA by Iberostar luxury collections. Note: Iberostar Beachfront Resorts also has IHG distribution partnerships in some markets — confirm property-level affiliation in diligence (do not assume all assets are IHG).",
    brand_narrative_relationship:
      "Owner/operator and expansion partner for beachfront resorts. Corporate Expansion page describes collaboration with property owners and centralized brand marketing.",
    brand_narrative_compliance:
      "Brand standards and Wave of Change sustainability commitments — confirm property-level certifications and any franchise/distribution obligations in diligence.",
    brand_portfolio_mix_json: j([
      {
        brandFlagType: "Iberostar Beachfront Resorts",
        portfolioMix: "Core hotel division",
        assetContext: "4–5★ beachfront resorts · upscale / upper-upscale lanes",
        relationshipStatus: "Active (corporate)",
      },
      {
        brandFlagType: "JOIA by Iberostar",
        portfolioMix: "Luxury collection",
        assetContext: "5★ beachfront luxury experiences (expansion materials)",
        relationshipStatus: "Active (corporate)",
      },
      {
        brandFlagType: "Iberostar The Club / W2M",
        portfolioMix: "Adjacent group businesses",
        assetContext: "Vacation club and travel division — label separately from hotel ops",
        relationshipStatus: "Active (corporate)",
      },
    ]),
    brand_relationship_depth_json: j([
      {
        title: "Beachfront resort platform",
        description: "100 hotels / 14 countries / 35 destinations cited on grupoiberostar.com.",
      },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Beachfront resort ops", description: "Vacation segment 4–5★ beachfront hotels across Europe, Americas, Africa." },
      { title: "Owner partnerships", description: "Expansion materials emphasize owner collaboration and brand marketing support." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Wave of Change", description: "Group sustainability movement spanning environmental and social dimensions." },
      { title: "Brand standards", description: "Centralized marketing and brand quality described on Expansion." },
    ]),
    overview_why_1_headline: "Global beachfront scale",
    overview_why_1_story:
      "About 100 hotels across 14 countries with family ownership and ~70 years of tourism experience cited corporately.",
    overview_why_2_headline: "Clear brand lanes",
    overview_why_2_story:
      "Upscale beachfront, upper-upscale, and JOIA luxury collections under Iberostar Hotels & Resorts.",
    overview_why_3_headline: "Responsible tourism platform",
    overview_why_3_story:
      "Wave of Change embeds sustainability across the group’s hotel and tourism businesses.",
    overview_bestat_1_headline: "Americas beachfront resorts",
    overview_bestat_1_story:
      "Americas leisure destinations within the 14-country footprint — confirm CALA inventory in diligence.",
    overview_bestat_2_headline: "Owner expansion partnerships",
    overview_bestat_2_story:
      "Corporate Expansion team seeks hotel owners and investors for beachfront growth partnerships.",
    overview_bestat_3_headline: "Integrated tourism group",
    overview_bestat_3_story:
      "Hotels plus Vacation Club and W2M travel division — label hotel lens vs group umbrella.",
    overview_signal_1_value: "~100 hotels · 14 countries (grupoiberostar.com)",
    overview_signal_2_value: "~40,000 people · €5.1B 2025 revenue cited",
    overview_signal_3_value: "Wave of Change · family-owned Fluxà leadership",
  }),
});
