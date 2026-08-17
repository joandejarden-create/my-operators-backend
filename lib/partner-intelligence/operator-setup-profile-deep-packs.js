/**
 * Deep Operator Setup - Profile & Positioning packs.
 * Pattern mirrors Arbor/Hotel Equities baseline field coverage:
 * identity + narrative + overview why/best-at/signals + brand JSON +
 * observed select options only (never invent select values).
 *
 * Sources: official operator sites / factory content (fetched/curated 2026-07-24).
 * Does not set companyLogo, brands (linked Brand Basics), or readyForInvestorPublication.
 */
import { OPERATOR_FACTORY_QUEUE } from "./operator-explorer-factory-queue.js";
import { BRAND_MANAGED_PROFILE_DEEP_PACKS } from "./operator-setup-brand-managed-content.js";
import { PLAYA_PROFILE_DEEP_PACK } from "./operator-setup-playa-hotels-content.js";
import { WAVE_D_PROFILE_DEEP_PACKS } from "./operator-setup-wave-d-content.js";
import { WAVE_E_PROFILE_DEEP_PACKS } from "./operator-setup-wave-e-content.js";

function packFor(slug) {
  return OPERATOR_FACTORY_QUEUE.find((o) => o.slug === slug) || null;
}

function j(arr) {
  return JSON.stringify(arr);
}

const NM = "Not Measured / N/A";
const FIGURES = "July 2026 (website-sourced Setup deepen)";

/**
 * @typedef {Record<string, unknown>} ProfileDeepPack
 * @type {Record<string, ProfileDeepPack>}
 */
export const OPERATOR_SETUP_PROFILE_DEEP_PACKS = Object.freeze({
  ...BRAND_MANAGED_PROFILE_DEEP_PACKS,
  "playa-hotels-resorts": PLAYA_PROFILE_DEEP_PACK,
  ...WAVE_D_PROFILE_DEEP_PACKS,
  ...WAVE_E_PROFILE_DEEP_PACKS,
  "aimbridge-latam": Object.freeze({
    company_name: "Aimbridge Hospitality (LATAM)",
    website: "https://aimbridgelatam.com/en/home/",
    companyTagline:
      "LATAM third-party management — international brand standards, regional depth, All-Inclusive specialty.",
    primaryServiceModel: "Mixed",
    headquarters: "Monterrey, Mexico",
    companySize: "Large (50+ properties)",
    figuresAsOf: FIGURES,
    companyDescription:
      "Aimbridge LATAM is Aimbridge Hospitality’s Latin America third-party hotel management division. Official materials describe operating a diverse portfolio for the modern traveler, combining international brand standards, deep Latin American market knowledge, and a results-oriented culture. The platform partners with owners across Mexico and Latin America on major international brands and independent assets, with a dedicated All-Inclusive division.",
    companyHistory:
      "Parent Aimbridge Hospitality is positioned as a leading global third-party manager; this profile is the LATAM division lens. Public 2026 leadership: Alex Fiz appointed President of the LATAM and All-Inclusive Divisions (effective March 2, 2026), succeeding Leandro Castillo (advisory transition). Named development leadership includes Luis René Sánchez (VP Development — Mexico & Central America) and Javier Sánchez (VP Business Development — Caribbean & All-Inclusive). F&B: Davide Preziuso, Director of Food & Beverage for LATAM & All-Inclusive.",
    missionStatement:
      "Inspiring experiences in every destination — international standards with Latin American market knowledge and a results-oriented culture (aimbridgelatam.com).",
    differentiators:
      "Aimbridge enterprise depth plus in-market LATAM leadership\nDedicated All-Inclusive division\nPublic brand alliances with IHG, Wyndham, Marriott, and Hilton\nMexico-weighted growth with Caribbean expansion signals (Noval Properties DR; Grupo Satli / Marriott Riviera Maya AI project, May 2026)",
    managementPhilosophy:
      "Owner-aligned third-party management with international standards, regional commercial and F&B depth, and specialized all-inclusive resort operating capability.",
    "Service Models Supported": ["Full-service", "Select-service", "Resort", "All-inclusive", "Lifestyle"],
    "Brand Families Operated": ["Marriott", "Hilton", "IHG", "Wyndham", "Independent", "Soft brands / collections"],
    chainScalesSupported: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    propertyTypes: ["Full Service", "Select Service", "Resort", "Lifestyle"],
    additionalExperience: ["Urban", "Resort"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    numberOfBrands: 4,
    brand_soft_independent_narrative:
      "Aimbridge LATAM manages both major-brand hotels and independent properties. Soft/collection formats appear within franchise ecosystems; independents are explicitly in public positioning on aimbridgelatam.com.",
    brand_narrative_relationship:
      "Public LATAM materials cite alliances with InterContinental Hotels Group, Wyndham Hotel Group, Marriott International, and Hilton Worldwide across Mexico and Latin America. Treat parent Aimbridge Hospitality enterprise scale as labeled context—not a substitute for LATAM-specific contract counts.",
    brand_narrative_compliance:
      "Brand-standard QA and franchise compliance by asset (Marriott, Hilton, IHG, Wyndham), coordinated with LATAM operating leaders and Aimbridge enterprise standards—confirm property-level audit cadence in diligence.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "Marriott International", portfolioMix: "LATAM branded", assetContext: "Full-service / lifestyle / AI pipeline", relationshipStatus: "Active (public)" },
      { brandFlagType: "Hilton Worldwide", portfolioMix: "LATAM branded", assetContext: "Mexico / LATAM alliances", relationshipStatus: "Active (public)" },
      { brandFlagType: "IHG Hotels & Resorts", portfolioMix: "LATAM branded", assetContext: "Mexico / LATAM alliances", relationshipStatus: "Active (public)" },
      { brandFlagType: "Wyndham Hotels & Resorts", portfolioMix: "LATAM branded", assetContext: "Mexico / LATAM alliances", relationshipStatus: "Active (public)" },
      { brandFlagType: "Independent", portfolioMix: "LATAM independent", assetContext: "Explicitly in public positioning", relationshipStatus: "Active (public)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "Marriott International", description: "Active LATAM relationships including JW Marriott Monterrey Valle recognition and Riviera Maya AI project signals." },
      { title: "Hilton Worldwide", description: "Cited alliance partner across Mexico/LATAM." },
      { title: "IHG Hotels & Resorts", description: "Cited alliance partner across Mexico/LATAM." },
      { title: "Wyndham Hotels & Resorts", description: "Cited alliance partner across Mexico/LATAM." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Franchise operating discipline", description: "Brand-system operations across major families." },
      { title: "All-Inclusive execution", description: "Dedicated AI division for resort leisure assets." },
      { title: "Conversion / reflag", description: "Lifestyle and brand conversions within franchise ecosystems." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Brand QA", description: "Franchise compliance and QA coordinated with LATAM operating leaders." },
      { title: "Enterprise standards", description: "Aimbridge Hospitality platform standards behind regional teams." },
    ]),
    overview_why_1_headline: "What Differentiates Aimbridge LATAM",
    overview_why_1_story:
      "Enterprise Aimbridge depth behind an in-region LATAM and All-Inclusive operating team—not a U.S.-only remote model.",
    overview_why_2_headline: "Mexico + Caribbean Growth",
    overview_why_2_story:
      "Mexico-weighted portfolio with named development coverage for Mexico/Central America and Caribbean & All-Inclusive, plus public DR and Riviera Maya pipeline signals.",
    overview_why_3_headline: "Brand Platform Breadth",
    overview_why_3_story:
      "Public alliances spanning IHG, Wyndham, Marriott, and Hilton plus independent assets under one LATAM third-party platform.",
    overview_bestat_1_headline: "All-Inclusive Specialty",
    overview_bestat_1_story:
      "Dedicated All-Inclusive division with Caribbean BD leadership and F&B leadership for LATAM & All-Inclusive.",
    overview_bestat_2_headline: "In-Market Development",
    overview_bestat_2_story:
      "VP Development Mexico & Central America and VP Business Development Caribbean & All-Inclusive publicly appointed.",
    overview_bestat_3_headline: "Owner-Aligned Third-Party",
    overview_bestat_3_story:
      "Results-oriented culture combining international brand standards with Latin American market knowledge (aimbridgelatam.com).",
    overview_signal_1_value: "LATAM third-party platform · Mexico-weighted portfolio (aimbridgelatam.com)",
    overview_signal_2_value: "Brand alliances: IHG · Wyndham · Marriott · Hilton (public LATAM materials)",
    overview_signal_3_value: "All-Inclusive division + Caribbean BD leadership (2025–2026 public appointments)",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Standard",
    sustainabilityPrograms:
      "Property and brand-dependent; no single LATAM public sustainability framework enumerated on aimbridgelatam.com — confirm in diligence.",
    esgReporting:
      "No standardized LATAM ESG reporting protocol published on the division site; align expectations during owner diligence. Parent Aimbridge programs are enterprise context only.",
  }),

  "tafer-hotels-resorts": Object.freeze({
    company_name: "Tafer Hotels & Resorts",
    website: "https://www.taferresorts.com/",
    companyTagline: "Mexico leisure excellence — design, management, and vacation ownership.",
    primaryServiceModel: "Full-service focus",
    headquarters: "Puerto Vallarta, Mexico",
    companySize: "Small (1-10 properties)",
    figuresAsOf: FIGURES,
    companyDescription:
      "TAFER Hotels & Resorts is a Mexico leisure and hospitality company with an evolving collection of award-winning hotels, resorts, and boutique villas. Official materials describe hotel design and construction, resort management, vacation ownership, residences, marketing and concept design, and tour/travel agent services.",
    companyHistory:
      "TAFER positions itself as a forward-thinking leisure company with more than 30 years of team experience in leisure and tourism. It is the principal owner of The Villagroup Resorts, whose properties feature within the TAFER collection. Portfolio highlights on the official About page include Hotel Mousai (Puerto Vallarta and Cancún), Garza Blanca Preserve Resort & Spa (Puerto Vallarta), Villa del Palmar Cancun Resort & Spa, Sierra Lago Resort & Spa, and Sian Ka’an Village.",
    missionStatement:
      "Merge excellence, quality and creativity to provide extraordinary vacation experiences that can be enjoyed for a day, a week, or a lifetime (taferresorts.com).",
    differentiators:
      "Integrated Mexico leisure platform: design-build, management, vacation ownership\nDistinctive collection brands (Mousai, Garza Blanca, Villa del Palmar, Sierra Lago)\nVillagroup Resorts ownership within the TAFER collection\nPacific and Caribbean beach destination concentration",
    managementPhilosophy:
      "Leisure-first operating culture focused on extraordinary vacation experiences, quality, and creative design across owned and managed resort assets.",
    "Service Models Supported": ["Resort", "Full-service", "Lifestyle", "Boutique"],
    "Brand Families Operated": ["Independent", "Soft brands / collections"],
    chainScalesSupported: ["Luxury", "Upper Upscale", "Upscale"],
    propertyTypes: ["Resort", "Full Service", "Boutique", "Lifestyle"],
    additionalExperience: ["Resort"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: NM,
    brand_signal_franchise_align: "Moderate",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    brand_soft_independent_narrative:
      "TAFER’s public collection is primarily proprietary/leisure brands (Mousai, Garza Blanca, Villa del Palmar, Sierra Lago) rather than major franchise flags—confirm any soft-brand or third-party management mandates in diligence.",
    brand_narrative_relationship:
      "Operator-owned leisure collection with Villagroup Resorts ownership inside the TAFER platform. Major franchise family relationships are not the primary public positioning on taferresorts.com.",
    brand_narrative_compliance:
      "Collection quality and award-winning positioning cited publicly; treat specific brand QA programs and certifications as asset-level diligence items.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "Hotel Mousai", portfolioMix: "TAFER collection", assetContext: "Puerto Vallarta & Cancún", relationshipStatus: "Active (public)" },
      { brandFlagType: "Garza Blanca Preserve Resort & Spa", portfolioMix: "TAFER collection", assetContext: "Puerto Vallarta", relationshipStatus: "Active (public)" },
      { brandFlagType: "Villa del Palmar Cancun Resort & Spa", portfolioMix: "TAFER / Villagroup", assetContext: "Cancún", relationshipStatus: "Active (public)" },
      { brandFlagType: "Sierra Lago Resort & Spa", portfolioMix: "TAFER collection", assetContext: "Mexico leisure", relationshipStatus: "Active (public)" },
      { brandFlagType: "Sian Ka’an Village", portfolioMix: "TAFER collection", assetContext: "Mexico leisure", relationshipStatus: "Active (public)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "Proprietary leisure brands", description: "Mousai, Garza Blanca, Villa del Palmar, Sierra Lago collection positioning." },
      { title: "Villagroup Resorts", description: "Principal ownership relationship within TAFER collection." },
      { title: "Vacation ownership / residences", description: "Public service lines include vacation ownership and residences." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Design & construction", description: "Hotel design and construction cited on official About materials." },
      { title: "Resort management", description: "Operating leisure resorts and boutique villas." },
      { title: "Vacation ownership", description: "Vacation ownership and residences as integrated leisure products." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Collection standards", description: "Quality/creative excellence positioning for TAFER collection hotels." },
      { title: "Asset diligence", description: "Confirm safety, insurance, and brand certifications per property." },
    ]),
    overview_why_1_headline: "What Differentiates TAFER",
    overview_why_1_story:
      "Integrated Mexico leisure platform spanning design-build, resort management, and vacation ownership—not a pure third-party franchise manager.",
    overview_why_2_headline: "Distinctive Collection",
    overview_why_2_story:
      "Published collection includes Mousai, Garza Blanca, Villa del Palmar, Sierra Lago, and Sian Ka’an Village across Pacific and Caribbean leisure destinations.",
    overview_why_3_headline: "Three Decades of Leisure Depth",
    overview_why_3_story:
      "Official materials cite more than 30 years of team experience in leisure and tourism.",
    overview_bestat_1_headline: "Mexico Beach Leisure",
    overview_bestat_1_story:
      "Core public footprint in Puerto Vallarta, Cancún, Los Cabos/Diamante, and related Mexico leisure corridors.",
    overview_bestat_2_headline: "Design-to-Operations",
    overview_bestat_2_story:
      "Public service lines include hotel design/construction through resort management and marketing/concept design.",
    overview_bestat_3_headline: "Vacation Ownership",
    overview_bestat_3_story:
      "Vacation ownership and residences are part of the published TAFER leisure offering.",
    overview_signal_1_value: "Mexico leisure resort collection (Mousai · Garza Blanca · Villa del Palmar · Sierra Lago)",
    overview_signal_2_value: "30+ years team experience in leisure & tourism (taferresorts.com)",
    overview_signal_3_value: "Villagroup Resorts ownership within TAFER collection",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Standard",
    sustainabilityPrograms:
      "Not fully enumerated on public About page — confirm property-level sustainability programs in diligence.",
    esgReporting:
      "No standardized ESG reporting protocol published on taferresorts.com About materials; align expectations during owner diligence.",
  }),

  "grupo-presidente": Object.freeze({
    company_name: "Grupo Presidente",
    website: "https://grupopresidente.com.mx/",
    companyTagline: "100% Mexican hospitality — hotels, restaurants, and guest experiences.",
    primaryServiceModel: "Mixed",
    headquarters: "Mexico City, Mexico",
    companySize: "Large (50+ properties)",
    figuresAsOf: FIGURES,
    companyDescription:
      "Grupo Presidente is a 100% Mexican hospitality company with more than 50 years of experience. Official materials state its purpose is to create unique experiences that satisfy guests and diners, with excellence as a core value. The company represents Marriott, Hyatt, and IHG brands in Mexico across major cities and beach destinations, and operates more than 50 bars and restaurants including Au Pied de Cochon, Alfredo di Roma, Chapulín, and The Palm.",
    companyHistory:
      "Public site positions Grupo Presidente as a multi-decade Mexican hospitality operator representing major international hotel brands (Marriott, Hyatt, IHG) in cities and beaches across Mexico, with a parallel luxury F&B portfolio and a Ballesol alliance for senior living residences.",
    missionStatement:
      "Create unique experiences that bring satisfaction to guests and diners (grupopresidente.com.mx).",
    differentiators:
      "Mexico-only operator representing Marriott, Hyatt, and IHG\nCity + beach portfolio\n50+ bars/restaurants with named prestige concepts\nTravacacion city-to-beach credit program",
    managementPhilosophy:
      "Excellence-led Mexican hospitality combining branded hotel operations with distinctive F&B experiences for guests and diners.",
    "Service Models Supported": ["Full-service", "Resort", "Lifestyle", "Select-service"],
    "Brand Families Operated": ["Marriott", "Hyatt", "IHG"],
    chainScalesSupported: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale"],
    propertyTypes: ["Full Service", "Resort", "Lifestyle", "Select Service"],
    additionalExperience: ["Urban", "Resort"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    numberOfBrands: 3,
    brand_soft_independent_narrative:
      "Public positioning centers on Marriott, Hyatt, and IHG representation in Mexico; soft/collection formats may appear within those ecosystems—confirm per asset.",
    brand_narrative_relationship:
      "Official materials state Grupo Presidente represents Marriott, Hyatt, and IHG brands in Mexico across major cities and beach destinations.",
    brand_narrative_compliance:
      "Brand-standard hotel operations expected under Marriott/Hyatt/IHG systems; confirm QA cadence and PIP obligations per management/franchise structure.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "Marriott International", portfolioMix: "Mexico branded", assetContext: "City & beach (e.g. Presidente InterContinental examples on site)", relationshipStatus: "Active (public)" },
      { brandFlagType: "Hyatt", portfolioMix: "Mexico branded", assetContext: "City & beach", relationshipStatus: "Active (public)" },
      { brandFlagType: "IHG", portfolioMix: "Mexico branded", assetContext: "City & beach (e.g. Holiday Inn Resort examples on site)", relationshipStatus: "Active (public)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "Marriott", description: "Represented brand family in Mexico city and beach destinations." },
      { title: "Hyatt", description: "Represented brand family in Mexico." },
      { title: "IHG", description: "Represented brand family in Mexico." },
      { title: "F&B portfolio", description: "50+ bars/restaurants including Au Pied de Cochon, Alfredo di Roma, Chapulín, The Palm." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Branded hotel operations", description: "Marriott/Hyatt/IHG hotel operations across Mexico." },
      { title: "Luxury F&B", description: "Named prestige restaurant concepts as guest demand drivers." },
      { title: "City-to-beach programs", description: "Travacacion credit program cited on official site." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Brand QA", description: "Franchise/brand standards under major international families." },
      { title: "F&B excellence", description: "Restaurant quality as a stated core value alongside hotel excellence." },
    ]),
    overview_why_1_headline: "What Differentiates Grupo Presidente",
    overview_why_1_story:
      "100% Mexican operator with 50+ years experience representing Marriott, Hyatt, and IHG plus a large prestige F&B portfolio.",
    overview_why_2_headline: "City + Beach Mexico",
    overview_why_2_story:
      "Portfolio spans major Mexican cities and beach destinations including Cancún, Cozumel, Tulum, and Ixtapa examples on the official site.",
    overview_why_3_headline: "F&B Depth",
    overview_why_3_story:
      "More than 50 bars and restaurants with nationally/internationally recognized concepts.",
    overview_bestat_1_headline: "Major Brand Representation",
    overview_bestat_1_story: "Public representation of Marriott, Hyatt, and IHG in Mexico.",
    overview_bestat_2_headline: "Integrated Hospitality",
    overview_bestat_2_story: "Hotels plus restaurants positioned as one guest-experience platform.",
    overview_bestat_3_headline: "Multi-Decade Tenure",
    overview_bestat_3_story: "Official site cites more than 50 years of hospitality experience.",
    overview_signal_1_value: "50+ years Mexican hospitality (grupopresidente.com.mx)",
    overview_signal_2_value: "Brand families: Marriott · Hyatt · IHG (Mexico)",
    overview_signal_3_value: "50+ bars & restaurants incl. Au Pied de Cochon, Alfredo di Roma, Chapulín, The Palm",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Standard",
    sustainabilityPrograms:
      "Not fully enumerated on public home materials — confirm property-level sustainability programs in diligence.",
    esgReporting:
      "No standardized ESG reporting protocol highlighted on the public home page; align expectations during owner diligence.",
  }),

  highgate: Object.freeze({
    company_name: "Highgate",
    website: "https://www.highgate.com/",
    companyTagline: "Hotel management, investment, technology, and development — label CALA vs enterprise scale.",
    primaryServiceModel: "Mixed",
    headquarters: "New York, United States",
    companySize: "Large (50+ properties)",
    figuresAsOf: FIGURES,
    yearsInBusiness: 30,
    companyDescription:
      "Highgate is a hotel management, investment, technology and development firm with a diverse portfolio across North America, Europe, the Caribbean, and Latin America. Official corporate metrics cite a 30-year track record, 200+ person corporate team, 79,348 keys across 400+ properties, and over $15B aggregate real estate value / $5B+ revenue under management.",
    companyHistory:
      "Highgate presents a multi-decade hospitality investment and management platform spanning lifestyle & luxury resort, full-service, and select-service assets, with in-house venture capital focused on hospitality-linked technology. CALA examples on the corporate site include properties such as Hotel Paracas, a Luxury Collection Resort (Peru) — treat enterprise scale as labeled parent/platform context for any CALA-specific Explorer profile.",
    missionStatement:
      "Operational optimization, branding, experiential curation, and favorable partner returns through operational outperformance (highgate.com).",
    differentiators:
      "Integrated management + investment + development + tech VC\nLarge multi-region portfolio (400+ properties / 79,348 keys per corporate site)\nDeep revenue management and branding capability\nExplorer must label CALA asset examples vs global enterprise scale",
    managementPhilosophy:
      "Operational outperformance through branding, experiential curation, and partner-aligned returns—with technology and investment capability inside the same platform.",
    "Service Models Supported": ["Full-service", "Select-service", "Resort", "Lifestyle"],
    "Brand Families Operated": ["Marriott", "Hilton", "Hyatt", "IHG", "Independent", "Soft brands / collections"],
    chainScalesSupported: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    propertyTypes: ["Full Service", "Select Service", "Resort", "Lifestyle", "Boutique"],
    additionalExperience: ["Urban", "Resort", "Airport"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    brand_soft_independent_narrative:
      "Enterprise portfolio spans branded lifestyle & luxury, full-service, and select-service assets plus soft/collection formats—confirm CALA-specific brand mix per asset list.",
    brand_narrative_relationship:
      "Global multi-brand platform. Caribbean and Latin America are cited among regions on highgate.com; CALA operating footprint should be diligence-confirmed and labeled separately from enterprise totals.",
    brand_narrative_compliance:
      "Enterprise brand and operating standards expected across the portfolio; confirm asset-level franchise QA and PIP obligations in diligence.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "Lifestyle & luxury resort", portfolioMix: "Enterprise platform", assetContext: "Multi-region including CALA examples (e.g. Hotel Paracas, Luxury Collection — Peru)", relationshipStatus: "Active (public)" },
      { brandFlagType: "Full-service", portfolioMix: "Enterprise platform", assetContext: "Multi-region", relationshipStatus: "Active (public)" },
      { brandFlagType: "Select-service", portfolioMix: "Enterprise platform", assetContext: "Multi-region", relationshipStatus: "Active (public)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "Enterprise brand platform", description: "Multi-family branded portfolio across North America, Europe, Caribbean, LATAM." },
      { title: "CALA examples", description: "Corporate site cites Caribbean/LATAM presence; label vs global totals." },
      { title: "Tech / VC adjacency", description: "In-house venture focus on hospitality-linked technology." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Management + investment", description: "Integrated operating and capital platform." },
      { title: "Development", description: "Hotel development capability cited on corporate site." },
      { title: "Commercial optimization", description: "Revenue and branding outperformance positioning." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Enterprise controls", description: "Institutional operating and partner governance expected at platform scale." },
      { title: "ESG program", description: "Corporate ESG/sustainability described on highgate.com — confirm property-level certifications." },
    ]),
    overview_why_1_headline: "What Differentiates Highgate",
    overview_why_1_story:
      "Full-stack management, investment, technology, and development under one enterprise platform.",
    overview_why_2_headline: "Scale with Regional Reach",
    overview_why_2_story:
      "Official metrics: 400+ properties / 79,348 keys across North America, Europe, Caribbean, and Latin America.",
    overview_why_3_headline: "Label CALA Clearly",
    overview_why_3_story:
      "Use enterprise figures as context only; underwrite the specific CALA assets and agreements separately.",
    overview_bestat_1_headline: "Operational Outperformance",
    overview_bestat_1_story: "Corporate positioning emphasizes branding, experiential curation, and partner returns.",
    overview_bestat_2_headline: "Multi-Segment Portfolio",
    overview_bestat_2_story: "Lifestyle & luxury resort, full-service, and select-service assets.",
    overview_bestat_3_headline: "Technology Adjacency",
    overview_bestat_3_story: "In-house hospitality technology investment capability.",
    overview_signal_1_value: "400+ properties · 79,348 keys (highgate.com)",
    overview_signal_2_value: "$15B+ aggregate real estate value · $5B+ revenue under management",
    overview_signal_3_value: "Regions include Caribbean & Latin America — confirm CALA managed subset",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Comprehensive",
    sustainabilityPrograms: "Yes - Standard",
    esgReporting: "Yes - Standard",
  }),

  "grupo-hotelero-santa-fe": Object.freeze({
    company_name: "Grupo Hotelero Santa Fe",
    website: "https://gsf-hotels.com/corporativo/en/",
    companyTagline: "Listed Mexican hotel group — Krystal plus Hyatt, Hilton, and Secrets.",
    primaryServiceModel: "Mixed",
    headquarters: "Mexico City, Mexico",
    companySize: "Medium (10-50 properties)",
    figuresAsOf: FIGURES,
    companyDescription:
      "Grupo Hotelero Santa Fe (S.A.B. de C.V., BMV: HOTEL) is a leading Mexican hotel company focused on acquiring, developing and operating proprietary and third-party hotels. Official materials describe a multi-brand management model with Krystal®, Hyatt®, Hilton® and Secrets®, combining strategic location and quality across Mexico.",
    companyHistory:
      "Public corporate site positions GSF as a publicly listed Mexican hotel group (BMV: HOTEL) operating owned and third-party hotels under Krystal and major international brands, with continuous portfolio growth across beach and city destinations (e.g. Cancún, Vallarta, Mexico City Insurgentes, Tulum, Los Cabos, San Miguel de Allende).",
    missionStatement:
      "Strategic location, exceptional quality, and multi-brand management across proprietary and third-party hotels (gsf-hotels.com).",
    differentiators:
      "Publicly listed Mexican hotel group (BMV: HOTEL)\nProprietary Krystal brand plus Hyatt, Hilton, and Secrets\nOwner-operator and third-party management mix\nBeach and city portfolio across Mexico",
    managementPhilosophy:
      "Acquire, develop, and operate proprietary and third-party hotels with multi-brand standards and strategic location discipline.",
    "Service Models Supported": ["Full-service", "Resort", "Lifestyle", "Select-service"],
    "Brand Families Operated": ["Hyatt", "Hilton", "Independent", "Soft brands / collections"],
    chainScalesSupported: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale"],
    propertyTypes: ["Full Service", "Resort", "Lifestyle", "Select Service"],
    additionalExperience: ["Urban", "Resort"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    brand_soft_independent_narrative:
      "Proprietary Krystal brand operates alongside Hyatt, Hilton, and Secrets; residences (e.g. San Miguel de Allende) appear in public portfolio highlights.",
    brand_narrative_relationship:
      "Official corporate materials describe multi-brand management with Krystal®, Hyatt®, Hilton® and Secrets® across proprietary and third-party hotels in Mexico.",
    brand_narrative_compliance:
      "Listed-company reporting expectations plus brand-family standards for Hyatt/Hilton/Secrets and Krystal collection standards—confirm per asset.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "Krystal", portfolioMix: "Proprietary", assetContext: "Mexico beach & city (e.g. Cancún, Vallarta, Los Cabos)", relationshipStatus: "Active (public)" },
      { brandFlagType: "Hyatt", portfolioMix: "International brand", assetContext: "e.g. Hyatt Regency Mexico City Insurgentes", relationshipStatus: "Active (public)" },
      { brandFlagType: "Hilton", portfolioMix: "International brand", assetContext: "Mexico portfolio", relationshipStatus: "Active (public)" },
      { brandFlagType: "Secrets", portfolioMix: "International brand", assetContext: "e.g. Secrets Tulum", relationshipStatus: "Active (public)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "Krystal proprietary", description: "Core owned/operated brand family." },
      { title: "Hyatt / Hilton / Secrets", description: "International brand relationships across Mexico." },
      { title: "Third-party management", description: "Public model includes third-party hotels alongside proprietary." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Acquire & develop", description: "Public focus on acquiring, developing, and operating hotels." },
      { title: "Multi-brand operations", description: "Krystal plus major international flags." },
      { title: "Beach + city", description: "Portfolio spans leisure and urban Mexico destinations." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Public company governance", description: "BMV: HOTEL listing implies institutional reporting expectations." },
      { title: "Brand standards", description: "Hyatt/Hilton/Secrets and Krystal compliance by asset." },
    ]),
    overview_why_1_headline: "What Differentiates GSF",
    overview_why_1_story:
      "Listed Mexican hotel group combining proprietary Krystal with Hyatt, Hilton, and Secrets.",
    overview_why_2_headline: "Owner-Operator + Third-Party",
    overview_why_2_story:
      "Public model covers proprietary and third-party hotels across Mexico.",
    overview_why_3_headline: "Beach and City Footprint",
    overview_why_3_story:
      "Portfolio highlights include Cancún, Vallarta, Mexico City, Tulum, Los Cabos, and San Miguel de Allende.",
    overview_bestat_1_headline: "Multi-Brand Mexico",
    overview_bestat_1_story: "Krystal®, Hyatt®, Hilton® and Secrets® management model.",
    overview_bestat_2_headline: "Listed Platform",
    overview_bestat_2_story: "S.A.B. de C.V., BMV: HOTEL — institutional ownership context.",
    overview_bestat_3_headline: "Growth Corridor Coverage",
    overview_bestat_3_story: "Continuous portfolio growth across Mexico beach and city destinations.",
    overview_signal_1_value: "BMV: HOTEL listed Mexican hotel group (gsf-hotels.com)",
    overview_signal_2_value: "Brands: Krystal · Hyatt · Hilton · Secrets",
    overview_signal_3_value: "Mexico beach + city portfolio (Cancún, Vallarta, CDMX, Tulum, Cabos, SMA)",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Standard",
    sustainabilityPrograms:
      "Confirm property-level sustainability programs in diligence — not fully enumerated on corporate EN overview.",
    esgReporting:
      "As a listed company, institutional reporting expectations apply; confirm ESG pack cadence in diligence.",
  }),

  "arriva-hospitality-group": Object.freeze({
    company_name: "Arriva Hospitality Group (AHG)",
    website: "https://www.arrivahotels.mx/",
    companyTagline: "50+ years Mexico tourism — Crown Paradise, Vista, Sensira, and select international flags.",
    primaryServiceModel: "Mixed",
    headquarters: "Guadalajara, Mexico",
    companySize: "Medium (10-50 properties)",
    figuresAsOf: FIGURES,
    yearEstablished: 1967,
    yearsInBusiness: 59,
    companyDescription:
      "Arriva Hospitality Group (AHG) is a Mexican hospitality group with more than 50 years in the tourist market. Official materials describe city and beach hotels under brands such as Crown Paradise, Vista, and Sensira, and cite creation of Arriva Hospitality Group in 2012 following a multi-decade hotel operating history dating to 1967.",
    companyHistory:
      "Timeline on the official About page: hotel operations from 1967 (Presidente and Alameda Morelia), Vista Hotels from 1978, Crown Paradise Club Cancun (1990), Crown Paradise Golden Puerto Vallarta (1997), fusion of Aranzazú and Vista (2005), Arriva Hospitality Group created (2012), Westin Cozumel (2017), Ibis Tijuana (2019), Sensira Resort & Spa Riviera Maya (2020). Public home materials also cite roughly 1,700+ rooms across beaches and cities.",
    missionStatement:
      "Make people happy while investments grow — seeking aligned investors (arrivahotels.mx).",
    differentiators:
      "50+ years Mexico tourism operating history\nProprietary Crown Paradise / Vista / Sensira brands\nSelect international flags in timeline (e.g. Westin, Ibis)\nCity + beach mix across Mexico",
    managementPhilosophy:
      "Long-tenured Mexican owner-operator culture pairing guest happiness with investor-aligned growth.",
    "Service Models Supported": ["Resort", "Full-service", "Select-service", "All-inclusive", "Lifestyle"],
    "Brand Families Operated": ["Marriott", "Accor", "Independent", "Soft brands / collections"],
    chainScalesSupported: ["Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    propertyTypes: ["Resort", "Full Service", "Select Service", "Lifestyle"],
    additionalExperience: ["Urban", "Resort"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    brand_soft_independent_narrative:
      "Portfolio mixes proprietary leisure brands (Crown Paradise, Vista, Sensira) with select international flags cited in the public timeline (e.g. Westin, Ibis).",
    brand_narrative_relationship:
      "Owner-operator platform with proprietary brands plus selective international affiliations in Mexico city and beach markets.",
    brand_narrative_compliance:
      "Confirm brand QA for international flags and proprietary brand standards per asset in diligence.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "Crown Paradise", portfolioMix: "Proprietary", assetContext: "Mexico beach leisure", relationshipStatus: "Active (public)" },
      { brandFlagType: "Vista", portfolioMix: "Proprietary", assetContext: "Mexico city & beach", relationshipStatus: "Active (public)" },
      { brandFlagType: "Sensira Resort & Spa", portfolioMix: "Proprietary", assetContext: "Riviera Maya", relationshipStatus: "Active (public)" },
      { brandFlagType: "Westin (timeline)", portfolioMix: "International flag", assetContext: "Cozumel (2017 timeline)", relationshipStatus: "Cited (public timeline)" },
      { brandFlagType: "Ibis (timeline)", portfolioMix: "International flag", assetContext: "Tijuana (2019 timeline)", relationshipStatus: "Cited (public timeline)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "Crown Paradise / Vista / Sensira", description: "Core proprietary leisure and city brands." },
      { title: "Select international flags", description: "Westin and Ibis appear in official timeline milestones." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Long-tenured operations", description: "Operating history from 1967 through AHG formation in 2012." },
      { title: "City + beach", description: "Portfolio across Cancún, PV, Riviera Maya, Manzanillo, Morelia, Guadalajara, Cozumel, Tijuana." },
      { title: "Investor alignment", description: "Public mission pairs guest happiness with investment growth." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Proprietary standards", description: "Crown Paradise / Vista / Sensira operating standards." },
      { title: "Flag compliance", description: "Confirm international brand obligations where applicable." },
    ]),
    overview_why_1_headline: "What Differentiates Arriva",
    overview_why_1_story:
      "Multi-decade Mexico tourism operator with proprietary leisure brands and selective international flags.",
    overview_why_2_headline: "1,700+ Rooms Public Scale",
    overview_why_2_story:
      "Corporate site cites roughly 1,700+ rooms across beaches and cities.",
    overview_why_3_headline: "Documented Operating Timeline",
    overview_why_3_story:
      "Public About timeline from 1967 through Sensira (2020) and AHG formation (2012).",
    overview_bestat_1_headline: "Mexico Leisure Brands",
    overview_bestat_1_story: "Crown Paradise, Vista, and Sensira as core guest-facing brands.",
    overview_bestat_2_headline: "City + Beach Mix",
    overview_bestat_2_story: "Footprint across leisure beach and secondary city markets.",
    overview_bestat_3_headline: "Investor-Seeking Platform",
    overview_bestat_3_story: "Public positioning seeks aligned investors while growing guest experiences.",
    overview_signal_1_value: "Operating roots from 1967 · AHG created 2012 (arrivahotels.mx)",
    overview_signal_2_value: "≈1,700+ rooms across Mexico beaches & cities (public site)",
    overview_signal_3_value: "Brands: Crown Paradise · Vista · Sensira (+ select international flags)",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Standard",
    sustainabilityPrograms:
      "Not fully enumerated on public About materials — confirm property-level programs in diligence.",
    esgReporting:
      "No standardized ESG reporting protocol highlighted on public About page; align expectations during owner diligence.",
  }),

  "brittain-resorts-hotels": Object.freeze({
    company_name: "Brittain Resorts & Hotels (BRH)",
    website: "https://brittainresorts.com/",
    companyTagline: "Full-service US Southeast management — confirm CALA relevance before Active release.",
    primaryServiceModel: "Full-service focus",
    headquarters: "Myrtle Beach, United States",
    companySize: "Medium (10-50 properties)",
    figuresAsOf: FIGURES,
    yearEstablished: 1943,
    yearsInBusiness: 83,
    companyDescription:
      "Brittain Resorts & Hotels (BRH) is a full-service hospitality management company founded in 1943 and based in the US Southeast (Myrtle Beach, SC). Official materials cite multi-layered expertise across hotel and resort operations, more than 4,700 rooms/suites/condos under management, and 45+ restaurants and bars.",
    companyHistory:
      "Founded in 1943, BRH presents itself as a leading full-service management company in the Southeast with decades of measured operational strategies and proprietary data-driven approaches in HR, revenue management, and sales & marketing.",
    missionStatement:
      "Enrich the lives of team members, guests, partners, and communities through exceptional guest experiences and superior returns (brittainresorts.com).",
    differentiators:
      "Long-tenured Southeast US full-service manager\n4,700+ rooms/suites/condos under management\n45+ restaurants and bars\nIntegrated ops, HR, revenue, sales & marketing, and call center services\nCALA owners: treat as non-core unless a specific mandate fits",
    managementPhilosophy:
      "Full-service management with data-driven HR, revenue, and sales & marketing disciplines oriented to guest experience and owner returns.",
    "Service Models Supported": ["Full-service", "Resort", "Lifestyle"],
    "Brand Families Operated": ["Independent", "Soft brands / collections", "Marriott", "Hilton", "IHG"],
    chainScalesSupported: ["Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    propertyTypes: ["Full Service", "Resort", "Lifestyle", "Select Service"],
    additionalExperience: ["Resort", "Urban"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: NM,
    brand_signal_franchise_align: "Moderate",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    brand_soft_independent_narrative:
      "Southeast US full-service management platform; brand mix should be confirmed per managed asset list—do not assume CALA franchise depth from the corporate homepage alone.",
    brand_narrative_relationship:
      "US Southeast third-party/full-service management focus. CALA relevance is not established on brittainresorts.com — gate before Active Explorer release.",
    brand_narrative_compliance:
      "Confirm brand QA and franchise obligations per managed asset; corporate site emphasizes operating services rather than a single brand family.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "Full-service hotels & resorts", portfolioMix: "US Southeast managed", assetContext: "4,700+ rooms/suites/condos under management", relationshipStatus: "Active (public)" },
      { brandFlagType: "F&B outlets", portfolioMix: "Managed F&B", assetContext: "45+ restaurants and bars", relationshipStatus: "Active (public)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "Full-service management", description: "Hotel and resort operations across the Southeast." },
      { title: "Integrated support services", description: "HR, revenue management, sales & marketing, call center." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Resort & hotel ops", description: "Multi-layered full-service operating expertise." },
      { title: "F&B scale", description: "45+ restaurants and bars under management." },
      { title: "Commercial support", description: "Revenue, sales & marketing, and call center services." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Owner returns focus", description: "Mission cites superior returns alongside guest experience." },
      { title: "Regional operating depth", description: "Southeast concentration — not a CALA platform by default." },
    ]),
    overview_why_1_headline: "What Differentiates BRH",
    overview_why_1_story:
      "Founded 1943 full-service Southeast manager with large F&B footprint and integrated support services.",
    overview_why_2_headline: "Scale Signal",
    overview_why_2_story: "Official: 4,700+ rooms/suites/condos and 45+ restaurants and bars.",
    overview_why_3_headline: "CALA Gate",
    overview_why_3_story:
      "US Southeast core — confirm any CALA mandate before treating this profile as region-relevant.",
    overview_bestat_1_headline: "Full-Service Depth",
    overview_bestat_1_story: "Hotel and resort operations with multi-department support services.",
    overview_bestat_2_headline: "F&B Platform",
    overview_bestat_2_story: "45+ restaurants and bars cited on the corporate site.",
    overview_bestat_3_headline: "Long Tenure",
    overview_bestat_3_story: "Operating since 1943 from Myrtle Beach, SC.",
    overview_signal_1_value: "Founded 1943 · Myrtle Beach, SC (brittainresorts.com)",
    overview_signal_2_value: "4,700+ rooms/suites/condos under management",
    overview_signal_3_value: "45+ restaurants & bars · US Southeast core (CALA relevance gated)",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Standard",
    sustainabilityPrograms:
      "Not fully enumerated on corporate homepage — confirm property-level programs in diligence.",
    esgReporting:
      "No standardized ESG reporting protocol highlighted on the corporate homepage; align expectations during owner diligence.",
  }),

  "atlantica-hotels-international": Object.freeze({
    company_name: "Atlantica Hotels International (AHI)",
    website: "https://atlanticahotels.com.br/",
    companyTagline: "Brazil hotel management & distribution — 195+ hotels across 75+ destinations.",
    primaryServiceModel: "Mixed",
    headquarters: "Barueri, Brazil",
    companySize: "Large (50+ properties)",
    figuresAsOf: FIGURES,
    companyDescription:
      "Atlantica Hotels International (AHI) operates the Let’s Atlantica official sales platform assembling more than 195 hotels and residential properties across more than 75 destinations in Brazil. Public materials associate the platform with brands such as Radisson, Quality, Comfort, Quality Suites, and Metropolitan across business and leisure travel.",
    companyHistory:
      "AHI is a Brazil-focused hospitality management/distribution platform. Official Let’s Atlantica pages emphasize national coverage (195+ hotels / 75+ destinations) and ongoing systems readiness (including tax-reform operational notes published for 2026).",
    missionStatement:
      "Let’s Travel. Let’s Enjoy. Let’s Experience. — national Brazil hotel distribution/operations platform (atlanticahotels.com.br).",
    differentiators:
      "Brazil-wide scale (195+ hotels / 75+ destinations)\nMulti-brand midscale to upper-midscale portfolio (Radisson, Quality, Comfort, etc.)\nIntegrated Let’s Atlantica distribution platform\nConfirm any non-Brazil CALA expansion separately",
    managementPhilosophy:
      "National Brazil hotel management and distribution with multi-brand operating standards for business and leisure travelers.",
    "Service Models Supported": ["Full-service", "Select-service", "Focused-service", "Lifestyle"],
    "Brand Families Operated": ["Choice", "Independent", "Soft brands / collections"],
    chainScalesSupported: ["Upper Upscale", "Upscale", "Upper Midscale", "Midscale", "Economy"],
    propertyTypes: ["Full Service", "Select Service", "Lifestyle"],
    additionalExperience: ["Urban", "Resort"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    numberOfBrands: 5,
    brand_soft_independent_narrative:
      "Let’s Atlantica assembles branded and residential products (Radisson, Quality, Comfort, Quality Suites, Metropolitan and related labels) across Brazil—confirm soft vs hard brand mix per asset.",
    brand_narrative_relationship:
      "Brazil-focused multi-brand management/distribution platform. Public materials associate AHI with Radisson, Quality, Comfort, Quality Suites, and Metropolitan.",
    brand_narrative_compliance:
      "Brand-family operating and distribution standards across the Let’s Atlantica network; confirm franchise QA per flag.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "Radisson", portfolioMix: "Brazil network", assetContext: "Upper midscale / upscale business & leisure", relationshipStatus: "Active (public)" },
      { brandFlagType: "Quality / Quality Suites", portfolioMix: "Brazil network", assetContext: "Midscale / upper midscale", relationshipStatus: "Active (public)" },
      { brandFlagType: "Comfort", portfolioMix: "Brazil network", assetContext: "Midscale / focused-service", relationshipStatus: "Active (public)" },
      { brandFlagType: "Metropolitan", portfolioMix: "Brazil network", assetContext: "Urban / lifestyle labels on public materials", relationshipStatus: "Active (public)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "Let’s Atlantica network", description: "195+ hotels/residentials across 75+ Brazilian destinations." },
      { title: "Choice / midscale families", description: "Quality, Comfort, and related labels featured publicly." },
      { title: "Radisson", description: "Upper midscale/upscale brand association on public materials." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "National distribution", description: "Let’s Atlantica sales platform across Brazil." },
      { title: "Multi-brand operations", description: "Business and leisure hotels under multiple midscale/upscale labels." },
      { title: "Systems readiness", description: "Public operational notes including 2026 tax-reform readiness." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Network standards", description: "Operating consistency across Let’s Atlantica hotels." },
      { title: "Brand QA", description: "Confirm franchise/brand compliance by flag and asset." },
    ]),
    overview_why_1_headline: "What Differentiates Atlantica",
    overview_why_1_story:
      "Brazil-wide management and distribution platform with 195+ hotels across 75+ destinations.",
    overview_why_2_headline: "Multi-Brand Midscale Strength",
    overview_why_2_story:
      "Public association with Radisson, Quality, Comfort, Quality Suites, and Metropolitan.",
    overview_why_3_headline: "National Coverage",
    overview_why_3_story:
      "Let’s Atlantica assembles hotels and residentials nationwide including Northeast leisure/business hubs.",
    overview_bestat_1_headline: "Scale in Brazil",
    overview_bestat_1_story: "Official: 195+ hotels and residentials in 75+ destinations.",
    overview_bestat_2_headline: "Distribution Platform",
    overview_bestat_2_story: "Let’s Atlantica official sales platform for the network.",
    overview_bestat_3_headline: "Business + Leisure",
    overview_bestat_3_story: "Portfolio spans business and leisure travel use cases.",
    overview_signal_1_value: "195+ hotels/residentials · 75+ Brazilian destinations (atlanticahotels.com.br)",
    overview_signal_2_value: "Brands cited: Radisson · Quality · Comfort · Quality Suites · Metropolitan",
    overview_signal_3_value: "Brazil nationwide platform — confirm any non-Brazil CALA expansion separately",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Standard",
    sustainabilityPrograms:
      "Not fully enumerated on Let’s Atlantica public pages reviewed — confirm property-level programs in diligence.",
    esgReporting:
      "No standardized ESG reporting protocol highlighted on the pages reviewed; align expectations during owner diligence.",
  }),
});

export function listProfileDeepPackSlugs() {
  return Object.keys(OPERATOR_SETUP_PROFILE_DEEP_PACKS);
}

export function getProfileDeepPack(slug) {
  return OPERATOR_SETUP_PROFILE_DEEP_PACKS[slug] || null;
}

export function resolveProfileDeepMasterMeta(slug) {
  const q = packFor(slug);
  const pack = getProfileDeepPack(slug);
  if (!q?.recordId || !pack) return null;
  return {
    slug,
    recordId: q.recordId,
    companyName: q.companyName,
    pack,
  };
}
