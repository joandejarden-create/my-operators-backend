/**
 * Wave D Operator Setup content — Royalton, Driftwood, Remington.
 * Source-backed from official sites (fetched 2026-07-24). Not invented CALA counts.
 */
const FIGURES = "Public sites as of 2026-07-24";
const NM = "Not Measured / N/A";

function j(arr) {
  return JSON.stringify(arr);
}

/** @type {Record<string, import('./operator-setup-website-content-packs.js').SetupContentPack>} */
export const WAVE_D_WEBSITE_CONTENT_PACKS = Object.freeze({
  "royalton-hotels-resorts": Object.freeze({
    sources: [
      { title: "Royalton Hotels & Resorts", url: "https://royalton.com/" },
      { title: "Royalton Resorts guest portfolio", url: "https://www.royaltonresorts.com/" },
      {
        title: "Blue Diamond → Royalton Hotels & Resorts press",
        url: "https://www.royaltonresorts.com/press/all-inclusive-redefined-again-blue-diamond-resorts-is-now-royalton-hotels--resorts",
      },
    ],
    profile: {
      website: "https://www.royaltonresorts.com/",
      primaryServiceModel: "Full-service focus",
      companyDescription:
        "Royalton Hotels & Resorts is a Caribbean and Mexico–focused all-inclusive hospitality company (formerly Blue Diamond Resorts; corporate identity consolidated under Royalton Hotels & Resorts beginning August 2025 per company press). Public materials describe a curated multi-brand all-inclusive portfolio across Caribbean and Mexico beach destinations (and Costa Rica), with brands including Royalton Luxury Resorts, Royalton Hideaway, Royalton CHIC, Planet Hollywood by Royalton, Royalton Vessence, Mystique by Royalton, and Grand Lido Negril. Guest site positioning cites on the order of ~23 resorts / 7 brands / 8,600+ rooms — treat as enterprise portfolio context and confirm current counts in diligence.",
      companyHistory:
        "Began as Blue Diamond Resorts (~2010/2011 public origin narratives); in 2025 the company publicly evolved its corporate identity to Royalton Hotels & Resorts to align with the recognition of its Royalton-branded resorts. Headquarters / marketing contact published as Cidel Place, Lower Collymore Rock, St. Michael, Barbados.",
      missionStatement:
        "Modern all-inclusive hospitality — All-In Luxury® guest experiences across distinct Royalton family brands (royaltonresorts.com / company press).",
      differentiators:
        "CALA all-inclusive multi-brand platform (Mexico, Jamaica, Dominican Republic, Antigua, St. Lucia, Grenada, Barbados, Costa Rica cited on guest site)\nFormerly Blue Diamond Resorts — now Royalton Hotels & Resorts (2025 identity consolidation)\nBrand family spans family, adults-only, entertainment (Planet Hollywood), wellness (Vessence), and boutique (Mystique)\nSelect properties participate in Marriott Bonvoy per Royalton guest FAQ — confirm property-level affiliation in diligence",
    },
    platformMarkets: {
      specificMarkets:
        "Mexico (Cancún / Riviera Maya / Holbox), Jamaica (Negril / Montego Bay), Dominican Republic (Punta Cana / Bávaro), Antigua, Saint Lucia, Grenada, Barbados, Costa Rica — per royaltonresorts.com destination list.",
    },
    commercial: {
      bf_operating_situations:
        "Owners and asset managers of beachfront all-inclusive resorts in Mexico and the Caribbean seeking a Royalton / former Blue Diamond operating or brand-portfolio path; assets that fit family, adults-only, entertainment, or wellness all-inclusive positioning.",
      bf_not_ideal_for:
        "Urban select-service or non-all-inclusive mandates; assets outside Caribbean/Mexico leisure beach corridors without an all-inclusive brief",
    },
    governance: {
      risk_programs_narrative:
        "Confirm brand standards (including any Marriott Bonvoy / soft-brand obligations), safety, and insurance certifications per asset. Corporate HQ published in St. Michael, Barbados. Sources: royaltonresorts.com; company rebrand press.",
    },
  }),

  "driftwood-hospitality-management": Object.freeze({
    sources: [
      { title: "Driftwood Hospitality Management", url: "https://www.driftwoodhospitality.com/" },
    ],
    profile: {
      website: "https://www.driftwoodhospitality.com/",
      primaryServiceModel: "Full-service focus",
      companyDescription:
        "Driftwood Hospitality Management (DHM) is a U.S. third-party hotel management company. Official site materials describe ~25 years in business, ranking among the top 20 management companies nationwide, with on the order of 80+ hotels, 15,000+ rooms, 5,000+ employees, ~40 ownership groups, and ~24 brands (Hilton, Marriott, Hyatt, IHG, Wyndham, Choice, Margaritaville, Best Western and others cited). DHM emphasizes full-service management plus select-service and lifestyle/independent hotels, with development, acquisition, and underperforming-asset turnaround capabilities.",
      companyHistory:
        "Public site positions Driftwood as a multi-decade U.S. hospitality management platform with leadership experience across management, development, third-party management, ownership, and asset management. Headquarters publicly associated with North Palm Beach, Florida.",
      missionStatement:
        "Foster enduring partnerships with hotel owners through data analytics, revenue generation, personal service, and operational excellence (driftwoodhospitality.com).",
      differentiators:
        "Full-service-led U.S. third-party platform with select-service and lifestyle/independent capability\nPublished scale: 80+ hotels / 15K+ keys / 24 brands / 40 ownership groups (site figures — confirm in diligence)\nF&B depth cited (branded + independent outlets)\nNew-build, acquisition, and turnaround operating situations highlighted on official site",
    },
    platformMarkets: {
      specificMarkets:
        "United States (nationwide third-party portfolio per driftwoodhospitality.com). Label any CALA relevance separately — do not invent CALA hotel counts.",
    },
    commercial: {
      bf_operating_situations:
        "U.S. owners seeking full-service (and select-service / lifestyle) third-party management with brand relationships across major flags; acquisition, new-build, and revitalization mandates cited on official site.",
      bf_not_ideal_for:
        "Owners needing a dedicated CALA in-market operator without confirming Driftwood regional coverage; pure all-inclusive Mexico/Caribbean mandates without U.S. platform fit",
    },
    governance: {
      risk_programs_narrative:
        "Brand-standard QA by flag. Confirm certifications and insurance per asset. Source: driftwoodhospitality.com.",
    },
  }),

  "remington-hospitality": Object.freeze({
    sources: [
      { title: "Remington Hospitality", url: "https://www.remingtonhospitality.com/" },
      { title: "Remington Hospitality CALA", url: "https://www.remingtonhospitality.com/cala" },
    ],
    profile: {
      website: "https://www.remingtonhospitality.com/",
      primaryServiceModel: "Mixed",
      companyDescription:
        "Remington Hospitality (formerly Remington Hotels) is a U.S.-based third-party hotel management company founded in 1968. Official materials describe managing 120+ hotels across 26 brands plus independent/boutique assets, with nearly $1.2B revenues under management cited on the corporate site. Remington publicly expanded into Caribbean & Latin America (CALA) from late 2022, establishing a Miami regional office in 2023; CALA materials cite ~18 existing and development properties / ~2,500 rooms across Costa Rica, Dominican Republic, Mexico, Puerto Rico, Cayman Islands, Belize, Panama, and Peru — label enterprise U.S. scale vs CALA footprint separately.",
      companyHistory:
        "Founded 1968; multi-decade U.S. management platform rebranded to Remington Hospitality. CALA expansion announced late 2022 with Miami regional HQ in 2023. Corporate office published at 14185 Dallas Parkway, Suite 1150, Dallas, Texas.",
      missionStatement:
        "Owner-mindset hotel management — operational excellence, commercial strategy, and long-term asset value for owners, guests, and associates (remingtonhospitality.com).",
      differentiators:
        "Nearly 60 years of U.S. hotel management experience\nCALA platform with Miami regional HQ and in-market ops leadership (Costa Rica, DR, Mexico, Puerto Rico, Cayman, Belize, Panama, Peru cited)\nEnterprise scale: 120+ hotels / 26 brands / ~$1.2B RUM (corporate site — not CALA-only)",
    },
    platformMarkets: {
      specificMarkets:
        "United States (enterprise portfolio). CALA: Costa Rica, Dominican Republic, Mexico, Puerto Rico, Cayman Islands, Belize, Panama, Peru — per remingtonhospitality.com/cala (~18 existing + development / ~2,500 rooms cited).",
    },
    commercial: {
      bf_operating_situations:
        "CALA owners seeking a U.S.-systems operator with in-market Remington CALA leadership; U.S. owners seeking Remington’s branded and independent management platform.",
      bf_not_ideal_for:
        "Mandates outside Remington’s published U.S./CALA coverage without expansion plan; owners needing pure all-inclusive Mexico/Caribbean specialist without confirming Remington asset fit",
    },
    governance: {
      risk_programs_narrative:
        "Brand QA by flag. CALA regional leadership spans operations, F&B, revenue, systems/transitions. Confirm certifications per asset. Sources: remingtonhospitality.com; /cala.",
    },
  }),
});

export const WAVE_D_PROFILE_DEEP_PACKS = Object.freeze({
  "royalton-hotels-resorts": Object.freeze({
    company_name: "Royalton Hotels & Resorts",
    website: "https://www.royaltonresorts.com/",
    companyTagline: "All-In Luxury® Caribbean & Mexico all-inclusive — Royalton family of brands.",
    primaryServiceModel: "Full-service focus",
    headquarters: "St. Michael, Barbados",
    companySize: "Medium (11-50 properties)",
    figuresAsOf: FIGURES,
    yearEstablished: 2010,
    yearsInBusiness: 16,
    totalProperties: "~23 resorts / 7 brands cited on royalton.com (confirm current)",
    totalRooms: "8,600+ rooms cited on royalton.com (confirm current)",
    companyDescription: WAVE_D_WEBSITE_CONTENT_PACKS["royalton-hotels-resorts"].profile.companyDescription,
    companyHistory: WAVE_D_WEBSITE_CONTENT_PACKS["royalton-hotels-resorts"].profile.companyHistory,
    missionStatement: WAVE_D_WEBSITE_CONTENT_PACKS["royalton-hotels-resorts"].profile.missionStatement,
    differentiators: WAVE_D_WEBSITE_CONTENT_PACKS["royalton-hotels-resorts"].profile.differentiators,
    managementPhilosophy:
      "All-inclusive resort operating and brand-portfolio model across distinct Royalton family brands (family, adults-only, entertainment, wellness, boutique).",
    "Service Models Supported": ["Resort", "Full-service"],
    "Brand Families Operated": ["Marriott", "Independent", "Soft brands / collections"],
    chainScalesSupported: ["Luxury", "Upper Upscale", "Upscale"],
    propertyTypes: ["Resort", "Full Service"],
    additionalExperience: ["Resort"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    brand_soft_independent_narrative:
      "Royalton family brands plus Planet Hollywood by Royalton; select resorts participate in Marriott Bonvoy per guest FAQ.",
    brand_narrative_relationship:
      "Owner/operator and brand-portfolio all-inclusive platform (formerly Blue Diamond). Confirm third-party management appetite separately.",
    brand_narrative_compliance:
      "Brand and loyalty program obligations (including Bonvoy where applicable) confirmed per property in diligence.",
    brand_portfolio_mix_json: j([
      {
        brandFlagType: "Royalton Luxury / Hideaway / CHIC / Vessence / Mystique",
        portfolioMix: "Owned/operated all-inclusive portfolio brands",
        assetContext: "Mexico & Caribbean beachfront (guest site destinations)",
        relationshipStatus: "Active (public portfolio)",
      },
    ]),
    brand_relationship_depth_json: j([
      {
        title: "Multi-brand all-inclusive platform",
        description: "Unified Royalton Hotels & Resorts corporate identity (2025) across former Blue Diamond portfolio brands.",
      },
    ]),
    brand_execution_capabilities_json: j([
      { title: "All-inclusive ops", description: "Dining, entertainment, family and adults-only products across destinations." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Brand standards", description: "Confirm property-level brand and loyalty obligations in diligence." },
    ]),
    overview_why_1_headline: "All-inclusive CALA depth",
    overview_why_1_story:
      "Multi-destination Caribbean and Mexico all-inclusive portfolio under a unified Royalton corporate identity.",
    overview_why_2_headline: "Distinct brand lanes",
    overview_why_2_story:
      "Family, adults-only, entertainment, wellness, and boutique products under one operating group.",
    overview_why_3_headline: "Barbados-based platform",
    overview_why_3_story: "Corporate/marketing HQ published in St. Michael, Barbados (Cidel Place).",
    overview_bestat_1_headline: "Best at beachfront all-inclusive",
    overview_bestat_1_story: "Prime beach destinations across Mexico and the Caribbean per royaltonresorts.com.",
    overview_bestat_2_headline: "Best at multi-brand AI product",
    overview_bestat_2_story: "Royalton, Hideaway, CHIC, Planet Hollywood, Vessence, Mystique, Grand Lido lanes.",
  }),

  "driftwood-hospitality-management": Object.freeze({
    company_name: "Driftwood Hospitality Management",
    website: "https://www.driftwoodhospitality.com/",
    companyTagline: "U.S. full-service third-party hotel management — brands, lifestyle, and F&B.",
    primaryServiceModel: "Full-service focus",
    headquarters: "North Palm Beach, United States",
    companySize: "Large (50+ properties)",
    figuresAsOf: FIGURES,
    yearEstablished: 1999,
    yearsInBusiness: 27,
    totalProperties: "80+ hotels (driftwoodhospitality.com — confirm current)",
    totalRooms: "15,000+ rooms / keys (driftwoodhospitality.com — confirm current)",
    companyDescription: WAVE_D_WEBSITE_CONTENT_PACKS["driftwood-hospitality-management"].profile.companyDescription,
    companyHistory: WAVE_D_WEBSITE_CONTENT_PACKS["driftwood-hospitality-management"].profile.companyHistory,
    missionStatement: WAVE_D_WEBSITE_CONTENT_PACKS["driftwood-hospitality-management"].profile.missionStatement,
    differentiators: WAVE_D_WEBSITE_CONTENT_PACKS["driftwood-hospitality-management"].profile.differentiators,
    managementPhilosophy:
      "Hands-on third-party management with data analytics, revenue focus, and owner partnership culture (DHM site).",
    "Service Models Supported": ["Full-service", "Select-service", "Lifestyle / boutique"],
    "Brand Families Operated": ["Marriott", "Hilton", "IHG", "Hyatt", "Wyndham", "Choice", "Independent"],
    chainScalesSupported: ["Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    propertyTypes: ["Full Service", "Select Service", "Lifestyle"],
    additionalExperience: ["Full Service"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    brand_soft_independent_narrative:
      "Major flags plus Margaritaville and lifestyle/independent hotels cited on official site.",
    brand_narrative_relationship: "Third-party management across major U.S. brand families.",
    brand_narrative_compliance: "Brand QA by flag — confirm property-level obligations in diligence.",
    brand_portfolio_mix_json: j([
      {
        brandFlagType: "Major flags (Hilton/Marriott/IHG/Hyatt/etc.)",
        portfolioMix: "Third-party managed",
        assetContext: "U.S. full-service and select-service",
        relationshipStatus: "Active (public positioning)",
      },
    ]),
    brand_relationship_depth_json: j([
      { title: "Multi-brand third-party", description: "~24 brands cited on driftwoodhospitality.com." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Full-service ops + F&B", description: "Branded and independent F&B outlets cited on site." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Brand standards", description: "Confirm per-flag compliance in diligence." },
    ]),
    overview_why_1_headline: "Full-service third-party scale",
    overview_why_1_story: "Top-20 U.S. management positioning with 80+ hotels / 15K+ keys cited.",
    overview_why_2_headline: "Brand breadth",
    overview_why_2_story: "Hilton, Marriott, Hyatt, IHG, Wyndham, Choice and more cited.",
    overview_why_3_headline: "Owner partnership model",
    overview_why_3_story: "Long-term owner relationships and turnaround / new-build situations highlighted.",
    overview_bestat_1_headline: "Best at U.S. full-service management",
    overview_bestat_1_story: "Full-service core with select-service and lifestyle capability.",
    overview_bestat_2_headline: "Best at multi-brand execution",
    overview_bestat_2_story: "24 brands / 40 ownership groups cited on official site.",
  }),

  "remington-hospitality": Object.freeze({
    company_name: "Remington Hospitality",
    website: "https://www.remingtonhospitality.com/",
    companyTagline: "U.S. hotel management with a growing Caribbean & Latin America platform.",
    primaryServiceModel: "Mixed",
    headquarters: "Dallas, United States",
    companySize: "Large (50+ properties)",
    figuresAsOf: FIGURES,
    yearEstablished: 1968,
    yearsInBusiness: 58,
    totalProperties: "120+ hotels enterprise; ~18 CALA existing+development cited (label separately)",
    totalRooms: "Enterprise RUM ~$1.2B cited; CALA ~2,500 rooms cited — confirm current",
    companyDescription: WAVE_D_WEBSITE_CONTENT_PACKS["remington-hospitality"].profile.companyDescription,
    companyHistory: WAVE_D_WEBSITE_CONTENT_PACKS["remington-hospitality"].profile.companyHistory,
    missionStatement: WAVE_D_WEBSITE_CONTENT_PACKS["remington-hospitality"].profile.missionStatement,
    differentiators: WAVE_D_WEBSITE_CONTENT_PACKS["remington-hospitality"].profile.differentiators,
    managementPhilosophy:
      "Owner-mindset operations with commercial strategy and expanding CALA in-market leadership (Miami regional HQ).",
    "Service Models Supported": ["Full-service", "Select-service", "Lifestyle / boutique"],
    "Brand Families Operated": ["Marriott", "Hilton", "IHG", "Hyatt", "Independent", "Soft brands / collections"],
    chainScalesSupported: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale"],
    propertyTypes: ["Full Service", "Select Service", "Resort"],
    additionalExperience: ["Full Service"],
    brandedVsIndependentMix: NM,
    brand_signal_audit: NM,
    brand_signal_reflag: "Moderate",
    brand_signal_franchise_align: "High",
    brand_signal_soft_retention: NM,
    brand_conversion_project_count: NM,
    brand_soft_independent_narrative:
      "26 brands + independent/boutique cited enterprise-wide; CALA examples include Kimpton, Hilton Garden Inn, Autograph/LXR pipeline items on CALA page.",
    brand_narrative_relationship: "Third-party management with CALA regional platform since 2022–2023.",
    brand_narrative_compliance: "Brand QA by flag — confirm property-level obligations in diligence.",
    brand_portfolio_mix_json: j([
      {
        brandFlagType: "Enterprise multi-brand (U.S.)",
        portfolioMix: "Third-party managed",
        assetContext: "120+ hotels / 26 brands cited",
        relationshipStatus: "Active (corporate site)",
      },
      {
        brandFlagType: "CALA portfolio",
        portfolioMix: "Existing + development",
        assetContext: "~18 properties / ~2,500 rooms across 8 markets cited",
        relationshipStatus: "Active growth (CALA page)",
      },
    ]),
    brand_relationship_depth_json: j([
      { title: "CALA in-market team", description: "Ops, F&B, revenue, systems leadership listed on /cala." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "U.S. systems + CALA delivery", description: "Dallas corporate infrastructure with Miami CALA HQ." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Brand standards", description: "Confirm per-flag compliance in diligence." },
    ]),
    overview_why_1_headline: "CALA expansion with U.S. systems",
    overview_why_1_story: "Miami regional HQ and ~18 CALA existing/development assets cited.",
    overview_why_2_headline: "Deep U.S. operating track record",
    overview_why_2_story: "Founded 1968; 120+ hotels / 26 brands cited enterprise-wide.",
    overview_why_3_headline: "Owner-mindset commercial engine",
    overview_why_3_story: "Performance and margin focus positioned for owners and asset managers.",
    overview_bestat_1_headline: "Best at branded third-party ops",
    overview_bestat_1_story: "Multi-brand U.S. platform with independent/boutique experience.",
    overview_bestat_2_headline: "Best at CALA growth platform",
    overview_bestat_2_story: "Costa Rica, DR, Mexico, Puerto Rico, Cayman, Belize, Panama, Peru cited.",
  }),
});

export const WAVE_D_COMPANY_LOGOS = Object.freeze({
  "royalton-hotels-resorts": Object.freeze({
    url: "https://icon.horse/icon/royaltonresorts.com?size=256",
    filename: "royalton-hotels-resorts-logo-square.png",
    note: "Domain icon proxy for royaltonresorts.com until curated square mark is harvested.",
    preferredSquare: true,
  }),
  "driftwood-hospitality-management": Object.freeze({
    url: "https://icon.horse/icon/driftwoodhospitality.com?size=256",
    filename: "driftwood-hospitality-management-logo-square.png",
    note: "Domain icon proxy for driftwoodhospitality.com.",
    preferredSquare: true,
  }),
  "remington-hospitality": Object.freeze({
    url: "https://icon.horse/icon/remingtonhospitality.com?size=256",
    filename: "remington-hospitality-logo-square.png",
    note: "Domain icon proxy for remingtonhospitality.com.",
    preferredSquare: true,
  }),
});

export const WAVE_D_SLUGS = Object.freeze(Object.keys(WAVE_D_WEBSITE_CONTENT_PACKS));
