/**
 * Playa Hotels & Resorts — Wave B Operator Setup content (owner/operator all-inclusive).
 * Sources: Playa investor Q1 2025 release; Hospitality Net org profile; Hyatt acquisition release (context).
 * Not a brand-managed parent company.
 */
const FIGURES = "Q1 2025 (Playa investor release as of Mar 31, 2025)";
const NM = "Not Measured / N/A";

function j(arr) {
  return JSON.stringify(arr);
}

export const PLAYA_SOURCE_URLS = Object.freeze([
  Object.freeze({
    title: "Playa Q1 2025 results — portfolio description",
    url: "https://investors.playaresorts.com/2025-05-05-Playa-Hotels-Resorts-N-V-Reports-First-Quarter-2025-Results",
    localCapturePath:
      "Playa Hotels & Resorts/website/Playa Q1 2025 results — portfolio description.html",
  }),
  Object.freeze({
    title: "Hospitality Net — Playa Hotels & Resorts profile",
    url: "https://www.hospitalitynet.org/organization/17016574/playa-hotels",
    localCapturePath: "Playa Hotels & Resorts/press/Hospitality Net — Playa Hotels & Resorts profile.html",
  }),
  Object.freeze({
    title: "Hyatt — acquisition of Playa Hotels & Resorts N.V.",
    url: "https://investors.hyatt.com/news/investor-news/news-details/2025/Hyatt-Strengthens-Leadership-in-All-Inclusive-Segment-with-Acquisition-of-Playa-Hotels--Resorts-N-V-/default.aspx",
    localCapturePath:
      "Playa Hotels & Resorts/press/Hyatt — acquisition of Playa Hotels & Resorts N.V..html",
  }),
]);

/** @type {import('./operator-setup-website-content-packs.js').SetupContentPack} */
export const PLAYA_WEBSITE_CONTENT_PACK = Object.freeze({
  sources: PLAYA_SOURCE_URLS,
  profile: {
    website: "https://www.playaresorts.com/",
    primaryServiceModel: "Full-service focus",
    companyDescription:
      "Playa Hotels & Resorts is a leading owner, operator, and developer of all-inclusive beachfront resorts in Mexico, Jamaica, and the Dominican Republic. Per Playa’s Q1 2025 investor release (as of March 31, 2025), Playa owned and/or managed a total portfolio of 22 resorts (8,342 rooms) and also managed seven resorts on behalf of third-party owners. Brands cited in Playa materials include Hyatt Zilara, Hyatt Ziva, Hilton All-Inclusive, Wyndham Alltra, Seadust, Kimpton, Jewel Resorts, and The Luxury Collection. Hyatt publicly announced acquisition of Playa Hotels & Resorts N.V. (2025) — underwrite current ownership/management structure in diligence; this Explorer profile preserves Playa’s published all-inclusive owner/operator lens.",
    companyHistory:
      "Playa positions itself (Hospitality Net / investor materials) as a multi-year all-inclusive resort owner, operator, and developer focused on prime beachfront locations in Mexico and the Caribbean. Public materials emphasize leveraging all-inclusive operating expertise with globally recognized hospitality brand relationships.",
    missionStatement:
      "Provide a best-in-class all-inclusive guest experience and exceptional value while building direct guest relationships to improve customer acquisition cost and drive repeat business (Playa public positioning).",
    differentiators:
      "CALA all-inclusive owner/operator focus (Mexico, Jamaica, Dominican Republic)\nOwns and/or manages beachfront resort portfolio; also third-party management (7 resorts cited Q1 2025)\nBrand relationships: Hyatt Ziva/Zilara, Hilton All-Inclusive, Wyndham Alltra, and others cited in Playa materials\nHyatt acquisition (2025) is public context — confirm post-close operating model in diligence",
  },
  platformMarkets: {
    specificMarkets:
      "Mexico (e.g. Cancún, Playa del Carmen, Puerto Vallarta, Los Cabos), Jamaica (Rose Hall / Montego Bay), Dominican Republic (La Romana, Cap Cana) — per Playa Q1 2025 named portfolio list.",
  },
  commercial: {
    bf_operating_situations:
      "Owners of all-inclusive beachfront resorts in Mexico and the Caribbean seeking an operator with Playa’s all-inclusive depth and brand relationships (Hyatt/Hilton/Wyndham paths cited in Playa materials). Also: assets where Playa third-party management is relevant (Playa cited managing seven resorts for third-party owners as of Q1 2025).",
    bf_not_ideal_for:
      "Owners needing urban select-service or non-all-inclusive focused platforms; assets outside Mexico/Caribbean leisure beach corridors without an all-inclusive mandate; pure brand-managed paths with no Playa operating role",
  },
  governance: {
    risk_programs_narrative:
      "Brand-standard QA applies by flag (Hyatt, Hilton, Wyndham, etc.). Confirm ownership structure and management agreements after Hyatt’s public acquisition of Playa (2025). Sources: Playa Q1 2025 investor release; Hyatt acquisition release.",
  },
});

export const PLAYA_PROFILE_DEEP_PACK = Object.freeze({
  company_name: "Playa Hotels & Resorts",
  website: "https://www.playaresorts.com/",
  companyTagline: "All-inclusive owner, operator, and developer — Mexico & Caribbean beachfront.",
  primaryServiceModel: "Full-service focus",
  headquarters: "Fairfax, United States",
  companySize: "Medium (11-50 properties)",
  figuresAsOf: FIGURES,
  totalProperties: "22 resorts owned and/or managed (8,342 rooms) as of Mar 31, 2025 (Playa Q1 2025); plus 7 third-party managed resorts cited",
  totalRooms: "8,342 rooms (owned and/or managed portfolio as of Mar 31, 2025 — Playa Q1 2025)",
  companyDescription: PLAYA_WEBSITE_CONTENT_PACK.profile.companyDescription,
  companyHistory: PLAYA_WEBSITE_CONTENT_PACK.profile.companyHistory,
  missionStatement: PLAYA_WEBSITE_CONTENT_PACK.profile.missionStatement,
  differentiators: PLAYA_WEBSITE_CONTENT_PACK.profile.differentiators,
  managementPhilosophy:
      "All-inclusive resort operating expertise paired with global brand relationships; owner/operator and third-party management capability in Mexico and the Caribbean (Playa investor materials).",
  "Service Models Supported": ["Resort", "Full-service"],
  "Brand Families Operated": ["Hyatt", "Hilton", "Wyndham", "IHG", "Independent", "Soft brands / collections"],
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
    "Playa materials cite brands including Hyatt Ziva/Zilara, Hilton All-Inclusive, Wyndham Alltra, Seadust, Kimpton, Jewel, and The Luxury Collection — brand-system affiliations within an all-inclusive owner/operator platform.",
  brand_narrative_relationship:
    "Owner/operator and third-party management of branded all-inclusive resorts; brand families per Playa public portfolio lists. Hyatt acquisition (2025) may change ownership context — confirm current agreements in diligence.",
  brand_narrative_compliance:
    "Brand QA by flag under Hyatt/Hilton/Wyndham (and other cited brands) — confirm property-level obligations in diligence.",
  brand_portfolio_mix_json: j([
    {
      brandFlagType: "Hyatt Ziva / Hyatt Zilara (cited)",
      portfolioMix: "Owned and/or managed all-inclusive",
      assetContext: "Mexico, Jamaica, Dominican Republic beachfront (Q1 2025 named assets)",
      relationshipStatus: "Active (public Playa portfolio as of Q1 2025)",
    },
    {
      brandFlagType: "Hilton All-Inclusive (cited)",
      portfolioMix: "Owned and/or managed all-inclusive",
      assetContext: "Mexico, Jamaica, Dominican Republic (Q1 2025 named assets)",
      relationshipStatus: "Active (public Playa portfolio as of Q1 2025)",
    },
  ]),
  brand_relationship_depth_json: j([
    {
      title: "All-inclusive brand partners",
      description: "Hyatt Ziva/Zilara, Hilton All-Inclusive, Wyndham Alltra and others cited in Playa materials.",
    },
    {
      title: "Third-party management",
      description: "Playa cited managing seven resorts on behalf of third-party owners (Q1 2025).",
    },
  ]),
  brand_execution_capabilities_json: j([
    {
      title: "All-inclusive operations",
      description: "Owner/operator depth in beachfront all-inclusive resorts in Mexico and the Caribbean.",
    },
    {
      title: "Brand-standard execution",
      description: "Operate under major brand systems cited in Playa portfolio lists.",
    },
    {
      title: "Development / ownership",
      description: "Owner, operator, and developer positioning in prime beachfront locations (Playa public materials).",
    },
  ]),
  brand_governance_compliance_json: j([
    {
      title: "Brand QA",
      description: "Brand audit and standards by affiliation.",
    },
    {
      title: "Ownership transition diligence",
      description: "Hyatt acquisition of Playa (2025) — confirm current ownership and management agreements in diligence.",
    },
  ]),
  overview_why_1_headline: "CALA All-Inclusive Owner/Operator",
  overview_why_1_story:
    "Focused beachfront all-inclusive platform in Mexico, Jamaica, and the Dominican Republic (Playa Q1 2025).",
  overview_why_2_headline: "Brand-Partnered Portfolio",
  overview_why_2_story:
    "Public portfolio cites Hyatt Ziva/Zilara, Hilton All-Inclusive, Wyndham Alltra, and additional brands.",
  overview_why_3_headline: "Third-Party Management Capability",
  overview_why_3_story:
    "As of Q1 2025 Playa also managed seven resorts for third-party owners — underwrite current scope post Hyatt acquisition.",
  overview_bestat_1_headline: "All-Inclusive Beach Resorts",
  overview_bestat_1_story: "Mexico and Caribbean leisure destinations with all-inclusive operating model.",
  overview_bestat_2_headline: "Named Market Footprint",
  overview_bestat_2_story: "Cancún, Playa del Carmen, Los Cabos, Puerto Vallarta, Rose Hall, Cap Cana, La Romana (Q1 2025).",
  overview_bestat_3_headline: "Owner + Manager",
  overview_bestat_3_story: "Owns and/or manages portfolio assets; third-party management cited.",
  overview_signal_1_value: "22 resorts · 8,342 rooms owned and/or managed (Mar 31, 2025 — Playa Q1 2025)",
  overview_signal_2_value: "Mexico · Jamaica · Dominican Republic all-inclusive beachfront",
  overview_signal_3_value: "Hyatt acquisition (2025) — confirm current structure in diligence",
  industryRecognition: NM,
  notableAchievements:
    "Public Q1 2025 portfolio scale (22 resorts / 8,342 rooms owned and/or managed; 7 third-party managed). Hyatt announced acquisition of Playa Hotels & Resorts N.V. (2025).",
  support24x7: "Yes - Full 24/7",
  businessContinuity: "Yes",
  emergencyResponse: "Yes - Standard",
  sustainabilityPrograms: "Yes - Standard",
  esgReporting: "Yes - Standard",
});

export const PLAYA_COMPANY_LOGO = Object.freeze({
  url: "https://icon.horse/icon/playaresorts.com?size=256",
  filename: "playa-hotels-resorts-logo-square.png",
  note: "Domain icon proxy for playaresorts.com (Google favicon too small).",
  preferredSquare: true,
});
