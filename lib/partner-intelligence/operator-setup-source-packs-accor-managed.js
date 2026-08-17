/**
 * Accor (Managed) — source pack from Accor hotel-development + overview materials.
 * Prefer local Brand Reference Material/Accor captures after harvest.
 *
 * Primary URLs:
 * - https://group.accor.com/en/hotel-development
 * - https://group.accor.com/en/hotel-development/solutions-for-every-project
 * - Accor Overview 2026 PDF (local after harvest)
 */
import { readLocalSourceText } from "./extract-source-text.js";

export const ACCOR_MANAGED_SOURCE_PACK_VERSION = "accor-managed-source-pack-v1";

export const ACCOR_PRIMARY_SOURCES = Object.freeze([
  Object.freeze({
    title: "Develop with Accor",
    url: "https://group.accor.com/en/hotel-development",
    localCapturePath: "Accor/development/Develop with Accor.html",
  }),
  Object.freeze({
    title: "Accor solutions for every project — partnership models",
    url: "https://group.accor.com/en/hotel-development/solutions-for-every-project",
    localCapturePath: "Accor/development/Accor solutions for every project — partnership models.html",
  }),
  Object.freeze({
    title: "Accor Overview 2026",
    url: "https://assets.group.accor.com/yrj0orc8tx24/1gfnIc7BnVhT7xnnVrKMYj/5cea7a817eaa174b0193af9b8b592a58/Accor_Overview_2026.pdf",
    localCapturePath: "Accor/development/Accor Overview 2026.pdf",
  }),
]);

const FIGURES = "July 2026 (Accor hotel-development + Overview 2026)";
const NM = "Not Measured / N/A";
const CALA =
  "CALA / Americas managed subset is Not Measured on these Accor development pages — confirm in diligence / census. Do not treat enterprise hotel totals (e.g. 5,800+ hotels) as CALA managed counts.";

function j(arr) {
  return JSON.stringify(arr);
}

/**
 * @returns {{ ok: boolean, checked: Array<object>, error?: string }}
 */
export function verifyAccorManagedLocalCaptures() {
  const checked = [];
  let ok = true;
  for (const src of ACCOR_PRIMARY_SOURCES) {
    try {
      const doc = readLocalSourceText(src.localCapturePath);
      const text = String(doc.text || "").toLowerCase();
      const row = {
        title: src.title,
        path: src.localCapturePath,
        textLength: text.length,
        readable: text.length > 200,
      };
      if (!row.readable) ok = false;
      checked.push(row);
    } catch (err) {
      ok = false;
      checked.push({
        title: src.title,
        path: src.localCapturePath,
        error: err?.message || String(err),
      });
    }
  }
  return { ok, checked };
}

export const ACCOR_MANAGED_WEBSITE_FROM_SOURCES = Object.freeze({
  sources: ACCOR_PRIMARY_SOURCES.map((s) => ({
    title: s.title,
    url: s.url,
    localCapturePath: s.localCapturePath,
  })),
  profile: {
    website: "https://group.accor.com/en/hotel-development",
    primaryServiceModel: "Mixed",
    companyDescription:
      "Accor (Managed) is the brand-managed / management-contract operating lens of Accor — a world-leading hospitality group that positions itself as a leading hotel operator and franchisor with an owner-centric, asset-light model. Accor Hotel Development materials describe management and franchise partnership models, 45+ hotel brands, and 360-degree owner performance solutions. Accor Overview (Feb/Mar 2026) cites 5,800+ hotels and 380,000+ team members under Accor brand, with only ~3% of rooms owned or leased (enterprise context). This Operator Explorer profile is for owners evaluating Accor management / brand-operator paths — not a pure third-party independent manager. " +
      CALA,
    companyHistory:
      "Accor Group public materials describe more than 50 years of hospitality experience and a transformation to an asset-light model privileging service provision and hotel management positioning (Accor Overview 2026). Development pages emphasize partner-centric mindset and daily development signing cadence as a leading operator and franchisor.",
    missionStatement:
      "Unlock owner potential, optimize performance, and maximize return on investment through Accor brand portfolio, global platform, and management/franchise partnership models (group.accor.com hotel development).",
    differentiators:
      "Management model: full operational oversight (budgets, accounting, performance, personnel) to maximize owner profits (Solutions for Every Project)\n45+ hotel brands spanning segments; ALL Accor loyalty / booking platform\nAsset-light enterprise: ~3% rooms owned/leased; 5,800+ hotels (Overview 2026 — enterprise label, not CALA)\n360° owner solutions: sales, digital marketing, revenue management, procurement\nMust label enterprise vs CALA managed footprint — no invented CALA managed counts",
  },
  platformMarkets: {
    specificMarkets:
      "Global enterprise footprint (5,800+ hotels per Accor Overview 2026) with leadership positions cited outside the US and China across Europe, Middle East, South America, Africa, Southeast Asia, and the Pacific (Develop with Accor). " +
      CALA,
  },
  commercial: {
    bf_operating_situations:
      "Owners seeking an Accor management-contract / brand-managed path with Accor brand power, ALL loyalty, distribution, and full operational oversight. Best-fit: assets where Accor management (not franchise-only) is the intended operating model per group.accor.com Solutions for Every Project.",
    bf_not_ideal_for:
      "Owners needing a pure third-party operator independent of Accor brand affiliation; owners seeking franchise-only control without Accor management; assets outside Accor managed coverage without a brand-managed mandate",
  },
  governance: {
    risk_programs_narrative:
      "Accor management contracts place Accor as operator on behalf of the owner with brand standards and performance oversight — confirm management agreement terms, PIP, and audit cadence in diligence. Sources: Accor hotel-development partnership models; Accor Overview 2026.",
  },
});

export const ACCOR_MANAGED_PROFILE_DEEP_FROM_SOURCES = Object.freeze({
  company_name: "Accor (Managed)",
  website: "https://group.accor.com/en/hotel-development",
  companyTagline: "Accor management model — maximize owner ROI with brand + operational oversight.",
  primaryServiceModel: "Mixed",
  headquarters: "Issy-les-Moulineaux, France",
  companySize: "Large (50+ properties)",
  figuresAsOf: FIGURES,
  totalProperties:
    "5,800+ hotels (enterprise — Accor Overview 2026); CALA / Americas managed subset Not Measured",
  companyDescription: ACCOR_MANAGED_WEBSITE_FROM_SOURCES.profile.companyDescription,
  companyHistory: ACCOR_MANAGED_WEBSITE_FROM_SOURCES.profile.companyHistory,
  missionStatement: ACCOR_MANAGED_WEBSITE_FROM_SOURCES.profile.missionStatement,
  differentiators: ACCOR_MANAGED_WEBSITE_FROM_SOURCES.profile.differentiators,
  managementPhilosophy:
    "Owner-centric management and franchise partnership models. Under the management model Accor provides complete operational oversight — budgets, accounting, performance, personnel — aiming to maximize owner profits (Solutions for Every Project). Asset-light positioning privileges hotel management over ownership (Overview 2026).",
  "Service Models Supported": ["Full-service", "Select-service", "Resort", "Lifestyle", "Focused-service", "Boutique"],
  "Brand Families Operated": ["Accor", "Soft brands / collections"],
  chainScalesSupported: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale", "Midscale", "Economy"],
  propertyTypes: ["Full Service", "Select Service", "Resort", "Lifestyle", "Boutique"],
  additionalExperience: ["Urban", "Resort", "Airport"],
  brandedVsIndependentMix: NM,
  brand_signal_audit: NM,
  brand_signal_reflag: "Moderate",
  brand_signal_franchise_align: "High",
  brand_signal_soft_retention: NM,
  brand_conversion_project_count: NM,
  brand_soft_independent_narrative:
    "Accor offers management and franchise models. Soft/lifestyle brands remain Accor brand-system affiliations — this Explorer lens is Accor brand-managed / management-contract, not independent third-party.",
  brand_narrative_relationship:
    "Management contracts and franchise partnerships under Accor. This profile emphasizes the management model (full operational oversight) vs franchise-only paths.",
  brand_narrative_compliance:
    "Accor brand standards and management-agreement obligations — confirm PIP and audit cadence in diligence.",
  brand_portfolio_mix_json: j([
    {
      brandFlagType: "Accor 45+ brand portfolio / ALL Accor",
      portfolioMix: "Enterprise management + franchise (asset-light)",
      assetContext: "5,800+ hotels enterprise (Overview 2026); CALA managed subset Not Measured",
      relationshipStatus: "Active (public Accor development)",
    },
  ]),
  brand_relationship_depth_json: j([
    {
      title: "Management model",
      description:
        "Full operational expertise — brand, loyalty, sales, distribution, day-to-day management to maximize owner profits (Solutions for Every Project).",
    },
    {
      title: "Franchise model",
      description:
        "Owner retains operational control with Accor brand, distribution, sales, marketing, loyalty, procurement, and training (Solutions for Every Project).",
    },
  ]),
  brand_execution_capabilities_json: j([
    {
      title: "360° owner solutions",
      description: "Sales ecosystem, digital marketing, revenue management, procurement (Maximize Your Revenue / development pages).",
    },
    {
      title: "Brand portfolio breadth",
      description: "45+ hotel brands spanning segments for tailored project fit (Develop with Accor).",
    },
    {
      title: "Asset-light management positioning",
      description: "~3% rooms owned or leased; focus on management and franchise (Overview 2026).",
    },
  ]),
  brand_governance_compliance_json: j([
    {
      title: "Management agreement controls",
      description: "Operational oversight of budgets, accounting, performance, personnel under management model.",
    },
    {
      title: "Brand standards",
      description: "Accor brand integrity and guest experience controls — confirm agreement terms in diligence.",
    },
  ]),
  overview_why_1_headline: "Management Model for Owners",
  overview_why_1_story:
    "Accor’s published management model delivers full operational oversight aimed at maximizing owner profits — distinct from franchise-only paths (Solutions for Every Project).",
  overview_why_2_headline: "Asset-Light Global Platform",
  overview_why_2_story:
    "Overview 2026 cites 5,800+ hotels and ~3% owned/leased rooms — enterprise scale labeled separately from CALA managed footprint.",
  overview_why_3_headline: "Owner-Centric Development",
  overview_why_3_story:
    "Hotel Development materials emphasize partner-centric mindset, tailored brand selection, and 360° performance solutions.",
  overview_bestat_1_headline: "Brand-Managed / Management Contract Path",
  overview_bestat_1_story: "Owners evaluating Accor management agreements and brand-operator structures.",
  overview_bestat_2_headline: "ALL Accor + Distribution",
  overview_bestat_2_story: "Loyalty and booking platform as commercial and guest distribution backbone.",
  overview_bestat_3_headline: "Multi-Brand Fit",
  overview_bestat_3_story: "45+ brands to match project segment and market (Develop with Accor).",
  overview_signal_1_value: "Accor management model · group.accor.com hotel development",
  overview_signal_2_value: "5,800+ hotels enterprise · ~3% owned/leased (Overview 2026)",
  overview_signal_3_value: "CALA managed footprint Not Measured — confirm in diligence / census",
  industryRecognition: NM,
  notableAchievements:
    "Public Accor Overview 2026 scale markers (5,800+ hotels; 380,000+ team members under Accor brand). Treat as enterprise context — not CALA-specific managed counts.",
  support24x7: "Yes - Full 24/7",
  businessContinuity: "Yes",
  emergencyResponse: "Yes - Comprehensive",
  sustainabilityPrograms: "Yes - Standard",
  esgReporting: "Yes - Standard",
});
