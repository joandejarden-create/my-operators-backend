/**
 * Marriott International (Managed) — source pack from local Brand Reference Material.
 * Binding primary: Managed by Marriott (MxM) development capture.
 *
 * Local: Marriott International/development/Managed by Marriott (MxM).html
 * URL: https://www.hotel-development.marriott.com/how-we-work-together/managed-by-marriott
 *
 * Enterprise MxM scale (~2,100 hotels) must never be labeled as CALA managed footprint.
 */
import { readLocalSourceText } from "./extract-source-text.js";

export const MXM_SOURCE_PACK_VERSION = "marriott-mxm-source-pack-v1";

export const MXM_PRIMARY = Object.freeze({
  title: "Managed by Marriott (MxM)",
  url: "https://www.hotel-development.marriott.com/how-we-work-together/managed-by-marriott",
  localCapturePath: "Marriott International/development/Managed by Marriott (MxM).html",
});

const FIGURES = "July 2026 (MxM local capture — Brand Reference Material)";
const NM = "Not Measured / N/A";
const CALA_FOOTPRINT =
  "CALA managed / operated subset is Not Measured on the MxM development page — confirm in diligence / census. Do not infer CALA managed counts from enterprise MxM (~2,100 hotels) totals.";

function j(arr) {
  return JSON.stringify(arr);
}

/**
 * Verify local MxM capture is readable and contains expected claim anchors.
 * @returns {{ ok: boolean, absolutePath?: string, missingAnchors: string[], error?: string }}
 */
export function verifyMarriottMxmLocalCapture() {
  const anchors = [
    "maximize your financial performance",
    "2,100 hotels",
    "turnkey management",
    "65 years",
    "50% better",
    "90% of our GMs",
  ];
  try {
    const doc = readLocalSourceText(MXM_PRIMARY.localCapturePath);
    const text = String(doc.text || "").toLowerCase();
    const missingAnchors = anchors.filter((a) => !text.includes(a.toLowerCase()));
    return {
      ok: missingAnchors.length === 0,
      absolutePath: doc.path,
      missingAnchors,
      textLength: text.length,
    };
  } catch (err) {
    return {
      ok: false,
      missingAnchors: anchors,
      error: err?.message || String(err),
    };
  }
}

/** Website / 1:1 Setup content derived from MxM capture. */
export const MARRIOTT_MANAGED_WEBSITE_FROM_MXM = Object.freeze({
  sources: [
    {
      title: MXM_PRIMARY.title,
      url: MXM_PRIMARY.url,
      localCapturePath: MXM_PRIMARY.localCapturePath,
    },
  ],
  profile: {
    website: "https://www.hotel-development.marriott.com/how-we-work-together/managed-by-marriott",
    primaryServiceModel: "Mixed",
    companyDescription:
      "Marriott International (Managed) is the Managed by Marriott (MxM) brand-managed operating lens — not a third-party independent manager. Per Marriott Hotel Development’s Managed by Marriott page, MxM’s stated goal is to maximize owner financial performance; MxM manages over 2,100 hotels in the enterprise MxM portfolio across luxury, premium, select, and all-inclusive brands globally. Partnership and adding value to the owner’s investment are positioned at the heart of MxM. Enterprise MxM scale is labeled separately from any CALA managed footprint (CALA managed counts are Not Measured on this page — confirm in diligence / census).",
    companyHistory:
      "Managed by Marriott materials cite nearly 100 years of hospitality experience and more than 65 years of strategic operational expertise behind turnkey management services. MxM is positioned as one of the largest operators of hotels globally, with brand-system depth across Marriott International brands. This Explorer profile is the managed-operating lens — not Brand Explorer brand tabs.",
    missionStatement:
      "One simple goal…to maximize your financial performance (Managed by Marriott / hotel-development.marriott.com).",
    differentiators:
      "Turnkey MxM management with stated 65+ years operational expertise (MxM page)\nEnterprise MxM portfolio: over 2,100 hotels managed (enterprise label — not CALA)\nTopline organization: Sales + Revenue Management + Marketing synergy; RevPAR premium positioning\nBottom-line scale efficiencies and Marriott brand/systems knowledge (“who better to run Marriott brands…”)\nPeople: associate retention ≈50% better than industry average (as stated); ~90% of GMs grew careers at MxM hotels\nMust label enterprise MxM vs CALA managed footprint — no invented CALA managed counts",
  },
  platformMarkets: {
    specificMarkets:
      "Global enterprise MxM footprint (over 2,100 hotels per Managed by Marriott page) spanning luxury, premium, select, and all-inclusive brands. " +
      CALA_FOOTPRINT,
  },
  commercial: {
    bf_operating_situations:
      "Owners seeking a Managed by Marriott / brand-managed path who want turnkey management, Marriott brand and systems expertise, and MxM topline (sales, revenue management, marketing) plus bottom-line scale support. Best-fit: assets where a Marriott management agreement / MxM operating model is the intended structure (per hotel-development.marriott.com Managed by Marriott).",
    bf_not_ideal_for:
      "Owners needing a pure third-party operator independent of Marriott brand affiliation; assets outside MxM managed coverage without a clear brand-managed mandate",
  },
  governance: {
    risk_programs_narrative:
      "MxM operates under Marriott International brand standards and management-agreement obligations — confirm property-level certifications, PIP, and audit cadence in diligence. Source: Managed by Marriott (MxM) development page (local capture).",
  },
});

/** Deep Profile (~50 fields) from MxM capture — observed selects only. */
export const MARRIOTT_MANAGED_PROFILE_DEEP_FROM_MXM = Object.freeze({
  company_name: "Marriott International (Managed)",
  website: "https://www.hotel-development.marriott.com/how-we-work-together/managed-by-marriott",
  companyTagline: "Managed by Marriott (MxM) — maximize owner financial performance.",
  primaryServiceModel: "Mixed",
  headquarters: "Bethesda, United States",
  companySize: "Large (50+ properties)",
  figuresAsOf: FIGURES,
  totalProperties: "Over 2,100 hotels in the MxM portfolio (enterprise — Managed by Marriott page); CALA managed subset Not Measured",
  companyDescription: MARRIOTT_MANAGED_WEBSITE_FROM_MXM.profile.companyDescription,
  companyHistory: MARRIOTT_MANAGED_WEBSITE_FROM_MXM.profile.companyHistory,
  missionStatement: MARRIOTT_MANAGED_WEBSITE_FROM_MXM.profile.missionStatement,
  differentiators: MARRIOTT_MANAGED_WEBSITE_FROM_MXM.profile.differentiators,
  managementPhilosophy:
    "Turnkey brand-managed operations under Managed by Marriott: partnership with owners, topline Sales/RM/Marketing synergy, bottom-line scale efficiencies, and Putting People First culture — not a pure third-party independent model. Source: MxM development page.",
  "Service Models Supported": ["Full-service", "Select-service", "Resort", "Lifestyle", "Focused-service"],
  "Brand Families Operated": ["Marriott", "Soft brands / collections"],
  chainScalesSupported: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
  propertyTypes: ["Full Service", "Select Service", "Resort", "Lifestyle", "Boutique"],
  additionalExperience: ["Urban", "Resort", "Airport", "Suburban"],
  brandedVsIndependentMix: NM,
  brand_signal_audit: NM,
  brand_signal_reflag: "Moderate",
  brand_signal_franchise_align: "High",
  brand_signal_soft_retention: NM,
  brand_conversion_project_count: NM,
  brand_soft_independent_narrative:
    "MxM states expertise across luxury, premium, select, and all-inclusive Marriott brands. Soft brands / collections remain inside Marriott ecosystems — this profile is brand-managed (MxM), not independent third-party.",
  brand_narrative_relationship:
    "Managed by Marriott / brand-operator relationships under Marriott International. Franchise-only paths are adjacent but distinct — this Explorer lens is MxM managed operating capability (hotel-development.marriott.com).",
  brand_narrative_compliance:
    "Marriott brand standards, QA, and management-agreement obligations under MxM — confirm PIP and audit cadence in diligence.",
  brand_portfolio_mix_json: j([
    {
      brandFlagType: "MxM — Marriott International brands",
      portfolioMix: "Enterprise brand-managed (Managed by Marriott)",
      assetContext:
        "Luxury, premium, select, all-inclusive per MxM page — over 2,100 hotels enterprise; CALA subset Not Measured",
      relationshipStatus: "Active (public MxM)",
    },
  ]),
  brand_relationship_depth_json: j([
    {
      title: "Managed by Marriott (MxM)",
      description:
        "Turnkey management services; stated goal to maximize owner financial performance (hotel-development.marriott.com).",
    },
    {
      title: "Marriott brand & systems expertise",
      description:
        "MxM positions itself as best positioned to operate Marriott International brands and systems.",
    },
  ]),
  brand_execution_capabilities_json: j([
    {
      title: "Turnkey management",
      description: "MxM provides turnkey management services with 65+ years strategic operational expertise framing (MxM page).",
    },
    {
      title: "Topline organization",
      description:
        "Sales, Revenue Management, and Marketing synergy; industry-leading RM tools; customize approach to owner needs.",
    },
    {
      title: "Bottom-line scale",
      description:
        "Scale efficiencies, negotiating power, ramp new hotels into MxM system for revenue, occupancy, and cost savings.",
    },
  ]),
  brand_governance_compliance_json: j([
    {
      title: "Brand QA / management agreement",
      description: "Marriott brand standards and MxM management-agreement controls — confirm in diligence.",
    },
    {
      title: "Talent & retention",
      description:
        "MxM cites associate retention ≈50% better than industry average and ~90% of GMs grew careers at MxM hotels (MxM page).",
    },
  ]),
  overview_why_1_headline: "Maximize Owner Financial Performance",
  overview_why_1_story:
    "MxM’s published goal is to maximize owner financial performance through partnership and investment value (Managed by Marriott page).",
  overview_why_2_headline: "Enterprise MxM Scale + Brand Systems",
  overview_why_2_story:
    "Over 2,100 hotels in the MxM portfolio (enterprise label) with turnkey management and Marriott brand/systems depth — not a third-party independent model. CALA managed subset Not Measured on this page.",
  overview_why_3_headline: "Topline + People Advantage",
  overview_why_3_story:
    "Sales/RM/Marketing synergy for RevPAR; Putting People First culture with stated retention and GM career-path claims on the MxM page.",
  overview_bestat_1_headline: "Brand-Managed / MxM Path",
  overview_bestat_1_story: "Owners evaluating Managed by Marriott management agreements and brand-operator structures.",
  overview_bestat_2_headline: "Topline Engine",
  overview_bestat_2_story:
    "MxM topline organization — Sales, Revenue Management, Marketing — customized to the hotel (per MxM page).",
  overview_bestat_3_headline: "Marriott Brand Operatorship",
  overview_bestat_3_story:
    "Positioned as the operator that knows Marriott International brands and systems best (MxM page).",
  overview_signal_1_value: "MxM · over 2,100 hotels managed (enterprise — hotel-development.marriott.com)",
  overview_signal_2_value: "Turnkey management · 65+ years operational expertise framing (MxM page)",
  overview_signal_3_value: "CALA managed footprint Not Measured on MxM page — confirm in diligence / census",
  industryRecognition: NM,
  notableAchievements:
    "MxM published case-study style outcomes on the Managed by Marriott page (e.g. customized group sales / transition examples with property-level KPI claims). Treat as MxM marketing case studies — underwrite independently; not CALA-specific unless confirmed.",
  support24x7: "Yes - Full 24/7",
  businessContinuity: "Yes",
  emergencyResponse: "Yes - Comprehensive",
  sustainabilityPrograms: "Yes - Standard",
  esgReporting: "Yes - Standard",
});

export function getMarriottManagedWebsitePack() {
  return MARRIOTT_MANAGED_WEBSITE_FROM_MXM;
}

export function getMarriottManagedProfileDeepPack() {
  return MARRIOTT_MANAGED_PROFILE_DEEP_FROM_MXM;
}
