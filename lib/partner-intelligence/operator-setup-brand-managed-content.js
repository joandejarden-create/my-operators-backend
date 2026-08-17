/**
 * Brand-managed Core 5 — website Setup packs + deep Profile packs.
 * Merged into website-content and profile-deepen pipelines via queue slugs.
 * Marriott: Managed by Marriott (MxM) local Brand Reference Material capture (binding).
 * Others: official corporate sites (2026-07-24). Always label enterprise vs CALA.
 */
import {
  MARRIOTT_MANAGED_WEBSITE_FROM_MXM,
  MARRIOTT_MANAGED_PROFILE_DEEP_FROM_MXM,
} from "./operator-setup-source-packs-marriott-managed.js";
import {
  ACCOR_MANAGED_WEBSITE_FROM_SOURCES,
  ACCOR_MANAGED_PROFILE_DEEP_FROM_SOURCES,
} from "./operator-setup-source-packs-accor-managed.js";

const FIGURES = "July 2026 (brand-managed Core 5 — website-sourced)";
const NM = "Not Measured / N/A";

function j(arr) {
  return JSON.stringify(arr);
}

/** @type {Record<string, object>} */
export const BRAND_MANAGED_WEBSITE_CONTENT_PACKS = Object.freeze({
  "marriott-international-managed": MARRIOTT_MANAGED_WEBSITE_FROM_MXM,

  "ihg-managed": Object.freeze({
    sources: [
      { title: "IHG Hotels & Resorts", url: "https://www.ihg.com/" },
      { title: "IHG PLC", url: "https://www.ihgplc.com/" },
    ],
    profile: {
      website: "https://www.ihg.com/",
      primaryServiceModel: "Mixed",
      companyDescription:
        "IHG Hotels & Resorts (Managed) is the brand-managed operating lens of IHG — a global hospitality company whose public materials describe brands across luxury, upscale, and midscale (IHG One Rewards). This Operator Explorer profile is for owners evaluating IHG brand-managed / brand-operator paths, not a third-party independent manager. Label enterprise scale vs CALA managed footprint separately.",
      companyHistory:
        "IHG is a global hospitality company with a multi-brand portfolio and owner/franchise/management pathways. Public corporate materials (ihg.com / ihgplc.com) emphasize brand system strength and loyalty. This profile is the managed-operating lens.",
      missionStatement:
        "Deliver great hotel experiences for guests and attractive returns for owners through IHG brand systems and managed operating capability (ihg.com — brand-managed lens).",
      differentiators:
        "Brand-managed path inside IHG / IHG One Rewards\nMulti-brand portfolio from luxury to midscale\nGlobal distribution and loyalty\nEnterprise vs CALA managed footprint must be labeled — no invented CALA managed counts",
    },
    platformMarkets: {
      specificMarkets:
        "Global enterprise footprint (ihg.com / ihgplc.com). CALA managed subset requires diligence confirmation — not inferred from enterprise totals.",
    },
    commercial: {
      bf_operating_situations:
        "Owners seeking an IHG brand-managed or brand-operator path with IHG One Rewards distribution and brand standards.",
      bf_not_ideal_for:
        "Owners needing a pure third-party operator independent of IHG brand affiliation; assets outside IHG managed coverage without a brand-managed mandate",
    },
    governance: {
      risk_programs_narrative:
        "IHG brand standards and management-agreement QA apply under brand-managed structures — confirm property-level obligations in diligence.",
    },
  }),

  "hilton-managed": Object.freeze({
    sources: [{ title: "Hilton", url: "https://www.hilton.com/" }],
    profile: {
      website: "https://www.hilton.com/",
      primaryServiceModel: "Mixed",
      companyDescription:
        "Hilton (Managed) is the brand-managed operating lens of Hilton — a global hospitality company whose public materials describe a multi-brand portfolio under Hilton Honors. This Operator Explorer profile is for owners evaluating Hilton brand-managed / brand-operator paths, not a third-party independent manager. Label enterprise scale vs CALA managed footprint separately.",
      companyHistory:
        "Hilton is a multi-decade global hospitality company with brand, franchise, and management pathways. Public materials emphasize Hilton Honors and brand portfolio breadth. This profile is the managed-operating lens.",
      missionStatement:
        "Fill the earth with the light and warmth of hospitality through Hilton brand systems and managed operating capability (hilton.com — brand-managed lens).",
      differentiators:
        "Brand-managed path inside Hilton / Hilton Honors\nMulti-brand portfolio across luxury, full-service, and focused-service\nGlobal distribution and loyalty\nEnterprise vs CALA managed footprint labeled — no invented CALA managed counts",
    },
    platformMarkets: {
      specificMarkets:
        "Global enterprise footprint (hilton.com). CALA managed subset requires diligence confirmation — not inferred from enterprise totals.",
    },
    commercial: {
      bf_operating_situations:
        "Owners seeking a Hilton brand-managed or brand-operator path with Hilton Honors distribution and brand standards.",
      bf_not_ideal_for:
        "Owners needing a pure third-party operator independent of Hilton brand affiliation; assets outside Hilton managed coverage without a brand-managed mandate",
    },
    governance: {
      risk_programs_narrative:
        "Hilton brand standards and management-agreement QA apply under brand-managed structures — confirm property-level obligations in diligence.",
    },
  }),

  "accor-managed": ACCOR_MANAGED_WEBSITE_FROM_SOURCES,

  "minor-hotels-managed": Object.freeze({
    sources: [{ title: "Minor Hotels", url: "https://www.minorhotels.com/" }],
    profile: {
      website: "https://www.minorhotels.com/",
      primaryServiceModel: "Mixed",
      companyDescription:
        "Minor Hotels (Managed) is the brand-managed operating lens of Minor Hotels — an international hospitality company whose public materials describe brands such as Anantara, Avani, NH Hotels, Tivoli, Oaks, and related labels. This Operator Explorer profile is for owners evaluating Minor brand-managed / brand-operator paths, not a pure third-party independent manager. Label enterprise scale vs CALA / Americas managed footprint separately.",
      companyHistory:
        "Minor Hotels presents as an international multi-brand hospitality platform with ownership, management, and brand pathways. Public materials (minorhotels.com) emphasize lifestyle and full-service brands across regions. This profile is the managed-operating lens.",
      missionStatement:
        "Deliver distinctive hospitality experiences through Minor Hotels brand systems and managed operating capability (minorhotels.com — brand-managed lens).",
      differentiators:
        "Brand-managed path inside Minor Hotels multi-brand platform\nLifestyle and full-service brand set (Anantara, Avani, NH, Tivoli, Oaks, and related)\nInternational footprint with regional depth\nEnterprise vs CALA/Americas managed footprint labeled — no invented regional managed counts",
    },
    platformMarkets: {
      specificMarkets:
        "International footprint per minorhotels.com. Americas / CALA managed subset requires diligence confirmation — not inferred from global brand list.",
    },
    commercial: {
      bf_operating_situations:
        "Owners seeking a Minor Hotels brand-managed or brand-operator path with Minor brand standards and distribution.",
      bf_not_ideal_for:
        "Owners needing a pure third-party operator independent of Minor brand affiliation; assets outside Minor managed coverage without a brand-managed mandate",
    },
    governance: {
      risk_programs_narrative:
        "Minor Hotels brand standards and management-agreement QA apply under brand-managed structures — confirm property-level obligations in diligence.",
    },
  }),
});

/** Deep Profile packs (~50 fields) for brand-managed Core 5. */
export const BRAND_MANAGED_PROFILE_DEEP_PACKS = Object.freeze({
  "marriott-international-managed": MARRIOTT_MANAGED_PROFILE_DEEP_FROM_MXM,

  "ihg-managed": Object.freeze({
    company_name: "IHG Hotels & Resorts (Managed)",
    website: "https://www.ihg.com/",
    companyTagline: "Brand-managed path inside IHG / IHG One Rewards.",
    primaryServiceModel: "Mixed",
    headquarters: "Denham, United Kingdom",
    companySize: "Large (50+ properties)",
    figuresAsOf: FIGURES,
    companyDescription:
      "IHG Hotels & Resorts (Managed) is the brand-managed operating lens of IHG — a global hospitality company whose public materials describe brands across luxury, upscale, and midscale (IHG One Rewards). This profile is for owners evaluating IHG brand-managed / brand-operator paths, not a third-party independent manager.",
    companyHistory:
      "IHG is a global hospitality company with a multi-brand portfolio and owner/franchise/management pathways. Enterprise scale is labeled separately from any CALA managed footprint.",
    missionStatement:
      "Deliver great hotel experiences for guests and attractive returns for owners through IHG brand systems and managed operating capability (ihg.com — brand-managed lens).",
    differentiators:
      "Brand-managed path inside IHG / IHG One Rewards\nMulti-brand portfolio luxury to midscale\nGlobal distribution and loyalty\nEnterprise vs CALA managed footprint labeled",
    managementPhilosophy:
      "Brand standards and managed operating discipline under IHG management agreements — not a pure third-party independent model.",
    "Service Models Supported": ["Full-service", "Select-service", "Resort", "Lifestyle", "Focused-service"],
    "Brand Families Operated": ["IHG", "Soft brands / collections"],
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
      "Soft/collection formats within IHG (e.g. voco, Vignette, Hotel Indigo paths) remain brand-system affiliations — this profile is brand-managed, not independent third-party.",
    brand_narrative_relationship:
      "Brand-managed / brand-operator relationships under IHG. Franchise-only paths are adjacent but distinct.",
    brand_narrative_compliance:
      "IHG brand standards, QA, and management-agreement obligations — confirm PIP and audit cadence in diligence.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "IHG One Rewards brand portfolio", portfolioMix: "Enterprise brand-managed / franchise / owner paths", assetContext: "Luxury through midscale — confirm agreement type per asset", relationshipStatus: "Active (public enterprise)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "IHG One Rewards", description: "Loyalty and distribution platform behind brand-managed hotels." },
      { title: "Management agreements", description: "Brand-managed operating path for owners seeking IHG management." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Brand-managed operations", description: "Operating hotels under IHG management agreements." },
      { title: "Brand standards execution", description: "QA, training, and brand-system participation." },
      { title: "Conversion / reflag", description: "Within IHG brand families where conversion paths exist." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Brand QA", description: "IHG brand audit and standards compliance." },
      { title: "Management agreement controls", description: "Owner reporting and brand obligations per agreement." },
    ]),
    overview_why_1_headline: "What Differentiates IHG (Managed)",
    overview_why_1_story:
      "Brand-managed operating path backed by IHG One Rewards — not a third-party independent manager.",
    overview_why_2_headline: "Multi-Brand System",
    overview_why_2_story: "Public portfolio spans luxury, upscale, and midscale brands under IHG.",
    overview_why_3_headline: "Label CALA Clearly",
    overview_why_3_story:
      "Enterprise footprint is labeled separately; underwrite CALA managed assets on their own evidence.",
    overview_bestat_1_headline: "Brand-Managed Path",
    overview_bestat_1_story: "Owners evaluating IHG management agreements and brand-operator structures.",
    overview_bestat_2_headline: "Loyalty Distribution",
    overview_bestat_2_story: "IHG One Rewards as commercial and guest distribution backbone.",
    overview_bestat_3_headline: "Standards Discipline",
    overview_bestat_3_story: "Brand QA and management-agreement operating rhythm.",
    overview_signal_1_value: "Brand-managed lens · IHG / IHG One Rewards (ihg.com)",
    overview_signal_2_value: "Enterprise scale labeled separately from CALA managed footprint",
    overview_signal_3_value: "Not a third-party independent operator profile",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Comprehensive",
    sustainabilityPrograms: "Yes - Standard",
    esgReporting: "Yes - Standard",
  }),

  "hilton-managed": Object.freeze({
    company_name: "Hilton (Managed)",
    website: "https://www.hilton.com/",
    companyTagline: "Brand-managed path inside Hilton / Hilton Honors.",
    primaryServiceModel: "Mixed",
    headquarters: "McLean, United States",
    companySize: "Large (50+ properties)",
    figuresAsOf: FIGURES,
    companyDescription:
      "Hilton (Managed) is the brand-managed operating lens of Hilton — a global hospitality company whose public materials describe a multi-brand portfolio under Hilton Honors. This profile is for owners evaluating Hilton brand-managed / brand-operator paths, not a third-party independent manager.",
    companyHistory:
      "Hilton is a multi-decade global hospitality company with brand, franchise, and management pathways. Enterprise scale is labeled separately from any CALA managed footprint.",
    missionStatement:
      "Fill the earth with the light and warmth of hospitality through Hilton brand systems and managed operating capability (hilton.com — brand-managed lens).",
    differentiators:
      "Brand-managed path inside Hilton / Hilton Honors\nMulti-brand portfolio luxury to focused-service\nGlobal distribution and loyalty\nEnterprise vs CALA managed footprint labeled",
    managementPhilosophy:
      "Brand standards and managed operating discipline under Hilton management agreements — not a pure third-party independent model.",
    "Service Models Supported": ["Full-service", "Select-service", "Resort", "Lifestyle", "Focused-service"],
    "Brand Families Operated": ["Hilton", "Soft brands / collections"],
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
      "Soft brands / collections (e.g. Curio, Tapestry) sit inside Hilton ecosystems — this profile is brand-managed, not independent third-party.",
    brand_narrative_relationship:
      "Brand-managed / brand-operator relationships under Hilton. Franchise-only paths are adjacent but distinct.",
    brand_narrative_compliance:
      "Hilton brand standards, QA, and management-agreement obligations — confirm PIP and audit cadence in diligence.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "Hilton Honors brand portfolio", portfolioMix: "Enterprise brand-managed / franchise / owner paths", assetContext: "Luxury through focused-service — confirm agreement type per asset", relationshipStatus: "Active (public enterprise)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "Hilton Honors", description: "Loyalty and distribution platform behind brand-managed hotels." },
      { title: "Management agreements", description: "Brand-managed operating path for owners seeking Hilton management." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Brand-managed operations", description: "Operating hotels under Hilton management agreements." },
      { title: "Brand standards execution", description: "QA, training, and brand-system participation." },
      { title: "Conversion / reflag", description: "Within Hilton brand families where conversion paths exist." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Brand QA", description: "Hilton brand audit and standards compliance." },
      { title: "Management agreement controls", description: "Owner reporting and brand obligations per agreement." },
    ]),
    overview_why_1_headline: "What Differentiates Hilton (Managed)",
    overview_why_1_story:
      "Brand-managed operating path backed by Hilton Honors — not a third-party independent manager.",
    overview_why_2_headline: "Multi-Brand System",
    overview_why_2_story: "Public portfolio spans luxury, full-service, and focused-service brands under Hilton.",
    overview_why_3_headline: "Label CALA Clearly",
    overview_why_3_story:
      "Enterprise footprint is labeled separately; underwrite CALA managed assets on their own evidence.",
    overview_bestat_1_headline: "Brand-Managed Path",
    overview_bestat_1_story: "Owners evaluating Hilton management agreements and brand-operator structures.",
    overview_bestat_2_headline: "Loyalty Distribution",
    overview_bestat_2_story: "Hilton Honors as commercial and guest distribution backbone.",
    overview_bestat_3_headline: "Standards Discipline",
    overview_bestat_3_story: "Brand QA and management-agreement operating rhythm.",
    overview_signal_1_value: "Brand-managed lens · Hilton / Hilton Honors (hilton.com)",
    overview_signal_2_value: "Enterprise scale labeled separately from CALA managed footprint",
    overview_signal_3_value: "Not a third-party independent operator profile",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Comprehensive",
    sustainabilityPrograms: "Yes - Standard",
    esgReporting: "Yes - Standard",
  }),

  "accor-managed": ACCOR_MANAGED_PROFILE_DEEP_FROM_SOURCES,

  "minor-hotels-managed": Object.freeze({
    company_name: "Minor Hotels (Managed)",
    website: "https://www.minorhotels.com/",
    companyTagline: "Brand-managed path inside Minor Hotels multi-brand platform.",
    primaryServiceModel: "Mixed",
    headquarters: "Bangkok, Thailand",
    companySize: "Large (50+ properties)",
    figuresAsOf: FIGURES,
    companyDescription:
      "Minor Hotels (Managed) is the brand-managed operating lens of Minor Hotels — an international hospitality company whose public materials describe brands such as Anantara, Avani, NH Hotels, Tivoli, Oaks, and related labels. This profile is for owners evaluating Minor brand-managed / brand-operator paths, not a pure third-party independent manager.",
    companyHistory:
      "Minor Hotels presents as an international multi-brand hospitality platform with ownership, management, and brand pathways. Enterprise/global brand footprint is labeled separately from any Americas/CALA managed subset.",
    missionStatement:
      "Deliver distinctive hospitality experiences through Minor Hotels brand systems and managed operating capability (minorhotels.com — brand-managed lens).",
    differentiators:
      "Brand-managed path inside Minor Hotels\nLifestyle and full-service brand set (Anantara, Avani, NH, Tivoli, Oaks, and related)\nInternational footprint with regional depth\nEnterprise vs Americas/CALA managed footprint labeled",
    managementPhilosophy:
      "Brand standards and managed operating discipline under Minor management agreements — not a pure third-party independent model.",
    "Service Models Supported": ["Full-service", "Select-service", "Resort", "Lifestyle"],
    "Brand Families Operated": ["Independent", "Soft brands / collections"],
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
      "Minor’s public brand set includes lifestyle and full-service labels; soft vs hard positioning varies by brand — this profile is brand-managed, not independent third-party.",
    brand_narrative_relationship:
      "Brand-managed / brand-operator relationships under Minor Hotels. Confirm agreement structure per brand and asset.",
    brand_narrative_compliance:
      "Minor Hotels brand standards and management-agreement obligations — confirm PIP and audit cadence in diligence.",
    brand_portfolio_mix_json: j([
      { brandFlagType: "Anantara / Avani / NH / Tivoli / Oaks (public labels)", portfolioMix: "Minor Hotels brand-managed / ownership / brand paths", assetContext: "Lifestyle and full-service — confirm agreement type per asset", relationshipStatus: "Active (public)" },
    ]),
    brand_relationship_depth_json: j([
      { title: "Minor Hotels brands", description: "Multi-brand platform including Anantara, Avani, NH, Tivoli, Oaks, and related labels on minorhotels.com." },
      { title: "Management agreements", description: "Brand-managed operating path for owners seeking Minor management." },
    ]),
    brand_execution_capabilities_json: j([
      { title: "Brand-managed operations", description: "Operating hotels under Minor management agreements." },
      { title: "Lifestyle & full-service", description: "Public brand set emphasizes lifestyle and full-service experiences." },
      { title: "Conversion / reflag", description: "Within Minor brand families where conversion paths exist." },
    ]),
    brand_governance_compliance_json: j([
      { title: "Brand QA", description: "Minor brand audit and standards compliance." },
      { title: "Management agreement controls", description: "Owner reporting and brand obligations per agreement." },
    ]),
    overview_why_1_headline: "What Differentiates Minor (Managed)",
    overview_why_1_story:
      "Brand-managed operating path inside Minor’s international multi-brand platform — not a pure third-party independent manager.",
    overview_why_2_headline: "Lifestyle + Full-Service Set",
    overview_why_2_story:
      "Public materials cite Anantara, Avani, NH Hotels, Tivoli, Oaks, and related labels.",
    overview_why_3_headline: "Label Regional Managed Footprint",
    overview_why_3_story:
      "Global brand list is labeled separately; underwrite Americas/CALA managed assets on their own evidence.",
    overview_bestat_1_headline: "Brand-Managed Path",
    overview_bestat_1_story: "Owners evaluating Minor management agreements and brand-operator structures.",
    overview_bestat_2_headline: "Multi-Brand Platform",
    overview_bestat_2_story: "Lifestyle and full-service brands under one Minor Hotels platform.",
    overview_bestat_3_headline: "Standards Discipline",
    overview_bestat_3_story: "Brand QA and management-agreement operating rhythm.",
    overview_signal_1_value: "Brand-managed lens · Minor Hotels (minorhotels.com)",
    overview_signal_2_value: "Enterprise/global footprint labeled separately from Americas/CALA managed subset",
    overview_signal_3_value: "Not a pure third-party independent operator profile",
    support24x7: "Yes - Full 24/7",
    businessContinuity: "Yes",
    emergencyResponse: "Yes - Standard",
    sustainabilityPrograms: "Yes - Standard",
    esgReporting: "Yes - Standard",
  }),
});

/** Logo specs for Core 5 (square-preferring). */
export const BRAND_MANAGED_COMPANY_LOGOS = Object.freeze({
  "marriott-international-managed": Object.freeze({
    url: "https://www.google.com/s2/favicons?sz=256&domain=marriott.com",
    filename: "marriott-international-managed-logo-square.png",
    note: "Square domain icon for Marriott International (managed lens).",
    preferredSquare: true,
  }),
  "ihg-managed": Object.freeze({
    url: "https://www.google.com/s2/favicons?sz=256&domain=ihg.com",
    filename: "ihg-managed-logo-square.png",
    note: "Square domain icon for IHG (managed lens).",
    preferredSquare: true,
  }),
  "hilton-managed": Object.freeze({
    url: "https://www.google.com/s2/favicons?sz=256&domain=hilton.com",
    filename: "hilton-managed-logo-square.png",
    note: "Square domain icon for Hilton (managed lens).",
    preferredSquare: true,
  }),
  "accor-managed": Object.freeze({
    url: "https://www.google.com/s2/favicons?sz=256&domain=group.accor.com",
    filename: "accor-managed-logo-square.png",
    note: "Square domain icon for Accor (managed lens).",
    preferredSquare: true,
  }),
  "minor-hotels-managed": Object.freeze({
    url: "https://www.google.com/s2/favicons?sz=256&domain=minorhotels.com",
    filename: "minor-hotels-managed-logo-square.png",
    note: "Square domain icon for Minor Hotels (managed lens).",
    preferredSquare: true,
  }),
});
