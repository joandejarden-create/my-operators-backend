/**
 * Verdict + copy-paste-ready suggested values for Arbor Lodging (CALA) Operator Setup / Explorer.
 * Select values MUST match option labels in public/third-party-operator-setup-new-two.html exactly.
 * Source: arborlodging.com (CALA platform page), Hotel Investment Today (Nov 2024), Arbor press (2025).
 * Legal/Comms review before external publication — metrics marked N/A where not publicly verified.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ENGAGEMENT_ER = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, "..", "fixtures", "operator-engagement-explorer-arbor-cala.json"),
    "utf8"
  )
).engagementReporting;

const COMPANY_NAME = "Arbor Lodging (CALA)";
const WEBSITE = "https://www.arborlodging.com/platforms";
const HQ =
  "Regional hub: Mexico City, Mexico; corporate headquarters: Chicago, Illinois, United States";
const DESC =
  "Arbor Lodging (CALA) is the Caribbean & Latin America practice of Arbor Lodging—a vertically integrated hotel investment and management company. From a fully operational Mexico City base, we combine local presence with U.S. platform depth across business development, operations, sales, revenue management, marketing, finance, accounting, HR, and communications. We are an approved third-party operator for Marriott, Hilton, Hyatt, and IHG, with growing third-party management momentum in Mexico and the broader CALA region.";
const HISTORY =
  "Arbor Lodging was founded in 2006 and built in Chicago by Vamsi Bonthala and Sheenal Patel into a vertically integrated owner-operator and manager. The company operates Arbor Lodging Partners (investment) and Arbor Lodging Management (operations), with Arbor Management Solutions providing outsourced accounting and commercial support to other management companies. CALA expansion has been deliberate: a fully operational Mexico City office integrated with U.S. operations for three+ years, relationship-building across the region, and Christian Hutchinson appointed Director of Business Development, CALA, in November 2025. This CALA profile does not represent a current managed-hotel footprint in the region.";
const TAGLINE =
  "Performance-driven hospitality—local CALA presence, global brand standards, owner mindset.";
const PHILOSOPHY =
  "We think in decades, not quarters: owner-aligned decisions, data-driven accountability, and operational excellence across branded and independent opportunities. In CALA we prioritize proximity, cultural understanding, and on-the-ground responsiveness—not remote-only management.";
const DIFF =
  "Vertically integrated platform (investment + management + outsourced solutions)\nApproved operator for Marriott, Hilton, Hyatt, and IHG\nMexico City regional hub with integrated CALA functional team\nThird-party management growth with conversion and repositioning capability\nEntrepreneurial asset management mindset from Arbor Lodging Partners";

const OV_COMMERCIAL = `Arbor Lodging (CALA) pairs in-market commercial leadership with enterprise revenue management, sales, and marketing discipline. We align pricing, channel mix, and group/leisure strategy to each destination’s demand cycle, with governance on discounting and displacement so owners see how commercial decisions connect to GOP.`;
const OV_DISCIPLINE = `We run accountable operating rhythms: brand QA calendars, SOP consistency, procurement discipline, and finance controls supported by regional leaders and U.S. shared services. Issues surface early with corrective plans, timelines, and owner visibility when trade-offs affect guest experience or economics.`;
const OV_COMMUNICATION = `Owners receive a predictable cadence—monthly operating and financial readouts and deeper quarterly business reviews—with a clear path from insight to decision to action. Material decisions include documented rationale so ownership, asset managers, and brand partners stay aligned.`;
const OV_FLEXIBILITY = `CALA assets require pragmatic trade-offs across brand standards, labor, seasonality, and owner objectives. We present data-backed options and staged investments rather than rigid playbooks—especially for conversions, soft brands, and resort-oriented product.`;
const OV_RISK = `Risk programs cover life safety, security, business continuity, insurance coordination, and crisis communications, scaled to asset type and location. Regional leadership coordinates with property teams, brands, and local counsel.`;

const CAP_PROFILE_OPS = `Arbor Lodging (CALA) operates from Mexico City with an integrated regional team across business development, operations, sales, revenue management, marketing, finance, accounting, HR, and communications—connected to Arbor’s U.S. platform for scale, training, and brand relationships.`;
const CAP_PROFILE_COMM = `Commercially, we integrate sales, marketing, and revenue management as one system: pacing and forecast discipline, comp-set context, and owner-ready dialogue tied to market dynamics in Mexico and expanding CALA markets.`;
const CAP_PROFILE_TRANS = `Transitions and openings use explicit milestones (cash, payroll, safety, brand systems, staffing, IT cutover) with regional accountability—Mexico City team positioned to mobilize for third-party takeovers, conversions, and pre-openings as opportunities are awarded.`;

const BRAND_COMPLIANCE = `We balance brand standards with owner economics: proactive QA readiness, corrective action plans when needed, and joint decisions with ownership when revenue trade-offs arise—supported by data on mix, channel, and performance context.`;
const BRAND_REL = `We operate as an approved third-party manager for Marriott, Hilton, Hyatt, and IHG, with flexibility for independent and collection-oriented owners seeking distribution lift without losing local identity. CALA growth emphasizes Mexico-first execution with Caribbean and Latin America relationship development.`;

/** ALM slide 19 flags + enterprise scope — see fixtures/operator-brand-explorer-arbor-cala.json */
const BRAND_MIX_JSON = JSON.stringify([
  {
    brandFlagType: "AC Hotels Marriott",
    portfolioMix: "Enterprise platform",
    assetContext: "Lifestyle / upper-upscale urban—Marriott family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Courtyard by Marriott",
    portfolioMix: "Enterprise platform",
    assetContext: "Select-service—Marriott family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Residence Inn by Marriott",
    portfolioMix: "Enterprise platform",
    assetContext: "Extended-stay—Marriott family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "SpringHill Suites by Marriott",
    portfolioMix: "Enterprise platform",
    assetContext: "Select-service suites—Marriott family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "TownePlace Suites by Marriott",
    portfolioMix: "Enterprise platform",
    assetContext: "Extended-stay—Marriott family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Aloft",
    portfolioMix: "Enterprise platform",
    assetContext: "Lifestyle select-service—Marriott family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Curio Collection by Hilton",
    portfolioMix: "Enterprise platform",
    assetContext: "Soft brand / collection—Hilton family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Hampton by Hilton",
    portfolioMix: "Enterprise platform",
    assetContext: "Upper-midscale select-service—Hilton family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Hilton Garden Inn",
    portfolioMix: "Enterprise platform",
    assetContext: "Upscale select-service—Hilton family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Homewood Suites by Hilton",
    portfolioMix: "Enterprise platform",
    assetContext: "Extended-stay—Hilton family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Home2 Suites by Hilton",
    portfolioMix: "Enterprise platform",
    assetContext: "Extended-stay—Hilton family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Hyatt Place",
    portfolioMix: "Enterprise platform",
    assetContext: "Select-service—Hyatt family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Holiday Inn Express",
    portfolioMix: "Enterprise platform",
    assetContext: "Upper-midscale select-service—IHG family",
    relationshipStatus: "Active / Approved",
  },
  {
    brandFlagType: "Hotel Indigo",
    portfolioMix: "Enterprise platform",
    assetContext: "Lifestyle boutique—IHG family",
    relationshipStatus: "Active / Approved",
  },
]);

const BRAND_DEPTH_JSON = JSON.stringify([
  {
    brandSegment: "Major brand families (enterprise)",
    relationshipType: "Active / approved",
    depth: "Strong",
    ownerContext:
      "Strong relationships and years of experience with Marriott, Hilton, Hyatt, and IHG across the U.S. platform—per Arbor ALM materials (slide 19).",
  },
  {
    brandSegment: "PIP & brand renovation",
    relationshipType: "Active",
    depth: "Strong",
    ownerContext:
      "Experience implementing PIP programs and complying with brand renovation and capital improvement guidelines; construction and operational excellence awards cited in Arbor materials.",
  },
  {
    brandSegment: "Mexico & Baja (CALA)",
    relationshipType: "Active third-party",
    depth: "Strong",
    ownerContext:
      "Mexico City hub and approved-operator positioning; pursuing third-party management opportunities—no current CALA managed portfolio on this profile.",
  },
  {
    brandSegment: "Conversion / reflag (enterprise)",
    relationshipType: "Active / approved",
    depth: "Strong",
    ownerContext:
      "40+ renovated and repositioned properties across urban, suburban, resort, and rural settings on the parent platform (ALM deck).",
  },
  {
    brandSegment: "Caribbean & Latin America",
    relationshipType: "Developing",
    depth: "Emerging",
    ownerContext:
      "Approved operator positioning in LATAM and Caribbean; relationship-building and feasibility-led growth—confirm live CALA contracts.",
  },
]);

const BRAND_EXEC_JSON = JSON.stringify([
  {
    title: "PIP & brand renovation execution",
    description:
      "Experience implementing PIP programs and complying with brand renovation and capital improvement guidelines across major franchise families.",
  },
  {
    title: "Third-party management onboarding",
    description:
      "Owner and brand onboarding from selection through stabilization—documentation, systems, staffing, and opening or takeover readiness.",
  },
  {
    title: "Conversion & reflag execution",
    description:
      "Playbooks for conversions and repositioning with brand technical services, training, and guest-facing transition planning.",
  },
  {
    title: "Brand-owner coordination",
    description:
      "Single operating rhythm between ownership, brand development, technical services, and property leadership.",
  },
  {
    title: "CALA regional proximity",
    description:
      "Mexico City–based team for business development, operations, and commercial support with U.S. platform depth.",
  },
]);

const BRAND_GOV_JSON = JSON.stringify([
  {
    title: "Brand compliance & QA",
    description:
      "Standards readiness, audit preparation, and recurring compliance tracking by flag—construction and operational excellence discipline cited in Arbor materials.",
  },
  {
    title: "Technical services & PIP coordination",
    description: "PIP, life safety, design review, and opening checklist alignment with brand-required deliverables.",
  },
  {
    title: "Owner decision support",
    description: "Clear trade-off framing for brand obligations, timing, capex, and operating economics.",
  },
]);

const ARBOR_FALLBACK =
  "Arbor Lodging (CALA): owner-ready language based on public company materials—have Legal/Comms confirm before external use.";
const BF_DEFAULT =
  "[Internal fill guidance — do not publish] Select deal types Arbor pursues in CALA from ALM materials (Mexico resort/leisure, urban gateway, conversion, third-party management). Use Not Measured / N/A on signals until you have a defensible benchmark.";
const INFRA_DEFAULT =
  "Systems vary by brand and asset—state brand-dependent stacks. Summarize owner reporting cadence and secure channels without inventing vendor names.";
const LEADERSHIP_FALLBACK =
  "Mirror CALA leadership from Leadership Team Members: Vamsi Bonthala (CEO), Sheenal Patel (co-founder), Christian Hutchinson (Director BD, CALA), Neil DeGuia (operations leadership), and regional functional leads in Mexico City.";

/** @type {Record<string, { verdict: string, suggestedCopyPaste: string } | Function>} */
const FIELD_SUGGESTIONS = {
  companyName: { verdict: "Change", suggestedCopyPaste: COMPANY_NAME },
  companyDescription: { verdict: "Change", suggestedCopyPaste: DESC },
  website: { verdict: "Change", suggestedCopyPaste: WEBSITE },
  headquarters: { verdict: "Change", suggestedCopyPaste: HQ },
  yearEstablished: { verdict: "Change", suggestedCopyPaste: "2006" },
  yearsInBusiness: { verdict: "Change", suggestedCopyPaste: "19" },
  companySize: { verdict: "Change", suggestedCopyPaste: "Medium (10-50 properties)" },
  companyTagline: { verdict: "Change", suggestedCopyPaste: TAGLINE },
  companyHistory: { verdict: "Change", suggestedCopyPaste: HISTORY },
  differentiators: { verdict: "Change", suggestedCopyPaste: DIFF },
  managementPhilosophy: { verdict: "Change", suggestedCopyPaste: PHILOSOPHY },
  regions: { verdict: "Change", suggestedCopyPaste: "Caribbean, Latin America" },
  brands: {
    verdict: "Change",
    suggestedCopyPaste:
      "AC Hotels, Courtyard by Marriott, Residence Inn by Marriott, SpringHill Suites by Marriott, TownePlace Suites by Marriott, Aloft, Curio Collection by Hilton, Hampton by Hilton, Hilton Garden Inn, Homewood Suites by Hilton, Home2 Suites by Hilton, Hyatt Place, Holiday Inn Express, Hotel Indigo",
  },
  additionalBrands: { verdict: "Change", suggestedCopyPaste: "None" },
  chainScalesSupported: {
    verdict: "Change",
    suggestedCopyPaste: "Upper Upscale, Upscale, Upper Midscale, Midscale",
  },
  contactName: { verdict: "Change", suggestedCopyPaste: "Christian Hutchinson" },
  contactEmail: { verdict: "Change", suggestedCopyPaste: "chutchinson@arborlodging.com" },
  contactPhone: { verdict: "Change", suggestedCopyPaste: "Not published — use owner-facing BD contact on management agreement" },
  diligenceDocumentLinks: { verdict: "Change", suggestedCopyPaste: WEBSITE },

  overview_signal_1_value: {
    verdict: "Change",
    suggestedCopyPaste: "CALA managed hotels: 0",
  },
  overview_signal_2_value: {
    verdict: "Change",
    suggestedCopyPaste:
      "12 CALA countries — team representative experience (Experiencia Regional, Jun 2026)",
  },
  overview_signal_3_value: {
    verdict: "Change",
    suggestedCopyPaste:
      "Mexico City regional hub; approved operator for Marriott, Hilton, Hyatt, IHG in LATAM & Caribbean",
  },

  brand_narrative_compliance: { verdict: "Change", suggestedCopyPaste: BRAND_COMPLIANCE },
  brand_narrative_relationship: { verdict: "Change", suggestedCopyPaste: BRAND_REL },
  brand_soft_independent_narrative: {
    verdict: "Change",
    suggestedCopyPaste:
      "For owners evaluating soft-brand affiliation, independent-to-branded conversion, or resort repositioning in Mexico and CALA, expect clear brand-requirement translation, staged transition planning, and commercial programs that respect local identity while improving distribution and loyalty contribution.",
  },
  brand_signal_audit: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  brand_signal_reflag: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  brand_signal_franchise_align: { verdict: "Change", suggestedCopyPaste: "High" },
  brand_signal_soft_retention: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  brand_conversion_project_count: { verdict: "Change", suggestedCopyPaste: "40+" },
  numberOfBrands: { verdict: "Change", suggestedCopyPaste: "17" },
  brandedVsIndependentMix: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  brand_portfolio_mix_json: { verdict: "Change", suggestedCopyPaste: BRAND_MIX_JSON },
  brand_relationship_depth_json: { verdict: "Change", suggestedCopyPaste: BRAND_DEPTH_JSON },
  brand_execution_capabilities_json: { verdict: "Change", suggestedCopyPaste: BRAND_EXEC_JSON },
  brand_governance_compliance_json: { verdict: "Change", suggestedCopyPaste: BRAND_GOV_JSON },
  brandFamiliesOperated: {
    verdict: "Change",
    suggestedCopyPaste: "Marriott, Hilton, Hyatt, IHG, Independent, Soft brands / collections",
  },

  cap_kpi_operating_model: { verdict: "Change", suggestedCopyPaste: "Mixed Branded and Independent Portfolio" },
  cap_kpi_execution_strength: { verdict: "Change", suggestedCopyPaste: "Proven" },
  cap_kpi_transition: { verdict: "Change", suggestedCopyPaste: "Strong" },
  cap_kpi_reporting: { verdict: "Change", suggestedCopyPaste: "Structured" },
  cap_profile_operational: { verdict: "Change", suggestedCopyPaste: CAP_PROFILE_OPS },
  cap_profile_commercial: { verdict: "Change", suggestedCopyPaste: CAP_PROFILE_COMM },
  cap_profile_transition: { verdict: "Change", suggestedCopyPaste: CAP_PROFILE_TRANS },
  cap_card_asset_positioning: {
    verdict: "Change",
    suggestedCopyPaste:
      "Arbor Lodging (CALA) is a regional management platform for owners who want Mexico-first execution with credible brand relationships—not a generic global scale story. We emphasize resort, lifestyle, select-service, and conversion-oriented assets as the region matures.",
  },
  cap_card_service_diff: {
    verdict: "Change",
    suggestedCopyPaste:
      "Differentiation is vertical integration: investment discipline from Arbor Lodging Partners, operating excellence from Arbor Lodging Management, and optional Arbor Management Solutions support—plus a Mexico City team that can scale immediately.",
  },
  cap_card_execution_rel: {
    verdict: "Change",
    suggestedCopyPaste:
      "Owners get transparent pacing, documented decisions, and escalation to regional leaders. CALA is growing quickly; continuity strength is the parent platform and brand approvals—confirm CALA-specific track record on live contracts.",
  },
  cap_card_governance: {
    verdict: "Change",
    suggestedCopyPaste:
      "Governance includes monthly operating reviews, quarterly strategy touchpoints, brand compliance oversight, and finance controls—pack formats are deal-specific; confirm in executed management agreements.",
  },
  cap_deep_revenue_systems: {
    verdict: "Change",
    suggestedCopyPaste:
      "Brand-appropriate PMS/RMS/channel tooling with governance on discounting, group displacement, and channel mix; owner participation in commercial reviews with comp-set and pacing context.",
  },
  cap_deep_execution_infra: {
    verdict: "Change",
    suggestedCopyPaste:
      "Mexico City regional pods with U.S. platform support for finance, HR, IT, and procurement; escalation property → CALA regional → enterprise; playbooks for resort, lifestyle, and select-service transitions.",
  },
  cap_signal_budget: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  cap_signal_lift: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  cap_signal_trans: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },

  ov_card_commercial: { verdict: "Change", suggestedCopyPaste: OV_COMMERCIAL },
  ov_card_discipline: { verdict: "Change", suggestedCopyPaste: OV_DISCIPLINE },
  ov_card_communication: { verdict: "Change", suggestedCopyPaste: OV_COMMUNICATION },
  ov_card_flexibility: { verdict: "Change", suggestedCopyPaste: OV_FLEXIBILITY },
  ov_card_risk: { verdict: "Change", suggestedCopyPaste: OV_RISK },
  ov_cluster_interaction: {
    verdict: "Change",
    suggestedCopyPaste:
      "Monthly operating and financial reviews, quarterly strategy sessions, and ad-hoc escalation for capex, brand, or commercial decisions—with materials in advance and tracked actions.",
  },
  ov_cluster_deliverables: {
    verdict: "Change",
    suggestedCopyPaste:
      "Operating plans, forecasts, labor and productivity analytics, commercial pacing, capex trackers, brand QA status, and guest insight summaries—scaled to lender and owner requirements.",
  },
  ownerEngagementNarrative: {
    verdict: "Change",
    suggestedCopyPaste:
      "Arbor Lodging (CALA) is built for owners who want a long-term partner with local presence in Mexico and a credible path into broader CALA markets—backed by a vertically integrated platform and approved brand relationships.",
  },
  ov_strategic_owner_value_json: {
    verdict: "Change",
    suggestedCopyPaste: JSON.stringify(ENGAGEMENT_ER.strategicOwnerValue),
  },
  ov_engagement_cadence_json: {
    verdict: "Change",
    suggestedCopyPaste: JSON.stringify(ENGAGEMENT_ER.engagementCadence),
  },
  ov_controls_governance_json: {
    verdict: "Change",
    suggestedCopyPaste: JSON.stringify(ENGAGEMENT_ER.controlsGovernance),
  },
  ov_reports_received_json: {
    verdict: "Change",
    suggestedCopyPaste: JSON.stringify(ENGAGEMENT_ER.reportsReceived),
  },
  ov_owner_tools_json: {
    verdict: "Change",
    suggestedCopyPaste: JSON.stringify(ENGAGEMENT_ER.ownerTools),
  },
  ov_lifecycle_support_json: {
    verdict: "Change",
    suggestedCopyPaste: JSON.stringify(ENGAGEMENT_ER.lifecycleSupport),
  },
  ownerReportingLevel: {
    verdict: "Change",
    suggestedCopyPaste: "Customized monthly packs + quarterly reviews",
  },
  governanceCadence: {
    verdict: "Change",
    suggestedCopyPaste: "Monthly operating reviews + quarterly strategic reviews",
  },

  lead_narrative_regional: {
    verdict: "Change",
    suggestedCopyPaste:
      "Colombia\nCosta Rica\nCuba\nEl Salvador\nGuatemala\nHaiti\nHonduras\nMexico\nPanama\nPeru\nPuerto Rico\nDominican Republic",
  },
  displayLeadershipOnExplorer: { verdict: "Change", suggestedCopyPaste: "true" },

  geo_cala_existing_hotels: {
    verdict: "Change",
    suggestedCopyPaste: "0",
  },
  geo_cala_existing_rooms: {
    verdict: "Change",
    suggestedCopyPaste: "0",
  },
  geo_cala_pipeline_hotels: {
    verdict: "Change",
    suggestedCopyPaste: "0",
  },
  geo_cala_pipeline_rooms: {
    verdict: "Change",
    suggestedCopyPaste: "0",
  },
  geo_cala_total_hotels: { verdict: "Change", suggestedCopyPaste: "0" },
  geo_cala_total_rooms: { verdict: "Change", suggestedCopyPaste: "0" },
  geo_na_existing_hotels: { verdict: "Change", suggestedCopyPaste: "0" },
  geo_na_existing_rooms: { verdict: "Change", suggestedCopyPaste: "0" },
  geo_na_pipeline_hotels: { verdict: "Change", suggestedCopyPaste: "0" },
  geo_na_pipeline_rooms: { verdict: "Change", suggestedCopyPaste: "0" },

  marketPresenceType: {
    verdict: "Change",
    suggestedCopyPaste: "Regional operator with dedicated CALA hub (Mexico City)",
  },
  newBuildOpeningExperience: {
    verdict: "Change",
    suggestedCopyPaste: "Strong",
  },
  governanceCadence: { verdict: "Change", suggestedCopyPaste: "Monthly operating review; quarterly strategy review" },
  salesPlatform: {
    verdict: "Change",
    suggestedCopyPaste: "Integrated sales + revenue management with brand-appropriate channel stack",
  },
  infra_technology_maturity_level: { verdict: "Change", suggestedCopyPaste: "Advanced" },

  exec_1_name: { verdict: "Change", suggestedCopyPaste: "Vamsi Bonthala" },
  exec_1_title: { verdict: "Change", suggestedCopyPaste: "Chief Executive Officer, Arbor Lodging Partners" },
  exec_1_role: { verdict: "Change", suggestedCopyPaste: "Enterprise · Investment & strategy" },
  exec_1_summary: {
    verdict: "Change",
    suggestedCopyPaste:
      "Co-founded Arbor Lodging; leads vertically integrated strategy across investment, management, and CALA expansion priorities.",
  },
  exec_2_name: { verdict: "Change", suggestedCopyPaste: "Christian Hutchinson" },
  exec_2_title: { verdict: "Change", suggestedCopyPaste: "Director of Business Development, CALA" },
  exec_2_role: { verdict: "Change", suggestedCopyPaste: "CALA · Development & third-party growth" },
  exec_2_summary: {
    verdict: "Change",
    suggestedCopyPaste:
      "Leads CALA business development from Mexico City; hospitality development and feasibility background across U.S. and CALA.",
  },
  exec_3_name: { verdict: "Change", suggestedCopyPaste: "Neil DeGuia" },
  exec_3_title: { verdict: "Change", suggestedCopyPaste: "Senior Operations Leadership (Third-Party)" },
  exec_3_role: { verdict: "Change", suggestedCopyPaste: "Operations · Brand execution" },
  exec_3_summary: {
    verdict: "Change",
    suggestedCopyPaste:
      "Strengthens third-party operating execution; prior Aimbridge senior operations leadership—supports owner and brand accountability on managed assets.",
  },

  infra_signal_uptime: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  infra_signal_incident: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  infra_signal_adoption: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  infra_signal_refresh: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  risk_signal_audit: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  risk_signal_bcp: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  risk_signal_control: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
  risk_signal_insurance: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
};

export function getSuggestionForRow(row) {
  const { fieldName, tab, isEmpty } = row;
  const raw = row.rawValue == null ? "" : String(row.rawValue).trim();

  const spec = FIELD_SUGGESTIONS[fieldName];
  if (typeof spec === "function") return spec({ raw, isEmpty, tab });
  if (spec && typeof spec === "object") {
    return { verdict: spec.verdict, suggestedCopyPaste: spec.suggestedCopyPaste };
  }

  if (tab === "Deal Terms" && isEmpty) {
    return {
      verdict: "Review",
      suggestedCopyPaste:
        "Complete only with Legal: fee schedules, central charges, performance tests, and renewal terms are deal-specific—use data room, not placeholders.",
    };
  }
  if (tab === "Infrastructure & Data" && isEmpty) {
    if (/^infra_signal_/.test(fieldName)) {
      return { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" };
    }
    return { verdict: "Review", suggestedCopyPaste: INFRA_DEFAULT };
  }
  if (tab === "Risk & Compliance" && isEmpty && /^risk_signal_/.test(fieldName)) {
    return { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" };
  }
  if (tab === "Best Fit & Preferences" && isEmpty) {
    return { verdict: "Review", suggestedCopyPaste: BF_DEFAULT };
  }
  if (tab === "Owner Value & Engagement" && isEmpty) {
    if (fieldName.startsWith("ov_")) {
      return { verdict: "Review", suggestedCopyPaste: OV_COMMERCIAL };
    }
  }
  if (tab === "Leadership & Team" && isEmpty) {
    return { verdict: "Review", suggestedCopyPaste: LEADERSHIP_FALLBACK };
  }
  if (fieldName.startsWith("geo_") && isEmpty) {
    return { verdict: "Review", suggestedCopyPaste: "0" };
  }

  if (!isEmpty && raw) {
    return { verdict: "Keep", suggestedCopyPaste: raw };
  }

  return { verdict: "Review", suggestedCopyPaste: ARBOR_FALLBACK };
}
