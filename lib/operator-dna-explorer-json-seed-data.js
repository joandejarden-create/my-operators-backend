/**
 * Realistic seed payloads for all 22 Operator Explorer DNA JSON fields.
 * Shapes match public/js/*-sections.js parsers (not generic placeholders).
 */
import { DNA_EXPLORER_JSON_TABLE, DNA_EXPLORER_JSON_FIELD_SPECS } from "./operator-dna-explorer-json-fields.js";
import {
  buildBrandExplorerSeedFields,
  brandExplorerSeedMarkerKeys,
} from "./operator-brand-explorer-seed-data.js";
import {
  buildEngagementExplorerSeedFields,
  ENGAGEMENT_JSON_FIELD_KEYS,
} from "./operator-engagement-explorer-seed-data.js";

function cloneJson(v) {
  return JSON.parse(JSON.stringify(v));
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function introForCompany(base, companyName, index) {
  const name = nz(companyName);
  const variants = [
    `${name} documents the following for owner-facing Explorer profiles.`,
    `Owner-facing capability summary for ${name} (verified operator-provided narrative).`,
    `${name} — structured operating story used on the public DNA profile.`,
  ];
  return variants[index % variants.length] || base;
}

/** Operating Platform pillars → op_*_json */
const OP_PILLAR_SEED = {
  commercial: {
    title: "Commercial Engine",
    intro:
      "Drives top-line performance through disciplined revenue strategy, pricing, distribution, and direct-booking focus.",
    items: [
      {
        title: "Revenue Management",
        description:
          "Centralized RMS governance, weekly pickup reviews, and rate fences aligned to segment and channel strategy.",
      },
      {
        title: "Sales Strategy",
        description:
          "Group and corporate contracting with local sales leadership accountable to forecast and pace targets.",
      },
      {
        title: "Distribution Strategy",
        description:
          "OTA, GDS, and wholesale mix managed with parity controls and cost-to-acquire discipline.",
      },
      {
        title: "Pricing Discipline",
        description:
          "BAR integrity, restriction governance, and promotional approval paths tied to owner reporting.",
      },
      {
        title: "Direct Booking",
        description:
          "Website conversion, CRM, and loyalty levers to grow owned-channel share in resort and urban assets.",
      },
      {
        title: "Forecasting",
        description:
          "Rolling 90-day forecasts linking commercial plans to staffing, purchasing, and owner variance reviews.",
      },
    ],
  },
  reporting: {
    title: "Owner Reporting & Communication",
    intro:
      "Transparent, proactive governance and reporting that keeps owners informed and in control.",
    items: [
      {
        title: "Reporting Cadence",
        description:
          "Monthly operating pack plus weekly flash during transitions; calendar published at onboarding.",
      },
      {
        title: "Monthly Business Reviews",
        description:
          "Structured review of P&L, KPIs, risks, capex, and action owners with documented follow-up.",
      },
      {
        title: "Dashboards & KPIs",
        description:
          "Owner portal views for RevPAR, GOP, guest scores, labor productivity, and open issues.",
      },
      {
        title: "CapEx Visibility",
        description:
          "PIP and renovation trackers with approval history, spend status, and brand coordination notes.",
      },
      {
        title: "Budget Process",
        description:
          "Annual budget development, owner approval workflow, and rolling forecast updates with variance bridges.",
      },
      {
        title: "Responsiveness Standards",
        description:
          "Defined response times for owner inquiries and escalation paths for material operating issues.",
      },
    ],
  },
  preopening: {
    title: "Pre-Opening & Transition Support",
    intro:
      "End-to-end opening and transition support from planning through stabilized operations.",
    items: [
      {
        title: "Recruiting",
        description:
          "GM and department head hiring plans with compensation benchmarks for the local labor market.",
      },
      {
        title: "Procurement",
        description:
          "Opening FF&E and OS&E sourcing with approved vendor lists and owner visibility on lead times.",
      },
      {
        title: "Systems Setup",
        description:
          "PMS, RMS, POS, and finance stack configuration with cutover testing before soft opening.",
      },
      {
        title: "Transition Planning",
        description:
          "Day-by-day transition roadmap from keys handover through stabilization milestones.",
      },
      {
        title: "Training",
        description:
          "Brand and service training paths for management and hourly teams before guest-facing launch.",
      },
      {
        title: "Opening Support",
        description:
          "On-site operating leadership during launch window to protect service and owner confidence.",
      },
    ],
  },
  conversion: {
    title: "Conversion & Repositioning",
    intro:
      "Proven ability to reposition assets and unlock value through thoughtful operational change.",
    items: [
      {
        title: "Brand Transitions",
        description:
          "Reflag timelines, brand liaison cadence, and owner communication through conversion windows.",
      },
      {
        title: "PIP Execution",
        description:
          "Scope control, contractor coordination, and operating plans that limit guest disruption.",
      },
      {
        title: "Renovation Coordination",
        description:
          "Phased work schedules balancing construction noise, outlet closures, and revenue protection.",
      },
      {
        title: "Operational Turnaround",
        description:
          "Service recovery, labor right-sizing, and commercial resets on underperforming assets.",
      },
      {
        title: "Reopening Ramp",
        description:
          "Post-renovation staffing, marketing, and rate ladders to rebuild index and occupancy.",
      },
      {
        title: "Stabilization",
        description:
          "KPI cadence and SOP discipline once the asset reaches steady-state performance.",
      },
    ],
  },
  fb: {
    title: "F&B, Lifestyle & Resort Capability",
    intro:
      "Elevated resort programming and F&B that drive guest satisfaction, spend, and local relevance.",
    items: [
      {
        title: "Restaurant Concepts",
        description:
          "Outlet positioning, menu engineering, and labor models aligned to resort and urban demand.",
      },
      {
        title: "Beach & Pool Operations",
        description:
          "Safe, service-oriented outdoor programming with ancillary revenue and staffing plans.",
      },
      {
        title: "Programming & Activities",
        description:
          "Curated guest events and partnerships that differentiate the stay without over-staffing.",
      },
      {
        title: "Local Partnerships",
        description:
          "Vendor and experience collaborations that add authenticity in CALA and gateway markets.",
      },
      {
        title: "Spa & Wellness",
        description:
          "Treatment menus and retail offers sized to asset positioning and owner return hurdles.",
      },
      {
        title: "Guest Experience Design",
        description:
          "Journey mapping across arrival, in-stay, and departure for lifestyle and resort assets.",
      },
    ],
  },
};

const OP_FORM_KEYS = {
  commercial: "op_commercial_engine_json",
  reporting: "op_owner_reporting_json",
  preopening: "op_preopening_transition_json",
  conversion: "op_conversion_repositioning_json",
  fb: "op_fb_lifestyle_resort_json",
};

const MKT_SEED = {
  mkt_regional_expertise_json: [
    {
      title: "Regional Leadership Hub",
      description:
        "Miami-based regional team with quarterly market visits and local legal/accounting advisors in key CALA markets.",
    },
    {
      title: "Language Capabilities",
      description:
        "English and Spanish operating support; Portuguese coordination available for Brazil pipeline markets.",
    },
    {
      title: "Labor Market Familiarity",
      description:
        "Seasonal staffing models for resort corridors; union and contract labor awareness in gateway cities.",
    },
    {
      title: "Regulatory Familiarity",
      description:
        "Island and cross-border markets supported with local counsel for labor, tax, and permitting requirements.",
    },
    {
      title: "Cultural Fluency",
      description:
        "Service and owner communication adapted to leisure-first and mixed owner-investor expectations.",
    },
    {
      title: "Vendor / Partner Network",
      description:
        "Preferred procurement, sales, and renovation partners active in Mexico, Caribbean, and Central America.",
    },
  ],
  mkt_market_fit_signals_json: [
    {
      title: "Coastal Destinations",
      description: "Documented conversions and resort stabilizations on beach and waterfront submarkets.",
    },
    {
      title: "Urban Leisure Gateway",
      description: "Mixed business/leisure demand management in Cancún, Panama City, and San Juan corridors.",
    },
    {
      title: "Island Complexity",
      description: "Experience coordinating logistics, staffing, and supply chain on island assets.",
    },
    {
      title: "Resort-Adjacent Select Service",
      description: "Select-service and extended-stay assets near resort demand generators.",
    },
    {
      title: "Independent Resorts",
      description: "Owner-led resorts seeking institutional reporting without losing local identity.",
    },
    {
      title: "Pipeline Selectivity",
      description: "Growth prioritizes fit and governance alignment over broad geographic coverage.",
    },
  ],
};

const BF_SEED = {
  bf_fit_criteria_json: [
    {
      fitCriteria: "Market Fit",
      operatorLooksFor:
        "Coastal, resort, island, leisure-led, or mixed leisure/business CALA and Mexico gateway markets.",
      importance: "High",
    },
    {
      fitCriteria: "Asset Type Fit",
      operatorLooksFor:
        "Full-service resort, lifestyle hotel, condo-hotel, soft brand, independent, or conversion/reflag.",
      importance: "High",
    },
    {
      fitCriteria: "Ownership Fit",
      operatorLooksFor:
        "Owners seeking transparent reporting, active operating partner, and defined governance cadence.",
      importance: "High",
    },
    {
      fitCriteria: "Brand / Flag Fit",
      operatorLooksFor:
        "Projects with realistic brand path, PIP awareness, and operator-led onboarding or conversion support.",
      importance: "Medium / High",
    },
    {
      fitCriteria: "Operating Complexity",
      operatorLooksFor:
        "Assets where resort, F&B, transition, or multi-owner structures benefit from specialized experience.",
      importance: "High",
    },
    {
      fitCriteria: "Commercial Upside",
      operatorLooksFor:
        "Revenue, distribution, direct booking, or repositioning opportunities with measurable lift potential.",
      importance: "High",
    },
    {
      fitCriteria: "Capital / CapEx Readiness",
      operatorLooksFor:
        "Funded PIP or renovation plan with owner approval path and realistic timeline.",
      importance: "Medium / High",
    },
    {
      fitCriteria: "Timing & Readiness",
      operatorLooksFor:
        "Clear decision timeline, data room access, and authority to execute management agreement.",
      importance: "High",
    },
  ],
  bf_best_fit_project_types_json: [
    {
      fitLevel: "Best Fit",
      projectType: "Conversion / Reflag",
      ownerContext:
        "Existing hotel with brand transition, repositioning, or operating reset within 12–18 months.",
    },
    {
      fitLevel: "Best Fit",
      projectType: "Resort / Leisure Asset",
      ownerContext:
        "Beach, island, spa, or F&B-heavy destination hotel with 120–450 keys.",
    },
    {
      fitLevel: "Best Fit",
      projectType: "Condo-Hotel / Mixed Ownership",
      ownerContext:
        "Complex owner pools requiring disciplined reporting and guest-ready operations.",
    },
    {
      fitLevel: "Selective Fit",
      projectType: "New Build",
      ownerContext:
        "Pre-opening support when capital stack, brand, and opening date are confirmed.",
    },
    {
      fitLevel: "Selective Fit",
      projectType: "Urban Full Service",
      ownerContext:
        "Gateway urban assets with meaningful F&B or owner reporting complexity.",
    },
    {
      fitLevel: "Limited Fit",
      projectType: "Pure Economy / Low-Touch",
      ownerContext:
        "Limited alignment with resort, lifestyle, and institutional owner reporting strengths.",
    },
  ],
  bf_preferred_deal_profile_json: [
    {
      label: "Preferred Owner Type",
      value:
        "Family offices, developers, institutional holders, and owners seeking hands-on management partnership.",
    },
    {
      label: "Preferred Agreement Type",
      value:
        "Third-party management, transition management, or pre-opening support with clear fee transparency.",
    },
    {
      label: "Preferred Market Position",
      value:
        "Upper-midscale through upscale resort, lifestyle, soft brand, and independent collections.",
    },
    {
      label: "Ideal Situation",
      value:
        "Owner has defined objectives, accessible financials, brand direction, and 60–120 day decision path.",
    },
    {
      label: "Less Ideal Situation",
      value:
        "Exploratory inquiry without authority, unfunded PIP, or passive owner with no governance appetite.",
    },
    {
      label: "Important Deal Signals",
      value:
        "Responsiveness, data quality, capex readiness, market demand proof, and alignment on reporting cadence.",
    },
  ],
  bf_evaluation_path_json: [
    {
      title: "Initial Screen",
      description:
        "Review market, asset type, keys, ownership, brand status, and transition timing.",
    },
    {
      title: "Fit Qualification",
      description:
        "Assess regional experience, service scope, and complexity fit against operator DNA.",
    },
    {
      title: "Information Request",
      description:
        "Collect trailing financials, STR, PIP/capex plan, labor model, and owner priority memo.",
    },
    {
      title: "Internal Review",
      description:
        "Operations, commercial, finance, and leadership sign-off on feasibility and resourcing.",
    },
    {
      title: "Owner Discussion",
      description:
        "Align on reporting, governance, brand obligations, fees, and transition milestones.",
    },
    {
      title: "Proposal Path",
      description:
        "Issue management proposal or defer with documented rationale if fit is weak.",
    },
  ],
  bf_red_flags_json: [
    {
      title: "Unclear Ownership Authority",
      description: "Decision-maker not identified or investor group not aligned on path.",
    },
    {
      title: "Insufficient CapEx Readiness",
      description: "PIP or renovation scope lacks funding or realistic schedule.",
    },
    {
      title: "Unrealistic Performance Expectations",
      description: "Owner underwriting ignores market, brand, or operating constraints.",
    },
    {
      title: "No Clear Timeline",
      description: "Perpetual exploratory phase without financing or transition trigger.",
    },
    {
      title: "Poor Strategic Fit",
      description: "Asset type or market outside core resort/CALA operating experience.",
    },
    {
      title: "Weak Data Access",
      description: "Inability to review financials, contracts, or operating history before proposal.",
    },
  ],
};

function buildOpPillarJson(pillarKey, companyName, index) {
  const pillar = OP_PILLAR_SEED[pillarKey];
  if (!pillar) return "";
  return JSON.stringify({
    intro: introForCompany(pillar.intro, companyName, index),
    items: cloneJson(pillar.items),
  });
}

function buildPlatformSeedFields(opts) {
  const index = Number(opts.index) || 0;
  const companyName = opts.companyName || "";
  const fields = {};
  for (const [pillarKey, formKey] of Object.entries(OP_FORM_KEYS)) {
    fields[formKey] = buildOpPillarJson(pillarKey, companyName, index);
  }
  for (const [key, rows] of Object.entries(MKT_SEED)) {
    fields[key] = JSON.stringify(cloneJson(rows));
  }
  return fields;
}

/**
 * @param {{ index?: number, companyName?: string, existingFields?: Record<string, unknown> }} opts
 * @returns {Record<string, string>}
 */
export function buildProfileDnaJsonSeedFields(opts) {
  return buildBrandExplorerSeedFields(opts);
}

/**
 * @returns {Record<string, string>}
 */
export function buildCommercialDnaJsonSeedFields() {
  return {
    ...buildEngagementExplorerSeedFields(),
    ...Object.fromEntries(
      Object.entries(BF_SEED).map(([k, v]) => [k, JSON.stringify(cloneJson(v))])
    ),
  };
}

/**
 * @param {{ index?: number, companyName?: string }} opts
 * @returns {Record<string, string>}
 */
export function buildPlatformDnaJsonSeedFields(opts) {
  return buildPlatformSeedFields(opts);
}

/** All 22 form keys (for markers / verification). */
export function dnaExplorerJsonSeedMarkerKeys() {
  return DNA_EXPLORER_JSON_FIELD_SPECS.map((s) => s.formKey);
}

/** Keys expected on each new-base table after full seed. */
export function dnaExplorerJsonKeysForTable(tableName) {
  return DNA_EXPLORER_JSON_FIELD_SPECS.filter((s) => s.airtableTable === tableName).map(
    (s) => s.formKey
  );
}

export function hasDnaJsonSeed(fields, tableName) {
  const keys = dnaExplorerJsonKeysForTable(tableName);
  return keys.some((k) => nz(fields[k]));
}

/** Tab labels for verification docs. */
export const DNA_JSON_FIELD_TO_TAB = Object.fromEntries(
  DNA_EXPLORER_JSON_FIELD_SPECS.map((s) => {
    let tab = "DNA";
    if (s.setupTab.includes("Operating Platform")) tab = "Operating Platform";
    else if (s.setupTab.includes("Brand")) tab = "Brand & Relationships";
    else if (s.setupTab.includes("Markets")) tab = "Markets & Footprint";
    else if (s.setupTab.includes("Owner Value")) tab = "Owner Engagement & Reporting";
    else if (s.setupTab.includes("Best Fit")) tab = "Project Fit & Deal Profile";
    return [s.formKey, tab];
  })
);

export { brandExplorerSeedMarkerKeys, ENGAGEMENT_JSON_FIELD_KEYS, DNA_EXPLORER_JSON_TABLE };
