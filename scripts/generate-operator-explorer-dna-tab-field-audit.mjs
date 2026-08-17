#!/usr/bin/env node
/**
 * Operator Explorer DNA — tab-by-tab UI field audit vs Airtable bindings.
 * Outputs:
 *   docs/operator-explorer-dna-tab-field-audit.md
 *   reports/operator-explorer-dna-ui-field-registry.csv
 *   reports/operator-setup-fields-unused-by-dna-explorer.csv
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BASICS_AIRTABLE_TO_FORM_KEY } from "../api/lib/third-party-operator-basics-to-prefill.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const BINDINGS_PATH = path.join(ROOT, "api/lib/third-party-operator-new-two-field-bindings.json");
const SETUP_AUDIT_CSV = path.join(ROOT, "reports/operator-setup-to-explorer-field-mapping-audit.csv");

const DNA_CODE_GLOBS = [
  "public/js/operator-explorer-gold-mock-data.js",
  "public/js/operator-dna-profile.js",
  "public/js/operator-dna-profile-mount.js",
  "public/js/operator-dna-profile-consolidate.js",
  "public/js/operator-dna-view-model.js",
  "public/js/operator-dna-materials.js",
  "public/js/operator-dna-dealality-insights.js",
  "public/js/operator-dna-case-studies-be.js",
  "public/js/operator-operating-platform-sections.js",
  "public/js/operator-brand-relationships-sections.js",
  "public/js/operator-engagement-reporting-sections.js",
  "public/js/operator-infrastructure-sections.js",
  "public/js/operator-leadership-team-sections.js",
  "public/js/operator-markets-footprint-sections.js",
  "public/js/operator-market-experience-section.js",
  "public/js/operator-best-fit-deal-profile-sections.js",
  "public/js/operator-leadership-profile-detail.js",
  "public/js/operator-leadership-member-map.js",
];

const DNA_TAB_ALIASES = [
  "Profile & Positioning",
  "Operating Platform",
  "Brand & Relationships",
  "Markets & Footprint",
  "Owner Engagement",
  "Owner Value",
  "Infrastructure",
  "Risk",
  "Leadership",
  "Best Fit",
  "Project Fit",
  "Proof",
  "Track Record",
  "Materials",
  "Dealality",
];

/** formKeys / JSON keys referenced in DNA Explorer JS (regex scan). */
function scanDnaCodeKeys() {
  const keys = new Set();
  const rePick = /pick\s*\(\s*ex\s*,\s*p\s*,\s*["']([a-zA-Z0-9_]+)["']/g;
  const rePrefill = /prefill\.([a-zA-Z0-9_]+)/g;
  const reJson = /["']([a-z][a-z0-9_]*_json)["']/g;
  const reField = /FIELD\.[a-zA-Z]+|\b([a-z][a-z0-9_]*_json)\b/g;
  for (const rel of DNA_CODE_GLOBS) {
    const fp = path.join(ROOT, rel);
    if (!fs.existsSync(fp)) continue;
    const src = fs.readFileSync(fp, "utf8");
    let m;
    while ((m = rePick.exec(src))) keys.add(m[1]);
    rePick.lastIndex = 0;
    while ((m = rePrefill.exec(src))) keys.add(m[1]);
    while ((m = reJson.exec(src))) keys.add(m[1]);
    while ((m = reField.exec(src))) {
      if (m[1] && m[1].endsWith("_json")) keys.add(m[1]);
    }
  }
  keys.add("explorerProfileJson");
  keys.add("leadershipTeam");
  keys.add("caseStudiesDetail");
  keys.add("ownerDiligenceQa");
  return keys;
}

function basicsFormKeyForAirtable(airtableField) {
  return BASICS_AIRTABLE_TO_FORM_KEY[airtableField] || null;
}

function isDnaExplorerSection(sectionTab) {
  const s = (sectionTab || "").toLowerCase();
  return DNA_TAB_ALIASES.some((t) => s.includes(t.toLowerCase()));
}

/** Registry: Explorer DNA tab → subsections → UI fields (code truth, 2026-06). */
const DNA_UI_REGISTRY = [
  {
    tab: "Profile & Positioning",
    subsections: [
      {
        name: "Operator Quick Facts",
        fields: [
          { key: "yearEstablished", binding: null, airtable: "Year Established", type: "scalar", notes: "fields map + prefill" },
          { key: "website", binding: null, airtable: "Website", type: "scalar" },
          { key: "totalEmployees / companySize", binding: null, airtable: "Company Size", type: "scalar" },
          { key: "totalProperties, totalRooms, minPropertySize, maxPropertySize", binding: null, airtable: "—", type: "derived", notes: "avg hotel size calculated" },
          { key: "primaryServiceModel", binding: null, airtable: "Primary Service Model", type: "scalar" },
          { key: "managementStructuresSupported", binding: "managementStructuresSupported", type: "multiselect" },
          { key: "typicalAgreement", binding: null, type: "prefill-only" },
          { key: "dataConfidenceLevel", binding: "dataConfidenceLevel", type: "singleSelect" },
        ],
      },
      {
        name: "Company Story & Positioning",
        fields: [
          { key: "companyHistory", binding: null, type: "prefill-only" },
          { key: "differentiators", binding: null, type: "prefill-only" },
          { key: "managementPhilosophy", binding: null, type: "prefill-only" },
          { key: "missionStatement", binding: null, type: "prefill-only" },
        ],
      },
      {
        name: "What They Are Best At",
        fields: [
          { key: "overview_bestat_1_headline", binding: "skipped", type: "explorerProfileJson", notes: "bindings.skipped" },
          { key: "overview_bestat_1_story", binding: "skipped", type: "explorerProfileJson" },
          { key: "overview_bestat_2_*", binding: "skipped", type: "explorerProfileJson" },
          { key: "overview_bestat_3_*", binding: "skipped", type: "explorerProfileJson" },
        ],
      },
      {
        name: "Why Owners Consider This Operator",
        fields: [
          { key: "overview_why_1_headline", binding: "skipped", type: "explorerProfileJson" },
          { key: "overview_why_1_story", binding: "skipped", type: "explorerProfileJson" },
          { key: "overview_why_2_*", binding: "skipped", type: "explorerProfileJson" },
          { key: "overview_why_3_*", binding: "skipped", type: "explorerProfileJson" },
        ],
      },
      {
        name: "Best-Fit Owner / Project Profile (overview)",
        fields: [
          { key: "bf_fit_criteria_json", binding: null, type: "JSON", notes: "TODO binding" },
          { key: "bestFitGeographies, priorityMarkets", binding: null, type: "multiselect/scalar" },
          { key: "bf_selected_asset_types", binding: "bf_selected_asset_types", type: "multiselect" },
          { key: "bestFitOwnerTypes", binding: null, type: "prefill-only" },
          { key: "bf_selected_situation_types", binding: "bf_selected_situation_types", type: "multiselect" },
          { key: "bf_not_ideal_for", binding: "bf_not_ideal_for", type: "multiline/multiselect" },
        ],
      },
      {
        name: "Leadership Snapshot",
        fields: [
          { key: "leadershipTeam[]", binding: null, type: "child-table", notes: "Operator Setup - Leadership Team Members" },
        ],
      },
      {
        name: "Recognition",
        fields: [
          { key: "certifications", binding: null, airtable: "Certifications", type: "scalar" },
          { key: "industryRecognition", binding: null, type: "prefill-only" },
          { key: "achievements / notableAchievements", binding: null, type: "prefill-only" },
        ],
      },
    ],
  },
  {
    tab: "Operating Platform",
    subsections: [
      {
        name: "Operating Platform (KPI snapshot)",
        fields: [
          { key: "revenueManagementCapability", binding: "revenueManagementCapability", type: "singleSelect" },
          { key: "ownerReportingLevel", binding: "ownerReportingLevel", type: "singleSelect" },
          { key: "cap_kpi_reporting", binding: "cap_kpi_reporting", type: "singleSelect", notes: "fallback KPI" },
          { key: "preOpeningSupportCapability", binding: "preOpeningSupportCapability", type: "singleSelect" },
          { key: "cap_kpi_transition", binding: "cap_kpi_transition", type: "singleSelect", notes: "fallback KPI" },
          { key: "conversionReflagExperience", binding: "conversionReflagExperience", type: "singleSelect" },
          { key: "fbCapabilityLevel / fBCapabilityLevel", binding: "fbCapabilityLevel", type: "singleSelect" },
        ],
      },
      {
        name: "Commercial Engine (pillar tiles)",
        fields: [
          { key: "op_commercial_engine_json", binding: null, type: "JSON", notes: "items[].title + description; TODO Airtable" },
          { key: "cap_profile_commercial", binding: "cap_profile_commercial", type: "multiline", notes: "fallback lines → tiles" },
          { key: "offeredServices", binding: "offeredServices", type: "multiselect", notes: "fallback tile titles only" },
        ],
      },
      {
        name: "Owner Reporting & Communication",
        fields: [
          { key: "op_owner_reporting_json", binding: null, type: "JSON", notes: "TODO Airtable" },
          { key: "cap_card_governance", binding: "cap_card_governance", type: "multiline", notes: "fallback" },
        ],
      },
      {
        name: "Pre-Opening & Transition Support",
        fields: [
          { key: "op_preopening_transition_json", binding: null, type: "JSON", notes: "TODO Airtable" },
          { key: "cap_profile_transition", binding: "cap_profile_transition", type: "multiline", notes: "fallback" },
        ],
      },
      {
        name: "Conversion & Repositioning",
        fields: [
          { key: "op_conversion_repositioning_json", binding: null, type: "JSON", notes: "TODO Airtable" },
          { key: "cap_deep_revenue_systems", binding: "cap_deep_revenue_systems", type: "multiline", notes: "fallback" },
        ],
      },
      {
        name: "F&B, Lifestyle & Resort Capability",
        fields: [
          { key: "op_fb_lifestyle_resort_json", binding: null, type: "JSON", notes: "TODO Airtable" },
          { key: "cap_card_service_diff", binding: "cap_card_service_diff", type: "multiline", notes: "fallback" },
        ],
      },
    ],
  },
  {
    tab: "Brand & Relationships",
    subsections: [
      {
        name: "Brand & Relationship Snapshot (KPIs)",
        fields: [
          { key: "numberOfBrands, brands[]", binding: null, type: "derived" },
          { key: "brand_portfolio_mix_json", binding: null, type: "JSON", notes: "TODO binding" },
          { key: "brandedVsIndependentMix", binding: "brandedVsIndependentMix", type: "scalar" },
          { key: "brand_conversion_project_count", binding: null, type: "scalar", notes: "TODO binding" },
          { key: "brand_signal_reflag", binding: "brand_signal_reflag", type: "singleSelect" },
          { key: "brand_relationship_depth_json", binding: null, type: "JSON" },
        ],
      },
      {
        name: "Portfolio Mix by Brand / Flag Type",
        fields: [{ key: "brand_portfolio_mix_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Brands & Relationship Depth",
        fields: [{ key: "brand_relationship_depth_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Brand Execution Capabilities",
        fields: [{ key: "brand_execution_capabilities_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Brand Governance & Compliance Support",
        fields: [{ key: "brand_governance_compliance_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Soft Brand / Independent Experience",
        fields: [{ key: "brand_soft_independent_narrative", binding: null, type: "scalar/JSON" }],
      },
    ],
  },
  {
    tab: "Markets & Footprint",
    subsections: [
      {
        name: "Markets & Footprint (KPI snapshot)",
        fields: [
          { key: "regions, regionsSupported", binding: "regionsSupported", type: "multiselect" },
          { key: "activeCountries", binding: "activeCountries", type: "multiselect" },
          { key: "activeMarkets", binding: "activeMarkets", type: "multiselect" },
          { key: "numberOfMarkets", binding: null, type: "scalar" },
          { key: "mkt_signal_years", binding: "mkt_signal_years", type: "singleSelect" },
          { key: "mkt_signal_gateway", binding: "mkt_signal_gateway", type: "singleSelect" },
          { key: "mkt_signal_mix", binding: "mkt_signal_mix", type: "singleSelect" },
        ],
      },
      {
        name: "Three-Layer Market Experience",
        fields: [
          { key: "activeCountries, activeMarkets, teamExperienceMarkets, targetGrowthMarkets", binding: "activeCountries, activeMarkets", type: "multiselect" },
          { key: "marketExperience.*", binding: null, type: "derived", notes: "DNA vm layers" },
        ],
      },
      {
        name: "Local / Regional Expertise",
        fields: [{ key: "mkt_regional_expertise_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Market Fit Signals",
        fields: [{ key: "mkt_market_fit_signals_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Footprint Metrics",
        fields: [
          { key: "geo_*", binding: null, type: "derived", notes: "tier/region footprint; not in bindings" },
          { key: "brandsPortfolioDetail", binding: null, type: "prefill-only" },
        ],
      },
    ],
  },
  {
    tab: "Owner Engagement & Reporting",
    subsections: [
      {
        name: "Engagement & Reporting (KPI snapshot)",
        fields: [
          { key: "ownerReportingLevel", binding: "ownerReportingLevel", type: "singleSelect" },
          { key: "reportingFrequency / ownerReportingCadence", binding: null, type: "prefill-only" },
          { key: "ownerResponseTime", binding: null, type: "prefill-only" },
          { key: "ov_q_touchpoints", binding: "ov_q_touchpoints", type: "scalar" },
          { key: "reportTypes", binding: null, type: "multiselect" },
          { key: "ownerPortalFeatures", binding: null, type: "prefill-only" },
        ],
      },
      {
        name: "Strategic Owner Value",
        fields: [{ key: "ov_strategic_owner_value_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Owner Engagement Cadence",
        fields: [{ key: "ov_engagement_cadence_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Controls & Governance",
        fields: [{ key: "ov_controls_governance_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Reports Owners Receive",
        fields: [{ key: "ov_reports_received_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Owner Tools & Support Channels",
        fields: [{ key: "ov_owner_tools_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Owner Support Across the Asset Lifecycle",
        fields: [{ key: "ov_lifecycle_support_json", binding: null, type: "JSON default" }],
      },
    ],
  },
  {
    tab: "Infrastructure & Data",
    subsections: [
      {
        name: "Infrastructure & Data (KPI snapshot)",
        fields: [
          { key: "infra_signal_uptime", binding: "infra_signal_uptime", type: "singleSelect" },
          { key: "infra_signal_incident", binding: "infra_signal_incident", type: "singleSelect" },
          { key: "infra_signal_adoption", binding: "infra_signal_adoption", type: "singleSelect" },
          { key: "infra_signal_refresh", binding: "infra_signal_refresh", type: "singleSelect" },
          { key: "risk_signal_audit", binding: "risk_signal_audit", type: "singleSelect" },
          { key: "risk_signal_bcp", binding: "risk_signal_bcp", type: "singleSelect" },
          { key: "risk_signal_control", binding: "risk_signal_control", type: "singleSelect" },
          { key: "risk_signal_insurance", binding: "risk_signal_insurance", type: "singleSelect" },
        ],
      },
      {
        name: "Technology Platform Stack",
        fields: [{ key: "infra_technology_stack_json", binding: "infra_technology_stack_json", type: "JSON default" }],
      },
      {
        name: "Infrastructure Services Offered",
        fields: [{ key: "infra_services_offered_json", binding: "infra_services_offered_json", type: "JSON default" }],
      },
      {
        name: "Data Domains Captured",
        fields: [{ key: "infra_data_domains_json", binding: "infra_data_domains_json", type: "JSON default" }],
      },
      {
        name: "Data Governance, Security & Controls",
        fields: [{ key: "infra_data_governance_json", binding: "infra_data_governance_json", type: "JSON default" }],
      },
      {
        name: "Analytics & Decision Support",
        fields: [{ key: "infra_analytics_support_json", binding: "infra_analytics_support_json", type: "JSON default" }],
      },
      {
        name: "Technology Maturity View",
        fields: [
          { key: "infra_technology_maturity_level", binding: "infra_technology_maturity_level", type: "singleSelect" },
          { key: "infra_technology_maturity_json", binding: null, type: "JSON default" },
        ],
      },
    ],
  },
  {
    tab: "Leadership",
    subsections: [
      {
        name: "Leadership Snapshot (KPIs)",
        fields: [
          { key: "lead_avg_hospitality_experience", binding: "lead_avg_hospitality_experience", type: "singleSelect" },
          { key: "lead_signal_tenure", binding: "lead_signal_tenure", type: "singleSelect" },
          { key: "lead_signal_crossbrand", binding: "lead_signal_crossbrand", type: "singleSelect" },
          { key: "lead_language_capability_json", binding: "Leadership Languages (JSON)", type: "JSON" },
        ],
      },
      {
        name: "Leadership Profiles",
        fields: [{ key: "leadershipTeam[]", binding: null, type: "child-table" }],
      },
      {
        name: "Organization Structure",
        fields: [{ key: "lead_org_structure_json", binding: "Leadership Org Structure (JSON)", type: "JSON" }],
      },
      {
        name: "Team Depth by Function",
        fields: [{ key: "lead_team_depth_json", binding: "Leadership Team Depth (JSON)", type: "JSON" }],
      },
      {
        name: "Language & Regional Capability",
        fields: [{ key: "lead_language_capability_json", binding: "Leadership Languages (JSON)", type: "JSON" }],
      },
      {
        name: "Governance & Communication Cadence",
        fields: [{ key: "lead_governance_cadence_json", binding: "Leadership Governance Cadence (JSON)", type: "JSON" }],
      },
      {
        name: "Team Experience Markets",
        fields: [{ key: "lead_team_markets_json", binding: "Leadership Team Markets (JSON)", type: "JSON" }],
      },
      {
        name: "Owner Relationship Model",
        fields: [{ key: "lead_owner_relationship_json", binding: "Leadership Owner Relationship (JSON)", type: "JSON" }],
      },
    ],
  },
  {
    tab: "Project Fit & Deal Profile",
    subsections: [
      {
        name: "Project Fit Snapshot (KPIs)",
        fields: [
          { key: "minPropertySize, maxPropertySize", binding: null, type: "scalar" },
          { key: "bf_signal_dealsize", binding: "bf_signal_dealsize", type: "singleSelect" },
          { key: "bf_signal_transition", binding: "bf_signal_transition", type: "singleSelect" },
        ],
      },
      {
        name: "Operator Fit Criteria",
        fields: [{ key: "bf_fit_criteria_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Best-Fit Project Types",
        fields: [{ key: "bf_best_fit_project_types_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Preferred Deal Profile",
        fields: [{ key: "bf_preferred_deal_profile_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Evaluation Path",
        fields: [{ key: "bf_evaluation_path_json", binding: null, type: "JSON default" }],
      },
      {
        name: "Potential Red Flags",
        fields: [{ key: "bf_red_flags_json", binding: null, type: "JSON default" }],
      },
    ],
  },
  {
    tab: "Proof & Track Record",
    subsections: [
      {
        name: "Proof & Track Record (KPIs)",
        fields: [
          { key: "totalProperties", binding: null, type: "scalar" },
          { key: "numberOfMarkets", binding: null, type: "scalar" },
          { key: "yearsInBusiness", binding: null, type: "scalar" },
          { key: "ownerReferences", binding: null, type: "scalar" },
          { key: "lenderReferences", binding: null, type: "scalar" },
        ],
      },
      {
        name: "Case Studies",
        fields: [{ key: "caseStudiesDetail[]", binding: null, type: "child-table" }],
      },
      {
        name: "Owner Diligence Highlights",
        fields: [{ key: "ownerDiligenceQa[]", binding: null, type: "child-table" }],
      },
      {
        name: "Decision Signals",
        fields: [
          { key: "tr_signal_revpar", binding: "tr_signal_revpar", type: "singleSelect" },
          { key: "tr_signal_occ", binding: "tr_signal_occ", type: "singleSelect" },
          { key: "tr_signal_adr", binding: "tr_signal_adr", type: "singleSelect" },
          { key: "tr_signal_repeat", binding: "tr_signal_repeat", type: "singleSelect" },
        ],
      },
    ],
  },
];

function loadBindings() {
  const raw = JSON.parse(fs.readFileSync(BINDINGS_PATH, "utf8"));
  const byFormKey = new Map();
  const byAirtable = new Map();
  for (const b of raw.bindings || []) {
    for (const fk of b.formKeys || []) {
      byFormKey.set(fk, b);
    }
    if (b.airtableName) byAirtable.set(b.airtableName, b);
  }
  return { raw, byFormKey, byAirtable };
}

function bindingForField(field, byFormKey) {
  if (field.binding === "skipped") return { skipped: true };
  const key =
    field.binding && field.binding !== "skipped"
      ? field.binding.split(/[\s/,]+/)[0]
      : field.key.split(/[\s/,]+/)[0];
  if (key.endsWith("_json") || key.includes("[]")) {
    const b = byFormKey.get(key);
    if (b) return b;
  }
  if (!field.binding) return null;
  const b = byFormKey.get(field.binding.split(/[\s/,]+/)[0]);
  return b || null;
}

function parseCsv(path) {
  const text = fs.readFileSync(path, "utf8");
  const lines = text.trim().split(/\r?\n/);
  const parseLine = (line) => {
    const out = [];
    let cur = "";
    let q = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (q) {
        if (c === '"' && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else if (c === '"') q = false;
        else cur += c;
      } else if (c === '"') q = true;
      else if (c === ",") {
        out.push(cur);
        cur = "";
      } else cur += c;
    }
    out.push(cur);
    return out;
  };
  const header = parseLine(lines[0]);
  const rows = lines.slice(1).map((line) => {
    const cells = parseLine(line);
    const o = {};
    header.forEach((h, i) => {
      o[h] = cells[i] || "";
    });
    return o;
  });
  return { header, rows };
}

function collectUiKeys() {
  const keys = new Set();
  for (const tab of DNA_UI_REGISTRY) {
    for (const sub of tab.subsections) {
      for (const f of sub.fields) {
        f.key.split(/[,/]+/).forEach((k) => {
          const t = k.trim().replace(/\s+/g, "");
          if (t && !t.includes("*") && !t.includes("[]")) keys.add(t.split(/\s/)[0]);
        });
      }
    }
  }
  return keys;
}

function resolveLinkStatus(field, b, basicsFk) {
  if (b && b.skipped) return "By design (explorerProfileJson blob)";
  if (field.type === "child-table") return "Linked (child table)";
  if (field.type === "derived") return "Calculated";
  if (b && !b.skipped) return "Linked (new-base binding)";
  if (basicsFk) return "Linked (Basics → prefill)";
  if (field.type === "JSON default" && field.binding) return "Linked (JSON binding)";
  if (field.type === "explorerProfileJson" || field.binding === "skipped") {
    return "By design (explorerProfileJson)";
  }
  if (field.type === "JSON" && !b) return "GAP — add Airtable column + binding";
  if (field.type === "prefill-only") return "Partial — prefill only (no new-base binding)";
  return "GAP — not in bindings or Basics map";
}

function main() {
  const { raw: bindingsRaw, byFormKey, byAirtable } = loadBindings();
  const uiKeys = collectUiKeys();
  const codeKeys = scanDnaCodeKeys();
  for (const k of codeKeys) uiKeys.add(k);
  const boundKeys = new Set((bindingsRaw.bindings || []).flatMap((b) => b.formKeys || []));
  const basicsByForm = new Map(Object.entries(BASICS_AIRTABLE_TO_FORM_KEY).map(([a, f]) => [f, a]));

  let setupRows = [];
  if (fs.existsSync(SETUP_AUDIT_CSV)) {
    setupRows = parseCsv(SETUP_AUDIT_CSV).rows;
  }

  const registryRows = [];
  let gapCount = 0;
  let linkedCount = 0;

  const md = [];
  md.push("# Operator Explorer DNA — tab-by-tab field audit");
  md.push("");
  md.push("Generated by `scripts/generate-operator-explorer-dna-tab-field-audit.mjs`.");
  md.push("");
  md.push("**Purpose:** Confirm every DNA UI element maps to Operator Setup / Airtable, document select options, flag gaps, and list Setup fields not used on DNA tabs.");
  md.push("");
  md.push("**Related artifacts:**");
  md.push("- [operator-setup-to-explorer-field-mapping-audit.md](./operator-setup-to-explorer-field-mapping-audit.md)");
  md.push("- [reports/operator-setup-to-explorer-field-mapping-audit.csv](../reports/operator-setup-to-explorer-field-mapping-audit.csv)");
  md.push("- Bindings source: `api/lib/third-party-operator-new-two-field-bindings.json`");
  md.push("");
  md.push("---");
  md.push("");
  md.push("## How data reaches the UI");
  md.push("");
  md.push("```text");
  md.push("Airtable (new-base tables) → GET /api/intake/third-party-operators/:id");
  md.push("  → prefill (camelCase) + fields (Airtable column titles) + child rows");
  md.push("  → mergeExplorerPrefill → vm.ex");
  md.push("  → buildPanels(vm) + section modules");
  md.push("```");
  md.push("");
  md.push("| Data type | Owner sees | Setup change reflected? |");
  md.push("|-----------|------------|-------------------------|");
  md.push("| **singleSelect / scalar** with binding | KPI value or text | **Yes** when saved to new-base |");
  md.push("| **multiselect** | Lists, chips, filters | **Yes** when writer persists |");
  md.push("| **JSON subsection** (`*_json`, `op_*`) | Tables/cards with defaults until filled | **Yes** when column populated |");
  md.push("| **JSON default only** (no Airtable yet) | Demo/default copy | **No** until Airtable + intake wired |");
  md.push("| **child table** | Leadership, case studies, diligence | **Yes** on child replace |");
  md.push("| **derived** | Counts, compact labels, % | Recalculates from source fields |");
  md.push("| **explorerProfileJson** / skipped | Overview cards | Only if JSON blob updated |");
  md.push("");
  md.push("---");
  md.push("");

  for (const tab of DNA_UI_REGISTRY) {
    md.push(`## ${tab.tab}`);
    md.push("");

    for (const sub of tab.subsections) {
      md.push(`### ${sub.name}`);
      md.push("");
      md.push("| UI label / key | Airtable field | Binding formKey | Field type | Select options | Link status | Notes |");
      md.push("|----------------|----------------|-----------------|------------|----------------|-------------|-------|");

      for (const f of sub.fields) {
        const b = bindingForField(f, byFormKey);
        let airtable = f.airtable || (b && b.airtableName) || "—";
        let formKey = f.binding || (b && (b.formKeys || [])[0]) || "—";
        let options = "—";
        if (b && b.skipped) {
          options = "skipped (explorerProfileJson)";
        } else if (b && b.selectOptions && b.selectOptions.length) {
          options = b.selectOptions.join("; ");
        }
        const firstKey = f.binding && f.binding !== "skipped" ? f.binding.split(/[\s/,]+/)[0] : f.key.split(/[\s/,]+/)[0];
        const basicsFk = basicsByForm.has(firstKey) ? firstKey : basicsFormKeyForAirtable(airtable);
        const status = resolveLinkStatus(f, b, basicsFk);
        if (status.startsWith("Linked") || status.startsWith("By design") || status === "Calculated") {
          if (!status.includes("GAP")) linkedCount++;
        } else {
          gapCount++;
        }

        md.push(
          `| ${f.key} | ${airtable} | ${formKey} | ${f.type} | ${options.length > 80 ? options.slice(0, 77) + "…" : options} | ${status} | ${f.notes || ""} |`
        );

        registryRows.push({
          tab: tab.tab,
          subsection: sub.name,
          uiKey: f.key,
          airtableField: airtable,
          formKey,
          fieldType: f.type,
          selectOptions: b && b.selectOptions ? b.selectOptions.join("|") : "",
          linkStatus: status,
          notes: f.notes || "",
        });
      }
      md.push("");
    }
  }

  md.push("---");
  md.push("");
  md.push("## Summary counts (UI registry)");
  md.push("");
  md.push(`- Registry rows: **${registryRows.length}**`);
  md.push(`- Direct binding links: **${linkedCount}**`);
  md.push(`- Gaps / prefill-only / TODO JSON: **${gapCount}** (approx.)`);
  md.push("");

  md.push("## JSON subsection fields (registry)");
  md.push("");
  md.push("All 22 DNA JSON keys are in `lib/operator-dna-explorer-json-fields.js`, bindings, build sheet, and Setup form. See [operator-dna-explorer-json-fields.md](./operator-dna-explorer-json-fields.md).");
  md.push("");
  md.push("## Former priority gaps (resolved 2026-06)");
  md.push("");
  const gapKeys = [
    "op_commercial_engine_json",
    "op_owner_reporting_json",
    "op_preopening_transition_json",
    "op_conversion_repositioning_json",
    "op_fb_lifestyle_resort_json",
    "brand_portfolio_mix_json",
    "brand_relationship_depth_json",
    "brand_execution_capabilities_json",
    "brand_governance_compliance_json",
    "ov_strategic_owner_value_json",
    "ov_engagement_cadence_json",
    "ov_controls_governance_json",
    "ov_reports_received_json",
    "ov_owner_tools_json",
    "ov_lifecycle_support_json",
    "mkt_regional_expertise_json",
    "mkt_market_fit_signals_json",
    "bf_fit_criteria_json",
    "bf_best_fit_project_types_json",
    "bf_preferred_deal_profile_json",
    "bf_evaluation_path_json",
    "bf_red_flags_json",
  ];
  for (const k of gapKeys) {
    md.push(`- \`${k}\``);
  }
  md.push("");

  md.push("## Code-scanned DNA consumer keys");
  md.push("");
  md.push(`Keys referenced in DNA JS modules: **${codeKeys.size}** (see \`reports/operator-explorer-dna-code-keys.csv\`).`);
  md.push("");

  md.push("## Operator Setup → DNA: fields **not displayed** (delete review)");
  md.push("");
  md.push("From setup audit CSV: rows where Explorer is not Yes/Partial for a DNA tab, or **not referenced in DNA JS**.");
  md.push("**Do not delete** without checking OAS, Strategy, Setup form, and new-base writer.");
  md.push("");

  const unusedBindingRows = [];
  for (const fk of boundKeys) {
    if (!codeKeys.has(fk) && !uiKeys.has(fk) && !fk.endsWith("Other")) {
      const b = byFormKey.get(fk);
      unusedBindingRows.push({
        source: "binding-not-in-dna-code",
        formKey: fk,
        airtableField: b?.airtableName || "",
        tableKey: b?.tableKey || "",
        fieldType: b?.fieldType || "",
        setupTab: "",
        displayedExplorer: "",
        newBaseWriter: "",
        selectOptions: (b?.selectOptions || []).join("|"),
        recommendation: "Binding exists; DNA does not read — safe to review for deprecation",
      });
    }
  }

  const setupNotDnaRows = [];
  for (const row of setupRows) {
    const formName = row["Operator Setup Form Field Name"] || "";
    const displayed = row["Displayed in Operator Explorer?"] || "";
    const section = row["Operator Explorer Section / Tab"] || "";
    const coverage = row["Coverage Status"] || "";
    const system = row["System / Derived / Admin-only?"] || "";
    if (system.startsWith("Yes")) continue;
    if (displayed === "Yes" || displayed === "Partial") {
      if (isDnaExplorerSection(section) && formName && codeKeys.has(formName)) continue;
    }
    const inCode = formName && codeKeys.has(formName);
    const inBindings = formName && boundKeys.has(formName);
    if (displayed === "No" && !inCode && coverage !== "System Field") {
      setupNotDnaRows.push({
        source: "setup-audit-not-displayed",
        formKey: formName,
        airtableField: row["Airtable Field"] || "",
        tableKey: row["Airtable Table"] || "",
        fieldType: row["Airtable Field Type"] || "",
        setupTab: row["Operator Setup UI Tab"] || "",
        displayedExplorer: displayed,
        newBaseWriter: row["New-Base Writer Mapped?"] || "",
        selectOptions: row["Live Options"] || "",
        recommendation:
          inBindings && !inCode
            ? "Bound in intake but DNA UI does not consume — deprecate after OAS check"
            : "Setup field not shown on DNA — deprecate if not used elsewhere",
      });
    }
  }

  const unusedRows = [...unusedBindingRows, ...setupNotDnaRows.filter((r) => r.formKey)];
  unusedRows.sort((a, b) => (a.setupTab || "").localeCompare(b.setupTab || "") || a.formKey.localeCompare(b.formKey));

  md.push("| Setup tab | formKey | Airtable field | Displayed? | New-base writer? | Recommendation |");
  md.push("|-----------|---------|----------------|------------|------------------|----------------|");
  for (const r of unusedRows.slice(0, 80)) {
    md.push(
      `| ${r.setupTab || "—"} | ${r.formKey} | ${r.airtableField} | ${r.displayedExplorer || "—"} | ${r.newBaseWriter || "—"} | ${r.recommendation.slice(0, 60)} |`
    );
  }
  if (unusedRows.length > 80) md.push(`| … | +${unusedRows.length - 80} more | see CSV | | | |`);
  md.push("");

  md.push("## Removed from DNA UI (still in Setup / bindings)");
  md.push("");
  md.push("- Operating Platform: `cap_kpi_operating_model`, `cap_kpi_execution_strength`, `cap_profile_operational`, `cap_card_*` (except fallbacks), `cap_signal_*`, Decision Signals strip on Operating tab");
  md.push("- Many `ov_card_*` narrative fields → replaced by `ov_*_json` subsection modules");
  md.push("");

  md.push("## Manual QA checklist");
  md.push("");
  md.push("1. Pick one live `rec…` with full Setup data.");
  md.push("2. For each tab, change one **singleSelect** in Airtable → refresh DNA → KPI must update.");
  md.push("3. Populate one `op_*_json` test payload → pillar tiles must show custom titles/descriptions.");
  md.push("4. Clear a JSON subsection field → UI should show empty state, not stale defaults (except JSON-default modules until data exists).");
  md.push("");

  const outMd = path.join(ROOT, "docs/operator-explorer-dna-tab-field-audit.md");
  fs.writeFileSync(outMd, md.join("\n"));

  const regCsv = path.join(ROOT, "reports/operator-explorer-dna-ui-field-registry.csv");
  const regHeader = Object.keys(registryRows[0] || { tab: "", subsection: "" }).join(",");
  fs.writeFileSync(
    regCsv,
    [regHeader, ...registryRows.map((r) => Object.values(r).map((v) => `"${String(v).replace(/"/g, '""')}"`).join(","))].join("\n")
  );

  const unusedCsv = path.join(ROOT, "reports/operator-setup-fields-unused-by-dna-explorer.csv");
  const uHeader =
    "source,setupTab,formKey,airtableField,tableKey,fieldType,displayedExplorer,newBaseWriter,selectOptions,recommendation";
  fs.writeFileSync(
    unusedCsv,
    [
      uHeader,
      ...unusedRows.map((r) =>
        [
          r.source,
          r.setupTab,
          r.formKey,
          r.airtableField,
          r.tableKey,
          r.fieldType,
          r.displayedExplorer,
          r.newBaseWriter,
          r.selectOptions,
          r.recommendation,
        ]
          .map((v) => `"${String(v).replace(/"/g, '""')}"`)
          .join(",")
      ),
    ].join("\n")
  );

  const codeKeysCsv = path.join(ROOT, "reports/operator-explorer-dna-code-keys.csv");
  const codeRows = [...codeKeys].sort().map((k) => {
    const b = byFormKey.get(k);
    const basicsA = basicsByForm.get(k) || "";
    return {
      formKey: k,
      inBindings: b ? "Yes" : "No",
      airtableField: b?.airtableName || basicsA,
      fieldType: b?.fieldType || (k.endsWith("_json") ? "json" : ""),
      selectOptions: (b?.selectOptions || []).join("|"),
    };
  });
  fs.writeFileSync(
    codeKeysCsv,
    [
      "formKey,inBindings,airtableField,fieldType,selectOptions",
      ...codeRows.map((r) =>
        [r.formKey, r.inBindings, r.airtableField, r.fieldType, r.selectOptions]
          .map((v) => `"${String(v).replace(/"/g, '""')}"`)
          .join(",")
      ),
    ].join("\n")
  );

  console.log("Wrote", outMd);
  console.log("Wrote", regCsv, "(" + registryRows.length + " rows)");
  console.log("Wrote", unusedCsv, "(" + unusedRows.length + " delete-review rows)");
  console.log("Wrote", codeKeysCsv, "(" + codeKeys.size + " code keys)");
}

main();
