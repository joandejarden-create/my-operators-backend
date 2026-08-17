/**
 * Realistic Infrastructure & Data Explorer seed payloads (Operator Setup — Governance table).
 * Field names match operator-infrastructure-sections.js / prefill keys.
 */

export const MATURITY_LEVELS = ["Basic", "Structured", "Integrated", "Advanced"];

const BASE_STACK = [
  {
    title: "PMS / Property Operations",
    description:
      "Property management, reservations, guest profile, folios, and operational workflows.",
    examples: "Opera, Cloudbeds, Mews, Infor, or equivalent",
  },
  {
    title: "RMS / Revenue Management",
    description: "Pricing, demand forecasting, yielding, restrictions, pace, and optimization.",
    examples: "Duetto, IDeaS, Atomize, or equivalent",
  },
  {
    title: "CRS / Distribution",
    description:
      "Central reservations, connectivity, brand distribution, channel management, and inventory controls.",
    examples: "Brand CRS, SynXis, SiteMinder, DerbySoft, or equivalent",
  },
  {
    title: "CRM / Guest Data",
    description:
      "Guest profiles, marketing segmentation, loyalty capture, lifecycle marketing, and direct booking support.",
    examples: "Salesforce, Revinate, Cendyn, HubSpot, or equivalent",
  },
  {
    title: "BI / Dashboards",
    description:
      "Owner dashboards, operating KPIs, commercial analytics, financial reporting, and portfolio views.",
    examples: "Power BI, Tableau, Looker, Domo, or equivalent",
  },
  {
    title: "Accounting / ERP",
    description:
      "Financial reporting, AP/AR, budget tracking, owner statements, and controls.",
    examples: "Sage, NetSuite, M3, Oracle, or equivalent",
  },
];

const BASE_SERVICES = [
  {
    title: "Systems Selection Support",
    description: "Can help owners evaluate, select, and transition key hotel systems.",
  },
  {
    title: "Implementation Coordination",
    description:
      "Coordinates PMS, RMS, CRS, POS, finance, labor, and reporting setup during opening or transition.",
  },
  {
    title: "Data Room / Document Setup",
    description:
      "Creates organized repositories for contracts, reports, capex files, brand documents, and transition materials.",
  },
  {
    title: "Owner Dashboard Configuration",
    description:
      "Configures dashboards around owner priorities, asset KPIs, portfolio reporting, and action items.",
  },
  {
    title: "Data Quality Review",
    description:
      "Reviews source data completeness, reporting consistency, coding, mappings, and KPI definitions.",
  },
  {
    title: "Integration Planning",
    description:
      "Identifies required system integrations and data flows before go-live or takeover.",
  },
];

const BASE_DOMAINS = [
  {
    title: "Commercial Data",
    items: [
      "ADR, occupancy, RevPAR, pace, pickup",
      "Segmentation, channel mix, booking window, forecast",
    ],
  },
  {
    title: "Financial Data",
    items: [
      "P&L, GOP, departmental expenses, payroll",
      "Forecast, budget, variance, owner distributions",
    ],
  },
  {
    title: "Guest Experience Data",
    items: [
      "Reviews, satisfaction, service recovery, reputation",
      "Survey results, complaint themes",
    ],
  },
  {
    title: "Labor Data",
    items: [
      "Staffing levels, productivity, scheduling, turnover",
      "Wage pressure, open roles",
    ],
  },
  {
    title: "Asset / CapEx Data",
    items: [
      "PIP items, project status, spend, approvals, timelines",
      "Asset condition, maintenance priorities",
    ],
  },
  {
    title: "Brand / Compliance Data",
    items: [
      "Brand standards, audits, QA results",
      "Compliance tasks, technical services feedback",
    ],
  },
];

const BASE_GOVERNANCE = [
  {
    title: "Role-Based Access",
    description:
      "Owners, operators, asset managers, and property teams see the information relevant to their role.",
  },
  {
    title: "Permissioned Documents",
    description:
      "Sensitive reports, contracts, brand materials, and owner files can be access-controlled.",
  },
  {
    title: "Approval Workflows",
    description:
      "Capital items, major spend, budget approvals, and key decisions can follow documented workflows.",
  },
  {
    title: "Audit History",
    description:
      "Tracks report versions, approvals, document updates, meeting actions, and key decisions.",
  },
  {
    title: "Data Source Labeling",
    description:
      "Distinguishes operator-provided, system-generated, owner-provided, and third-party data.",
  },
  {
    title: "Confidentiality Controls",
    description:
      "Supports confidential owner/operator communication and controlled sharing of asset information.",
  },
];

const BASE_ANALYTICS = [
  {
    title: "Performance Benchmarking",
    description:
      "Compares current performance against budget, forecast, prior year, competitive set, or portfolio averages.",
  },
  {
    title: "Market Intelligence",
    description:
      "Tracks supply, pipeline, demand drivers, competitor movement, and brand/operator presence.",
  },
  {
    title: "Owner Priority Tracking",
    description:
      "Connects KPIs and action plans to owner priorities such as exit strategy, cash flow, capex discipline, or ramp-up.",
  },
  {
    title: "Risk Signals",
    description:
      "Flags reporting gaps, underperformance, delayed capex, declining reviews, labor pressure, or missed follow-ups.",
  },
  {
    title: "Action Item Tracking",
    description:
      "Links insights to accountable owners, due dates, meeting notes, and follow-up status.",
  },
  {
    title: "Portfolio View",
    description:
      "Allows multi-asset owners to compare asset performance, risks, capex, and operating themes.",
  },
];

/** KPI signal presets — values must match Setup form single-select options. */
export const INFRA_SIGNAL_PRESETS = [
  {
    infra_signal_uptime: "99.9%+",
    infra_signal_incident: "<30 min",
    infra_signal_adoption: "95%+",
    infra_signal_refresh: "Daily + Weekly",
    risk_signal_audit: "95%+",
    risk_signal_bcp: "Quarterly",
    risk_signal_control: "95%+ closed on time",
    risk_signal_insurance: "Semi-Annual",
  },
  {
    infra_signal_uptime: "99.5–99.89%",
    infra_signal_incident: "<2 hours",
    infra_signal_adoption: "90–94%",
    infra_signal_refresh: "Daily",
    risk_signal_audit: "90–94%",
    risk_signal_bcp: "Semi-Annual",
    risk_signal_control: "90–94% closed on time",
    risk_signal_insurance: "Annual",
  },
  {
    infra_signal_uptime: "99.9%+",
    infra_signal_incident: "<1 hour",
    infra_signal_adoption: "90–94%",
    infra_signal_refresh: "Daily + Weekly",
    risk_signal_audit: "95%+",
    risk_signal_bcp: "Quarterly",
    risk_signal_control: "95%+ closed on time",
    risk_signal_insurance: "Quarterly",
  },
  {
    infra_signal_uptime: "99.0–99.49%",
    infra_signal_incident: "<4 hours",
    infra_signal_adoption: "85–89%",
    infra_signal_refresh: "Weekly",
    risk_signal_audit: "85–89%",
    risk_signal_bcp: "Annual",
    risk_signal_control: "80–89% closed on time",
    risk_signal_insurance: "Annual",
  },
];

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

/** Pull vendor hints from legacy Systems & Technology text into stack examples. */
export function personalizeTechnologyStack(stack, systemsText) {
  const out = cloneJson(stack);
  const text = nz(systemsText).toLowerCase();
  if (!text) return out;

  const hints = [
    { re: /opera|oracle hospitality/i, title: "PMS / Property Operations", label: "Opera / Oracle Hospitality" },
    { re: /cloudbeds|mews|infor/i, title: "PMS / Property Operations", label: "Cloud PMS" },
    { re: /duetto|ideas|atomize|rms/i, title: "RMS / Revenue Management", label: "RMS platform" },
    { re: /power bi|tableau|looker|domo|bi /i, title: "BI / Dashboards", label: "Owner BI" },
    { re: /m3|netsuite|sage|oracle financial/i, title: "Accounting / ERP", label: "Finance system" },
  ];

  for (const h of hints) {
    if (!h.re.test(systemsText)) continue;
    const card = out.find((c) => c.title === h.title);
    if (card) card.examples = `Typical stack includes ${h.label}; brand-dependent by asset`;
  }
  return out;
}

/**
 * Build Airtable field payload for one Governance row.
 * @param {{ index?: number, existingFields?: Record<string, unknown>, companyName?: string }} opts
 */
export function buildInfraExplorerSeedFields(opts) {
  opts = opts || {};
  const index = Number(opts.index) || 0;
  const existing = opts.existingFields || {};
  const preset = INFRA_SIGNAL_PRESETS[index % INFRA_SIGNAL_PRESETS.length];
  const maturity = MATURITY_LEVELS[index % MATURITY_LEVELS.length];

  const stack = personalizeTechnologyStack(
    BASE_STACK,
    existing.infra_systems_technology || existing["Systems & Technology"]
  );

  const fields = {
    infra_technology_stack_json: JSON.stringify(stack),
    infra_services_offered_json: JSON.stringify(cloneJson(BASE_SERVICES)),
    infra_data_domains_json: JSON.stringify(cloneJson(BASE_DOMAINS)),
    infra_data_governance_json: JSON.stringify(cloneJson(BASE_GOVERNANCE)),
    infra_analytics_support_json: JSON.stringify(cloneJson(BASE_ANALYTICS)),
    infra_technology_maturity_level: maturity,
  };

  for (const [key, value] of Object.entries(preset)) {
    const cur = existing[key];
    if (cur == null || cur === "" || cur === "Not Measured / N/A") {
      fields[key] = value;
    }
  }

  return fields;
}

export const INFRA_EXPLORER_AIRTABLE_FIELD_SPECS = [
  { name: "infra_technology_stack_json", type: "multilineText" },
  { name: "infra_services_offered_json", type: "multilineText" },
  { name: "infra_data_domains_json", type: "multilineText" },
  { name: "infra_data_governance_json", type: "multilineText" },
  { name: "infra_analytics_support_json", type: "multilineText" },
  {
    name: "infra_technology_maturity_level",
    type: "singleSelect",
    options: {
      choices: MATURITY_LEVELS.map((name) => ({ name })),
    },
  },
];
