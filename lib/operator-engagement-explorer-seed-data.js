/**
 * Engagement & Reporting Explorer seed payloads (Operator Setup — Commercial table).
 * Keep in sync with public/js/operator-engagement-reporting-sections.js DEFAULTS.
 */

export const ENGAGEMENT_JSON_FIELD_KEYS = [
  "ov_strategic_owner_value_json",
  "ov_engagement_cadence_json",
  "ov_controls_governance_json",
  "ov_reports_received_json",
  "ov_owner_tools_json",
  "ov_lifecycle_support_json",
];

export const ENGAGEMENT_EXPLORER_AIRTABLE_FIELD_SPECS = ENGAGEMENT_JSON_FIELD_KEYS.map((name) => ({
  name,
  type: "multilineText",
}));

const SEED_DEFAULTS = {
  ov_strategic_owner_value_json: [
    {
      title: "Owner Strategy Translation",
      description:
        "Converts owner objectives into an operating plan, reporting cadence, and measurable priorities.",
    },
    {
      title: "Asset Performance Visibility",
      description:
        "Creates a clearer view of revenue, expenses, GOP, guest feedback, capex, and market position.",
    },
    {
      title: "Decision Support",
      description:
        "Helps owners understand trade-offs around pricing, staffing, brand requirements, capex timing, and repositioning.",
    },
    {
      title: "Governance Discipline",
      description:
        "Provides a structured rhythm for reviews, approvals, escalations, and accountability.",
    },
    {
      title: "Value Protection",
      description:
        "Supports long-term value by connecting operating decisions to asset condition, guest experience, and exit readiness.",
    },
    {
      title: "Owner Education",
      description:
        "Helps owners understand hotel metrics, brand obligations, distribution dynamics, and operating levers.",
    },
  ],
  ov_engagement_cadence_json: [
    {
      cadence: "Weekly",
      engagementType: "Transition / ramp-up call",
      focus: "Pre-opening, takeover, conversion, or major repositioning periods.",
    },
    {
      cadence: "Monthly",
      engagementType: "Owner performance review",
      focus: "P&L, forecast, KPIs, action plan, guest feedback, capex status, and open issues.",
    },
    {
      cadence: "Quarterly",
      engagementType: "Strategic business review",
      focus:
        "Market performance, competitive position, owner priorities, investment needs, and brand strategy.",
    },
    {
      cadence: "Annually",
      engagementType: "Budget and business plan",
      focus: "Operating budget, capex plan, staffing model, commercial plan, and asset strategy.",
    },
  ],
  ov_controls_governance_json: [
    {
      title: "Budget Process",
      description:
        "Annual budget planning, owner review, approval workflow, forecast updates, and variance tracking.",
    },
    {
      title: "CapEx Planning",
      description:
        "Capital prioritization, PIP planning, ROI framing, approval records, and project status tracking.",
    },
    {
      title: "Performance Reviews",
      description:
        "Structured operating reviews tied to owner priorities, KPI trends, and corrective actions.",
    },
    {
      title: "Approval Controls",
      description:
        "Documented approval thresholds for spend, staffing, contract commitments, and capital projects.",
    },
    {
      title: "Issue Escalation",
      description:
        "Defined escalation path for service, financial, brand, legal, compliance, and owner-sensitive issues.",
    },
    {
      title: "Audit Trail",
      description:
        "Records major decisions, reporting packages, approval history, and follow-up actions.",
    },
  ],
  ov_reports_received_json: [
    {
      title: "Monthly Owner Report",
      description: "P&L summary, KPI trends, forecast, variance commentary, and action items.",
    },
    {
      title: "Commercial Performance Pack",
      description: "ADR, RevPAR, channel mix, segmentation, pace, pickup, and market intelligence.",
    },
    {
      title: "CapEx / PIP Tracker",
      description:
        "Capital projects, timing, approvals, spend status, risk items, and brand requirements.",
    },
    {
      title: "Guest Experience Report",
      description:
        "Reputation, guest satisfaction, service recovery, review trends, and operational responses.",
    },
    {
      title: "Labor & Productivity Report",
      description:
        "Staffing levels, productivity, payroll trends, turnover, and key operating constraints.",
    },
    {
      title: "Asset Value Narrative",
      description:
        "Quarterly summary connecting operating performance to asset value, positioning, and owner objectives.",
    },
  ],
  ov_owner_tools_json: [
    {
      title: "Owner Portal",
      description:
        "Secure owner access to reports, dashboards, documents, meeting notes, and approvals.",
    },
    {
      title: "Dashboard Views",
      description:
        "KPI summary, revenue trends, expense controls, capex status, guest experience, and action items.",
    },
    {
      title: "Document Library",
      description:
        "Budgets, contracts, brand documents, reports, meeting packs, capex files, and project records.",
    },
    {
      title: "Meeting Center",
      description: "Agenda, notes, decisions, action owners, due dates, and follow-up status.",
    },
    {
      title: "Owner Advisory Boards / Councils",
      description:
        "Structured owner feedback forums, portfolio insights, education sessions, and strategic input.",
    },
    {
      title: "Owner Education Library",
      description:
        "Plain-language explainers on hotel metrics, brand terms, distribution, capex, and operating models.",
    },
  ],
  ov_lifecycle_support_json: [
    {
      stage: "Evaluation",
      support: "Deal review, owner goals, asset needs, operating assumptions, and management fit.",
    },
    {
      stage: "Onboarding",
      support: "Kickoff, transition plan, document collection, reporting setup, and governance rhythm.",
    },
    {
      stage: "Pre-Opening / Transition",
      support: "Critical path, staffing, systems, vendor setup, brand coordination, and owner updates.",
    },
    {
      stage: "Stabilized Operations",
      support:
        "Monthly reviews, forecast updates, performance management, capex tracking, and strategy check-ins.",
    },
    {
      stage: "Repositioning / Exit",
      support:
        "Asset narrative, capex decisions, performance proof, buyer readiness, and value enhancement.",
    },
  ],
};

export function buildEngagementExplorerSeedFields() {
  const fields = {};
  for (const key of ENGAGEMENT_JSON_FIELD_KEYS) {
    fields[key] = JSON.stringify(SEED_DEFAULTS[key] || []);
  }
  return fields;
}
