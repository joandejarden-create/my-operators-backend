/**
 * Founder Project Plan — schema targets and view definitions.
 * Table: Founder Project Plan (tblpCg0QZ0kIPXihE) on GTM / Owner Targets base.
 */

export const FOUNDER_PROJECT_PLAN_TABLE_ID = "tblpCg0QZ0kIPXihE";
export const FOUNDER_PROJECT_PLAN_TABLE_NAME = "Founder Project Plan";

/** Fields that must not be deleted or renamed by automation. */
export const FOUNDER_PROJECT_PLAN_PRESERVED_FIELDS = [
  "Task",
  "Workstream",
  "Status",
  "Phase",
  "Assigned To",
  "Duration (Days)",
  "Progress",
  "Task Objective/Description",
  "Deliverables",
  "Field 14",
];

export const VAL_FOUNDER_PRIORITY = [
  "P0 = Urgent / Launch-Critical",
  "P1 = Important Near-Term",
  "P2 = Useful but Not Urgent",
  "P3 = Backlog / Nice-to-Have",
];

export const VAL_FOUNDER_SPRINT_WAVE = [
  "Founder Sprint 1",
  "Founder Sprint 2",
  "Founder Sprint 3",
  "Pilot Wave 1",
  "Pilot Wave 2",
  "Post-Pilot",
  "Backlog",
];

export const VAL_FOUNDER_RELATED_TABLE = [
  "Founder Project Plan",
  "Properties",
  "Companies",
  "Contacts",
  "Pilot Target List",
  "Owner Targets",
];

export const VAL_FOUNDER_PHASE = [
  "Strategy & Foundations",
  "Product Definition",
  "Data & Pipeline Setup",
  "Platform Design",
  "Platform Build",
  "Pilot Readiness",
  "Testing & Pilot",
  "Launch & Operations",
  "Scale & Optimize",
];

export const VAL_FOUNDER_WORKSTREAM = [
  "Founder / PMO",
  "Strategic Positioning",
  "Business Model",
  "Legal & Compliance",
  "Product / UX",
  "Matching Logic",
  "Data Model & Intake",
  "Platform Build",
  "Pilot Pipeline",
  "Owner Outreach",
  "Brand / Operator Outreach",
  "CRM & Communications",
  "Referral Program",
  "GTM & Content",
  "Analytics & Reporting",
  "Operations & Support",
];

export const VAL_FOUNDER_STATUS = [
  "Backlog",
  "Not Started",
  "In Progress",
  "Blocked",
  "Ready for Review",
  "Completed",
  "Deferred",
  "Not Needed",
];

/** New fields to create when absent (case-insensitive name match). */
export const FOUNDER_PROJECT_PLAN_FIELDS_TO_CREATE = [
  {
    name: "Priority",
    type: "singleSelect",
    options: { choices: VAL_FOUNDER_PRIORITY.map((name) => ({ name })) },
  },
  {
    name: "Start Date",
    type: "date",
    options: { dateFormat: { name: "iso", format: "YYYY-MM-DD" } },
    aliases: ["start"],
  },
  {
    name: "Due Date",
    type: "date",
    options: { dateFormat: { name: "iso", format: "YYYY-MM-DD" } },
    aliases: ["end", "due"],
  },
  {
    name: "Sprint / Wave",
    type: "singleSelect",
    options: { choices: VAL_FOUNDER_SPRINT_WAVE.map((name) => ({ name })) },
  },
  { name: "Dependency", type: "multilineText" },
  { name: "Blocker", type: "multilineText" },
  { name: "Next Action", type: "multilineText" },
  {
    name: "Milestone?",
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
  },
  {
    name: "Related Table",
    type: "singleSelect",
    options: { choices: VAL_FOUNDER_RELATED_TABLE.map((name) => ({ name })) },
  },
  { name: "Success Metric", type: "multilineText" },
  {
    name: "Step Number",
    type: "number",
    options: { precision: 0 },
  },
];

/** Select fields to merge options on (never remove existing choices). */
export const FOUNDER_PROJECT_PLAN_SELECT_NORMALIZATIONS = [
  { fieldName: "Phase", targetOptions: VAL_FOUNDER_PHASE },
  { fieldName: "Status", targetOptions: VAL_FOUNDER_STATUS },
  {
    fieldName: "Workstream",
    targetOptions: VAL_FOUNDER_WORKSTREAM,
    requireSingleSelect: true,
  },
];

export const FOUNDER_PROJECT_PLAN_VIEW_NAMES = [
  "Founder Command Center",
  "This Week",
  "By Phase",
  "By Workstream",
  "Pilot Readiness",
  "Platform Build Tracker",
  "Blocked / Needs Decision",
  "Completed Milestones",
  "GTM & Outreach",
  "Executive Roadmap",
];

/** @typedef {{ name: string, purpose: string, filterFormula: string, filterNotes?: string, groupBy?: string[], sort: { field: string, direction?: 'asc'|'desc', notes?: string }[], hideFields?: string[], notes?: string[] }} FounderViewConfig */

/** @type {FounderViewConfig[]} */
export const FOUNDER_PROJECT_PLAN_VIEW_CONFIGS = [
  {
    name: "Founder Command Center",
    purpose: "Main operating view.",
    filterFormula: [
      "AND(",
      "  {Status} != 'Completed',",
      "  {Status} != 'Not Needed',",
      "  {Status} != 'Deferred'",
      ")",
    ].join("\n"),
    groupBy: ["Phase"],
    sort: [
      { field: "Priority", direction: "asc", notes: "P0 → P1 → P2 → P3" },
      { field: "End", direction: "asc", notes: "Use Due Date after field rename" },
    ],
    hideFields: ["Field 14"],
    notes: ["Hide legacy import columns (Field 14+) from this view."],
  },
  {
    name: "This Week",
    purpose: "Current-week focus and Founder Sprint 1.",
    filterFormula: [
      "OR(",
      "  IS_SAME({End}, TODAY(), 'week'),",
      "  {Sprint / Wave} = 'Founder Sprint 1'",
      ")",
    ].join("\n"),
    filterNotes:
      "Uses End (existing date field). After renaming End → Due Date in Airtable, update this formula to {Due Date}.",
    sort: [
      { field: "Priority", direction: "asc" },
      { field: "End", direction: "asc", notes: "Rename to Due Date when field is renamed" },
    ],
  },
  {
    name: "By Phase",
    purpose: "Roadmap grouped by phase.",
    groupBy: ["Phase"],
    filterFormula: "",
    sort: [
      { field: "Priority", direction: "asc" },
      { field: "End", direction: "asc", notes: "Use Due Date after field rename" },
    ],
  },
  {
    name: "By Workstream",
    purpose: "Workstream swimlanes.",
    groupBy: ["Workstream"],
    filterFormula: "",
    sort: [
      { field: "Phase", direction: "asc" },
      { field: "Priority", direction: "asc" },
    ],
    notes: ["If Workstream remains free text, grouping uses raw text values until migrated."],
  },
  {
    name: "Pilot Readiness",
    purpose: "Pilot pipeline and outreach tasks.",
    filterFormula: [
      "OR(",
      "  {Phase} = 'Pilot Readiness',",
      "  {Workstream} = 'Pilot Pipeline',",
      "  {Workstream} = 'Owner Outreach',",
      "  {Workstream} = 'Brand / Operator Outreach',",
      "  {Related Table} = 'Pilot Target List',",
      "  {Related Table} = 'Owner Targets'",
      ")",
    ].join("\n"),
    sort: [
      { field: "Priority", direction: "asc" },
      { field: "End", direction: "asc", notes: "Use Due Date after field rename" },
    ],
  },
  {
    name: "Platform Build Tracker",
    purpose: "Engineering and platform delivery.",
    filterFormula: [
      "OR(",
      "  {Phase} = 'Platform Build',",
      "  {Workstream} = 'Platform Build'",
      ")",
    ].join("\n"),
    sort: [
      { field: "Priority", direction: "asc" },
      { field: "End", direction: "asc", notes: "Use Due Date after field rename" },
    ],
  },
  {
    name: "Blocked / Needs Decision",
    purpose: "Escalations and review queue.",
    filterFormula: [
      "OR(",
      "  {Status} = 'Blocked',",
      "  {Status} = 'Ready for Review'",
      ")",
    ].join("\n"),
    sort: [
      { field: "Priority", direction: "asc" },
      { field: "End", direction: "asc", notes: "Use Due Date after field rename" },
    ],
  },
  {
    name: "Completed Milestones",
    purpose: "Shipped milestones.",
    filterFormula: "AND({Milestone?}, {Status} = 'Completed')",
    sort: [{ field: "End", direction: "desc", notes: "Use Due Date after field rename" }],
  },
  {
    name: "GTM & Outreach",
    purpose: "Go-to-market and relationship workstreams.",
    filterFormula: [
      "OR(",
      "  {Workstream} = 'GTM & Content',",
      "  {Workstream} = 'CRM & Communications',",
      "  {Workstream} = 'Owner Outreach',",
      "  {Workstream} = 'Brand / Operator Outreach',",
      "  {Workstream} = 'Referral Program'",
      ")",
    ].join("\n"),
    sort: [
      { field: "Priority", direction: "asc" },
      { field: "End", direction: "asc", notes: "Use Due Date after field rename" },
    ],
  },
  {
    name: "Executive Roadmap",
    purpose: "Milestone-only executive rollup.",
    filterFormula: "{Milestone?}",
    groupBy: ["Phase"],
    sort: [
      { field: "Phase", direction: "asc" },
      { field: "End", direction: "asc", notes: "Use Due Date after field rename" },
    ],
  },
];

export function buildFounderProjectPlanSelectOptionsManualMarkdown(items, meta) {
  const lines = [
    "# Founder Project Plan — manual single-select option updates",
    "",
    `Base: \`${meta.baseId}\``,
    `Table: **${meta.tableName}** (\`${meta.tableId}\`)`,
    "",
    "Airtable Meta API field PATCH for `options.choices` returned 422 in this environment.",
    "Add the missing options below in the Airtable UI (Field configuration → Options).",
    "Do **not** delete legacy options yet — record migration will happen in a separate script.",
    "",
  ];

  for (const item of items) {
    lines.push(`## ${item.fieldName}`);
    lines.push("");
    lines.push(`- Field type: ${item.fieldType}`);
    if (item.recommendation) {
      lines.push(`- Note: ${item.recommendation}`);
      lines.push("");
      continue;
    }
    lines.push(`- Options to add: ${item.optionsToAdd.length ? item.optionsToAdd.join(", ") : "(none — already complete)"}`);
    if (item.targetOptions?.length) {
      lines.push("");
      lines.push("**Target option order (new options should follow this list; keep legacy values at end):**");
      for (const o of item.targetOptions) {
        lines.push(`- ${o}`);
      }
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

export function buildFounderProjectPlanViewsManualMarkdown(plan, meta) {
  const lines = [
    "# Founder Project Plan — manual Airtable view setup",
    "",
    `Base: \`${meta.baseId}\``,
    `Table: **${meta.tableName}** (\`${meta.tableId}\`)`,
    "",
    "Airtable Meta API cannot configure filters, sorts, grouping, or hidden fields in this environment.",
    "Create each grid view below in the Airtable UI.",
    "",
  ];

  for (const config of plan.configs) {
    lines.push(`## ${config.name}`);
    lines.push("");
    lines.push(`**Purpose:** ${config.purpose}`);
    if (config.filterFormula) {
      lines.push("");
      lines.push("**Filter formula:**");
      lines.push("```");
      lines.push(config.filterFormula);
      lines.push("```");
    } else {
      lines.push("");
      lines.push("**Filter:** (none)");
    }
    if (config.filterNotes) {
      lines.push("");
      lines.push(`*Note:* ${config.filterNotes}`);
    }
    if (config.groupBy?.length) {
      lines.push("");
      lines.push(`**Group by:** ${config.groupBy.join(" → ")}`);
    }
    if (config.sort?.length) {
      lines.push("");
      lines.push("**Sort:**");
      for (const s of config.sort) {
        lines.push(`- ${s.field} (${s.direction || "asc"})${s.notes ? ` — ${s.notes}` : ""}`);
      }
    }
    if (config.hideFields?.length) {
      lines.push("");
      lines.push(`**Hide fields:** ${config.hideFields.join(", ")}`);
    }
    if (config.notes?.length) {
      lines.push("");
      for (const n of config.notes) {
        lines.push(`- ${n}`);
      }
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}
