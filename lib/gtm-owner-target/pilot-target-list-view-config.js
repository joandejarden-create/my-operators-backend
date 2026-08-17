/**
 * Pilot Target List — Airtable view configuration (outreach workflow).
 *
 * Canonical source for the four pilot outreach grid views.
 * Airtable Meta API can list views but cannot create views or configure
 * filters, sorts, or visible fields in this environment — use manual setup
 * instructions from buildPilotTargetListViewsManualMarkdown().
 */
import { MAP_PILOT_TARGET_LIST } from "./pilot-target-list-field-map.js";

const F = MAP_PILOT_TARGET_LIST;

/** @typedef {{ field: string, direction?: 'asc' | 'desc', notes?: string }} PilotViewSort */
/** @typedef {{ name: string, purpose: string, visibleFieldKeys: string[], visibleFields: string[], optionalVisibleFieldKeys?: string[], filterFormula: string, filterNotes?: string, sort: PilotViewSort[], notes?: string[] }} PilotViewConfig */

/** @type {PilotViewConfig[]} */
export const PILOT_TARGET_LIST_VIEW_CONFIGS = [
  {
    name: "Pilot Outreach Pipeline",
    purpose: "Daily working view for active outreach tracking.",
    visibleFieldKeys: [
      "name",
      "company",
      "role",
      "category",
      "outreachSegment",
      "pilotRegion",
      "region",
      "priority",
      "pilotFit",
      "pilotRelevance",
      "warmIntro",
      "relationshipStrength",
      "likelyContribution",
      "whyTheyMatter",
      "outreachMessageAngle",
      "outreachStatus",
      "nextAction",
      "lastContactDate",
      "nextFollowUpDate",
      "replyNotes",
      "notes",
    ],
    optionalVisibleFieldKeys: ["messageAngle"],
    visibleFields: [],
    filterFormula: [
      "AND(",
      "  NOT({Do Not Contact}),",
      "  OR(",
      "    {Outreach Status} = BLANK(),",
      "    AND(",
      "      {Outreach Status} != 'Archived',",
      "      {Outreach Status} != 'Not Interested'",
      "    )",
      "  )",
      ")",
    ].join("\n"),
    filterNotes:
      "Includes rows with blank Outreach Status. Excludes Do Not Contact, Archived, and Not Interested.",
    sort: [
      { field: F.priority, direction: "asc", notes: "P1 → P2 → P3 (ascending works alphabetically)" },
      { field: F.nextFollowUpDate, direction: "asc" },
      { field: F.company, direction: "asc" },
      { field: F.name, direction: "asc" },
    ],
    notes: [
      "Hide Do Not Contact from this view or keep it hidden — filter already excludes checked rows.",
      "First wave outreach is manual/founder-led; this view does not send email.",
    ],
  },
  {
    name: "Drafting Queue",
    purpose: "Rows where messaging needs to be written, reviewed, or approved.",
    visibleFieldKeys: [
      "name",
      "company",
      "role",
      "outreachSegment",
      "pilotRegion",
      "priority",
      "pilotFit",
      "whyTheyMatter",
      "outreachMessageAngle",
      "personalizationLine",
      "emailSubject",
      "emailDraft",
      "finalApprovedEmail",
      "linkedInDmDraft",
      "followUpDraft",
      "outreachStatus",
      "notes",
    ],
    visibleFields: [],
    filterFormula: [
      "AND(",
      "  NOT({Do Not Contact}),",
      "  OR(",
      "    {Outreach Status} = 'Draft Needed',",
      "    {Outreach Status} = 'Drafted',",
      "    {Outreach Status} = 'Needs Review'",
      "  )",
      ")",
    ].join("\n"),
    sort: [
      { field: F.priority, direction: "asc" },
      { field: F.outreachSegment, direction: "asc" },
      { field: F.company, direction: "asc" },
      { field: F.name, direction: "asc" },
    ],
    notes: [
      "Use Final Approved Email as the send/export copy once reviewed.",
      "Email Draft is working copy only.",
    ],
  },
  {
    name: "Approved for Send / Mail Merge",
    purpose: "Rows approved for manual sending or CSV export.",
    visibleFieldKeys: [
      "name",
      "email",
      "company",
      "role",
      "pilotRegion",
      "linkedInUrl",
      "sendChannel",
      "mailMergeBatch",
      "emailSubject",
      "finalApprovedEmail",
      "linkedInDmDraft",
      "readyForMailMerge",
      "outreachStatus",
      "lastContactDate",
      "nextFollowUpDate",
      "doNotContact",
      "doNotContactReason",
    ],
    visibleFields: [],
    filterFormula: [
      "AND(",
      "  {Outreach Status} = 'Approved',",
      "  {Ready for Mail Merge},",
      "  NOT({Do Not Contact}),",
      "  LEN(TRIM({Email Subject} & '')) > 0,",
      "  LEN(TRIM({Final Approved Email} & '')) > 0,",
      "  OR(",
      "    {Send Channel} != 'Email',",
      "    LEN(TRIM({Email} & '')) > 0",
      "  )",
      ")",
    ].join("\n"),
    filterNotes:
      "Use this view before running export-owner-targets-mail-merge.mjs. Requires Email when Send Channel = Email.",
    sort: [
      { field: F.mailMergeBatch, direction: "asc" },
      { field: F.sendChannel, direction: "asc" },
      { field: F.company, direction: "asc" },
      { field: F.name, direction: "asc" },
    ],
    notes: [
      "Export script: node scripts/export-owner-targets-mail-merge.mjs",
      "Do Not Contact columns shown for verification — filter excludes checked rows.",
    ],
  },
  {
    name: "Follow-Up Needed",
    purpose: "Rows requiring a follow-up.",
    visibleFieldKeys: [
      "name",
      "email",
      "company",
      "role",
      "pilotRegion",
      "sendChannel",
      "outreachStatus",
      "lastContactDate",
      "nextFollowUpDate",
      "followUpDraft",
      "replyNotes",
      "notes",
      "doNotContact",
    ],
    visibleFields: [],
    filterFormula: [
      "AND(",
      "  NOT({Do Not Contact}),",
      "  OR(",
      "    {Outreach Status} = 'Follow-Up Needed',",
      "    IS_BEFORE({Next Follow-Up Date}, TODAY()),",
      "    IS_SAME({Next Follow-Up Date}, TODAY(), 'day')",
      "  )",
      ")",
    ].join("\n"),
    sort: [
      { field: F.nextFollowUpDate, direction: "asc" },
      { field: F.priority, direction: "asc" },
      { field: F.company, direction: "asc" },
      { field: F.name, direction: "asc" },
    ],
    notes: [
      "Review Follow-Up Draft and Reply Notes before contacting again.",
      "Manual follow-up only — no automated sending.",
    ],
  },
];

export const PILOT_TARGET_LIST_VIEW_NAMES = PILOT_TARGET_LIST_VIEW_CONFIGS.map((v) => v.name);

/** Resolve config field keys to Airtable column names. */
export function resolveViewVisibleFields(config, tableFieldNames) {
  const onTable = new Set(tableFieldNames || []);
  const resolved = [];
  const omittedOptional = [];
  const missingRequired = [];

  for (const key of config.visibleFieldKeys) {
    const fieldName = F[key];
    if (!fieldName) {
      missingRequired.push(key);
      continue;
    }
    if (onTable.has(fieldName)) {
      resolved.push(fieldName);
    } else {
      missingRequired.push(fieldName);
    }
  }

  for (const key of config.optionalVisibleFieldKeys || []) {
    const fieldName = F[key];
    if (fieldName && onTable.has(fieldName)) {
      resolved.push(fieldName);
    } else if (fieldName) {
      omittedOptional.push(fieldName);
    }
  }

  return { resolved, omittedOptional, missingRequired };
}

/** Hydrate visibleFields on each config from live table schema. */
export function hydratePilotViewConfigs(tableFieldNames) {
  return PILOT_TARGET_LIST_VIEW_CONFIGS.map((config) => {
    const { resolved, omittedOptional, missingRequired } = resolveViewVisibleFields(
      config,
      tableFieldNames
    );
    return {
      ...config,
      visibleFields: resolved,
      omittedOptionalFields: omittedOptional,
      missingRequiredFields: missingRequired,
    };
  });
}

/**
 * Plan view setup against existing Airtable views.
 * @param {Array<{ id: string, name: string, type?: string }>} existingViews
 */
export function planPilotTargetListViewSetup(existingViews, tableFieldNames) {
  const existingByName = new Map((existingViews || []).map((v) => [v.name, v]));
  const configs = hydratePilotViewConfigs(tableFieldNames);

  const viewsToCreate = [];
  const viewsAlreadyPresent = [];
  const viewsToUpdate = [];

  for (const config of configs) {
    const existing = existingByName.get(config.name);
    if (existing) {
      viewsAlreadyPresent.push({ ...config, viewId: existing.id, viewType: existing.type });
    } else {
      viewsToCreate.push(config);
    }
  }

  return {
    configs,
    viewsToCreate,
    viewsAlreadyPresent,
    viewsToUpdate,
    unsupportedConfiguration: [
      "view_creation_via_meta_api",
      "filter_formula_via_api",
      "sort_order_via_api",
      "visible_field_order_via_api",
    ],
  };
}

export function buildPilotTargetListViewsManualMarkdown(plan, tableMeta = {}) {
  const lines = [
    "# Pilot Target List — manual Airtable view setup",
    "",
    "The Airtable Meta API **cannot create or configure views** (filters, sorts, visible fields) in this environment.",
    "Create each grid view manually in Airtable using the steps below.",
    "",
    `Base: \`${tableMeta.baseId || "AIRTABLE_GTM_BASE_ID"}\``,
    `Table: **${tableMeta.tableName || "Pilot Target List"}** (\`${tableMeta.tableId || ""}\`)`,
    "",
    "## Quick steps (repeat per view)",
    "",
    "1. Open **Pilot Target List** in Airtable.",
    "2. Click **+** next to the view tabs → **Grid view**.",
    "3. Rename the view to the exact name below.",
    "4. Open **Filter** → **Add condition** → switch to **Formula** and paste the filter.",
    "5. Open **Sort** and add the sort fields in order.",
    "6. Hide all columns, then show only the listed fields **in order** (drag to reorder).",
    "7. Save the view.",
    "",
  ];

  if (plan.viewsAlreadyPresent?.length) {
    lines.push("## Views already present");
    lines.push("");
    for (const v of plan.viewsAlreadyPresent) {
      lines.push(`- **${v.name}** (\`${v.viewId}\`) — verify filter, sort, and visible fields match below`);
    }
    lines.push("");
  }

  if (plan.viewsToCreate?.length) {
    lines.push("## Views to create");
    lines.push("");
    for (const v of plan.viewsToCreate) {
      lines.push(`- **${v.name}**`);
    }
    lines.push("");
  }

  for (const config of plan.configs || []) {
    lines.push(`---`);
    lines.push("");
    lines.push(`## ${config.name}`);
    lines.push("");
    lines.push(`**Purpose:** ${config.purpose}`);
    lines.push("");

    if (config.filterFormula) {
      lines.push("### Filter (formula)");
      lines.push("");
      lines.push("```");
      lines.push(config.filterFormula);
      lines.push("```");
      lines.push("");
      if (config.filterNotes) {
        lines.push(`*${config.filterNotes}*`);
        lines.push("");
      }
    }

    lines.push("### Sort");
    lines.push("");
    for (const sort of config.sort || []) {
      const dir = sort.direction === "desc" ? "descending" : "ascending";
      lines.push(`1. **${sort.field}** — ${dir}${sort.notes ? ` (${sort.notes})` : ""}`);
    }
    lines.push("");

    lines.push("### Visible fields (in order)");
    lines.push("");
    for (const field of config.visibleFields || []) {
      lines.push(`- ${field}`);
    }
    if (config.omittedOptionalFields?.length) {
      lines.push("");
      lines.push("*Optional fields omitted (not on table):*");
      for (const field of config.omittedOptionalFields) {
        lines.push(`- ${field}`);
      }
    }
    if (config.missingRequiredFields?.length) {
      lines.push("");
      lines.push("**Warning — expected fields missing from table:**");
      for (const field of config.missingRequiredFields) {
        lines.push(`- ${field}`);
      }
    }
    lines.push("");

    if (config.notes?.length) {
      lines.push("### Notes");
      lines.push("");
      for (const note of config.notes) {
        lines.push(`- ${note}`);
      }
      lines.push("");
    }
  }

  lines.push("---");
  lines.push("");
  lines.push("## Reminders");
  lines.push("");
  lines.push("- These are **Airtable views only** — they do not send email.");
  lines.push("- **Approved for Send / Mail Merge** is the pre-export checklist view.");
  lines.push("- **Do Not Contact** excludes targets from outreach and CSV export.");
  lines.push("- First wave should remain **manual / founder-led**.");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

/** @returns {boolean} */
export function metaApiSupportsViewCreation() {
  return false;
}

/** @returns {boolean} */
export function metaApiSupportsViewConfiguration() {
  return false;
}
