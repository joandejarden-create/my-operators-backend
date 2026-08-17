/**
 * Founder Project Plan — recommended Airtable views (manual UI setup).
 * Table: tblpCg0QZ0kIPXihE | Base: appKZuK006BWIVjNW
 */

/**
 * Exclude phase summary rows — use LEFT() (reliable in Airtable UI; avoid REGEX escaping issues).
 * Paste exactly into Airtable filter formula:
 *   LEFT({Task}, 14) != '[Phase rollup]'
 */
export const FPP_EXCLUDE_PHASE_ROLLUP_FILTER = "LEFT({Task}, 14) != '[Phase rollup]'";

/** Active operational + current-week focus (avoids all historical overdue roadmap rows). */
export const FPP_TODAYS_FOCUS_FILTER = [
  "AND(",
  "  {Status} != 'Completed',",
  "  {Status} != 'Deferred',",
  "  {Status} != 'Not Needed',",
  `  ${FPP_EXCLUDE_PHASE_ROLLUP_FILTER},`,
  "  OR(",
  "    AND(",
  "      {Source} = 'ChatGPT Master To-Do',",
  "      OR(",
  "        {Priority} = 'P0 = Urgent / Launch-Critical',",
  "        {Priority} = 'P1 = Important Near-Term'",
  "      )",
  "    ),",
  "    AND(",
  "      {Status} = 'In Progress',",
  "      OR(",
  "        {Priority} = 'P0 = Urgent / Launch-Critical',",
  "        {Priority} = 'P1 = Important Near-Term'",
  "      )",
  "    ),",
  "    AND(",
  "      OR(",
  "        {Priority} = 'P0 = Urgent / Launch-Critical',",
  "        {Priority} = 'P1 = Important Near-Term'",
  "      ),",
  "      OR(",
  "        IS_SAME({End}, TODAY(), 'day'),",
  "        IS_SAME({End}, TODAY(), 'week')",
  "      )",
  "    )",
  "  )",
  ")",
].join("\n");

/** @typedef {{ name: string, purpose: string, filterFormula?: string, filterNotes?: string[], groupBy?: string[], sort?: string[], visibleFields?: string[], summaryNotes?: string[], setupSteps?: string[] }} FppViewConfig */

/** @type {FppViewConfig[]} */
export const FPP_PRIORITY_VIEW_CONFIGS = [
  {
    name: "Today's Focus",
    purpose:
      "What to work on now: all active master to-dos, in-progress P0/P1 founder tasks, and P0/P1 due this week. Excludes old overdue roadmap noise.",
    filterFormula: FPP_TODAYS_FOCUS_FILTER,
    sort: ["Priority (asc)", "Roadmap Sort (asc)", "End (asc)"],
    visibleFields: [
      "Task",
      "Priority",
      "Status",
      "End",
      "Next Action",
      "Phase",
      "Roadmap Sort",
      "Workstream",
      "Source",
      "Progress",
    ],
    setupSteps: [
      "Duplicate Grid view or click + Add view → Grid.",
      "Name: Today's Focus",
      "Filter → Formula → paste filter above.",
      "Sort: Priority ascending, then End ascending.",
      "Hide columns you do not need; keep Next Action visible.",
      "Pin this view in your Airtable sidebar.",
    ],
  },
  {
    name: "Master To-Do — Today",
    purpose:
      "Operational pilot/GTM tasks only (Source = ChatGPT Master To-Do), sorted by priority and due date.",
    filterFormula: [
      "AND(",
      "  {Source} = 'ChatGPT Master To-Do',",
      "  {Status} != 'Completed',",
      "  {Status} != 'Deferred'",
      ")",
    ].join("\n"),
    sort: ["Roadmap Sort (asc)"],
    visibleFields: [
      "Task",
      "Roadmap Sort",
      "Phase Number",
      "Step Number",
      "Priority",
      "Status",
      "End",
      "Next Action",
      "Related Area",
      "Workstream",
      "Progress",
      "Blocker",
    ],
  },
  {
    name: "Executive Roadmap (Ordered)",
    purpose:
      "Full founder roadmap in execution order. Uses Roadmap Sort (phase.step key). Founder / PMO tracker is step 1 in each phase.",
    filterFormula: [
      "AND(",
      "  {Status} != 'Completed',",
      "  {Status} != 'Deferred',",
      "  {Status} != 'Not Needed',",
      `  ${FPP_EXCLUDE_PHASE_ROLLUP_FILTER}`,
      ")",
    ].join("\n"),
    groupBy: ["Phase Number"],
    sort: ["Roadmap Sort (asc)"],
    visibleFields: [
      "Roadmap Sort",
      "Phase Number",
      "Step Number",
      "Phase",
      "Task",
      "Status",
      "Priority",
      "Start",
      "End",
      "Workstream",
      "Progress",
      "Next Action",
    ],
    setupSteps: [
      "Add view → Grid → name: Executive Roadmap (Ordered).",
      "Filter → Formula → paste filter above (active tasks only).",
      "Sort: Roadmap Sort ascending (required — single sort rule).",
      "Group: Phase Number ascending — NOT the Phase name field (name groups use old Airtable option order).",
      "Optional: hide Phase Number column after grouping if redundant.",
      "Pin next to Today's Focus.",
    ],
  },
  {
    name: "Phase Progress (Grouped)",
    purpose:
      "See how many tasks exist per Phase and scan completion by Status. Group headers show record counts.",
    filterFormula: FPP_EXCLUDE_PHASE_ROLLUP_FILTER,
    groupBy: ["Phase Number"],
    sort: ["Roadmap Sort (asc)"],
    visibleFields: ["Task", "Roadmap Sort", "Step Number", "Status", "Priority", "Progress", "End", "Workstream"],
    summaryNotes: [
      "Each Phase Number group header shows total task count.",
      "Group by Phase Number (1–15), not Phase name — matches roadmap order.",
      "Within each group, Roadmap Sort orders tasks (Founder / PMO first, then execution sequence).",
      "For numeric completion %, use the Phase Progress Report script (see below) or an Interface chart.",
    ],
    setupSteps: [
      "Add view → Grid → name: Phase Progress (Grouped).",
      "Filter → Formula → paste exclude-rollup filter.",
      "Group → Phase Number (ascending). Do not group by Phase name.",
      "Sort within groups: Roadmap Sort ascending.",
      "Optional: color records by Status field for faster scanning.",
    ],
  },
  {
    name: "Phase — Completed Only",
    purpose: "Quick list of completed tasks grouped by Phase (for auditing what shipped).",
    filterFormula: [
      "AND(",
      "  {Status} = 'Completed',",
      `  ${FPP_EXCLUDE_PHASE_ROLLUP_FILTER}`,
      ")",
    ].join("\n"),
    groupBy: ["Phase"],
    sort: ["End (desc)", "Task (asc)"],
    visibleFields: ["Task", "Phase", "Completed Date", "End", "Workstream", "Deliverables"],
  },
];

export function buildFppViewsManualMarkdown(meta = {}) {
  const baseId = meta.baseId || "appKZuK006BWIVjNW";
  const tableId = meta.tableId || "tblpCg0QZ0kIPXihE";
  const tableName = meta.tableName || "Founder Project Plan";

  const lines = [
    "# Founder Project Plan — daily & phase views (manual setup)",
    "",
    `Base: \`${baseId}\``,
    `Table: **${tableName}** (\`${tableId}\`)`,
    "",
    "> **Note:** Airtable's API cannot create or configure views in this workspace.",
    "> Create each view below in the Airtable UI (~2 min per view).",
    "",
    "---",
    "",
  ];

  for (const view of FPP_PRIORITY_VIEW_CONFIGS) {
    lines.push(`## ${view.name}`);
    lines.push("");
    lines.push(`**Purpose:** ${view.purpose}`);
    lines.push("");
    if (view.filterFormula) {
      lines.push("**Filter formula:**");
      lines.push("```");
      lines.push(view.filterFormula);
      lines.push("```");
      lines.push("");
    }
    if (view.filterNotes?.length) {
      for (const n of view.filterNotes) lines.push(`- ${n}`);
      lines.push("");
    }
    if (view.groupBy?.length) {
      lines.push(`**Group by:** ${view.groupBy.join(" → ")}`);
      lines.push("");
    }
    if (view.sort?.length) {
      lines.push("**Sort:**");
      for (const s of view.sort) lines.push(`- ${s}`);
      lines.push("");
    }
    if (view.visibleFields?.length) {
      lines.push(`**Recommended visible fields:** ${view.visibleFields.join(", ")}`);
      lines.push("");
    }
    if (view.summaryNotes?.length) {
      lines.push("**How to read completion counts:**");
      for (const n of view.summaryNotes) lines.push(`- ${n}`);
      lines.push("");
    }
    if (view.setupSteps?.length) {
      lines.push("**Setup steps:**");
      view.setupSteps.forEach((s, i) => lines.push(`${i + 1}. ${s}`));
      lines.push("");
    }
    lines.push("---");
    lines.push("");
  }

  lines.push("## Why Phase sections looked out of order");
  lines.push("");
  lines.push("- **Roadmap Sort** (1.01, 5.03…) is correct per row.");
  lines.push("- **Group by Phase** (name) uses the Airtable select option order — which was Platform Design before GTM.");
  lines.push("- **Fix in views:** Group by **Phase Number** (1–15) or sort by **Roadmap Sort** only.");
  lines.push("- **Optional UI fix:** Reorder Phase field options manually (see below).");
  lines.push("");
  lines.push("### Manual: reorder Phase select options (optional)");
  lines.push("");
  lines.push("1. Open **Founder Project Plan** → click **Phase** column header → **Edit field**.");
  lines.push("2. Drag options into roadmap order:");
  lines.push("   1. Strategy & Foundations → 2. Product Definition → 3. Strategy & Design →");
  lines.push("   4. Resources / Collateral → 5. GTM / Outreach → 6. Pilot Conversion →");
  lines.push("   7. Pilot Delivery → 8. Product / Access → 9. Platform Design →");
  lines.push("   10. Platform Build → 11. Content & GTM → 12. Testing & Pilot →");
  lines.push("   13. Launch & Operations → 14. Scale & Optimize → 15. Later");
  lines.push("3. Save. Then **Group by Phase** will match roadmap order.");
  lines.push("");
  lines.push("API reorder script (if schema write is enabled later): `npm run sync:fpp-phase-select-order`");
  lines.push("");
  lines.push("");
  lines.push("| Field | Purpose |");
  lines.push("|-------|---------|");
  lines.push("| **Phase Number** | Roadmap phase index (1–15) |");
  lines.push("| **Step Number** | Order within phase (1, 2, 3…) |");
  lines.push("| **Roadmap Sort** | Formula: `Phase Number + Step Number/100` — sort this column ↑ for full order |");
  lines.push("");
  lines.push("Re-sync step order from repo:");
  lines.push("```bash");
  lines.push("npm run sync:fpp-phase-order");
  lines.push("```");
  lines.push("");
  lines.push("---");
  lines.push("");
  lines.push("## Phase completion report (automated)");
  lines.push("");
  lines.push("Run locally for exact completed/total counts per phase:");
  lines.push("```bash");
  lines.push("node scripts/report-founder-project-plan-phase-progress.mjs");
  lines.push("```");
  lines.push("");
  lines.push("Output: `reports/founder-project-plan-phase-progress.json`");
  lines.push("");

  return `${lines.join("\n")}\n`;
}
