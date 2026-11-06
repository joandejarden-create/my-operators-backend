/**
 * Airtable schema proposal for Need-Date Demand Intelligence.
 * READ-ONLY proposal — no live table/field mutation from this module.
 */

import {
  NEED_DATE_DEMAND_INTELLIGENCE_V1,
  AIRTABLE_MUTATION_BLOCKED,
  PERIOD_GRANULARITY,
  NEED_PERIOD_STATUS,
  NEED_REVENUE_TYPE_LABEL,
} from "./need-date-demand-contract-v1.js";

/**
 * Audit note: repo docs/*-airtable-fields.md contain no existing
 * Need Dates / Need Periods / GDI Need Months table for hotels.
 * Reuse path: link to Hotel Property Census (HPC) via linked record.
 */
export function proposeNeedDateAirtableSchema() {
  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    mutationStatus: "PROPOSED_ONLY",
    mutationGuard: AIRTABLE_MUTATION_BLOCKED,
    existingTableReuseDecision: {
      searched: [
        "Need Dates",
        "Need Periods",
        "Sales Need Dates",
        "GDI Need Months",
        "Hotel demand gaps",
        "Occupancy need periods",
      ],
      foundCompatibleTable: false,
      evidence:
        "No matching table documented in docs/*-airtable-fields.md; GDI hotel link target remains HPC / census rec id.",
      recommendation: "CREATE_NEW_TABLES_AFTER_FOUNDER_GO",
    },
    tables: [
      {
        name: "GDI Hotel Need Periods",
        purpose: "Hotel-declared commercial need months / date ranges",
        primaryField: "Need Period ID",
        fields: [
          { name: "Need Period ID", type: "singleLineText", required: true },
          {
            name: "Hotel",
            type: "multipleRecordLinks",
            linkedTable: "Hotel Property Census",
            required: true,
            note: "Canonical HPC record (same id used as GDI hotelId)",
          },
          { name: "Period Label", type: "singleLineText", required: true },
          { name: "Start Date", type: "date", required: true },
          { name: "End Date", type: "date", required: true },
          {
            name: "Period Granularity",
            type: "singleSelect",
            options: Object.values(PERIOD_GRANULARITY),
            required: true,
          },
          {
            name: "Revenue Need",
            type: "multipleSelects",
            options: Object.values(NEED_REVENUE_TYPE_LABEL),
            required: true,
          },
          {
            name: "Priority",
            type: "singleSelect",
            options: ["HIGH", "MEDIUM", "LOW"],
          },
          { name: "Business Objective", type: "multilineText" },
          { name: "Target Volume", type: "number", optional: true },
          { name: "Target Revenue", type: "currency", optional: true },
          { name: "Target Demand Segments", type: "multipleSelects", optional: true },
          { name: "Commercial Restrictions", type: "multilineText" },
          {
            name: "Status",
            type: "singleSelect",
            options: Object.values(NEED_PERIOD_STATUS),
          },
          { name: "Last Research Date", type: "dateTime" },
          { name: "Next Planned Review", type: "date" },
          { name: "Hotel Sales Notes", type: "multilineText" },
          { name: "Min Group Size", type: "number", optional: true },
          { name: "Max Group Size", type: "number", optional: true },
          { name: "Preferred Weekpart", type: "singleSelect", optional: true, options: ["Weekday", "Weekend", "Either"] },
          { name: "Meeting/Catering Requirements", type: "multilineText", optional: true },
          { name: "Current Version", type: "number", required: true, note: "Monotonic version; never destructive overwrite" },
          { name: "Last Confirmed By Hotel", type: "dateTime", optional: true },
          { name: "Change Reason", type: "multilineText", optional: true },
          {
            name: "Superseded By",
            type: "multipleRecordLinks",
            linkedTable: "GDI Hotel Need Periods",
            optional: true,
          },
          {
            name: "Related Need Periods",
            type: "multipleRecordLinks",
            linkedTable: "GDI Hotel Need Periods",
            optional: true,
          },
          {
            name: "Linked Opportunities",
            type: "multilineText",
            optional: true,
            note: "Canonical GDI opportunity IDs (or link table)",
          },
          { name: "Research History", type: "multilineText", optional: true },
          { name: "Created", type: "createdTime" },
          { name: "Updated", type: "lastModifiedTime" },
        ],
      },
      {
        name: "GDI Need Period Change History",
        purpose: "Append-only versioning of need-period changes (Filled ≠ Cancelled ≠ Replaced)",
        primaryField: "Change ID",
        fields: [
          { name: "Change ID", type: "singleLineText", required: true },
          {
            name: "Need Period",
            type: "multipleRecordLinks",
            linkedTable: "GDI Hotel Need Periods",
            required: true,
          },
          { name: "Previous Values JSON", type: "multilineText" },
          { name: "New Values JSON", type: "multilineText" },
          {
            name: "Change Type",
            type: "singleSelect",
            options: [
              "ADD",
              "MODIFY",
              "SPLIT",
              "PARTIALLY_FILLED",
              "FILLED",
              "CLOSED",
              "REOPEN",
              "REPLACE",
              "CANCEL",
              "PRIORITY_CHANGED",
              "REVENUE_OBJECTIVE_CHANGED",
            ],
            required: true,
          },
          { name: "Timestamp", type: "dateTime", required: true },
          { name: "Actor/Source", type: "singleLineText" },
          { name: "Reason", type: "multilineText" },
          { name: "Impact On Existing Matches", type: "multilineText" },
          { name: "Research Implications", type: "multilineText" },
        ],
        note: "Append-only. Prefer this over destructive field overwrite for auditability.",
      },
      {
        name: "GDI Need-Date Campaign Runs",
        purpose: "Campaign ledger for need-date orchestrated research",
        primaryField: "Campaign ID",
        fields: [
          { name: "Campaign ID", type: "singleLineText", required: true },
          {
            name: "Hotel",
            type: "multipleRecordLinks",
            linkedTable: "Hotel Property Census",
            required: true,
          },
          {
            name: "Linked Need Periods",
            type: "multipleRecordLinks",
            linkedTable: "GDI Hotel Need Periods",
          },
          { name: "Research Start", type: "dateTime" },
          { name: "Research End", type: "dateTime" },
          { name: "Selected Workflows", type: "multilineText" },
          { name: "Authorized Spend", type: "currency" },
          { name: "Actual Spend", type: "currency" },
          { name: "New Candidates", type: "number" },
          { name: "Existing Matches", type: "number" },
          { name: "New Qualified", type: "number" },
          { name: "Existing Opportunities Enriched", type: "number" },
          { name: "Duplicates Rejected", type: "number" },
          { name: "Research Continuation", type: "number" },
          { name: "Delivery Status", type: "singleSelect", options: ["NOT_STARTED", "PREVIEW", "STAGED", "DELIVERED", "FAILED"] },
          { name: "Publication Status", type: "singleSelect", options: ["NOT_AUTHORIZED", "STAGED", "PUBLISHED", "BLOCKED"] },
          { name: "Run Outcome", type: "multilineText" },
          { name: "Next Review", type: "date" },
        ],
        note: "Prefer local file ledger until Airtable GO. Do not duplicate supervisor campaign tables if founder chooses extension instead.",
      },
      {
        name: "GDI Need-Period Opportunity Links",
        purpose: "Many-to-many association; preserves canonical opportunity IDs",
        primaryField: "Link ID",
        fields: [
          { name: "Link ID", type: "singleLineText" },
          { name: "Need Period", type: "multipleRecordLinks", linkedTable: "GDI Hotel Need Periods" },
          { name: "Opportunity ID", type: "singleLineText", note: "Canonical GDI opportunity id" },
          { name: "Overlap Status", type: "singleSelect" },
          { name: "Output Category", type: "singleSelect" },
          { name: "Campaign Run", type: "multipleRecordLinks", linkedTable: "GDI Need-Date Campaign Runs" },
        ],
      },
    ],
    migrationPlan: [
      "1. Founder approves table names + field options.",
      "2. Create tables in GTM/platform base (confirm base id).",
      "3. Document fields in docs/gdi-need-date-airtable-fields.md.",
      "4. Add ensure-* script with --dry-run before --apply.",
      "5. Wire read path in need-period-airtable-reader (separate PR).",
      "6. Keep local fixtures as default until production reads approved.",
    ],
    localFixtureFallback: {
      path: "fixtures/group-demand-intelligence/need-date-demand/",
      supported: true,
    },
  };
}
