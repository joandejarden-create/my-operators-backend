/**
 * Audit Dealality master to-do Airtable structure (read-only).
 *
 *   node scripts/audit-dealality-master-todo-structure.mjs --dry-run
 *   node scripts/audit-dealality-master-todo-structure.mjs --dry-run --report reports/dealality-master-todo-structure-report.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  compareFields,
  fetchAllTables,
  getGtmConfig,
  isCandidateTaskTable,
  MASTER_TODO_VIEW_CONFIGS,
  missingSelectOptions,
  recommendMasterTable,
  serializeTableForAudit,
} from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import {
  MAP_MASTER_TODO,
  MASTER_TODO_DEFAULT_TABLE_ID,
  RECOMMENDED_MASTER_TODO_FIELDS,
  VAL_FPP_PRIORITY,
  VAL_MASTER_TODO_PHASE,
  VAL_MASTER_TODO_RELATED_AREA,
  VAL_MASTER_TODO_SOURCE,
  VAL_MASTER_TODO_STATUS,
  VAL_MASTER_TODO_WORKSTREAM,
} from "../lib/dealality-master-todo/master-todo-field-map.js";
import { MASTER_TODO_SEED } from "../lib/dealality-master-todo/master-todo-seed.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_JSON = path.resolve(
  ROOT,
  process.argv.includes("--report")
    ? process.argv[process.argv.indexOf("--report") + 1]
    : "reports/dealality-master-todo-structure-report.json"
);
const REPORT_MANUAL = path.resolve(ROOT, "reports/dealality-master-todo-manual-actions.md");
const REPORT_VIEWS = path.resolve(ROOT, "reports/dealality-master-todo-views-manual.md");

function buildManualMarkdown(report) {
  const lines = [
    "# Dealality Master To-Do — manual actions",
    "",
    `Generated: ${report.generatedAt}`,
    `Base: \`${report.baseId}\``,
    "",
    "## Selected source-of-truth table",
    "",
    `- **${report.recommendation.tableName}** (\`${report.recommendation.tableId}\`)`,
    `- ${report.recommendation.reason}`,
    `- New table needed: **${report.recommendation.newTableNeeded ? "YES — requires Joan approval" : "NO"}**`,
    "",
    "## Candidate tables inspected",
    "",
  ];
  for (const t of report.candidateTables) {
    lines.push(`### ${t.name} (\`${t.id}\`)`);
    lines.push(`- Fields: ${t.fieldCount} | Views: ${t.viewCount}`);
    lines.push("");
  }

  lines.push("## Missing fields (add in Airtable UI — do not change types without approval)");
  lines.push("");
  if (!report.fieldGap.missing.length) {
    lines.push("(none — all recommended fields present)");
  } else {
    for (const f of report.fieldGap.missing) {
      lines.push(`- **${f.name}** (${f.type}) — ${f.priority}`);
    }
  }
  lines.push("");
  lines.push("## Missing select options (add manually; API PATCH may return 422)");
  lines.push("");
  for (const item of report.missingSelectOptions) {
    lines.push(`### ${item.fieldName}`);
    if (item.note) lines.push(item.note);
    if (item.missing?.length) lines.push(`Add: ${item.missing.join(", ")}`);
    else if (item.recommendedOptions?.length) {
      lines.push(`Recommended options: ${item.recommendedOptions.join(", ")}`);
    } else {
      lines.push("(see note above)");
    }
    lines.push("");
  }

  lines.push("## Views to create");
  lines.push("");
  lines.push(`See ${REPORT_VIEWS}`);
  lines.push("");
  lines.push("## Upsert preview");
  lines.push("");
  lines.push(`Seed tasks: ${MASTER_TODO_SEED.length}`);
  lines.push("Run dry-run upsert:");
  lines.push("```");
  lines.push("node scripts/upsert-dealality-master-todo.mjs --dry-run");
  lines.push("```");
  lines.push("");
  lines.push("## Requires Joan approval");
  lines.push("");
  for (const item of report.approvalRequired) {
    lines.push(`- ${item}`);
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

function buildViewsManual(baseId, tableId, tableName) {
  const lines = [
    "# Dealality Master To-Do — view setup (manual)",
    "",
    `Base: \`${baseId}\``,
    `Table: **${tableName}** (\`${tableId}\`)`,
    "",
    "Filter master tasks with `{Source} = 'ChatGPT Master To-Do'` once that field exists.",
    "",
  ];
  for (const v of MASTER_TODO_VIEW_CONFIGS) {
    lines.push(`## ${v.name}`);
    lines.push("");
    lines.push("```");
    lines.push(v.filterFormula);
    lines.push("```");
    if (v.sort?.length) {
      lines.push("");
      lines.push(`Sort: ${v.sort.join(", ")}`);
    }
    if (v.notes) {
      lines.push("");
      lines.push(`Note: ${v.notes}`);
    }
    lines.push("");
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const allTables = await fetchAllTables(baseId, token);
  const candidates = allTables.filter(isCandidateTaskTable).map(serializeTableForAudit);
  const recommendation = recommendMasterTable(candidates);

  const selected =
    candidates.find((t) => t.id === recommendation.tableId) ||
    candidates.find((t) => t.id === MASTER_TODO_DEFAULT_TABLE_ID) ||
    null;

  const fieldGap = selected
    ? compareFields(selected.fields, RECOMMENDED_MASTER_TODO_FIELDS)
    : { missing: RECOMMENDED_MASTER_TODO_FIELDS, present: [] };

  const fieldByName = new Map((selected?.fields || []).map((f) => [f.name, f]));
  const selectTargets = [
    { fieldName: MAP_MASTER_TODO.status, options: VAL_MASTER_TODO_STATUS },
    { fieldName: MAP_MASTER_TODO.priority, options: VAL_FPP_PRIORITY },
    { fieldName: MAP_MASTER_TODO.phase, options: VAL_MASTER_TODO_PHASE },
    { fieldName: MAP_MASTER_TODO.source, options: VAL_MASTER_TODO_SOURCE },
    { fieldName: MAP_MASTER_TODO.relatedArea, options: VAL_MASTER_TODO_RELATED_AREA },
  ];

  const missingSelectOptionsReport = [];
  for (const target of selectTargets) {
    const field = fieldByName.get(target.fieldName);
    if (!field) {
      missingSelectOptionsReport.push({
        fieldName: target.fieldName,
        missing: target.options,
        note: "field does not exist",
      });
      continue;
    }
    if (field.type !== "singleSelect") {
      if (target.fieldName === MAP_MASTER_TODO.workstream) continue;
      missingSelectOptionsReport.push({
        fieldName: target.fieldName,
        fieldType: field.type,
        missing: target.options,
        note: "not single select — options must be added after type conversion (requires approval)",
      });
      continue;
    }
    const missing = missingSelectOptions(field, target.options);
    if (missing.length) {
      missingSelectOptionsReport.push({ fieldName: target.fieldName, missing });
    }
  }

  const workstreamField = fieldByName.get(MAP_MASTER_TODO.workstream);
  if (workstreamField && workstreamField.type !== "singleSelect") {
    missingSelectOptionsReport.push({
      fieldName: MAP_MASTER_TODO.workstream,
      fieldType: workstreamField.type,
      note: "Workstream is free text today. Master workstream values can be written as text; convert to single select later with Joan approval.",
      recommendedOptions: VAL_MASTER_TODO_WORKSTREAM,
    });
  }

  const approvalRequired = [];
  if (recommendation.newTableNeeded) {
    approvalRequired.push("Create new Dealality Master To-Do table OR confirm Founder Project Plan as shared table.");
  }
  if (fieldGap.missing.some((f) => f.priority === "required")) {
    approvalRequired.push("Add required fields listed above before treating table as full master to-do.");
  }
  if (missingSelectOptionsReport.some((x) => x.fieldName === MAP_MASTER_TODO.status && x.missing?.length)) {
    approvalRequired.push(
      "Add Status options: Drafted, Waiting, Deferred, Blocked, Needs Review (Completed already exists — do not add Done)."
    );
  }
  if (missingSelectOptionsReport.some((x) => x.fieldName === MAP_MASTER_TODO.phase)) {
    approvalRequired.push("Add master Phase options (GTM / Outreach, Pilot Conversion, etc.) alongside existing founder phases.");
  }
  approvalRequired.push("Review dry-run upsert report before running --execute.");

  const report = {
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    baseId,
    allTableNames: allTables.map((t) => ({ id: t.id, name: t.name })),
    candidateTables: candidates,
    recommendation,
    selectedTable: selected,
    fieldGap,
    missingSelectOptions: missingSelectOptionsReport,
    existingViews: selected?.views || [],
    recommendedViews: MASTER_TODO_VIEW_CONFIGS.map((v) => v.name),
    seedTaskCount: MASTER_TODO_SEED.length,
    approvalRequired,
  };

  fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
  fs.writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(REPORT_MANUAL, buildManualMarkdown(report), "utf8");
  if (selected) {
    fs.writeFileSync(
      REPORT_VIEWS,
      buildViewsManual(baseId, selected.id, selected.name),
      "utf8"
    );
  }

  console.log("Dealality master to-do structure audit (dry-run)");
  console.log(`Base: ${baseId}`);
  console.log(`Candidate tables: ${candidates.length}`);
  for (const t of candidates) {
    console.log(`  - ${t.name} (${t.id}) fields=${t.fieldCount} views=${t.viewCount}`);
  }
  console.log(`\nRecommendation: ${recommendation.tableName} (${recommendation.tableId})`);
  console.log(`New table needed: ${recommendation.newTableNeeded}`);
  console.log(`Missing fields: ${fieldGap.missing.length}`);
  console.log(`Missing select option groups: ${missingSelectOptionsReport.length}`);
  console.log(`\nReports:`);
  console.log(`  ${REPORT_JSON}`);
  console.log(`  ${REPORT_MANUAL}`);
  console.log(`  ${REPORT_VIEWS}`);
}

main().catch((err) => {
  console.error("[audit-dealality-master-todo-structure]", err.message || err);
  process.exit(1);
});
