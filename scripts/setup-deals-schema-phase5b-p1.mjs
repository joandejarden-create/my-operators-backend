#!/usr/bin/env node
/**
 * Ensure approved Deals schema Phase 5B P1 fields + Location Other text columns.
 * Idempotent — skips fields that already exist. Does not modify or delete records.
 *
 * Approved fields:
 *   - Preferred Operator Management Structure (Market - Performance)
 *   - Operator Structure Intent (Strategic Intent)
 *   - Ownership Type Other Text (Location) — UI has "Other" option
 *   - Zoning Status Other Text (Location) — UI has "Other" option
 *
 * Usage:
 *   node scripts/setup-deals-schema-phase5b-p1.mjs --dry-run
 *   node scripts/setup-deals-schema-phase5b-p1.mjs --apply
 *
 * Requires:
 *   AIRTABLE_API_KEY (or AIRTABLE_PAT) with schema.bases:read + schema.bases:write
 *   AIRTABLE_BASE_ID
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  DEALS_TABLE,
  LOCATION_PROPERTY_TABLE,
  MARKET_PERFORMANCE_TABLE,
  STRATEGIC_INTENT_TABLE,
} from "../api/schemas/deal-setup-fields.js";
import {
  OAS_PREFERRED_OPERATOR_MANAGEMENT_STRUCTURE_OPTIONS,
  OAS_DEAL_MP_FIELD_NAMES,
  OAS_DEAL_SI_FIELD_NAMES,
} from "../lib/operator-alignment-field-options.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "deals-schema-phase5b-p1-setup.json");
const REPORT_MD = join(ROOT, "reports", "deals-schema-phase5b-p1-setup.md");

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;

/** UI form options include "Other" for both fields (deal-setup-form-options.json). */
const INCLUDE_LOCATION_OTHER_TEXT = true;

function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}

function buildFieldSpecs() {
  const specs = [
    {
      tableName: MARKET_PERFORMANCE_TABLE,
      tableKey: "marketPerformance",
      fields: [
        {
          name: OAS_DEAL_MP_FIELD_NAMES.preferredOperatorManagementStructure,
          type: "multipleSelects",
          description:
            "Phase 5B P1 — owner operator management path (split from brand Preferred Deal Structure).",
          options: { choices: choices(OAS_PREFERRED_OPERATOR_MANAGEMENT_STRUCTURE_OPTIONS) },
        },
      ],
    },
    {
      tableName: STRATEGIC_INTENT_TABLE,
      tableKey: "strategicIntent",
      fields: [
        {
          name: OAS_DEAL_SI_FIELD_NAMES.operatorStructureIntent,
          type: "singleSelect",
          description: "Phase 5B P1 — clarifies owner operator path when MP structure is ambiguous.",
          options: { choices: choices(OAS_PREFERRED_OPERATOR_MANAGEMENT_STRUCTURE_OPTIONS) },
        },
      ],
    },
  ];

  if (INCLUDE_LOCATION_OTHER_TEXT) {
    specs.push({
      tableName: LOCATION_PROPERTY_TABLE,
      tableKey: "location",
      fields: [
        {
          name: "Ownership Type Other Text",
          type: "multilineText",
          description: "Free text when Ownership Type includes Other (Deal Setup form).",
        },
        {
          name: "Zoning Status Other Text",
          type: "multilineText",
          description: "Free text when Zoning Status is Other (Deal Setup form).",
        },
      ],
    });
  }

  return specs;
}

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

async function ensureField(baseId, token, tableId, fieldDef, existingNames, report) {
  if (existingNames.has(fieldDef.name)) {
    report.present.push(fieldDef.name);
    console.log(`  exists: "${fieldDef.name}"`);
    return { status: "present" };
  }

  const payload = {
    name: fieldDef.name,
    type: fieldDef.type,
    ...(fieldDef.description ? { description: fieldDef.description } : {}),
    ...(fieldDef.options ? { options: fieldDef.options } : {}),
  };

  if (DRY_RUN) {
    report.wouldCreate.push(fieldDef.name);
    console.log(`  [dry-run] would create: "${fieldDef.name}" (${fieldDef.type})`);
    return { status: "would_create" };
  }

  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const err = json?.error?.message || JSON.stringify(json);
    report.failed.push({ field: fieldDef.name, error: err, status: res.status });
    console.error(`  FAIL "${fieldDef.name}": ${res.status} ${err}`);
    return { status: "failed", error: err };
  }

  report.created.push({ name: fieldDef.name, id: json.id, type: fieldDef.type });
  existingNames.add(fieldDef.name);
  console.log(`  created: "${fieldDef.name}" (${json.id})`);
  await new Promise((r) => setTimeout(r, 220));
  return { status: "created", id: json.id };
}

function buildMarkdownReport(report) {
  const lines = [
    "# Deals Schema Phase 5B P1 Setup Report",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: ${report.mode}`,
    `Base: \`${report.baseId}\``,
    "",
    "> Schema-only. No records modified.",
    "",
    "## Summary",
    "",
    `| Metric | Count |`,
    `|--------|-------|`,
    `| Fields already present | ${report.present.length} |`,
    `| Fields created | ${report.created.length} |`,
    `| Fields would create (dry-run) | ${report.wouldCreate.length} |`,
    `| Fields failed | ${report.failed.length} |`,
    "",
  ];

  if (report.created.length) {
    lines.push("## Created", "", ...report.created.map((f) => `- \`${f.name}\` (${f.type})`), "");
  }
  if (report.wouldCreate.length) {
    lines.push("## Would create (dry-run)", "", ...report.wouldCreate.map((f) => `- \`${f}\``), "");
  }
  if (report.present.length) {
    lines.push("## Already present", "", ...report.present.map((f) => `- \`${f}\``), "");
  }
  if (report.failed.length) {
    lines.push("## Failed", "");
    for (const f of report.failed) {
      lines.push(`- \`${f.field}\`: ${f.error}`);
    }
    lines.push("");
  }
  if (report.warnings.length) {
    lines.push("## Warnings", "", ...report.warnings.map((w) => `- ${w}`), "");
  }

  return lines.join("\n");
}

async function main() {
  const token =
    process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;

  if (!token) {
    console.error("Missing AIRTABLE_API_KEY, AIRTABLE_PAT, or AIRTABLE_TOKEN");
    process.exit(1);
  }
  if (!baseId) {
    console.error("Missing AIRTABLE_BASE_ID");
    process.exit(1);
  }

  console.log(`[setup-deals-schema-phase5b-p1] mode=${DRY_RUN ? "dry-run" : "apply"}`);

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    console.error("Meta API list tables failed:", res.status, json.error?.message || json);
    process.exit(1);
  }

  const tables = json.tables || [];
  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    baseId,
    includeLocationOtherText: INCLUDE_LOCATION_OTHER_TEXT,
    tables: {},
    present: [],
    created: [],
    wouldCreate: [],
    failed: [],
    warnings: [],
  };

  for (const spec of buildFieldSpecs()) {
    const table = tables.find((t) => t.name === spec.tableName);
    console.log(`\n${spec.tableName}`);
    if (!table) {
      const msg = `Table not found: ${spec.tableName}`;
      report.warnings.push(msg);
      console.warn(`  WARN: ${msg}`);
      report.tables[spec.tableKey] = { found: false, tableName: spec.tableName };
      continue;
    }

    const existingNames = new Set((table.fields || []).map((f) => f.name));
    const tableReport = { found: true, tableId: table.id, results: [] };

    for (const fieldDef of spec.fields) {
      const result = await ensureField(baseId, token, table.id, fieldDef, existingNames, report);
      tableReport.results.push({ field: fieldDef.name, ...result });
    }

    report.tables[spec.tableKey] = tableReport;
  }

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  writeFileSync(REPORT_MD, buildMarkdownReport(report));

  console.log("\nWrote", REPORT_JSON);
  console.log("Wrote", REPORT_MD);
  console.log(
    `Summary: present=${report.present.length} created=${report.created.length} ` +
      `wouldCreate=${report.wouldCreate.length} failed=${report.failed.length}`
  );

  if (report.failed.length) process.exit(1);
  if (DRY_RUN && report.wouldCreate.length) {
    console.log("\nRe-run with --apply to create missing fields.");
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
