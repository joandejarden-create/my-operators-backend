#!/usr/bin/env node
/**
 * Ensure mixed-use / branded-residence intake fields on Deals workflow tables.
 * Idempotent — skips fields that already exist. Does not modify or delete records.
 *
 * Usage:
 *   node scripts/setup-deals-schema-mixed-use-intake.mjs --dry-run
 *   node scripts/setup-deals-schema-mixed-use-intake.mjs --apply
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
  CONTACT_UPLOADS_TABLE,
} from "../api/schemas/deal-setup-fields.js";
import {
  MIXED_USE_INTAKE_FIELD_NAMES,
  BRANDED_RESIDENCE_PROGRAM_MODEL_OPTIONS,
  CONDO_RENTAL_PROGRAM_MODEL_OPTIONS,
  FB_OPERATING_MODEL_OPTIONS,
  DEVELOPMENT_PROFORMA_AVAILABLE_OPTIONS,
} from "../lib/mixed-use-intake-field-options.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "deals-schema-mixed-use-intake-setup.json");
const REPORT_MD = join(ROOT, "reports", "deals-schema-mixed-use-intake-setup.md");

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;

function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}

function buildFieldSpecs() {
  const F = MIXED_USE_INTAKE_FIELD_NAMES;
  return [
    {
      tableName: LOCATION_PROPERTY_TABLE,
      tableKey: "location",
      fields: [
        {
          name: F.numberOfCondoUnits,
          type: "number",
          description: "Count of branded residence / condo units when mixed-use program includes residential.",
          options: { precision: 0 },
        },
      ],
    },
    {
      tableName: DEALS_TABLE,
      tableKey: "deals",
      fields: [
        {
          name: F.fbOperatingModel,
          type: "singleSelect",
          description: "How ground-floor or flagship F&B is operated (hotel vs third-party lease).",
          options: { choices: choices(FB_OPERATING_MODEL_OPTIONS) },
        },
      ],
    },
    {
      tableName: MARKET_PERFORMANCE_TABLE,
      tableKey: "marketPerformance",
      fields: [
        {
          name: F.stabilizedAdrUsd,
          type: "currency",
          description: "Owner underwriting stabilized average daily rate in USD (development proforma).",
          options: { precision: 0, symbol: "USD" },
        },
        {
          name: F.stabilizedOccupancyPct,
          type: "percent",
          description: "Owner underwriting stabilized occupancy (development proforma).",
          options: { precision: 1 },
        },
      ],
    },
    {
      tableName: STRATEGIC_INTENT_TABLE,
      tableKey: "strategicIntent",
      fields: [
        {
          name: F.brandedResidenceProgramModel,
          type: "singleSelect",
          description: "How branded residences relate to the hotel component.",
          options: { choices: choices(BRANDED_RESIDENCE_PROGRAM_MODEL_OPTIONS) },
        },
        {
          name: F.condoRentalProgramModel,
          type: "singleSelect",
          description: "Condo owner rental participation and operator role.",
          options: { choices: choices(CONDO_RENTAL_PROGRAM_MODEL_OPTIONS) },
        },
      ],
    },
    {
      tableName: CONTACT_UPLOADS_TABLE,
      tableKey: "contactUploads",
      fields: [
        {
          name: F.developmentProformaAvailable,
          type: "singleSelect",
          description: "Whether a development-stage hotel operating proforma is available.",
          options: { choices: choices(DEVELOPMENT_PROFORMA_AVAILABLE_OPTIONS) },
        },
      ],
    },
  ];
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

async function syncSelectChoices(baseId, token, tableId, fieldId, fieldDef, existingField, report) {
  const existingChoices = existingField?.options?.choices || [];
  const desiredChoices = fieldDef.options?.choices || [];
  const mergedChoices = desiredChoices.map((choice, i) => {
    const existing = existingChoices[i];
    const entry = { name: choice.name };
    if (existing?.id) entry.id = existing.id;
    if (existing?.color) entry.color = existing.color;
    return entry;
  });

  const payload = {
    options: {
      ...(existingField?.options || {}),
      choices: mergedChoices,
    },
  };
  if (DRY_RUN) {
    report.wouldSyncChoices.push(fieldDef.name);
    console.log(`  [dry-run] would sync choices: "${fieldDef.name}"`);
    return { status: "would_sync_choices" };
  }

  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields/${fieldId}`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const err = json?.error?.message || JSON.stringify(json);
    report.failed.push({ field: fieldDef.name, error: err, status: res.status });
    console.error(`  FAIL sync choices "${fieldDef.name}": ${res.status} ${err}`);
    return { status: "failed", error: err };
  }

  report.syncedChoices.push(fieldDef.name);
  console.log(`  synced choices: "${fieldDef.name}"`);
  await new Promise((r) => setTimeout(r, 220));
  return { status: "synced_choices" };
}

async function ensureField(baseId, token, tableId, fieldDef, tableFields, report) {
  const existingField = (tableFields || []).find((f) => f.name === fieldDef.name);
  if (existingField) {
    if (fieldDef.type === "singleSelect" && fieldDef.options?.choices?.length) {
      const desired = fieldDef.options.choices.map((c) => c.name).sort().join("|");
      const current = (existingField.options?.choices || []).map((c) => c.name).sort().join("|");
      if (desired !== current) {
        return syncSelectChoices(baseId, token, tableId, existingField.id, fieldDef, existingField, report);
      }
    }
    report.present.push(fieldDef.name);
    console.log(`  exists: "${fieldDef.name}"`);
    return { status: "present" };
  }

  const existingNames = new Set((tableFields || []).map((f) => f.name));

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
    "# Mixed-Use Intake Schema Setup Report",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: ${report.mode}`,
    `Base: \`${report.baseId}\``,
    "",
    "> Schema-only. No records modified.",
    "",
    "## Summary",
    "",
    "| Metric | Count |",
    "|--------|-------|",
    `| Fields already present | ${report.present.length} |`,
    `| Fields created | ${report.created.length} |`,
    `| Fields would create (dry-run) | ${report.wouldCreate.length} |`,
    `| Select choices synced | ${report.syncedChoices.length} |`,
    `| Select choices would sync (dry-run) | ${report.wouldSyncChoices.length} |`,
    `| Fields failed | ${report.failed.length} |`,
    "",
  ];

  if (report.created.length) {
    lines.push("## Created", "", ...report.created.map((f) => `- \`${f.name}\` (${f.type})`), "");
  }
  if (report.wouldCreate.length) {
    lines.push("## Would create (dry-run)", "", ...report.wouldCreate.map((f) => `- \`${f}\``), "");
  }
  if (report.syncedChoices.length) {
    lines.push("## Synced select choices", "", ...report.syncedChoices.map((f) => `- \`${f}\``), "");
  }
  if (report.wouldSyncChoices.length) {
    lines.push("## Would sync select choices (dry-run)", "", ...report.wouldSyncChoices.map((f) => `- \`${f}\``), "");
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

  return lines.join("\n");
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  if (!baseId || !token) {
    console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY / AIRTABLE_PAT / AIRTABLE_TOKEN");
    process.exit(1);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    baseId,
    present: [],
    wouldCreate: [],
    created: [],
    syncedChoices: [],
    wouldSyncChoices: [],
    failed: [],
    warnings: [],
  };

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    console.error("Failed to load base schema:", json?.error?.message || res.status);
    process.exit(1);
  }

  const tableByName = new Map((json.tables || []).map((t) => [t.name, t]));
  const specs = buildFieldSpecs();

  console.log(`Mode: ${report.mode}`);
  console.log(`Base: ${baseId}`);
  console.log("");

  for (const spec of specs) {
    const table = tableByName.get(spec.tableName);
    if (!table) {
      report.warnings.push(`Table not found: ${spec.tableName}`);
      console.warn(`SKIP table not found: ${spec.tableName}`);
      continue;
    }
    const existingNames = new Set((table.fields || []).map((f) => f.name));
    console.log(`Table: ${spec.tableName}`);
    for (const fieldDef of spec.fields) {
      await ensureField(baseId, token, table.id, fieldDef, table.fields || [], report);
    }
    console.log("");
  }

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  writeFileSync(REPORT_MD, buildMarkdownReport(report));

  console.log(`Report: ${REPORT_MD}`);
  if (report.failed.length) process.exit(1);
  if (DRY_RUN && report.wouldCreate.length) {
    console.log("\nReview report, then: node scripts/setup-deals-schema-mixed-use-intake.mjs --apply");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
