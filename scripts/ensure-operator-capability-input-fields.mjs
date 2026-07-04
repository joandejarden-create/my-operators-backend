/**
 * Create P0 Operator Capability intake fields on Deals, Location, Strategic Intent.
 * Expands Project Type choices on Deals (merge, does not remove existing).
 *
 * Requires AIRTABLE_API_KEY with schema.bases:write and AIRTABLE_BASE_ID.
 *
 * Usage:
 *   node scripts/ensure-operator-capability-input-fields.mjs
 *   node scripts/ensure-operator-capability-input-fields.mjs --dry-run
 */
import "../load-env.js";
import {
  DEALS_TABLE,
  LOCATION_PROPERTY_TABLE,
  STRATEGIC_INTENT_TABLE,
} from "../api/schemas/deal-setup-fields.js";
import {
  DEALS_FIELDS,
  LOCATION_FIELDS,
  SI_FIELDS,
  CURRENT_OPERATING_MODEL_OPTIONS,
  PREFERRED_FUTURE_OPERATING_MODEL_OPTIONS,
  OPERATOR_STRATEGY_STATUS_OPTIONS,
  OPERATOR_CAPABILITY_PRIORITY_OPTIONS,
  OWNER_REPORTING_PACKAGE_OPTIONS,
  OWNER_REPORTING_FREQUENCY_OPTIONS,
  OPENING_TRANSITION_PHASE_OPTIONS,
  PRIMARY_MARKET_REGION_OPTIONS,
} from "../lib/operator-capability-inputs.js";
import { projectTypeOptionsToEnsure } from "../lib/project-type.js";

function choices(names) {
  return names.map((name) => ({ name }));
}

const FIELD_DEFS_BY_TABLE = {
  [DEALS_TABLE]: [
    {
      name: DEALS_FIELDS.currentOperatingModel,
      type: "singleSelect",
      description: "Canonical current operating model for Operator Capability Snapshot.",
      options: { choices: choices(CURRENT_OPERATING_MODEL_OPTIONS) },
    },
    {
      name: DEALS_FIELDS.openingTransitionPhase,
      type: "singleSelect",
      description: "Opening/reopening/transition phase for operator capability review.",
      options: { choices: choices(OPENING_TRANSITION_PHASE_OPTIONS) },
    },
  ],
  [LOCATION_PROPERTY_TABLE]: [
    {
      name: LOCATION_FIELDS.primaryMarketRegion,
      type: "singleSelect",
      description: "Primary market region for operator capability context (e.g. CALA).",
      options: { choices: choices(PRIMARY_MARKET_REGION_OPTIONS) },
    },
  ],
  [STRATEGIC_INTENT_TABLE]: [
    {
      name: SI_FIELDS.preferredFutureOperatingModel,
      type: "singleSelect",
      description: "Target operating model after this deal (P0 canonical).",
      options: { choices: choices(PREFERRED_FUTURE_OPERATING_MODEL_OPTIONS) },
    },
    {
      name: SI_FIELDS.operatorStrategyStatus,
      type: "singleSelect",
      description: "Where the owner is in operator planning (not a recommendation).",
      options: { choices: choices(OPERATOR_STRATEGY_STATUS_OPTIONS) },
    },
    {
      name: SI_FIELDS.operatorCapabilityPriorities,
      type: "multipleSelects",
      description: "Operator capability areas that may matter for this deal.",
      options: { choices: choices(OPERATOR_CAPABILITY_PRIORITY_OPTIONS) },
    },
    {
      name: SI_FIELDS.ownerReportingPackage,
      type: "multipleSelects",
      description: "Owner reporting and oversight package expected from an operator.",
      options: { choices: choices(OWNER_REPORTING_PACKAGE_OPTIONS) },
    },
    {
      name: SI_FIELDS.ownerReportingFrequency,
      type: "singleSelect",
      description: "Canonical reporting frequency (maps from Preferred Reporting Frequency).",
      options: { choices: choices(OWNER_REPORTING_FREQUENCY_OPTIONS) },
    },
  ],
};

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${path}`;
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

async function ensureField(tableId, fieldDef, existingNames, dryRun, report) {
  if (existingNames.has(fieldDef.name)) {
    report.fieldsPresent.push(fieldDef.name);
    console.log(`  exists: "${fieldDef.name}"`);
    return null;
  }
  if (dryRun) {
    console.log(`  [dry-run] would add: "${fieldDef.name}"`);
    report.fieldsCreated.push(fieldDef.name);
    return null;
  }
  const { res, json } = await metaFetch(
    process.env.AIRTABLE_BASE_ID,
    process.env.AIRTABLE_API_KEY,
    `/tables/${tableId}/fields`,
    { method: "POST", body: JSON.stringify(fieldDef) }
  );
  if (!res.ok) {
    throw new Error(`Add "${fieldDef.name}" failed ${res.status}: ${JSON.stringify(json)}`);
  }
  report.fieldsCreated.push(fieldDef.name);
  console.log(`  created: "${fieldDef.name}" (${json.id})`);
  return json;
}

async function mergeProjectTypeChoices(table, dryRun, report) {
  const field = (table.fields || []).find((f) => f.name === DEALS_FIELDS.projectType);
  if (!field) {
    console.warn(`  Project Type field not found on ${table.name} — add manually`);
    report.warnings.push("Project Type field missing");
    return;
  }
  const existing = new Set((field.options?.choices || []).map((c) => c.name));
  const toAdd = projectTypeOptionsToEnsure([...existing]);
  if (!toAdd.length) {
    console.log("  Project Type: all additional options already present");
    report.projectTypeMerged = [];
    return;
  }
  const mergedChoices = [
    ...(field.options?.choices || []),
    ...toAdd.map((name) => ({ name })),
  ];
  if (dryRun) {
    console.log(`  [dry-run] would add Project Type options: ${toAdd.join(", ")}`);
    report.projectTypeMerged = toAdd;
    return;
  }
  const { res, json } = await metaFetch(
    process.env.AIRTABLE_BASE_ID,
    process.env.AIRTABLE_API_KEY,
    `/tables/${table.id}/fields/${field.id}`,
    {
      method: "PATCH",
      body: JSON.stringify({
        options: { choices: mergedChoices },
      }),
    }
  );
  if (!res.ok) {
    throw new Error(`PATCH Project Type failed ${res.status}: ${JSON.stringify(json)}`);
  }
  console.log(`  Project Type: added options ${toAdd.join(", ")}`);
  report.projectTypeMerged = toAdd;
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token) throw new Error("Set AIRTABLE_API_KEY (schema.bases:write)");
  if (!baseId) throw new Error("Set AIRTABLE_BASE_ID");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);

  const report = {
    dryRun,
    tables: {},
    warnings: [],
    projectTypeMerged: [],
  };

  for (const [tableName, defs] of Object.entries(FIELD_DEFS_BY_TABLE)) {
    const table = (listJson.tables || []).find((t) => t.name === tableName);
    if (!table) {
      report.warnings.push(`Table not found: ${tableName}`);
      console.warn(`Table not found: ${tableName}`);
      continue;
    }
    console.log(`\n${tableName} (${table.id})`);
    const existingNames = new Set((table.fields || []).map((f) => f.name));
    const tableReport = { fieldsCreated: [], fieldsPresent: [] };
    for (const def of defs) {
      await ensureField(table.id, def, existingNames, dryRun, tableReport);
    }
    if (tableName === DEALS_TABLE) {
      try {
        await mergeProjectTypeChoices(table, dryRun, report);
      } catch (e) {
        const msg = e.message || String(e);
        report.warnings.push(`Project Type merge: ${msg}`);
        console.warn(`  Project Type merge skipped: ${msg}`);
      }
    }
    report.tables[tableName] = tableReport;
  }

  console.log("\n" + JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
