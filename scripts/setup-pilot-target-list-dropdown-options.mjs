/**
 * Standardize Pilot Target List dropdown options safely.
 *
 * Usage:
 *   node scripts/setup-pilot-target-list-dropdown-options.mjs --dry-run
 *   node scripts/setup-pilot-target-list-dropdown-options.mjs --execute
 *   node scripts/setup-pilot-target-list-dropdown-options.mjs --execute --migrate-values
 *   node scripts/setup-pilot-target-list-dropdown-options.mjs --dry-run --report reports/pilot-target-list-dropdown-options-report.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  GTM_PILOT_TARGET_LIST_TABLE,
  MAP_PILOT_TARGET_LIST,
  VAL_PILOT_FIT,
  VAL_PILOT_OUTREACH_MESSAGE_ANGLE,
  VAL_PILOT_OUTREACH_SEGMENT,
  VAL_PILOT_OUTREACH_STATUS,
  VAL_PILOT_REGION,
  VAL_PILOT_RELATIONSHIP_STRENGTH,
  VAL_PILOT_RELEVANCE,
  VAL_PILOT_SEND_CHANNEL,
  VAL_PILOT_PRIORITY,
} from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import { assertGtmBaseConfigured, assertNotProductBase, getGtmAirtableBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_REPORT_PATH = "reports/pilot-target-list-dropdown-options-report.json";
const DEFAULT_MANUAL_PATH = "reports/pilot-target-list-dropdown-options-manual.md";

const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;
const MIGRATE_VALUES = process.argv.includes("--migrate-values");
const REPORT_PATH = argValue("--report", DEFAULT_REPORT_PATH);

const F = MAP_PILOT_TARGET_LIST;
const FIELD_TARGETS = [
  { key: "outreachSegment", name: F.outreachSegment, targetOptions: VAL_PILOT_OUTREACH_SEGMENT },
  { key: "pilotFit", name: F.pilotFit, targetOptions: VAL_PILOT_FIT },
  { key: "priority", name: F.priority, targetOptions: VAL_PILOT_PRIORITY },
  { key: "outreachStatus", name: F.outreachStatus, targetOptions: VAL_PILOT_OUTREACH_STATUS },
  { key: "sendChannel", name: F.sendChannel, targetOptions: VAL_PILOT_SEND_CHANNEL },
  { key: "outreachMessageAngle", name: F.outreachMessageAngle, targetOptions: VAL_PILOT_OUTREACH_MESSAGE_ANGLE },
  { key: "relationshipStrength", name: F.relationshipStrength, targetOptions: VAL_PILOT_RELATIONSHIP_STRENGTH },
  { key: "pilotRelevance", name: F.pilotRelevance, targetOptions: VAL_PILOT_RELEVANCE },
];

const VALUE_MIGRATION_MAP = {
  [F.pilotFit]: {
    Strong: "Strong Pilot Candidate",
    Possible: "Possible Pilot Candidate",
    Weak: "Weak Fit",
    "Not a Fit": "Not A Fit",
  },
  [F.relationshipStrength]: {
    Strong: "Strong Warm Relationship",
    Warm: "Known Contact",
    Light: "LinkedIn / Light Connection",
    LinkedIn: "LinkedIn / Light Connection",
    "Cold Outreach": "Cold",
    Cold: "Cold",
  },
  [F.outreachSegment]: {
    "Brand / Referral Source or Operator": "Brand / Referral Source",
  },
  [F.outreachStatus]: {
    "Converted to Pilot": "Converted To Pilot",
  },
  [F.outreachMessageAngle]: {
    "Operator Profile": "Operator Perspective",
    "Brand Profile": "Brand Criteria Input",
    "Referral Ask": "Warm Intro / Referral",
    "Feedback Ask": "Feedback / Perspective",
    "Feedback / Profile Input / Referral Only If Owner Opts In": "Owner-Opt-In Referral Only",
  },
};
const TARGET_OPTIONS_BY_FIELD = Object.fromEntries(FIELD_TARGETS.map((f) => [f.name, new Set(f.targetOptions)]));

function argValue(flag, fallback = "") {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] || fallback;
}

function ensureDir(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
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

function uniqueStrings(list) {
  return [...new Set((list || []).map((x) => String(x || "").trim()).filter(Boolean))];
}

function mergeChoices(existingChoices, targetOptions) {
  const byName = new Map();
  for (const c of existingChoices || []) {
    const name = String(c.name || "").trim();
    if (!name || byName.has(name)) continue;
    byName.set(name, { name, color: c.color });
  }
  for (const name of targetOptions) {
    if (!byName.has(name)) {
      byName.set(name, { name });
    }
  }
  return [...byName.values()];
}

function classifyField(field) {
  if (!field) return "missing";
  switch (field.type) {
    case "singleSelect":
      return "single_select";
    case "multipleSelects":
      return "multi_select";
    case "checkbox":
      return "checkbox";
    case "singleLineText":
      return "text";
    default:
      return field.type || "unknown";
  }
}

function buildRegionMigrationMap(values) {
  const out = {};
  for (const raw of values) {
    const v = String(raw || "").trim();
    if (!v) continue;
    const s = v.toLowerCase();
    if (["cala", "cala-first", "latam", "latin america"].includes(s)) out[v] = "CALA";
    else if (s.includes("mexic")) out[v] = "Mexico";
    else if (s.includes("caribbean")) out[v] = "Caribbean";
    else if (s.includes("central america")) out[v] = "Central America";
    else if (s.includes("south america")) out[v] = "South America";
    else if (s.includes("latin")) out[v] = "Latin America";
    else if (s.includes("us") || s.includes("united states") || s.includes("canada") || s.includes("north america")) out[v] = "United States / Canada";
    else if (s.includes("spain") || s.includes("europe")) out[v] = "Europe / Spain";
    else if (s.includes("global") || s.includes("multi")) out[v] = "Global / Multi-Region";
    else if (["unknown", "tbd", "na", "n/a"].includes(s)) out[v] = "Unknown / TBD";
  }
  return out;
}

function summarizeValues(records, fieldName) {
  const counts = {};
  for (const rec of records) {
    const val = rec.get(fieldName);
    const key = val == null || String(val).trim() === "" ? "(blank)" : String(val);
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

function buildManualMarkdown(report) {
  const lines = [
    "# Pilot Target List dropdown options — manual fallback",
    "",
    "Use this if Airtable Meta API option updates fail or are blocked by permissions.",
    "",
    `Base: \`${report.baseId}\``,
    `Table: **${report.tableName}** (\`${report.tableId}\`)`,
    "",
    "## Region dropdown",
    "",
    `- Legacy field: **${F.region}** (${report.region.fieldType})`,
    `- Structured dropdown field: **${F.pilotRegion}**`,
    "- If missing, create it as Single select with these options:",
    ...VAL_PILOT_REGION.map((x) => `  - ${x}`),
    "",
    "## Single-select option updates",
    "",
  ];
  for (const f of report.fields) {
    lines.push(`### ${f.fieldName}`);
    lines.push("");
    lines.push(`- Current type: ${f.fieldType}`);
    if (f.fieldType !== "single_select") {
      lines.push("- No option update attempted (field is not single-select).");
      lines.push("");
      continue;
    }
    lines.push(`- Add options (if missing): ${f.optionsToAdd.join(", ") || "(none)"}`);
    lines.push("");
  }
  if (report.migration && report.migration.proposedUpdates.length) {
    lines.push("## Value migration (manual if needed)");
    lines.push("");
    lines.push(
      "- Review report JSON `migration.proposedUpdates` before changing existing values."
    );
    lines.push("");
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const { res: tablesRes, json: tablesJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!tablesRes.ok) throw new Error(`Failed to list tables (${tablesRes.status}): ${JSON.stringify(tablesJson)}`);
  const table = (tablesJson.tables || []).find((t) => t.id === "tblgsKWuI25MWohAP" || t.name === GTM_PILOT_TARGET_LIST_TABLE);
  if (!table) throw new Error(`Pilot Target List not found in base ${baseId}`);

  const fieldByName = new Map((table.fields || []).map((f) => [f.name, f]));
  const requestedFields = [
    ...new Set([
      ...FIELD_TARGETS.map((x) => x.name),
      F.region,
      F.pilotRegion,
      F.warmIntro,
      F.status,
      F.category,
      F.doNotContact,
    ]),
  ];
  const existingFieldsForSelect = requestedFields.filter((name) => fieldByName.has(name));
  const records = await base(table.name).select({ fields: existingFieldsForSelect }).all();

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "execute",
    migrateValues: Boolean(MIGRATE_VALUES),
    baseId,
    tableName: table.name,
    tableId: table.id,
    totalRecords: records.length,
    fieldsInspected: {},
    fields: [],
    region: {},
    migration: {
      proposedUpdates: [],
      unmappedValues: [],
      updatedRecords: [],
      skippedRecords: [],
    },
    schemaUpdates: {
      fieldCreates: [],
      fieldUpdates: [],
      failures: [],
    },
  };

  for (const name of [
    F.region,
    F.outreachSegment,
    F.pilotFit,
    F.priority,
    F.outreachStatus,
    F.sendChannel,
    F.outreachMessageAngle,
    F.relationshipStrength,
    F.warmIntro,
    F.status,
    F.category,
    F.pilotRelevance,
    F.doNotContact,
  ]) {
    const field = fieldByName.get(name);
    report.fieldsInspected[name] = {
      fieldId: field?.id || null,
      fieldType: classifyField(field),
      options:
        field?.type === "singleSelect"
          ? (field.options?.choices || []).map((c) => c.name)
          : field?.options || null,
      valueCounts: summarizeValues(records, name),
    };
  }

  // Region handling: keep Region text, add/use Pilot Region dropdown safely.
  const legacyRegion = fieldByName.get(F.region);
  const pilotRegion = fieldByName.get(F.pilotRegion);
  const legacyValues = uniqueStrings(
    records.map((r) => {
      const v = r.get(F.region);
      return v == null ? "" : String(v);
    })
  );
  const regionMap = buildRegionMigrationMap(legacyValues);
  const regionUnmapped = legacyValues.filter((v) => !regionMap[v]);
  report.region = {
    legacyFieldName: F.region,
    legacyFieldType: classifyField(legacyRegion),
    pilotRegionFieldName: F.pilotRegion,
    pilotRegionExists: Boolean(pilotRegion),
    pilotRegionType: classifyField(pilotRegion),
    targetOptions: VAL_PILOT_REGION,
    legacyValueCounts: summarizeValues(records, F.region),
    mappedLegacyValues: regionMap,
    unmappedLegacyValues: regionUnmapped,
  };

  if (!pilotRegion) {
    const spec = {
      name: F.pilotRegion,
      type: "singleSelect",
      options: { choices: VAL_PILOT_REGION.map((name) => ({ name })) },
      description:
        "Structured outreach region focus (CALA-first, not CALA-only). Keep Region as legacy free text.",
    };
    report.schemaUpdates.fieldCreates.push(spec);
    if (!DRY_RUN) {
      const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields`, {
        method: "POST",
        body: JSON.stringify(spec),
      });
      if (!res.ok) {
        report.schemaUpdates.failures.push({
          action: "create_field",
          fieldName: F.pilotRegion,
          status: res.status,
          error: json,
        });
      } else {
        fieldByName.set(F.pilotRegion, json);
      }
    }
  }

  for (const target of FIELD_TARGETS) {
    const field = fieldByName.get(target.name);
    const fieldType = classifyField(field);
    const currentOptions = field?.type === "singleSelect" ? (field.options?.choices || []).map((c) => c.name) : [];
    const optionsToAdd = target.targetOptions.filter((o) => !currentOptions.includes(o));
    const currentValues = summarizeValues(records, target.name);
    const valuesOutsideTarget = Object.keys(currentValues).filter(
      (v) => v !== "(blank)" && !target.targetOptions.includes(v)
    );

    report.fields.push({
      fieldName: target.name,
      fieldId: field?.id || null,
      fieldType,
      currentOptions,
      targetOptions: target.targetOptions,
      optionsToAdd,
      valuesOutsideTarget,
      valueCounts: currentValues,
      wouldAffectValues: valuesOutsideTarget.length > 0,
    });

    if (fieldType !== "single_select") continue;
    if (!optionsToAdd.length) continue;

    const merged = mergeChoices(field.options?.choices || [], target.targetOptions);
    report.schemaUpdates.fieldUpdates.push({
      fieldName: target.name,
      fieldId: field.id,
      optionsBefore: currentOptions,
      optionsAfter: merged.map((c) => c.name),
      optionsAdded: optionsToAdd,
    });

    if (!DRY_RUN) {
      const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields/${field.id}`, {
        method: "PATCH",
        body: JSON.stringify({ options: { choices: merged } }),
      });
      if (!res.ok) {
        report.schemaUpdates.failures.push({
          action: "patch_field_options",
          fieldName: target.name,
          fieldId: field.id,
          status: res.status,
          error: json,
        });
      }
    }
  }

  if (MIGRATE_VALUES) {
    const patchByRecordId = new Map();
    for (const rec of records) {
      const patch = {};
      for (const [fieldName, map] of Object.entries(VALUE_MIGRATION_MAP)) {
        const curr = rec.get(fieldName);
        if (curr == null || String(curr).trim() === "") continue;
        const currText = String(curr);
        const targetSet = TARGET_OPTIONS_BY_FIELD[fieldName] || new Set();
        if (targetSet.has(currText)) continue;
        const mapped = map[String(curr)];
        if (!mapped) {
          if (Object.prototype.hasOwnProperty.call(map, currText) === false) {
            report.migration.unmappedValues.push({ recordId: rec.id, fieldName, value: currText });
          }
          continue;
        }
        if (mapped !== curr) {
          patch[fieldName] = mapped;
          report.migration.proposedUpdates.push({
            recordId: rec.id,
            fieldName,
            from: String(curr),
            to: mapped,
          });
        }
      }

      // Region legacy text -> Pilot Region dropdown (only when map is clear)
      const legacy = rec.get(F.region);
      const legacyText = legacy == null ? "" : String(legacy).trim();
      if (legacyText) {
        const mappedRegion = regionMap[legacyText];
        if (mappedRegion) {
          patch[F.pilotRegion] = mappedRegion;
          report.migration.proposedUpdates.push({
            recordId: rec.id,
            fieldName: F.pilotRegion,
            from: legacyText,
            to: mappedRegion,
          });
        } else {
          report.migration.unmappedValues.push({
            recordId: rec.id,
            fieldName: F.pilotRegion,
            value: legacyText,
            reason: "no_safe_region_mapping",
          });
        }
      }

      if (Object.keys(patch).length) {
        patchByRecordId.set(rec.id, patch);
      }
    }

    if (!DRY_RUN && patchByRecordId.size) {
      const api = new Airtable({
        apiKey: process.env.AIRTABLE_GTM_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_API_KEY,
      }).base(baseId);
      const entries = [...patchByRecordId.entries()];
      for (let i = 0; i < entries.length; i += 10) {
        const batch = entries.slice(i, i + 10).map(([id, fields]) => ({ id, fields }));
        await api(table.name).update(batch);
        for (const [id, fields] of entries.slice(i, i + 10)) {
          report.migration.updatedRecords.push({ recordId: id, fields });
        }
      }
    } else {
      for (const [id, fields] of patchByRecordId.entries()) {
        report.migration.skippedRecords.push({ recordId: id, fields });
      }
    }
  }

  const reportAbsPath = path.resolve(ROOT, REPORT_PATH);
  ensureDir(reportAbsPath);
  fs.writeFileSync(reportAbsPath, `${JSON.stringify(report, null, 2)}\n`);

  const manualAbsPath = path.resolve(ROOT, DEFAULT_MANUAL_PATH);
  ensureDir(manualAbsPath);
  fs.writeFileSync(manualAbsPath, buildManualMarkdown(report));

  console.log(`Pilot Target List dropdown setup (${report.mode})`);
  console.log(`Table: ${report.tableName} (${report.tableId})`);
  console.log(`Total records: ${report.totalRecords}`);
  console.log(`Field option updates planned: ${report.schemaUpdates.fieldUpdates.length}`);
  console.log(`Field creates planned: ${report.schemaUpdates.fieldCreates.length}`);
  if (MIGRATE_VALUES) {
    console.log(`Migration proposed updates: ${report.migration.proposedUpdates.length}`);
    console.log(`Migration unmapped values: ${report.migration.unmappedValues.length}`);
  } else {
    console.log("Migration disabled (pass --migrate-values to apply safe value mappings).");
  }
  console.log(`Report: ${reportAbsPath}`);
  console.log(`Manual fallback: ${manualAbsPath}`);

  if (report.schemaUpdates.failures.length) {
    console.error("Some schema updates failed. See report and manual fallback instructions.");
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("[setup-pilot-target-list-dropdown-options]", err.message || err);
  process.exit(1);
});

