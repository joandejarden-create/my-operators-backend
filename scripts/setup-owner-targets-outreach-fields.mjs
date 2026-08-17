/**
 * Add outreach / mail-merge fields to Pilot Target List (GTM base).
 *
 *   node scripts/setup-owner-targets-outreach-fields.mjs           # dry-run (default)
 *   node scripts/setup-owner-targets-outreach-fields.mjs --dry-run
 *   node scripts/setup-owner-targets-outreach-fields.mjs --execute
 *
 * Report: reports/setup-owner-targets-outreach-fields.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_PILOT_TARGET_LIST_TABLE,
  MAP_PILOT_TARGET_LIST,
} from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  buildFieldMappingReport,
  buildOutreachFieldSpecs,
  getExistingPilotTargetFieldNames,
  getPilotOutreachViewInstructions,
} from "../lib/gtm-owner-target/pilot-target-list-outreach.js";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;

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

async function main() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { res: listRes, json: listJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!listRes.ok) {
    throw new Error(`List tables failed (${listRes.status}): ${JSON.stringify(listJson)}`);
  }

  const table = (listJson.tables || []).find((t) => t.name === GTM_PILOT_TARGET_LIST_TABLE);
  if (!table) {
    throw new Error(
      `Table "${GTM_PILOT_TARGET_LIST_TABLE}" not found in GTM base ${baseId}. Create it in Airtable first.`
    );
  }

  const existing = new Set((table.fields || []).map((f) => f.name));
  const fieldSpecs = buildOutreachFieldSpecs();
  const mapping = buildFieldMappingReport(existing);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "execute",
    baseId,
    tableName: table.name,
    tableId: table.id,
    existingFieldCount: existing.size,
    existingFields: [...existing].sort(),
    baselineExistingFields: getExistingPilotTargetFieldNames().filter((n) => existing.has(n)),
    fieldMapping: mapping,
    fieldsWouldCreate: [],
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
    potentialDuplicatesAvoided: [
      {
        required: "Message Angle",
        reused: MAP_PILOT_TARGET_LIST.outreachMessageAngle,
        note: "Outreach Message Angle picklist + Why They Matter free text",
      },
      {
        required: "Last Contacted Date",
        reused: MAP_PILOT_TARGET_LIST.lastContactDate,
        note: "Existing Last Contact Date",
      },
      {
        required: "Outreach Status vs Status",
        reused: MAP_PILOT_TARGET_LIST.status,
        note: "Legacy Status kept; new Outreach Status field added for mail-merge workflow",
      },
    ],
    recommendedViews: getPilotOutreachViewInstructions(),
  };

  console.log(`Pilot Target List: ${table.name} (${table.id}) in base ${baseId}`);
  console.log(`Mode: ${report.mode}`);
  console.log("\nField mapping (required → existing → add new?):");
  for (const row of mapping) {
    console.log(
      `  ${row.requiredField}: ${row.existingField || "—"} | add=${row.addNewField}${row.notes ? ` | ${row.notes}` : ""}`
    );
  }

  for (const { mapKey, spec } of fieldSpecs) {
    if (existing.has(spec.name)) {
      report.fieldsSkipped.push(spec.name);
      console.log("SKIP (exists)", spec.name);
      continue;
    }
    if (DRY_RUN) {
      report.fieldsWouldCreate.push({ mapKey, name: spec.name, type: spec.type, options: spec.options || null });
      console.log("WOULD CREATE", spec.name, `(${spec.type})`);
      continue;
    }

    const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(spec),
    });
    if (!res.ok) {
      report.fieldsFailed.push({ name: spec.name, status: res.status, error: json });
      console.error("FIELD FAILED", spec.name, res.status, JSON.stringify(json));
    } else {
      report.fieldsCreated.push(spec.name);
      console.log("CREATED", spec.name);
    }
  }

  if (DRY_RUN) {
    console.log("\nNo Airtable changes made (dry-run). Use --execute to create missing fields.");
    console.log("\nRecommended views (create manually in Airtable):");
    for (const view of report.recommendedViews) {
      console.log(`  - ${view.name}: filter=${view.filter || "none"}`);
    }
  }

  const outPath = path.join(ROOT, "reports", "setup-owner-targets-outreach-fields.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("\nWrote", outPath);

  if (report.fieldsFailed.length) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
