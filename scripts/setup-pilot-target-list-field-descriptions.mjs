/**
 * Add or update Airtable field descriptions on Pilot Target List (descriptions only).
 *
 *   node scripts/setup-pilot-target-list-field-descriptions.mjs --dry-run
 *   node scripts/setup-pilot-target-list-field-descriptions.mjs --execute
 *   node scripts/setup-pilot-target-list-field-descriptions.mjs --execute --overwrite
 *
 * Reports:
 *   reports/pilot-target-list-field-descriptions-report.json
 *   reports/pilot-target-list-field-descriptions-manual.md (if Meta API unavailable)
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { GTM_PILOT_TARGET_LIST_TABLE } from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  buildManualDescriptionMarkdown,
  planPilotTargetListDescriptionUpdates,
} from "../lib/gtm-owner-target/pilot-target-list-field-descriptions.js";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_JSON = path.join(ROOT, "reports", "pilot-target-list-field-descriptions-report.json");
const REPORT_MANUAL = path.join(ROOT, "reports", "pilot-target-list-field-descriptions-manual.md");

const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;
const OVERWRITE = process.argv.includes("--overwrite");

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

async function probeDescriptionPatch(baseId, token, tableId, fieldId, description) {
  const { res, json } = await metaFetch(
    baseId,
    token,
    `/tables/${tableId}/fields/${fieldId}`,
    {
      method: "PATCH",
      body: JSON.stringify({ description }),
    }
  );
  return { ok: res.ok, status: res.status, json };
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
    throw new Error(`Table "${GTM_PILOT_TARGET_LIST_TABLE}" not found in base ${baseId}`);
  }

  const tableFields = table.fields || [];
  const plan = planPilotTargetListDescriptionUpdates(tableFields, { overwrite: OVERWRITE });

  let metaApiSupportsDescriptionUpdates = null;
  const pendingUpdates = [...plan.descriptionsToAdd, ...plan.descriptionsToOverwrite];

  console.log(`Pilot Target List: ${table.name} (${table.id})`);
  console.log(`Mode: ${DRY_RUN ? "dry-run" : "execute"}${OVERWRITE ? " (overwrite)" : ""}`);
  console.log(`Fields on table: ${tableFields.length}`);
  console.log(`Descriptions already present (skip unless --overwrite): ${plan.descriptionsAlreadyPresent.length}`);
  console.log(`Descriptions to add: ${plan.descriptionsToAdd.length}`);
  console.log(`Descriptions to overwrite: ${plan.descriptionsToOverwrite.length}`);
  console.log(`Fields missing from table: ${plan.fieldsMissingFromTable.length}`);
  console.log(`Fields with no description map: ${plan.fieldsWithNoDescriptionMap.length}`);

  if (plan.descriptionsAlreadyPresent.length) {
    console.log("\nExisting descriptions (skipped):");
    for (const name of plan.descriptionsAlreadyPresent) {
      const field = tableFields.find((f) => f.name === name);
      console.log(`  ${name}: ${field?.description || ""}`);
    }
  }

  if (plan.descriptionsToAdd.length) {
    console.log("\nWould add descriptions:");
    for (const item of plan.descriptionsToAdd) {
      console.log(`  ${item.fieldName}`);
    }
  }

  if (plan.descriptionsToOverwrite.length) {
    console.log("\nWould overwrite descriptions:");
    for (const item of plan.descriptionsToOverwrite) {
      console.log(`  ${item.fieldName}`);
    }
  }

  if (plan.fieldsMissingFromTable.length) {
    console.log("\nMap entries not on table:");
    for (const name of plan.fieldsMissingFromTable) {
      console.log(`  ${name}`);
    }
  }

  if (!DRY_RUN && pendingUpdates.length) {
    const probe = pendingUpdates[0];
    const probeResult = await probeDescriptionPatch(
      baseId,
      apiKey,
      table.id,
      probe.fieldId,
      probe.targetDescription
    );
    metaApiSupportsDescriptionUpdates = probeResult.ok;

    if (probeResult.ok) {
      plan.fieldsUpdated.push({
        fieldName: probe.fieldName,
        fieldId: probe.fieldId,
        action: plan.descriptionsToOverwrite.some((x) => x.fieldId === probe.fieldId)
          ? "overwrite"
          : "add",
      });
      console.log("PATCH probe OK — Meta API supports field description updates");
    } else {
      console.error(
        "PATCH probe failed — Meta API may not support description updates:",
        probeResult.status,
        JSON.stringify(probeResult.json)
      );
    }

    if (metaApiSupportsDescriptionUpdates) {
      const remaining = pendingUpdates.slice(1);
      for (const item of remaining) {
        const { res, json } = await metaFetch(
          baseId,
          apiKey,
          `/tables/${table.id}/fields/${item.fieldId}`,
          {
            method: "PATCH",
            body: JSON.stringify({ description: item.targetDescription }),
          }
        );
        if (!res.ok) {
          plan.fieldsFailed.push({
            fieldName: item.fieldName,
            fieldId: item.fieldId,
            status: res.status,
            error: json,
          });
          console.error("UPDATE FAILED", item.fieldName, res.status, JSON.stringify(json));
        } else {
          plan.fieldsUpdated.push({
            fieldName: item.fieldName,
            fieldId: item.fieldId,
            action: plan.descriptionsToOverwrite.some((x) => x.fieldId === item.fieldId)
              ? "overwrite"
              : "add",
          });
          console.log("UPDATED", item.fieldName);
        }
      }
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "execute",
    overwrite: OVERWRITE,
    baseId,
    tableName: table.name,
    tableId: table.id,
    fieldCount: tableFields.length,
    metaApiSupportsDescriptionUpdates:
      metaApiSupportsDescriptionUpdates ?? "not_tested_in_dry_run",
    fieldsFound: plan.fieldsFound,
    descriptionsAlreadyPresent: plan.descriptionsAlreadyPresent,
    descriptionsToAdd: plan.descriptionsToAdd.map((x) => ({
      fieldName: x.fieldName,
      fieldId: x.fieldId,
      targetDescription: x.targetDescription,
    })),
    descriptionsToOverwrite: plan.descriptionsToOverwrite.map((x) => ({
      fieldName: x.fieldName,
      fieldId: x.fieldId,
      existingDescription: x.existingDescription,
      targetDescription: x.targetDescription,
    })),
    descriptionsUnchanged: plan.descriptionsUnchanged,
    fieldsUpdated: plan.fieldsUpdated,
    fieldsSkippedExisting: plan.fieldsSkippedExisting,
    fieldsMissingFromTable: plan.fieldsMissingFromTable,
    fieldsWithNoDescriptionMap: plan.fieldsWithNoDescriptionMap,
    fieldsFailed: plan.fieldsFailed,
  };

  fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  console.log("\nWrote", REPORT_JSON);

  if (metaApiSupportsDescriptionUpdates === false) {
    const manual = buildManualDescriptionMarkdown(plan, {
      baseId,
      tableName: table.name,
      tableId: table.id,
    });
    fs.writeFileSync(REPORT_MANUAL, manual, "utf8");
    console.log("Wrote manual instructions:", REPORT_MANUAL);
    process.exit(1);
  }

  if (DRY_RUN) {
    console.log("\nNo Airtable changes made (dry-run). Use --execute to apply descriptions.");
    const manual = buildManualDescriptionMarkdown(plan, {
      baseId,
      tableName: table.name,
      tableId: table.id,
    });
    fs.writeFileSync(REPORT_MANUAL, manual, "utf8");
    console.log("Wrote manual fallback:", REPORT_MANUAL);
  }

  if (plan.fieldsFailed.length) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
