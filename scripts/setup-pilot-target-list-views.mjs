/**
 * Inspect and plan Pilot Target List outreach views (read-only — manual Airtable setup).
 *
 * Airtable Meta API can list views on a table but cannot create views or set
 * filters, sorts, or visible fields in this environment.
 *
 *   node scripts/setup-pilot-target-list-views.mjs --dry-run
 *   node scripts/setup-pilot-target-list-views.mjs --execute
 *   node scripts/setup-pilot-target-list-views.mjs --execute --update
 *
 * Reports:
 *   reports/pilot-target-list-views-report.json
 *   reports/pilot-target-list-views-manual.md
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { GTM_PILOT_TARGET_LIST_TABLE } from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  PILOT_TARGET_LIST_VIEW_NAMES,
  buildPilotTargetListViewsManualMarkdown,
  metaApiSupportsViewConfiguration,
  metaApiSupportsViewCreation,
  planPilotTargetListViewSetup,
} from "../lib/gtm-owner-target/pilot-target-list-view-config.js";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_JSON = path.join(ROOT, "reports", "pilot-target-list-views-report.json");
const REPORT_MANUAL = path.join(ROOT, "reports", "pilot-target-list-views-manual.md");

const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;
const UPDATE = process.argv.includes("--update");

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

async function probeViewCreation(baseId, token, tableId, primaryFieldId) {
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/views`, {
    method: "POST",
    body: JSON.stringify({
      name: "__api_probe_view_do_not_use",
      type: "grid",
      visibleFieldIds: primaryFieldId ? [primaryFieldId] : undefined,
    }),
  });
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
  const tableFieldNames = tableFields.map((f) => f.name);
  const existingViews = table.views || [];
  const plan = planPilotTargetListViewSetup(existingViews, tableFieldNames);

  const targetViewStatus = PILOT_TARGET_LIST_VIEW_NAMES.map((name) => {
    const found = existingViews.find((v) => v.name === name);
    return { name, exists: Boolean(found), viewId: found?.id || null, type: found?.type || null };
  });

  console.log(`Pilot Target List: ${table.name} (${table.id})`);
  console.log(`Mode: ${DRY_RUN ? "dry-run" : "execute"}${UPDATE ? " (update)" : ""}`);
  console.log(`Existing views: ${existingViews.length}`);
  for (const v of existingViews) {
    console.log(`  - ${v.name} (${v.id}, ${v.type || "grid"})`);
  }

  console.log("\nTarget outreach views:");
  for (const item of targetViewStatus) {
    console.log(`  ${item.exists ? "EXISTS" : "MISSING"}: ${item.name}${item.viewId ? ` (${item.viewId})` : ""}`);
  }

  let viewCreationProbe = null;
  let viewsCreated = [];
  let viewsUpdated = [];
  const manualActionsRequired = [
    "Create each missing grid view in Airtable UI",
    "Apply filter formula from manual report",
    "Configure sort order from manual report",
    "Show/hide and reorder visible fields from manual report",
  ];

  if (!DRY_RUN) {
    viewCreationProbe = await probeViewCreation(baseId, apiKey, table.id, table.primaryFieldId);
    const apiCanCreate = viewCreationProbe.ok;
    console.log(
      `\nView creation API probe: ${viewCreationProbe.status} (${apiCanCreate ? "supported" : "not supported"})`
    );

    if (apiCanCreate) {
      for (const config of plan.viewsToCreate) {
        const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/views`, {
          method: "POST",
          body: JSON.stringify({ name: config.name, type: "grid" }),
        });
        if (res.ok) {
          viewsCreated.push({ name: config.name, viewId: json.id || null });
          console.log("CREATED VIEW", config.name);
        } else {
          console.error("VIEW CREATE FAILED", config.name, res.status, JSON.stringify(json));
        }
      }
    } else if (plan.viewsToCreate.length) {
      console.log("Skipping API view creation — use manual instructions report.");
    }

    if (UPDATE && plan.viewsAlreadyPresent.length) {
      console.log(
        "View update via API is not supported — re-apply filter/sort/field layout manually using the report."
      );
      viewsUpdated = plan.viewsAlreadyPresent.map((v) => ({
        name: v.name,
        viewId: v.viewId,
        action: "manual_verification_required",
      }));
    }
  } else {
    console.log("\nViews to create:");
    for (const v of plan.viewsToCreate) {
      console.log(`  + ${v.name}`);
    }
    console.log("\nViews already present:");
    for (const v of plan.viewsAlreadyPresent) {
      console.log(`  = ${v.name} (${v.viewId})`);
    }
    console.log("\nManual configuration required for all views (filters, sorts, visible fields).");
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "execute",
    updateFlag: UPDATE,
    baseId,
    tableName: table.name,
    tableId: table.id,
    fieldCount: tableFields.length,
    metaApiSupportsViewListing: true,
    metaApiSupportsViewCreation: metaApiSupportsViewCreation() && viewCreationProbe?.ok === true,
    metaApiSupportsViewConfiguration: metaApiSupportsViewConfiguration(),
    viewCreationProbe: viewCreationProbe || { tested: false, reason: "dry_run" },
    existingViews: existingViews.map((v) => ({ id: v.id, name: v.name, type: v.type || "grid" })),
    targetViewStatus,
    viewsToCreate: plan.viewsToCreate.map((v) => v.name),
    viewsAlreadyPresent: plan.viewsAlreadyPresent.map((v) => ({
      name: v.name,
      viewId: v.viewId,
      viewType: v.viewType,
    })),
    viewsCreated,
    viewsUpdated,
    unsupportedConfigurationItems: plan.unsupportedConfiguration,
    manualActionsRequired,
    viewConfigs: plan.configs.map((c) => ({
      name: c.name,
      purpose: c.purpose,
      filterFormula: c.filterFormula,
      filterNotes: c.filterNotes || null,
      sort: c.sort,
      visibleFields: c.visibleFields,
      omittedOptionalFields: c.omittedOptionalFields || [],
      missingRequiredFields: c.missingRequiredFields || [],
      notes: c.notes || [],
    })),
  };

  const manual = buildPilotTargetListViewsManualMarkdown(plan, {
    baseId,
    tableName: table.name,
    tableId: table.id,
  });

  fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  fs.writeFileSync(REPORT_MANUAL, manual, "utf8");

  console.log("\nWrote", REPORT_JSON);
  console.log("Wrote", REPORT_MANUAL);

  if (DRY_RUN) {
    console.log("\nNo Airtable view changes made (dry-run). Follow manual report to create views.");
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
