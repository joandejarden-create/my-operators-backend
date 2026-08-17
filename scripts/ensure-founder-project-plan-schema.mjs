/**
 * Ensure Founder Project Plan Airtable schema (fields + select options + views probe).
 *
 * Usage:
 *   node scripts/ensure-founder-project-plan-schema.mjs --dry-run
 *   node scripts/ensure-founder-project-plan-schema.mjs --execute
 *
 * Env:
 *   AIRTABLE_TOKEN (preferred) or AIRTABLE_PAT
 *   AIRTABLE_BASE_ID — primary base; falls back to AIRTABLE_GTM_BASE_ID when table not found
 *
 * Reports:
 *   reports/founder-project-plan-schema-report.json
 *   reports/founder-project-plan-views-manual.md
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  FOUNDER_PROJECT_PLAN_FIELDS_TO_CREATE,
  FOUNDER_PROJECT_PLAN_SELECT_NORMALIZATIONS,
  FOUNDER_PROJECT_PLAN_TABLE_ID,
  FOUNDER_PROJECT_PLAN_TABLE_NAME,
  FOUNDER_PROJECT_PLAN_VIEW_CONFIGS,
  FOUNDER_PROJECT_PLAN_VIEW_NAMES,
  buildFounderProjectPlanSelectOptionsManualMarkdown,
  buildFounderProjectPlanViewsManualMarkdown,
} from "../lib/gtm-owner-target/founder-project-plan-schema-config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_JSON = path.join(ROOT, "reports", "founder-project-plan-schema-report.json");
const REPORT_MANUAL = path.join(ROOT, "reports", "founder-project-plan-views-manual.md");
const REPORT_SELECT_MANUAL = path.join(ROOT, "reports", "founder-project-plan-select-options-manual.md");

const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;

function normalizeName(name) {
  return String(name || "").trim().toLowerCase();
}

function getToken() {
  const token =
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_GTM_API_KEY ||
    "";
  const source = process.env.AIRTABLE_TOKEN
    ? "AIRTABLE_TOKEN"
    : process.env.AIRTABLE_PAT
      ? "AIRTABLE_PAT"
      : process.env.AIRTABLE_GTM_API_KEY
        ? "AIRTABLE_GTM_API_KEY"
        : null;
  return { token: token.trim(), source };
}

function getBaseCandidates() {
  return [
    process.env.AIRTABLE_BASE_ID,
    process.env.AIRTABLE_GTM_BASE_ID,
  ]
    .map((id) => String(id || "").trim())
    .filter(Boolean)
    .filter((id, idx, arr) => arr.indexOf(id) === idx);
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

function mergeChoicesOrdered(existingChoices, targetOptions) {
  const byName = new Map();
  for (const c of existingChoices || []) {
    const name = String(c.name || "").trim();
    if (!name || byName.has(name)) continue;
    byName.set(name, {
      ...(c.id ? { id: c.id } : {}),
      name,
      ...(c.color ? { color: c.color } : {}),
    });
  }
  const ordered = [];
  for (const name of targetOptions) {
    if (!byName.has(name)) {
      byName.set(name, { name, color: "grayLight2" });
    }
    ordered.push(byName.get(name));
    byName.delete(name);
  }
  for (const c of byName.values()) {
    ordered.push(c);
  }
  return ordered;
}

function fieldExistsCaseInsensitive(fieldByNormName, name, aliases = []) {
  const candidates = [name, ...(aliases || [])].map(normalizeName);
  for (const key of candidates) {
    if (fieldByNormName.has(key)) {
      return fieldByNormName.get(key);
    }
  }
  return null;
}

function planViewSetup(existingViews) {
  const existingByName = new Map((existingViews || []).map((v) => [v.name, v]));
  const viewsToCreate = [];
  const viewsAlreadyPresent = [];
  for (const name of FOUNDER_PROJECT_PLAN_VIEW_NAMES) {
    const found = existingByName.get(name);
    if (found) {
      viewsAlreadyPresent.push({ name, viewId: found.id, viewType: found.type || "grid" });
    } else {
      viewsToCreate.push({ name });
    }
  }
  return {
    viewsToCreate,
    viewsAlreadyPresent,
    configs: FOUNDER_PROJECT_PLAN_VIEW_CONFIGS,
  };
}

async function resolveTable(token, baseCandidates) {
  const attempts = [];
  for (const baseId of baseCandidates) {
    const { res, json } = await metaFetch(baseId, token, "/tables");
    attempts.push({ baseId, status: res.status, ok: res.ok });
    if (!res.ok) continue;
    const table = (json.tables || []).find(
      (t) => t.id === FOUNDER_PROJECT_PLAN_TABLE_ID || t.name === FOUNDER_PROJECT_PLAN_TABLE_NAME
    );
    if (table) {
      return { baseId, table, attempts };
    }
  }
  return { baseId: null, table: null, attempts };
}

async function probeViewCreation(baseId, token, tableId, primaryFieldId) {
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/views`, {
    method: "POST",
    body: JSON.stringify({
      name: "__api_probe_founder_plan_do_not_use",
      type: "grid",
      visibleFieldIds: primaryFieldId ? [primaryFieldId] : undefined,
    }),
  });
  return { ok: res.ok, status: res.status, json };
}

async function main() {
  const { token, source } = getToken();
  if (!token) {
    throw new Error(
      "No Airtable token. Set AIRTABLE_TOKEN (schema.bases:read/write) or AIRTABLE_PAT scoped to the Founder Project Plan base."
    );
  }

  const baseCandidates = getBaseCandidates();
  if (!baseCandidates.length) {
    throw new Error("Set AIRTABLE_BASE_ID (and/or AIRTABLE_GTM_BASE_ID).");
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "execute",
    tokenSource: source,
    baseResolution: { candidates: baseCandidates, attempts: [], resolvedBaseId: null },
    tableId: FOUNDER_PROJECT_PLAN_TABLE_ID,
    tableName: FOUNDER_PROJECT_PLAN_TABLE_NAME,
    inspectedSchema: null,
    fieldsCreated: [],
    fieldsAlreadyPresent: [],
    selectFieldsUpdated: [],
    selectFieldsManual: [],
    selectFieldsSkipped: [],
    errors: [],
    viewsCreated: [],
    viewsManual: [],
    viewCreationProbe: null,
    warnings: [],
  };

  const { baseId, table, attempts } = await resolveTable(token, baseCandidates);
  report.baseResolution.attempts = attempts;
  report.baseResolution.resolvedBaseId = baseId;

  if (!table || !baseId) {
    report.errors.push({
      message:
        "Founder Project Plan table not found in AIRTABLE_BASE_ID or AIRTABLE_GTM_BASE_ID. Grant token access to the base containing tblpCg0QZ0kIPXihE.",
      attempts,
    });
    fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
    fs.writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
    console.error(JSON.stringify(report, null, 2));
    process.exit(1);
  }

  if (baseId !== process.env.AIRTABLE_BASE_ID) {
    report.warnings.push(
      `Table not in AIRTABLE_BASE_ID (${process.env.AIRTABLE_BASE_ID}); used ${baseId} (AIRTABLE_GTM_BASE_ID).`
    );
  }
  if (!process.env.AIRTABLE_TOKEN && source === "AIRTABLE_PAT") {
    report.warnings.push("AIRTABLE_TOKEN not set; used AIRTABLE_PAT instead.");
  }

  const fieldByName = new Map((table.fields || []).map((f) => [f.name, f]));
  const fieldByNormName = new Map((table.fields || []).map((f) => [normalizeName(f.name), f]));

  report.inspectedSchema = {
    fieldCount: table.fields.length,
    fields: (table.fields || []).map((f) => ({
      id: f.id,
      name: f.name,
      type: f.type,
      choices: f.options?.choices?.map((c) => c.name) || null,
    })),
    views: (table.views || []).map((v) => ({ id: v.id, name: v.name, type: v.type || "grid" })),
  };

  // --- Create missing fields ---
  for (const spec of FOUNDER_PROJECT_PLAN_FIELDS_TO_CREATE) {
    const existing = fieldExistsCaseInsensitive(fieldByNormName, spec.name, spec.aliases);
    if (existing) {
      const viaAlias = normalizeName(existing.name) !== normalizeName(spec.name);
      report.fieldsAlreadyPresent.push({
        requestedName: spec.name,
        existingName: existing.name,
        existingId: existing.id,
        existingType: existing.type,
        matchedViaAlias: viaAlias,
      });
      continue;
    }

    const payload = {
      name: spec.name,
      type: spec.type,
      ...(spec.options ? { options: spec.options } : {}),
    };

    if (DRY_RUN) {
      report.fieldsCreated.push({ ...payload, dryRun: true });
      fieldByName.set(spec.name, { id: null, name: spec.name, type: spec.type });
      fieldByNormName.set(normalizeName(spec.name), fieldByName.get(spec.name));
      continue;
    }

    const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      report.errors.push({
        action: "create_field",
        fieldName: spec.name,
        status: res.status,
        error: json,
      });
      continue;
    }
    report.fieldsCreated.push({ fieldName: spec.name, fieldId: json.id, type: spec.type });
    fieldByName.set(spec.name, json);
    fieldByNormName.set(normalizeName(spec.name), json);
  }

  // Refresh table schema after creates (for accurate field ids on select patches)
  if (!DRY_RUN && report.fieldsCreated.length) {
    const { res, json } = await metaFetch(baseId, token, "/tables");
    if (res.ok) {
      const refreshed = (json.tables || []).find((t) => t.id === table.id);
      if (refreshed) {
        for (const f of refreshed.fields || []) {
          fieldByName.set(f.name, f);
          fieldByNormName.set(normalizeName(f.name), f);
        }
      }
    }
  }

  // --- Normalize select options ---
  for (const target of FOUNDER_PROJECT_PLAN_SELECT_NORMALIZATIONS) {
    const field = fieldByName.get(target.fieldName);
    if (!field) {
      report.selectFieldsSkipped.push({
        fieldName: target.fieldName,
        reason: "field_not_found",
      });
      continue;
    }
    if (target.requireSingleSelect && field.type !== "singleSelect") {
      report.selectFieldsSkipped.push({
        fieldName: target.fieldName,
        fieldType: field.type,
        reason: "not_single_select",
        recommendation:
          target.fieldName === "Workstream"
            ? "Workstream is free text today. Convert to Single select in Airtable UI, then re-run this script to add standard options."
            : "Manual conversion to Single select recommended before option normalization.",
      });
      continue;
    }
    if (field.type !== "singleSelect") {
      report.selectFieldsSkipped.push({
        fieldName: target.fieldName,
        fieldType: field.type,
        reason: "unsupported_field_type",
      });
      continue;
    }

    const currentOptions = (field.options?.choices || []).map((c) => c.name);
    const optionsToAdd = target.targetOptions.filter((o) => !currentOptions.includes(o));
    const merged = mergeChoicesOrdered(field.options?.choices || [], target.targetOptions);

    if (!optionsToAdd.length) {
      report.fieldsAlreadyPresent.push({
        requestedName: target.fieldName,
        existingName: field.name,
        note: "select options already include all target values",
      });
      continue;
    }

    const updatePayload = {
      fieldName: target.fieldName,
      fieldId: field.id,
      fieldType: field.type,
      optionsToAdd,
      optionsBefore: currentOptions,
      optionsAfter: merged.map((c) => c.name),
      targetOptions: target.targetOptions,
    };

    if (DRY_RUN) {
      report.selectFieldsManual.push({ ...updatePayload, action: "manual_required_dry_run" });
      continue;
    }

    const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields/${field.id}`, {
      method: "PATCH",
      body: JSON.stringify({
        type: "singleSelect",
        options: { choices: merged },
      }),
    });
    if (res.ok) {
      report.selectFieldsUpdated.push(updatePayload);
    } else {
      report.selectFieldsManual.push({ ...updatePayload, action: "manual_required_api_patch_failed", apiStatus: res.status, apiError: json });
      report.errors.push({
        action: "patch_select_options",
        fieldName: target.fieldName,
        fieldId: field.id,
        status: res.status,
        error: json,
        manualFallback: REPORT_SELECT_MANUAL,
      });
    }
  }

  // --- Views ---
  const plan = planViewSetup(table.views || []);
  report.viewsManual = plan.configs.map((c) => c.name);

  if (!DRY_RUN) {
    const probe = await probeViewCreation(baseId, token, table.id, table.primaryFieldId);
    report.viewCreationProbe = { status: probe.status, supported: probe.ok };

    if (probe.ok) {
      for (const config of plan.viewsToCreate) {
        const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/views`, {
          method: "POST",
          body: JSON.stringify({ name: config.name, type: "grid" }),
        });
        if (res.ok) {
          report.viewsCreated.push({ name: config.name, viewId: json.id || null, configured: false });
        } else {
          report.errors.push({
            action: "create_view",
            viewName: config.name,
            status: res.status,
            error: json,
          });
        }
      }
    }
  } else {
    report.viewCreationProbe = { tested: false, reason: "dry_run" };
  }

  const manual = buildFounderProjectPlanViewsManualMarkdown(plan, {
    baseId,
    tableName: table.name,
    tableId: table.id,
  });
  const selectManualItems = [
    ...report.selectFieldsManual,
    ...report.selectFieldsSkipped.map((s) => ({
      fieldName: s.fieldName,
      fieldType: s.fieldType || "unknown",
      optionsToAdd: [],
      targetOptions: FOUNDER_PROJECT_PLAN_SELECT_NORMALIZATIONS.find((n) => n.fieldName === s.fieldName)?.targetOptions || [],
      recommendation: s.recommendation || s.reason,
    })),
  ];
  const selectManual = buildFounderProjectPlanSelectOptionsManualMarkdown(selectManualItems, {
    baseId,
    tableName: table.name,
    tableId: table.id,
  });
  fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
  fs.writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(REPORT_MANUAL, manual, "utf8");
  fs.writeFileSync(REPORT_SELECT_MANUAL, selectManual, "utf8");

  // --- Console summary ---
  console.log(`\n=== Founder Project Plan Schema Migration (${report.mode}) ===\n`);
  console.log(`Base: ${baseId}`);
  console.log(`Table: ${table.name} (${table.id})`);
  console.log(`Token: ${source}`);
  if (report.warnings.length) {
    console.log("\nWarnings:");
    for (const w of report.warnings) console.log(`  - ${w}`);
  }

  console.log("\n--- Fields created ---");
  if (report.fieldsCreated.length) {
    for (const f of report.fieldsCreated) console.log(`  + ${f.fieldName || f.name}${f.fieldId ? ` (${f.fieldId})` : ""}`);
  } else {
    console.log("  (none)");
  }

  console.log("\n--- Fields already present ---");
  if (report.fieldsAlreadyPresent.length) {
    for (const f of report.fieldsAlreadyPresent) {
      console.log(
        `  = ${f.requestedName}${f.existingName && f.existingName !== f.requestedName ? ` (as "${f.existingName}")` : ""}${f.note ? ` — ${f.note}` : ""}`
      );
    }
  } else {
    console.log("  (none)");
  }

  console.log("\n--- Select fields updated (API) ---");
  if (report.selectFieldsUpdated.length) {
    for (const s of report.selectFieldsUpdated) {
      console.log(`  ~ ${s.fieldName}: added ${s.optionsToAdd.join(", ")}`);
    }
  } else {
    console.log("  (none)");
  }

  console.log("\n--- Select fields — manual UI updates required ---");
  if (report.selectFieldsManual.length || report.selectFieldsSkipped.length) {
    for (const s of report.selectFieldsManual) {
      console.log(`  * ${s.fieldName}: add ${s.optionsToAdd.join(", ")}`);
    }
    for (const s of report.selectFieldsSkipped) {
      console.log(`  * ${s.fieldName}: ${s.recommendation || s.reason}`);
    }
    console.log(`  Instructions: ${REPORT_SELECT_MANUAL}`);
  } else {
    console.log("  (none)");
  }

  console.log("\n--- Views created (API) ---");
  if (report.viewsCreated.length) {
    for (const v of report.viewsCreated) console.log(`  + ${v.name}${v.viewId ? ` (${v.viewId})` : ""} — filters/sorts need manual setup`);
  } else {
    console.log("  (none — configure manually or API unsupported)");
  }

  console.log("\n--- Views to create manually ---");
  const manualViews = plan.viewsToCreate.map((v) => v.name);
  const needsManualConfig = [
    ...new Set([
      ...manualViews,
      ...plan.viewsAlreadyPresent.map((v) => v.name),
      ...report.viewsCreated.map((v) => v.name),
    ]),
  ];
  for (const name of needsManualConfig) {
    console.log(`  * ${name}`);
  }
  console.log(`\nManual instructions: ${REPORT_MANUAL}`);

  if (report.errors.length) {
    console.log("\n--- Errors ---");
    for (const e of report.errors) console.log(`  ! ${JSON.stringify(e)}`);
    process.exitCode = 1;
  }

  console.log(`\nReport: ${REPORT_JSON}`);
  console.log("\nNo record data was changed.");
}

main().catch((err) => {
  console.error("[ensure-founder-project-plan-schema]", err.message || err);
  process.exit(1);
});
