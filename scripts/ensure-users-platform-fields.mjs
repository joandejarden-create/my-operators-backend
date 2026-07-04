#!/usr/bin/env node
/**
 * Add platform-user fields on the Users table (consolidation from User Management).
 * Copies single-select choices from legacy User Management when present.
 *
 * Requires AIRTABLE_API_KEY with schema.bases:write
 *
 *   node scripts/ensure-users-platform-fields.mjs
 *   node scripts/ensure-users-platform-fields.mjs --dry-run
 */
import "../load-env.js";
import {
  LEGACY_USER_MANAGEMENT_TABLE_ID,
  PLATFORM_USERS_COMPANY_TABLE_ID,
  PLATFORM_USERS_TABLE_ID,
  PUF,
  REGION_CHECKBOX_FIELDS,
} from "../lib/airtable/platform-users-table.js";

const PROFILE_NAMES = ["Profile", "Profile Picture", "Headshot", "Photo", "Avatar"];

const STATIC_FIELDS = [
  { name: PUF.companyTitle, type: "singleLineText" },
  { name: PUF.companyEmail, type: "email" },
  { name: PUF.country, type: "singleLineText" },
  {
    name: PUF.closedDeals,
    type: "number",
    options: { precision: 0 },
  },
  {
    name: PUF.uniqueBrandsDeals,
    type: "number",
    options: { precision: 0 },
  },
  {
    name: PUF.submittedBids,
    type: "number",
    options: { precision: 0 },
  },
  { name: PUF.coverageTerritories, type: "multilineText" },
  ...REGION_CHECKBOX_FIELDS.map((name) => ({
    name,
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
  })),
];

const COPY_OPTIONS_FROM_UM = [
  PUF.platformRole,
  PUF.contactVisibility,
  PUF.dealAccess,
  PUF.documentAccess,
];

const DEFAULT_SINGLE_SELECT = {
  [PUF.platformRole]: [
    "Company Admin",
    "Strategic Lead",
    "Deal Manager",
    "Analyst / Support",
    "Legal / Compliance",
    "External Collaborator",
  ],
  [PUF.contactVisibility]: ["Show Contact", "Hide Contact", "Visible on Match", "Admin Controlled"],
  [PUF.dealAccess]: ["View Only", "Edit", "Full"],
  [PUF.documentAccess]: ["View Only", "Edit", "Full"],
};

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

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

function singleSelectField(name, choiceNames) {
  return {
    name,
    type: "singleSelect",
    options: {
      choices: choiceNames.map((n) => ({ name: String(n) })),
    },
  };
}

function choicesFromUmField(umField) {
  if (!umField?.options?.choices?.length) return null;
  return umField.options.choices.map((c) => c.name).filter(Boolean);
}

const BRAND_BASICS_TABLE_ID = process.env.BRAND_BASICS_TABLE_ID || "tbl1x6S7I7JwTcRdV";

const EXTRA_UM_FIELD_NAMES = [
  "Languages",
  "Brands Supported",
  "Added_By_User",
  "Memberstack ID",
  "responsiveness_response_time_category",
  "responsiveness_response_time_icon",
  "responsiveness_frequency_category",
  "responsiveness_frequency_icon",
  "responsiveness_combined_badge",
];

function fieldSpecFromUm(umField, usersTableId) {
  if (!umField) return null;
  const { name, type, options } = umField;
  if (type === "multipleRecordLinks") {
    const linkedTableId = options?.linkedTableId;
    if (!linkedTableId) return null;
    return {
      name,
      type: "multipleRecordLinks",
      options: { linkedTableId },
    };
  }
  if (type === "multipleSelects" && options?.choices?.length) {
    return {
      name,
      type: "multipleSelects",
      options: {
        choices: options.choices.map((c) => ({
          name: c.name,
          ...(c.color ? { color: c.color } : {}),
        })),
      },
    };
  }
  if (type === "singleSelect" && options?.choices?.length) {
    return singleSelectField(
      name,
      options.choices.map((c) => c.name).filter(Boolean)
    );
  }
  if (type === "singleLineText") {
    return { name, type: "singleLineText" };
  }
  if (type === "email") return { name, type: "email" };
  if (type === "phoneNumber") return { name, type: "phoneNumber" };
  if (type === "number") {
    return { name, type: "number", options: options || { precision: 0 } };
  }
  if (type === "multilineText") return { name, type: "multilineText" };
  if (type === "checkbox") {
    return {
      name,
      type: "checkbox",
      options: options || { icon: "check", color: "greenBright" },
    };
  }
  return null;
}

function buildFieldsToCreate(usersTable, umTable) {
  const existing = new Set((usersTable.fields || []).map((f) => f.name));
  const umByName = new Map((umTable?.fields || []).map((f) => [f.name, f]));
  const toCreate = [];

  for (const spec of STATIC_FIELDS) {
    if (!existing.has(spec.name)) toCreate.push(spec);
  }

  for (const name of COPY_OPTIONS_FROM_UM) {
    if (existing.has(name)) continue;
    const umField = umByName.get(name);
    const choices =
      (umField && umField.type === "singleSelect" && choicesFromUmField(umField)) ||
      DEFAULT_SINGLE_SELECT[name];
    toCreate.push(singleSelectField(name, choices || DEFAULT_SINGLE_SELECT[name]));
  }

  const hasProfile = PROFILE_NAMES.some((n) => existing.has(n));
  if (!hasProfile) {
    toCreate.push({
      name: "Profile",
      type: "multipleAttachments",
      description: "Headshot for Partner Directory / user admin",
    });
  }

  for (const name of EXTRA_UM_FIELD_NAMES) {
    if (existing.has(name)) continue;
    const spec = fieldSpecFromUm(umByName.get(name), usersTable.id);
    if (spec) toCreate.push(spec);
  }

  if (!existing.has("Company")) {
    const umCompany = umByName.get("Company");
    const linkedTableId =
      umCompany?.options?.linkedTableId || PLATFORM_USERS_COMPANY_TABLE_ID;
    toCreate.push({
      name: "Company",
      type: "multipleRecordLinks",
      options: { linkedTableId },
      description: "Same company link as Company Profile (legacy User Management column name)",
    });
  }

  return toCreate;
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) {
    throw new Error(
      `List tables failed ${listRes.status}: ${JSON.stringify(listJson)}. PAT may need schema.bases:write.`
    );
  }

  const tables = listJson.tables || [];
  const usersTable = tables.find((t) => t.id === PLATFORM_USERS_TABLE_ID);
  if (!usersTable) throw new Error(`Users table not found: ${PLATFORM_USERS_TABLE_ID}`);

  const umTable = tables.find((t) => t.id === LEGACY_USER_MANAGEMENT_TABLE_ID);
  const toCreate = buildFieldsToCreate(usersTable, umTable);

  if (!toCreate.length) {
    console.log(`All platform fields already exist on "${usersTable.name}".`);
    return;
  }

  console.log(`Users table: ${usersTable.name} (${usersTable.id})`);
  console.log(dryRun ? "Dry run — would create:" : "Creating:");
  toCreate.forEach((f) => console.log("  -", f.name, `(${f.type})`));

  if (dryRun) return;

  for (const field of toCreate) {
    const body = { name: field.name, type: field.type };
    if (field.options) body.options = field.options;
    if (field.description) body.description = field.description;

    const { res, json } = await metaFetch(baseId, token, `/tables/${usersTable.id}/fields`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`Create "${field.name}" failed ${res.status}: ${JSON.stringify(json)}`);
    }
    console.log(`Created "${field.name}" (${json.id})`);
    await new Promise((r) => setTimeout(r, 220));
  }

  console.log("\nDone. Re-run: node scripts/audit-users-vs-user-management.mjs");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
