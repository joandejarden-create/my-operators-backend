#!/usr/bin/env node
/**
 * Ensure My Operator Deals Phase 2 Airtable schema (non-destructive).
 *
 *   node scripts/ensure-operator-deal-requests-phase-2-schema.mjs --dry-run
 *   node scripts/ensure-operator-deal-requests-phase-2-schema.mjs --apply
 *
 * Requires AIRTABLE_API_KEY with schema.bases:read + schema.bases:write
 */
import "../load-env.js";
import { OAS_DATA_CONFIDENCE_OPTIONS } from "../lib/operator-alignment-field-options.js";

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;

const USERS_TABLE_ID = process.env.AIRTABLE_ME_USERS_TABLE || "tbl6shiyz2wdUqE5F";
const USERS_OPERATOR_LINK =
  process.env.AIRTABLE_ME_USERS_OPERATOR_SETUP_LINK || "Operator Setup - Master";
const ODR_TABLE_NAME = process.env.AIRTABLE_TABLE_OPERATOR_DEAL_REQUESTS || "Operator Deal Requests";
const ACTIVITY_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";
const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";
const MASTER_TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

const ODR_STATUS_OPTIONS = [
  "New",
  "Viewed",
  "Brand Viewed",
  "Operator Viewed",
  "Sent / Awaiting Response",
  "Accepted",
  "Declined",
  "Archived",
  "Responded - Accepted",
  "Responded - Declined",
  "More Info Requested",
  "Revisit Later",
  "Pre-LOI",
  "Pre-LOI / Term Comparison",
  "Finalist",
  "Deal Room Active",
  "Feasibility",
  "Feasibility In Progress",
  "LOI Signed",
  "LOI Signed / Platform Exit",
];

const ALIGNMENT_BAND_OPTIONS = ["Strong", "Moderate", "Conditional", "Limited", "Insufficient"];
const NDA_STATUS_OPTIONS = ["Not Required", "Not Sent", "Sent", "Signed - Owner Confirmed", "Declined", "Expired"];
const DEAL_ROOM_ACCESS_OPTIONS = ["Blocked", "Granted", "Revoked"];
const STAKEHOLDER_OPTIONS = ["Owner", "Brand", "Operator"];

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

function findTable(tables, nameOrId) {
  return (tables || []).find((t) => t.name === nameOrId || t.id === nameOrId) || null;
}

function hasField(table, name) {
  return (table?.fields || []).some((f) => f.name === name);
}

function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}

function singleSelect(name, optionNames) {
  return { name, type: "singleSelect", options: { choices: choices(optionNames) } };
}

function dateTimeField(name) {
  return {
    name,
    type: "dateTime",
    options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "utc" },
  };
}

function dateField(name) {
  return { name, type: "date", options: { dateFormat: { name: "iso" } } };
}

function linkField(name, linkedTableId) {
  return {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
}

async function createField(baseId, token, tableId, spec) {
  if (DRY) {
    console.log(`[dry-run] would create field ${spec.name} on ${tableId}`);
    return { ok: true, dry: true };
  }
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify({
      name: spec.name,
      type: spec.type,
      ...(spec.options ? { options: spec.options } : {}),
      ...(spec.description ? { description: spec.description } : {}),
    }),
  });
  if (!res.ok) return { ok: false, status: res.status, json };
  return { ok: true, json };
}

async function createTable(baseId, token, body) {
  if (DRY) {
    console.log(`[dry-run] would create table ${body.name}`);
    return { ok: true, dry: true, id: "dry_run" };
  }
  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) return { ok: false, status: res.status, json };
  return { ok: true, json };
}

function odrFieldSpecs(dealsId, masterId) {
  return [
    { name: "Operating Company Name", type: "singleLineText" },
    linkField("Deal", dealsId),
    linkField("Operator Setup", masterId),
    singleSelect("Status", ODR_STATUS_OPTIONS),
    { name: "Alignment Score", type: "number", options: { precision: 1 } },
    singleSelect("Alignment Band", ALIGNMENT_BAND_OPTIONS),
    singleSelect("Data Confidence", OAS_DATA_CONFIDENCE_OPTIONS),
    dateTimeField("Request Sent At"),
    dateTimeField("Response Date"),
    { name: "Response Notes", type: "multilineText" },
    dateTimeField("Created At"),
    dateTimeField("Last Updated"),
    { name: "Owner Notes", type: "multilineText" },
    { name: "Next Follow-up Notes (Internal)", type: "multilineText" },
    { name: "Next Follow-up Notes (External)", type: "multilineText" },
    dateField("Next Follow-up Date"),
    { name: "Next Follow-up Header", type: "singleLineText" },
    { name: "NDA Required?", type: "checkbox", options: { icon: "check", color: "greenBright" } },
    singleSelect("NDA Status", NDA_STATUS_OPTIONS),
    singleSelect("Deal Room Access", DEAL_ROOM_ACCESS_OPTIONS),
  ];
}

async function ensureFieldOnTable(baseId, token, table, spec, logLabel) {
  if (hasField(table, spec.name)) {
    console.log(`  skip (exists): ${spec.name}`);
    return { skipped: true };
  }
  const r = await createField(baseId, token, table.id, spec);
  if (!r.ok) {
    console.error(`  FAIL ${spec.name}: ${r.status} ${JSON.stringify(r.json)}`);
    return { failed: true, spec: spec.name };
  }
  console.log(`  created: ${spec.name}${logLabel ? ` (${logLabel})` : ""}`);
  return { created: true };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  console.log(DRY ? "=== DRY RUN ===" : "=== APPLY ===");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);

  let tables = listJson.tables || [];
  const usersTable = findTable(tables, USERS_TABLE_ID) || findTable(tables, "Users");
  const masterTable = findTable(tables, MASTER_TABLE);
  const dealsTable = findTable(tables, DEALS_TABLE);
  const activityTable = findTable(tables, ACTIVITY_TABLE);

  if (!usersTable) throw new Error("Users table not found");
  if (!masterTable) throw new Error(`Master table not found: ${MASTER_TABLE}`);
  if (!dealsTable) throw new Error(`Deals table not found: ${DEALS_TABLE}`);
  if (!activityTable) throw new Error(`Activity table not found: ${ACTIVITY_TABLE}`);

  const applied = [];

  console.log("\nUsers —", USERS_OPERATOR_LINK);
  const userLinkSpec = linkField(USERS_OPERATOR_LINK, masterTable.id);
  const u = await ensureFieldOnTable(baseId, token, usersTable, userLinkSpec, "Users scope link");
  if (u.created) applied.push(`Users.${USERS_OPERATOR_LINK}`);

  let odrTable = findTable(tables, ODR_TABLE_NAME);
  if (!odrTable) {
    console.log(`\nCreate table: ${ODR_TABLE_NAME}`);
    const initialFields = odrFieldSpecs(dealsTable.id, masterTable.id);
    const createBody = {
      name: ODR_TABLE_NAME,
      description: "My Operator Deals — scoped operating opportunities (Phase 2).",
      fields: initialFields,
    };
    const cr = await createTable(baseId, token, createBody);
    if (!cr.ok) throw new Error(`Create ODR table failed ${cr.status}: ${JSON.stringify(cr.json)}`);
    applied.push(`table:${ODR_TABLE_NAME}`);
    console.log(`  created table ${ODR_TABLE_NAME} (${cr.json?.id || "dry_run"})`);
    if (!DRY) {
      const refresh = await metaFetch(baseId, token, "/tables");
      tables = refresh.json.tables || [];
      odrTable = findTable(tables, ODR_TABLE_NAME);
    }
  } else {
    console.log(`\nOperator Deal Requests — add missing fields on ${odrTable.name}`);
    for (const spec of odrFieldSpecs(dealsTable.id, masterTable.id)) {
      const r = await ensureFieldOnTable(baseId, token, odrTable, spec);
      if (r.created) applied.push(`ODR.${spec.name}`);
    }
  }

  console.log("\nDeal Activity Log — Phase 2 fields");
  for (const spec of [
    { name: "Operating Company Name", type: "singleLineText" },
    singleSelect("Stakeholder", STAKEHOLDER_OPTIONS),
  ]) {
    const r = await ensureFieldOnTable(baseId, token, activityTable, spec);
    if (r.created) applied.push(`Activity.${spec.name}`);
  }

  console.log("\nDone.", applied.length ? `Applied: ${applied.join(", ")}` : "No changes needed.");
  if (DRY) console.log("Re-run with --apply to create missing schema.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
