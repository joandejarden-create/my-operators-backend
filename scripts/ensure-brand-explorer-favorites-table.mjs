/**
 * Create "Brand Explorer Favorites" table (User + Brand links) if missing.
 *
 * Env: AIRTABLE_API_KEY (schema.bases:write), AIRTABLE_BASE_ID
 *
 * Usage: node scripts/ensure-brand-explorer-favorites-table.mjs
 */
import "../load-env.js";

const TABLE_NAME = "Brand Explorer Favorites";
const USERS_TABLE = process.env.USERS_TABLE_ID || "tbl6shiyz2wdUqE5F";
const BRAND_BASICS_TABLE_NAME =
  process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";

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

function findTable(tables, name) {
  return (tables || []).find((t) => t.name === name);
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);

  const tables = listJson.tables || [];
  const usersTable = findTable(tables, "Users") || tables.find((t) => t.id === USERS_TABLE);
  const brandTable = findTable(tables, BRAND_BASICS_TABLE_NAME);

  if (!usersTable) throw new Error(`Users table not found (expected id ${USERS_TABLE} or name Users)`);
  if (!brandTable) throw new Error(`Brand basics table not found: ${BRAND_BASICS_TABLE_NAME}`);

  let favTable = findTable(tables, TABLE_NAME);
  if (favTable) {
    console.log(`Table "${TABLE_NAME}" already exists (${favTable.id}).`);
    console.log(`Set BRAND_EXPLORER_FAVORITES_TABLE_ID=${favTable.id}`);
    return;
  }

  const body = {
    name: TABLE_NAME,
    description: "Per-user saved brands for Brand Explorer combined (syncs across devices).",
    fields: [
      {
        name: "Name",
        type: "singleLineText",
        description: "Optional label; primary field required by Airtable.",
      },
      {
        name: "User_ID",
        type: "multipleRecordLinks",
        options: { linkedTableId: usersTable.id },
      },
      {
        name: "Brand",
        type: "multipleRecordLinks",
        options: { linkedTableId: brandTable.id },
      },
      {
        name: "Favorited Date",
        type: "dateTime",
        options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "utc" },
      },
    ],
  };

  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`Create table failed ${res.status}: ${JSON.stringify(json)}`);

  console.log(`Created table "${TABLE_NAME}" (${json.id}).`);
  console.log(`Add to .env: BRAND_EXPLORER_FAVORITES_TABLE_ID=${json.id}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
