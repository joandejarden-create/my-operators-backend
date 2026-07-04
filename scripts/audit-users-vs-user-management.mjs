#!/usr/bin/env node
/**
 * Compare legacy User Management vs Users table: schema fields, row counts, email overlap.
 *
 *   node scripts/audit-users-vs-user-management.mjs
 */
import "../load-env.js";
import {
  LEGACY_USER_MANAGEMENT_TABLE_ID,
  PLATFORM_USERS_TABLE_ID,
  PUF,
  REGION_CHECKBOX_FIELDS,
} from "../lib/airtable/platform-users-table.js";

const PROFILE_ANY_OF = ["Profile", "Profile Picture", "Headshot", "Photo", "Avatar"];

const REQUIRED_SCALAR_FIELDS = [
  PUF.companyTitle,
  PUF.phoneNumber,
  PUF.companyEmail,
  PUF.platformRole,
  PUF.contactVisibility,
  PUF.dealAccess,
  PUF.documentAccess,
  PUF.country,
  PUF.closedDeals,
  PUF.uniqueBrandsDeals,
  PUF.submittedBids,
  PUF.coverageTerritories,
  ...REGION_CHECKBOX_FIELDS,
];

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;

function enc(s) {
  return encodeURIComponent(s);
}

async function metaFetch(path) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/${path}`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`${path} ${res.status}: ${JSON.stringify(json)}`);
  }
  return json;
}

async function listAllRecords(tableId) {
  const records = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${enc(tableId)}?${qs}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${tableId}: ${res.status} ${JSON.stringify(json)}`);
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return records;
}

function normEmail(fields) {
  const e =
    fields?.Email ||
    fields?.["Company Email"] ||
    fields?.email ||
    "";
  return String(Array.isArray(e) ? e[0] : e)
    .trim()
    .toLowerCase();
}

async function main() {
  if (!apiKey || !baseId) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env.local");
    process.exit(1);
  }

  const tables = await metaFetch("tables");
  const byId = new Map((tables.tables || []).map((t) => [t.id, t]));

  const usersMeta = byId.get(PLATFORM_USERS_TABLE_ID);
  const umMeta = byId.get(LEGACY_USER_MANAGEMENT_TABLE_ID);

  console.log("\n=== Tables ===");
  console.log("Users:", usersMeta?.name || PLATFORM_USERS_TABLE_ID, PLATFORM_USERS_TABLE_ID);
  console.log("Legacy User Management:", umMeta?.name || "?", LEGACY_USER_MANAGEMENT_TABLE_ID);

  if (usersMeta) {
    const names = new Set((usersMeta.fields || []).map((f) => f.name));
    const missing = REQUIRED_SCALAR_FIELDS.filter((n) => !names.has(n));
    const hasProfile = PROFILE_ANY_OF.some((n) => names.has(n));
    console.log("\n=== Fields to add on Users (before migration) ===");
    if (missing.length) missing.forEach((n) => console.log("  -", n));
    if (!hasProfile) console.log("  -", "Profile (or Profile Picture / Headshot)");
    if (!missing.length && hasProfile) {
      console.log("  (all recommended fields present)");
    }
  } else {
    console.warn("\nCould not load Users table metadata (check table id / PAT schema scope).");
  }

  const [usersRecs, umRecs] = await Promise.all([
    listAllRecords(PLATFORM_USERS_TABLE_ID),
    listAllRecords(LEGACY_USER_MANAGEMENT_TABLE_ID).catch((e) => {
      console.warn("Legacy User Management list failed:", e.message);
      return [];
    }),
  ]);

  const usersByEmail = new Map();
  usersRecs.forEach((r) => {
    const em = normEmail(r.fields);
    if (em) usersByEmail.set(em, r.id);
  });

  let umOnly = 0;
  let overlap = 0;
  umRecs.forEach((r) => {
    const em = normEmail(r.fields);
    if (!em) return;
    if (usersByEmail.has(em)) overlap++;
    else umOnly++;
  });

  console.log("\n=== Row counts ===");
  console.log("Users rows:", usersRecs.length);
  console.log("User Management rows:", umRecs.length);
  console.log("UM emails also on Users:", overlap);
  console.log("UM emails NOT on Users (need migration create):", umOnly);

  console.log("\nNext: node scripts/migrate-user-management-to-users.mjs --dry-run\n");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
