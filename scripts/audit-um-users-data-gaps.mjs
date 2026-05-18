#!/usr/bin/env node
/**
 * Per-field gap report: User Management vs mapped Users rows.
 *   node scripts/audit-um-users-data-gaps.mjs
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import Airtable from "airtable";
import { LEGACY_USER_MANAGEMENT_TABLE_ID, PLATFORM_USERS_TABLE_ID } from "../lib/airtable/platform-users-table.js";
import { UM_FIELD_RENAMES, UM_SKIP_FIELDS } from "../lib/airtable/copy-um-fields-to-users.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

function hasValue(v) {
  if (v === undefined || v === null || v === "") return false;
  if (Array.isArray(v) && v.length === 0) return false;
  return true;
}

function valuesEqual(a, b) {
  if (!hasValue(a) && !hasValue(b)) return true;
  if (Array.isArray(a) && Array.isArray(b)) {
    const sa = [...a].map(String).sort().join(",");
    const sb = [...b].map(String).sort().join(",");
    return sa === sb;
  }
  return String(a) === String(b);
}

async function fetchAll(tableId) {
  const records = [];
  await new Promise((resolve, reject) => {
    base(tableId)
      .select({ pageSize: 100 })
      .eachPage(
        (page, next) => {
          records.push(...page);
          next();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });
  return records;
}

async function metaTables() {
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${process.env.AIRTABLE_BASE_ID}/tables`,
    { headers: { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` } }
  );
  return (await res.json()).tables || [];
}

function loadIdMap() {
  const path = "scripts/output/um-to-users-id-map.json";
  if (!existsSync(path)) return {};
  return JSON.parse(readFileSync(path, "utf8")).idMap || {};
}

function destFieldName(umFieldName) {
  if (UM_SKIP_FIELDS.has(umFieldName)) return null;
  if (umFieldName.startsWith("Brand Name")) return null;
  return UM_FIELD_RENAMES[umFieldName] || umFieldName;
}

async function main() {
  const idMap = loadIdMap();
  const tables = await metaTables();
  const usersFieldNames = new Set(
    (tables.find((t) => t.id === PLATFORM_USERS_TABLE_ID)?.fields || []).map((f) => f.name)
  );
  usersFieldNames.add("Unique_Webflow_ID");
  usersFieldNames.add("Slug");

  const [umRecords, userRecords] = await Promise.all([
    fetchAll(LEGACY_USER_MANAGEMENT_TABLE_ID),
    fetchAll(PLATFORM_USERS_TABLE_ID),
  ]);
  const usersById = new Map(userRecords.map((r) => [r.id, r]));

  const gapCounts = {};
  const missingOnUsersTable = new Set();
  let noUserTarget = 0;

  for (const um of umRecords) {
    const userId = idMap[um.id];
    const user = userId ? usersById.get(userId) : null;
    if (!user) {
      noUserTarget++;
      continue;
    }

    for (const [umKey, umVal] of Object.entries(um.fields || {})) {
      if (!hasValue(umVal)) continue;
      const dest = destFieldName(umKey);
      if (!dest) continue;

      const destKeys =
        umKey === "Memberstack ID"
          ? ["Unique_Webflow_ID", "Slug", "Memberstack ID"].filter((k) => usersFieldNames.has(k))
          : [dest];

      for (const dk of destKeys) {
        if (!usersFieldNames.has(dk)) {
          missingOnUsersTable.add(`${umKey} -> ${dk}`);
          continue;
        }
        const userVal = user.fields[dk];
        if (!valuesEqual(umVal, userVal) && !hasValue(userVal)) {
          const gapKey = `${umKey} -> ${dk}`;
          gapCounts[gapKey] = (gapCounts[gapKey] || 0) + 1;
        }
      }
    }
  }

  console.log("=== Fields missing on Users table (schema) ===");
  if (missingOnUsersTable.size) {
    [...missingOnUsersTable].sort().forEach((x) => console.log(" ", x));
  } else {
    console.log("  (none — all UM data fields have a Users column)");
  }

  console.log("\n=== UM has value but mapped Users row is empty ===");
  const sorted = Object.entries(gapCounts).sort((a, b) => b[1] - a[1]);
  if (!sorted.length) console.log("  (none)");
  else sorted.forEach(([k, n]) => console.log(`  ${n}x  ${k}`));

  console.log("\n=== Records ===");
  console.log("UM:", umRecords.length, "| No Users mapping:", noUserTarget);

  // Company Profile team links still pointing at UM ids
  const umIds = new Set(umRecords.map((r) => r.id));
  const cpTable = tables.find((t) => t.id === "tblItyfH6MlOnMKZ9");
  const cpUmField = (cpTable?.fields || []).find((f) => {
    const n = f.name.toLowerCase();
    return n.includes("user management") && f.type === "multipleRecordLinks";
  });
  if (cpUmField) {
    const cpRecords = await fetchAll("tblItyfH6MlOnMKZ9");
    let cpLinksToUm = 0;
    for (const cp of cpRecords) {
      const links = cp.fields[cpUmField.name];
      if (!Array.isArray(links)) continue;
      if (links.some((id) => umIds.has(id))) cpLinksToUm++;
    }
    console.log("\n=== Company Profile links still using UM record ids ===");
    console.log(`  Field: ${cpUmField.name} | Companies with stale UM links: ${cpLinksToUm}`);
  }

  const usersUmLink = (tables.find((t) => t.id === PLATFORM_USERS_TABLE_ID)?.fields || []).find(
    (f) => f.name === "User Management" && f.type === "multipleRecordLinks"
  );
  if (usersUmLink) {
    let usersWithUmLink = 0;
    for (const u of userRecords) {
      const links = u.fields["User Management"];
      if (Array.isArray(links) && links.some((id) => umIds.has(id))) usersWithUmLink++;
    }
    console.log("\n=== Users.User Management link still points at UM table rows ===");
    console.log("  Users rows with UM rec ids in 'User Management':", usersWithUmLink);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
