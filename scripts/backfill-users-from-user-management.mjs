#!/usr/bin/env node
/**
 * Copy all User Management field values onto mapped Users rows (post-migration).
 * Uses scripts/output/um-to-users-id-map.json and fills any field that exists on Users.
 *
 *   node scripts/backfill-users-from-user-management.mjs --dry-run
 *   node scripts/backfill-users-from-user-management.mjs --apply
 *   node scripts/backfill-users-from-user-management.mjs --apply --overwrite
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import Airtable from "airtable";
import {
  LEGACY_USER_MANAGEMENT_TABLE_ID,
  PLATFORM_USERS_TABLE_ID,
} from "../lib/airtable/platform-users-table.js";
import {
  buildUsersPayloadFromUm,
  normEmailFromUm,
} from "../lib/airtable/copy-um-fields-to-users.js";

const args = process.argv.slice(2);
const dryRun = !args.includes("--apply");
const overwrite = args.includes("--overwrite");

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

async function metaTables() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json.tables || [];
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

function loadIdMap() {
  const path = "scripts/output/um-to-users-id-map.json";
  if (!existsSync(path)) throw new Error(`Missing ${path} — run migrate with --apply first.`);
  return JSON.parse(readFileSync(path, "utf8")).idMap || {};
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("Missing AIRTABLE_API_KEY / AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const idMap = loadIdMap();
  const tables = await metaTables();
  const usersTable = tables.find((t) => t.id === PLATFORM_USERS_TABLE_ID);
  const usersFieldNames = new Set((usersTable?.fields || []).map((f) => f.name));

  const [umRecords, userRecords] = await Promise.all([
    fetchAll(LEGACY_USER_MANAGEMENT_TABLE_ID),
    fetchAll(PLATFORM_USERS_TABLE_ID),
  ]);

  const usersById = new Map(userRecords.map((r) => [r.id, r]));
  const usersByEmail = new Map();
  userRecords.forEach((r) => {
    const em = normEmailFromUm(r.fields);
    if (em) usersByEmail.set(em, r);
  });

  let updated = 0;
  let skipped = 0;
  let missingTarget = 0;

  console.log(overwrite ? "Mode: overwrite UM values onto Users\n" : "Mode: fill empty Users fields only\n");
  if (dryRun) console.log("DRY RUN\n");

  for (const um of umRecords) {
    let userId = idMap[um.id];
    if (!userId || !userId.startsWith("rec")) {
      const em = normEmailFromUm(um.fields);
      userId = em ? usersByEmail.get(em)?.id : null;
    }
    if (!userId) {
      missingTarget++;
      console.warn("No Users row for UM", um.id);
      continue;
    }

    const user = usersById.get(userId);
    const payload = buildUsersPayloadFromUm(um.fields, usersFieldNames, {
      overwrite,
      existingUserFields: user?.fields || {},
    });

    if (Object.keys(payload).length === 0) {
      skipped++;
      continue;
    }

    console.log(
      `${dryRun ? "Would update" : "UPDATE"} ${userId} <- UM ${um.id}:`,
      Object.keys(payload).join(", ")
    );

    if (!dryRun) {
      await base(PLATFORM_USERS_TABLE_ID).update(userId, payload, { typecast: true });
      await new Promise((r) => setTimeout(r, 200));
    }
    updated++;
  }

  console.log("\n=== Summary ===");
  console.log("UM records:", umRecords.length);
  console.log("Updated:", updated);
  console.log("Skipped (nothing to write):", skipped);
  console.log("Missing Users target:", missingTarget);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
