#!/usr/bin/env node
/**
 * Merge legacy User Management rows into the Users table (Option A consolidation).
 *
 *   node scripts/migrate-user-management-to-users.mjs --dry-run
 *   node scripts/migrate-user-management-to-users.mjs --apply
 *
 * Before --apply:
 *   1. Add fields on Users (see docs/users-table-consolidation.md)
 *   2. node scripts/audit-users-vs-user-management.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import { mkdirSync, writeFileSync } from "fs";
import { dirname } from "path";
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
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

async function usersFieldNames() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  const t = (json.tables || []).find((x) => x.id === PLATFORM_USERS_TABLE_ID);
  return new Set((t?.fields || []).map((f) => f.name));
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

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("Missing AIRTABLE_API_KEY / AIRTABLE_BASE_ID");
    process.exit(1);
  }

  console.log(dryRun ? "DRY RUN (pass --apply to write)\n" : "APPLYING migration\n");

  const fieldNames = await usersFieldNames();

  const [umRecords, userRecords] = await Promise.all([
    fetchAll(LEGACY_USER_MANAGEMENT_TABLE_ID),
    fetchAll(PLATFORM_USERS_TABLE_ID),
  ]);

  const usersByEmail = new Map();
  userRecords.forEach((r) => {
    const em = normEmailFromUm(r.fields);
    if (em) usersByEmail.set(em, r);
  });

  const idMap = {};
  let created = 0;
  let updated = 0;
  let skipped = 0;

  for (const um of umRecords) {
    const email = normEmailFromUm(um.fields);
    const fields = buildUsersPayloadFromUm(um.fields || {}, fieldNames, {
      existingUserFields: {},
    });
    if (!email && Object.keys(fields).length === 0) {
      skipped++;
      continue;
    }

    const existing = email ? usersByEmail.get(email) : null;
    if (existing) {
      idMap[um.id] = existing.id;
      const patch = {};
      for (const [k, v] of Object.entries(fields)) {
        const cur = existing.fields[k];
        if (cur === undefined || cur === null || cur === "" || (Array.isArray(cur) && cur.length === 0)) {
          patch[k] = v;
        }
      }
      if (Object.keys(patch).length === 0) {
        skipped++;
        continue;
      }
      console.log(`UPDATE ${existing.id} <- UM ${um.id} (${email}) fields:`, Object.keys(patch).join(", "));
      if (!dryRun) {
        await base(PLATFORM_USERS_TABLE_ID).update(existing.id, patch, { typecast: true });
      }
      updated++;
    } else {
      console.log(`CREATE from UM ${um.id} (${email || "no email"})`);
      if (!dryRun) {
        const createdRec = await base(PLATFORM_USERS_TABLE_ID).create(fields, { typecast: true });
        idMap[um.id] = createdRec.id;
        if (email) usersByEmail.set(email, createdRec);
      } else {
        idMap[um.id] = `(new-for-${um.id})`;
      }
      created++;
    }
  }

  const mapPath = "scripts/output/um-to-users-id-map.json";
  if (!dryRun) {
    mkdirSync(dirname(mapPath), { recursive: true });
    writeFileSync(mapPath, JSON.stringify({ generatedAt: new Date().toISOString(), idMap }, null, 2));
    console.log("\nWrote", mapPath);
  }

  console.log("\n=== Summary ===");
  console.log("UM records:", umRecords.length);
  console.log("Would create:", created);
  console.log("Would update:", updated);
  console.log("Skipped:", skipped);
  if (dryRun) console.log("\nRe-run with --apply after adding missing Users fields in Airtable.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
