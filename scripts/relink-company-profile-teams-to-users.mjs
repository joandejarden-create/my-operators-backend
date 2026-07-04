#!/usr/bin/env node
/**
 * Copy Company Profile "User Management" team links → "Team Members" (Users record ids).
 *   node scripts/relink-company-profile-teams-to-users.mjs --dry-run
 *   node scripts/relink-company-profile-teams-to-users.mjs --apply
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import Airtable from "airtable";
import {
  LEGACY_USER_MANAGEMENT_TABLE_ID,
  PLATFORM_USERS_COMPANY_TABLE_ID,
} from "../lib/airtable/platform-users-table.js";

const TEAM_FIELD = process.env.COMPANY_PROFILE_TEAM_FIELD_NAME || "Team Members";
const UM_TEAM_FIELD = process.env.COMPANY_PROFILE_UM_TEAM_FIELD_NAME || "User Management";

const args = process.argv.slice(2);
const dryRun = !args.includes("--apply");

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

function loadIdMap() {
  const path = "scripts/output/um-to-users-id-map.json";
  if (!existsSync(path)) throw new Error(`Missing ${path}`);
  return JSON.parse(readFileSync(path, "utf8")).idMap || {};
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

function mapLinkIds(umIds, idMap) {
  const out = [];
  const seen = new Set();
  for (const id of umIds || []) {
    const mapped = idMap[id] || id;
    if (!mapped.startsWith("rec") || seen.has(mapped)) continue;
    seen.add(mapped);
    out.push(mapped);
  }
  return out;
}

async function main() {
  const idMap = loadIdMap();
  const companies = await fetchAll(PLATFORM_USERS_COMPANY_TABLE_ID);

  let updated = 0;
  let skipped = 0;

  for (const cp of companies) {
    const umLinks = cp.fields[UM_TEAM_FIELD];
    if (!Array.isArray(umLinks) || umLinks.length === 0) {
      skipped++;
      continue;
    }

    const teamIds = mapLinkIds(umLinks, idMap);
    if (!teamIds.length) {
      console.warn("No mapped Users ids for", cp.fields["Company Name"] || cp.id);
      skipped++;
      continue;
    }

    const existing = Array.isArray(cp.fields[TEAM_FIELD]) ? cp.fields[TEAM_FIELD] : [];
    const merged = [...new Set([...existing, ...teamIds])];

    console.log(
      `${dryRun ? "Would update" : "UPDATE"} ${cp.fields["Company Name"] || cp.id}:`,
      `${umLinks.length} UM → ${merged.length} Users team link(s)`
    );

    if (!dryRun) {
      await base(PLATFORM_USERS_COMPANY_TABLE_ID).update(cp.id, { [TEAM_FIELD]: merged });
      await new Promise((r) => setTimeout(r, 200));
    }
    updated++;
  }

  console.log("\n=== Summary ===");
  console.log("Companies:", companies.length);
  console.log("Updated:", updated);
  console.log("Skipped:", skipped);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
