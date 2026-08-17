#!/usr/bin/env node
/**
 * Backfill Deals → Company Profile from a pilot user's linked Company Profile.
 * Only updates deals already linked to the user via User_ID.
 *
 *   node scripts/backfill-pilot-deal-company-profile.mjs --email pilot@example.com --dry-run
 *   node scripts/backfill-pilot-deal-company-profile.mjs --email pilot@example.com --execute
 */
import "../load-env.js";
import Airtable from "airtable";
import { INTAKE_DEALS_USER_LINK_NAME } from "../api/schemas/intake-deal-fields.js";
import { extractLinkedRecordIds, readAirtableField, cellToString } from "../lib/airtable-utils.js";
import { DEALS_COMPANY_LINK_FIELD } from "../lib/pilot-provisioning/pilot-field-registry.js";
import { detectDealsCompanyProfileField } from "../lib/pilot-provisioning/pilot-validators.js";

const USERS_TABLE = process.env.AIRTABLE_INTAKE_USERS_TABLE || "tbl6shiyz2wdUqE5F";
const DEALS_TABLE = process.env.AIRTABLE_INTAKE_DEALS_TABLE || "tblbvSxjiIhXzW6XW";

function parseArgs(argv) {
  const out = { email: null, execute: false, dryRun: true };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--email" && argv[i + 1]) out.email = String(argv[++i]).trim().toLowerCase();
    else if (argv[i] === "--execute") {
      out.execute = true;
      out.dryRun = false;
    } else if (argv[i] === "--dry-run") out.dryRun = true;
  }
  return out;
}

function escapeFormula(value) {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.email) {
    console.error("Usage: node scripts/backfill-pilot-deal-company-profile.mjs --email <email> [--dry-run|--execute]");
    process.exit(1);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const probe = await detectDealsCompanyProfileField(base, DEALS_TABLE, DEALS_COMPANY_LINK_FIELD, {
    apiKey,
    baseId,
  });
  if (probe.present !== true) {
    console.error(`
Deals table does not expose "${probe.fieldName}" (probe: ${probe.reason}).

Manual Airtable step required:
  1. Open Deals table (${DEALS_TABLE})
  2. Add linked-record field: Company Profile → Company Profile table
  3. Re-run this script
`);
    process.exit(1);
  }

  const lit = escapeFormula(args.email);
  const users = await base(USERS_TABLE)
    .select({ filterByFormula: `LOWER({Email}) = '${lit}'`, maxRecords: 1 })
    .firstPage();
  if (!users.length) {
    console.error("No Users row for", args.email);
    process.exit(1);
  }

  const userRec = users[0];
  const cpIds = extractLinkedRecordIds(userRec.fields?.["Company Profile"]);
  if (!cpIds.length) {
    console.error("User has no Company Profile link — link Users → Company Profile first.");
    process.exit(1);
  }
  const companyId = cpIds[0];

  const patches = [];
  await base(DEALS_TABLE)
    .select({ pageSize: 100 })
    .eachPage((records, next) => {
      for (const rec of records) {
        const dealUserIds =
          extractLinkedRecordIds(readAirtableField(rec.fields, INTAKE_DEALS_USER_LINK_NAME)) ||
          extractLinkedRecordIds(rec.fields?.Users);
        if (!dealUserIds.includes(userRec.id)) continue;

        const existingCp = extractLinkedRecordIds(rec.fields?.[DEALS_COMPANY_LINK_FIELD]);
        if (existingCp.includes(companyId)) continue;

        patches.push({
          id: rec.id,
          name: cellToString(rec.fields?.Name) || rec.id,
          fields: { [DEALS_COMPANY_LINK_FIELD]: [companyId] },
          previousCp: existingCp,
        });
      }
      next();
    });

  console.log(args.execute ? "=== EXECUTE ===" : "=== DRY RUN ===");
  console.log("User:", userRec.id, args.email);
  console.log("Company Profile:", companyId);
  console.log("Deals to backfill:", patches.length);
  for (const p of patches) {
    console.log(" -", p.id, p.name, "prev CP:", p.previousCp.length ? p.previousCp : "(none)");
  }

  if (!patches.length) {
    console.log("Nothing to update.");
    return;
  }

  if (args.dryRun) {
    console.log("\nRe-run with --execute to apply.");
    return;
  }

  for (let i = 0; i < patches.length; i += 10) {
    const batch = patches.slice(i, i + 10).map((p) => ({ id: p.id, fields: p.fields }));
    await base(DEALS_TABLE).update(batch, { typecast: true });
  }
  console.log("Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
