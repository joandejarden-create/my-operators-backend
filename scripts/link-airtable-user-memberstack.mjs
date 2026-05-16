#!/usr/bin/env node
/**
 * Link an Airtable Users row to a Memberstack member id (Unique Webflow ID + slug).
 *
 * Usage:
 *   node scripts/link-airtable-user-memberstack.mjs --email owner@example.com --memberstack-id mem_sb_xxxxx
 *   node scripts/link-airtable-user-memberstack.mjs --record-id recXXXXXXXX --memberstack-id mem_sb_xxxxx
 *
 * Requires AIRTABLE_API_KEY and AIRTABLE_BASE_ID (loads .env / .env.local via load-env).
 */

import "../load-env.js";
import Airtable from "airtable";

const USERS_TABLE = process.env.AIRTABLE_INTAKE_USERS_TABLE || "tbl6shiyz2wdUqE5F";
const EMAIL_FIELD = process.env.AIRTABLE_INTAKE_USERS_EMAIL_FIELD || "fldBl7IXEscwkMhnZ";
const MS_ID_FIELD =
  process.env.AIRTABLE_INTAKE_USERS_UNIQUE_WEBFLOW_ID_FIELD || "flddTfp7oLdcPwBIC";
/** Airtable column is "Slug" (fldEgbHu5MvfyrxgE) — not lowercase "slug". */
const SLUG_FIELD =
  process.env.AIRTABLE_USERS_SLUG_FIELD || "fldEgbHu5MvfyrxgE";

function escapeFormula(value) {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function parseArgs(argv) {
  const out = { email: null, recordId: null, memberstackId: null, dryRun: false };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--email" && argv[i + 1]) out.email = argv[++i];
    else if (a === "--record-id" && argv[i + 1]) out.recordId = argv[++i];
    else if ((a === "--memberstack-id" || a === "--mem") && argv[i + 1]) out.memberstackId = argv[++i];
    else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--help" || a === "-h") out.help = true;
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help || !args.memberstackId || (!args.email && !args.recordId)) {
    console.log(`Usage:
  node scripts/link-airtable-user-memberstack.mjs --email <email> --memberstack-id mem_sb_...
  node scripts/link-airtable-user-memberstack.mjs --record-id rec... --memberstack-id mem_sb_...
  --dry-run   print only, do not update Airtable`);
    process.exit(args.help ? 0 : 1);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const msId = String(args.memberstackId).trim();
  if (!msId.startsWith("mem_")) {
    console.warn("Warning: memberstack id usually starts with mem_sb_ or mem_");
  }

  const base = new Airtable({ apiKey }).base(baseId);
  let recordId = args.recordId;

  if (!recordId && args.email) {
    const lit = escapeFormula(args.email.toLowerCase());
    const formula = `LOWER({${EMAIL_FIELD}}) = '${lit}'`;
    const rows = await base(USERS_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).firstPage();
    if (!rows.length) {
      console.error("No Users row for email:", args.email);
      process.exit(1);
    }
    recordId = rows[0].id;
    console.log("Found Users row:", recordId);
  }

  const patch = {
    [MS_ID_FIELD]: msId,
    [SLUG_FIELD]: msId,
  };

  if (args.dryRun) {
    console.log("Dry run — would PATCH", recordId, patch);
    return;
  }

  const updated = await base(USERS_TABLE).update(recordId, patch, { typecast: true });
  console.log("Updated Users row:", updated.id);
  console.log("  Unique Webflow ID / slug set to:", msId);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
