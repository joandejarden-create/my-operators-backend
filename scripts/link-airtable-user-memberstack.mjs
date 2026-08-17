#!/usr/bin/env node
/**
 * Link Airtable Users row to Memberstack member id (legacy fields: Unique Webflow ID + Slug).
 *
 * Usage:
 *   node scripts/link-airtable-user-memberstack.mjs --email owner@example.com --memberstack-id mem_XXXXX
 *   node scripts/link-airtable-user-memberstack.mjs --record-id recXXXXXXXX --memberstack-id mem_XXXXX --dry-run
 *
 * Rejects mem_sb_ test ids unless --allow-test-memberstack-id (loud warning).
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  MEMBERSTACK_MEMBER_ID_FIELD_NAMES,
  memberstackIdLabel,
  memberstackSlugLabel,
} from "../lib/pilot-provisioning/pilot-field-registry.js";
import { isTestMemberstackId } from "../lib/pilot-provisioning/pilot-validators.js";

const USERS_TABLE = process.env.AIRTABLE_INTAKE_USERS_TABLE || "tbl6shiyz2wdUqE5F";
const EMAIL_FIELD = process.env.AIRTABLE_INTAKE_USERS_EMAIL_FIELD || "fldBl7IXEscwkMhnZ";
const MS_ID_FIELD = MEMBERSTACK_MEMBER_ID_FIELD_NAMES.primaryFieldId;
const SLUG_FIELD = MEMBERSTACK_MEMBER_ID_FIELD_NAMES.mirrorFieldId;

function escapeFormula(value) {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function parseArgs(argv) {
  const out = {
    email: null,
    recordId: null,
    memberstackId: null,
    dryRun: false,
    allowTestMemberstackId: false,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--email" && argv[i + 1]) out.email = argv[++i];
    else if (a === "--record-id" && argv[i + 1]) out.recordId = argv[++i];
    else if ((a === "--memberstack-id" || a === "--mem") && argv[i + 1]) out.memberstackId = argv[++i];
    else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--allow-test-memberstack-id") out.allowTestMemberstackId = true;
    else if (a === "--help" || a === "-h") out.help = true;
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help || !args.memberstackId || (!args.email && !args.recordId)) {
    console.log(`Usage:
  node scripts/link-airtable-user-memberstack.mjs --email <email> --memberstack-id mem_...
  node scripts/link-airtable-user-memberstack.mjs --record-id rec... --memberstack-id mem_...
  --dry-run                      print only, do not update Airtable
  --allow-test-memberstack-id    allow mem_sb_ (Test Mode) — NOT for production rows`);
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
    console.error("ERROR: Memberstack member id must start with mem_");
    process.exit(1);
  }

  if (isTestMemberstackId(msId) && !args.allowTestMemberstackId) {
    console.error(`
ERROR: Refusing to write Test Mode member id (${msId}) to Airtable Users.
Never paste mem_sb_ into production Airtable Users rows.

For localhost-only sandbox testing, re-run with --allow-test-memberstack-id (not for pilot/production).
`);
    process.exit(1);
  }

  if (isTestMemberstackId(msId) && args.allowTestMemberstackId) {
    console.warn(`
*** WARNING ***
Writing Test Mode mem_sb_ id to Airtable. Do NOT use for production pilot users.
`);
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

  console.log("Memberstack member id:", msId);
  console.log("Will set both:", memberstackIdLabel(), "and", memberstackSlugLabel());

  if (args.dryRun) {
    console.log("Dry run — would PATCH", recordId, patch);
    return;
  }

  const updated = await base(USERS_TABLE).update(recordId, patch, { typecast: true });
  console.log("Updated Users row:", updated.id);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
