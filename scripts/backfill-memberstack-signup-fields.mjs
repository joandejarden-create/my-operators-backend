#!/usr/bin/env node
/**
 * Patch Memberstack custom fields from an existing Airtable Users row (fix pre-mapping signups).
 *
 *   node scripts/backfill-memberstack-signup-fields.mjs --email joandejarden@gmail.com
 *   node scripts/backfill-memberstack-signup-fields.mjs --email x@y.com --dry-run
 */
import "../load-env.js";
import axios from "axios";
import Airtable from "airtable";
import { patchMemberstackAfterAirtable } from "../lib/memberstack/signup-member.js";
import { USERS_SIGNUP } from "../lib/signup-airtable-upsert.js";
import {
  readMemberstackIdsFromUserFields,
  isTestMemberstackId,
  isLiveMemberstackId,
} from "../lib/pilot-provisioning/pilot-validators.js";
import { memberstackIdLabel } from "../lib/pilot-provisioning/pilot-field-registry.js";

const BASE = (process.env.MEMBERSTACK_BASE_URL || "https://admin.memberstack.com").replace(/\/$/, "");

function parseArgs() {
  const emailIdx = process.argv.indexOf("--email");
  if (emailIdx < 0) {
    console.error("Usage: node scripts/backfill-memberstack-signup-fields.mjs --email <email> [--dry-run]");
    process.exit(1);
  }
  return {
    email: process.argv[emailIdx + 1].trim().toLowerCase(),
    dryRun: process.argv.includes("--dry-run"),
    allowTestMemberstackId: process.argv.includes("--allow-test-memberstack-id"),
  };
}

async function main() {
  const { email, dryRun, allowTestMemberstackId } = parseArgs();
  const key = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();
  const apiKey = (process.env.AIRTABLE_API_KEY || "").trim();
  const baseId = (process.env.AIRTABLE_BASE_ID || "").trim();
  if (!key || !apiKey || !baseId) {
    console.error("Need MEMBERSTACK_SECRET_KEY, AIRTABLE_API_KEY, AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const headers = { "X-API-KEY": key, "Content-Type": "application/json" };
  const msRes = await axios.get(`${BASE}/members/${encodeURIComponent(email)}`, {
    headers,
    validateStatus: () => true,
  });
  const member = msRes.data?.data || msRes.data?.member;
  if (msRes.status !== 200 || !member?.id) {
    console.error("Memberstack member not found:", msRes.status, msRes.data);
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const escaped = email.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  const rows = await base(USERS_SIGNUP.table)
    .select({ filterByFormula: `{Email} = '${escaped}'`, maxRecords: 1 })
    .firstPage();
  if (!rows.length) {
    console.error("No Airtable Users row for", email);
    process.exit(1);
  }
  const f = rows[0].fields;
  const msIds = readMemberstackIdsFromUserFields(f);

  if (isTestMemberstackId(member.id) && !allowTestMemberstackId) {
    console.error(`
ERROR: Memberstack member is Test Mode (${member.id}).
For production pilots, use live mem_ member and ensure Airtable ${memberstackIdLabel()} is live mem_.
Re-run with --allow-test-memberstack-id only for localhost sandbox.
`);
    process.exit(1);
  }

  if (!allowTestMemberstackId && msIds.primary && isTestMemberstackId(msIds.primary)) {
    console.error(`
ERROR: Airtable Users row has Test Mode id (${msIds.primary}). Fix Airtable before MS custom-field backfill.
Never paste mem_sb_ into production Airtable Users rows.
`);
    process.exit(1);
  }

  if (!msIds.primary || !isLiveMemberstackId(msIds.primary)) {
    console.warn(
      `WARNING: Airtable missing live ${memberstackIdLabel()} — link with link-airtable-user-memberstack.mjs first.`
    );
  } else if (member.id !== msIds.primary) {
    console.warn(
      `WARNING: Memberstack id (${member.id}) differs from Airtable (${msIds.primary}) — reconcile before backfill.`
    );
  }

  const custom = member.customFields || member.custom_fields || {};
  const staleDemo =
    custom["unique-webflow-id"] &&
    isTestMemberstackId(String(custom["unique-webflow-id"])) &&
    isLiveMemberstackId(member.id);
  if (staleDemo) {
    console.warn(
      "WARNING: Memberstack custom field unique-webflow-id still has mem_sb_ while member is live — backfill will overwrite."
    );
  }

  const body = {
    email,
    firstName: f[USERS_SIGNUP.firstName] || f["First Name"] || "",
    lastName: f[USERS_SIGNUP.lastName] || f["Last Name"] || "",
    companyName: f["Company Name"] || "",
    phone: f["Phone Number"] || "",
    companyType: f["User Type"] || "",
    reasonToJoin: f["Reason to Join Platform"] || "",
    howDidYouHear: f["How Did You Hear About Us"] || "",
  };
  const companyProfileId = Array.isArray(f["Company Profile"])
    ? f["Company Profile"][0]
    : typeof f["Company Profile"] === "string"
      ? f["Company Profile"]
      : "";

  console.log("Member:", member.id, member.email);
  console.log("Airtable:", rows[0].id);
  console.log("Body:", body);
  if (dryRun) {
    console.log("(dry-run — no patch)");
    return;
  }

  const result = await patchMemberstackAfterAirtable(member.id, {
    airtableRecordId: rows[0].id,
    body,
    companyProfileId,
  });
  console.log("Patch result:", result);
  if (!result.ok) {
    process.exit(1);
  }
  console.log("Done. Re-check Memberstack dashboard columns.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
