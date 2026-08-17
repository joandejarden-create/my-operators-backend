#!/usr/bin/env node
/**
 * Create/find Live Memberstack member from Airtable Users row and relink mem id.
 * Use when going Live without a Test API key (Airtable is source for identity fields).
 *
 *   node scripts/provision-live-member-from-airtable.mjs --email you@example.com
 *   node scripts/provision-live-member-from-airtable.mjs --email you@example.com --execute
 *
 * Does NOT assign Memberstack plans — assign Hotel Owner / Brand / Operator manually in dashboard.
 */
import "../load-env.js";
import Airtable from "airtable";
import { memberstackSecretEnvironment } from "../lib/memberstack/environment.js";
import {
  provisionMemberstackForSignup,
  patchMemberstackAfterAirtable,
} from "../lib/memberstack/signup-member.js";
import { buildMemberstackCustomFields } from "../lib/memberstack/memberstack-custom-fields.js";
import { USERS_SIGNUP } from "../lib/signup-airtable-upsert.js";

const MS_ID_FIELD =
  process.env.AIRTABLE_INTAKE_USERS_UNIQUE_WEBFLOW_ID_FIELD || USERS_SIGNUP.uniqueWebflowId;
const SLUG_FIELD = process.env.AIRTABLE_USERS_SLUG_FIELD || "fldEgbHu5MvfyrxgE";

function parseArgs(argv) {
  const out = { email: null, execute: false, help: false };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--email" && argv[i + 1]) out.email = String(argv[++i]).trim().toLowerCase();
    else if (a === "--execute") out.execute = true;
    else if (a === "--help" || a === "-h") out.help = true;
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help || !args.email) {
    console.log(`Usage:
  node scripts/provision-live-member-from-airtable.mjs --email <email> [--execute]

Requires Live MEMBERSTACK_SECRET_KEY (sk_…), AIRTABLE_API_KEY, AIRTABLE_BASE_ID.
Does not change Workspace Access or Company Profile on Airtable.
Does NOT assign Memberstack plans — assign the correct plan manually per user role.`);
    process.exit(args.help ? 0 : 1);
  }

  const liveKey = (
    process.env.MEMBERSTACK_LIVE_SECRET_KEY ||
    process.env.MEMBERSTACK_SECRET_KEY ||
    ""
  ).trim();
  if (!liveKey) {
    console.error("Set MEMBERSTACK_SECRET_KEY or MEMBERSTACK_LIVE_SECRET_KEY (sk_…)");
    process.exit(1);
  }
  if (memberstackSecretEnvironment(liveKey) !== "live") {
    console.error("This script requires a Live key (sk_…, not sk_sb_).");
    process.exit(1);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const email = args.email;
  console.log("Mode:", args.execute ? "EXECUTE" : "DRY-RUN");
  console.log("Email:", email);
  console.log("Memberstack env:", memberstackSecretEnvironment(liveKey));

  const esc = email.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  const tableId = process.env.AIRTABLE_INTAKE_USERS_TABLE || USERS_SIGNUP.table;
  const base = new Airtable({ apiKey }).base(baseId);
  const rows = await base(tableId)
    .select({ filterByFormula: `{Email} = '${esc}'`, maxRecords: 1 })
    .firstPage();
  if (!rows.length) {
    console.error("No Airtable Users row for:", email);
    process.exit(1);
  }

  const row = rows[0];
  const f = row.fields;
  const body = {
    email,
    firstName: f[USERS_SIGNUP.firstName] || f["First Name"] || "",
    lastName: f[USERS_SIGNUP.lastName] || f["Last Name"] || "",
    companyName: f["Company Name"] || "",
    phone: f["Phone Number"] || "",
    companyType: f["Auth Role Hint"] || f["User Type"] || "",
    reasonToJoin: f["Reason to Join Platform"] || "",
    howDidYouHear: f["How Did You Hear About Us"] || "",
  };
  const companyProfileId = Array.isArray(f["Company Profile"]) ? f["Company Profile"][0] : "";
  const oldMs = f[MS_ID_FIELD] || f["Unique Webflow ID"] || "";

  console.log("Airtable record:", row.id);
  console.log("Current mem id:", oldMs || "(empty)");
  console.log("Name:", body.firstName, body.lastName);
  console.log("Company Profile:", companyProfileId || "(none)");

  const customFields = buildMemberstackCustomFields(body, {
    airtableRecordId: row.id,
    companyProfileId,
  });

  if (!args.execute) {
    console.log("\nDry-run — would call provisionMemberstackForSignup (create or link existing Live member).");
    console.log("Plans: none (manual assignment required in Memberstack dashboard).");
    console.log("Custom field keys:", Object.keys(customFields).join(", ") || "(none)");
    console.log("Re-run with --execute to apply.");
    return;
  }

  const ms = await provisionMemberstackForSignup({
    email,
    firstName: body.firstName,
    lastName: body.lastName,
    customFields,
    mode: "create",
    assignPlansOnCreate: false,
  });

  if (!ms.memberstackId) {
    console.error("Memberstack provision failed:", ms.memberstackNote);
    process.exit(1);
  }

  console.log("Memberstack:", ms.memberstackId, ms.memberstackNote);

  await base(tableId).update(
    row.id,
    { [MS_ID_FIELD]: ms.memberstackId, [SLUG_FIELD]: ms.memberstackId },
    { typecast: true }
  );
  console.log("Airtable Unique Webflow ID updated");

  const patch = await patchMemberstackAfterAirtable(ms.memberstackId, {
    airtableRecordId: row.id,
    body,
    companyProfileId,
  });
  console.log("Memberstack custom fields patch:", patch.ok ? "ok" : "failed");

  if (ms.memberstackNote.includes("password_reset") || ms.memberstackNote.includes("created_member")) {
    console.log("\nIf member was newly created, send password reset from Memberstack dashboard or verification email.");
  }
  console.log("\nNext: assign the correct Memberstack plan for this user in the dashboard, then verify login on dealality.com.");
}

main().catch((err) => {
  console.error(err.response?.data || err.message || err);
  process.exit(1);
});
