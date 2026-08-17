/**
 * Fix duplicate Memberstack id on Users rows (orphan signup row vs real user row).
 * Usage: node scripts/fix-duplicate-memberstack-user.mjs --keep recxGecN3JR90n7uN --clear recgX2piU7DakT2ug
 */
import "../load-env.js";
import Airtable from "airtable";
import axios from "axios";
import { INTAKE_USERS_UNIQUE_WEBFLOW_ID, INTAKE_USERS_EMAIL } from "../api/schemas/intake-deal-fields.js";
import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";
import { dealRecordAllowedForUser } from "../lib/dealality/deal-record-access.js";
import { patchMemberstackAfterAirtable, ensureMemberstackPendingPlan } from "../lib/memberstack/signup-member.js";

const SLUG_FIELD = process.env.AIRTABLE_USERS_SLUG_FIELD || "fldEgbHu5MvfyrxgE";
const USERS_TABLE = "tbl6shiyz2wdUqE5F";

function parseArgs(argv) {
  const out = { keep: null, clear: null, dryRun: false };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--keep" && argv[i + 1]) out.keep = argv[++i];
    else if (a === "--clear" && argv[i + 1]) out.clear = argv[++i];
    else if (a === "--dry-run") out.dryRun = true;
  }
  return out;
}

const args = parseArgs(process.argv);
if (!args.keep || !args.clear) {
  console.error("Usage: node scripts/fix-duplicate-memberstack-user.mjs --keep rec... --clear rec... [--dry-run]");
  process.exit(1);
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const keepRec = await base(USERS_TABLE).find(args.keep);
const clearRec = await base(USERS_TABLE).find(args.clear);
const keepFields = keepRec.fields || {};
const memberstackId =
  keepFields.Unique_Webflow_ID ||
  keepFields["Unique Webflow ID"] ||
  keepFields[INTAKE_USERS_UNIQUE_WEBFLOW_ID] ||
  keepFields.Slug;
const email = String(keepFields.Email || keepFields[INTAKE_USERS_EMAIL] || "")
  .trim()
  .toLowerCase();

if (!memberstackId) {
  console.error("Keep row has no Memberstack id — aborting");
  process.exit(1);
}

console.log("Keep row:", args.keep, "| email:", email, "| mem:", memberstackId);
console.log("Clear row:", args.clear, "| deals:", (clearRec.fields?.Deals || []).length);

const keepPatch = {
  [INTAKE_USERS_EMAIL]: email,
  [INTAKE_USERS_UNIQUE_WEBFLOW_ID]: memberstackId,
  [SLUG_FIELD]: memberstackId,
  Unique_Webflow_ID: memberstackId,
  Slug: memberstackId,
};

const clearPatch = {
  [INTAKE_USERS_UNIQUE_WEBFLOW_ID]: "",
  [SLUG_FIELD]: "",
  Unique_Webflow_ID: "",
  Slug: "",
};

if (args.dryRun) {
  console.log("\nDry run — would PATCH keep:", keepPatch);
  console.log("Dry run — would PATCH clear:", clearPatch);
  process.exit(0);
}

await base(USERS_TABLE).update(args.clear, clearPatch, { typecast: true });
console.log("Cleared Memberstack id from orphan row:", args.clear);

await base(USERS_TABLE).update(args.keep, keepPatch, { typecast: true });
console.log("Confirmed Memberstack id on keep row:", args.keep);

await patchMemberstackAfterAirtable(memberstackId, {
  airtableRecordId: args.keep,
  body: {
    firstName: keepFields["First Name"] || "",
    lastName: keepFields["Last Name"] || "",
    companyName: keepFields["Company Name"] || "",
  },
  companyProfileId: (keepFields["Company Profile"] || [])[0] || null,
});
console.log("Patched Memberstack custom fields");

const plan = await ensureMemberstackPendingPlan(memberstackId);
console.log("Pending plan ensure:", plan.ok ? "ok" : "skipped/failed");

const resolved = await resolveDealalityUser({ memberstackId, email });
console.log("\nPost-fix resolve:", resolved.found, resolved.userRecordId);
console.log("Matches keep row?", resolved.userRecordId === args.keep);

let allowed = 0;
await base("tblbvSxjiIhXzW6XW")
  .select({ pageSize: 100 })
  .eachPage((records, next) => {
    for (const rec of records) {
      if (
        dealRecordAllowedForUser(rec.fields, {
          isAdmin: false,
          isOwner: true,
          userRecordId: args.keep,
          companyId: (keepFields["Company Profile"] || [])[0] || null,
          companyIds: keepFields["Company Profile"] || [],
        })
      ) {
        allowed += 1;
      }
    }
    next();
  });
console.log("My Deals would return:", allowed, "deals");
