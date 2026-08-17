#!/usr/bin/env node
/**
 * Provision joan@aohospitalityadvisors.com — AO Hospitality Advisors admin/owner pilot.
 * Usage:
 *   node scripts/provision-joan-aoh-pilot.mjs           # dry-run
 *   node scripts/provision-joan-aoh-pilot.mjs --execute
 */
import "../load-env.js";
import Airtable from "airtable";
import axios from "axios";
import { INTAKE_DEALS_USER_LINK_NAME } from "../api/schemas/intake-deal-fields.js";
import { readAirtableField } from "../lib/airtable-utils.js";
import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";
import { isTestMemberstackId } from "../lib/pilot-provisioning/pilot-validators.js";

const EMAIL = "joan@aohospitalityadvisors.com";
const USER_ID = "recNemUemQ98o6NSA";
const AO_COMPANY_ID = "recfkFTlz8UeSbQrD";
const DEMO_COMPANY_ID = "recr0XXnseXNlIlxk";
const USERS_TABLE = "tbl6shiyz2wdUqE5F";
const CP_TABLE = "tblItyfH6MlOnMKZ9";
const DEALS_TABLE = "tblbvSxjiIhXzW6XW";
const MS_ID = "mem_cmqdv53pi00bf0suj25u42l46";
const MS_ID_FIELD = "flddTfp7oLdcPwBIC";
const SLUG_FIELD = "fldEgbHu5MvfyrxgE";

const execute = process.argv.includes("--execute");
const allowTestMemberstackId = process.argv.includes("--allow-test-memberstack-id");

async function findMemberstackMember(apiKey, email) {
  const BASE = (process.env.MEMBERSTACK_BASE_URL || "https://admin.memberstack.com").replace(/\/$/, "");
  let after;
  for (let page = 0; page < 100; page++) {
    const params = { limit: 100, order: "ASC" };
    if (after != null) params.after = after;
    const res = await axios.get(`${BASE}/members`, {
      headers: { "X-API-KEY": apiKey },
      params,
      validateStatus: () => true,
    });
    if (res.status !== 200) throw new Error(`Memberstack HTTP ${res.status}`);
    const batch = res.data?.data || [];
    const found = batch.find(
      (m) =>
        (m.auth?.email || m.email || "").trim().toLowerCase() === email.toLowerCase()
    );
    if (found) return found;
    if (!res.data?.hasNextPage) break;
    after = res.data?.endCursor;
  }
  return null;
}

async function main() {
  if (isTestMemberstackId(MS_ID) && !allowTestMemberstackId) {
    console.error(`
ERROR: Refusing to provision with Test Mode member id (${MS_ID}).
Use live mem_ id for production pilot rows. Pass --allow-test-memberstack-id only for sandbox.
`);
    process.exit(1);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const msKey = process.env.MEMBERSTACK_LIVE_SECRET_KEY || process.env.MEMBERSTACK_SECRET_KEY;
  if (!apiKey || !baseId) throw new Error("Missing Airtable env");

  const base = new Airtable({ apiKey }).base(baseId);
  const user = await base(USERS_TABLE).find(USER_ID);
  const uf = user.fields || {};

  const dealsCompanyField =
    process.env.AIRTABLE_DEALS_COMPANY_LINK_FIELD || "Company Profile";

  const dealPatches = [];
  await base(DEALS_TABLE)
    .select({ pageSize: 100 })
    .eachPage((records, next) => {
      for (const rec of records) {
        const dealUserIds =
          readAirtableField(rec.fields, INTAKE_DEALS_USER_LINK_NAME) || rec.fields?.Users || [];
        if (Array.isArray(dealUserIds) && dealUserIds.includes(USER_ID)) {
          dealPatches.push({
            id: rec.id,
            fields: { [dealsCompanyField]: [AO_COMPANY_ID] },
          });
        }
      }
      next();
    });

  const dealIds = dealPatches.map((p) => p.id);

  const userPatch = {
    "Company Profile": [AO_COMPANY_ID],
    [MS_ID_FIELD]: MS_ID,
    [SLUG_FIELD]: MS_ID,
  };

  const statusField = (process.env.SIGNUP_AIRTABLE_STATUS_FIELD || "").trim();
  if (statusField) {
    userPatch[statusField] = "Active";
  }

  const cpPatch = {
    "Workspace Access": ["Admin", "Owner"],
    "Company Type": "Hotel Owner",
  };

  console.log(execute ? "=== EXECUTE ===" : "=== DRY RUN ===");
  console.log("User:", USER_ID, EMAIL);
  console.log("Current Company Profile:", uf["Company Profile"]);
  console.log("Current MS id:", uf["Unique Webflow ID"] || uf.Slug);
  console.log("User patch:", userPatch);
  console.log("AO Company Profile patch:", AO_COMPANY_ID, cpPatch);
  console.log(
    "Deals with User_ID back-link:",
    dealIds.length,
    dealIds,
    dealsCompanyField !== "Company Profile"
      ? `(optional CP field: ${dealsCompanyField})`
      : "(Deals table has no Company Profile column in this base — skipped)"
  );

  if (msKey) {
    const ms = await findMemberstackMember(msKey, EMAIL);
    console.log("Memberstack member:", ms ? { id: ms.id, verified: ms.verified } : "NOT FOUND");
    if (ms && ms.id !== MS_ID) {
      console.warn("WARNING: live Memberstack id differs from expected", ms.id);
      userPatch[MS_ID_FIELD] = ms.id;
      userPatch[SLUG_FIELD] = ms.id;
    }
  }

  if (!execute) {
    console.log("\nRe-run with --execute to apply.");
    return;
  }

  await base(USERS_TABLE).update(USER_ID, userPatch, { typecast: true });
  await base(CP_TABLE).update(AO_COMPANY_ID, cpPatch, { typecast: true });
  if (dealPatches.length && dealsCompanyField) {
    try {
      for (let i = 0; i < dealPatches.length; i += 10) {
        await base(DEALS_TABLE).update(dealPatches.slice(i, i + 10), { typecast: true });
      }
    } catch (dealErr) {
      if (dealErr?.error === "UNKNOWN_FIELD_NAME") {
        console.warn("Skipped deal Company Profile backfill:", dealErr.message);
      } else {
        throw dealErr;
      }
    }
  }

  const resolved = await resolveDealalityUser({ email: EMAIL, memberstackId: userPatch[MS_ID_FIELD] });
  console.log("\n=== After provision ===");
  console.log(
    JSON.stringify(
      {
        found: resolved.found,
        isAdmin: resolved.isAdmin,
        isOwner: resolved.isOwner,
        workspaceAccess: resolved.workspaceAccess,
        companyName: resolved.companyName,
        companyIds: resolved.companyIds,
        accessWarnings: resolved.accessWarnings,
      },
      null,
      2
    )
  );

  if (Array.isArray(uf["Company Profile"]) && uf["Company Profile"].includes(DEMO_COMPANY_ID)) {
    console.log("\nNote: user was moved off Dealality Owner Demo CP", DEMO_COMPANY_ID);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
