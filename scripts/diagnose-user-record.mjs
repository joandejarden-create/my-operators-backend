/**
 * Diagnose a specific Airtable Users row vs Memberstack + My Deals access.
 * Usage: node scripts/diagnose-user-record.mjs recxGecN3JR90n7uN
 */
import "../load-env.js";
import Airtable from "airtable";
import axios from "axios";
import {
  INTAKE_DEALS_USER_LINK_NAME,
  INTAKE_USERS_UNIQUE_WEBFLOW_ID,
  INTAKE_USERS_EMAIL,
} from "../api/schemas/intake-deal-fields.js";
import { dealRecordAllowedForUser } from "../lib/dealality/deal-record-access.js";
import { readAirtableField, cellToString } from "../lib/airtable-utils.js";
import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";

const USER_REC_ID = process.argv[2] || "recxGecN3JR90n7uN";
const USERS_TABLE = "tbl6shiyz2wdUqE5F";
const DEALS_TABLE = "tblbvSxjiIhXzW6XW";

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
if (!apiKey || !baseId) {
  console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
  process.exit(1);
}

const base = new Airtable({ apiKey }).base(baseId);

let userRec;
try {
  userRec = await base(USERS_TABLE).find(USER_REC_ID);
} catch (e) {
  console.error("Failed to load user:", e.message);
  process.exit(1);
}

const uf = userRec.fields || {};
const email = cellToString(uf.Email) || cellToString(uf[INTAKE_USERS_EMAIL]);
const uniqueWebflowId =
  cellToString(uf["Unique Webflow ID"]) ||
  cellToString(uf.Unique_Webflow_ID) ||
  cellToString(uf[INTAKE_USERS_UNIQUE_WEBFLOW_ID]);
const slug = cellToString(uf.Slug) || cellToString(uf.slug);
const memberstackIdField = cellToString(uf["Memberstack ID"]);
const companyIds = uf["Company Profile"] || [];
const userDeals = uf.Deals || [];
const platformRole = cellToString(uf["Platform Role"]) || cellToString(uf["User Type"]);

console.log("=== Airtable Users row", USER_REC_ID, "===");
console.log("Email:", email || "(empty)");
console.log("Unique Webflow ID:", uniqueWebflowId || "(empty)");
console.log("Slug:", slug || "(empty)");
console.log("Memberstack ID field:", memberstackIdField || "(empty)");
console.log("Platform Role / User Type:", platformRole || "(empty)");
console.log("Company Profile:", companyIds.length ? companyIds.join(", ") : "(none)");
console.log("Deals on user row:", userDeals.length);

if (email) {
  const lit = email.toLowerCase().replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  const dupes = await base(USERS_TABLE)
    .select({ filterByFormula: `LOWER({Email}) = '${lit}'`, maxRecords: 10 })
    .firstPage();
  console.log("\nUsers rows with same email:", dupes.length);
  for (const d of dupes) {
    const f = d.fields || {};
    const ms =
      cellToString(f["Unique Webflow ID"]) || cellToString(f[INTAKE_USERS_UNIQUE_WEBFLOW_ID]);
    console.log(" -", d.id, "| ms:", ms || "(empty)", "| deals:", (f.Deals || []).length);
  }
}

const msKey = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();
let msMember = null;
if (msKey && email) {
  const BASE = (process.env.MEMBERSTACK_BASE_URL || "https://admin.memberstack.com").replace(
    /\/$/,
    ""
  );
  const res = await axios.get(`${BASE}/members/${encodeURIComponent(email)}`, {
    headers: { "X-API-KEY": msKey, "Content-Type": "application/json" },
    validateStatus: () => true,
  });
  if (res.status === 200) msMember = res.data?.data || res.data;
  else {
    console.log("\nMemberstack GET by email status:", res.status);
    if (res.data) console.log(JSON.stringify(res.data).slice(0, 300));
  }
} else if (!msKey) {
  console.log("\n(MEMBERSTACK_SECRET_KEY not set — skipping Memberstack lookup)");
}

if (msMember) {
  console.log("\n=== Memberstack member (by email) ===");
  console.log("Member id:", msMember.id);
  console.log("Email:", msMember.email);
  const cf = msMember.customFields || {};
  const airtableCf =
    cf["air-table-user-id"] || cf["AirTable User ID"] || cf.airtableUserId || "";
  console.log("AirTable User ID in MS:", airtableCf || "(empty)");
  console.log("MS id matches Airtable Unique Webflow ID?", msMember.id === uniqueWebflowId);
  console.log("MS id matches Airtable Slug?", msMember.id === slug);
  console.log("AirTable User ID matches this row?", airtableCf === USER_REC_ID);

  const resolved = await resolveDealalityUser({ memberstackId: msMember.id, email });
  console.log("\n=== resolveDealalityUser (login simulation) ===");
  console.log("found:", resolved.found);
  if (resolved.found) {
    console.log("resolved userRecordId:", resolved.userRecordId);
    console.log("matches target row?", resolved.userRecordId === USER_REC_ID);
    console.log("isOwner:", resolved.isOwner, "| canAccessOwnerWorkspace:", resolved.canAccessOwnerWorkspace);
    console.log("workspaceAccess:", (resolved.workspaceAccess || []).join(", ") || "(none)");
    console.log("companyId:", resolved.companyId || "(none)");
  } else {
    console.log("reason:", resolved.reason);
  }
} else if (uniqueWebflowId) {
  const resolved = await resolveDealalityUser({ memberstackId: uniqueWebflowId, email });
  console.log("\n=== resolveDealalityUser (from Airtable ms id) ===");
  console.log("found:", resolved.found, "userRecordId:", resolved.userRecordId);
  console.log("matches target?", resolved.userRecordId === USER_REC_ID);
}

const dealalityUser = {
  isAdmin: false,
  isOwner: true,
  userRecordId: USER_REC_ID,
  companyId: companyIds[0] || null,
  companyIds,
};

let viaUserLink = 0;
let viaCompany = 0;
let allowed = 0;
let total = 0;
const sampleMissing = [];

await base(DEALS_TABLE)
  .select({ pageSize: 100 })
  .eachPage((records, next) => {
    for (const rec of records) {
      total += 1;
      const dealUserIds =
        readAirtableField(rec.fields, INTAKE_DEALS_USER_LINK_NAME) ||
        rec.fields?.Users ||
        rec.fields?.User_ID ||
        [];
      const dealCompanies = rec.fields?.["Company Profile"] || [];
      if (Array.isArray(dealUserIds) && dealUserIds.includes(USER_REC_ID)) viaUserLink += 1;
      if (companyIds.length && dealCompanies.some((id) => companyIds.includes(id))) viaCompany += 1;
      if (dealRecordAllowedForUser(rec.fields, dealalityUser)) allowed += 1;
      else if (userDeals.includes(rec.id) && sampleMissing.length < 3) {
        sampleMissing.push({
          dealId: rec.id,
          dealUserIds: Array.isArray(dealUserIds) ? dealUserIds : [],
          dealCompanies: dealCompanies.slice(0, 2),
        });
      }
    }
    next();
  });

console.log("\n=== Deal access for", USER_REC_ID, "===");
console.log("Total deals:", total);
console.log("Deals linked on USER row:", userDeals.length);
console.log("Deals with User_ID back-link:", viaUserLink);
console.log("Deals via Company Profile:", viaCompany);
console.log("My Deals would return:", allowed);
if (sampleMissing.length) {
  console.log("Sample linked-on-user but NOT allowed:", JSON.stringify(sampleMissing, null, 2));
}
