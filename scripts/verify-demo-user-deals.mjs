/**
 * Why My Deals shows 0 rows for dealalitydemo@dealality.com
 */
import "../load-env.js";
import Airtable from "airtable";
import { INTAKE_DEALS_USER_LINK_NAME } from "../api/schemas/intake-deal-fields.js";
import { dealRecordAllowedForUser } from "../lib/dealality/deal-record-access.js";
import { readAirtableField } from "../lib/airtable-utils.js";

const DEMO_EMAIL = "dealalitydemo@dealality.com";
const USERS_TABLE = "tbl6shiyz2wdUqE5F";
const DEALS_TABLE = "tblbvSxjiIhXzW6XW";
const EMAIL_FIELD = "fldBl7IXEscwkMhnZ";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const lit = DEMO_EMAIL.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
const users = await base(USERS_TABLE)
  .select({ filterByFormula: `LOWER({${EMAIL_FIELD}}) = '${lit}'`, maxRecords: 1 })
  .firstPage();

if (!users.length) {
  console.log("No user for", DEMO_EMAIL);
  process.exit(1);
}

const userRec = users[0];
const uf = userRec.fields || {};
const userDealsOnUserRow = uf.Deals || [];
const companyIds = uf["Company Profile"] || [];

console.log("=== User row ===");
console.log("recordId:", userRec.id);
console.log("Deals linked ON user row (count):", userDealsOnUserRow.length);
console.log("Company Profile linked (count):", companyIds.length, companyIds[0] || "(none)");

const dealalityUser = {
  isAdmin: false,
  isOwner: true,
  userRecordId: userRec.id,
  companyId: companyIds[0] || null,
  companyIds: companyIds,
};

let viaUserLink = 0;
let viaCompany = 0;
let allowed = 0;
let total = 0;

await base(DEALS_TABLE)
  .select({ pageSize: 100 })
  .eachPage((records, next) => {
    for (const rec of records) {
      total += 1;
      const dealUserIds =
        readAirtableField(rec.fields, INTAKE_DEALS_USER_LINK_NAME) || rec.fields?.Users || [];
      const dealCompanies = rec.fields?.["Company Profile"] || [];
      if (Array.isArray(dealUserIds) && dealUserIds.includes(userRec.id)) viaUserLink += 1;
      if (companyIds.length && dealCompanies.some((id) => companyIds.includes(id))) viaCompany += 1;
      if (dealRecordAllowedForUser(rec.fields, dealalityUser)) allowed += 1;
    }
    next();
  });

console.log("\n=== Deals table (same base as Railway AIRTABLE_BASE_ID) ===");
console.log("Total deals in table:", total);
console.log("Deals with USER link back to this user:", viaUserLink);
console.log("Deals with same Company Profile as user:", viaCompany);
console.log("Deals My Deals API would return (filter):", allowed);

if (userDealsOnUserRow.length && !viaUserLink) {
  console.log(
    "\n⚠️  You linked Deals on the USER row, but those deals do NOT have the user link on the DEAL row.",
  );
  console.log("   Fix: open each deal in Deals table and set the User/Owner link field, OR use Company Profile on both.");
}

if (!companyIds.length && !viaUserLink) {
  console.log("\n⚠️  User has no Company Profile and no deals link back from Deals table.");
  console.log("   Fix A: Users row → Company Profile → pick company; Deals rows → same Company Profile.");
  console.log("   Fix B: On each Deal row, link the intake User field to", userRec.id);
}
