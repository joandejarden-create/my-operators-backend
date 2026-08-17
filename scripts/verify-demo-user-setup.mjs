import "../load-env.js";
import Airtable from "airtable";

const DEMO_EMAIL = "dealalitydemo@dealality.com";
const USERS_TABLE = "tbl6shiyz2wdUqE5F";
const EMAIL_FIELD = "fldBl7IXEscwkMhnZ";
const MS_FIELD = "flddTfp7oLdcPwBIC";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const lit = DEMO_EMAIL.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
const rows = await base(USERS_TABLE)
  .select({ filterByFormula: `LOWER({${EMAIL_FIELD}}) = '${lit}'`, maxRecords: 1 })
  .firstPage();

console.log("=== Airtable Users (dealalitydemo@dealality.com) ===");
if (!rows.length) {
  console.log("MISSING: no Users row with this email");
} else {
  const f = rows[0].fields || {};
  console.log("recordId:", rows[0].id);
  console.log("name:", [f["First Name"], f["Last Name"]].filter(Boolean).join(" "));
  console.log("role:", f["Platform Role"] || f["User Type"] || f.Role);
  console.log(
    "memberstackId:",
    f["Unique Webflow ID"] || f["Unique_Webflow_ID"] || f[MS_FIELD] || f.Slug || f.slug || "(empty)"
  );
  console.log("companyProfile:", Array.isArray(f["Company Profile"]) ? f["Company Profile"].length : 0);
}

console.log("\n=== Railway env hints (not secrets) ===");
console.log("MEMBERSTACK_APP_ID set:", !!process.env.MEMBERSTACK_APP_ID);
console.log("MEMBERSTACK_SECRET_KEY set:", !!process.env.MEMBERSTACK_SECRET_KEY);
console.log("AIRTABLE_BASE_ID prefix:", (process.env.AIRTABLE_BASE_ID || "").slice(0, 8));
