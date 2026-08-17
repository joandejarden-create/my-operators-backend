import "../load-env.js";
import Airtable from "airtable";
import {
  isParentCompanyHomepage,
  normalizeBrandWebsiteUrl,
} from "../lib/brand-explorer/active-brand-website-corrections.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const rows = await base("Brand Setup - Brand Basics")
  .select({
    filterByFormula: "OR({Brand Status}='Active', {Brand Status}='Live')",
    fields: ["Brand Name", "Parent Company", "Brand Website"],
  })
  .all();

const issues = [];
for (const r of rows) {
  const name = r.get("Brand Name");
  const url = r.get("Brand Website") || "";
  const norm = normalizeBrandWebsiteUrl(url);
  if (!url) issues.push({ name, issue: "blank" });
  else if (/^https:\/\/www\./i.test(url)) issues.push({ name, issue: "www", url });
  else if (!/^https:\/\//i.test(url)) issues.push({ name, issue: "not-https", url });
  else if (isParentCompanyHomepage(norm, r.get("Parent Company"))) {
    issues.push({ name, issue: "parent-homepage", url: norm });
  }
}

console.log(JSON.stringify({ total: rows.length, issueCount: issues.length, issues }, null, 2));
process.exit(issues.length ? 1 : 0);
