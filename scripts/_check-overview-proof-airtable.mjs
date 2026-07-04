import "../load-env.js";
import Airtable from "airtable";

const brandName = process.argv[2] || "Comfort Inn & Suites";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const esc = brandName.replace(/"/g, '\\"');
const basics = await base("Brand Setup - Brand Basics")
  .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
  .firstPage();
if (!basics.length) {
  console.error("No basics for", brandName);
  process.exit(1);
}
console.log("Basics ID:", basics[0].id);
const id = basics[0].id;
const linked = await base("Brand Setup - Brand Explorer Presentation")
  .select({
    filterByFormula: `{Brand Name} = "${esc}"`,
    pageSize: 100,
  })
  .all();
console.log("Total presentation rows linked:", linked.length);
const proof = linked
  .filter((r) => String(r.fields["Slot Key"] || "").startsWith("overview.proof"))
  .sort((a, b) => String(a.fields["Slot Key"]).localeCompare(String(b.fields["Slot Key"])));
const overview = linked
  .filter((r) => String(r.fields["Slot Key"] || "").startsWith("overview."))
  .sort((a, b) => String(a.fields["Slot Key"]).localeCompare(String(b.fields["Slot Key"])));
console.log(brandName, "— overview rows:", overview.length, "| proof:", proof.length);
for (const r of overview) {
  const sk = r.fields["Slot Key"];
  if (!sk.startsWith("overview.proof")) continue;
  console.log(
    sk,
    "|",
    (r.fields.Title || "(no title)").slice(0, 55),
    "|",
    (r.fields.Body || "").slice(0, 70)
  );
}
const missing = ["overview.proof.1", "overview.proof.2", "overview.proof.3", "overview.proof.4", "overview.proof.5", "overview.proof.6"].filter(
  (sk) => !proof.some((r) => r.fields["Slot Key"] === sk)
);
if (missing.length) console.log("MISSING:", missing.join(", "));
const allKeys = linked.map((r) => r.fields["Slot Key"]).sort();
console.log("All slot keys:", allKeys.join(", "));
