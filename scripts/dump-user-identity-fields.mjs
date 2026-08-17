import "../load-env.js";
import Airtable from "airtable";

const ids = process.argv.slice(2);
if (!ids.length) ids.push("recgX2piU7DakT2ug", "recxGecN3JR90n7uN");

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

for (const id of ids) {
  const rec = await base("tbl6shiyz2wdUqE5F").find(id);
  console.log("\n===", id, "===");
  const keys = Object.keys(rec.fields || {}).filter((k) =>
    /webflow|slug|email|memberstack|deals|company profile/i.test(k)
  );
  for (const k of keys.sort()) {
    console.log(k + ":", JSON.stringify(rec.fields[k]));
  }
  console.log("flddTfp7oLdcPwBIC:", JSON.stringify(rec.fields.flddTfp7oLdcPwBIC));
}
