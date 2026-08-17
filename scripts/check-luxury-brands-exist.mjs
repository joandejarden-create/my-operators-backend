import "../load-env.js";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const names = ["Four Seasons", "Aman", "Design Hotels"];

for (const n of names) {
  const esc = n.replace(/"/g, '\\"');
  const basics = await base("Brand Setup - Brand Basics")
    .select({
      filterByFormula: `{Brand Name} = "${esc}"`,
      maxRecords: 1,
      fields: [
        "Brand Name",
        "Parent Company",
        "Brand Status",
        "Hotel Chain Scale",
        "Branded Residences Status",
        "Branded Residences Review Status",
        "Branded Residences Source URL",
      ],
    })
    .firstPage();
  if (!basics.length) {
    console.log(n + ": NOT FOUND");
    continue;
  }
  const b = basics[0];
  const pf = await base("Brand Setup - Project Fit")
    .select({
      filterByFormula: `{Brand Name} = "${esc}"`,
      maxRecords: 1,
      fields: ["Brand Name", "Branded Residences Allowed", "Soft/Collection Brand"],
    })
    .firstPage();
  console.log(JSON.stringify({ basicsId: b.id, basics: b.fields, projectFit: pf[0]?.fields || null }, null, 2));
}
