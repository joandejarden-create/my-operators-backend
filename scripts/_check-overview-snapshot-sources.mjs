import "../load-env.js";
import Airtable from "airtable";

const brandName = process.argv[2] || "Comfort Inn & Suites";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const esc = brandName.replace(/"/g, '\\"');

const pf = await base("Brand Setup - Project Fit")
  .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
  .firstPage();
const pp = await base("Brand Setup - Portfolio & Performance")
  .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
  .firstPage();
const basics = await base("Brand Setup - Brand Basics")
  .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
  .firstPage();
const basicsId = basics[0]?.id;
let pres = await base("Brand Setup - Brand Explorer Presentation")
  .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
  .all();
if (!pres.length && basicsId) {
  pres = await base("Brand Setup - Brand Explorer Presentation")
    .select({
      filterByFormula: `FIND("${basicsId}", ARRAYJOIN({Brand})) > 0`,
      maxRecords: 500,
    })
    .all();
}

const pfF = pf[0]?.fields || {};
const sweetKey = Object.keys(pfF).find((k) => k.includes("sweet spot"));
console.log("Brand:", brandName);
console.log("Project Fit Min/Max rooms:", pfF["Min - Room Count"], pfF["Max - Room Count"]);
console.log("Project Fit sweet-spot notes:", sweetKey ? String(pfF[sweetKey]).slice(0, 100) : "(empty)");
console.log(
  "Portfolio min/max property size:",
  pp[0]?.fields?.["Minimum Property Size (Rooms)"],
  pp[0]?.fields?.["Maximum Property Size (Rooms)"]
);
const slot = pres.filter((r) => String(r.fields["Slot Key"] || "") === "overview.typical_use_case");
console.log("Presentation rows (linked):", pres.length);
console.log("Presentation overview.typical_use_case rows:", slot.length);
if (slot[0]) console.log("  body:", String(slot[0].fields.Body || "").slice(0, 100));
const overviewSlots = pres
  .map((r) => String(r.fields["Slot Key"] || ""))
  .filter((k) => k.startsWith("overview."));
console.log("overview.* slot keys present:", [...new Set(overviewSlots)].sort().join(", "));
if (basicsId) {
  const byBrandCol = await base("Brand Setup - Brand Explorer Presentation")
    .select({ filterByFormula: `{Brand} = "${esc}"`, maxRecords: 5 })
    .firstPage()
    .catch(() => []);
  console.log("Presentation by {Brand} name match:", byBrandCol.length);
}
