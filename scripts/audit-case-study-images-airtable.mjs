import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

const rows = await base(TABLE)
  .select({
    filterByFormula: `{Slot Key} = "materials.caseStudy"`,
    maxRecords: 1000,
  })
  .all();

let withImage = 0;
let without = 0;
const samples = [];

for (const r of rows) {
  const imgs = r.get("Image");
  const has = Array.isArray(imgs) && imgs.length > 0;
  if (has) withImage++;
  else without++;
  if (samples.length < 8 && String(r.get("Brand Name") || "").includes("Comfort")) {
    samples.push({
      id: r.id,
      title: r.get("Title"),
      brand: r.get("Brand Name"),
      image: imgs,
    });
  }
}

console.log(`materials.caseStudy: ${rows.length} total, ${withImage} with Image, ${without} without`);
console.log("Comfort sample:", JSON.stringify(samples, null, 2));

// Check one we claimed to update
const comfort = rows.find(
  (r) =>
    String(r.get("Title") || "").includes("Scarborough") &&
    String(r.get("Slot Key") || "") === "materials.caseStudy"
);
if (comfort) {
  console.log("\nScarborough row:", comfort.id);
  console.log("Image field:", JSON.stringify(comfort.get("Image"), null, 2));
  console.log("All field keys:", Object.keys(comfort.fields || {}));
}

const withImg = rows.find((r) => {
  const imgs = r.get("Image");
  return Array.isArray(imgs) && imgs.length > 0;
});
if (withImg) {
  console.log("\nRow WITH image:", withImg.id, withImg.get("Title"));
  console.log("Image:", JSON.stringify(withImg.get("Image"), null, 2));
  console.log("Keys:", Object.keys(withImg.fields || {}));
}
