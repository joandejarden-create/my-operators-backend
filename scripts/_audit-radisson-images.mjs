import "../load-env.js";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const rows = await base("Brand Setup - Brand Explorer Presentation")
  .select({ filterByFormula: '{Brand Name} = "Radisson by Choice"', maxRecords: 500 })
  .all();

let ok = 0;
let miss = 0;
for (const r of rows) {
  const sk = String(r.get("Slot Key") || "");
  if (!/overview\.scenario|footprint\.openings|materials\.gallery/.test(sk)) continue;
  const img = r.get("Image");
  if (Array.isArray(img) && img[0]?.thumbnails) {
    ok++;
    console.log("OK", sk, String(r.get("Title") || "").slice(0, 35));
  } else {
    miss++;
    console.log("MISS", sk);
  }
}
console.log(`\n${ok} with thumbnails, ${miss} missing`);
