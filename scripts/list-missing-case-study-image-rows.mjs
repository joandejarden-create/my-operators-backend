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

const out = [];
for (const r of rows) {
  const imgs = r.get("Image");
  if (Array.isArray(imgs) && imgs.length) continue;
  const body = String(r.get("Body") || "");
  const m = body.match(/https?:\/\/[^\s)]+/i);
  if (!m) continue;
  const url = m[0];
  if (!/choicehotels\.com/i.test(url)) continue;
  out.push({
    id: r.id,
    brand: String(r.get("Brand Name") || ""),
    title: String(r.get("Title") || ""),
    url,
  });
}
console.log(JSON.stringify(out, null, 2));
