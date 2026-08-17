import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";

const token = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
const base = new Airtable({ apiKey: token }).base(baseId);

const basicsRows = await base("Brand Setup - Brand Basics")
  .select({
    filterByFormula: "OR({Brand Status}='Active', {Brand Status}='Live')",
    fields: [
      "Brand Name",
      "Parent Company",
      "Hotel Chain Scale",
      "Brand Model",
      "Hotel Service Model",
      "Brand Architecture",
      "Brand Status",
    ],
    sort: [{ field: "Brand Name", direction: "asc" }],
  })
  .all();

const stdRows = await base("Brand Setup - Brand Standards")
  .select({
    fields: [
      "Brand Name",
      "Brand",
      "F&B Outlets Required",
      "Meeting Space Required",
      "Typical Number of F&B Outlets",
      "Typical Number of Meeting Rooms",
    ],
  })
  .all();

const stdByBrandId = new Map();
const stdByName = new Map();
for (const r of stdRows) {
  stdByName.set(String(r.get("Brand Name") || "").trim(), r);
  for (const id of r.get("Brand") || []) stdByBrandId.set(id, r);
}

const metaRes = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
  headers: { Authorization: `Bearer ${token}` },
});
const meta = await metaRes.json();
const stdTable = (meta.tables || []).find((t) => t.name === "Brand Setup - Brand Standards");
const fbField = (stdTable?.fields || []).find((f) => f.name === "F&B Outlets Required");
const meetField = (stdTable?.fields || []).find((f) => f.name === "Meeting Space Required");

const brands = basicsRows.map((b) => {
  const std = stdByBrandId.get(b.id) || stdByName.get(b.get("Brand Name"));
  return {
    basicsId: b.id,
    name: b.get("Brand Name"),
    parent: b.get("Parent Company"),
    chainScale: b.get("Hotel Chain Scale"),
    brandModel: b.get("Brand Model"),
    serviceModel: b.get("Hotel Service Model"),
    architecture: b.get("Brand Architecture"),
    standardsId: std?.id || null,
    fbRequired: std?.get("F&B Outlets Required") || null,
    meetingRequired: std?.get("Meeting Space Required") || null,
    fbCount: std?.get("Typical Number of F&B Outlets") ?? null,
    meetingCount: std?.get("Typical Number of Meeting Rooms") ?? null,
  };
});

const payload = {
  generatedAt: new Date().toISOString(),
  activeBrandCount: brands.length,
  standardsRowCount: stdRows.length,
  selectOptions: {
    fbOutletsRequired: fbField?.options?.choices?.map((c) => c.name) || [],
    meetingSpaceRequired: meetField?.options?.choices?.map((c) => c.name) || [],
  },
  missingStandardsRow: brands.filter((b) => !b.standardsId).map((b) => b.name),
  brands,
};

fs.mkdirSync("data", { recursive: true });
fs.writeFileSync("data/active-brand-standards-fb-meeting-audit.json", JSON.stringify(payload, null, 2) + "\n", "utf8");
console.log(JSON.stringify({
  activeBrandCount: payload.activeBrandCount,
  missingStandardsRow: payload.missingStandardsRow.length,
  selectOptions: payload.selectOptions,
  written: "data/active-brand-standards-fb-meeting-audit.json",
}, null, 2));
