/**
 * Read-only audit: Project Fit & Deal coverage for all Brand Basics rows.
 */
import "../load-env.js";
import Airtable from "airtable";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PF_TABLE = "Brand Setup - Project Fit";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const basics = await base(BASICS_TABLE)
  .select({
    fields: [
      "Brand Name",
      "Brand Status",
      "Parent Company",
      "Hotel Chain Scale",
      "Brand Model",
      "Brand Architecture",
      "Brand Setup - Project Fit",
      "Brand Development Stage",
      "Branded Residences Status",
    ],
  })
  .all();

const pfRows = await base(PF_TABLE)
  .select({
    fields: [
      "Brand Name",
      "Brand",
      "Acceptable Project Type",
      "Acceptable Agreements Type",
      "Co-Branding Allowed",
      "Mixed-Use Development Allowed",
      "Soft/Collection Brand",
      "Branded Residences Allowed",
    ],
  })
  .all();

const pfByBrandId = new Map();
for (const r of pfRows) {
  for (const id of r.get("Brand") || []) pfByBrandId.set(id, r);
}

const statusCounts = {};
let missingPf = 0;
let incompletePf = 0;

for (const b of basics) {
  const status = String(b.get("Brand Status") || "(empty)").trim();
  statusCounts[status] = (statusCounts[status] || 0) + 1;
  const linked = b.get("Brand Setup - Project Fit");
  const pf = (Array.isArray(linked) && linked[0] && pfByBrandId.get(b.id)) || pfByBrandId.get(b.id);
  if (!pf && !(Array.isArray(linked) && linked.length)) {
    missingPf++;
    continue;
  }
  const row = pf || null;
  if (!row) continue;
  const incomplete =
    !row.get("Acceptable Project Type")?.length ||
    !row.get("Acceptable Agreements Type")?.length ||
    !row.get("Co-Branding Allowed") ||
    !row.get("Mixed-Use Development Allowed") ||
    !row.get("Soft/Collection Brand") ||
    !row.get("Branded Residences Allowed");
  if (incomplete) incompletePf++;
}

console.log(
  JSON.stringify(
    {
      totalBrands: basics.length,
      statusCounts,
      projectFitRows: pfRows.length,
      missingProjectFitLink: missingPf,
      incompleteProjectFitDealFields: incompletePf,
    },
    null,
    2
  )
);
