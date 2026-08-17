/**
 * Read-only audit: Project Fit & Deal fields for Active brands.
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
    filterByFormula: '{Brand Status}="Active"',
    fields: [
      "Brand Name",
      "Parent Company",
      "Hotel Chain Scale",
      "Brand Model",
      "Brand Architecture",
      "Hotel Service Model",
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
const pfByName = new Map();
for (const r of pfRows) {
  pfByName.set(String(r.get("Brand Name") || "").trim(), r);
  for (const id of r.get("Brand") || []) pfByBrandId.set(id, r);
}

console.log(JSON.stringify({ activeBrandCount: basics.length, projectFitRowCount: pfRows.length }, null, 2));

for (const b of basics) {
  const name = b.get("Brand Name");
  const pf = pfByBrandId.get(b.id) || pfByName.get(name);
  console.log(
    JSON.stringify({
      id: b.id,
      name,
      parentCompany: b.get("Parent Company"),
      chainScale: b.get("Hotel Chain Scale"),
      brandModel: b.get("Brand Model"),
      architecture: b.get("Brand Architecture"),
      serviceModel: b.get("Hotel Service Model"),
      brandDevelopmentStage: b.get("Brand Development Stage") || null,
      brandedResidencesStatus: b.get("Branded Residences Status") || null,
      projectFitId: pf?.id || null,
      acceptableProjectTypes: pf?.get("Acceptable Project Type") || null,
      acceptableAgreementTypes: pf?.get("Acceptable Agreements Type") || null,
      coBrandingAllowed: pf?.get("Co-Branding Allowed") || null,
      mixedUseAllowed: pf?.get("Mixed-Use Development Allowed") || null,
      softCollectionBrand: pf?.get("Soft/Collection Brand") || null,
      brandedResidencesAllowed: pf?.get("Branded Residences Allowed") || null,
    })
  );
}
