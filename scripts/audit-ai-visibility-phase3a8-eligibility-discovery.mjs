#!/usr/bin/env node
/**
 * Phase 3A.8 — read-only discovery of Brand Basics fields for eligibility hardening.
 * No writes. No provider calls.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "..",
  "data",
  "ai-visibility",
  "phase3a8-eligibility-data-discovery.json"
);

const COHORT = [
  { brandId: "recEJCTDj1zrsjPM6", brandName: "Autograph Collection" },
  { brandId: "recCvV0PuZOi8c3hC", brandName: "Tribute Portfolio" },
  { brandId: "rec02zPClpWUTCyXM", brandName: "Design Hotels" },
  { brandId: "recIPuBC50fv13zRR", brandName: "Westin" },
  { brandId: "rec9aZp7GHtzUEg0c", brandName: "AC Hotels by Marriott" },
  { brandId: "receQkxgjlezsc1xg", brandName: "Curio Collection by Hilton" },
  { brandId: "reccXxMHEh7NNRhIE", brandName: "Tapestry Collection by Hilton" },
  { brandId: "recsggfbKlJbjeRP9", brandName: "Canopy by Hilton" },
  { brandId: "recqiHq3GHKMj8Meo", brandName: "Tempo by Hilton" },
  { brandId: "reclkgOzvAcBheUSo", brandName: "Ascend Hotel Collection" },
  { brandId: "recRyvM8OmLlDj9G7", brandName: "Radisson Individuals by Choice" },
  { brandId: "recWPEvxBQxVVzSq3", brandName: "Radisson Blu by Choice" },
  { brandId: "recegXrqaPiSLGCIe", brandName: "Hotel Indigo" },
  { brandId: "recCKuXCmGvxHPfb3", brandName: "Kimpton Hotels" },
  { brandId: "recrWCD1LMqu864oU", brandName: "MGallery Collection" },
];

const FIELDS = [
  "Brand Name",
  "Parent Company",
  "Brand Status",
  "Hotel Chain Scale",
  "Brand Model",
  "Region Offered",
  "Brand Development Stage",
  "Branded Residences Status",
  "Branded Residences Notes",
  "Branded Residences Source URL",
  "Branded Residences Review Status",
  "Brand Setup - Brand Footprint",
  "Source Region",
];

function cell(v) {
  if (v == null) return null;
  if (Array.isArray(v)) {
    if (!v.length) return null;
    if (typeof v[0] === "object" && v[0]?.id) return v.map((x) => x.id || x);
    return v.map((x) => (typeof x === "object" && x?.name != null ? x.name : String(x)));
  }
  if (typeof v === "object" && v.name != null) return String(v.name).trim();
  const s = String(v).trim();
  return s || null;
}

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
const base = new Airtable({ apiKey }).base(baseId);
const table = process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";

const formula = `OR(${COHORT.map((b) => `RECORD_ID()='${b.brandId}'`).join(",")})`;
const recs = await base(table)
  .select({ filterByFormula: formula, fields: FIELDS })
  .all();
const byId = new Map(recs.map((r) => [r.id, r]));

const metaRes = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
  headers: { Authorization: `Bearer ${apiKey}` },
});
const meta = await metaRes.json();
const basicsTable = (meta.tables || []).find((t) => t.name === table);
const fieldCatalog = (basicsTable?.fields || []).map((f) => ({
  name: f.name,
  type: f.type,
  choices: f.options?.choices?.map((c) => c.name) || undefined,
}));

const discovery = {
  Region_Offered: fieldCatalog.find((f) => f.name === "Region Offered"),
  Branded_Residences_Status: fieldCatalog.find((f) => f.name === "Branded Residences Status"),
  Brand_Development_Stage: fieldCatalog.find((f) => f.name === "Brand Development Stage"),
  has_New_Build_field: fieldCatalog.some((f) => /new.?build/i.test(f.name)),
  has_Mixed_Use_field: fieldCatalog.some((f) => /mixed.?use/i.test(f.name)),
  has_Development_Countries: fieldCatalog.some((f) => /development.?countr/i.test(f.name)),
  has_Supports_New_Build: fieldCatalog.some((f) => /supports.?new.?build/i.test(f.name)),
};

const rows = COHORT.map((b) => {
  const r = byId.get(b.brandId);
  const f = r?.fields || {};
  return {
    brandId: b.brandId,
    brandName: cell(f["Brand Name"]) || b.brandName,
    brandModel: cell(f["Brand Model"]),
    chainScale: cell(f["Hotel Chain Scale"]),
    regionOffered: cell(f["Region Offered"]),
    brandDevelopmentStage: cell(f["Brand Development Stage"]),
    brandedResidencesStatus: cell(f["Branded Residences Status"]),
    brandedResidencesNotes: cell(f["Branded Residences Notes"]),
    brandedResidencesSourceUrl: cell(f["Branded Residences Source URL"]),
    brandedResidencesReviewStatus: cell(f["Branded Residences Review Status"]),
    footprintLinkIds: cell(f["Brand Setup - Brand Footprint"]),
    sourceRegion: cell(f["Source Region"]),
    found: Boolean(r),
  };
});

// Footprint table probe (if exists)
const footprintTable = (meta.tables || []).find((t) =>
  /Brand Footprint/i.test(t.name)
);
let footprintSample = null;
if (footprintTable) {
  const fpFields = footprintTable.fields.map((f) => f.name);
  footprintSample = {
    table: footprintTable.name,
    fields: fpFields,
    interesting: fpFields.filter((n) =>
      /region|countr|market|hotel|presence|pipeline|develop/i.test(n)
    ),
  };
}

const report = {
  generatedAt: new Date().toISOString(),
  AIRTABLE_WRITES: 0,
  LIVE_PROVIDER_CALLS: 0,
  discovery,
  fieldCatalogInteresting: fieldCatalog.filter((f) =>
    /resid|mixed|new.?build|develop|region|countr|geograph|flexib|conver|foot|market|pipeline|prototype|growth/i.test(
      f.name
    )
  ),
  footprintSample,
  rows,
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2), "utf8");
console.log(JSON.stringify(report, null, 2));
