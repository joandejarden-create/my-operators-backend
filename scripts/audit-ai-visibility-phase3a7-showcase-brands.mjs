#!/usr/bin/env node
/**
 * Phase 3A.7 — read-only Brand Basics audit for showcase portfolios / peer v2.
 * No writes. No provider calls.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { isBrandStatusActive } from "../lib/brand-status-active.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "..",
  "data",
  "ai-visibility",
  "phase3a7-showcase-brand-basics-audit.json"
);

const EXPECTED = [
  { label: "Autograph Collection", brandId: "recEJCTDj1zrsjPM6", canonicalParent: "Marriott International" },
  { label: "Tribute Portfolio", brandId: "recCvV0PuZOi8c3hC", canonicalParent: "Marriott International" },
  { label: "Design Hotels", brandId: "rec02zPClpWUTCyXM", canonicalParent: "Marriott International" },
  { label: "Westin", brandId: "recIPuBC50fv13zRR", canonicalParent: "Marriott International" },
  { label: "AC Hotels by Marriott", brandId: "rec9aZp7GHtzUEg0c", canonicalParent: "Marriott International" },
  { label: "Curio Collection by Hilton", brandId: "receQkxgjlezsc1xg", canonicalParent: "Hilton" },
  { label: "Tapestry Collection by Hilton", brandId: "reccXxMHEh7NNRhIE", canonicalParent: "Hilton" },
  { label: "Canopy by Hilton", brandId: "recsggfbKlJbjeRP9", canonicalParent: "Hilton" },
  { label: "Tempo by Hilton", brandId: "recqiHq3GHKMj8Meo", canonicalParent: "Hilton" },
  { label: "Ascend Hotel Collection", brandId: "reclkgOzvAcBheUSo", canonicalParent: "Choice Hotels" },
  { label: "Radisson Individuals by Choice", brandId: "recRyvM8OmLlDj9G7", canonicalParent: "Choice Hotels" },
  { label: "Radisson Blu by Choice", brandId: "recWPEvxBQxVVzSq3", canonicalParent: "Choice Hotels" },
  { label: "Radisson RED by Choice", brandId: "recmKqo7M7mLZgRqQ", canonicalParent: "Choice Hotels" },
  { label: "Hotel Indigo", brandId: "recegXrqaPiSLGCIe", canonicalParent: "IHG" },
  { label: "Kimpton", brandId: "recCKuXCmGvxHPfb3", canonicalParent: "IHG" },
];

const FIELDS = [
  "Brand Name",
  "Parent Company",
  "Brand Status",
  "Hotel Chain Scale",
  "Brand Model",
];

function cell(v) {
  if (v == null) return null;
  if (Array.isArray(v)) return cell(v[0]);
  if (typeof v === "object" && v.name != null) return String(v.name).trim();
  const s = String(v).trim();
  return s || null;
}

function normalizeParent(raw) {
  const s = String(raw || "").trim();
  if (!s) return { canonical: null, normalizeRequired: false };
  const key = s.toLowerCase();
  if (key.startsWith("marriott")) {
    return {
      canonical: "Marriott International",
      normalizeRequired: s !== "Marriott International",
      observed: s,
    };
  }
  if (key.startsWith("hilton")) {
    return {
      canonical: "Hilton",
      normalizeRequired: !["hilton", "hilton worldwide", "hilton worldwide holdings"].includes(key) && s !== "Hilton",
      observed: s,
    };
  }
  if (key.includes("choice")) {
    return {
      canonical: "Choice Hotels",
      normalizeRequired: s !== "Choice Hotels",
      observed: s,
    };
  }
  if (key.includes("ihg") || key.includes("intercontinental hotels")) {
    return {
      canonical: "IHG",
      normalizeRequired: s !== "IHG",
      observed: s,
    };
  }
  if (key.includes("accor")) {
    return {
      canonical: "Accor",
      normalizeRequired: s !== "Accor",
      observed: s,
    };
  }
  return { canonical: s, normalizeRequired: false, observed: s };
}

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
if (!apiKey || !baseId) {
  console.error("Missing AIRTABLE_API_KEY / AIRTABLE_BASE_ID");
  process.exit(1);
}

const base = new Airtable({ apiKey }).base(baseId);
const table = process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";

const knownIds = EXPECTED.map((e) => e.brandId);
const formula = `OR(${knownIds.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
const recs = await base(table)
  .select({ filterByFormula: formula, fields: FIELDS })
  .all();
const byId = new Map(recs.map((r) => [r.id, r]));

const nameHits = await base(table)
  .select({
    filterByFormula:
      "OR(FIND('MGallery',{Brand Name}),FIND('M Gallery',{Brand Name}),FIND('Kimpton',{Brand Name}))",
    fields: FIELDS,
    maxRecords: 30,
  })
  .all();

const rows = EXPECTED.map((e) => {
  const r = byId.get(e.brandId);
  const parentRaw = cell(r?.fields?.["Parent Company"]);
  const parentNorm = normalizeParent(parentRaw);
  const status = cell(r?.fields?.["Brand Status"]);
  const validParent =
    !!parentNorm.canonical &&
    parentNorm.canonical.toLowerCase() === e.canonicalParent.toLowerCase();
  return {
    BRAND: e.label,
    BRAND_ID: e.brandId,
    CURRENT_PARENT: parentRaw,
    CANONICAL_PARENT: parentNorm.canonical,
    EXPECTED_PARENT: e.canonicalParent,
    SOURCE: "Brand Setup - Brand Basics",
    VALID: Boolean(r) && isBrandStatusActive(status) && validParent,
    ACTIVE_LIVE: r ? isBrandStatusActive(status) : false,
    BRAND_STATUS: status,
    BRAND_NAME_LIVE: cell(r?.fields?.["Brand Name"]),
    CHAIN_SCALE: cell(r?.fields?.["Hotel Chain Scale"]),
    BRAND_MODEL: cell(r?.fields?.["Brand Model"]),
    NORMALIZATION_REQUIRED: parentNorm.normalizeRequired || false,
    FOUND: Boolean(r),
    PARENT_MATCH: validParent,
  };
});

const mgalleryCandidates = nameHits.map((r) => ({
  brandId: r.id,
  brandName: cell(r.fields["Brand Name"]),
  parent: cell(r.fields["Parent Company"]),
  status: cell(r.fields["Brand Status"]),
  activeLive: isBrandStatusActive(r.fields["Brand Status"]),
  chainScale: cell(r.fields["Hotel Chain Scale"]),
  brandModel: cell(r.fields["Brand Model"]),
  parentNorm: normalizeParent(cell(r.fields["Parent Company"])),
}));

const report = {
  generatedAt: new Date().toISOString(),
  AIRTABLE_WRITES: 0,
  LIVE_PROVIDER_CALLS: 0,
  rows,
  mgalleryAndKimptonNameHits: mgalleryCandidates,
  missingIds: rows.filter((r) => !r.FOUND).map((r) => r.BRAND_ID),
  invalid: rows.filter((r) => !r.VALID),
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2), "utf8");
console.log(JSON.stringify(report, null, 2));
console.log(`\nWrote ${OUT}`);
