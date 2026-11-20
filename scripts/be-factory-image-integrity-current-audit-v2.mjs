#!/usr/bin/env node
/**
 * Live Airtable CURRENT image audit (required gallery+scenario slots).
 * Separates CURRENT attachment presence from Factory PROPOSED completeness.
 *
 * Usage: node scripts/be-factory-image-integrity-current-audit-v2.mjs [--probe]
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import Airtable from "airtable";
import {
  BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2,
  REQUIRED_IMAGE_SLOTS,
  IMAGE_SLOT_CLASS,
  IMAGE_COVERAGE_LAYER,
  classifyLiveAttachment,
  bumpCoverage,
  emptyCoverageTotals,
  isAirtableSignedCdnUrl,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v2.js";
import { probeAttachmentUrl } from "../lib/brand-explorer/factory-2/be-factory-image-attachment-ingest-v2.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/brand-explorer/portfolio-image-integrity-v2");
mkdirSync(OUT, { recursive: true });

const probe = process.argv.includes("--probe");
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const TABLE = "Brand Setup - Brand Explorer Presentation";
const REQUIRED = new Set(REQUIRED_IMAGE_SLOTS);

const brands = await base("Brand Setup - Brand Basics")
  .select({
    filterByFormula: "OR({Brand Status}='Active',{Brand Status}='Live')",
    fields: ["Brand Name", "Brand Status", "Parent Company"],
    pageSize: 100,
  })
  .all();

const brandById = new Map(
  brands.map((r) => [
    r.id,
    {
      id: r.id,
      name: String(r.get("Brand Name") || ""),
      status: String(r.get("Brand Status") || ""),
      parent: String(r.get("Parent Company") || ""),
    },
  ])
);

const names = [...brandById.values()].map((b) => b.name).filter(Boolean);
const rows = [];
const seen = new Set();

async function fetchChunk(chunk) {
  const formula =
    "OR(" +
    chunk.map((n) => `{Brand Name}="${String(n).replace(/"/g, '\\"')}"`).join(",") +
    ")";
  try {
    return await base(TABLE).select({ filterByFormula: formula, pageSize: 100 }).all();
  } catch {
    return [];
  }
}

for (let i = 0; i < names.length; i += 8) {
  const chunk = names.slice(i, i + 8);
  for (const r of await fetchChunk(chunk)) {
    if (seen.has(r.id)) continue;
    seen.add(r.id);
    rows.push(r);
  }
  process.stderr.write(`scanned ${Math.min(i + 8, names.length)}/${names.length} rows=${rows.length}\n`);
}

const byBrand = new Map();
let totals = emptyCoverageTotals();
const matrix = [];

for (const r of rows) {
  const slot = String(r.get("Slot Key") || "").trim();
  if (!REQUIRED.has(slot) || r.get("Active") === false) continue;
  const brandIds = r.get("Brand") || [];
  const brandId = Array.isArray(brandIds) ? brandIds[0] : null;
  const brand = brandById.get(brandId) || {
    id: brandId,
    name: String(r.get("Brand Name") || ""),
  };
  const img = r.get("Image");
  const att = Array.isArray(img) && img[0] ? img[0] : null;
  let probeResult = null;
  if (probe && att?.url) {
    probeResult = await probeAttachmentUrl(att.url);
  } else if (!att) {
    probeResult = null;
  } else if (att?.url && isAirtableSignedCdnUrl(att.url) && !probe) {
    probeResult = null; // presence only
  }
  const classification = classifyLiveAttachment({
    attachment: att,
    probe: probeResult,
  });
  totals = bumpCoverage(totals, classification);
  const key = brand.id || brand.name || r.id;
  if (!byBrand.has(key)) {
    byBrand.set(key, {
      brandId: brand.id,
      brandName: brand.name,
      parent: brand.parent || null,
      slots: {},
      missing: 0,
      present: 0,
    });
  }
  const entry = byBrand.get(key);
  entry.slots[slot] = {
    recordId: r.id,
    title: String(r.get("Title") || ""),
    classification,
    attachmentId: att?.id || null,
    filename: att?.filename || null,
    type: att?.type || null,
    size: att?.size || null,
    urlHost: att?.url
      ? (() => {
          try {
            return new URL(att.url).hostname;
          } catch {
            return null;
          }
        })()
      : null,
    probe: probeResult
      ? {
          ok: probeResult.ok,
          httpStatus: probeResult.httpStatus,
          format: probeResult.format,
          bytes: probeResult.bytes,
        }
      : null,
  };
  if (classification === IMAGE_SLOT_CLASS.ATTACHMENT_MISSING || classification === IMAGE_SLOT_CLASS.MISSING) {
    entry.missing += 1;
  } else {
    entry.present += 1;
  }
  matrix.push({
    brandId: brand.id,
    brandName: brand.name,
    presentationRecordId: r.id,
    slotKey: slot,
    previousAttachment: att?.id || null,
    issue: classification,
    layer: IMAGE_COVERAGE_LAYER.AIRTABLE_CURRENT,
  });
}

const brandList = [...byBrand.values()].sort((a, b) =>
  String(a.brandName).localeCompare(String(b.brandName))
);
const fullyMissing = brandList.filter((b) => b.missing === 9 && b.present === 0);
const partialMissing = brandList.filter((b) => b.missing > 0 && b.present > 0);
const galleryComplete = brandList.filter((b) =>
  REQUIRED_IMAGE_SLOTS.filter((s) => s.startsWith("materials.gallery.")).every(
    (s) => b.slots[s] && b.slots[s].classification !== IMAGE_SLOT_CLASS.ATTACHMENT_MISSING
  )
).length;
const scenarioComplete = brandList.filter((b) =>
  REQUIRED_IMAGE_SLOTS.filter((s) => s.startsWith("overview.scenario.")).every(
    (s) => b.slots[s] && b.slots[s].classification !== IMAGE_SLOT_CLASS.ATTACHMENT_MISSING
  )
).length;

const summary = {
  generatedAt: new Date().toISOString(),
  contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2,
  layer: IMAGE_COVERAGE_LAYER.AIRTABLE_CURRENT,
  baseId: process.env.AIRTABLE_BASE_ID,
  probed: probe,
  activeLiveBrandBasics: brands.length,
  brandsWithRequiredRows: brandList.length,
  totals,
  galleryCompleteBrands: galleryComplete,
  scenarioCompleteBrands: scenarioComplete,
  fullyMissingBrands: fullyMissing.map((b) => ({ id: b.brandId, name: b.brandName })),
  partialMissingBrands: partialMissing.map((b) => ({
    id: b.brandId,
    name: b.brandName,
    missing: Object.entries(b.slots)
      .filter(([, v]) => v.classification === IMAGE_SLOT_CLASS.ATTACHMENT_MISSING)
      .map(([k]) => k),
  })),
};

writeFileSync(join(OUT, "01-current-audit-summary.json"), JSON.stringify(summary, null, 2) + "\n");
writeFileSync(
  join(OUT, "02-current-audit-by-brand.json"),
  JSON.stringify({ generatedAt: summary.generatedAt, brands: brandList }, null, 2) + "\n"
);
writeFileSync(join(OUT, "03-current-slot-matrix.json"), JSON.stringify({ generatedAt: summary.generatedAt, rows: matrix }, null, 2) + "\n");
console.log(JSON.stringify(summary, null, 2));
