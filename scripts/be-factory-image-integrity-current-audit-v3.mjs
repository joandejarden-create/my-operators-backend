#!/usr/bin/env node
/**
 * Full live Airtable CURRENT image audit — Image Integrity V3.
 *
 * Requeries Active/Live Brand Basics + Presentation Image slots.
 * Classifies every required gallery/scenario attachment with V3 vocabulary,
 * exact-brand heuristic, duplicate detection, and optional HTTP probe/delivery check.
 *
 *   node scripts/be-factory-image-integrity-current-audit-v3.mjs
 *   node scripts/be-factory-image-integrity-current-audit-v3.mjs --probe
 *   node scripts/be-factory-image-integrity-current-audit-v3.mjs --probe --delivery
 *
 * $0 — no paid research. Non-destructive. No Airtable writes.
 */
import "../load-env.js";
import { createHash } from "crypto";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import Airtable from "airtable";
import {
  BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3,
  REQUIRED_IMAGE_SLOTS,
  GALLERY_SLOTS,
  SCENARIO_SLOTS,
  IMAGE_V3_CLASS,
  classifyLiveAttachment,
  mapV2ClassToV3,
  evaluateGalleryCertLayer,
  evaluateScenarioCertLayer,
  evaluateExactBrandImageIdentity,
  isAirtableSignedCdnUrl,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v3.js";
import { probeAttachmentUrl } from "../lib/brand-explorer/factory-2/be-factory-image-attachment-ingest-v2.js";
import {
  ensureLibraryDirs,
  summarizeLibrary,
} from "../lib/brand-explorer/factory-2/be-shared-verified-hotel-image-library-v1.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/brand-explorer/portfolio-image-integrity-v3");
mkdirSync(OUT, { recursive: true });
ensureLibraryDirs();

const probe = process.argv.includes("--probe");
const delivery = process.argv.includes("--delivery");
const BASE_URL = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(
  /\/$/,
  ""
);

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const TABLE = "Brand Setup - Brand Explorer Presentation";
const REQUIRED = new Set(REQUIRED_IMAGE_SLOTS);

function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function fingerprintAtt(att) {
  if (!att) return null;
  const id = att.id || "";
  const url = String(att.url || "").split("?")[0];
  return createHash("sha256").update(`${id}|${url}|${att.filename || ""}`).digest("hex");
}

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
      slug: slugify(r.get("Brand Name")),
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
  process.stderr.write(
    `v3-audit brands ${Math.min(i + 8, names.length)}/${names.length} presentationRows=${rows.length}\n`
  );
}

/** brandId → slotKey → rows[] */
const byBrandSlot = new Map();
const classCounts = Object.fromEntries(Object.values(IMAGE_V3_CLASS).map((k) => [k, 0]));
const matrix = [];
const duplicateConflicts = [];
const hashCounts = new Map();

for (const r of rows) {
  const slot = String(r.get("Slot Key") || "").trim();
  if (!REQUIRED.has(slot) || r.get("Active") === false) continue;
  const brandIds = r.get("Brand") || [];
  const brandId = Array.isArray(brandIds) ? brandIds[0] : null;
  if (!brandId) continue;
  if (!byBrandSlot.has(brandId)) byBrandSlot.set(brandId, new Map());
  const slotMap = byBrandSlot.get(brandId);
  if (!slotMap.has(slot)) slotMap.set(slot, []);
  slotMap.get(slot).push(r);
}

async function probeDelivery(recordId) {
  try {
    const res = await fetch(
      `${BASE_URL}/api/brand-library/presentation-image/${encodeURIComponent(recordId)}`,
      { method: "GET", redirect: "follow" }
    );
    const buf = Buffer.from(await res.arrayBuffer());
    return {
      ok: res.ok && buf.length > 32,
      httpStatus: res.status,
      contentType: res.headers.get("content-type"),
      byteLength: buf.length,
      magic: buf.slice(0, 3).toString("hex"),
    };
  } catch (err) {
    return { ok: false, error: String(err?.message || err) };
  }
}

for (const [brandId, slotMap] of byBrandSlot) {
  const brand = brandById.get(brandId) || { id: brandId, name: "?", slug: "" };
  const brandSlots = [];
  for (const slot of REQUIRED_IMAGE_SLOTS) {
    const list = slotMap.get(slot) || [];
    if (list.length > 1) {
      duplicateConflicts.push({
        brandId,
        brandName: brand.name,
        slotKey: slot,
        recordIds: list.map((r) => r.id),
        count: list.length,
      });
    }
    // Deterministic resolution: prefer row with attachment, then lowest record id
    const sorted = list.slice().sort((a, b) => {
      const aAtt = Array.isArray(a.get("Image")) && a.get("Image")[0] ? 1 : 0;
      const bAtt = Array.isArray(b.get("Image")) && b.get("Image")[0] ? 1 : 0;
      if (aAtt !== bAtt) return bAtt - aAtt;
      return String(a.id).localeCompare(String(b.id));
    });
    const primary = sorted[0] || null;
    const img = primary ? primary.get("Image") : null;
    const att = Array.isArray(img) && img[0] ? img[0] : null;
    let probeResult = null;
    let deliveryResult = null;
    if (probe && att?.url) {
      probeResult = await probeAttachmentUrl(att.url);
    }
    if (delivery && primary?.id) {
      deliveryResult = await probeDelivery(primary.id);
    }
    const v2Class = classifyLiveAttachment({ attachment: att, probe: probeResult });
    const fp = fingerprintAtt(att);
    if (fp) hashCounts.set(fp, (hashCounts.get(fp) || 0) + 1);

    // Affinity must use attachment filename only — NOT Body/Title copy and NOT
    // ephemeral Airtable CDN URLs (signed path tokens can randomly match sibling
    // substrings like "tempo"/"curio"). Marketing titles often name sibling brands
    // on purpose (e.g. "Hard Separation From Regency … And Thompson").
    const exactBrand = evaluateExactBrandImageIdentity({
      targetBrandId: brandId,
      targetSlug: brand.slug,
      imageUrl: "",
      imageMeta: {
        propertyName: String(att?.filename || ""),
        filename: String(att?.filename || ""),
        label: String(att?.filename || ""),
      },
      // Without census join, property brand is unverified unless affinity passes.
      propertyVerified: false,
    });

    const duplicate = fp ? (hashCounts.get(fp) || 0) > 1 : false;
    const techOk =
      (probeResult && probeResult.ok && probeResult.decodeOk !== false) ||
      (deliveryResult && deliveryResult.ok);
    const v3Class = mapV2ClassToV3(v2Class, {
      exactBrand: exactBrand.ok ? exactBrand : null,
      // Do not brand-verify from title heuristics alone at CURRENT audit time
      // unless affinity explicitly passes with brand hints.
      rightsOk: null,
      browserOk: null,
      duplicate: false,
    });
    // Prefer technical mapping; overlay sibling only when affinity rejects.
    let finalClass = v3Class;
    if (!att) finalClass = IMAGE_V3_CLASS.IMAGE_MISSING;
    else if (probeResult?.httpStatus === 410 || isAirtableSignedCdnUrl(att.url) && probeResult?.ok === false) {
      finalClass = IMAGE_V3_CLASS.IMAGE_SIGNED_URL_EXPIRED;
    } else if (techOk) finalClass = IMAGE_V3_CLASS.IMAGE_TECHNICALLY_VALID;
    else if (att) finalClass = IMAGE_V3_CLASS.IMAGE_ATTACHMENT_PRESENT;

    if (exactBrand.ok === false && exactBrand.failureCode === "IMAGE_SIBLING_BRAND_REJECTED") {
      finalClass = IMAGE_V3_CLASS.IMAGE_SIBLING_BRAND;
    }

    classCounts[finalClass] = (classCounts[finalClass] || 0) + 1;

    const entry = {
      brandId,
      brandName: brand.name,
      brandSlug: brand.slug,
      slotKey: slot,
      recordId: primary?.id || null,
      duplicateRowCount: list.length,
      attachmentId: att?.id || null,
      attachmentFilename: att?.filename || null,
      attachmentUrlHost: (() => {
        try {
          return att?.url ? new URL(att.url).hostname : null;
        } catch {
          return null;
        }
      })(),
      fingerprint: fp,
      v2Class,
      v3Class: finalClass,
      exactBrandDecision: exactBrand.decision,
      exactBrandOk: exactBrand.ok,
      probe: probeResult
        ? {
            ok: probeResult.ok,
            httpStatus: probeResult.httpStatus,
            format: probeResult.format,
            width: probeResult.width,
            height: probeResult.height,
          }
        : null,
      delivery: deliveryResult,
      rightsOk: null,
      browserOk: null,
    };
    brandSlots.push(entry);
    matrix.push(entry);
  }

  // Mark cross-slot duplicates within brand (same fingerprint on different slots)
  const fps = new Map();
  for (const e of brandSlots) {
    if (!e.fingerprint) continue;
    if (!fps.has(e.fingerprint)) fps.set(e.fingerprint, []);
    fps.get(e.fingerprint).push(e.slotKey);
  }
  for (const e of brandSlots) {
    if (e.fingerprint && (fps.get(e.fingerprint) || []).length > 1) {
      e.duplicateWithinBrand = true;
      if (e.v3Class === IMAGE_V3_CLASS.IMAGE_TECHNICALLY_VALID) {
        // keep technical valid but flag
        e.duplicateFlag = true;
      }
    }
  }
}

const byBrandSummary = [];
for (const [brandId, slotMap] of byBrandSlot) {
  const brand = brandById.get(brandId);
  const entries = matrix.filter((m) => m.brandId === brandId);
  const gallery = entries.filter((e) => GALLERY_SLOTS.includes(e.slotKey));
  const scenarios = entries.filter((e) => SCENARIO_SLOTS.includes(e.slotKey));
  const missing = entries.filter((e) => e.v3Class === IMAGE_V3_CLASS.IMAGE_MISSING).length;
  const sibling = entries.filter((e) => e.v3Class === IMAGE_V3_CLASS.IMAGE_SIBLING_BRAND).length;
  const techValid = entries.filter(
    (e) => e.v3Class === IMAGE_V3_CLASS.IMAGE_TECHNICALLY_VALID
  ).length;
  byBrandSummary.push({
    brandId,
    brandName: brand?.name,
    brandSlug: brand?.slug,
    requiredSlots: REQUIRED_IMAGE_SLOTS.length,
    populatedSlots: entries.filter((e) => e.v3Class !== IMAGE_V3_CLASS.IMAGE_MISSING).length,
    missing,
    sibling,
    techValid,
    rawRowsForRequired: [...slotMap.values()].reduce((n, arr) => n + arr.length, 0),
    uniqueRequiredSlots: slotMap.size,
    galleryLayer: evaluateGalleryCertLayer(gallery),
    scenarioLayer: evaluateScenarioCertLayer(scenarios),
  });
}

const rawRequiredRows = matrix.length;
const uniqueSlots = byBrandSummary.reduce((n, b) => n + b.uniqueRequiredSlots, 0);

const summary = {
  contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3,
  at: new Date().toISOString(),
  baseId: process.env.AIRTABLE_BASE_ID,
  probe,
  delivery,
  activeLiveBrands: brands.length,
  rawRequiredImageRows: rawRequiredRows,
  uniqueBrandSlotPairs: uniqueSlots,
  duplicateSlotConflicts: duplicateConflicts.length,
  classCounts,
  brandsWithAllNinePresent: byBrandSummary.filter((b) => b.missing === 0).length,
  brandsWithAllSixGalleryPresent: 0,
  brandsWithAllThreeScenarioPresent: 0,
  brandsWithSiblingReject: byBrandSummary.filter((b) => b.sibling > 0).length,
  library: summarizeLibrary(),
  paidResearchSpendUsd: 0,
  note:
    "Exact-brand CERTIFIED requires census-linked property verification + rights + browser; this audit reports technical CURRENT state and sibling heuristic rejects.",
};

function galleryPresent(b, matrixAll) {
  return matrixAll.filter(
    (m) =>
      m.brandId === b.brandId &&
      GALLERY_SLOTS.includes(m.slotKey) &&
      m.v3Class !== IMAGE_V3_CLASS.IMAGE_MISSING
  ).length;
}
function scenarioPresent(b, matrixAll) {
  return matrixAll.filter(
    (m) =>
      m.brandId === b.brandId &&
      SCENARIO_SLOTS.includes(m.slotKey) &&
      m.v3Class !== IMAGE_V3_CLASS.IMAGE_MISSING
  ).length;
}

summary.brandsWithAllSixGalleryPresent = byBrandSummary.filter(
  (b) => galleryPresent(b, matrix) === 6
).length;
summary.brandsWithAllThreeScenarioPresent = byBrandSummary.filter(
  (b) => scenarioPresent(b, matrix) === 3
).length;

writeFileSync(join(OUT, "01-current-audit-summary.json"), JSON.stringify(summary, null, 2) + "\n");
writeFileSync(
  join(OUT, "02-current-audit-by-brand.json"),
  JSON.stringify({ contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3, brands: byBrandSummary }, null, 2) +
    "\n"
);
writeFileSync(
  join(OUT, "03-current-slot-matrix.json"),
  JSON.stringify({ contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3, slots: matrix }, null, 2) + "\n"
);
writeFileSync(
  join(OUT, "04-duplicate-slot-conflicts.json"),
  JSON.stringify(
    { contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3, conflicts: duplicateConflicts },
    null,
    2
  ) + "\n"
);

const md = [
  `# Brand Explorer Image Integrity V3 — CURRENT Airtable Audit`,
  ``,
  `- At: ${summary.at}`,
  `- Active/Live brands: **${summary.activeLiveBrands}**`,
  `- Raw required image rows: **${summary.rawRequiredImageRows}**`,
  `- Unique brand×slot pairs: **${summary.uniqueBrandSlotPairs}**`,
  `- Duplicate slot conflicts: **${summary.duplicateSlotConflicts}**`,
  `- Brands with all 9 present: **${summary.brandsWithAllNinePresent}**`,
  `- Brands with 6 gallery present: **${summary.brandsWithAllSixGalleryPresent}**`,
  `- Brands with 3 scenario present: **${summary.brandsWithAllThreeScenarioPresent}**`,
  `- Probe: ${probe} · Delivery check: ${delivery}`,
  `- Paid research: $0`,
  ``,
  `## Classification counts`,
  ``,
  ...Object.entries(classCounts).map(([k, v]) => `- ${k}: **${v}**`),
  ``,
  `## Notes`,
  ``,
  summary.note,
  ``,
].join("\n");
writeFileSync(join(OUT, "01-current-audit-summary.md"), md);

console.log(JSON.stringify(summary, null, 2));
