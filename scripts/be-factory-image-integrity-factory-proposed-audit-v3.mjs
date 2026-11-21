#!/usr/bin/env node
/**
 * Factory Proposed / frozen candidate image audit — Image Integrity V3.
 * Scans public/brand-explorer-phase2e-preview/*-factory2-frozen-approval-payload-v1.json
 * and reports gallery/scenario completeness + ephemeral CDN + exact-brand heuristic.
 *
 * $0 · read-only · no Airtable writes
 */
import { readdirSync, readFileSync, mkdirSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import {
  BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3,
  GALLERY_SLOTS,
  SCENARIO_SLOTS,
  IMAGE_V3_CLASS,
  isAirtableSignedCdnUrl,
  evaluateExactBrandImageIdentity,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v3.js";

const ROOT = process.cwd();
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");
const OUT = join(ROOT, "reports/brand-explorer/portfolio-image-integrity-v3");
mkdirSync(OUT, { recursive: true });

function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

const files = existsSync(PREVIEW)
  ? readdirSync(PREVIEW).filter((f) => /-factory2-frozen-approval-payload-v1\.json$/i.test(f))
  : [];

const brands = [];
let galleryGaps = 0;
let scenarioGaps = 0;
let ephemeralCdnSlots = 0;
let siblingRejects = 0;

for (const file of files) {
  let payload;
  try {
    payload = JSON.parse(readFileSync(join(PREVIEW, file), "utf8"));
  } catch {
    continue;
  }
  const blocks = payload?.brandExplorer?.blocks || payload?.blocks || [];
  const brandId = payload.brandId || null;
  const brandName = payload.brandName || file.replace(/-factory2-frozen.*/, "");
  const slug = payload.slug || slugify(brandName);
  const bySlot = new Map();
  for (const b of blocks) {
    const sk = String(b.slotKey || "").trim();
    if (!sk) continue;
    if (!bySlot.has(sk)) bySlot.set(sk, []);
    bySlot.get(sk).push(b);
  }
  const gallery = GALLERY_SLOTS.map((sk) => {
    const b = (bySlot.get(sk) || [])[0];
    const url = b?.imageUrl || b?.image_url || "";
    const exact = evaluateExactBrandImageIdentity({
      targetBrandId: brandId,
      targetSlug: slug,
      imageUrl: url,
      imageMeta: { title: b?.title, body: b?.body, propertyName: b?.title },
    });
    const ephemeral = isAirtableSignedCdnUrl(url);
    if (ephemeral) ephemeralCdnSlots += 1;
    if (exact.failureCode === "IMAGE_SIBLING_BRAND_REJECTED") siblingRejects += 1;
    return {
      slotKey: sk,
      hasUrl: !!url,
      ephemeralCdn: ephemeral,
      exactBrandDecision: exact.decision,
      exactBrandOk: exact.ok,
      v3Hint: !url
        ? IMAGE_V3_CLASS.IMAGE_MISSING
        : ephemeral
          ? IMAGE_V3_CLASS.IMAGE_SIGNED_URL_EXPIRED
          : exact.ok === false && exact.failureCode === "IMAGE_SIBLING_BRAND_REJECTED"
            ? IMAGE_V3_CLASS.IMAGE_SIBLING_BRAND
            : IMAGE_V3_CLASS.IMAGE_ATTACHMENT_PRESENT,
    };
  });
  const scenarios = SCENARIO_SLOTS.map((sk) => {
    const b = (bySlot.get(sk) || [])[0];
    const url = b?.imageUrl || b?.image_url || "";
    const exact = evaluateExactBrandImageIdentity({
      targetBrandId: brandId,
      targetSlug: slug,
      imageUrl: url,
      imageMeta: { title: b?.title, body: b?.body, propertyName: b?.title },
    });
    const ephemeral = isAirtableSignedCdnUrl(url);
    if (ephemeral) ephemeralCdnSlots += 1;
    if (exact.failureCode === "IMAGE_SIBLING_BRAND_REJECTED") siblingRejects += 1;
    return {
      slotKey: sk,
      hasUrl: !!url,
      ephemeralCdn: ephemeral,
      exactBrandDecision: exact.decision,
      exactBrandOk: exact.ok,
      v3Hint: !url
        ? IMAGE_V3_CLASS.IMAGE_MISSING
        : ephemeral
          ? IMAGE_V3_CLASS.IMAGE_SIGNED_URL_EXPIRED
          : IMAGE_V3_CLASS.IMAGE_ATTACHMENT_PRESENT,
    };
  });
  const galleryCount = gallery.filter((g) => g.hasUrl).length;
  const scenarioCount = scenarios.filter((s) => s.hasUrl).length;
  if (galleryCount < 6) galleryGaps += 1;
  if (scenarioCount < 3) scenarioGaps += 1;
  brands.push({
    file,
    brandId,
    brandName,
    slug,
    galleryCount,
    scenarioCount,
    gallery,
    scenarios,
    frozenContract: payload.contract || null,
  });
}

const summary = {
  contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3,
  at: new Date().toISOString(),
  frozenPayloadFiles: files.length,
  brandsAudited: brands.length,
  brandsWithSixGalleryUrls: brands.filter((b) => b.galleryCount >= 6).length,
  brandsWithThreeScenarioUrls: brands.filter((b) => b.scenarioCount >= 3).length,
  galleryGaps,
  scenarioGaps,
  ephemeralCdnSlots,
  siblingRejects,
  paidResearchSpendUsd: 0,
  note:
    "URL presence ≠ certification. Ephemeral airtableusercontent URLs require CURRENT delivery rehydration (V2) before browser cert.",
};

writeFileSync(join(OUT, "10-factory-proposed-audit-summary.json"), JSON.stringify(summary, null, 2) + "\n");
writeFileSync(
  join(OUT, "11-factory-proposed-by-brand.json"),
  JSON.stringify({ contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3, brands }, null, 2) + "\n"
);
console.log(JSON.stringify(summary, null, 2));
