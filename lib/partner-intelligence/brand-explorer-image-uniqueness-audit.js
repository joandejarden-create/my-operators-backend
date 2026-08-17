/**
 * Brand Explorer image uniqueness audit — live rendered presentation rows.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import {
  evaluateImageUniqueness,
  evaluateImageUniquenessForTest,
  IMAGE_UNIQUENESS_VERSION,
} from "./brand-explorer-image-uniqueness.js";
import { pickDistinctImageAssets } from "./brand-explorer-image-uniqueness.js";

export { evaluateImageUniqueness, evaluateImageUniquenessForTest, IMAGE_UNIQUENESS_VERSION };

export const REPORT_JSON = "brand-explorer-image-uniqueness-audit.json";
export const REPORT_MD = "brand-explorer-image-uniqueness-audit.md";

const BRAND_MD = Object.freeze({
  "hotel-indigo": "brand-explorer-image-uniqueness-hotel-indigo.md",
  "mgallery-collection": "brand-explorer-image-uniqueness-mgallery.md",
  "small-luxury-hotels-of-the-world": "brand-explorer-image-uniqueness-slh.md",
});

const DEFAULT_BRANDS = Object.freeze([
  "hotel-indigo",
  "mgallery-collection",
  "small-luxury-hotels-of-the-world",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

async function fetchBrandApi(slugOrId) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const { getActiveProfileBrandConfig } = await import("./brand-explorer-active-profile-brand-config.js");
  const { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } = await import(
    "./brand-explorer-factory-preview-candidates.js"
  );
  const cfg = getActiveProfileBrandConfig(slugOrId);
  const factory = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[String(slugOrId || "").trim().toLowerCase()];
  const brandId = cfg?.recordId || factory?.recordId || slugOrId;
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`Brand API failed for ${slugOrId}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

function proposeReplacements(result, registryAssets = []) {
  const used = new Set(
    [...(result.gallery || []), ...(result.scenarios || []), ...(result.properties || [])]
      .filter((i) => i.uniquenessStatus === "unique")
      .map((i) => i.duplicateGroupId)
  );
  const inventory = (registryAssets || [])
    .map((a) => ({
      imageUrl: a.sourceUrl || a.imageUrl,
      sourcePageUrl: a.sourcePageUrl,
      propertyName: a.propertyName || a.assetName,
      title: a.assetName,
    }))
    .filter((a) => a.imageUrl);

  const proposals = [];
  for (const g of result.duplicateGroups || []) {
    const keep = (g.slots || [])[0];
    const rest = (g.slots || []).slice(1);
    for (const slot of rest) {
      const picks = pickDistinctImageAssets(inventory, 1, { excludeGroupIds: [...used] });
      const pick = picks[0] || null;
      if (pick?._imageIdentity?.duplicateGroupId) used.add(pick._imageIdentity.duplicateGroupId);
      proposals.push({
        section: g.section,
        slot,
        currentImage: g.imageUrl,
        canonicalImageId: g.sourceImageId || g.duplicateGroupId,
        duplicateGroup: g.duplicateGroupId,
        status: "needs_reassignment",
        requiredFix: g.requiredFix,
        proposedReplacement: pick?.imageUrl || null,
        keepSlot: keep,
      });
    }
  }
  return proposals;
}

export async function auditBrandImageUniqueness(brandSlug) {
  const { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } = await import(
    "./brand-explorer-factory-preview-candidates.js"
  );
  const factory = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[String(brandSlug || "").trim().toLowerCase()];
  const brand = await fetchBrandApi(brandSlug);
  const ctx = await loadBrandFactoryContext(factory?.recordId || brandSlug);
  const rows = ctx.presentationRows || brand?.brandExplorer?.blocks || [];
  const result = evaluateImageUniqueness({
    brand,
    presentationRows: rows,
    brandSlug,
  });
  const proposals = proposeReplacements(result, ctx.registryAssets || []);
  const fieldRows = [];
  for (const section of ["gallery", "scenarios", "properties"]) {
    const list = result[section] || [];
    for (const img of list) {
      const dup = (result.duplicateGroups || []).find(
        (g) => g.duplicateGroupId === img.duplicateGroupId && g.section === (section === "scenarios" ? "scenario" : section === "properties" ? "property_example" : "gallery")
      );
      const proposal = proposals.find((p) => p.slot === img.slotKey || p.slot?.startsWith?.(img.slotKey));
      fieldRows.push({
        brand: brandSlug,
        section: section === "scenarios" ? "scenario" : section === "properties" ? "property_example" : "gallery",
        slot: img.slotKey,
        currentImage: img.imageUrl,
        canonicalImageId: img.sourceImageId || img.canonicalImageUrl,
        duplicateGroup: img.duplicateGroupId,
        status: img.uniquenessStatus,
        requiredFix: dup ? dup.requiredFix : img.imageUrl ? "no_action" : "add_image",
        proposedReplacement: proposal?.proposedReplacement || null,
        title: img.title,
        recordId: img.recordId,
      });
    }
  }
  return {
    ...result,
    proposals,
    fieldRows,
    liveState: {
      displayState: brand.brandExplorerDisplayState,
      shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
    },
  };
}

export async function runImageUniquenessAudit({ brands = DEFAULT_BRANDS } = {}) {
  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await auditBrandImageUniqueness(brandSlug));
  }
  return {
    version: IMAGE_UNIQUENESS_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands,
    brandResults,
    summary: {
      brandCount: brandResults.length,
      passCount: brandResults.filter((b) => b.pass).length,
      failCount: brandResults.filter((b) => !b.pass).length,
      auditPass: brandResults.every((b) => b.pass),
    },
    auditPass: brandResults.every((b) => b.pass),
  };
}

function brandMd(b) {
  const lines = [
    `# Image Uniqueness — ${b.brandName || b.brandSlug}`,
    "",
    `Slug: \`${b.brandSlug}\``,
    `auditPass: **${b.pass}**`,
    `galleryDistinct: **${b.galleryDistinctCount}** / slots ${b.gallerySlotCount}`,
    `scenarioDistinct: **${b.scenarioDistinctCount}** / slots ${b.scenarioSlotCount}`,
    `propertyDistinct: **${b.propertyExampleDistinctCount}** / slots ${b.propertySlotCount}`,
    "",
    "## Findings",
    "",
  ];
  if (!(b.findings || []).length) lines.push("- none");
  for (const f of b.findings || []) lines.push(`- **${f.id}**: ${f.detail}`);
  lines.push("", "## Field matrix", "");
  lines.push(
    "| Brand | Section | Slot | Current Image | Canonical Image ID | Duplicate Group | Status | Required Fix | Proposed Replacement |"
  );
  lines.push("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
  for (const r of b.fieldRows || []) {
    lines.push(
      `| ${r.brand} | ${r.section} | ${r.slot} | ${(r.currentImage || "").slice(0, 80)} | ${r.canonicalImageId || ""} | ${r.duplicateGroup || ""} | ${r.status} | ${r.requiredFix} | ${(r.proposedReplacement || "—").toString().slice(0, 60)} |`
    );
  }
  lines.push("");
  return lines.join("\n");
}

export function writeImageUniquenessReports(report, { reportsDir = path.join(ROOT, "reports") } = {}) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const md = [
    "# Brand Explorer Image Uniqueness Audit",
    "",
    `Generated: ${report.generatedAt}`,
    `auditPass: **${report.auditPass}**`,
    "",
    `- Brands: **${report.summary.brandCount}**`,
    `- Pass: **${report.summary.passCount}**`,
    `- Fail: **${report.summary.failCount}**`,
    "",
  ];
  for (const b of report.brandResults) {
    md.push(`### ${b.brandSlug}`);
    md.push(
      `- pass: **${b.pass}** · galleryDistinct=${b.galleryDistinctCount}/${b.gallerySlotCount} · scenario=${b.scenarioDistinctCount} · property=${b.propertyExampleDistinctCount}`
    );
    md.push("");
    const per = BRAND_MD[b.brandSlug];
    if (per) {
      const p = path.join(reportsDir, per);
      fs.writeFileSync(p, brandMd(b));
      md.push(`- Per-brand: \`${per}\``);
      md.push("");
    }
  }
  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}
