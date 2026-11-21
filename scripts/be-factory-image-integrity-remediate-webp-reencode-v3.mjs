#!/usr/bin/env node
/**
 * Re-encode CURRENT webp attachments → JPEG via wsrv of the live Airtable CDN URL,
 * then content-API replace+read-back. No new research ($0).
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  ingestPresentationImageFromSource,
  readPresentationImage,
} from "../lib/brand-explorer/factory-2/be-factory-image-attachment-ingest-v2.js";
import {
  upsertVerifiedImageAsset,
  SCENE_CATEGORY,
  summarizeLibrary,
} from "../lib/brand-explorer/factory-2/be-shared-verified-hotel-image-library-v1.js";

const OUT = join(process.cwd(), "reports/brand-explorer/portfolio-image-integrity-v3");
mkdirSync(OUT, { recursive: true });
const apply = process.argv.includes("--apply");

const JOBS = [
  {
    brandId: "recRyvM8OmLlDj9G7",
    brandName: "Radisson Individuals by Choice",
    brandSlug: "radisson-individuals-by-choice",
    recordId: "reco6kIWyBsLOwohj",
    slotKey: "materials.gallery.6",
  },
  {
    brandId: "recjjSnY2opb8P4DG",
    brandName: "Small Luxury Hotels of the World",
    brandSlug: "small-luxury-hotels-of-the-world",
    recordId: "recMOXzrBAeMD99Vn",
    slotKey: "overview.scenario.1",
  },
  {
    brandId: "recjjSnY2opb8P4DG",
    brandName: "Small Luxury Hotels of the World",
    brandSlug: "small-luxury-hotels-of-the-world",
    recordId: "recDlSumnIo8GK1Pv",
    slotKey: "overview.scenario.2",
  },
];

const rows = [];
for (const job of JOBS) {
  const before = await readPresentationImage(job.recordId);
  if (!before.attachment?.url) {
    rows.push({ ...job, ok: false, reason: "missing_attachment" });
    continue;
  }
  const proxy = `https://wsrv.nl/?url=${encodeURIComponent(before.attachment.url)}&output=jpg&q=85`;
  const result = await ingestPresentationImageFromSource({
    recordId: job.recordId,
    sourceUrl: proxy,
    slotKey: job.slotKey,
    dryRun: !apply,
    replaceExisting: true,
  });
  if (result.ok && apply && result.fetch?.byteHash) {
    upsertVerifiedImageAsset(
      {
        contentHash: result.fetch.byteHash,
        mimeType: "image/jpeg",
        width: result.fetch.width,
        height: result.fetch.height,
        byteSize: result.fetch.bytes,
        brandBasicsId: job.brandId,
        brandSlug: job.brandSlug,
        brandName: job.brandName,
        propertyName: before.title || null,
        sceneCategory: SCENE_CATEGORY.OTHER,
        retrievalMethod: "airtable_webp_reencode_wsrv",
        permissions: {
          internalResearch: true,
          brandExplorerDisplay: true,
          hotelExplorerDisplay: false,
          customerFacing: false,
          externalDistribution: false,
        },
        presentationRecordIds: [job.recordId],
        technicalValid: true,
        certificationStatus: "STAGED",
        verifiedAt: new Date().toISOString(),
      },
      { dryRun: false }
    );
  }
  rows.push({ ...job, ok: result.ok, lifecycle: result.lifecycle, reason: result.reason });
  console.error(`${result.ok ? "OK" : "FAIL"} ${job.brandSlug} ${job.slotKey}`);
}

const stamp = new Date().toISOString().replace(/[:.]/g, "-");
const path = join(OUT, `23-webp-reencode-${apply ? "APPLY" : "DRY"}-${stamp}.json`);
writeFileSync(path, JSON.stringify({ apply, rows, library: summarizeLibrary() }, null, 2) + "\n");
console.log(JSON.stringify({ report: path, ok: rows.every((r) => r.ok), library: summarizeLibrary() }, null, 2));
process.exit(rows.every((r) => r.ok) ? 0 : 1);
