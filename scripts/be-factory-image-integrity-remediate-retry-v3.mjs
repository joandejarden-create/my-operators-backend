#!/usr/bin/env node
/**
 * Retry only slots that failed V3 apply read-back (Bunkhouse galleries + Suburban).
 * Uses replaceExisting clear→upload path.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  ingestPresentationImageFromBytes,
  ingestPresentationImageFromSource,
  probeAttachmentUrl,
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

function jpegProxyUrl(sourceUrl) {
  return `https://wsrv.nl/?url=${encodeURIComponent(sourceUrl)}&output=jpg&q=85`;
}

const RETRIES = [
  {
    brandId: "recGv268Wda31PlSZ",
    brandName: "Bunkhouse Hotels",
    brandSlug: "bunkhouse-hotels",
    recordId: "recmQhhB4eV5H4Hl7",
    slotKey: "materials.gallery.1",
    origin:
      "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2026/05/SJDKP-drone-edits-20-Large.webp",
    propertyName: "Hotel San Cristóbal",
  },
  {
    brandId: "recGv268Wda31PlSZ",
    brandName: "Bunkhouse Hotels",
    brandSlug: "bunkhouse-hotels",
    recordId: "recRViyqWoFXdrix4",
    slotKey: "materials.gallery.2",
    origin:
      "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2023/04/Room-Ocean-King-1.webp",
    propertyName: "Hotel San Cristóbal",
  },
  {
    brandId: "recGv268Wda31PlSZ",
    brandName: "Bunkhouse Hotels",
    brandSlug: "bunkhouse-hotels",
    recordId: "recFZiWitn2bmaSSV",
    slotKey: "materials.gallery.3",
    origin:
      "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2023/11/FrontDesk_HotelSanCristobal_NickSimonite_August2018_018-e1701216271377.webp",
    propertyName: "Hotel San Cristóbal",
  },
  {
    brandId: "recGv268Wda31PlSZ",
    brandName: "Bunkhouse Hotels",
    brandSlug: "bunkhouse-hotels",
    recordId: "recbUbiqxoceOfXzZ",
    slotKey: "materials.gallery.4",
    origin:
      "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2023/04/Pool-Bar-2.webp",
    propertyName: "Hotel San Cristóbal",
  },
  {
    brandId: "recGv268Wda31PlSZ",
    brandName: "Bunkhouse Hotels",
    brandSlug: "bunkhouse-hotels",
    recordId: "rec3W1HAEtF0jh7f1",
    slotKey: "materials.gallery.5",
    origin:
      "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2025/11/SJDKP-Pool-Loungers-8.webp",
    propertyName: "Hotel San Cristóbal",
  },
  {
    brandId: "recGv268Wda31PlSZ",
    brandName: "Bunkhouse Hotels",
    brandSlug: "bunkhouse-hotels",
    recordId: "recHUoOCFTuY8bDBG",
    slotKey: "materials.gallery.6",
    origin:
      "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2023/12/Landscape-7.webp",
    propertyName: "Hotel San Cristóbal",
  },
  {
    brandId: "recGv268Wda31PlSZ",
    brandName: "Bunkhouse Hotels",
    brandSlug: "bunkhouse-hotels",
    recordId: "recM35qlgThGEYkvW",
    slotKey: "overview.scenario.1",
    origin:
      "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2024/07/SCB_GeneralProperty-25.webp",
    propertyName: "Hotel San Cristóbal",
  },
];

const rows = [];

for (const job of RETRIES) {
  const result = await ingestPresentationImageFromSource({
    recordId: job.recordId,
    sourceUrl: jpegProxyUrl(job.origin),
    slotKey: job.slotKey,
    dryRun: !apply,
  });
  if (result.ok && apply && result.fetch?.byteHash) {
    upsertVerifiedImageAsset(
      {
        contentHash: result.fetch.byteHash,
        mimeType: "image/jpeg",
        width: result.fetch.width,
        height: result.fetch.height,
        byteSize: result.fetch.bytes,
        originalImageUrl: job.origin,
        propertyName: job.propertyName,
        brandBasicsId: job.brandId,
        brandSlug: job.brandSlug,
        brandName: job.brandName,
        sceneCategory: SCENE_CATEGORY.OTHER,
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
  console.error(`${result.ok ? "OK" : "FAIL"} ${job.slotKey} ${result.lifecycle || result.reason}`);
}

// Suburban: clear PDF then reuse gallery.3 bytes
{
  const donor = await readPresentationImage("recr4Lb4pcVH8MMfF");
  const resFetch = await fetch(donor.attachment.url, {
    headers: { "User-Agent": "DealalityBrandExplorerBot/1.0", Accept: "image/*" },
  });
  const buf = Buffer.from(await resFetch.arrayBuffer());
  const result = await ingestPresentationImageFromBytes({
    recordId: "recMojLeVV4cUcJxY",
    buffer: buf,
    slotKey: "materials.gallery.5",
    sourceUrl: `reuse:recr4Lb4pcVH8MMfF`,
    dryRun: !apply,
    replaceExisting: true,
  });
  rows.push({
    brandSlug: "suburban-studios",
    slotKey: "materials.gallery.5",
    ok: result.ok,
    lifecycle: result.lifecycle,
    reason: result.reason,
  });
  console.error(`${result.ok ? "OK" : "FAIL"} suburban gallery.5 ${result.lifecycle || result.reason}`);
}

const stamp = new Date().toISOString().replace(/[:.]/g, "-");
const path = join(OUT, `21-remediation-retry-${apply ? "APPLY" : "DRY"}-${stamp}.json`);
writeFileSync(
  path,
  JSON.stringify(
    {
      apply,
      rows,
      okCount: rows.filter((r) => r.ok).length,
      failCount: rows.filter((r) => !r.ok).length,
      library: summarizeLibrary(),
    },
    null,
    2
  ) + "\n"
);
console.log(JSON.stringify({ report: path, okCount: rows.filter((r) => r.ok).length, failCount: rows.filter((r) => !r.ok).length, library: summarizeLibrary() }, null, 2));
process.exit(rows.every((r) => r.ok) ? 0 : 1);
