#!/usr/bin/env node
/**
 * Image Integrity V3 — authorized recoverable remediation.
 *
 * Priority: Aloft (9 missing), Bunkhouse (webp/expired + 2 missing),
 * Suburban gallery.5 (PDF mis-slot), plus optional sibling false-positive holds.
 *
 * Pipeline: Discover → Download/Load → Decode → Exact-brand check → Content-API → Read-back
 * Seeds Shared Verified Hotel Image Library on successful read-back.
 *
 *   node scripts/be-factory-image-integrity-remediate-v3.mjs
 *   node scripts/be-factory-image-integrity-remediate-v3.mjs --apply
 *
 * $0 paid research. Preserves valid CURRENT JPEG/PNG attachments.
 */
import "../load-env.js";
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import {
  BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v3.js";
import { evaluateExactBrandImageIdentity } from "../lib/brand-explorer/factory-2/be-factory-permanent-exact-brand-image-identity-v1.js";
import {
  ingestPresentationImageFromBytes,
  ingestPresentationImageFromSource,
  probeAttachmentUrl,
  readPresentationImage,
} from "../lib/brand-explorer/factory-2/be-factory-image-attachment-ingest-v2.js";
import {
  classifyLiveAttachment,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v2.js";
import {
  upsertVerifiedImageAsset,
  SCENE_CATEGORY,
  summarizeLibrary,
} from "../lib/brand-explorer/factory-2/be-shared-verified-hotel-image-library-v1.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/brand-explorer/portfolio-image-integrity-v3");
mkdirSync(OUT, { recursive: true });
const apply = process.argv.includes("--apply");

function loadJson(rel) {
  const p = join(ROOT, rel);
  if (!existsSync(p)) return [];
  return JSON.parse(readFileSync(p, "utf8"));
}

function pickPool(pool, role, used) {
  const ranked = [
    ...pool.filter((x) => x.role === role),
    ...pool.filter((x) => !role || x.role !== role),
  ];
  for (const c of ranked) {
    const url = c.imageUrl || c.url;
    if (url && !used.has(url) && !/\.webp(\?|$)/i.test(url)) {
      used.add(url);
      return c;
    }
  }
  for (const c of ranked) {
    const url = c.imageUrl || c.url;
    if (url && !used.has(url)) {
      used.add(url);
      return c;
    }
  }
  return null;
}

/** Official webp → JPEG via wsrv (bytes validated before Airtable content-API). */
function jpegProxyUrl(sourceUrl) {
  const url = String(sourceUrl || "").trim();
  if (!url) return null;
  if (/\.jpe?g(\?|$)/i.test(url) || /\.png(\?|$)/i.test(url)) return url;
  return `https://wsrv.nl/?url=${encodeURIComponent(url)}&output=jpg&q=85`;
}

function roleToScene(role) {
  const r = String(role || "").toLowerCase();
  if (/exterior|arrival/.test(r)) return SCENE_CATEGORY.EXTERIOR;
  if (/lobby|public/.test(r)) return SCENE_CATEGORY.LOBBY;
  if (/guest|room|suite/.test(r)) return SCENE_CATEGORY.GUEST_ROOM;
  if (/food|beverage|restaurant|bar|dining/.test(r)) return SCENE_CATEGORY.RESTAURANT_BAR;
  if (/pool/.test(r)) return SCENE_CATEGORY.POOL;
  if (/spa|wellness/.test(r)) return SCENE_CATEGORY.SPA;
  if (/ground|setting|resort/.test(r)) return SCENE_CATEGORY.RESORT_GROUNDS;
  return SCENE_CATEGORY.OTHER;
}

function needsRemediation(before, probe) {
  if (!before.attachment) return true;
  if (!probe?.ok) return true;
  const type = String(before.attachment.type || "").toLowerCase();
  if (type.includes("webp") || type.includes("pdf") || type.includes("octet")) return true;
  const fn = String(before.attachment.filename || "").toLowerCase();
  if (fn.endsWith(".webp") || fn.endsWith(".pdf")) return true;
  return false;
}

async function remediateSlot(job) {
  const before = await readPresentationImage(job.recordId);
  const beforeProbe = before.attachment?.url
    ? await probeAttachmentUrl(before.attachment.url)
    : null;
  const beforeClass = classifyLiveAttachment({
    attachment: before.attachment,
    probe: beforeProbe,
  });

  if (!needsRemediation(before, beforeProbe) && !job.force) {
    return {
      ...job,
      action: "PRESERVE_VALID_CURRENT",
      ok: true,
      beforeClass,
      afterClass: beforeClass,
      attachment: before.attachment,
    };
  }

  const exact = evaluateExactBrandImageIdentity({
    targetBrandId: job.brandId,
    targetSlug: job.brandSlug,
    imageUrl: job.sourceUrl || job.localPath || "",
    imageMeta: {
      propertyName: job.propertyName || job.sourceMeta?.propertyName || "",
      title: job.slotKey,
      filename: job.localPath || job.sourceUrl || "",
      label: job.propertyName || "",
    },
    propertyBrandSlug: job.propertyBrandSlug || job.brandSlug,
    propertyVerified: !!job.propertyVerified,
  });
  if (exact.ok === false && exact.failureCode === "IMAGE_SIBLING_BRAND_REJECTED") {
    return {
      ...job,
      action: "BLOCKED_SIBLING_BRAND",
      ok: false,
      exact,
      beforeClass,
    };
  }

  let result;
  if (job.localPath && existsSync(job.localPath)) {
    const buf = readFileSync(job.localPath);
    result = await ingestPresentationImageFromBytes({
      recordId: job.recordId,
      buffer: buf,
      slotKey: job.slotKey,
      sourceUrl: job.sourceUrl || `file://${job.localPath.replace(/\\/g, "/")}`,
      dryRun: !apply,
    });
  } else if (job.reuseAttachmentUrl) {
    // Same-brand reuse of an already-validated Airtable attachment (bytes → content-API).
    // fetchImageBytes rejects ephemeral CDN as a *source of truth*; reuse is intentional.
    const probe = await probeAttachmentUrl(job.reuseAttachmentUrl);
    if (!probe?.ok) {
      return {
        ...job,
        action: "BLOCKED_DONOR_FETCH_FAILED",
        ok: false,
        beforeClass,
        reason: probe?.reason || "donor_probe_failed",
      };
    }
    const res = await fetch(job.reuseAttachmentUrl, {
      headers: {
        "User-Agent": "DealalityBrandExplorerBot/1.0 (+https://dealality.com; image reuse v3)",
        Accept: "image/*",
      },
    });
    const buf = Buffer.from(await res.arrayBuffer());
    result = await ingestPresentationImageFromBytes({
      recordId: job.recordId,
      buffer: buf,
      slotKey: job.slotKey,
      sourceUrl: `reuse:${job.sourceMeta?.donorRecordId || "attachment"}`,
      dryRun: !apply,
    });
  } else if (job.sourceUrl) {
    result = await ingestPresentationImageFromSource({
      recordId: job.recordId,
      sourceUrl: job.sourceUrl,
      slotKey: job.slotKey,
      dryRun: !apply,
    });
  } else {
    return {
      ...job,
      action: "BLOCKED_NO_SOURCE",
      ok: false,
      beforeClass,
      reason: "no_verified_source",
    };
  }

  if (result.ok && apply && result.fetch?.byteHash) {
    upsertVerifiedImageAsset(
      {
        contentHash: result.fetch.byteHash,
        mimeType: result.fetch.format === "png" ? "image/png" : "image/jpeg",
        width: result.fetch.width,
        height: result.fetch.height,
        byteSize: result.fetch.bytes,
        filename: result.attachment?.filename || null,
        durableStorageRef: result.attachment?.id || null,
        originalImageUrl: job.sourceUrl || null,
        sourcePageUrl: job.sourcePageUrl || null,
        propertyName: job.propertyName || null,
        brandBasicsId: job.brandId,
        brandSlug: job.brandSlug,
        brandName: job.brandName,
        sceneCategory: roleToScene(job.role),
        sourceType: job.localPath ? "local_verified_thumb" : "official_brand_cdn",
        retrievalMethod: job.localPath ? "local_file" : "http_get",
        confidence: "high",
        verificationNotes: `V3 remediation ${job.slotKey}`,
        verifiedAt: new Date().toISOString(),
        permissions: {
          internalResearch: true,
          brandExplorerDisplay: true,
          hotelExplorerDisplay: false,
          customerFacing: false,
          externalDistribution: false,
        },
        licenseBasis: "official_brand_or_property_media_internal_research",
        presentationRecordIds: [job.recordId],
        exactBrandDecision: exact,
        technicalValid: true,
        certificationStatus: "STAGED",
      },
      { dryRun: false }
    );
  }

  return {
    ...job,
    action: apply ? "INGEST_CONTENT_API" : "DRY_RUN_VALIDATE",
    beforeClass,
    exact,
    ...result,
  };
}

const aloftPool = loadJson("fixtures/wave14-aloft-hotels-gallery-pool.json");

const JOBS = [];

// --- Aloft Hotels: 9 missing ---
{
  const used = new Set();
  const brand = {
    brandId: "recJ1GZQpttX7qHgw",
    brandName: "Aloft Hotels",
    brandSlug: "aloft-hotels",
  };
  const slots = [
    { recordId: "recavzUzn2TUyiiGN", slotKey: "materials.gallery.1", role: "exterior_arrival" },
    { recordId: "rec4dp36X6T1vbuET", slotKey: "materials.gallery.2", role: "guest_room_suite" },
    { recordId: "reczIwyfVWvXTg0ek", slotKey: "materials.gallery.3", role: "guest_room_suite" },
    { recordId: "recY3NoUzwrdOBiAb", slotKey: "materials.gallery.4", role: "public_space_lobby" },
    { recordId: "rec6xHUeZqK9mzgz9", slotKey: "materials.gallery.5", role: "wellness_pool_spa" },
    { recordId: "recvh5tzzIn0fP5g7", slotKey: "materials.gallery.6", role: "property_setting" },
    { recordId: "recVqWTI5b4B8PmD2", slotKey: "overview.scenario.1", role: "exterior_arrival" },
    { recordId: "rec6MNF9zPGOMhWwu", slotKey: "overview.scenario.2", role: "guest_room_suite" },
    { recordId: "reckQJHez9BeUO4n4", slotKey: "overview.scenario.3", role: "public_space_lobby" },
  ];
  for (const s of slots) {
    const pick = pickPool(aloftPool, s.role, used);
    JOBS.push({
      ...brand,
      ...s,
      sourceUrl: pick?.imageUrl || null,
      sourcePageUrl: pick?.sourcePageUrl || null,
      propertyName: pick?.propertyName || "Aloft Hotels (brand photography)",
      propertyBrandSlug: "aloft-hotels",
      propertyVerified: false,
      sourceMeta: pick,
    });
  }
}

// --- Bunkhouse: official property webp → JPEG proxy → content-API ---
{
  const brand = {
    brandId: "recGv268Wda31PlSZ",
    brandName: "Bunkhouse Hotels",
    brandSlug: "bunkhouse-hotels",
  };
  const map = [
    {
      recordId: "recmQhhB4eV5H4Hl7",
      slotKey: "materials.gallery.1",
      role: "exterior_arrival",
      propertyName: "Hotel San Cristóbal",
      origin:
        "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2026/05/SJDKP-drone-edits-20-Large.webp",
      sourcePageUrl: "https://www.bunkhousehotels.com/hotel-san-cristobal",
    },
    {
      recordId: "recRViyqWoFXdrix4",
      slotKey: "materials.gallery.2",
      role: "guest_room_suite",
      propertyName: "Hotel San Cristóbal",
      origin:
        "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2023/04/Room-Ocean-King-1.webp",
      sourcePageUrl: "https://www.bunkhousehotels.com/hotel-san-cristobal",
    },
    {
      recordId: "recFZiWitn2bmaSSV",
      slotKey: "materials.gallery.3",
      role: "public_space_lobby",
      propertyName: "Hotel San Cristóbal",
      origin:
        "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2023/11/FrontDesk_HotelSanCristobal_NickSimonite_August2018_018-e1701216271377.webp",
      sourcePageUrl: "https://www.bunkhousehotels.com/hotel-san-cristobal",
    },
    {
      recordId: "recbUbiqxoceOfXzZ",
      slotKey: "materials.gallery.4",
      role: "food_beverage",
      propertyName: "Hotel San Cristóbal",
      origin:
        "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2023/04/Pool-Bar-2.webp",
      sourcePageUrl: "https://www.bunkhousehotels.com/hotel-san-cristobal",
    },
    {
      recordId: "rec3W1HAEtF0jh7f1",
      slotKey: "materials.gallery.5",
      role: "wellness_pool_spa",
      propertyName: "Hotel San Cristóbal",
      origin:
        "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2025/11/SJDKP-Pool-Loungers-8.webp",
      sourcePageUrl: "https://www.bunkhousehotels.com/hotel-san-cristobal",
    },
    {
      recordId: "recHUoOCFTuY8bDBG",
      slotKey: "materials.gallery.6",
      role: "property_setting",
      propertyName: "Hotel San Cristóbal",
      origin:
        "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2023/12/Landscape-7.webp",
      sourcePageUrl: "https://www.bunkhousehotels.com/hotel-san-cristobal",
    },
    {
      recordId: "recM35qlgThGEYkvW",
      slotKey: "overview.scenario.1",
      role: "exterior_arrival",
      propertyName: "Hotel San Cristóbal",
      origin:
        "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2024/07/SCB_GeneralProperty-25.webp",
      sourcePageUrl: "https://www.bunkhousehotels.com/hotel-san-cristobal",
    },
    {
      recordId: "recNqOJsrJofvD7H5",
      slotKey: "overview.scenario.2",
      role: "guest_room_suite",
      propertyName: "Hotel San Cristóbal",
      origin:
        "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2026/05/SJDKP-poolside-king1113-Edit-Large.webp",
      sourcePageUrl: "https://www.bunkhousehotels.com/hotel-san-cristobal",
    },
    {
      recordId: "recKS9WM9yKcB8dBW",
      slotKey: "overview.scenario.3",
      role: "property_setting",
      propertyName: "Hotel San Cristóbal",
      origin:
        "https://login.bunkhousehotels.com/hotel-san-cristobal/wp-content/uploads/sites/6/2024/01/20231127_HotelSanCristobal_Bunkhouse0311-1-1.webp",
      sourcePageUrl: "https://www.bunkhousehotels.com/hotel-san-cristobal",
    },
  ];
  for (const s of map) {
    JOBS.push({
      ...brand,
      recordId: s.recordId,
      slotKey: s.slotKey,
      role: s.role,
      sourceUrl: jpegProxyUrl(s.origin),
      sourcePageUrl: s.sourcePageUrl,
      propertyName: s.propertyName,
      propertyBrandSlug: "bunkhouse-hotels",
      propertyVerified: true,
      force: true,
      sourceMeta: { originWebp: s.origin, retrieval: "wsrv_webp_to_jpeg" },
    });
  }
}

// --- Suburban Studios: PDF mis-attached in gallery.5 — reuse valid same-brand gallery PNG ---
{
  const donor = await readPresentationImage("recr4Lb4pcVH8MMfF"); // materials.gallery.3
  JOBS.push({
    brandId: "reclcjg5Foa9Vs5TC",
    brandName: "Suburban Studios",
    brandSlug: "suburban-studios",
    recordId: "recMojLeVV4cUcJxY",
    slotKey: "materials.gallery.5",
    role: "exterior_arrival",
    reuseAttachmentUrl: donor.attachment?.url || null,
    sourcePageUrl: "https://www.choicehotels.com/suburban",
    propertyName: "Suburban Studios (same-brand gallery reuse)",
    propertyBrandSlug: "suburban-studios",
    force: true,
    sourceMeta: {
      donorRecordId: "recr4Lb4pcVH8MMfF",
      note: "Choice CDN blocked (403); reuse validated CURRENT gallery.3 bytes",
    },
  });
}

const results = {
  contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3,
  generatedAt: new Date().toISOString(),
  apply,
  paidResearchSpendUsd: 0,
  rows: [],
};

for (const job of JOBS) {
  const row = await remediateSlot(job);
  results.rows.push({
    brand: job.brandName,
    brandId: job.brandId,
    slotKey: job.slotKey,
    recordId: job.recordId,
    action: row.action,
    ok: row.ok,
    lifecycle: row.lifecycle || null,
    classification: row.classification || row.afterClass || row.beforeClass || null,
    reason: row.reason || null,
    byteHash: row.fetch?.byteHash || null,
    sourceUrl: job.sourceUrl || null,
    localPath: job.localPath || null,
  });
  console.error(
    `${row.ok ? "OK" : "FAIL"} ${job.brandSlug} ${job.slotKey} ${row.action} ${row.reason || row.lifecycle || ""}`
  );
}

const stamp = results.generatedAt.replace(/[:.]/g, "-");
const path = join(OUT, `20-remediation-${apply ? "APPLY" : "DRY"}-${stamp}.json`);
results.okCount = results.rows.filter((r) => r.ok).length;
results.failCount = results.rows.filter((r) => !r.ok).length;
results.library = summarizeLibrary();
writeFileSync(path, JSON.stringify(results, null, 2) + "\n");
console.log(
  JSON.stringify(
    {
      ok: results.failCount === 0,
      apply,
      report: path,
      rows: results.rows.length,
      okCount: results.okCount,
      failCount: results.failCount,
      library: results.library,
    },
    null,
    2
  )
);
process.exit(results.failCount === 0 ? 0 : 1);
