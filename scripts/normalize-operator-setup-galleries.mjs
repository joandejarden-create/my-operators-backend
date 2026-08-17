#!/usr/bin/env node
/**
 * Normalize Operator Setup Image Galleries from registry.
 *
 *   node scripts/normalize-operator-setup-galleries.mjs
 *   node scripts/normalize-operator-setup-galleries.mjs --apply
 *   node scripts/normalize-operator-setup-galleries.mjs --apply --only ghl-hoteles,royalton-hotels-resorts
 *   node scripts/normalize-operator-setup-galleries.mjs --apply --clear-staging-only
 *   node scripts/normalize-operator-setup-galleries.mjs --apply --replace-images --only …
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  OPERATOR_GALLERY_BY_SLUG,
  galleryBodyFromHotel,
  listReadyGallerySpecs,
} from "../lib/partner-intelligence/operator-setup-gallery-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const CLEAR_STAGING_ONLY = process.argv.includes("--clear-staging-only");
const REPLACE_IMAGES = process.argv.includes("--replace-images");
const onlyArg = process.argv.find((a, i) => process.argv[i - 1] === "--only");
const ONLY = onlyArg
  ? onlyArg.split(",").map((s) => s.trim()).filter(Boolean)
  : null;

const MATS_TABLE = "Operator Setup - Explorer Materials";
const GENERIC_TITLES = new Set([
  "Resort Exterior",
  "Hotel Lobby & Lounge",
  "Guest Room",
  "Resort Pool & Terrace",
  "Restaurant & Dining Room",
  "Front Desk & Reception",
]);

function pickSpecs() {
  const all = Object.entries(OPERATOR_GALLERY_BY_SLUG).map(([slug, s]) => ({ slug, ...s }));
  let specs = all;
  if (ONLY) specs = specs.filter((s) => ONLY.includes(s.slug));
  if (CLEAR_STAGING_ONLY) specs = specs.filter((s) => s.clearGenericGallery);
  return specs;
}

async function fetchImageProbe(url, attempt = 1) {
  const headers = {
    "user-agent": "DealalityGalleryNormalize/1.0 (dealality.com; gallery ingest)",
    accept: "image/*,*/*",
  };
  let res = await fetch(url, { headers: { ...headers, Range: "bytes=0-1023" }, redirect: "follow" });
  if (res.status === 429 && attempt < 8) {
    await new Promise((r) => setTimeout(r, 5000 * attempt));
    return fetchImageProbe(url, attempt + 1);
  }
  if (!res.ok && res.status !== 206) {
    res = await fetch(url, { headers, redirect: "follow" });
    if (res.status === 429 && attempt < 8) {
      await new Promise((r) => setTimeout(r, 5000 * attempt));
      return fetchImageProbe(url, attempt + 1);
    }
    if (!res.ok) throw new Error(`Image fetch failed ${res.status}: ${url}`);
    const buf = Buffer.from(await res.arrayBuffer());
    return buf.length;
  }
  const cr = res.headers.get("content-range") || "";
  const m = cr.match(/\/(\d+)/);
  return m ? Number(m[1]) : Number(res.headers.get("content-length") || 0);
}

async function assertUniqueImages(hotels) {
  const urls = hotels.map((h) => h.imageUrl);
  if (new Set(urls).size !== urls.length) throw new Error("Duplicate image URLs in gallery");
  const sizes = [];
  for (const h of hotels) {
    await new Promise((r) => setTimeout(r, 400));
    sizes.push(await fetchImageProbe(h.imageUrl));
  }
  // soft check: identical sizes may indicate placeholder CDN
  const sizeSet = new Set(sizes.filter(Boolean));
  if (sizeSet.size === 1 && hotels.length > 2) {
    console.warn("WARN: all images share same byte size — possible placeholders", sizes[0]);
  }
}

async function upsertGallery(base, spec) {
  const rows = await base(MATS_TABLE).select({ pageSize: 100 }).all();
  const linked = rows.filter((r) => (r.fields.Operator || []).includes(spec.masterId));
  const gallery = linked.filter((r) =>
    String(r.fields["Slot Key"] || "").startsWith("materials.gallery.")
  );

  if (spec.clearGenericGallery) {
    const toClear = gallery.filter((r) => GENERIC_TITLES.has(String(r.fields.Title || "").trim()));
    const actions = [];
    for (const r of toClear) {
      actions.push({ id: r.id, title: r.fields.Title, action: "deactivate" });
      if (APPLY) {
        await base(MATS_TABLE).update(r.id, { Active: false });
      }
    }
    return { type: "clearGeneric", actions };
  }

  if (spec.deactivateGallery) {
    const actions = [];
    for (const r of gallery) {
      actions.push({
        id: r.id,
        slotKey: r.fields["Slot Key"],
        title: r.fields.Title,
        action: "deactivate",
      });
      if (APPLY) await base(MATS_TABLE).update(r.id, { Active: false });
    }
    return {
      type: "deactivateGallery",
      reason: spec.skipReason || "Deactivated until verified hotel/image pairs exist",
      actions,
    };
  }

  if (spec.skip || (spec.hotels?.length || 0) < 6) {
    return {
      type: "skipped",
      reason: spec.skipReason || `Need 6 hotels, have ${spec.hotels?.length || 0}`,
    };
  }

  const missingCountry = spec.hotels.filter((h) => !String(h.country || "").trim());
  if (missingCountry.length) {
    throw new Error(
      `Missing country on ${missingCountry.length} hotel(s): ${missingCountry
        .map((h) => h.title)
        .join("; ")}`
    );
  }

  await assertUniqueImages(spec.hotels);

  const bySlot = new Map(
    gallery.map((r) => [String(r.fields["Slot Key"]), r])
  );
  const actions = [];
  for (let i = 0; i < 6; i++) {
    const hotel = spec.hotels[i];
    const slotKey = `materials.gallery.${i + 1}`;
    const existing = bySlot.get(slotKey);
    const body = galleryBodyFromHotel(hotel);
    const baseFields = {
      Title: hotel.title,
      Body: body,
      "Sort Order": i + 1,
      Active: true,
      Operator: [spec.masterId],
      "Company Name": spec.companyName,
      "Slot Key": slotKey,
    };
    if (existing) {
      // Default: do not re-attach Image (avoids Airtable re-download). Remediation: --replace-images.
      const fields = REPLACE_IMAGES
        ? { ...baseFields, Image: [{ url: hotel.imageUrl }] }
        : baseFields;
      actions.push({
        slotKey,
        action: REPLACE_IMAGES ? "update+image" : "update",
        recordId: existing.id,
        title: hotel.title,
        body,
        imageUrl: REPLACE_IMAGES ? hotel.imageUrl : undefined,
      });
      if (APPLY) await base(MATS_TABLE).update(existing.id, fields);
    } else {
      const fields = { ...baseFields, Image: [{ url: hotel.imageUrl }] };
      actions.push({ slotKey, action: "create", title: hotel.title, body, imageUrl: hotel.imageUrl });
      if (APPLY) await base(MATS_TABLE).create([{ fields }]);
    }
  }
  return { type: "upsert", actions };
}

async function main() {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required");

  const specs = pickSpecs();
  const base = new Airtable({ apiKey: key }).base(baseId);
  const report = {
    mode: APPLY ? "apply" : "dry-run",
    readyCount: listReadyGallerySpecs().length,
    results: [],
  };

  for (const spec of specs) {
    console.log("→", spec.slug, spec.companyName);
    try {
      const result = await upsertGallery(base, spec);
      report.results.push({ slug: spec.slug, companyName: spec.companyName, ...result });
      console.log("  ", result.type, result.reason || `${result.actions?.length || 0} actions`);
    } catch (e) {
      console.error("  ERROR", e.message);
      report.results.push({
        slug: spec.slug,
        companyName: spec.companyName,
        type: "error",
        error: e.message,
      });
    }
  }

  const outPath = path.join(ROOT, "reports", "normalize-operator-setup-galleries.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("report", outPath);
  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        upserted: report.results.filter((r) => r.type === "upsert").length,
        cleared: report.results.filter((r) => r.type === "clearGeneric").length,
        skipped: report.results.filter((r) => r.type === "skipped").length,
        errors: report.results.filter((r) => r.type === "error").length,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
