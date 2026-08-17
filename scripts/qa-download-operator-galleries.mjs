#!/usr/bin/env node
/**
 * Download gallery images for visual QA — filename encodes slug + title slug.
 */
import fs from "fs";
import path from "path";
import { OPERATOR_GALLERY_BY_SLUG } from "../lib/partner-intelligence/operator-setup-gallery-registry.js";

const OUT = "data/operator-gallery-qa";
fs.mkdirSync(OUT, { recursive: true });

const slugs = [
  "arriva-hospitality-group",
  "brittain-resorts-hotels",
  "tafer-hotels-resorts",
  "grupo-hotelero-santa-fe",
  "atlantica-hotels-international",
  "marriott-international-managed",
  "hilton-managed",
  "minor-hotels-managed",
  "playa-hotels-resorts",
  "driftwood-hospitality-management",
];

function safe(s) {
  return String(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 60);
}

const manifest = [];
for (const slug of slugs) {
  const spec = OPERATOR_GALLERY_BY_SLUG[slug];
  if (!spec?.hotels?.length) continue;
  const dir = path.join(OUT, slug);
  fs.mkdirSync(dir, { recursive: true });
  for (let i = 0; i < spec.hotels.length; i++) {
    const h = spec.hotels[i];
    const ext = (h.imageUrl.match(/\.(jpe?g|png|webp)(?:\?|$)/i) || [, "jpg"])[1].toLowerCase();
    const file = path.join(dir, `${i + 1}-${safe(h.title)}.${ext === "jpeg" ? "jpg" : ext}`);
    try {
      await new Promise((r) => setTimeout(r, 300));
      const res = await fetch(h.imageUrl, {
        headers: { "user-agent": "DealalityGalleryQA/1.0" },
        redirect: "follow",
      });
      if (!res.ok) {
        console.log("FAIL", res.status, slug, h.title);
        manifest.push({ slug, i: i + 1, title: h.title, status: res.status, error: true });
        continue;
      }
      const buf = Buffer.from(await res.arrayBuffer());
      fs.writeFileSync(file, buf);
      console.log("OK", buf.length, file);
      manifest.push({
        slug,
        i: i + 1,
        title: h.title,
        country: h.country,
        file,
        bytes: buf.length,
        imageUrl: h.imageUrl,
      });
    } catch (e) {
      console.log("ERR", slug, h.title, e.message);
      manifest.push({ slug, i: i + 1, title: h.title, error: e.message });
    }
  }
}
fs.writeFileSync(path.join(OUT, "manifest.json"), JSON.stringify(manifest, null, 2));
console.log("wrote", manifest.length);
