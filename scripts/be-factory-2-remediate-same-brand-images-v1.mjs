#!/usr/bin/env node
/**
 * Strip or replace cross-brand property images on Factory 2.0 frozen payloads
 * and depth candidates. Permanent rule: never show a sibling flag's hotel
 * photography on another brand (e.g. Hilton Hotels on Waldorf Astoria).
 */
import "../load-env.js";
import {
  writeFileSync,
  mkdirSync,
  copyFileSync,
  existsSync,
  readdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
} from "fs";
import { join } from "path";
import { setTimeout as sleep } from "timers/promises";
import {
  pickScenarioHotelImages,
  loadGalleryCardsFromFixture,
} from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";
import {
  isSameBrandImage,
  assessSameBrandImageAffinity,
} from "../lib/brand-explorer/be-permanent-same-brand-image-affinity-v1.js";
import { isPeopleOrLifestyleMarketingImage } from "../lib/brand-explorer/be-permanent-six-image-gallery-standard-v1.js";

const ROOT = process.cwd();
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");
const REPORT_DIR = join(ROOT, "reports/brand-explorer/factory-2-0");

const PROPERTY_SLOT_RE =
  /^(?:overview\.scenario\.\d+|materials\.gallery\.\d+|footprint\.openings|materials\.caseStudy|valueOwners\.scenario\.\d+)$/i;

function nz(s) {
  return s == null ? "" : String(s).trim();
}

async function writeJsonAtomic(abs, obj, attempts = 8) {
  const tmp = `${abs}.tmp-${process.pid}`;
  const body = JSON.stringify(obj, null, 2) + "\n";
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      writeFileSync(tmp, body);
      try {
        renameSync(tmp, abs);
      } catch {
        writeFileSync(abs, body);
        try {
          unlinkSync(tmp);
        } catch {
          /* ignore */
        }
      }
      return;
    } catch (err) {
      lastErr = err;
      await sleep(150 + i * 200);
    }
  }
  throw lastErr;
}

function syncHe(relFromPublic) {
  const src = join(ROOT, "public", relFromPublic);
  const dest = join(HE, "public", relFromPublic);
  if (!existsSync(src)) return;
  try {
    mkdirSync(join(dest, ".."), { recursive: true });
    copyFileSync(src, dest);
  } catch {
    /* HE may be locked */
  }
}

function collectPoolImages(slug, brand) {
  const scenario = pickScenarioHotelImages(ROOT, slug, brand, 6);
  const gallery = loadGalleryCardsFromFixture(ROOT, slug, brand);
  const urls = [];
  const seen = new Set();
  for (const row of [...scenario, ...gallery]) {
    const u = nz(row.imageUrl);
    if (!u || seen.has(u)) continue;
    if (!isSameBrandImage(slug, u, row)) continue;
    if (isPeopleOrLifestyleMarketingImage(u)) continue;
    seen.add(u);
    urls.push({ imageUrl: u, source: row.source || u, title: row.title || brand });
  }
  return urls;
}

function patchBlocks(blocks, slug, brand, pool) {
  let changed = false;
  let cleared = 0;
  let replaced = 0;
  let poolIdx = 0;
  const nextPool = () => {
    if (!pool.length) return null;
    const row = pool[poolIdx % pool.length];
    poolIdx += 1;
    return row;
  };

  for (const b of blocks || []) {
    const key = nz(b.slotKey);
    if (!PROPERTY_SLOT_RE.test(key)) continue;
    const url = nz(b.imageUrl);
    if (!url) continue;
    const meta = {
      title: b.title,
      body: b.body,
      caption: b.title,
      propertyName: b.title,
      source: b.source,
    };
    if (isSameBrandImage(slug, url, meta) && !isPeopleOrLifestyleMarketingImage(url)) {
      continue;
    }
    const assessment = assessSameBrandImageAffinity(slug, url, meta);
    const replacement = nextPool();
    if (replacement) {
      b.imageUrl = replacement.imageUrl;
      b.source = replacement.source || b.source;
      // Keep scenario titles; refresh gallery titles/bodies that still name the sibling brand
      if (/^materials\.gallery\./i.test(key) && replacement.title) {
        b.title = replacement.title;
        b.body = replacement.title;
      } else if (/hilton panama|hilton cancun|hilton bogota|hilton hotels/i.test(nz(b.body) + "\n" + nz(b.title))) {
        b.body = nz(b.body)
          .replace(/Hilton Panama[^—\n]*/gi, brand)
          .replace(/Hilton Cancun[^—\n]*/gi, brand)
          .replace(/Hilton Bogotá?[^—\n]*/gi, brand)
          .replace(/Hilton Hotels(?:\s*&\s*Resorts)?/gi, brand);
      }
      replaced += 1;
      changed = true;
    } else {
      b.imageUrl = null;
      cleared += 1;
      changed = true;
    }
    b._sameBrandAffinity = assessment.code || "remediated";
  }
  return { changed, cleared, replaced };
}

async function main() {
  mkdirSync(REPORT_DIR, { recursive: true });
  const files = readdirSync(PREVIEW).filter((x) =>
    /factory2-(?:frozen-approval-payload|depth-candidate)/i.test(x)
  );
  const rows = [];
  let patchedFiles = 0;

  for (const f of files) {
    const abs = join(PREVIEW, f);
    const j = JSON.parse(readFileSync(abs, "utf8"));
    const slug = nz(j.slug) || f.replace(/-factory2-.*\.json$/i, "");
    const brand = nz(j.brandName) || slug;
    const pool = collectPoolImages(slug, brand);
    const blocks = j.brandExplorer?.blocks || j.blocks || [];
    if (!Array.isArray(blocks) || !blocks.length) {
      rows.push({ file: f, slug, status: "no_blocks" });
      continue;
    }
    const beforeBad = blocks.filter((b) => {
      const url = nz(b.imageUrl);
      if (!url || !PROPERTY_SLOT_RE.test(nz(b.slotKey))) return false;
      return !isSameBrandImage(slug, url, { title: b.title, body: b.body, source: b.source });
    }).length;

    const { changed, cleared, replaced } = patchBlocks(blocks, slug, brand, pool);
    if (changed) {
      if (j.brandExplorer?.blocks) j.brandExplorer.blocks = blocks;
      else j.blocks = blocks;
      await writeJsonAtomic(abs, j);
      syncHe(`brand-explorer-phase2e-preview/${f}`);
      patchedFiles += 1;
    }
    rows.push({
      file: f,
      slug,
      brand,
      beforeBad,
      cleared,
      replaced,
      poolSize: pool.length,
      status: changed ? "patched" : beforeBad ? "still_bad_no_pool" : "ok",
    });
  }

  const report = {
    contract: "BE_FACTORY_2_SAME_BRAND_IMAGE_REMEDIATION_V1",
    generatedAt: new Date().toISOString(),
    patchedFiles,
    totalFiles: files.length,
    remainingBad: rows.filter((r) => r.status === "still_bad_no_pool" || (r.beforeBad > 0 && r.cleared + r.replaced < r.beforeBad)).length,
    rows,
  };
  writeFileSync(join(REPORT_DIR, "98-same-brand-images-remediation.json"), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        patchedFiles,
        totalFiles: files.length,
        waldorf: rows.filter((r) => /waldorf/i.test(r.slug)),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
