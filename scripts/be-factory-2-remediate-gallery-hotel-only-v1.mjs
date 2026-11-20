#!/usr/bin/env node
/**
 * In-place remediate Factory 2.0 frozen galleries to Curio/Ascend hotel-photography bar:
 * exterior / interior / renderings only — never people/lifestyle marketing.
 * Accurate scene captions. Patches frozen payloads + depth candidates; no queue.json writes.
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
import { loadGalleryCardsFromFixture } from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";
import {
  isPeopleOrLifestyleMarketingImage,
  isLikelyHotelPropertyPhotograph,
} from "../lib/brand-explorer/be-permanent-six-image-gallery-standard-v1.js";

const ROOT = process.cwd();
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");
const GALLERY_SLOT_RE = /^materials\.gallery\.(\d+)$/;

function patchBlocks(blocks, cards) {
  if (!cards.length) return { changed: false, galleryCount: 0 };
  const next = blocks.filter((b) => !GALLERY_SLOT_RE.test(b.slotKey || ""));
  const maxSort = Math.max(0, ...next.map((b) => Number(b.sort) || 0));
  cards.slice(0, 6).forEach((c, i) => {
    next.push({
      slotKey: `materials.gallery.${i + 1}`,
      title: c.title,
      body: c.body,
      sort: maxSort + i + 1,
      active: true,
      imageUrl: c.imageUrl,
      externalDisplayStatus: "Internal Preview",
      source: c.source || "factory-2-0-gallery-hotel-only-remediation",
    });
  });
  return { changed: true, galleryCount: Math.min(6, cards.length), blocks: next };
}

function auditGallery(blocks) {
  const gallery = (blocks || []).filter((b) => GALLERY_SLOT_RE.test(b.slotKey || ""));
  const withImage = gallery.filter((b) => /^https?:\/\//i.test(b.imageUrl || ""));
  const people = withImage.filter((b) =>
    isPeopleOrLifestyleMarketingImage(b.imageUrl, { caption: b.title, title: b.title })
  );
  const nonHotel = withImage.filter(
    (b) => !isLikelyHotelPropertyPhotograph(b.imageUrl, { role: b.title, caption: b.title })
  );
  const mislabeledExterior = withImage.filter(
    (b) =>
      /\bExterior\b/i.test(b.title || "") &&
      !/exterior|arrival|facade|aerial|cochere/i.test(`${b.imageUrl} ${b.title}`)
  );
  return {
    gallerySlots: gallery.length,
    withImage: withImage.length,
    peopleCount: people.length,
    nonHotelCount: nonHotel.length,
    mislabeledExterior: mislabeledExterior.length,
    pass: people.length === 0 && mislabeledExterior.length === 0,
  };
}

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
        // Windows: rename over existing can fail — overwrite then unlink tmp
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

async function mainAsync() {
  const files = readdirSync(PREVIEW).filter((x) =>
    x.endsWith("-factory2-frozen-approval-payload-v1.json")
  );
  const rows = [];
  let patched = 0;
  const writeErrors = [];

  for (const f of files) {
    const abs = join(PREVIEW, f);
    const j = JSON.parse(readFileSync(abs, "utf8"));
    const slug = nz(j.slug) || f.replace(/-factory2-frozen-approval-payload-v1\.json$/i, "");
    const brand = nz(j.brandName) || slug;
    const cards = loadGalleryCardsFromFixture(ROOT, slug, brand);
    let blocks = j.brandExplorer?.blocks || [];

    if (cards.length >= 4) {
      const result = patchBlocks(blocks, cards);
      if (result.changed) {
        blocks = result.blocks;
        j.brandExplorer = j.brandExplorer || {};
        j.brandExplorer.blocks = blocks;
        j.galleryHotelOnlyRemediation = {
          contract: "BE_FACTORY_2_0_GALLERY_HOTEL_ONLY_REMEDIATION_V1",
          remediatedAt: new Date().toISOString(),
          galleryCount: result.galleryCount,
        };
        try {
          await writeJsonAtomic(abs, j);
          patched += 1;

          const depthName = f.replace(
            "-factory2-frozen-approval-payload-v1.json",
            "-factory2-depth-candidate.json"
          );
          const depthAbs = join(PREVIEW, depthName);
          if (existsSync(depthAbs)) {
            try {
              const d = JSON.parse(readFileSync(depthAbs, "utf8"));
              if (d.brandExplorer?.blocks || d.blocks) {
                const dBlocks = d.brandExplorer?.blocks || d.blocks;
                const dResult = patchBlocks(dBlocks, cards);
                if (d.brandExplorer) d.brandExplorer.blocks = dResult.blocks;
                else d.blocks = dResult.blocks;
                await writeJsonAtomic(depthAbs, d);
              }
            } catch (err) {
              console.error(`depth sync skip ${depthName}:`, err.message || err);
            }
          }
        } catch (err) {
          writeErrors.push({ file: f, error: String(err.message || err) });
          console.error(`WRITE FAIL ${f}:`, err.message || err);
        }
      }
    }

    const after = auditGallery(blocks);
    rows.push({
      slug,
      brand,
      cardsAvailable: cards.length,
      after,
      pass: after.pass,
      remediated: cards.length >= 4,
    });
    const mark = after.pass ? "PASS" : "FAIL";
    console.error(
      `${mark} ${brand} (${slug}) cards=${cards.length} people=${after.peopleCount} misExt=${after.mislabeledExterior}`
    );
  }

  if (existsSync(HE)) {
    for (const rel of [
      "public/js/brand-explorer-staged-candidate-registry.js",
      "public/js/brand-explorer-compare-studio.js",
      "public/js/brand-explorer-cumulative-proposal-assembler.js",
    ]) {
      const src = join(ROOT, rel);
      const dest = join(HE, rel);
      if (existsSync(src) && existsSync(join(HE, "public/js"))) copyFileSync(src, dest);
    }
    const previewDest = join(HE, "public/brand-explorer-phase2e-preview");
    if (existsSync(previewDest)) {
      for (const f of readdirSync(PREVIEW).filter(
        (x) =>
          x.endsWith("-factory2-frozen-approval-payload-v1.json") ||
          x.endsWith("-factory2-depth-candidate.json")
      )) {
        try {
          copyFileSync(join(PREVIEW, f), join(previewDest, f));
        } catch (err) {
          console.error(`HE sync skip ${f}:`, err.message || err);
        }
      }
    }
    console.error("Synced hotel-only gallery payloads to HE 8080");
  }

  const fail = rows.filter((r) => !r.pass && r.after.withImage > 0);
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const report = {
    generatedAt: new Date().toISOString(),
    contract: "BE_FACTORY_2_0_GALLERY_HOTEL_ONLY_REMEDIATION_V1",
    summary: {
      audited: rows.length,
      patched,
      pass: rows.filter((r) => r.pass).length,
      fail: fail.length,
      writeErrors: writeErrors.length,
      withImages: rows.filter((r) => r.after.withImage > 0).length,
    },
    writeErrors,
    fail: fail.map((r) => ({
      slug: r.slug,
      brand: r.brand,
      peopleCount: r.after.peopleCount,
      nonHotelCount: r.after.nonHotelCount,
      mislabeledExterior: r.after.mislabeledExterior,
    })),
  };
  writeFileSync(join(dir, "94-gallery-hotel-only-remediation.json"), JSON.stringify({ ...report, rows }, null, 2) + "\n");
  writeFileSync(
    join(dir, "94-gallery-hotel-only-remediation.md"),
    [
      `# Factory 2.0 — Hotel-Only Gallery Remediation`,
      ``,
      `Generated: ${report.generatedAt}`,
      `Standard: Curio / Ascend — hotel exterior / interior / renderings only. **No people/lifestyle marketing.** Captions must match the scene.`,
      ``,
      `| Metric | Value |`,
      `|---|---:|`,
      `| Audited | ${report.summary.audited} |`,
      `| Patched | ${report.summary.patched} |`,
      `| PASS | ${report.summary.pass} |`,
      `| FAIL (with images) | ${report.summary.fail} |`,
      `| Write errors | ${report.summary.writeErrors} |`,
      `| Brands with gallery images | ${report.summary.withImages} |`,
      ``,
      fail.length
        ? `## Failures\n\n` +
          fail
            .map(
              (r) =>
                `- ${r.brand} (\`${r.slug}\`) people=${r.peopleCount} nonHotel=${r.nonHotelCount} mislabeledExterior=${r.mislabeledExterior}`
            )
            .join("\n")
        : `All audited Factory frozen galleries with images pass the hotel-only bar.`,
      ``,
    ].join("\n")
  );
  console.log(JSON.stringify(report.summary, null, 2));
  if (fail.length || writeErrors.length) process.exit(2);
}

mainAsync().catch((e) => {
  console.error(e);
  process.exit(1);
});
