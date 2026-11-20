#!/usr/bin/env node
/**
 * Openings / Examples (footprint.openings) must show hotel exterior / interior /
 * renderings — never people/lifestyle, never blank property cards when a hotel
 * photo pool exists. Curio/Ascend bar for all Factory brands.
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
  buildOpeningsHotelExampleBlocks,
  pickOpeningsHotelImages,
} from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";
import {
  isPeopleOrLifestyleMarketingImage,
  isLikelyHotelPropertyPhotograph,
} from "../lib/brand-explorer/be-permanent-six-image-gallery-standard-v1.js";

const ROOT = process.cwd();
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function isDiligenceOnlyOpening(block) {
  const t = nz(block?.title);
  return /^openings\s*\(diligence\)$/i.test(t) || /^openings$/i.test(t);
}

function needsHotelImage(block) {
  const url = nz(block?.imageUrl);
  if (!/^https?:\/\//i.test(url)) return true;
  if (isPeopleOrLifestyleMarketingImage(url)) return true;
  if (/airtableusercontent\.com/i.test(url)) return false;
  if (!isLikelyHotelPropertyPhotograph(url, { role: "hotel_property" })) return true;
  return false;
}

function sceneRoleFromTitle(title) {
  const t = nz(title);
  if (/exterior/i.test(t)) return "exterior_arrival";
  if (/guest\s*room/i.test(t)) return "guest_room";
  if (/lobby|public\s*space/i.test(t)) return "public_space_lobby";
  if (/f&b|restaurant|dining/i.test(t)) return "food_beverage";
  if (/pool|wellness|spa/i.test(t)) return "wellness_pool_spa";
  return "exterior_arrival";
}

function isAcceptableOpeningsHotelImage(block) {
  const url = nz(block?.imageUrl);
  if (!/^https?:\/\//i.test(url)) return false;
  if (isPeopleOrLifestyleMarketingImage(url)) return false;
  // Curated Airtable property attachments
  if (/airtableusercontent\.com/i.test(url)) return true;
  // Official parent DAM hosts (Hyatt/Marriott/Hilton) used as International Reference
  if (
    /assets\.hyatt\.com\/content\/dam\/hyatt/i.test(url) ||
    /cache\.marriott\.com\//i.test(url) ||
    /hilton\.com\/im\//i.test(url) ||
    /media\.radissonhotels\.net\//i.test(url) ||
    /hotelfinial\.com\//i.test(url)
  ) {
    return true;
  }
  return isLikelyHotelPropertyPhotograph(url, { role: sceneRoleFromTitle(block.title) });
}

function auditOpenings(blocks) {
  const rows = (blocks || []).filter((b) => b.slotKey === "footprint.openings");
  const visual = rows.filter((b) => !isDiligenceOnlyOpening(b));
  const withImage = visual.filter((b) => /^https?:\/\//i.test(nz(b.imageUrl)));
  const people = withImage.filter((b) => isPeopleOrLifestyleMarketingImage(b.imageUrl));
  const hotelOk = withImage.filter((b) => isAcceptableOpeningsHotelImage(b));
  return {
    slots: rows.length,
    visual: visual.length,
    withImage: withImage.length,
    hotelOk: hotelOk.length,
    peopleCount: people.length,
    pass: hotelOk.length >= 3 && people.length === 0,
  };
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

function patchOpenings(blocks, slug, brand, packLite) {
  const images = pickOpeningsHotelImages(ROOT, slug, brand, 3);
  let next = Array.isArray(blocks) ? [...blocks] : [];
  let changed = false;
  const openings = next.filter((b) => b.slotKey === "footprint.openings");
  const visual = openings.filter((b) => !isDiligenceOnlyOpening(b));
  const diligence = openings.filter((b) => isDiligenceOnlyOpening(b));

  // Case A: no visual openings — replace diligence + empty with 3 hotel example cards
  if (visual.length === 0 && images.length >= 3) {
    next = next.filter((b) => b.slotKey !== "footprint.openings");
    const built = buildOpeningsHotelExampleBlocks(ROOT, packLite, { sortStart: 490 });
    next.push(...built);
    return { blocks: next, changed: true, mode: "create_examples", images: built.length };
  }

  // Case B: fill/replace images on existing visual openings that need hotel photos
  if (images.length >= 1) {
    let imgIdx = 0;
    next = next.map((b) => {
      if (b.slotKey !== "footprint.openings") return b;
      if (isDiligenceOnlyOpening(b)) return b;
      if (!needsHotelImage(b)) return b;
      const img = images[imgIdx % images.length];
      imgIdx += 1;
      changed = true;
      return {
        ...b,
        imageUrl: img.imageUrl,
        source: b.source || img.source || "factory-2-0-openings-hotel-image-remediation",
      };
    });
  }

  // Drop diligence-only rows when we already have visual openings with images
  const afterVisual = next.filter(
    (b) => b.slotKey === "footprint.openings" && !isDiligenceOnlyOpening(b)
  );
  const afterWithImg = afterVisual.filter((b) => /^https?:\/\//i.test(nz(b.imageUrl)));
  if (afterWithImg.length >= 1 && diligence.length) {
    next = next.filter((b) => !(b.slotKey === "footprint.openings" && isDiligenceOnlyOpening(b)));
    changed = true;
  }

  // If still fewer than 3 visual hotel images, append example cards
  if (afterWithImg.length < 3 && images.length >= 3) {
    const need = 3 - afterWithImg.length;
    const built = buildOpeningsHotelExampleBlocks(ROOT, packLite, { sortStart: 500 });
    const append = built.slice(0, need).filter(
      (c) => !afterWithImg.some((e) => e.imageUrl === c.imageUrl)
    );
    if (append.length) {
      next.push(...append);
      changed = true;
    }
  }

  return {
    blocks: next,
    changed,
    mode: changed ? "patch" : "noop",
    images: images.length,
  };
}

async function main() {
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
    const packLite = {
      slug,
      brandName: brand,
      parentCompany: j.parentCompany || "the parent company",
      sources: j.sources || {},
    };
    let blocks = j.brandExplorer?.blocks || [];
    const result = patchOpenings(blocks, slug, brand, packLite);
    blocks = result.blocks;

    if (result.changed) {
      j.brandExplorer = j.brandExplorer || {};
      j.brandExplorer.blocks = blocks;
      j.openingsHotelImageRemediation = {
        contract: "BE_FACTORY_2_0_OPENINGS_HOTEL_IMAGES_V1",
        remediatedAt: new Date().toISOString(),
        mode: result.mode,
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
            const dBlocks = d.brandExplorer?.blocks || d.blocks;
            if (Array.isArray(dBlocks)) {
              const dResult = patchOpenings(dBlocks, slug, brand, packLite);
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
      }
    }

    const after = auditOpenings(blocks);
    const softGap = !after.pass && result.images < 3;
    rows.push({
      slug,
      brand,
      pool: result.images,
      after,
      pass: after.pass,
      softGap,
    });
    console.error(
      `${after.pass ? "PASS" : softGap ? "GAP" : "FAIL"} ${brand} hotelOk=${after.hotelOk}/3 people=${after.peopleCount} pool=${result.images}`
    );
  }

  if (existsSync(HE)) {
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
    console.error("Synced openings hotel images to HE 8080");
  }

  const hardFail = rows.filter((r) => !r.pass && !r.softGap);
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const report = {
    generatedAt: new Date().toISOString(),
    contract: "BE_FACTORY_2_0_OPENINGS_HOTEL_IMAGES_V1",
    summary: {
      audited: rows.length,
      patched,
      pass: rows.filter((r) => r.pass).length,
      softGap: rows.filter((r) => r.softGap).length,
      hardFail: hardFail.length,
      writeErrors: writeErrors.length,
    },
    writeErrors,
    hardFail: hardFail.map((r) => ({
      slug: r.slug,
      brand: r.brand,
      hotelOk: r.after.hotelOk,
      people: r.after.peopleCount,
    })),
  };
  writeFileSync(join(dir, "97-openings-hotel-images-remediation.json"), JSON.stringify({ ...report, rows }, null, 2) + "\n");
  writeFileSync(
    join(dir, "97-openings-hotel-images-remediation.md"),
    [
      `# Factory 2.0 — Openings / Examples Hotel Images Remediation`,
      ``,
      `Generated: ${report.generatedAt}`,
      `Rule: **Openings / Examples / Properties** must show hotel exterior / interior / renderings for all brands. No people/lifestyle. No blank IMAGE when a hotel photo pool exists.`,
      ``,
      `| Metric | Value |`,
      `|---|---:|`,
      `| Audited | ${report.summary.audited} |`,
      `| Patched | ${report.summary.patched} |`,
      `| PASS (≥3 hotel photos) | ${report.summary.pass} |`,
      `| Soft gap (no photo pool) | ${report.summary.softGap} |`,
      `| Hard FAIL | ${report.summary.hardFail} |`,
      ``,
    ].join("\n")
  );
  console.log(JSON.stringify(report.summary, null, 2));
  if (hardFail.length || writeErrors.length) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
