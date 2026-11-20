#!/usr/bin/env node
/**
 * Fill overview.scenario.1–3 (Where This Brand Creates the Most Value) with
 * hotel exterior / interior / rendering photos — Curio/Ascend bar.
 * Never leave IMAGE placeholders when a fixture hotel pool exists.
 * Never people/lifestyle marketing.
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
import { pickScenarioHotelImages } from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";
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

function needsHotelImage(block) {
  const url = nz(block?.imageUrl);
  if (!/^https?:\/\//i.test(url)) return true;
  // People/lifestyle check is URL/path only — scenario titles often say
  // "Lifestyle-Select" as a product term and must not false-positive the image.
  if (isPeopleOrLifestyleMarketingImage(url)) return true;
  // Keep curated Airtable / CDN hotel photos even when path cues are opaque
  if (/airtableusercontent\.com/i.test(url)) return false;
  if (!isLikelyHotelPropertyPhotograph(url, { role: "hotel_property" })) return true;
  return false;
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

function auditScenarios(blocks) {
  const sc = (blocks || []).filter((b) => /^overview\.scenario\.[123]$/.test(b.slotKey || ""));
  const withImage = sc.filter((b) => /^https?:\/\//i.test(nz(b.imageUrl)));
  const people = withImage.filter((b) => isPeopleOrLifestyleMarketingImage(b.imageUrl));
  return {
    slots: sc.length,
    withImage: withImage.length,
    peopleCount: people.length,
    pass: sc.length >= 3 && withImage.length >= 3 && people.length === 0,
  };
}

async function main() {
  const files = readdirSync(PREVIEW).filter((x) =>
    x.endsWith("-factory2-frozen-approval-payload-v1.json")
  );
  const rows = [];
  let patched = 0;
  const writeErrors = [];
  const noPool = [];

  for (const f of files) {
    const abs = join(PREVIEW, f);
    const j = JSON.parse(readFileSync(abs, "utf8"));
    const slug = nz(j.slug) || f.replace(/-factory2-frozen-approval-payload-v1\.json$/i, "");
    const brand = nz(j.brandName) || slug;
    const images = pickScenarioHotelImages(ROOT, slug, brand, 3);
    let blocks = j.brandExplorer?.blocks || [];
    let changed = false;

    if (images.length >= 3) {
      for (let i = 0; i < 3; i++) {
        const key = `overview.scenario.${i + 1}`;
        const idx = blocks.findIndex((b) => b.slotKey === key);
        const img = images[i];
        if (idx < 0) {
          blocks.push({
            slotKey: key,
            title: `Owner Scenario ${i + 1}`,
            body: `Diligence ${brand} fit for this owner use case.`,
            sort: 2100 + i,
            active: true,
            imageUrl: img.imageUrl,
            externalDisplayStatus: "Internal Preview",
            source: img.source || "factory-2-0-scenario-hotel-image-remediation",
          });
          changed = true;
          continue;
        }
        if (needsHotelImage(blocks[idx])) {
          blocks[idx] = {
            ...blocks[idx],
            imageUrl: img.imageUrl,
            source: blocks[idx].source || img.source || "factory-2-0-scenario-hotel-image-remediation",
          };
          changed = true;
        }
      }
    } else {
      noPool.push({ slug, brand, available: images.length });
    }

    if (changed) {
      j.brandExplorer = j.brandExplorer || {};
      j.brandExplorer.blocks = blocks;
      j.scenarioHotelImageRemediation = {
        contract: "BE_FACTORY_2_0_SCENARIO_HOTEL_IMAGES_V1",
        remediatedAt: new Date().toISOString(),
        scenarioImages: 3,
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
              for (let i = 0; i < 3; i++) {
                const key = `overview.scenario.${i + 1}`;
                const idx = dBlocks.findIndex((b) => b.slotKey === key);
                const img = images[i];
                if (idx >= 0 && img && needsHotelImage(dBlocks[idx])) {
                  dBlocks[idx] = { ...dBlocks[idx], imageUrl: img.imageUrl };
                }
              }
              if (d.brandExplorer) d.brandExplorer.blocks = dBlocks;
              else d.blocks = dBlocks;
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

    const after = auditScenarios(blocks);
    // Brands with no pool and empty images are READY gaps, not hard FAIL for this remediator
    // Soft gap: no hotel photo pool and still missing images (READY / inventory gap)
    const softGap = !after.pass && images.length < 3;
    rows.push({
      slug,
      brand,
      imagesAvailable: images.length,
      after,
      pass: after.pass,
      softGap,
    });
    console.error(
      `${after.pass ? "PASS" : softGap ? "GAP" : "FAIL"} ${brand} (${slug}) scenImg=${after.withImage}/3 pool=${images.length}`
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
    console.error("Synced scenario hotel images to HE 8080");
  }

  const hardFail = rows.filter((r) => !r.pass && !r.softGap);
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const report = {
    generatedAt: new Date().toISOString(),
    contract: "BE_FACTORY_2_0_SCENARIO_HOTEL_IMAGES_V1",
    summary: {
      audited: rows.length,
      patched,
      pass: rows.filter((r) => r.pass).length,
      softGap: rows.filter((r) => r.softGap).length,
      hardFail: hardFail.length,
      writeErrors: writeErrors.length,
      noPool: noPool.length,
    },
    noPool,
    writeErrors,
    hardFail: hardFail.map((r) => ({
      slug: r.slug,
      brand: r.brand,
      withImage: r.after.withImage,
      people: r.after.peopleCount,
    })),
  };
  writeFileSync(join(dir, "95-scenario-hotel-images-remediation.json"), JSON.stringify({ ...report, rows }, null, 2) + "\n");
  writeFileSync(
    join(dir, "95-scenario-hotel-images-remediation.md"),
    [
      `# Factory 2.0 — Scenario Hotel Images Remediation`,
      ``,
      `Generated: ${report.generatedAt}`,
      `Standard: **Where This Brand Creates the Most Value** cards must show hotel exterior / interior / renderings (Curio/Ascend). No empty IMAGE placeholders when a hotel photo pool exists. No people/lifestyle.`,
      ``,
      `| Metric | Value |`,
      `|---|---:|`,
      `| Audited | ${report.summary.audited} |`,
      `| Patched | ${report.summary.patched} |`,
      `| PASS (3/3 hotel images) | ${report.summary.pass} |`,
      `| Soft gap (no fixture pool) | ${report.summary.softGap} |`,
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
