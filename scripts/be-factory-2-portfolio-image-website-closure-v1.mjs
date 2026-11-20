#!/usr/bin/env node
/**
 * Factory 2.0 — portfolio image + brand-website quality closure ($0 paid).
 *
 * For every Factory 2 frozen candidate:
 *  - Enforce brand-specific Brand Website (never parent homepage)
 *  - Discover / harvest authentic same-brand hotel photographs
 *  - Fill gallery (6), scenario (3), openings images when possible
 *  - Rebuild frozen hashes; update stage gaps
 *  - Soft-block certification when imagery cannot be verified
 *
 * Does NOT write production Airtable Presentation.
 *
 * Usage:
 *   node scripts/be-factory-2-portfolio-image-website-closure-v1.mjs --dry-run
 *   node scripts/be-factory-2-portfolio-image-website-closure-v1.mjs --apply
 *   node scripts/be-factory-2-portfolio-image-website-closure-v1.mjs --apply --limit 20
 */
import "../load-env.js";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
  copyFileSync,
  renameSync,
  unlinkSync,
} from "fs";
import { join } from "path";
import { setTimeout as sleep } from "timers/promises";
import { createHash } from "crypto";
import {
  discoverBrandHotelImages,
  poolToGalleryCards,
  resolveOfficialBrandWebsite,
  harvestImagesFromBlocks,
} from "../lib/brand-explorer/factory-2/be-factory-2-hotel-image-discovery-v1.js";
import { hashPresentationBlocks } from "../lib/brand-explorer/be-founder-approval-publication-v1.js";
import {
  ACTIVE_BRAND_WEBSITE_CORRECTIONS,
  isParentCompanyHomepage,
  normalizeBrandWebsiteUrl,
} from "../lib/brand-explorer/active-brand-website-corrections.js";

const ROOT = process.cwd();
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");
const STAGING = join(ROOT, "data/brand-explorer/factory-2-0/brands");
const OUT = join(ROOT, "reports/brand-explorer/portfolio-image-website-closure-v1");
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";
const apply = process.argv.includes("--apply");
const dryRun = !apply;
const onlyGaps = process.argv.includes("--only-gaps");
const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : Number(
  process.argv.includes("--limit")
    ? process.argv[process.argv.indexOf("--limit") + 1]
    : 0
) || 0;

function nz(s) {
  return s == null ? "" : String(s).trim();
}

async function writeJsonAtomic(abs, obj) {
  const tmp = `${abs}.tmp-${process.pid}`;
  const body = JSON.stringify(obj, null, 2) + "\n";
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
}

function countImages(blocks) {
  const gallery = (blocks || []).filter(
    (b) =>
      /^materials\.gallery\.\d+$/.test(b.slotKey || "") &&
      /^https?:\/\//i.test(nz(b.imageUrl))
  ).length;
  const scenario = (blocks || []).filter(
    (b) =>
      /^overview\.scenario\.[123]$/.test(b.slotKey || "") &&
      /^https?:\/\//i.test(nz(b.imageUrl))
  ).length;
  const openings = (blocks || []).filter(
    (b) =>
      /^footprint\.openings/.test(b.slotKey || "") &&
      /^https?:\/\//i.test(nz(b.imageUrl))
  ).length;
  return { gallery, scenario, openings };
}

function patchGalleryScenariosOpenings(blocks, cards, brandName) {
  let next = Array.isArray(blocks) ? blocks.map((b) => ({ ...b })) : [];
  let changed = false;
  const maxSort = Math.max(0, ...next.map((b) => Number(b.sort) || 0));

  // Gallery 1–6
  if (cards.length >= 1) {
    next = next.filter((b) => !/^materials\.gallery\.\d+$/.test(b.slotKey || ""));
    cards.slice(0, 6).forEach((c, i) => {
      next.push({
        slotKey: `materials.gallery.${i + 1}`,
        title: c.title,
        body: c.body,
        sort: maxSort + 10 + i,
        active: true,
        imageUrl: c.imageUrl,
        externalDisplayStatus: "Internal Preview",
        source: c.source || "factory-2-0-image-discovery",
      });
    });
    changed = true;
  }

  // Scenarios 1–3 — fill empty imageUrl only
  for (let i = 0; i < 3; i++) {
    const key = `overview.scenario.${i + 1}`;
    const idx = next.findIndex((b) => b.slotKey === key);
    const img = cards[i] || cards[i % Math.max(1, cards.length)];
    if (!img) continue;
    if (idx < 0) continue;
    if (!/^https?:\/\//i.test(nz(next[idx].imageUrl))) {
      next[idx] = {
        ...next[idx],
        imageUrl: img.imageUrl,
        source: next[idx].source || img.source || "factory-2-0-scenario-image-discovery",
      };
      changed = true;
    }
  }

  // Openings — fill first 3 empty image cards when present
  const openingIdxs = next
    .map((b, i) => (/^footprint\.openings/.test(b.slotKey || "") ? i : -1))
    .filter((i) => i >= 0);
  let oi = 0;
  for (const idx of openingIdxs) {
    if (oi >= cards.length) break;
    if (!/^https?:\/\//i.test(nz(next[idx].imageUrl))) {
      const img = cards[oi++];
      next[idx] = {
        ...next[idx],
        imageUrl: img.imageUrl,
        source: next[idx].source || img.source || "factory-2-0-openings-image-discovery",
      };
      changed = true;
    }
  }

  return { blocks: next, changed, brandName };
}

function recomputeGaps(blocks, brandWebsite, brandName) {
  const c = countImages(blocks);
  const gaps = [];
  gaps.push("fee_amounts_unverified_pending_disclosure_exhibit");
  if (c.gallery < 6) gaps.push(`gallery_images_pending_verified_cdn:${c.gallery}/6`);
  if (c.scenario < 3) gaps.push(`scenario_images_pending:${c.scenario}/3`);
  if (!brandWebsite || isParentCompanyHomepage(brandWebsite, "", brandName)) {
    gaps.push("brand_website_parent_homepage_or_missing");
  }
  return { gaps, counts: c };
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  let files = readdirSync(PREVIEW)
    .filter((f) => f.endsWith("-factory2-frozen-approval-payload-v1.json"))
    .sort();
  if (onlyGaps) {
    files = files.filter((f) => {
      try {
        const j = JSON.parse(readFileSync(join(PREVIEW, f), "utf8"));
        const c = countImages(j.brandExplorer?.blocks || []);
        return c.gallery < 6 || c.scenario < 3;
      } catch {
        return true;
      }
    });
  }
  const selected = LIMIT > 0 ? files.slice(0, LIMIT) : files;

  const before = { galleryOk: 0, galleryGap: 0, scenarioOk: 0, scenarioGap: 0 };
  const rows = [];
  let patched = 0;
  let discoveryOk = 0;
  let discoveryBlocked = 0;
  let websitesOk = 0;
  let websitesFixed = 0;

  console.error(
    `Portfolio image/website closure · mode=${dryRun ? "dry-run" : "apply"} · brands=${selected.length}`
  );

  for (const f of selected) {
    const abs = join(PREVIEW, f);
    const j = JSON.parse(readFileSync(abs, "utf8"));
    const slug = nz(j.slug) || f.replace(/-factory2-frozen-approval-payload-v1\.json$/i, "");
    const brandName = nz(j.brandName) || slug;
    const brandId = nz(j.brandId);
    let blocks = j.brandExplorer?.blocks || [];
    const countsBefore = countImages(blocks);
    if (countsBefore.gallery >= 6) before.galleryOk += 1;
    else before.galleryGap += 1;
    if (countsBefore.scenario >= 3) before.scenarioOk += 1;
    else before.scenarioGap += 1;

    const site = resolveOfficialBrandWebsite({
      brandName,
      sources: j.sources || {},
    });
    if (site.url && !site.parentHomepage) websitesOk += 1;
    else websitesFixed += 0;

    const needsImages = countsBefore.gallery < 6 || countsBefore.scenario < 3;
    let pool = null;
    let cards = [];

    if (needsImages) {
      // Fast path: harvest existing same-candidate images + durable pool
      const harvested = harvestImagesFromBlocks(blocks, slug, brandName);
      pool = await discoverBrandHotelImages(
        ROOT,
        {
          slug,
          brandName,
          brandId,
          blocks,
          sources: {
            ...(j.sources || {}),
            brandPage: site.url || j.sources?.brandPage,
          },
        },
        {
          need: 6,
          persist: !dryRun,
          skipHeadCheck: false,
        }
      );
      cards = poolToGalleryCards(pool, brandName, 6);
      if (cards.length < 6 && harvested.length) {
        const seen = new Set(cards.map((c) => c.imageUrl));
        for (const h of harvested) {
          if (cards.length >= 6) break;
          if (seen.has(h.imageUrl)) continue;
          cards.push(h);
          seen.add(h.imageUrl);
        }
      }
      if (pool?.images?.length >= 6) discoveryOk += 1;
      else discoveryBlocked += 1;
    } else {
      discoveryOk += 1;
      cards = harvestImagesFromBlocks(blocks, slug, brandName);
    }

    const patch = needsImages
      ? patchGalleryScenariosOpenings(blocks, cards, brandName)
      : { blocks, changed: false };
    const websiteChanged =
      site.url &&
      !site.parentHomepage &&
      normalizeBrandWebsiteUrl(j.sources?.brandPage || "") !== site.url;

    if (websiteChanged) {
      j.sources = { ...(j.sources || {}), brandPage: site.url };
      j.brandWebsite = site.url;
      websitesFixed += 1;
    } else if (site.url && !site.parentHomepage) {
      j.brandWebsite = site.url;
    }

    const countsAfter = countImages(patch.blocks);
    const gapInfo = recomputeGaps(patch.blocks, site.url, brandName);
    const assembledHash = hashPresentationBlocks(patch.blocks);

    const row = {
      slug,
      brandId,
      brandName,
      before: countsBefore,
      after: countsAfter,
      brandWebsite: site.url || null,
      brandWebsiteOk: Boolean(site.url && !site.parentHomepage),
      imagesDiscovered: cards.length,
      discoveryBlocker: pool?.certificationBlocker || null,
      gaps: gapInfo.gaps,
      assembledHash,
      changed: patch.changed || websiteChanged,
      patched: false,
    };

    if (!dryRun && (patch.changed || websiteChanged)) {
      j.brandExplorer = j.brandExplorer || {};
      j.brandExplorer.blocks = patch.blocks;
      j.approvalContentHash = assembledHash;
      j.imageWebsiteClosure = {
        contract: "BE_FACTORY_2_0_IMAGE_WEBSITE_CLOSURE_V1",
        remediatedAt: new Date().toISOString(),
        researchSpendUsd: 0,
        gallery: countsAfter.gallery,
        scenario: countsAfter.scenario,
        brandWebsite: site.url,
      };
      await writeJsonAtomic(abs, j);

      // Mirror depth candidate blocks when present
      const depthName = f.replace(
        "-factory2-frozen-approval-payload-v1.json",
        "-factory2-depth-candidate.json"
      );
      const depthAbs = join(PREVIEW, depthName);
      if (existsSync(depthAbs)) {
        try {
          const d = JSON.parse(readFileSync(depthAbs, "utf8"));
          if (d.brandExplorer) d.brandExplorer.blocks = patch.blocks;
          else d.blocks = patch.blocks;
          d.presentationBlocksHashSha256 = createHash("sha256")
            .update(JSON.stringify(d.brandExplorer?.blocks || d.blocks || []))
            .digest("hex");
          await writeJsonAtomic(depthAbs, d);
        } catch (err) {
          console.error(`depth sync skip ${depthName}:`, err.message || err);
        }
      }

      // Update stage gaps / hashes when stage folder exists
      const stageDir = join(STAGING, brandId);
      const stagePath = join(stageDir, "stage.json");
      if (existsSync(stagePath)) {
        try {
          const stage = JSON.parse(readFileSync(stagePath, "utf8"));
          stage.assembledHash = assembledHash;
          stage.candidateHash = assembledHash;
          stage.gaps = gapInfo.gaps;
          stage.factoryStatus =
            gapInfo.gaps.some((g) => /gallery_images|scenario_images|brand_website/i.test(g))
              ? "NEEDS_REMEDIATION"
              : stage.factoryStatus;
          stage.brandWebsite = site.url;
          stage.imageWebsiteClosureAt = new Date().toISOString();
          await writeJsonAtomic(stagePath, stage);
        } catch (err) {
          console.error(`stage sync skip ${brandId}:`, err.message || err);
        }
      }

      // Sync to HE preview for 8080 boot path
      if (existsSync(join(HE, "public/brand-explorer-phase2e-preview"))) {
        try {
          copyFileSync(abs, join(HE, "public/brand-explorer-phase2e-preview", f));
        } catch {
          /* ignore */
        }
      }

      row.patched = true;
      patched += 1;
    }

    rows.push(row);
    const mark =
      countsAfter.gallery >= 6 && countsAfter.scenario >= 3
        ? "PASS_IMG"
        : row.discoveryBlocker
          ? "BLOCK"
          : "GAP";
    console.error(
      `${mark} ${brandName} gal ${countsBefore.gallery}→${countsAfter.gallery} scen ${countsBefore.scenario}→${countsAfter.scenario} web=${site.parentHomepage ? "PARENT" : "OK"}`
    );

    // polite pacing for free HTTP discovery
    if (needsImages) await sleep(150);
  }

  const after = {
    galleryOk: rows.filter((r) => r.after.gallery >= 6).length,
    galleryGap: rows.filter((r) => r.after.gallery < 6).length,
    scenarioOk: rows.filter((r) => r.after.scenario >= 3).length,
    scenarioGap: rows.filter((r) => r.after.scenario < 3).length,
  };

  const summary = {
    generatedAt: new Date().toISOString(),
    contract: "BE_FACTORY_2_0_IMAGE_WEBSITE_CLOSURE_V1",
    mode: dryRun ? "dry-run" : "apply",
    brandsAudited: rows.length,
    researchSpendUsd: 0,
    before,
    after,
    patched,
    discoveryOk,
    discoveryBlocked,
    websitesOk,
    websitesFixed,
    brandWebsiteCorrectionsMapSize: Object.keys(ACTIVE_BRAND_WEBSITE_CORRECTIONS).length,
    stillNeedsRemediation: rows.filter(
      (r) => r.after.gallery < 6 || r.after.scenario < 3 || !r.brandWebsiteOk
    ).length,
    fullyImageReady: rows.filter(
      (r) => r.after.gallery >= 6 && r.after.scenario >= 3 && r.brandWebsiteOk
    ).length,
    feeGapsUnchanged: rows.length, // still pending disclosure exhibits
    note:
      "Fee amounts remain unverified by policy. Image discovery uses $0 free HTTP only. Soft-block when verified imagery cannot be obtained.",
  };

  writeFileSync(join(OUT, "00-summary.json"), JSON.stringify(summary, null, 2) + "\n");
  writeFileSync(join(OUT, "01-brand-rows.json"), JSON.stringify({ generatedAt: summary.generatedAt, rows }, null, 2) + "\n");
  writeFileSync(
    join(OUT, "02-founder-dashboard.md"),
    [
      `# Brand Explorer — Image & Website Quality Closure`,
      ``,
      `Generated: ${summary.generatedAt}`,
      `Mode: \`${summary.mode}\``,
      `Research spend: **$0**`,
      ``,
      `## Summary`,
      ``,
      `| Metric | Before | After |`,
      `|---|---:|---:|`,
      `| Brands audited | ${rows.length} | ${rows.length} |`,
      `| Gallery 6/6 OK | ${before.galleryOk} | ${after.galleryOk} |`,
      `| Gallery gaps | ${before.galleryGap} | ${after.galleryGap} |`,
      `| Scenario 3/3 OK | ${before.scenarioOk} | ${after.scenarioOk} |`,
      `| Scenario gaps | ${before.scenarioGap} | ${after.scenarioGap} |`,
      `| Brand websites OK | — | ${rows.filter((r) => r.brandWebsiteOk).length} |`,
      `| Patched candidates | — | ${patched} |`,
      `| Image-ready (gal+scen+web) | — | ${summary.fullyImageReady} |`,
      `| Still needs remediation | — | ${summary.stillNeedsRemediation} |`,
      `| Fee verification gaps | ${rows.length} | ${rows.length} (unchanged — disclosure exhibits) |`,
      ``,
      `## Permanent rules`,
      ``,
      `- Brand Website = brand-specific official page (never parent homepage).`,
      `- Gallery / scenarios / openings = authentic same-brand hotel photography.`,
      `- Missing verified imagery → certification blocker (no invented photos).`,
      `- Fee amounts never fabricated.`,
      ``,
      `## PORTFOLIO_AUDIT_COMPLETE`,
      ``,
      `**NO** — fee exhibits + authenticated :8080 browser certification remain.`,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(summary, null, 2));
  if (dryRun) {
    console.error("Dry run only. Re-run with --apply to write candidates.");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
