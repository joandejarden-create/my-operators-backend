#!/usr/bin/env node
/**
 * Browser-level image integrity certification for Factory required slots.
 * Loads authenticated Brand Explorer on :8080 and verifies each required
 * image actually renders (naturalWidth/Height, complete, currentSrc).
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 \
 *     node scripts/be-factory-image-integrity-browser-cert-v1.mjs --slugs ac-hotels-by-marriott
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import {
  BE_FACTORY_IMAGE_INTEGRITY_V1,
  REQUIRED_IMAGE_SLOTS,
  IMAGE_INTEGRITY_STATUS,
  evaluateBrandBlocksImageIntegrity,
} from "../lib/brand-explorer/factory-2/be-factory-image-integrity-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(
  /\/$/,
  ""
);
const PREVIEW = path.join(ROOT, "public/brand-explorer-phase2e-preview");
const OUT = path.join(ROOT, "reports/brand-explorer/factory-image-integrity-v1/browser");

const FAKE = "dealality-local-founder-preview-shell-token";
const ME = {
  ok: true,
  success: true,
  user: { firstName: "Dealality", lastName: "Founder", email: "dealalitydemo@dealality.com" },
  dealality: {
    role: "owner",
    isAdmin: true,
    isOwner: true,
    isDemo: true,
    localDealalityAdminAccess: true,
    flags: { isAdmin: true, isDemo: true },
  },
  data: {
    dealality: {
      role: "owner",
      isAdmin: true,
      isOwner: true,
      isDemo: true,
      localDealalityAdminAccess: true,
      flags: { isAdmin: true },
    },
  },
};

function parseArgs(argv) {
  const slugs = new Set();
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--slugs" && argv[i + 1]) {
      String(argv[++i])
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
        .forEach((s) => slugs.add(s));
    }
  }
  return { slugs };
}

async function auth(page) {
  await page.evaluate((jwt) => {
    window.__dealalityMemberstackJwt = jwt;
    const g = document.getElementById("dealalityAppShellAuthGate");
    if (g) {
      g.hidden = true;
      g.style.display = "none";
    }
    window.dispatchEvent(
      new CustomEvent("dealality-shell-auth-ready", {
        detail: { ok: true, jwt, authorized: true },
      })
    );
  }, FAKE);
}

async function waitFrame(page) {
  for (let i = 0; i < 100; i++) {
    for (const f of page.frames()) {
      if (/brand-explorer-combined\.html/i.test(f.url() || "")) return f;
    }
    await page.waitForTimeout(250);
  }
  return null;
}

async function collectRenderedImages(frame) {
  return frame.evaluate(() => {
    const root = document.getElementById("beCombinedPopupAtelierRoot") || document.body;
    const imgs = [...root.querySelectorAll("img")];
    return imgs.map((img) => {
      const src = img.currentSrc || img.src || img.getAttribute("src") || "";
      const srcset = img.getAttribute("srcset") || "";
      return {
        src,
        srcset,
        complete: img.complete,
        naturalWidth: img.naturalWidth || 0,
        naturalHeight: img.naturalHeight || 0,
        alt: img.alt || "",
        loading: img.loading || "",
        className: img.className || "",
      };
    });
  });
}

function matchBrowserEvidence(slotUrl, rendered) {
  if (!slotUrl) return { ok: false, reason: "missing_slot_url" };
  const norm = (u) => String(u || "").split("?")[0].toLowerCase();
  const target = norm(slotUrl);
  const hit = (rendered || []).find((r) => {
    const s = norm(r.src);
    return s && (s === target || s.includes(target.slice(-48)) || target.includes(s.slice(-48)));
  });
  if (!hit) {
    // Also accept if any rendered image equals full URL (CDN query variance)
    const hit2 = (rendered || []).find((r) => String(r.src || "").includes(slotUrl.slice(0, 80)));
    if (!hit2) return { ok: false, reason: "not_found_in_dom" };
    if (!hit2.complete || hit2.naturalWidth < 2 || hit2.naturalHeight < 2) {
      return {
        ok: false,
        reason: "zero_dimension_or_incomplete",
        naturalWidth: hit2.naturalWidth,
        naturalHeight: hit2.naturalHeight,
        currentSrc: hit2.src,
      };
    }
    return {
      ok: true,
      currentSrc: hit2.src,
      naturalWidth: hit2.naturalWidth,
      naturalHeight: hit2.naturalHeight,
      lazy: hit2.loading === "lazy",
    };
  }
  if (!hit.complete || hit.naturalWidth < 2 || hit.naturalHeight < 2) {
    return {
      ok: false,
      reason: "zero_dimension_or_incomplete",
      naturalWidth: hit.naturalWidth,
      naturalHeight: hit.naturalHeight,
      currentSrc: hit.src,
    };
  }
  return {
    ok: true,
    currentSrc: hit.src,
    naturalWidth: hit.naturalWidth,
    naturalHeight: hit.naturalHeight,
    lazy: hit.loading === "lazy",
    srcset: hit.srcset || null,
  };
}

async function certifyBrand(page, slug) {
  const file = `${slug}-factory2-frozen-approval-payload-v1.json`;
  const abs = path.join(PREVIEW, file);
  if (!fs.existsSync(abs)) throw new Error(`missing ${file}`);
  const frozen = JSON.parse(fs.readFileSync(abs, "utf8"));
  const brandId = frozen.brandId;
  const brandName = frozen.brandName || slug;
  const blocks = frozen.brandExplorer?.blocks || [];
  const shotDir = path.join(OUT, slug);
  fs.mkdirSync(shotDir, { recursive: true });

  await page.goto(
    `${BASE}/app#/brand-explorer-combined?id=${brandId}&beInternalPreview=1&beProfileMode=proposed`,
    { waitUntil: "domcontentloaded", timeout: 120000 }
  );
  await auth(page);
  await page.waitForTimeout(1500);
  await auth(page);
  const frame = await waitFrame(page);
  if (!frame) throw new Error("no_combined_frame");

  await frame.evaluate(
    async ({ id, name }) => {
      if (window.openBrandEducationPopupForBrand) {
        await window.openBrandEducationPopupForBrand(id, name);
      }
    },
    { id: brandId, name: brandName }
  );
  await page.waitForTimeout(4000);

  // Visit overview + materials tabs so gallery/scenario images mount (lazy-load)
  for (const tab of ["atelier-overview", "atelier-materials", "atelier-footprint"]) {
    await frame.evaluate((id) => {
      const root = document.getElementById("beCombinedPopupAtelierRoot");
      const btn = root && root.querySelector(`[data-atelier-tab="${id}"]`);
      if (btn) btn.click();
    }, tab);
    await page.waitForTimeout(800);
  }
  // Scroll to trigger lazy images
  await frame.evaluate(() => {
    const root = document.getElementById("beCombinedPopupAtelierRoot");
    if (root) root.scrollTop = root.scrollHeight;
  });
  await page.waitForTimeout(1500);

  const rendered = await collectRenderedImages(frame);
  await page.screenshot({
    path: path.join(shotDir, "01-proposed-materials.png"),
    fullPage: false,
  });

  const browserBySlot = {};
  for (const slotKey of REQUIRED_IMAGE_SLOTS) {
    const block = blocks.find((b) => b.slotKey === slotKey) || {};
    const url = block.imageUrl || "";
    browserBySlot[slotKey] = matchBrowserEvidence(url, rendered);
  }

  const integrity = await evaluateBrandBlocksImageIntegrity(
    blocks,
    { brandId, brandName, slug, repoRoot: ROOT },
    {
      browserBySlot,
      recordRejections: true,
      allowTechnicalCertify: false,
    }
  );

  const report = {
    contract: "BE_FACTORY_IMAGE_INTEGRITY_BROWSER_CERT_V1",
    parentContract: BE_FACTORY_IMAGE_INTEGRITY_V1,
    at: new Date().toISOString(),
    base: BASE,
    slug,
    brandId,
    brandName,
    renderedImageCount: rendered.length,
    browserBySlot,
    score: integrity.score,
    gate: integrity.gate,
    slotResults: integrity.slotResults,
    ok: integrity.gate.ok === true,
  };
  fs.writeFileSync(path.join(shotDir, "BROWSER_CERT.json"), JSON.stringify(report, null, 2) + "\n");
  return report;
}

async function main() {
  const { slugs } = parseArgs(process.argv);
  if (!slugs.size) {
    console.error("Require --slugs <slug[,slug...]>");
    process.exit(2);
  }
  if (/:8082\b/.test(BASE)) {
    console.error("Refuse :8082 — authenticated :8080 required");
    process.exit(2);
  }
  fs.mkdirSync(OUT, { recursive: true });

  const browser = await chromium.launch({
    headless: true,
    executablePath: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  await page.route("**/api/me**", async (r) =>
    r.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(ME) })
  );
  await page.addInitScript((jwt) => {
    window.__dealalityMemberstackJwt = jwt;
  }, FAKE);

  const results = [];
  for (const slug of slugs) {
    process.stderr.write(`browser-cert ${slug}...\n`);
    try {
      const r = await certifyBrand(page, slug);
      results.push({
        slug,
        ok: r.ok,
        certified: r.score.certifiedImages,
        required: r.score.totalRequiredImageSlots,
        broken: r.score.invalidBrokenImages,
      });
      process.stderr.write(
        `  → certified=${r.score.certifiedImages}/${r.score.totalRequiredImageSlots} ok=${r.ok}\n`
      );
    } catch (err) {
      results.push({ slug, ok: false, error: String(err?.message || err) });
      process.stderr.write(`  → ERROR ${err?.message || err}\n`);
    }
  }
  await browser.close();

  const summary = {
    contract: "BE_FACTORY_IMAGE_INTEGRITY_BROWSER_CERT_SUMMARY_V1",
    generatedAt: new Date().toISOString(),
    base: BASE,
    results,
    statuses: IMAGE_INTEGRITY_STATUS,
  };
  fs.writeFileSync(path.join(OUT, "00-summary.json"), JSON.stringify(summary, null, 2) + "\n");
  console.log(JSON.stringify(summary, null, 2));
  process.exit(results.every((r) => r.ok) ? 0 : 2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
