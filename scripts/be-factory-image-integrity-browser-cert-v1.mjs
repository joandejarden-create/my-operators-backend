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
        recordId: img.getAttribute("data-be-record-id") || "",
        slotKey: img.getAttribute("data-be-slot") || "",
      };
    });
  });
}

function isRenderedOk(hit) {
  return !!(hit && hit.complete && hit.naturalWidth >= 2 && hit.naturalHeight >= 2);
}

function matchBrowserEvidence(slotUrl, rendered, block = {}) {
  const recordId = String(block.recordId || "").trim();
  const slotKey = String(block.slotKey || "").trim();
  const deliveryHint = recordId
    ? `/api/brand-library/presentation-image/${encodeURIComponent(recordId)}`
    : "";
  const norm = (u) => String(u || "").split("?")[0].toLowerCase();

  // Prefer DOM identity (recordId / slotKey / delivery path) over frozen signed CDN URLs.
  let hit =
    (rendered || []).find((r) => recordId && r.recordId === recordId) ||
    (rendered || []).find((r) => slotKey && r.slotKey === slotKey) ||
    (rendered || []).find(
      (r) =>
        deliveryHint &&
        (norm(r.src).includes(norm(deliveryHint)) ||
          /\/api\/brand-(?:library|explorer)\/presentation-image\//i.test(r.src) &&
            r.src.includes(recordId))
    );

  if (!hit && slotUrl) {
    const target = norm(slotUrl);
    hit = (rendered || []).find((r) => {
      const s = norm(r.src);
      return s && (s === target || s.includes(target.slice(-48)) || target.includes(s.slice(-48)));
    });
    if (!hit) {
      hit = (rendered || []).find((r) => String(r.src || "").includes(String(slotUrl).slice(0, 80)));
    }
  }

  if (!hit) return { ok: false, reason: "not_found_in_dom", expectedRecordId: recordId || null };
  if (!isRenderedOk(hit)) {
    return {
      ok: false,
      reason: "zero_dimension_or_incomplete",
      naturalWidth: hit.naturalWidth,
      naturalHeight: hit.naturalHeight,
      currentSrc: hit.src,
      recordId: hit.recordId || null,
      slotKey: hit.slotKey || null,
    };
  }
  // Fail closed if DOM still points at ephemeral Airtable CDN for a known presentation record.
  if (recordId && /airtableusercontent\.com/i.test(hit.src || "")) {
    return {
      ok: false,
      reason: "ephemeral_cdn_still_in_dom",
      currentSrc: hit.src,
      recordId,
      slotKey: hit.slotKey || slotKey || null,
    };
  }
  return {
    ok: true,
    currentSrc: hit.src,
    naturalWidth: hit.naturalWidth,
    naturalHeight: hit.naturalHeight,
    lazy: hit.loading === "lazy",
    srcset: hit.srcset || null,
    recordId: hit.recordId || recordId || null,
    slotKey: hit.slotKey || slotKey || null,
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

  // Overview first — founder-visible scenario cards (mandatory visual gate).
  await frame.evaluate(() => {
    const root = document.getElementById("beCombinedPopupAtelierRoot");
    const btn = root && root.querySelector(`[data-atelier-tab="atelier-overview"]`);
    if (btn) btn.click();
  });
  await page.waitForTimeout(1200);
  await frame.evaluate(() => {
    const card = document.querySelector(".scenario-card--visual, .scenario-card");
    if (card) card.scrollIntoView({ block: "center" });
  });
  await page.waitForTimeout(2000);

  const scenarioDom = await frame.evaluate(() => {
    const cards = [...document.querySelectorAll(".scenario-card--visual, .scenario-card")].slice(0, 3);
    return cards.map((card, idx) => {
      const img = card.querySelector("img");
      const title = (card.querySelector("h4, .scenario-card__title, h3") || {}).textContent || "";
      return {
        slotHint: `overview.scenario.${idx + 1}`,
        title: String(title).trim(),
        src: img ? img.currentSrc || img.src || "" : "",
        complete: img ? img.complete : false,
        naturalWidth: img ? img.naturalWidth || 0 : 0,
        naturalHeight: img ? img.naturalHeight || 0 : 0,
        recordId: img ? img.getAttribute("data-be-record-id") || "" : "",
        slotKey: img ? img.getAttribute("data-be-slot") || "" : "",
      };
    });
  });
  await page.screenshot({
    path: path.join(shotDir, "00-overview-scenarios-three-cards.png"),
    fullPage: false,
  });

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
    browserBySlot[slotKey] = matchBrowserEvidence(url, rendered, { ...block, slotKey });
  }

  const scenarioGate = {
    required: 3,
    cards: scenarioDom,
    ok:
      scenarioDom.length === 3 &&
      scenarioDom.every(
        (c) =>
          c.complete &&
          c.naturalWidth >= 2 &&
          c.naturalHeight >= 2 &&
          !/airtableusercontent\.com/i.test(c.src || "")
      ),
  };

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
    profileMode: "proposed",
    renderedImageCount: rendered.length,
    scenarioGate,
    browserBySlot,
    score: integrity.score,
    gate: integrity.gate,
    slotResults: integrity.slotResults,
    fullIntegrityOk: integrity.gate.ok === true,
    // Visible Overview scenario cards are the mandatory founder gate.
    // Full 9-slot integrity remains reported separately (gallery/openings).
    ok:
      scenarioGate.ok === true &&
      ["overview.scenario.1", "overview.scenario.2", "overview.scenario.3"].every(
        (k) => browserBySlot[k] && browserBySlot[k].ok === true
      ),
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
