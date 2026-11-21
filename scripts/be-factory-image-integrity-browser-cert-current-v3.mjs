#!/usr/bin/env node
/**
 * Image Integrity V3 — authenticated CURRENT browser certification on :8080.
 *
 * Unlike V1 (Proposed/frozen payloads), this certifies live CURRENT Presentation
 * attachments via delivery URLs and DOM naturalWidth/Height.
 *
 *   node scripts/be-factory-image-integrity-browser-cert-current-v3.mjs
 *   node scripts/be-factory-image-integrity-browser-cert-current-v3.mjs --slugs ac-hotels-by-marriott,courtyard-by-marriott,aloft-hotels,bunkhouse-hotels
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import { chromium } from "playwright";
import Airtable from "airtable";
import { REQUIRED_IMAGE_SLOTS } from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v3.js";

const ROOT = process.cwd();
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
const OUT = join(ROOT, "reports/brand-explorer/portfolio-image-integrity-v3/browser-current");
mkdirSync(OUT, { recursive: true });

const FAKE = "dealality-local-founder-preview-shell-token";
const REQUIRED = new Set(REQUIRED_IMAGE_SLOTS);

function parseSlugs(argv) {
  if (argv.includes("--all")) return ["__ALL__"];
  const i = argv.indexOf("--slugs");
  if (i < 0 || !argv[i + 1]) {
    return [
      "ac-hotels-by-marriott",
      "courtyard-by-marriott",
      "aloft-hotels",
      "bunkhouse-hotels",
    ];
  }
  return argv[i + 1].split(",").map((s) => s.trim()).filter(Boolean);
}

function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

const targetSlugs = new Set(parseSlugs(process.argv));
const matrixPath = join(
  ROOT,
  "reports/brand-explorer/portfolio-image-integrity-v3/03-current-slot-matrix.json"
);
const discrepancyPath = join(
  ROOT,
  "reports/brand-explorer/portfolio-image-integrity-v3/30-discrepancy-resolution.json"
);

const primaryByKey = new Map();
if (existsSync(discrepancyPath)) {
  const d = JSON.parse(readFileSync(discrepancyPath, "utf8"));
  for (const c of d.duplicateConflicts || []) {
    primaryByKey.set(`${c.brandId}|${c.slotKey}`, c.primaryRecordId);
  }
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const brands = await base("Brand Setup - Brand Basics")
  .select({
    filterByFormula: "OR({Brand Status}='Active',{Brand Status}='Live')",
    fields: ["Brand Name", "Brand Status"],
  })
  .all();

const selected = brands
  .map((r) => ({
    id: r.id,
    name: String(r.get("Brand Name") || ""),
    slug: slugify(r.get("Brand Name")),
  }))
  .filter((b) =>
    targetSlugs.has("__ALL__")
      ? true
      : targetSlugs.has(b.slug) ||
        (targetSlugs.has("courtyard") && b.slug === "courtyard-by-marriott")
  )
  .sort((a, b) => a.name.localeCompare(b.name));

if (!selected.length) {
  console.error("No matching brands for slugs", [...targetSlugs]);
  process.exit(1);
}
console.error(`Certifying ${selected.length} brand(s) on ${BASE}`);

async function loadSlots(brand) {
  const rows = await base("Brand Setup - Brand Explorer Presentation")
    .select({
      filterByFormula: `{Brand Name}="${brand.name.replace(/"/g, '\\"')}"`,
      pageSize: 100,
    })
    .all();
  const bySlot = new Map();
  for (const r of rows) {
    const slot = String(r.get("Slot Key") || "").trim();
    if (!REQUIRED.has(slot) || r.get("Active") === false) continue;
    const brandIds = r.get("Brand") || [];
    if (Array.isArray(brandIds) && brandIds[0] && brandIds[0] !== brand.id) continue;
    if (!bySlot.has(slot)) bySlot.set(slot, []);
    bySlot.get(slot).push(r);
  }
  const slots = [];
  for (const slot of REQUIRED_IMAGE_SLOTS) {
    const list = bySlot.get(slot) || [];
    const preferred = primaryByKey.get(`${brand.id}|${slot}`);
    const primary =
      list.find((r) => r.id === preferred) ||
      list.find((r) => Array.isArray(r.get("Image")) && r.get("Image")[0]) ||
      list[0] ||
      null;
    const img = primary?.get("Image");
    const att = Array.isArray(img) && img[0] ? img[0] : null;
    slots.push({
      slotKey: slot,
      recordId: primary?.id || null,
      hasAttachment: !!att,
      filename: att?.filename || null,
      title: String(primary?.get("Title") || ""),
    });
  }
  return slots;
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
  for (let i = 0; i < 120; i++) {
    for (const f of page.frames()) {
      if (/brand-explorer-combined\.html/i.test(f.url() || "")) return f;
    }
    await page.waitForTimeout(250);
  }
  return null;
}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1100 } });
const page = await context.newPage();

const brandReports = [];

for (const brand of selected) {
  const slots = await loadSlots(brand);
  const shotDir = join(OUT, brand.slug);
  mkdirSync(shotDir, { recursive: true });

  await page.goto(
    `${BASE}/app#/brand-explorer-combined?id=${brand.id}&beInternalPreview=1&beProfileMode=current`,
    { waitUntil: "domcontentloaded", timeout: 120000 }
  );
  await auth(page);
  await page.waitForTimeout(1200);
  await auth(page);
  const frame = await waitFrame(page);
  if (!frame) {
    brandReports.push({
      brandId: brand.id,
      brandName: brand.name,
      brandSlug: brand.slug,
      ok: false,
      error: "no_combined_frame",
    });
    continue;
  }

  await frame.evaluate(
    async ({ id, name }) => {
      if (window.openBrandEducationPopupForBrand) {
        await window.openBrandEducationPopupForBrand(id, name);
      }
    },
    { id: brand.id, name: brand.name }
  );
  await page.waitForTimeout(3500);

  // Overview scenarios — scroll past Brand Snapshot to scenario visual cards
  await frame.evaluate(() => {
    const root = document.getElementById("beCombinedPopupAtelierRoot");
    const btn = root && root.querySelector(`[data-atelier-tab="atelier-overview"]`);
    if (btn) btn.click();
  });
  await page.waitForTimeout(1200);
  await frame.evaluate(async () => {
    const root = document.getElementById("beCombinedPopupAtelierRoot") || document.body;
    const scrollParent =
      root.querySelector(".atelier-body, .be-atelier-scroll, .popup-body, [data-atelier-scroll]") ||
      root;
    for (let i = 0; i < 12; i++) {
      scrollParent.scrollTop = (scrollParent.scrollTop || 0) + 500;
      window.scrollBy(0, 500);
      await new Promise((r) => setTimeout(r, 200));
      const card = document.querySelector(
        ".scenario-card--visual img, .scenario-card img, [data-be-slot^='overview.scenario'] img"
      );
      if (card) {
        card.scrollIntoView({ block: "center" });
        break;
      }
    }
    // Eager-load any lazy scenario images
    for (const img of document.querySelectorAll("img[loading='lazy']")) {
      try {
        img.loading = "eager";
        if (img.dataset.src && !img.src) img.src = img.dataset.src;
      } catch {
        /* ignore */
      }
    }
  });
  await page.waitForTimeout(2500);
  await page.screenshot({
    path: join(shotDir, "01-overview-scenarios.png"),
    fullPage: false,
  });

  // Materials gallery
  await frame.evaluate(() => {
    const root = document.getElementById("beCombinedPopupAtelierRoot");
    const btn = root && root.querySelector(`[data-atelier-tab="atelier-materials"]`);
    if (btn) btn.click();
  });
  await page.waitForTimeout(1200);
  await frame.evaluate(async () => {
    const root = document.getElementById("beCombinedPopupAtelierRoot") || document.body;
    const scrollParent =
      root.querySelector(".atelier-body, .be-atelier-scroll, .popup-body, [data-atelier-scroll]") ||
      root;
    const heading = [...document.querySelectorAll("h1,h2,h3,h4,.section-title,p,div")].find((el) =>
      /^image gallery$/i.test(String(el.textContent || "").trim())
    );
    if (heading) heading.scrollIntoView({ block: "start" });
    for (let i = 0; i < 24; i++) {
      scrollParent.scrollTop = (scrollParent.scrollTop || 0) + 350;
      for (const img of document.querySelectorAll("img[loading='lazy']")) {
        try {
          img.loading = "eager";
        } catch {
          /* ignore */
        }
      }
      await new Promise((r) => setTimeout(r, 220));
      const galleryImgs = [...document.querySelectorAll("img")].filter((img) =>
        /presentation-image/i.test(img.currentSrc || img.src || "")
      );
      const materialSlots = galleryImgs.filter((img) =>
        /materials\.gallery/i.test(img.getAttribute("data-be-slot") || "")
      );
      if (materialSlots.length >= 6 || galleryImgs.length >= 9) break;
    }
    // Final nudge: scroll last gallery image into view
    const last = [...document.querySelectorAll("img")].filter((img) =>
      /presentation-image/i.test(img.currentSrc || img.src || "")
    );
    if (last.length) last[last.length - 1].scrollIntoView({ block: "end" });
  });
  await page.waitForTimeout(3500);
  await page.screenshot({
    path: join(shotDir, "02-materials-gallery.png"),
    fullPage: false,
  });

  const rendered = await frame.evaluate(() => {
    const root = document.getElementById("beCombinedPopupAtelierRoot") || document.body;
    return [...root.querySelectorAll("img")].map((img) => {
      const src = img.currentSrc || img.src || "";
      const m = src.match(/presentation-image\/(rec[a-zA-Z0-9]+)/);
      return {
        src,
        complete: img.complete,
        naturalWidth: img.naturalWidth || 0,
        naturalHeight: img.naturalHeight || 0,
        recordId: img.getAttribute("data-be-record-id") || (m ? m[1] : ""),
        slotKey: img.getAttribute("data-be-slot") || "",
        alt: img.alt || "",
      };
    });
  });

  const slotResults = [];
  for (const s of slots) {
    const delivery = s.recordId
      ? `${BASE}/api/brand-library/presentation-image/${encodeURIComponent(s.recordId)}`
      : null;
    let deliveryOk = null;
    if (delivery) {
      try {
        const res = await fetch(delivery, { redirect: "follow" });
        const buf = Buffer.from(await res.arrayBuffer());
        deliveryOk = {
          http: res.status,
          bytes: buf.length,
          jpeg: buf[0] === 0xff && buf[1] === 0xd8,
          png: buf[0] === 0x89 && buf[1] === 0x50,
        };
      } catch (e) {
        deliveryOk = { error: String(e.message || e).slice(0, 120) };
      }
    }

    let hit =
      rendered.find((r) => s.recordId && r.recordId === s.recordId) ||
      rendered.find((r) => r.slotKey === s.slotKey) ||
      rendered.find(
        (r) =>
          s.recordId &&
          /presentation-image/i.test(r.src) &&
          r.src.includes(s.recordId)
      );

    const browserOk = !!(hit && hit.complete && hit.naturalWidth >= 2 && hit.naturalHeight >= 2);
    const ephemeral = !!(hit && /airtableusercontent\.com/i.test(hit.src || ""));
    const techOk = !!(deliveryOk && deliveryOk.http === 200 && (deliveryOk.jpeg || deliveryOk.png));

    let classification = "IMAGE_MISSING";
    if (!s.hasAttachment) classification = "IMAGE_MISSING";
    else if (!techOk) classification = "IMAGE_FETCH_FAILED";
    else if (!hit) classification = "IMAGE_TECHNICALLY_VALID_DOM_NOT_MATCHED";
    else if (ephemeral) classification = "IMAGE_SIGNED_URL_IN_DOM";
    else if (!browserOk) classification = "IMAGE_BROWSER_RENDER_FAILED";
    else classification = "IMAGE_BROWSER_VALIDATED";

    slotResults.push({
      ...s,
      classification,
      browserOk,
      techOk,
      ephemeral,
      deliveryOk,
      currentSrc: hit?.src || null,
      naturalWidth: hit?.naturalWidth || 0,
      naturalHeight: hit?.naturalHeight || 0,
    });
  }

  const browserValidated = slotResults.filter((s) => s.classification === "IMAGE_BROWSER_VALIDATED").length;
  const report = {
    brandId: brand.id,
    brandName: brand.name,
    brandSlug: brand.slug,
    mode: "CURRENT",
    base: BASE,
    required: REQUIRED_IMAGE_SLOTS.length,
    browserValidated,
    techValidDelivery: slotResults.filter((s) => s.techOk).length,
    ok: browserValidated === REQUIRED_IMAGE_SLOTS.length,
    slots: slotResults,
    at: new Date().toISOString(),
  };
  writeFileSync(join(shotDir, "CERT.json"), JSON.stringify(report, null, 2) + "\n");
  brandReports.push(report);
  console.error(
    `${report.ok ? "PASS" : "PARTIAL"} ${brand.slug} browserValidated=${browserValidated}/9 techDelivery=${report.techValidDelivery}/9`
  );
}

await browser.close();

const summary = {
  contract: "BE_FACTORY_IMAGE_INTEGRITY_BROWSER_CERT_CURRENT_V3",
  at: new Date().toISOString(),
  base: BASE,
  mergeCommit: "d6dd99ddc588835c67cf46c8a4089b19089878ec",
  brands: brandReports.map((b) => ({
    slug: b.brandSlug,
    ok: b.ok,
    browserValidated: b.browserValidated,
    techValidDelivery: b.techValidDelivery,
    error: b.error || null,
  })),
  fullyBrowserValidated: brandReports.filter((b) => b.ok).length,
  paidResearchSpendUsd: 0,
  note: "CURRENT mode only. IMAGE_BROWSER_VALIDATED requires DOM natural dimensions + non-ephemeral src. Exact-brand census verification remains separate.",
};
writeFileSync(join(OUT, "00-summary.json"), JSON.stringify(summary, null, 2) + "\n");
writeFileSync(join(ROOT, "reports/brand-explorer/portfolio-image-integrity-v3/70-browser-cert-current-summary.json"), JSON.stringify(summary, null, 2) + "\n");
console.log(JSON.stringify(summary, null, 2));
process.exit(summary.fullyBrowserValidated === brandReports.length ? 0 : 2);
