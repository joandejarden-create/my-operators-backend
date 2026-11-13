#!/usr/bin/env node
/**
 * Sheraton Guadalajara Expo — local founder STAGED package browser verification.
 * Does NOT publish. Does NOT mutate CURRENT / Bethesda.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-sheraton-guadalajara-expo-founder-review-v1",
);
const SHOTS = path.join(OUT, "screenshots");
const HOTEL_ID = "recCAGqkERSXgiku4";
const EXPECTED_FP =
  "60f2d7ef732ac9578e33e159988092b90f49ec8372b8b174c7630296240e6211";
const EXPECTED_VERSION = "gdi_pub_recCAGqkERSXgiku4_20261113_full_e2e_v1";
const BASE = process.env.GDI_LOCAL_BASE || "http://127.0.0.1:8080";
const PREVIEW_URL = `${BASE}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;

function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(obj, null, 2) + "\n");
}

function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

fs.mkdirSync(SHOTS, { recursive: true });

const sha = sh("git rev-parse HEAD") || "UNKNOWN";

const apiRes = await fetch(
  `${BASE}/api/group-demand-intelligence/hotels/${encodeURIComponent(
    HOTEL_ID,
  )}/opportunities?publicationPackage=1&surface=customer&preview=staged`,
);
const apiJson = await apiRes.json();
const packageIds = (apiJson.opportunities || []).map((o) => String(o.id));

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });

const consoleErrors = [];
page.on("console", (msg) => {
  if (msg.type() === "error") consoleErrors.push(msg.text());
});

await page.goto(PREVIEW_URL, { waitUntil: "domcontentloaded", timeout: 120000 });
await page.waitForTimeout(4000);

// Wait for inventory or error
await page.waitForFunction(
  () => {
    const t = document.body?.innerText || "";
    return (
      /Showing\s+\d+\s+of\s+\d+/i.test(t) ||
      /Sheraton Guadalajara Expo/i.test(t) ||
      /STAGED_PUBLICATION|Refusing to load|unavailable/i.test(t)
    );
  },
  null,
  { timeout: 120000 },
).catch(() => null);

await page.screenshot({
  path: path.join(SHOTS, "01-overview.png"),
  fullPage: true,
});

const overview = await page.evaluate(() => {
  const body = document.body?.innerText || "";
  const showing = body.match(/Showing\s+(\d+)\s+of\s+(\d+)/i);
  const hotel =
    document.querySelector(".gdi-property-bar__name, .gdi-hotel-name, h1, h2")
      ?.textContent || null;
  const modeBanner = document.querySelector(
    "[data-gdi-presentation-mode], #gdiPresentationModeBanner",
  );
  const buttons = Array.from(
    document.querySelectorAll("button.brand-card__more-btn[data-open]"),
  );
  const cards = buttons.map((btn, idx) => {
    const id = btn.getAttribute("data-open");
    const card = btn.closest(".brand-card") || btn.parentElement;
    const title =
      card?.querySelector(".brand-card__name")?.textContent?.trim() || null;
    const contact =
      card
        ?.querySelector(
          ".brand-card__contact, .gdi-card-contact, [data-gdi-contact]",
        )
        ?.textContent?.replace(/\s+/g, " ")
        .trim() || null;
    const action =
      Array.from(card?.querySelectorAll("*") || [])
        .map((el) => (el.textContent || "").trim())
        .find((t) =>
          /Build Account|Qualify Opportunity|Monitor|Pursue/i.test(t),
        ) || null;
    const svgs = card?.querySelectorAll("svg")?.length || 0;
    return { index: idx, id, title, contact, action, svgCount: svgs };
  });
  const bethesdaLeak = /Bethesda Marriott/i.test(body) && !/Sheraton/i.test(body);
  return {
    bodyHotelMatch: /Sheraton Guadalajara Expo/i.test(body),
    hotelLabel: hotel,
    showing: showing ? { shown: +showing[1], total: +showing[2] } : null,
    cardCount: cards.length,
    cards,
    modeBannerPresent: Boolean(modeBanner),
    modeBannerText: modeBanner?.textContent?.trim() || null,
    bethesdaSilentFallback: bethesdaLeak,
    hasResetView: /Reset View/i.test(body),
    hasMarketScope: /Market Scope/i.test(body),
    hasOpportunityType: /Opportunity Type/i.test(body),
    hasPriority: /Priority/i.test(body),
    hasSalesOpportunities: /Sales Opportunities/i.test(body),
    hasCsv: /CSV|Export/i.test(body),
    hasTour: /Tour|Guided/i.test(body),
  };
});

const drawerResults = [];
const idsToOpen = overview.cards.map((c) => c.id).filter(Boolean);
for (let i = 0; i < idsToOpen.length; i++) {
  const id = idsToOpen[i];
  const btn = page.locator(`button.brand-card__more-btn[data-open="${id}"]`);
  try {
    await btn.first().scrollIntoViewIfNeeded();
    await btn.first().click({ timeout: 10000 });
    await page.waitForTimeout(350);
    const drawer = await page.evaluate((oppId) => {
      const body = document.body?.innerText || "";
      const drawerEl =
        document.querySelector("#gdiDrawer[open], dialog#gdiDrawer, #gdiDrawer") ||
        document.querySelector("[data-gdi-drawer]");
      const open =
        Boolean(drawerEl?.hasAttribute?.("open")) ||
        drawerEl?.open === true ||
        /What is happening|Recommended next step|Who to contact|Supporting evidence/i.test(
          body,
        );
      const title =
        document.querySelector("#gdiDrawerTitle")?.textContent?.trim() || null;
      const drawerText =
        document.querySelector("#gdiDrawerBody")?.innerText?.replace(/\s+/g, " ").trim() ||
        "";
      return {
        opportunityId: oppId,
        open,
        title,
        hasWhat: /What is happening/i.test(drawerText + body),
        hasWhyHotel: /Why this hotel/i.test(drawerText + body),
        hasContact: /Who to contact/i.test(drawerText + body),
        hasNext: /Recommended next step/i.test(drawerText + body),
        hasEvidence: /Supporting evidence|http/i.test(drawerText + body),
        drawerTextLen: drawerText.length,
      };
    }, id);
    drawerResults.push({ ok: drawer.open, ...drawer });
    if (i === 0) {
      await page.screenshot({
        path: path.join(SHOTS, "02-first-drawer.png"),
        fullPage: false,
      });
    }
    // Close drawer
    const close = page.locator("#gdiDrawerClose, button:has-text('Close')");
    if (await close.count()) {
      await close.first().click({ timeout: 3000 }).catch(() => null);
    } else {
      await page.keyboard.press("Escape");
    }
    await page.waitForTimeout(150);
  } catch (err) {
    drawerResults.push({
      opportunityId: id,
      ok: false,
      error: String(err?.message || err),
    });
  }
}

// Filter / tour smoke
let filterTour = { resetView: null, tourSteps: null, search: null };
try {
  const reset = page.locator("button:has-text('Reset View'), a:has-text('Reset View')");
  if (await reset.count()) {
    await reset.first().click();
    await page.waitForTimeout(500);
    filterTour.resetView = true;
  }
  const tourBtn = page.locator(
    "button:has-text('Tour'), button:has-text('Guided'), [data-gdi-tour]",
  );
  if (await tourBtn.count()) {
    await tourBtn.first().click();
    await page.waitForTimeout(800);
    filterTour.tourOpened = true;
    await page.screenshot({
      path: path.join(SHOTS, "03-tour.png"),
      fullPage: false,
    });
    await page.keyboard.press("Escape");
  }
} catch (e) {
  filterTour.error = String(e?.message || e);
}

await browser.close();

const missingIds = packageIds.filter(
  (id) => !overview.cards.some((c) => c.id === id),
);
const drawerFail = drawerResults.filter((d) => !d.ok);
const fingerprintOk =
  apiJson?.publication?.contentFingerprintSha256 === EXPECTED_FP;
const versionOk = apiJson?.publication?.publicationVersion === EXPECTED_VERSION;

const report = {
  hotelId: HOTEL_ID,
  previewUrl: PREVIEW_URL,
  serverGitSha: sha,
  api: {
    status: apiRes.status,
    count: apiJson.count,
    listScope: apiJson.listScope,
    previewSource: apiJson.publication?.previewSource || null,
    fingerprint: apiJson.publication?.contentFingerprintSha256 || null,
    publicationVersion: apiJson.publication?.publicationVersion || null,
    fingerprintMatch: fingerprintOk,
    versionMatch: versionOk,
  },
  overview,
  drawers: {
    attempted: drawerResults.length,
    passed: drawerResults.filter((d) => d.ok).length,
    failed: drawerFail.length,
    failures: drawerFail.slice(0, 20),
  },
  inventoryParity: {
    apiCount: packageIds.length,
    cardCount: overview.cardCount,
    missingFromCards: missingIds,
    missingCount: missingIds.length,
  },
  filterTour,
  consoleErrors: consoleErrors.slice(0, 30),
  acceptance: {
    fingerprintUnchanged: fingerprintOk,
    all62Api: packageIds.length === 62,
    all62Cards: overview.cardCount === 62 && missingIds.length === 0,
    all62Drawers: drawerFail.length === 0 && drawerResults.length === 62,
    noModeBanner: overview.modeBannerPresent === false,
    noBethesdaFallback: overview.bethesdaSilentFallback === false,
    hotelNameVisible: overview.bodyHotelMatch === true,
  },
};

const ready =
  report.acceptance.fingerprintUnchanged &&
  report.acceptance.all62Api &&
  report.acceptance.all62Cards &&
  report.acceptance.all62Drawers &&
  report.acceptance.noBethesdaFallback &&
  report.acceptance.hotelNameVisible;

report.founderPreviewReady = ready;

writeJson("BROWSER_FOUNDER_PREVIEW_VERIFICATION.json", report);
console.log(
  JSON.stringify(
    {
      founderPreviewReady: ready,
      apiCount: packageIds.length,
      cardCount: overview.cardCount,
      drawersPassed: report.drawers.passed,
      fingerprintOk,
      previewUrl: PREVIEW_URL,
      out: OUT,
    },
    null,
    2,
  ),
);

process.exit(ready ? 0 : 2);
