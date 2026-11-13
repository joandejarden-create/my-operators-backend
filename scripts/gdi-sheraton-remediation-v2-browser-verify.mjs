#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-sheraton-guadalajara-commercial-e2e-remediation-v2",
);
const SHOTS = path.join(OUT, "screenshots");
const HOTEL_ID = "recCAGqkERSXgiku4";
const BASE = process.env.GDI_LOCAL_BASE || "http://127.0.0.1:8080";
const PREVIEW = `${BASE}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;
const meta = JSON.parse(
  fs.readFileSync(path.join(OUT, "14-NEW_STAGED_PUBLICATION.json"), "utf8"),
);
const EXPECTED_FP = meta.fingerprint;
const EXPECTED_COUNT = meta.opportunityCount;

fs.mkdirSync(SHOTS, { recursive: true });

const api = await (
  await fetch(
    `${BASE}/api/group-demand-intelligence/hotels/${encodeURIComponent(
      HOTEL_ID,
    )}/opportunities?publicationPackage=1&surface=customer&preview=staged`,
  )
).json();

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
await page.goto(PREVIEW, { waitUntil: "domcontentloaded", timeout: 120000 });
await page.waitForTimeout(4500);
await page.waitForFunction(
  (n) => new RegExp(`Showing\\s+${n}\\s+of\\s+${n}`, "i").test(document.body?.innerText || ""),
  EXPECTED_COUNT,
  { timeout: 120000 },
).catch(() => null);

await page.screenshot({ path: path.join(SHOTS, "01-overview.png"), fullPage: true });

const overview = await page.evaluate(() => {
  const body = document.body?.innerText || "";
  const showing = body.match(/Showing\s+(\d+)\s+of\s+(\d+)/i);
  const cards = Array.from(
    document.querySelectorAll("button.brand-card__more-btn[data-open]"),
  ).map((btn, idx) => ({
    index: idx,
    id: btn.getAttribute("data-open"),
    title:
      btn.closest(".brand-card")?.querySelector(".brand-card__name")
        ?.textContent?.trim() || null,
  }));
  const hotelScope = body.match(/Hotel\s*\(?\s*(\d+)/i);
  const competitiveScope = body.match(/Competitive\s*\(?\s*(\d+)/i);
  const actions = {};
  for (const label of [
    "Qualify Opportunity",
    "Build Account",
    "Build Venue Partnership",
    "Monitor",
    "Pursue Catering",
    "Engage Planner",
  ]) {
    const re = new RegExp(label, "gi");
    const m = body.match(re);
    if (m) actions[label] = m.length;
  }
  return {
    showing: showing ? { shown: +showing[1], total: +showing[2] } : null,
    cardCount: cards.length,
    cards,
    hotelName: /Sheraton Guadalajara Expo/i.test(body),
    bethesdaLeak: /Bethesda Marriott/i.test(body) && !/Sheraton/i.test(body),
    modeBanner: Boolean(
      document.querySelector("[data-gdi-presentation-mode], #gdiPresentationModeBanner"),
    ),
    marketScopeHotel: hotelScope ? +hotelScope[1] : null,
    marketScopeCompetitive: competitiveScope ? +competitiveScope[1] : null,
    actionsSeen: actions,
    hasSalesFilters: /Hotel Unconfirmed|Pursue Now|Future Groups|Partnerships|Guest Rooms|Catering/i.test(
      body,
    ),
  };
});

const drawerFails = [];
for (const c of overview.cards) {
  try {
    const btn = page.locator(`button.brand-card__more-btn[data-open="${c.id}"]`);
    await btn.first().scrollIntoViewIfNeeded();
    await btn.first().click({ timeout: 10000 });
    await page.waitForFunction(
      () => {
        const drawer = document.querySelector("#gdiDrawer");
        const open =
          drawer?.hasAttribute("open") ||
          drawer?.open === true ||
          (document.querySelector("#gdiDrawerBody")?.innerText || "").length > 40;
        const t = document.querySelector("#gdiDrawerBody")?.innerText || "";
        return open && /What is happening|Recommended next step|Who to contact|Potential hotel business/i.test(t);
      },
      null,
      { timeout: 8000 },
    );
    const close = page.locator("#gdiDrawerClose");
    if (await close.count()) await close.first().click({ timeout: 3000 });
    else await page.keyboard.press("Escape");
    await page.waitForTimeout(120);
  } catch (e) {
    drawerFails.push(c.id);
    await page.keyboard.press("Escape").catch(() => null);
  }
}
if (overview.cards[0]) {
  await page.locator(`button.brand-card__more-btn[data-open="${overview.cards[0].id}"]`).first().click().catch(() => null);
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(SHOTS, "02-drawer.png"), fullPage: false });
}

await browser.close();

const report = {
  previewUrl: PREVIEW,
  serverSha: (() => {
    try {
      return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
    } catch {
      return null;
    }
  })(),
  api: {
    count: api.count,
    fingerprint: api.publication?.contentFingerprintSha256,
    previewSource: api.publication?.previewSource,
    version: api.publication?.publicationVersion,
    fingerprintMatch: api.publication?.contentFingerprintSha256 === EXPECTED_FP,
  },
  overview,
  drawers: {
    attempted: overview.cards.length,
    failed: drawerFails.length,
    failureIds: drawerFails.slice(0, 20),
  },
  acceptance: {
    newPackageLoaded: api.publication?.contentFingerprintSha256 === EXPECTED_FP,
    countMatch: overview.cardCount === EXPECTED_COUNT && api.count === EXPECTED_COUNT,
    allDrawers: drawerFails.length === 0,
    hotelVisible: overview.hotelName === true,
    noBethesdaFallback: overview.bethesdaLeak === false,
    noModeBanner: overview.modeBanner === false,
    marketScopeHotelNonZero: (overview.marketScopeHotel || 0) > 0,
    salesFiltersPresent: overview.hasSalesFilters === true,
  },
};

report.founderPreviewReady = Object.values(report.acceptance).every(Boolean);
fs.writeFileSync(
  path.join(OUT, "13-LOCAL_FOUNDER_PREVIEW_VERIFICATION.json"),
  JSON.stringify(report, null, 2) + "\n",
);
console.log(
  JSON.stringify(
    {
      founderPreviewReady: report.founderPreviewReady,
      acceptance: report.acceptance,
      cardCount: overview.cardCount,
      marketScopeHotel: overview.marketScopeHotel,
      actionsSeen: overview.actionsSeen,
    },
    null,
    2,
  ),
);
process.exit(report.founderPreviewReady ? 0 : 2);
