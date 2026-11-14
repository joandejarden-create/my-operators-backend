#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-sheraton-paid-full-e2e-discovery-v3");
const HOTEL = "recCAGqkERSXgiku4";
const BASE = process.env.GDI_LOCAL_BASE || "http://127.0.0.1:8080";
const meta = JSON.parse(
  fs.readFileSync(path.join(OUT, "13-NEW_STAGED_PUBLICATION.json"), "utf8"),
);
const EXPECTED_FP = meta.fingerprint;
const EXPECTED_COUNT = meta.opportunityCount;
const PREVIEW = `${BASE}/group-demand-intelligence.html?hotelId=${HOTEL}&preview=staged`;
fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

const api = await (
  await fetch(
    `${BASE}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities?publicationPackage=1&surface=customer&preview=staged`,
  )
).json();

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
await page.goto(PREVIEW, { waitUntil: "domcontentloaded", timeout: 120000 });
await page.waitForTimeout(5000);
await page
  .waitForFunction(
    (n) =>
      new RegExp(`Showing\\s+${n}\\s+of\\s+${n}`, "i").test(
        document.body?.innerText || "",
      ),
    EXPECTED_COUNT,
    { timeout: 120000 },
  )
  .catch(() => null);
await page.screenshot({
  path: path.join(OUT, "screenshots/01-overview.png"),
  fullPage: true,
});
const overview = await page.evaluate(() => {
  const body = document.body?.innerText || "";
  const showing = body.match(/Showing\s+(\d+)\s+of\s+(\d+)/i);
  const cards = Array.from(
    document.querySelectorAll("button.brand-card__more-btn[data-open]"),
  ).map((btn, idx) => ({ index: idx, id: btn.getAttribute("data-open") }));
  const hotelScope = body.match(/Hotel\s*\(?\s*(\d+)/i);
  return {
    showing: showing ? { shown: +showing[1], total: +showing[2] } : null,
    cardCount: cards.length,
    cards,
    hotelName: /Sheraton Guadalajara Expo/i.test(body),
    bethesdaLeak: /Bethesda Marriott/i.test(body) && !/Sheraton/i.test(body),
    modeBanner: Boolean(
      document.querySelector(
        "[data-gdi-presentation-mode], #gdiPresentationModeBanner",
      ),
    ),
    marketScopeHotel: hotelScope ? +hotelScope[1] : null,
    hasSalesFilters:
      /Hotel Unconfirmed|Future Groups|Partnerships|Guest Rooms|Catering/i.test(
        body,
      ),
  };
});
const fails = [];
for (const c of overview.cards) {
  try {
    const btn = page.locator(
      `button.brand-card__more-btn[data-open="${c.id}"]`,
    );
    await btn.first().scrollIntoViewIfNeeded();
    await btn.first().click({ timeout: 10000 });
    await page.waitForFunction(
      () => {
        const t = document.querySelector("#gdiDrawerBody")?.innerText || "";
        return (
          t.length > 40 &&
          /What is happening|Recommended next step|Who to contact|Potential hotel business/i.test(
            t,
          )
        );
      },
      null,
      { timeout: 8000 },
    );
    const close = page.locator("#gdiDrawerClose");
    if (await close.count()) await close.first().click({ timeout: 3000 });
    else await page.keyboard.press("Escape");
    await page.waitForTimeout(80);
  } catch {
    fails.push(c.id);
    await page.keyboard.press("Escape").catch(() => null);
  }
}
await browser.close();
const report = {
  previewUrl: PREVIEW,
  api: {
    count: api.count,
    fingerprint: api.publication?.contentFingerprintSha256,
    fingerprintMatch:
      api.publication?.contentFingerprintSha256 === EXPECTED_FP,
    version: api.publication?.publicationVersion,
  },
  overview,
  drawers: { attempted: overview.cards.length, failed: fails.length, failureIds: fails.slice(0, 15) },
  acceptance: {
    newPackageLoaded:
      api.publication?.contentFingerprintSha256 === EXPECTED_FP,
    countMatch:
      overview.cardCount === EXPECTED_COUNT && api.count === EXPECTED_COUNT,
    allDrawers: fails.length === 0,
    hotelVisible: overview.hotelName === true,
    noBethesdaFallback: overview.bethesdaLeak === false,
    noModeBanner: overview.modeBanner === false,
    marketScopeHotelNonZero: (overview.marketScopeHotel || 0) > 0,
    salesFiltersPresent: overview.hasSalesFilters === true,
  },
};
report.founderPreviewReady = Object.values(report.acceptance).every(Boolean);
fs.writeFileSync(
  path.join(OUT, "14-LOCAL_BROWSER_VERIFICATION.json"),
  JSON.stringify(report, null, 2) + "\n",
);
console.log(
  JSON.stringify(
    {
      founderPreviewReady: report.founderPreviewReady,
      acceptance: report.acceptance,
      cardCount: overview.cardCount,
    },
    null,
    2,
  ),
);
process.exit(report.founderPreviewReady ? 0 : 2);
