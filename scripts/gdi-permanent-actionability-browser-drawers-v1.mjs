#!/usr/bin/env node
/**
 * Click every visible opportunity card (.brand-card--gdi-opp) and inspect drawers.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-permanent-commercial-actionability-gate-v1",
);
const HOTEL = "recCAGqkERSXgiku4";
const URL = `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL}&preview=staged`;
const FORBIDDEN = [
  /Notariado Mexicano/i,
  /CEO Summit 2026/i,
  /Nutrici[oó]n y Endocrinolog/i,
  /Sinergia Cient/i,
  /Yo Tambi[eé]n Juego/i,
  /Smart Technology Expo 2026/i,
];

const prev = JSON.parse(
  fs.readFileSync(path.join(OUT, "12-LOCAL_BROWSER_VERIFICATION.json"), "utf8"),
);
const drawerScan = JSON.parse(
  fs.readFileSync(path.join(OUT, "_drawer_api_scan.json"), "utf8"),
);
const uiInspect = JSON.parse(
  fs.readFileSync(path.join(OUT, "_ui_inspect.json"), "utf8"),
);

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
await page.goto(URL, { waitUntil: "networkidle", timeout: 90000 });
await page.waitForSelector(".brand-card--gdi-opp", { timeout: 30000 });
await page.waitForTimeout(1500);

const cardCount = await page.locator(".brand-card--gdi-opp").count();
const drawers = [];

for (let i = 0; i < cardCount; i++) {
  const card = page.locator(".brand-card--gdi-opp").nth(i);
  const title = (
    await card.locator(".brand-card__name").first().innerText().catch(() => "")
  ).trim();
  const pill = (
    await card.locator(".gdi-tile-pill, .gdi-pill").first().innerText().catch(() => "")
  ).trim();
  await card.scrollIntoViewIfNeeded();
  await card.click();
  await page.waitForTimeout(400);
  const drawerInfo = await page.evaluate(() => {
    const drawer = document.querySelector(
      ".gdi-drawer, [data-gdi-drawer], [role='dialog'], .gdi-detail-panel, .gdi-opportunity-drawer",
    );
    const text = (drawer?.innerText || document.body?.innerText || "").slice(
      0,
      600,
    );
    return {
      drawerFound: Boolean(drawer),
      hasDate:
        /Dates TBD|Ongoing|2026|2027|Nov|Dec|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct/i.test(
          text,
        ),
      forbidden:
        /Notariado Mexicano|CEO Summit 2026|Smart Technology Expo 2026|Endocrinolog/i.test(
          drawer?.innerText || "",
        ),
      snippet: text.slice(0, 220),
    };
  });
  drawers.push({ index: i, title, pill, ...drawerInfo });
  await page.keyboard.press("Escape");
  await page.waitForTimeout(120);
}

// Competitive filter
await page.evaluate(() => {
  const buttons = Array.from(
    document.querySelectorAll("button, [role='tab'], a, label, span"),
  );
  const el = buttons.find((b) =>
    /competitive market intelligence|^competitive$/i.test(
      (b.textContent || "").trim(),
    ),
  );
  if (el) el.click();
});
await page.waitForTimeout(1200);
await page.screenshot({
  path: path.join(OUT, "screenshots", "06-competitive-after-full-pass.png"),
  fullPage: true,
});
const competitiveBody = await page.evaluate(() => document.body?.innerText || "");
await browser.close();

const bodyForbidden = drawers.some((d) =>
  FORBIDDEN.some((re) => re.test(d.title || "")),
);
const result = {
  ...prev,
  ok:
    prev.checks?.fingerprintMatch === true &&
    cardCount === 38 &&
    drawerScan.rows.every((r) => r.detailStatus === 200) &&
    uiInspect.hasNotariado === false &&
    uiInspect.hasCEO === false &&
    uiInspect.hasSMNE === false &&
    uiInspect.hasSmartTech === false &&
    !bodyForbidden &&
    drawers.every((d) => d.forbidden !== true) &&
    /Hard Rock|Barcel|Hyatt|Fiesta Americana/i.test(competitiveBody),
  checks: {
    ...prev.checks,
    domCardCount: cardCount,
    brandCardGdiOppCount: cardCount,
    allDetailDrawersApiOk: drawerScan.rows.every((r) => r.detailStatus === 200),
    detailDrawerApiCount: drawerScan.rows.length,
    playwrightDrawersOpened: drawers.length,
    playwrightDrawerForbiddenAbsent: drawers.every((d) => d.forbidden !== true),
    uiForbiddenAbsent:
      uiInspect.hasNotariado === false &&
      uiInspect.hasCEO === false &&
      uiInspect.hasSMNE === false &&
      uiInspect.hasSmartTech === false,
    retainedSignals: {
      FIL: uiInspect.hasFIL === true,
      Foxconn: uiInspect.hasFoxconn === true,
      IMIN_or_HardRock: uiInspect.hasIMIN === true,
    },
  },
  cards: drawers.map((d) => ({
    index: d.index,
    title: d.title,
    pill: d.pill,
  })),
  drawers,
  drawerApiScanSummary: {
    count: drawerScan.rows.length,
    all200: drawerScan.rows.every((r) => r.detailStatus === 200),
  },
  verifiedAt: new Date().toISOString(),
};

fs.writeFileSync(
  path.join(OUT, "12-LOCAL_BROWSER_VERIFICATION.json"),
  `${JSON.stringify(result, null, 2)}\n`,
);

console.log(
  JSON.stringify(
    {
      ok: result.ok,
      cards: cardCount,
      drawersOpened: drawers.length,
      samplePills: drawers.slice(0, 8).map((d) => ({
        title: d.title.slice(0, 40),
        pill: d.pill,
      })),
      forbiddenUiAbsent: result.checks.uiForbiddenAbsent,
    },
    null,
    2,
  ),
);
if (!result.ok) process.exit(1);
