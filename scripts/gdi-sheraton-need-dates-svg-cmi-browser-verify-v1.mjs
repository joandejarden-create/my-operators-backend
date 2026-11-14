#!/usr/bin/env node
/**
 * Browser verify Sheraton need-dates / SVG / CMI staged founder preview.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-sheraton-need-dates-svg-cmi-v1");
const HOTEL_ID = "recCAGqkERSXgiku4";
const EXPECT_FP =
  "9a75e3e993ce4df03b56b11a9ca4b49473e403f4b913adddc7797790846895f6";
const URL = `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;

fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const result = {
  url: URL,
  expectedFingerprint: EXPECT_FP,
  ok: false,
  checks: {},
  errors: [],
};

try {
  await page.goto(URL, { waitUntil: "networkidle", timeout: 60000 });
  await page.waitForTimeout(2500);

  const api = await page.evaluate(async (hotelId) => {
    const r = await fetch(
      `/api/group-demand-intelligence/hotels/${hotelId}/opportunities?publicationPackage=1&surface=customer&preview=staged`,
    );
    return r.json();
  }, HOTEL_ID);

  result.checks.apiFingerprint = api?.publication?.contentFingerprintSha256 || null;
  result.checks.fingerprintMatch =
    result.checks.apiFingerprint === EXPECT_FP;
  result.checks.hotelCount = api?.publication?.inventoryCounts?.hotel ?? null;
  result.checks.competitiveCount =
    api?.publication?.inventoryCounts?.competitive ?? null;
  result.checks.needStatus = api?.hotelNeedDateConfigV1?.status || null;
  result.checks.needLabel = api?.hotelNeedDateConfigV1?.statusLabel || null;
  result.checks.estimatedPeriods =
    api?.hotelNeedDateConfigV1?.estimatedPeriodCount ?? null;

  const ops = Array.isArray(api?.opportunities) ? api.opportunities : [];
  result.checks.guestActive = ops.filter(
    (o) => o.revenueStreamIcons?.guestrooms === "active",
  ).length;
  result.checks.caterActive = ops.filter(
    (o) => o.revenueStreamIcons?.eventFnb === "active",
  ).length;
  result.checks.needMatches = ops.filter((o) => o.needDateMatchV1).length;

  const dom = await page.evaluate(() => {
    const needLabel =
      document.querySelector("[data-hotel-need-date-status]")?.getAttribute(
        "data-hotel-need-date-status",
      ) ||
      document.querySelector(".gdi-hotel-need-date-status__text")?.textContent ||
      null;
    const needText =
      document.querySelector(".gdi-hotel-need-date-status__text")?.textContent ||
      null;
    const cards = document.querySelectorAll(
      ".gdi-opportunity-card, [data-opportunity-id], .gdi-card",
    ).length;
    const guestSvgs = document.querySelectorAll(
      '[data-revenue-stream="guestrooms"][data-state="active"], .gdi-revenue-icon--guestrooms.gdi-revenue-icon--active, svg[data-stream="guestrooms"].is-active',
    ).length;
    const diagnostic = !!document.querySelector(
      ".gdi-diagnostic-banner, [data-gdi-diagnostic]",
    );
    const bodyText = document.body?.innerText || "";
    return {
      needAttr: needLabel,
      needText,
      cards,
      guestSvgs,
      diagnostic,
      hasProvisionalCopy: /Provisional Need Dates|Awaiting Hotel Confirmation/i.test(
        bodyText,
      ),
      hasHotelConfirmedLabel: /\bHotel Confirmed\b/i.test(bodyText),
      hotelFilterText: (bodyText.match(/Hotel\s*\((\d+)\)/) || [])[1] || null,
      competitiveFilterText:
        (bodyText.match(/Competitive[^\n]*\((\d+)\)/i) || [])[1] || null,
    };
  });
  result.checks.dom = dom;

  await page.screenshot({
    path: path.join(OUT, "screenshots", "01-overview.png"),
    fullPage: false,
  });

  result.ok =
    result.checks.fingerprintMatch === true &&
    result.checks.hotelCount === 43 &&
    result.checks.competitiveCount === 0 &&
    result.checks.guestActive > 0 &&
    result.checks.needStatus === "MARKET_ESTIMATED" &&
    /Provisional Need Dates/i.test(String(result.checks.needLabel || "")) &&
    dom.diagnostic === false;
} catch (e) {
  result.errors.push(String(e?.message || e));
} finally {
  await browser.close();
}

fs.writeFileSync(
  path.join(OUT, "13-LOCAL_BROWSER_VERIFICATION.json"),
  `${JSON.stringify(result, null, 2)}\n`,
);
console.log(JSON.stringify(result, null, 2));
process.exit(result.ok ? 0 : 2);
