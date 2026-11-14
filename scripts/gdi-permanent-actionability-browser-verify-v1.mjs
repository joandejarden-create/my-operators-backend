#!/usr/bin/env node
/**
 * Browser-verify new Sheraton commercial-actionability staged package.
 * Inspects every retained card + opens drawers; asserts expired theses absent.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-permanent-commercial-actionability-gate-v1",
);
const HOTEL_ID = "recCAGqkERSXgiku4";
const META = JSON.parse(
  fs.readFileSync(path.join(OUT, "11-NEW_STAGED_PUBLICATION.json"), "utf8"),
);
const EXPECT_FP = META.contentFingerprintSha256;
const EXPECT_COUNT = META.opportunityCount;
const EXPECT_VERSION = META.publicationVersion;
const URL = `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;

const FORBIDDEN_TITLES = [
  /Notariado Mexicano/i,
  /CEO Summit 2026/i,
  /Sociedad Mexicana de Nutrici[oó]n y Endocrinolog/i,
  /Sinergia Cient[ií]fica/i,
  /Yo Tambi[eé]n Juego Rumbo/i,
  /Smart Technology Expo 2026/i,
  /Ciclo de Conferencias M[eé]dicas \(CICOM\)/i,
];

fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const result = {
  url: URL,
  expectedFingerprint: EXPECT_FP,
  expectedCount: EXPECT_COUNT,
  ok: false,
  checks: {},
  cards: [],
  drawers: [],
  errors: [],
};

try {
  await page.goto(URL, { waitUntil: "networkidle", timeout: 90000 });
  await page.waitForTimeout(3000);

  const api = await page.evaluate(async (hotelId) => {
    const r = await fetch(
      `/api/group-demand-intelligence/hotels/${hotelId}/opportunities?publicationPackage=1&surface=customer&preview=staged`,
    );
    return r.json();
  }, HOTEL_ID);

  const ops = Array.isArray(api?.opportunities) ? api.opportunities : [];
  result.checks.apiFingerprint =
    api?.publication?.contentFingerprintSha256 || null;
  result.checks.fingerprintMatch =
    result.checks.apiFingerprint === EXPECT_FP;
  result.checks.hotelCount = api?.publication?.inventoryCounts?.hotel ?? null;
  result.checks.competitiveCount =
    api?.publication?.inventoryCounts?.competitive ?? null;
  result.checks.allCount = api?.publication?.inventoryCounts?.all ?? null;
  result.checks.apiOpportunityCount = ops.length;
  result.checks.countMatch = ops.length === EXPECT_COUNT;

  const forbiddenHits = ops
    .map((o) => o.displayTitle || o.title || "")
    .filter((t) => FORBIDDEN_TITLES.some((re) => re.test(t)));
  result.checks.forbiddenTitlesAbsent = forbiddenHits.length === 0;
  result.checks.forbiddenHits = forbiddenHits;

  // Contaminated research dates should not appear as Nov 5/10 TBD pills for expired
  result.checks.sampleDatePills = ops.slice(0, 12).map((o) => ({
    id: o.id,
    title: (o.displayTitle || o.title || "").slice(0, 60),
    pill: o.datePillDisplayLabel || o.eventDateDisplay || null,
    start: o.eventStartDate || o.tileEventStartDate || null,
    state: o.commercialActionabilityStateV1 || null,
    hotel: o.hotelMarketScopeEligible === true,
    competitive: o.competitiveMarketScopeEligible === true,
  }));

  // DOM cards
  const cardInfo = await page.evaluate(() => {
    const nodes = Array.from(
      document.querySelectorAll(
        "[data-opportunity-id], .gdi-opportunity-card, .gdi-card",
      ),
    );
    return nodes.map((n, i) => ({
      index: i,
      id: n.getAttribute("data-opportunity-id") || null,
      text: (n.innerText || "").slice(0, 220),
    }));
  });
  result.checks.domCardCount = cardInfo.length;
  result.cards = cardInfo;

  const bodyText = await page.evaluate(() => document.body?.innerText || "");
  result.checks.domForbiddenAbsent = !FORBIDDEN_TITLES.some((re) =>
    re.test(bodyText),
  );

  await page.screenshot({
    path: path.join(OUT, "screenshots", "01-hotel-market-overview.png"),
    fullPage: true,
  });

  // Open up to first 8 drawers by clicking cards
  const drawerResults = [];
  const clickable = await page.$$(
    "[data-opportunity-id], .gdi-opportunity-card, .gdi-card",
  );
  const limit = Math.min(clickable.length, 8);
  for (let i = 0; i < limit; i++) {
    try {
      await clickable[i].click({ timeout: 3000 });
      await page.waitForTimeout(700);
      const drawer = await page.evaluate(() => {
        const d =
          document.querySelector(
            ".gdi-drawer, [data-gdi-drawer], .opportunity-drawer, aside[open], .drawer",
          ) || document.querySelector("[role='dialog']");
        const text = (d?.innerText || document.body?.innerText || "").slice(
          0,
          800,
        );
        return {
          found: Boolean(d),
          hasDate: /Dates TBD|Ongoing|2026|2027|Nov|Dec|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct/i.test(
            text,
          ),
          snippet: text.slice(0, 280),
        };
      });
      drawerResults.push({ index: i, ...drawer });
      await page.keyboard.press("Escape");
      await page.waitForTimeout(300);
    } catch (err) {
      drawerResults.push({ index: i, error: String(err.message || err) });
    }
  }
  result.drawers = drawerResults;

  // Competitive scope
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
  await page.waitForTimeout(1000);
  await page.screenshot({
    path: path.join(OUT, "screenshots", "02-competitive-market.png"),
    fullPage: true,
  });
  const competitiveDom = await page.evaluate(() => {
    const body = document.body?.innerText || "";
    return {
      hardRock: /Hard Rock/i.test(body),
      barcelo: /Barcel[oó]/i.test(body),
      hyatt: /Hyatt/i.test(body),
      fiesta: /Fiesta Americana/i.test(body),
      notariado: /Notariado Mexicano/i.test(body),
      ceoSummit: /CEO Summit 2026/i.test(body),
    };
  });
  result.checks.competitiveDom = competitiveDom;

  result.ok =
    result.checks.fingerprintMatch === true &&
    result.checks.countMatch === true &&
    result.checks.forbiddenTitlesAbsent === true &&
    result.checks.domForbiddenAbsent === true &&
    result.checks.hotelCount === 34 &&
    result.checks.competitiveCount === 4 &&
    competitiveDom.notariado !== true &&
    competitiveDom.ceoSummit !== true;
} catch (err) {
  result.errors.push(String(err.message || err));
  result.ok = false;
} finally {
  await browser.close();
}

fs.writeFileSync(
  path.join(OUT, "12-LOCAL_BROWSER_VERIFICATION.json"),
  `${JSON.stringify(result, null, 2)}\n`,
);
console.log(
  JSON.stringify(
    {
      ok: result.ok,
      fingerprintMatch: result.checks.fingerprintMatch,
      counts: {
        all: result.checks.allCount,
        hotel: result.checks.hotelCount,
        competitive: result.checks.competitiveCount,
      },
      forbiddenAbsent: result.checks.forbiddenTitlesAbsent,
      drawersOpened: result.drawers.length,
      errors: result.errors,
    },
    null,
    2,
  ),
);
if (!result.ok) process.exit(1);
