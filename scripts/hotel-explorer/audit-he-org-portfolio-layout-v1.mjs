#!/usr/bin/env node
/**
 * Browser visual certification for Organization & Portfolio layout.
 * Fails on overlapping cells, clipped badges, zero first/last padding,
 * missing Economic Sponsor label, or Key People / Contact Intelligence mismatch.
 *
 * Usage:
 *   node scripts/hotel-explorer/audit-he-org-portfolio-layout-v1.mjs
 *   HE_BASE_URL=http://127.0.0.1:8091 node scripts/hotel-explorer/audit-he-org-portfolio-layout-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const BASE = process.env.HE_BASE_URL || "http://127.0.0.1:8091";
const HOTEL = process.env.HE_HOTEL_ID || "recLuxvwwxID7U2B8";
const OUT_DIR = path.join(ROOT, "reports", "hotel-explorer");
fs.mkdirSync(OUT_DIR, { recursive: true });

async function launch() {
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return await chromium.launch({ channel: "chrome", headless: true });
  }
}

function fail(msg, details) {
  console.error(JSON.stringify({ ok: false, error: msg, details }, null, 2));
  process.exit(1);
}

const browser = await launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 960 } });
await page.goto(`${BASE}/deal-capture-radar-with-ranked-list.html?cb=${Date.now()}`, {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, {
  timeout: 30000,
});
await page.evaluate(async (hotelId) => {
  const hotel = { id: hotelId, recordId: hotelId, name: "Bethesda Marriott" };
  const maybe = HotelExplorer.open(hotel, { tab: "organization", requestedHotelId: hotelId });
  if (maybe && typeof maybe.then === "function") await maybe;
}, HOTEL);
await page.waitForSelector('[data-hex-owner-portfolio="1"]', { timeout: 60000 });
await page.waitForTimeout(2500);
// Allow Key People CI reconcile
await page.waitForFunction(
  () => {
    const host = document.querySelector("[data-hex-key-people-host]");
    if (!host) return false;
    return !/Loading contacts/i.test(host.textContent || "");
  },
  null,
  { timeout: 45000 }
).catch(() => {});

const desktop = await page.evaluate(() => {
  function overlaps(a, b) {
    return !(
      a.right <= b.left ||
      a.left >= b.right ||
      a.bottom <= b.top ||
      a.top >= b.bottom
    );
  }
  const portfolio = document.querySelector(".hiw-table--portfolio");
  const wrap = portfolio && portfolio.closest(".hiw-table-wrap, .dl-table-wrap");
  const ths = portfolio ? Array.from(portfolio.querySelectorAll("thead th")) : [];
  const firstRow = portfolio ? portfolio.querySelector("tbody tr") : null;
  const tds = firstRow ? Array.from(firstRow.children) : [];
  const cellBoxes = tds.map((td) => td.getBoundingClientRect());
  let cellOverlap = false;
  for (let i = 0; i < cellBoxes.length; i++) {
    for (let j = i + 1; j < cellBoxes.length; j++) {
      if (overlaps(cellBoxes[i], cellBoxes[j])) cellOverlap = true;
    }
  }
  const headerBoxes = ths.map((th) => th.getBoundingClientRect());
  let headerDataMisalign = false;
  for (let i = 0; i < Math.min(headerBoxes.length, cellBoxes.length); i++) {
    if (Math.abs(headerBoxes[i].left - cellBoxes[i].left) > 3) headerDataMisalign = true;
  }
  const firstTh = ths[0];
  const firstTd = tds[0];
  const lastTh = ths[ths.length - 1];
  const lastTd = tds[tds.length - 1];
  const pad = (el, side) => {
    if (!el) return 0;
    return parseFloat(getComputedStyle(el)["padding" + side]) || 0;
  };
  const badges = Array.from(document.querySelectorAll(".hex-owner-portfolio .hiw-status"));
  const clippedBadge = badges.some((b) => {
    const r = b.getBoundingClientRect();
    const parent = b.parentElement && b.parentElement.getBoundingClientRect();
    if (!parent) return false;
    return r.right > parent.right + 1 || r.width < 8;
  });
  const text = (document.querySelector("[data-hex-owner-portfolio]") || document.body)
    .textContent || "";
  const keyHost = document.querySelector("[data-hex-key-people-host]");
  const keyText = (keyHost && keyHost.textContent) || "";
  const sections = Array.from(document.querySelectorAll("[data-hex-org-section]")).map((el) => {
    const r = el.getBoundingClientRect();
    return { id: el.getAttribute("data-hex-org-section"), left: Math.round(r.left), width: Math.round(r.width) };
  });
  const sectionLefts = sections.map((s) => s.left);
  const aligned =
    sectionLefts.length <= 1 ||
    sectionLefts.every((l) => Math.abs(l - sectionLefts[0]) <= 2);

  return {
    hasPortfolioTable: !!portfolio,
    colCount: ths.length,
    headers: ths.map((th) => (th.textContent || "").trim().toUpperCase()),
    firstPaddingLeft: Math.min(pad(firstTh, "Left"), pad(firstTd, "Left")),
    lastPaddingRight: Math.min(pad(lastTh, "Right"), pad(lastTd, "Right")),
    cellOverlap,
    headerDataMisalign,
    clippedBadge,
    hasEconomicSponsor: /Economic Sponsor/i.test(text),
    hasEconomicOwnerLabel: /\bECONOMIC OWNER\b/.test(text) && !/Economic Sponsor/i.test(text),
    hasInternalStrategic: /Executive assessment synthesized from verified/i.test(text),
    hasSurfeCopy: /Surfe/i.test(text),
    keyPeopleNames: ["Jeff Mullen", "Katherine Button", "Meinrad Lang", "Jenny Pyne", "Patrick Reece"].filter(
      (n) => keyText.includes(n)
    ),
    keyPeopleCount: (keyText.match(/[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+)+/g) || []).length,
    sectionAligned: aligned,
    sections,
    canScrollX: wrap ? wrap.scrollWidth > wrap.clientWidth - 1 || (portfolio && portfolio.scrollWidth >= 1100) : false,
    sampleRel: (tds[5] && tds[5].textContent || "").replace(/\s+/g, " ").trim(),
  };
});

await page.screenshot({
  path: path.join(OUT_DIR, "he-org-portfolio-desktop-recLuxvwwxID7U2B8.png"),
  fullPage: false,
});

await page.setViewportSize({ width: 1100, height: 900 });
await page.waitForTimeout(500);
const narrow = await page.evaluate(() => {
  const wrap = document.querySelector(".hiw-table--portfolio")?.closest(".hiw-table-wrap, .dl-table-wrap");
  const portfolio = document.querySelector(".hiw-table--portfolio");
  return {
    overflowX: wrap ? wrap.scrollWidth > wrap.clientWidth + 2 : false,
    tableMinOk: portfolio ? portfolio.getBoundingClientRect().width >= 900 || (wrap && wrap.scrollWidth >= 1100) : false,
  };
});
await page.screenshot({
  path: path.join(OUT_DIR, "he-org-portfolio-narrow-recLuxvwwxID7U2B8.png"),
  fullPage: false,
});

await browser.close();

const errors = [];
if (!desktop.hasPortfolioTable) errors.push("missing_portfolio_table");
if (desktop.colCount !== 7) errors.push("expected_7_columns_got_" + desktop.colCount);
if (desktop.firstPaddingLeft < 12) errors.push("first_column_padding_lt_12");
if (desktop.lastPaddingRight < 12) errors.push("last_column_padding_lt_12");
if (desktop.cellOverlap) errors.push("cell_overlap");
if (desktop.headerDataMisalign) errors.push("header_data_misalign");
if (desktop.clippedBadge) errors.push("status_badge_clipped");
if (!desktop.hasEconomicSponsor) errors.push("missing_economic_sponsor_label");
if (desktop.hasEconomicOwnerLabel) errors.push("raw_economic_owner_label");
if (desktop.hasInternalStrategic) errors.push("internal_strategic_lede");
if (desktop.hasSurfeCopy) errors.push("surfe_copy_present");
if (desktop.keyPeopleNames.length < 3) errors.push("key_people_ci_reconcile_failed");
if (!desktop.sectionAligned) errors.push("section_alignment_failed");
if (!narrow.overflowX && !narrow.tableMinOk) errors.push("narrow_scroll_or_minwidth_failed");

const report = { ok: errors.length === 0, base: BASE, hotel: HOTEL, desktop, narrow, errors };
fs.writeFileSync(
  path.join(OUT_DIR, "he-org-portfolio-layout-audit-recLuxvwwxID7U2B8.json"),
  JSON.stringify(report, null, 2)
);
console.log(JSON.stringify(report, null, 2));
if (errors.length) fail("org_portfolio_layout_cert_failed", errors);
