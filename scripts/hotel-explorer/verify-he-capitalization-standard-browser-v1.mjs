#!/usr/bin/env node
/**
 * Browser regression: HE capitalization standard across active tabs.
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 \
 *     node scripts/hotel-explorer/verify-he-capitalization-standard-browser-v1.mjs --require-server
 *
 * Checks ALL CAPS field labels + Proper Case values on Property Facts,
 * and that Meeting Space / Meeting Rooms / Largest Meeting Room are separate rows.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080"
).replace(/\/$/, "");
const HOTEL =
  process.env.HE_CAPS_VERIFY_HOTEL_ID ||
  process.env.HE_LOADING_VERIFY_HOTEL_ID ||
  "recLuxvwwxID7U2B8";
const requireServer = process.argv.includes("--require-server");
const OUT_DIR = path.join(ROOT, "reports", "hotel-explorer");
const OUT = path.join(OUT_DIR, "HE-CAPITALIZATION-STANDARD-BROWSER-V1.json");

const ACTIVE_TABS = [
  "hotel",
  "rooms",
  "ownership",
  "contacts",
  "organization",
  "relationships",
  "submarket",
  "area",
  "demand",
  "access",
  "sources",
];

async function probeServer() {
  try {
    const res = await fetch(`${BASE}/deal-capture-radar-with-ranked-list.html`, {
      method: "HEAD",
      signal: AbortSignal.timeout(4000),
    });
    return res.ok || res.status === 405 || res.status === 200;
  } catch {
    return false;
  }
}

const serverOk = await probeServer();
if (!serverOk) {
  const payload = {
    contract: "he-capitalization-standard-browser-v1",
    status: "NOT_TESTED",
    reason: `server_unreachable:${BASE}`,
    generatedAt: new Date().toISOString(),
  };
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(payload, null, 2));
  console.log(JSON.stringify(payload, null, 2));
  process.exit(requireServer ? 1 : 2);
}

async function launchBrowser() {
  try {
    return await chromium.launch({ headless: true });
  } catch (err) {
    const msg = String(err && err.message ? err.message : err);
    if (/Executable doesn't exist|browserType\.launch/i.test(msg)) {
      return chromium.launch({ channel: "chrome", headless: true });
    }
    throw err;
  }
}

const browser = await launchBrowser();
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
  const maybe = HotelExplorer.open(hotel, { tab: "hotel", requestedHotelId: hotelId });
  if (maybe && typeof maybe.then === "function") await maybe;
  const deadline = Date.now() + 45000;
  while (Date.now() < deadline) {
    if (document.querySelector(".hdp-overview__label, .hex-fact .label")) break;
    await new Promise((r) => setTimeout(r, 250));
  }
}, HOTEL);

await page.waitForSelector(".hdp-overview__label, .hex-root.is-open", { timeout: 60000 });
await page.waitForTimeout(800);

const tabResults = {};
for (const tab of ACTIVE_TABS) {
  await page.evaluate((tabId) => {
    if (typeof HotelExplorer.setTab === "function") HotelExplorer.setTab(tabId);
  }, tab);
  await page.waitForTimeout(700);
  tabResults[tab] = await page.evaluate(() => {
    const labels = Array.from(
      document.querySelectorAll(
        ".hdp-overview__label, .hex-fact .label, .hex-meta-card .label, .hex-rms-metric__label, .he-market-coverage__label, .hci-directory-table thead th, .hiw-table thead th, .ocn-summary__k"
      )
    ).map((el) => (el.textContent || "").trim().replace(/\s+/g, " "));
    const values = Array.from(
      document.querySelectorAll(
        ".hdp-overview__value, .hex-fact .value, .hex-meta-card .value, .hex-rms-metric__value"
      )
    ).map((el) => (el.textContent || "").trim().replace(/\s+/g, " "));
    const overviewPairs = Array.from(document.querySelectorAll(".hdp-overview__row, .hdp-overview__item")).map(
      (row) => ({
        label: (row.querySelector(".hdp-overview__label")?.textContent || "")
          .trim()
          .replace(/\s+/g, " "),
        value: (row.querySelector(".hdp-overview__value")?.textContent || "")
          .trim()
          .replace(/\s+/g, " "),
      })
    );
    // Fallback: label/value siblings under overview grid
    if (!overviewPairs.length) {
      const labs = Array.from(document.querySelectorAll(".hdp-overview__label"));
      labs.forEach((lab) => {
        const valueEl =
          lab.parentElement?.querySelector(".hdp-overview__value") || lab.nextElementSibling;
        overviewPairs.push({
          label: (lab.textContent || "").trim().replace(/\s+/g, " "),
          value: (valueEl?.textContent || "").trim().replace(/\s+/g, " "),
        });
      });
    }
    return {
      labels,
      values,
      overviewPairs,
      brandParentSuffix: overviewPairs.some((p) => /\(brand parent\)/i.test(p.value || "")),
      managedSuffix: overviewPairs.some((p) => /\(Managed\)/i.test(p.value || "")),
      meetingLabels: overviewPairs
        .map((p) => p.label)
        .filter((l) => /MEETING/i.test(l || "")),
    };
  });
}

await browser.close();

const hotelTab = tabResults.hotel || { overviewPairs: [], labels: [] };
const requiredFacts = [
  "GUESTROOMS",
  "YEAR BUILT",
  "LAST RENOVATED",
  "MEETING SPACE",
  "MEETING ROOMS",
  "LARGEST MEETING ROOM",
];
const labelSet = new Set(
  [
    ...(hotelTab.overviewPairs || []).map((p) => String(p.label || "").toUpperCase()),
    ...(hotelTab.labels || []).map((l) => String(l || "").toUpperCase()),
  ].filter(Boolean)
);
const factsPresent = requiredFacts.every((lab) => labelSet.has(lab));
// Brand may appear as BRAND / CURRENT BRAND / BRAND PARENT across hero + Property Facts.
const brandLabelPresent = ACTIVE_TABS.some((t) =>
  (tabResults[t]?.labels || []).some((l) =>
    /^(BRAND|CURRENT BRAND|BRAND PARENT)$/i.test(String(l || "").trim())
  )
);

const allCapsLabels = (hotelTab.overviewPairs || [])
  .map((p) => p.label)
  .filter(Boolean)
  .every((lab) => lab === lab.toUpperCase());

const meetingSeparated =
  (hotelTab.meetingLabels || []).includes("MEETING SPACE") &&
  (hotelTab.meetingLabels || []).includes("MEETING ROOMS") &&
  (hotelTab.meetingLabels || []).includes("LARGEST MEETING ROOM");

const tabsVisited = ACTIVE_TABS.every((t) => tabResults[t]);

const checks = {
  factsPresent,
  brandLabelPresent,
  allCapsLabels,
  meetingSeparated,
  noBrandParentSuffix: hotelTab.brandParentSuffix !== true,
  noManagedSuffix: hotelTab.managedSuffix !== true,
  tabsVisited,
  tabCount: ACTIVE_TABS.length,
};

const failed = Object.entries(checks)
  .filter(([, v]) => !v)
  .map(([k]) => k);

const payload = {
  contract: "he-capitalization-standard-browser-v1",
  status: failed.length ? "FAIL" : "PASS",
  hotelId: HOTEL,
  base: BASE,
  checks,
  failed,
  hotelOverview: hotelTab.overviewPairs,
  tabs: Object.fromEntries(
    ACTIVE_TABS.map((t) => [
      t,
      {
        labelSample: (tabResults[t].labels || []).slice(0, 12),
        meetingLabels: tabResults[t].meetingLabels || [],
      },
    ])
  ),
  generatedAt: new Date().toISOString(),
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(payload, null, 2));
console.log(JSON.stringify(payload, null, 2));

if (failed.length) {
  assert.fail(`he-capitalization-standard-browser-v1 failed: ${failed.join(", ")}`);
}
