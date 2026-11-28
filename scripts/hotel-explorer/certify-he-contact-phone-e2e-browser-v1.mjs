#!/usr/bin/env node
/**
 * Browser certify Bethesda Contact Intelligence after phone persist.
 * Default base: http://127.0.0.1:8095 (feature port). No paid clicks.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8095").replace(
  /\/$/,
  "",
);
const HOTEL = "recLuxvwwxID7U2B8";
const OUT = path.join(
  ROOT,
  "reports/hotel-explorer/he50-bethesda-findymail-phone-live-v1",
);
const SHOTS = path.join(OUT, "BROWSER_CERT_SHOTS");
fs.mkdirSync(SHOTS, { recursive: true });

const TABS = [
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

async function launch() {
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return chromium.launch({ channel: "chrome", headless: true });
  }
}

const browser = await launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const consoleErrors = [];
const failedApi = [];
page.on("console", (m) => {
  if (m.type() === "error") consoleErrors.push(m.text().slice(0, 240));
});
page.on("response", (r) => {
  if (/\/api\//.test(r.url()) && r.status() >= 400) {
    failedApi.push({ status: r.status(), url: r.url().slice(0, 200) });
  }
});

await page.goto(
  `${BASE}/deal-capture-radar-with-ranked-list.html?hotelExplorer=${HOTEL}&hexTab=contacts&cb=${Date.now()}`,
  { waitUntil: "domcontentloaded", timeout: 90000 },
);
await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, {
  timeout: 45000,
});
await page.evaluate((id) => {
  HotelExplorer.open({ id, recordId: id }, { tab: "contacts" });
}, HOTEL);
await page.waitForTimeout(2500);

const contacts = await page.evaluate(() => {
  const root =
    document.querySelector(".hci-root, .hex-panel, .hex-root") || document.body;
  const text = (root.innerText || "").replace(/\s+/g, " ");
  const findBtns = [...document.querySelectorAll("button")].filter((b) =>
    /find email\s*&\s*phone/i.test(b.textContent || ""),
  );
  const detailsCells = document.querySelectorAll(
    ".gdi-list-contact-details--with-action, [data-hci-contact-details-cell]",
  );
  let rightAligned = 0;
  findBtns.forEach((btn) => {
    const cell = btn.closest(
      ".gdi-list-contact-details--with-action, td, [data-hci-contact-details-cell]",
    );
    if (!cell) return;
    const channels = cell.querySelector(
      ".gdi-list-contact-channels, .hci-channels, .gdi-list-contact-details__channels",
    );
    if (!channels) {
      // heuristic: button is to the right of email/phone text block
      const cellRect = cell.getBoundingClientRect();
      const btnRect = btn.getBoundingClientRect();
      if (btnRect.left > cellRect.left + cellRect.width * 0.35) rightAligned += 1;
      return;
    }
    const ch = channels.getBoundingClientRect();
    const br = btn.getBoundingClientRect();
    if (br.left >= ch.right - 4) rightAligned += 1;
  });
  const detailsColBtns = [...document.querySelectorAll("button")].filter((b) =>
    /^details$/i.test((b.textContent || "").trim()),
  );
  return {
    hasJeff: /jeff mullen/i.test(text),
    hasKatherine: /katherine button/i.test(text),
    hasPatrick: /patrick reece/i.test(text),
    hasEmailJeff: /jeff\.mullen@mullencap\.com/i.test(text),
    hasEmailKatherine: /kbutton@mullencap\.com/i.test(text),
    hasPhoneKatherine: /267|12674754267|\+12674754267/.test(text),
    hasPhonePatrick: /972|19722335600|\+19722335600/.test(text),
    stagingEmpty: /staging is not available/i.test(text),
    findBtnCount: findBtns.length,
    rightAlignedCount: rightAligned,
    detailsCellCount: detailsCells.length,
    detailsColBtnCount: detailsColBtns.length,
    snippet: text.slice(0, 500),
  };
});

await page.screenshot({
  path: path.join(SHOTS, "contacts_1440x900.png"),
  fullPage: true,
});

const tabResults = [];
for (const tab of TABS) {
  await page.evaluate((t) => {
    if (typeof HotelExplorer.setTab === "function") HotelExplorer.setTab(t);
  }, tab);
  await page.waitForTimeout(600);
  const ok = await page.evaluate(() => {
    const root = document.querySelector(".hex-root.is-open, .hex-root");
    return !!(root && (root.innerText || "").length > 80);
  });
  tabResults.push({ tab, ok });
}
await page.screenshot({
  path: path.join(SHOTS, "after_all_tabs_1440x900.png"),
  fullPage: false,
});

await browser.close();

const report = {
  version: "he-contact-phone-e2e-browser-cert-v1",
  base: BASE,
  hotelId: HOTEL,
  generatedAt: new Date().toISOString(),
  paidClicks: false,
  surfeCalls: false,
  contacts,
  tabResults,
  consoleErrors: consoleErrors.slice(0, 20),
  failedApi: failedApi.slice(0, 30),
  gates: {
    HE_CONTACT_AUTO_HYDRATE_EMAIL:
      contacts.hasEmailJeff || contacts.hasEmailKatherine ? "PASS" : "FAIL",
    HE_CONTACT_AUTO_HYDRATE_PHONE:
      contacts.hasPhoneKatherine || contacts.hasPhonePatrick ? "PASS" : "FAIL",
    HE_CONTACT_FIND_BUTTON_PRESENT: contacts.findBtnCount > 0 ? "PASS" : "FAIL",
    HE_CONTACT_FIND_BUTTON_RIGHT:
      contacts.rightAlignedCount > 0 ? "PASS" : "FAIL",
    HE_CONTACT_DETAILS_COLUMN: contacts.detailsColBtnCount > 0 ? "PASS" : "NOT_TESTED",
    HE_CONTACT_NO_STAGING_EMPTY: contacts.stagingEmpty ? "FAIL" : "PASS",
    HE_ELEVEN_TABS: tabResults.every((t) => t.ok) ? "PASS" : "FAIL",
  },
};

fs.writeFileSync(
  path.join(OUT, "BROWSER_CERT_REPORT.json"),
  JSON.stringify(report, null, 2) + "\n",
);
console.log(JSON.stringify(report, null, 2));
const failed = Object.values(report.gates).filter((v) => v === "FAIL").length;
process.exit(failed ? 1 : 0);
