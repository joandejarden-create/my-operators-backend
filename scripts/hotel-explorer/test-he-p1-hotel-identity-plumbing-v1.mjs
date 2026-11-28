#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const OUT = path.join(ROOT, "reports/hotel-explorer/he-p1-identity-recovery");
fs.mkdirSync(path.join(OUT, "SCREENSHOTS"), { recursive: true });
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
const HOTEL = "recLuxvwwxID7U2B8";

const heSrc = fs.readFileSync(path.join(ROOT, "public/js/hotel-explorer.js"), "utf8");
const hciSrc = fs.readFileSync(path.join(ROOT, "public/js/hotel-contact-intelligence.js"), "utf8");
assert.match(heSrc, /normalizeHotelInput/);
assert.match(heSrc, /HOTEL_IDENTITY_UNRESOLVED/);
assert.match(heSrc, /isCanonicalHpcId/);
assert.match(hciSrc, /Hotel identity could not be resolved/);
assert.match(hciSrc, /if \(!hotelId\) return Promise\.resolve\(null\)/);

const browser = await chromium.launch({ headless: true }).catch(() =>
  chromium.launch({ channel: "chrome", headless: true }),
);
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const urls = [];
page.on("request", (r) => {
  const u = r.url();
  if (/contact-intelligence\/hotels|ownership\/hotel\//.test(u)) urls.push(u);
});

await page.goto(
  `${BASE}/deal-capture-radar-with-ranked-list.html?hotelExplorer=${HOTEL}&hexTab=contacts&cb=${Date.now()}`,
  { waitUntil: "domcontentloaded", timeout: 90000 },
);
await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, { timeout: 45000 });
await page.evaluate((hid) => {
  HotelExplorer.open(hid, { tab: "contacts" }); // bare string must work
}, HOTEL);
await page.waitForFunction(
  () => document.querySelectorAll("[data-hci-channel-stack]").length >= 5,
  null,
  { timeout: 90000 },
);

const probe = await page.evaluate(() => {
  const root = document.querySelector("[data-hex-requested-hotel]");
  const stacks = [...document.querySelectorAll("[data-hci-channel-stack]")].map((el) =>
    (el.innerText || "").replace(/\s+/g, " "),
  );
  const cols = [...document.querySelectorAll(".hci-root thead th, .hci-root [role='columnheader']")].map(
    (el) => (el.textContent || "").trim(),
  );
  return {
    requested: root?.getAttribute("data-hex-requested-hotel"),
    rendered: root?.getAttribute("data-hex-rendered-hotel"),
    stacks,
    emails: stacks.filter((s) => /@/.test(s)).length,
    cols,
    findEmail: /Find Email/i.test(document.body.innerText || ""),
  };
});

assert.equal(probe.requested, HOTEL);
assert.equal(probe.rendered, HOTEL);
assert.equal(probe.stacks.length, 5);
assert.ok(probe.emails >= 3, `expected >=3 emails, got ${probe.emails}`);
assert.equal(
  urls.filter((u) => /\/hotels\/\?|\/hotel\/(\?|$)/.test(u)).length,
  0,
  "empty hotel id URLs present",
);
assert.ok(urls.some((u) => u.includes(`/hotels/${HOTEL}`)));
assert.ok(urls.some((u) => u.includes(`/ownership/hotel/${HOTEL}`)));

await page.screenshot({
  path: path.join(OUT, "SCREENSHOTS", "bethesda-contacts-identity-fixed-1440x900.png"),
  fullPage: false,
});

const api = await (await fetch(`${BASE}/api/contact-intelligence/hotels/${HOTEL}?audience=internal`)).json();
const people = api?.package?.people || api?.people || [];
const report = {
  at: new Date().toISOString(),
  hotelId: HOTEL,
  classification: "HE_P1_IDENTITY_RECOVERY_PASS",
  probe,
  emptyHotelIdRequests: 0,
  apiPeopleCount: Array.isArray(people) ? people.length : null,
  apiSuccess: Boolean(api?.success || api?.package),
  sampleUrls: urls.slice(0, 10),
};
fs.writeFileSync(path.join(OUT, "IDENTITY_FIX_RESULTS.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
await browser.close();
