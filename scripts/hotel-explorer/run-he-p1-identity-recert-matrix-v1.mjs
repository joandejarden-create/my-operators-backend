import fs from "fs";
import path from "path";
import { chromium } from "playwright";
const OUT = "reports/hotel-explorer/he-p1-identity-recovery";
const SHOTS = path.join(OUT, "SCREENSHOTS");
fs.mkdirSync(SHOTS, { recursive: true });
const HOTEL = "recLuxvwwxID7U2B8";
const BASE = "http://127.0.0.1:8080";
const TABS = ["hotel","rooms","ownership","contacts","organization","relationships","submarket","area","demand","access","sources"];
const VPS = [
  [1920,1080],[1440,900],[1366,768],[768,1024],[390,844]
];
const SMOKE = [
  { name: "Bethesda Marriott", hpcId: "recLuxvwwxID7U2B8" },
  { name: "AC Hotel Newtown Square", hpcId: "recF7DQBPDW5aUDoC" },
  { name: "Carton House, a Fairmont Hotel", hpcId: "recYf7HZWmCdWPaSd" },
  { name: "Hilton Garden Inn Newtown Square Radnor", hpcId: "recgaBAwa24wsl3xF" },
  { name: "Secrets Papagayo Costa Rica", hpcId: "recFQ0WvGLbfK5Gpa" },
  { name: "Secrets St. Lucia", hpcId: "recwZJuMcx6kA7f7I" },
  { name: "Secrets Cap Cana Resort & Spa", hpcId: "recd7XAM6tLjzQhaI" },
];

async function launch() {
  try { return await chromium.launch({ headless: true }); }
  catch { return chromium.launch({ channel: "chrome", headless: true }); }
}

const browser = await launch();
const matrix = [];
const netIssues = [];

for (const [w,h] of VPS) {
  const page = await browser.newPage({ viewport: { width: w, height: h } });
  const bad = [];
  page.on("request", (r) => {
    const u = r.url();
    if (/\/hotels\/\?|\/ownership\/hotel\/(\?|$)/.test(u)) bad.push(u);
  });
  await page.goto(`${BASE}/deal-capture-radar-with-ranked-list.html?hotelExplorer=${HOTEL}&hexTab=hotel&cb=${Date.now()}`, { waitUntil: "domcontentloaded", timeout: 90000 });
  await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, { timeout: 45000 });
  for (const tab of TABS) {
    const urls = [];
    const onReq = (r) => {
      const u = r.url();
      if (/contact-intelligence\/hotels|ownership\/hotel\//.test(u)) urls.push(u);
    };
    page.on("request", onReq);
    await page.evaluate(({ hid, tab }) => {
      HotelExplorer.open({ id: hid, recordId: hid }, { tab, requestedHotelId: hid });
    }, { hid: HOTEL, tab });
    await page.waitForTimeout(tab === "contacts" || tab === "ownership" ? 3500 : 1200);
    page.off("request", onReq);
    const empty = urls.filter((u) => /\/hotels\/\?|\/hotel\/(\?|$)/.test(u));
    const probe = await page.evaluate((t) => {
      const root = document.querySelector("[data-hex-requested-hotel]");
      const body = document.body.innerText || "";
      return {
        requested: root?.getAttribute("data-hex-requested-hotel"),
        rendered: root?.getAttribute("data-hex-rendered-hotel"),
        hasHci: t === "contacts" ? !!document.querySelector(".hci-root, [data-hci-channel-stack]") : null,
        contentLen: body.length,
        stagingMiss: /staging is not available/i.test(body),
      };
    }, tab);
    const idOk = probe.requested === HOTEL && probe.rendered === HOTEL;
    const dataOk = tab === "contacts" ? probe.hasHci && !probe.stagingMiss && empty.length === 0 : empty.length === 0 && idOk;
    const status = idOk && dataOk ? "PASS" : idOk ? "PARTIAL_SHELL" : "FAIL";
    matrix.push({ tab, viewport: `${w}x${h}`, status, idOk, emptyHotelUrls: empty.length, contentLen: probe.contentLen });
    if (empty.length) netIssues.push({ tab, viewport: `${w}x${h}`, empty });
    if (tab === "contacts" && w === 1440) {
      await page.screenshot({ path: path.join(SHOTS, `matrix-contacts-${w}x${h}.png`), fullPage: false });
    }
  }
  await page.close();
}

const smoke = [];
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
for (const hotel of SMOKE) {
  const urls = [];
  page.on("request", (r) => {
    const u = r.url();
    if (/\/hotels\/\?|\/ownership\/hotel\/(\?|$)/.test(u)) urls.push(u);
  });
  await page.goto(`${BASE}/deal-capture-radar-with-ranked-list.html?hotelExplorer=${hotel.hpcId}&hexTab=hotel&cb=${Date.now()}`, { waitUntil: "domcontentloaded", timeout: 90000 });
  await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, { timeout: 45000 }).catch(() => null);
  await page.evaluate((hid) => {
    if (window.HotelExplorer) HotelExplorer.open({ id: hid, recordId: hid }, { tab: "hotel", requestedHotelId: hid });
  }, hotel.hpcId);
  await page.waitForTimeout(2000);
  const probe = await page.evaluate(() => {
    const root = document.querySelector("[data-hex-requested-hotel]");
    return {
      requested: root?.getAttribute("data-hex-requested-hotel"),
      rendered: root?.getAttribute("data-hex-rendered-hotel"),
      contentLen: (document.body.innerText || "").length,
    };
  });
  smoke.push({
    hotel: hotel.name,
    hpcId: hotel.hpcId,
    status: probe.requested === hotel.hpcId && probe.rendered === hotel.hpcId && urls.length === 0 ? "PASS" : "FAIL",
    emptyHotelUrls: urls.length,
    contentLen: probe.contentLen,
  });
}
await page.close();
await browser.close();

const pass = matrix.filter((r) => r.status === "PASS").length;
const summary = {
  at: new Date().toISOString(),
  matrixCells: matrix.length,
  matrixPass: pass,
  matrixFail: matrix.length - pass,
  matrix,
  smoke,
  netIssues,
  scoutP2: "NOT_IN_SCOPE — document only",
  overviewIdentityP2: "NOT_IN_SCOPE — document only",
};
fs.writeFileSync(path.join(OUT, "PLAYWRIGHT_RECERT_MATRIX.json"), JSON.stringify(summary, null, 2));
console.log(JSON.stringify({ matrixPass: pass, matrixTotal: matrix.length, smokePass: smoke.filter(s=>s.status==="PASS").length, smokeTotal: smoke.length, emptyNet: netIssues.length }, null, 2));
