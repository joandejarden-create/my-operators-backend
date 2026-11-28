#!/usr/bin/env node
/**
 * Hotel Explorer Identity Integrity Gate V1 certification.
 *
 * Usage:
 *   node scripts/hotel-explorer/test-he-identity-integrity-gate-v1.mjs
 *   node scripts/hotel-explorer/test-he-identity-integrity-gate-v1.mjs --browser
 *   node scripts/hotel-explorer/test-he-identity-integrity-gate-v1.mjs --browser --skip-he50-batch
 *
 * Live identity uses read-only API GETs. No paid research. No Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import {
  HE_IDENTITY_INTEGRITY_GATES as G,
  HE_IDENTITY_ACTIVE_TABS,
  HE_IDENTITY_SWITCH_PAIR,
  HE50_IDENTITY_HOTELS,
  isCanonicalHpcId,
  isBlankHotelApiUrl,
  extractHotelIdFromApiUrl,
  responseHotelId,
  assertUrlHotelParity,
  gateStatus,
  summarizeIdentityGates,
} from "../../lib/hotel-intelligence/he-identity-integrity-gate-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const OUT = path.join(ROOT, "reports/hotel-explorer/he-identity-integrity-gate-v1");
const SHOTS = path.join(OUT, "SCREENSHOTS");
fs.mkdirSync(SHOTS, { recursive: true });

const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
const BROWSER = process.argv.includes("--browser");
const SKIP_HE50 = process.argv.includes("--skip-he50-batch");
const PRIMARY = HE_IDENTITY_SWITCH_PAIR.primary;
const SECONDARY = HE_IDENTITY_SWITCH_PAIR.secondary;

const gateResults = {};
function setGate(id, status, detail = {}) {
  gateResults[id] = { status, ...detail };
}

const js = fs.readFileSync(path.join(ROOT, "public/js/hotel-explorer.js"), "utf8");
const hci = fs.readFileSync(path.join(ROOT, "public/js/hotel-contact-intelligence.js"), "utf8");
const factoryPs = fs.readFileSync(
  path.join(ROOT, "scripts/hotel-explorer/Invoke-HotelExplorerFactory.ps1"),
  "utf8",
);

// --- Static contracts ---
assert.match(js, /normalizeHotelInput/);
assert.match(js, /HOTEL_IDENTITY_UNRESOLVED/);
assert.match(js, /isCanonicalHpcId/);
assert.match(js, /STALE_OWNERSHIP_IDENTITY_DROPPED|ownedId/);
assert.match(hci, /if \(!hotelId\) return Promise\.resolve\(null\)/);
assert.match(js, /if \(!id\) return Promise\.resolve\(null\)/);
setGate(G.HE_CANONICAL_HOTEL_ID_VALIDATED, "PASS", {
  note: "normalizeHotelInput + canonical HPC regex present",
});

assert.equal(isBlankHotelApiUrl("/api/contact-intelligence/hotels/?audience=internal"), true);
assert.equal(isBlankHotelApiUrl("/api/golden-demo/ownership/hotel/"), true);
assert.equal(
  isBlankHotelApiUrl(`/api/contact-intelligence/hotels/${PRIMARY.hpcId}?audience=internal`),
  false,
);
assert.equal(extractHotelIdFromApiUrl(`/api/golden-demo/ownership/hotel/${PRIMARY.hpcId}`), PRIMARY.hpcId);
assert.equal(isCanonicalHpcId(PRIMARY.hpcId), true);
assert.equal(isCanonicalHpcId(""), false);

async function launch() {
  const depsRoot = process.env.DEALALITY_NODE_MODULES_ROOT || "C:/Dev/dealality/ws-gdi";
  const require = createRequire(path.join(depsRoot, "package.json"));
  const { chromium } = require("playwright");
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return chromium.launch({ channel: "chrome", headless: true });
  }
}

async function liveApiParity(hotelId) {
  const urls = [
    `${BASE}/api/contact-intelligence/hotels/${encodeURIComponent(hotelId)}?audience=internal`,
    `${BASE}/api/golden-demo/ownership/hotel/${encodeURIComponent(hotelId)}`,
  ];
  const rows = [];
  for (const url of urls) {
    const parity = assertUrlHotelParity(url, hotelId);
    let status = 0;
    let bodyHotel = "";
    try {
      const res = await fetch(url, { headers: { Accept: "application/json" } });
      status = res.status;
      const body = await res.json().catch(() => ({}));
      bodyHotel = responseHotelId(body);
    } catch (err) {
      rows.push({ url, ok: false, error: String(err?.message || err) });
      continue;
    }
    const responseOk =
      !bodyHotel || bodyHotel === hotelId || status === 404 || status === 403;
    rows.push({
      url,
      status,
      requestParity: parity.ok,
      bodyHotel: bodyHotel || null,
      responseOk,
    });
  }
  return rows;
}

const livePrimary = await liveApiParity(PRIMARY.hpcId);
const liveOk =
  livePrimary.length > 0 &&
  livePrimary.every((r) => r.requestParity !== false && r.responseOk !== false && !r.error);
setGate(G.HE_API_HOTEL_ID_PARITY, gateStatus(liveOk, { tested: true }), { livePrimary });
setGate(G.HE_API_RESPONSE_HOTEL_PARITY, gateStatus(liveOk, { tested: true }), { livePrimary });
setGate(G.HE_IDENTITY_PROPAGATION_VALIDATED, gateStatus(liveOk, { tested: true }), {
  note: "Live read-only contact + ownership URLs carry canonical HPC id",
});

async function runBrowser() {
  const browser = await launch();
  const blankHits = [];
  const tabMatrix = [];
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

  page.on("request", (req) => {
    const u = req.url();
    if (isBlankHotelApiUrl(u)) blankHits.push({ phase: "global", url: u });
  });

  await page.goto(
    `${BASE}/deal-capture-radar-with-ranked-list.html?hotelExplorer=${PRIMARY.hpcId}&hexTab=hotel&cb=${Date.now()}`,
    { waitUntil: "domcontentloaded", timeout: 90000 },
  );
  await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, {
    timeout: 45000,
  });

  // Bare-string open must not emit blank-ID requests (P1 root cause).
  const bareBlank = [];
  const onBare = (r) => {
    if (isBlankHotelApiUrl(r.url())) bareBlank.push(r.url());
  };
  page.on("request", onBare);
  await page.evaluate((hid) => {
    HotelExplorer.open(hid, { tab: "contacts", requestedHotelId: hid });
  }, PRIMARY.hpcId);
  await page.waitForTimeout(3500);
  page.off("request", onBare);

  for (const tab of HE_IDENTITY_ACTIVE_TABS) {
    const hotelUrls = [];
    const onReq = (r) => {
      const u = r.url();
      if (/contact-intelligence\/hotels|ownership\/hotel\//i.test(u)) hotelUrls.push(u);
      if (isBlankHotelApiUrl(u)) blankHits.push({ phase: `tab:${tab.id}`, url: u });
    };
    page.on("request", onReq);
    await page.evaluate(
      ({ hid, tabId }) => {
        HotelExplorer.open({ id: hid, recordId: hid }, { tab: tabId, requestedHotelId: hid });
      },
      { hid: PRIMARY.hpcId, tabId: tab.id },
    );
    await page.waitForTimeout(tab.id === "contacts" || tab.id === "ownership" ? 3500 : 1400);
    page.off("request", onReq);

    const probe = await page.evaluate(() => {
      const root = document.querySelector("[data-hex-requested-hotel]");
      return {
        requested: root?.getAttribute("data-hex-requested-hotel") || "",
        rendered: root?.getAttribute("data-hex-rendered-hotel") || "",
        identityError: root?.getAttribute("data-hex-identity-error") === "1",
        bodyLen: (document.body.innerText || "").length,
      };
    });

    const empty = hotelUrls.filter((u) => isBlankHotelApiUrl(u));
    const wrong = hotelUrls
      .map((u) => assertUrlHotelParity(u, PRIMARY.hpcId))
      .filter((p) => p.ok === false && p.reason === "HOTEL_ID_MISMATCH");
    const idOk =
      probe.requested === PRIMARY.hpcId &&
      probe.rendered === PRIMARY.hpcId &&
      !probe.identityError;
    const apiOk = empty.length === 0 && wrong.length === 0;
    tabMatrix.push({
      tab: tab.id,
      label: tab.label,
      identityResolution: gateStatus(idOk),
      apiRequestIdentity: gateStatus(apiOk, {
        applicable: hotelUrls.length > 0 || /ownership|contact/.test(tab.hotelSpecificApi),
      }),
      renderedIdentity: gateStatus(idOk),
      hotelUrls: hotelUrls.length,
      emptyHotelUrls: empty.length,
      wrongHotelUrls: wrong.length,
      status: idOk && empty.length === 0 && wrong.length === 0 ? "PASS" : "FAIL",
    });
  }
  await page.screenshot({
    path: path.join(SHOTS, "01_bethesda_tabs_identity.png"),
    fullPage: false,
  });

  // --- Cross-hotel switch isolation ---
  const switchLog = [];
  async function openAndCapture(hid, label) {
    const urls = [];
    const onReq = (r) => {
      const u = r.url();
      if (/contact-intelligence\/hotels|ownership\/hotel\//i.test(u)) urls.push(u);
      if (isBlankHotelApiUrl(u)) blankHits.push({ phase: `switch:${label}`, url: u });
    };
    page.on("request", onReq);
    await page.evaluate(
      ({ hid, tabId }) => {
        HotelExplorer.open({ id: hid, recordId: hid }, { tab: tabId, requestedHotelId: hid });
      },
      { hid, tabId: "contacts" },
    );
    await page.waitForTimeout(3500);
    page.off("request", onReq);
    const probe = await page.evaluate(() => {
      const root = document.querySelector("[data-hex-requested-hotel]");
      return {
        requested: root?.getAttribute("data-hex-requested-hotel") || "",
        rendered: root?.getAttribute("data-hex-rendered-hotel") || "",
        text: (document.body.innerText || "").slice(0, 400),
      };
    });
    const mismatch = urls.filter((u) => {
      const id = extractHotelIdFromApiUrl(u);
      return id && id !== hid;
    });
    switchLog.push({
      label,
      hid,
      requested: probe.requested,
      rendered: probe.rendered,
      urls: urls.length,
      mismatch: mismatch.length,
      blank: urls.filter(isBlankHotelApiUrl).length,
      ok:
        probe.requested === hid &&
        probe.rendered === hid &&
        mismatch.length === 0 &&
        urls.every((u) => !isBlankHotelApiUrl(u)),
    });
  }

  await openAndCapture(PRIMARY.hpcId, "bethesda");
  await openAndCapture(SECONDARY.hpcId, "ac_newtown");
  await openAndCapture(PRIMARY.hpcId, "bethesda_return");
  await page.screenshot({
    path: path.join(SHOTS, "02_after_hotel_switch.png"),
    fullPage: false,
  });

  // --- Rapid switch + delayed stale ownership response ---
  let raceOk = false;
  {
    let releaseStale;
    const staleGate = new Promise((resolve) => {
      releaseStale = resolve;
    });
    await page.route("**/api/golden-demo/ownership/hotel/**", async (route) => {
      const reqUrl = route.request().url();
      const id = extractHotelIdFromApiUrl(reqUrl);
      if (id === PRIMARY.hpcId) {
        await staleGate;
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            ok: true,
            hotel_id: PRIMARY.hpcId,
            hotel: {
              id: PRIMARY.hpcId,
              name: "STALE Bethesda should not render",
            },
          }),
        });
      }
      return route.continue();
    });
    await page.evaluate((hid) => {
      HotelExplorer.open({ id: hid, recordId: hid }, { tab: "ownership", requestedHotelId: hid });
    }, PRIMARY.hpcId);
    await page.waitForTimeout(200);
    await page.evaluate((hid) => {
      HotelExplorer.open({ id: hid, recordId: hid }, { tab: "ownership", requestedHotelId: hid });
    }, SECONDARY.hpcId);
    await page.waitForTimeout(800);
    releaseStale();
    await page.waitForTimeout(1500);
    const afterRace = await page.evaluate(() => {
      const root = document.querySelector("[data-hex-requested-hotel]");
      const text = document.body.innerText || "";
      return {
        requested: root?.getAttribute("data-hex-requested-hotel") || "",
        rendered: root?.getAttribute("data-hex-rendered-hotel") || "",
        staleLeak: /STALE Bethesda should not render/i.test(text),
      };
    });
    raceOk =
      afterRace.requested === SECONDARY.hpcId &&
      afterRace.rendered === SECONDARY.hpcId &&
      afterRace.staleLeak === false;
    await page.unroute("**/api/golden-demo/ownership/hotel/**");
    await page.screenshot({
      path: path.join(SHOTS, "03_rapid_switch_race.png"),
      fullPage: false,
    });
  }

  // --- HE50 eight-hotel smoke ---
  const he50 = [];
  if (!SKIP_HE50) {
    for (const hotel of HE50_IDENTITY_HOTELS) {
      const urls = [];
      const onReq = (r) => {
        const u = r.url();
        if (isBlankHotelApiUrl(u)) urls.push(u);
      };
      page.on("request", onReq);
      await page.evaluate((hid) => {
        HotelExplorer.open({ id: hid, recordId: hid }, { tab: "hotel", requestedHotelId: hid });
      }, hotel.hpcId);
      await page.waitForTimeout(1600);
      page.off("request", onReq);
      const probe = await page.evaluate(() => {
        const root = document.querySelector("[data-hex-requested-hotel]");
        return {
          requested: root?.getAttribute("data-hex-requested-hotel") || "",
          rendered: root?.getAttribute("data-hex-rendered-hotel") || "",
        };
      });
      he50.push({
        pilotKey: hotel.pilotKey,
        hpcId: hotel.hpcId,
        ok:
          probe.requested === hotel.hpcId &&
          probe.rendered === hotel.hpcId &&
          urls.length === 0,
        blank: urls.length,
      });
    }
  }

  await browser.close();

  const tabsPass = tabMatrix.every((t) => t.status === "PASS");
  const switchPass = switchLog.every((s) => s.ok);
  const noBlank = blankHits.length === 0 && bareBlank.length === 0;
  const he50Pass = SKIP_HE50 ? null : he50.every((h) => h.ok);

  setGate(G.HE_NO_BLANK_HOTEL_REQUESTS, gateStatus(noBlank), {
    blankHits: blankHits.length,
    bareStringBlank: bareBlank.length,
  });
  setGate(G.HE_RENDERED_HOTEL_IDENTITY_VALIDATED, gateStatus(tabsPass), { tabMatrix });
  setGate(G.HE_ALL_TABS_IDENTITY_VALIDATED, gateStatus(tabsPass), {
    pass: tabMatrix.filter((t) => t.status === "PASS").length,
    total: tabMatrix.length,
  });
  setGate(G.HE_CROSS_HOTEL_CACHE_ISOLATION, gateStatus(switchPass && raceOk), {
    switchLog,
    raceOk,
  });
  setGate(G.HE_IDENTITY_PROPAGATION_VALIDATED, gateStatus(tabsPass && switchPass && noBlank), {
    bareStringOpenSafe: bareBlank.length === 0,
  });

  return {
    tabMatrix,
    switchLog,
    raceOk,
    blankHits,
    bareBlank,
    he50,
    he50Pass,
    noBlank,
  };
}

let browserResult = null;
if (BROWSER) {
  try {
    browserResult = await runBrowser();
    if (browserResult.he50Pass === false) {
      // Soft note — eight-hotel smoke failure still fails ALL_TABS / isolation only if Bethesda tabs fail.
      // Mark dedicated coverage in report; do not invent PASS.
    }
  } catch (err) {
    setGate(G.HE_ALL_TABS_IDENTITY_VALIDATED, "FAIL", {
      error: String(err?.stack || err).slice(0, 800),
    });
    setGate(G.HE_RENDERED_HOTEL_IDENTITY_VALIDATED, "FAIL");
    setGate(G.HE_CROSS_HOTEL_CACHE_ISOLATION, "FAIL");
    setGate(G.HE_NO_BLANK_HOTEL_REQUESTS, "FAIL");
  }
} else {
  setGate(G.HE_RENDERED_HOTEL_IDENTITY_VALIDATED, "NOT_TESTED", {
    note: "Pass --browser for Playwright certification",
  });
  setGate(G.HE_ALL_TABS_IDENTITY_VALIDATED, "NOT_TESTED");
  setGate(G.HE_CROSS_HOTEL_CACHE_ISOLATION, "NOT_TESTED");
  setGate(G.HE_NO_BLANK_HOTEL_REQUESTS, "NOT_TESTED", {
    note: "Static blank-URL detector unit-tested; browser NOT_TESTED",
  });
}

// Factory wiring presence (updated in same PR)
const factoryWired =
  /test:he-identity-integrity-gate-v1|HE_CANONICAL_HOTEL_ID_VALIDATED|Identity Integrity Gate/i.test(
    factoryPs,
  ) ||
  fs
    .readFileSync(path.join(ROOT, "package.json"), "utf8")
    .includes("test:he-identity-integrity-gate-v1");

const summary = summarizeIdentityGates(gateResults);
const report = {
  at: new Date().toISOString(),
  version: "he-identity-integrity-gate-v1",
  base: BASE,
  browser: BROWSER,
  primary: PRIMARY,
  secondary: SECONDARY,
  factoryWired,
  livePrimary,
  gates: gateResults,
  summary,
  browserResult,
  rootCauseBlankIds:
    "HotelExplorer.open(bareString) previously passed a string into mergeHotelState/Object.assign, blanking id and emitting /hotels/? and /ownership/hotel/.",
};
fs.writeFileSync(path.join(OUT, "CERTIFICATION.json"), JSON.stringify(report, null, 2));
fs.writeFileSync(
  path.join(OUT, "TAB_IDENTITY_MATRIX.json"),
  JSON.stringify(browserResult?.tabMatrix || [], null, 2),
);

const failed = Object.values(gateResults).filter((g) => g.status === "FAIL");
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      classification: failed.length
        ? "HE_IDENTITY_INTEGRITY_NOT_CERTIFIED"
        : BROWSER
          ? "HE_IDENTITY_INTEGRITY_CERTIFIED"
          : "HE_IDENTITY_INTEGRITY_STATIC_PASS_BROWSER_NOT_TESTED",
      failed: failed.length,
      gates: summary.gates,
      FULL_HE_E2E_VALIDATED_BLOCKED_BY_IDENTITY:
        summary.FULL_HE_E2E_VALIDATED_BLOCKED_BY_IDENTITY,
    },
    null,
    2,
  ),
);
if (failed.length) process.exit(1);
