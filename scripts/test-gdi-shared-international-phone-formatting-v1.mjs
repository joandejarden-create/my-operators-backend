#!/usr/bin/env node
/**
 * GDI consumer + cross-product contracts for
 * DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1
 *
 *   node scripts/test-gdi-shared-international-phone-formatting-v1.mjs
 *   DEALALITY_PHONE_BROWSER=1 node scripts/test-gdi-shared-international-phone-formatting-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  CONTRACT_ID,
  formatDealalityPhone,
} from "../lib/dealality-shared/dealality-shared-international-phone-formatting-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports",
  "dealality-shared-international-phone-formatting-v1",
);
fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

const results = [];
function check(id, fn) {
  try {
    fn();
    results.push({ id, ok: true });
  } catch (err) {
    results.push({
      id,
      ok: false,
      error: String(err && err.message ? err.message : err),
    });
  }
}

const GDI_UI = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);
const LIST = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
  "utf8",
);
const HE = fs.readFileSync(
  path.join(ROOT, "public/js/hotel-contact-intelligence.js"),
  "utf8",
);
const GDI_HTML = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);
const SHARE_HTML = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence-share.html"),
  "utf8",
);
const BROWSER_FACADE = fs.readFileSync(
  path.join(ROOT, "public/js/dealality-shared-international-phone-formatting-v1.js"),
  "utf8",
);
const VENDOR = fs.existsSync(
  path.join(ROOT, "public/vendor/libphonenumber-js.min.js"),
);

check("GDI_PHONE_SHARED_UTIL_LOADED_IN_HTML", () => {
  assert.match(GDI_HTML, /libphonenumber-js\.min\.js/);
  assert.match(GDI_HTML, /dealality-shared-international-phone-formatting-v1\.js/);
  assert.match(SHARE_HTML, /libphonenumber-js\.min\.js/);
  assert.match(SHARE_HTML, /dealality-shared-international-phone-formatting-v1\.js/);
  assert.equal(VENDOR, true);
  assert.match(BROWSER_FACADE, /DealalitySharedPhoneFormattingV1/);
});

check("GDI_PHONE_CONSUMERS_USE_SHARED_FORMATTER", () => {
  assert.match(GDI_UI, /formatPhonePresentation/);
  assert.match(GDI_UI, /phoneAnchorHtml/);
  assert.match(GDI_UI, /DealalitySharedPhoneFormattingV1/);
  assert.match(LIST, /DealalitySharedPhoneFormattingV1/);
  assert.match(LIST, /formatDealalityPhone/);
  // Primary path prefers shared util before any digit-strip fallback.
  assert.match(LIST, /phoneApi\.formatDealalityPhone/);
});

check("GDI_PHONE_NO_RAW_TEL_DIGIT_STRIP_IN_UI", () => {
  // Migrated consumers must not rebuild tel: via esc(String(...).replace(
  assert.doesNotMatch(GDI_UI, /href="tel:"\s*\+\s*esc\(String\(/);
  assert.doesNotMatch(HE, /href="tel:"\s*\+\s*esc\(String\(/);
});

check("DEALALITY_PHONE_CROSS_PRODUCT_PARITY", () => {
  assert.match(HE, /DealalitySharedPhoneFormattingV1/);
  assert.match(HE, /formatPhonePresentation/);
  assert.match(HE, /phoneAnchorHtml/);
  const a = formatDealalityPhone("+12674754267");
  const b = formatDealalityPhone("+19722335600");
  assert.equal(a.displayValue, "+1 (267) 475-4267");
  assert.equal(b.displayValue, "+1 (972) 233-5600");
  assert.equal(a.telHref, "tel:+12674754267");
  assert.equal(b.telHref, "tel:+19722335600");
});

check("GDI_PHONE_LIST_DRAWER_PARITY", () => {
  // Both list + drawer path through the same shared API contract.
  assert.match(LIST, /phoneDisplay/);
  assert.match(LIST, /telHref/);
  assert.match(GDI_UI, /phoneAnchorHtml\(c\.phone\)/);
  assert.match(GDI_UI, /phoneAnchorHtml\(contact\.phone\)/);
  const same = formatDealalityPhone("+12674754267");
  assert.equal(same.displayValue, "+1 (267) 475-4267");
  assert.equal(same.telHref, "tel:+12674754267");
  assert.equal(same.originalRaw, "+12674754267");
});

check("GDI_PHONE_PRIMARY_ALTERNATIVE_ATTRIBUTION", () => {
  // Primary and alternative phones remain separate call sites (not merged).
  assert.match(GDI_UI, /phoneAnchorHtml\(slotPersonPhone\)/);
  assert.match(GDI_UI, /phoneAnchorHtml\(slotCompanyPhone\)/);
  assert.match(GDI_UI, /formatPhonePresentation\(b\.phone\)/);
});

check("GDI_PHONE_SURFE_TRANSIENT_ONLY", () => {
  assert.match(GDI_UI, /Surfe/);
  assert.match(HE, /data-surfe-session/);
  assert.match(HE, /data-persist="forbidden"/);
  assert.match(HE, /phoneAnchorHtml\(session\.phone\)|phoneAnchorHtml\(phone\)/);
  // Formatter itself has no Surfe / fetch / Airtable
  const libSrc = fs.readFileSync(
    path.join(
      ROOT,
      "lib/dealality-shared/dealality-shared-international-phone-formatting-v1.js",
    ),
    "utf8",
  );
  assert.doesNotMatch(libSrc, /surfe|findymail|airtable|fetch\(/i);
});

check("GDI_PHONE_NO_PROVIDER_CALLS", () => {
  const libSrc = fs.readFileSync(
    path.join(
      ROOT,
      "lib/dealality-shared/dealality-shared-international-phone-formatting-v1.js",
    ),
    "utf8",
  );
  assert.doesNotMatch(libSrc, /\bfetch\s*\(|\baxios\b|\bXMLHttpRequest\b/);
  assert.doesNotMatch(libSrc, /airtable|surfe|findymail/i);
});

check("GDI_PHONE_NO_PUBLICATION_MUTATION", () => {
  // This assignment must not touch STAGED/CURRENT publication pointers.
  const stagedPtr = path.join(
    ROOT,
    "data/group-demand-intelligence/publications/STAGED.json",
  );
  const currentPtr = path.join(
    ROOT,
    "data/group-demand-intelligence/publications/CURRENT.json",
  );
  // Pointers may or may not exist locally; if present, ensure this test does not rewrite them.
  const beforeStaged = fs.existsSync(stagedPtr)
    ? fs.readFileSync(stagedPtr, "utf8")
    : null;
  const beforeCurrent = fs.existsSync(currentPtr)
    ? fs.readFileSync(currentPtr, "utf8")
    : null;
  formatDealalityPhone("+12674754267");
  if (beforeStaged != null) {
    assert.equal(fs.readFileSync(stagedPtr, "utf8"), beforeStaged);
  }
  if (beforeCurrent != null) {
    assert.equal(fs.readFileSync(currentPtr, "utf8"), beforeCurrent);
  }
});

check("GDI_PHONE_TEMPLATE_NON_REGRESSION", () => {
  // No layout/CSS redesign markers introduced for phone formatting.
  assert.doesNotMatch(GDI_UI, /gdi-phone-redesign|phone-layout-v2/i);
  assert.match(GDI_UI, /GDI_CANONICAL|canonical|gdi-drawer|gdi-list/i);
});

check("DEALALITY_PHONE_RAW_VALUE_PRESERVED", () => {
  const raw = "+12674754267";
  const r = formatDealalityPhone(raw);
  assert.equal(r.originalRaw, raw);
  assert.notEqual(r.displayValue, raw);
});

async function runBrowser() {
  const { chromium } = await import("playwright");
  const BASE = (
    process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080"
  ).replace(/\/$/, "");
  const browser = await chromium.launch({ headless: true });
  const viewports = [
    { w: 1920, h: 1080 },
    { w: 1440, h: 900 },
    { w: 1366, h: 768 },
    { w: 768, h: 1024 },
    { w: 390, h: 844 },
  ];
  const browserResults = [];
  try {
    for (const vp of viewports) {
      const page = await browser.newPage({
        viewport: { width: vp.w, height: vp.h },
      });
      await page.route("**/api/me**", async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            ok: true,
            success: true,
            user: { email: "dealalitydemo@dealality.com" },
            dealality: {
              email: "dealalitydemo@dealality.com",
              isAdmin: true,
              isDemo: true,
              flags: { isAdmin: true, gdiPilot: true },
            },
          }),
        });
      });
      const url = `${BASE}/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8`;
      const resp = await page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: 60000,
      });
      assert.ok(resp && resp.ok(), `HTTP ${resp && resp.status()}`);
      await page.waitForTimeout(2500);
      const fmt = await page.evaluate(() => {
        const api = window.DealalitySharedPhoneFormattingV1;
        if (!api || typeof api.formatDealalityPhone !== "function") {
          return { ok: false, error: "API_MISSING" };
        }
        const a = api.formatDealalityPhone("+12674754267");
        const b = api.formatDealalityPhone("+19722335600");
        return {
          ok: true,
          aDisplay: a.displayValue,
          aTel: a.telHref,
          bDisplay: b.displayValue,
          bTel: b.telHref,
          contract: api.CONTRACT_ID,
        };
      });
      assert.equal(fmt.ok, true, fmt.error || "formatter missing in browser");
      assert.equal(fmt.aDisplay, "+1 (267) 475-4267");
      assert.equal(fmt.aTel, "tel:+12674754267");
      assert.equal(fmt.bDisplay, "+1 (972) 233-5600");
      assert.equal(fmt.bTel, "tel:+19722335600");

      const shot = path.join(
        OUT,
        "screenshots",
        `gdi-bethesda-${vp.w}x${vp.h}.png`,
      );
      await page.screenshot({ path: shot, fullPage: false });
      browserResults.push({
        viewport: `${vp.w}x${vp.h}`,
        ok: true,
        screenshot: shot,
        formatter: fmt,
      });
      await page.close();
    }
  } finally {
    await browser.close();
  }
  return browserResults;
}

const failed = results.filter((r) => !r.ok);
let browserResults = null;
if (process.env.DEALALITY_PHONE_BROWSER === "1") {
  try {
    browserResults = await runBrowser();
    results.push({ id: "GDI_PHONE_PLAYWRIGHT_VIEWPORTS", ok: true });
  } catch (err) {
    results.push({
      id: "GDI_PHONE_PLAYWRIGHT_VIEWPORTS",
      ok: false,
      error: String(err && err.message ? err.message : err),
    });
  }
}

const allFailed = results.filter((r) => !r.ok);
const summary = {
  contract: CONTRACT_ID,
  ok: allFailed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: allFailed.length,
  results,
  browserResults,
  generatedAt: new Date().toISOString(),
};
fs.writeFileSync(
  path.join(OUT, "GDI_CONSUMER_RESULTS.json"),
  JSON.stringify(summary, null, 2),
);
console.log(
  JSON.stringify(
    {
      ok: summary.ok,
      passed: summary.passed,
      failed: summary.failed,
      browser: Boolean(browserResults),
    },
    null,
    2,
  ),
);
if (!summary.ok) {
  console.error(allFailed);
  process.exit(1);
}
