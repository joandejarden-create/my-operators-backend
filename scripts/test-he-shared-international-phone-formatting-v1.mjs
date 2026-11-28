#!/usr/bin/env node
/**
 * HE Contact Intelligence consumer + Bethesda golden browser cert for
 * DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1
 *
 *   node scripts/test-he-shared-international-phone-formatting-v1.mjs
 *   DEALALITY_PHONE_BROWSER=1 node scripts/test-he-shared-international-phone-formatting-v1.mjs
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
const SHOTS = path.join(OUT, "screenshots-he");
fs.mkdirSync(SHOTS, { recursive: true });

const HOTEL = "recLuxvwwxID7U2B8";
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(
  /\/$/,
  "",
);

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

const HE = fs.readFileSync(
  path.join(ROOT, "public/js/hotel-contact-intelligence.js"),
  "utf8",
);
const HE_HTML = fs.readFileSync(
  path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html"),
  "utf8",
);
const PREVIEW = fs.readFileSync(
  path.join(ROOT, "public/hotel-explorer-preview.html"),
  "utf8",
);

check("HE_PHONE_SHARED_UTIL_LOADED_IN_HTML", () => {
  assert.match(HE_HTML, /libphonenumber-js\.min\.js/);
  assert.match(HE_HTML, /dealality-shared-international-phone-formatting-v1\.js/);
  assert.match(PREVIEW, /libphonenumber-js\.min\.js/);
  assert.match(PREVIEW, /dealality-shared-international-phone-formatting-v1\.js/);
});

check("HE_PHONE_CONSUMER_USES_SHARED_FORMATTER", () => {
  assert.match(HE, /DealalitySharedPhoneFormattingV1/);
  assert.match(HE, /formatPhonePresentation/);
  assert.match(HE, /phoneAnchorHtml/);
  assert.match(HE, /formatDealalityPhone/);
});

check("DEALALITY_PHONE_COUNTRY_FORMAT_VALIDATED", () => {
  const cases = [
    ["+12674754267", "+1 (267) 475-4267"], // US Bethesda golden
    ["+19722335600", "+1 (972) 233-5600"], // US Bethesda golden
    ["+14165550100", "+1 (416) 555-0100"], // CA
    ["+442079460958", "+44 20 7946 0958"], // UK
    ["+34612345678", "+34 612 34 56 78"], // ES (library international)
    ["+33612345678", "+33 6 12 34 56 78"], // FR
    ["+525512345678", "+52 55 1234 5678"], // MX
    ["+18095550123", "+1 (809) 555-0123"], // DO
    ["+50622221234", "+506 2222 1234"], // CR
  ];
  for (const [raw, expected] of cases) {
    const r = formatDealalityPhone(raw);
    assert.equal(r.displayValue, expected, `${raw} → ${r.displayValue}`);
    assert.equal(r.originalRaw, raw);
  }
});

check("DEALALITY_PHONE_E164_LINK_VALIDATED", () => {
  const a = formatDealalityPhone("+12674754267");
  const b = formatDealalityPhone("+19722335600");
  assert.equal(a.telHref, "tel:+12674754267");
  assert.equal(b.telHref, "tel:+19722335600");
  assert.equal(a.e164, "+12674754267");
  assert.equal(b.e164, "+19722335600");
});

check("DEALALITY_PHONE_RAW_VALUE_PRESERVED", () => {
  const raw = "+12674754267";
  const r = formatDealalityPhone(raw);
  assert.equal(r.originalRaw, raw);
  assert.equal(r.displayValue, "+1 (267) 475-4267");
});

check("DEALALITY_PHONE_INVALID_FALLBACK", () => {
  const r = formatDealalityPhone("not-a-phone");
  assert.equal(r.displayValue, "not-a-phone");
  assert.equal(r.telHref, null);
  assert.notEqual(r.formattingOutcome, "FORMATTED");
});

check("DEALALITY_PHONE_NO_AIRTABLE_MUTATION", () => {
  const libSrc = fs.readFileSync(
    path.join(
      ROOT,
      "lib/dealality-shared/dealality-shared-international-phone-formatting-v1.js",
    ),
    "utf8",
  );
  assert.doesNotMatch(libSrc, /airtable|fetch\(|axios/i);
  assert.match(HE, /presentation only|never mutate stored channel values/i);
  const input = { phone: "+12674754267" };
  const before = JSON.stringify(input);
  formatDealalityPhone(input.phone);
  assert.equal(JSON.stringify(input), before);
});

check("BETHESDA_GOLDEN_REFERENCE", () => {
  assert.equal(formatDealalityPhone("+12674754267").displayValue, "+1 (267) 475-4267");
  assert.equal(formatDealalityPhone("+19722335600").displayValue, "+1 (972) 233-5600");
});

check("HE_UI_CONTRACT_PRESERVED", () => {
  // Do not restyle Find Email & Phone / Details as part of phone formatting.
  assert.match(HE, /FIND_LABEL_HTML|Find Email/);
  assert.match(HE, /data-hci-hybrid-enrich|hci-btn--enrich/);
  assert.doesNotMatch(HE, /phone-layout-v2|hci-phone-redesign/i);
});

async function launch() {
  const { chromium } = await import("playwright");
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return chromium.launch({ channel: "chrome", headless: true });
  }
}

async function runBrowser() {
  const browser = await launch();
  const viewports = [
    { w: 1920, h: 1080, label: "desktop" },
    { w: 1440, h: 900, label: "laptop" },
    { w: 390, h: 844, label: "mobile" },
  ];
  const browserResults = [];
  try {
    for (const vp of viewports) {
      const page = await browser.newPage({
        viewport: { width: vp.w, height: vp.h },
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
      await page.waitForSelector(".hci-root", { timeout: 60000 });
      await page.waitForFunction(
        () => {
          const tels = [...document.querySelectorAll('a[href^="tel:"]')].map(
            (a) => ((a.textContent || "") + (a.getAttribute("href") || "")),
          );
          return (
            tels.some((t) => t.includes("(267) 475-4267") && t.includes("tel:+12674754267")) &&
            tels.some((t) => t.includes("(972) 233-5600") && t.includes("tel:+19722335600"))
          );
        },
        null,
        { timeout: 90000 },
      );
      await page.waitForTimeout(500);

      const probe = await page.evaluate(() => {
        const api = window.DealalitySharedPhoneFormattingV1;
        const sample = api
          ? {
              a: api.formatDealalityPhone("+12674754267"),
              b: api.formatDealalityPhone("+19722335600"),
              contract: api.CONTRACT_ID,
            }
          : null;
        const telLinks = [...document.querySelectorAll('a[href^="tel:"]')].map((a) => ({
          text: (a.textContent || "").trim(),
          href: a.getAttribute("href") || "",
        }));
        const body = document.body.innerText || "";
        return {
          hasApi: Boolean(api),
          sample,
          telLinks: telLinks.slice(0, 20),
          has267: body.includes("+1 (267) 475-4267"),
          has972: body.includes("+1 (972) 233-5600"),
          hasFiveCols: /Contact[\s\S]{0,80}Organization[\s\S]{0,80}Function/i.test(body),
          phoneNotAvailableOk: /Phone Not Available/i.test(body),
          findEmailPresent: /Find Email/i.test(body),
          rawUnformatted267: /\+12674754267/.test(body),
          rawUnformatted972: /\+19722335600/.test(body),
        };
      });

      assert.equal(probe.hasApi, true, "DealalitySharedPhoneFormattingV1 missing");
      assert.equal(probe.sample.a.displayValue, "+1 (267) 475-4267");
      assert.equal(probe.sample.a.telHref, "tel:+12674754267");
      assert.equal(probe.sample.b.displayValue, "+1 (972) 233-5600");
      assert.equal(probe.sample.b.telHref, "tel:+19722335600");
      assert.equal(probe.has267, true, "Bethesda +1 (267) 475-4267 not visible");
      assert.equal(probe.has972, true, "Bethesda +1 (972) 233-5600 not visible");
      assert.equal(probe.rawUnformatted267, false, "raw +12674754267 still shown");
      assert.equal(probe.rawUnformatted972, false, "raw +19722335600 still shown");
      assert.equal(probe.findEmailPresent, true);
      assert.equal(probe.hasFiveCols, true);

      const tel267 = probe.telLinks.find((t) => t.text.includes("(267) 475-4267"));
      const tel972 = probe.telLinks.find((t) => t.text.includes("(972) 233-5600"));
      assert.ok(tel267, "tel link for 267 missing");
      assert.ok(tel972, "tel link for 972 missing");
      assert.equal(tel267.href, "tel:+12674754267");
      assert.equal(tel972.href, "tel:+19722335600");

      const shot = path.join(SHOTS, `he-bethesda-contacts-${vp.label}-${vp.w}x${vp.h}.png`);
      await page.screenshot({ path: shot, fullPage: false });
      browserResults.push({
        viewport: `${vp.w}x${vp.h}`,
        label: vp.label,
        ok: true,
        screenshot: shot,
        probe: {
          has267: probe.has267,
          has972: probe.has972,
          tel267: tel267,
          tel972: tel972,
          findEmailPresent: probe.findEmailPresent,
        },
      });
      await page.close();
    }
  } finally {
    await browser.close();
  }
  return browserResults;
}

let browserResults = null;
if (process.env.DEALALITY_PHONE_BROWSER === "1") {
  try {
    browserResults = await runBrowser();
    results.push({ id: "HE_PHONE_PLAYWRIGHT_BETHESDA_VIEWPORTS", ok: true });
  } catch (err) {
    results.push({
      id: "HE_PHONE_PLAYWRIGHT_BETHESDA_VIEWPORTS",
      ok: false,
      error: String(err && err.message ? err.message : err),
    });
  }
}

const failed = results.filter((r) => !r.ok);
const summary = {
  contract: CONTRACT_ID,
  hotelId: HOTEL,
  hotelName: "Bethesda Marriott",
  ok: failed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
  browserResults,
  generatedAt: new Date().toISOString(),
};
fs.writeFileSync(path.join(OUT, "HE_CONSUMER_RESULTS.json"), JSON.stringify(summary, null, 2));
fs.writeFileSync(
  path.join(OUT, "HE_BETHESDA_CLOSEOUT.md"),
  [
    "# HE Contact Intelligence — Shared International Phone Formatting",
    "",
    `**Status:** ${summary.ok ? "PASS" : "FAIL"}`,
    "",
    `Hotel: Bethesda Marriott (\`${HOTEL}\`)`,
    "Golden displays: `+1 (267) 475-4267`, `+1 (972) 233-5600`",
    "Dial links: `tel:+12674754267`, `tel:+19722335600`",
    "",
    "## Gates",
    "",
    ...results.map((r) => `- ${r.id}: **${r.ok ? "PASS" : "FAIL"}**${r.error ? ` — ${r.error}` : ""}`),
    "",
  ].join("\n"),
);
console.log(
  JSON.stringify(
    { ok: summary.ok, passed: summary.passed, failed: summary.failed, browser: Boolean(browserResults) },
    null,
    2,
  ),
);
if (!summary.ok) {
  console.error(failed);
  process.exit(1);
}
